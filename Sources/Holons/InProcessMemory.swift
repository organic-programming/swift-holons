import Dispatch
import Foundation
#if os(Linux)
import Glibc
#else
import Darwin
#endif

protocol RelayHandle: AnyObject {
    var boundURI: String { get }
    var debugSummary: String { get }
    func close()
}

private let memRegistryLock = NSLock()
private var memRegistry: [String: MemRuntimeListener] = [:]

func registerMemListener(_ listener: MemRuntimeListener, name: String) throws {
    let key = normalizedMemRegistryKey(name)

    memRegistryLock.lock()
    defer { memRegistryLock.unlock() }

    if let existing = memRegistry[key], existing !== listener {
        throw TransportError.listenFailed("mem listener already registered for mem://\(key)")
    }
    memRegistry[key] = listener
}

func lookupMemListener(name: String) -> MemRuntimeListener? {
    let key = normalizedMemRegistryKey(name)
    memRegistryLock.lock()
    let listener = memRegistry[key]
    memRegistryLock.unlock()
    return listener
}

func unregisterMemListener(_ listener: MemRuntimeListener, name: String) {
    let key = normalizedMemRegistryKey(name)

    memRegistryLock.lock()
    if let existing = memRegistry[key], existing === listener {
        memRegistry.removeValue(forKey: key)
    }
    memRegistryLock.unlock()
}

private func normalizedMemRegistryKey(_ name: String) -> String {
    name.trimmingCharacters(in: .whitespacesAndNewlines)
}

private final class RuntimeConnectionRelay {
    private let left: RuntimeConnection
    private let right: RuntimeConnection
    private let onClose: (ObjectIdentifier) -> Void
    private let stateLock = NSLock()
    private var closed = false
    private var leftPreview = Data()
    private var rightPreview = Data()

    init(
        left: RuntimeConnection,
        right: RuntimeConnection,
        onClose: @escaping (ObjectIdentifier) -> Void
    ) {
        self.left = left
        self.right = right
        self.onClose = onClose
    }

    var debugSummary: String {
        stateLock.lock()
        defer { stateLock.unlock() }
        return "left=\(relayHexPreview(leftPreview)) right=\(relayHexPreview(rightPreview))"
    }

    func start() {
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            self?.pump(from: self?.left, to: self?.right, previewLeft: true)
        }
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            self?.pump(from: self?.right, to: self?.left, previewLeft: false)
        }
    }

    func close() {
        stateLock.lock()
        if closed {
            stateLock.unlock()
            return
        }
        closed = true
        stateLock.unlock()

        try? left.close()
        try? right.close()
        onClose(ObjectIdentifier(self))
    }

    private func pump(
        from source: RuntimeConnection?,
        to destination: RuntimeConnection?,
        previewLeft: Bool
    ) {
        guard let source, let destination else {
            return
        }

        while true {
            do {
                let data = try source.read(maxBytes: 16 * 1024)
                if data.isEmpty {
                    close()
                    return
                }

                appendPreview(data, previewLeft: previewLeft)
                try destination.write(data)
            } catch {
                close()
                return
            }
        }
    }

    private func appendPreview(_ bytes: Data, previewLeft: Bool) {
        stateLock.lock()
        defer { stateLock.unlock() }

        let remaining: Int
        if previewLeft {
            remaining = max(0, 64 - leftPreview.count)
            if remaining > 0 {
                leftPreview.append(bytes.prefix(remaining))
            }
        } else {
            remaining = max(0, 64 - rightPreview.count)
            if remaining > 0 {
                rightPreview.append(bytes.prefix(remaining))
            }
        }
    }
}

final class MemServeBridge {
    private let name: String
    private let host: String
    private let port: Int
    private let listener: MemRuntimeListener
    private let stateLock = NSLock()
    private var started = false
    private var closed = false
    private var relays: [ObjectIdentifier: RuntimeConnectionRelay] = [:]

    init(name: String, host: String, port: Int) throws {
        self.name = name
        self.host = host
        self.port = port
        self.listener = MemRuntimeListener(name: name)
        try registerMemListener(listener, name: name)
    }

    func start() {
        stateLock.lock()
        if started {
            stateLock.unlock()
            return
        }
        started = true
        stateLock.unlock()

        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            self?.acceptLoop()
        }
    }

    func stop() {
        stateLock.lock()
        if closed {
            stateLock.unlock()
            return
        }
        closed = true
        let relays = Array(self.relays.values)
        self.relays.removeAll()
        stateLock.unlock()

        unregisterMemListener(listener, name: name)
        try? listener.close()
        relays.forEach { $0.close() }
    }

    private func acceptLoop() {
        while true {
            if isClosed {
                return
            }

            let memConnection: RuntimeConnection
            do {
                memConnection = try listener.accept()
            } catch {
                if isClosed {
                    return
                }
                stop()
                return
            }

            do {
                let tcpConnection = try dialRuntimeLoopbackConnection(host: host, port: port)
                let relay = RuntimeConnectionRelay(
                    left: memConnection,
                    right: tcpConnection
                ) { [weak self] identifier in
                    self?.removeRelay(identifier)
                }
                storeRelay(relay)
                relay.start()
            } catch {
                try? memConnection.close()
            }
        }
    }

    private var isClosed: Bool {
        stateLock.lock()
        let value = closed
        stateLock.unlock()
        return value
    }

    private func storeRelay(_ relay: RuntimeConnectionRelay) {
        stateLock.lock()
        if closed {
            stateLock.unlock()
            relay.close()
            return
        }
        relays[ObjectIdentifier(relay)] = relay
        stateLock.unlock()
    }

    private func removeRelay(_ identifier: ObjectIdentifier) {
        stateLock.lock()
        relays.removeValue(forKey: identifier)
        stateLock.unlock()
    }
}

final class MemDialRelay: RelayHandle {
    private let upstream: RuntimeConnection
    private let listener: TCPRuntimeListener
    private let stateLock = NSLock()
    private var closed = false
    private var relay: RuntimeConnectionRelay?

    var boundURI: String {
        listener.boundURI
    }

    var debugSummary: String {
        stateLock.lock()
        let summary = relay?.debugSummary ?? "left=- right=-"
        stateLock.unlock()
        return summary
    }

    init(name: String) throws {
        guard let memListener = lookupMemListener(name: name) else {
            throw ConnectError.memListenerNotFound(name)
        }
        self.upstream = try memListener.dial()
        self.listener = try TCPRuntimeListener(host: "127.0.0.1", port: 0)

        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            self?.acceptAndRelay()
        }
    }

    func close() {
        stateLock.lock()
        if closed {
            stateLock.unlock()
            return
        }
        closed = true
        let relay = self.relay
        self.relay = nil
        stateLock.unlock()

        try? listener.close()
        relay?.close()
        try? upstream.close()
    }

    private func acceptAndRelay() {
        let clientConnection: RuntimeConnection
        do {
            clientConnection = try listener.accept()
        } catch {
            close()
            return
        }

        stateLock.lock()
        if closed {
            stateLock.unlock()
            try? clientConnection.close()
            return
        }

        let relay = RuntimeConnectionRelay(
            left: upstream,
            right: clientConnection
        ) { [weak self] _ in
            self?.close()
        }
        self.relay = relay
        stateLock.unlock()

        relay.start()
    }
}

private func relayHexPreview(_ data: Data) -> String {
    if data.isEmpty {
        return "-"
    }
    return data.map { String(format: "%02x", $0) }.joined()
}

private func dialRuntimeLoopbackConnection(host: String, port: Int) throws -> RuntimeConnection {
    let fd = memBridgeSocket(AF_INET, memBridgeSocketStreamType, 0)
    guard fd >= 0 else {
        throw TransportError.listenFailed(String(cString: strerror(errno)))
    }

    var address = sockaddr_in()
    #if !os(Linux)
    address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
    #endif
    address.sin_family = sa_family_t(AF_INET)
    address.sin_port = in_port_t(UInt16(port).bigEndian)
    address.sin_addr = in_addr(s_addr: inet_addr(host))

    let result = withUnsafePointer(to: &address) { ptr in
        ptr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sockaddrPtr in
            memBridgeConnect(fd, sockaddrPtr, socklen_t(MemoryLayout<sockaddr_in>.size))
        }
    }
    if result != 0 {
        let message = String(cString: strerror(errno))
        _ = memBridgeClose(fd)
        throw TransportError.listenFailed(message)
    }

    return POSIXRuntimeConnection(
        readFD: fd,
        writeFD: fd,
        ownsReadFD: true,
        ownsWriteFD: true
    )
}

private var memBridgeSocketStreamType: Int32 {
    #if os(Linux)
    return Int32(SOCK_STREAM.rawValue)
    #else
    return SOCK_STREAM
    #endif
}

private func memBridgeSocket(_ domain: Int32, _ type: Int32, _ proto: Int32) -> Int32 {
    #if os(Linux)
    return Glibc.socket(domain, type, proto)
    #else
    return Darwin.socket(domain, type, proto)
    #endif
}

private func memBridgeConnect(_ fd: Int32, _ address: UnsafePointer<sockaddr>?, _ length: socklen_t) -> Int32 {
    #if os(Linux)
    return Glibc.connect(fd, address, length)
    #else
    return Darwin.connect(fd, address, length)
    #endif
}

private func memBridgeClose(_ fd: Int32) -> Int32 {
    #if os(Linux)
    return Glibc.close(fd)
    #else
    return Darwin.close(fd)
    #endif
}
