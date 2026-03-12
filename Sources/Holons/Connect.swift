import Dispatch
import Foundation
import GRPC
import Logging
import NIOCore
import NIOPosix
#if os(Linux)
import Glibc
#else
import Darwin
#endif

public struct ConnectOptions {
    public var timeout: TimeInterval
    public var transport: String
    public var start: Bool
    public var portFile: String?

    public init(
        timeout: TimeInterval = 5.0,
        transport: String = "stdio",
        start: Bool = true,
        portFile: String? = nil
    ) {
        self.timeout = timeout
        self.transport = transport
        self.start = start
        self.portFile = portFile
    }
}

public enum ConnectError: Error, CustomStringConvertible {
    case targetRequired
    case unsupportedTransport(String)
    case unsupportedDialTarget(String)
    case invalidDirectTarget(String)
    case holonNotFound(String)
    case holonNotRunning(String)
    case missingManifest(String)
    case missingBinary(String)
    case binaryNotFound(String)
    case startupFailed(String)
    case readinessFailed(String)
    case ioFailure(String)

    public var description: String {
        switch self {
        case .targetRequired:
            return "target is required"
        case let .unsupportedTransport(transport):
            return "unsupported transport \"\(transport)\""
        case let .unsupportedDialTarget(target):
            return "unsupported dial target \"\(target)\""
        case let .invalidDirectTarget(target):
            return "invalid direct target \"\(target)\""
        case let .holonNotFound(slug):
            return "holon \"\(slug)\" not found"
        case let .holonNotRunning(slug):
            return "holon \"\(slug)\" is not running"
        case let .missingManifest(slug):
            return "holon \"\(slug)\" has no manifest"
        case let .missingBinary(slug):
            return "holon \"\(slug)\" has no artifacts.binary"
        case let .binaryNotFound(slug):
            return "built binary not found for holon \"\(slug)\""
        case let .startupFailed(message):
            return message
        case let .readinessFailed(message):
            return message
        case let .ioFailure(message):
            return message
        }
    }
}

private struct RawBytesPayload: GRPCPayload {
    var data: Data

    init(data: Data = Data()) {
        self.data = data
    }

    init(serializedByteBuffer: inout ByteBuffer) throws {
        self.data = serializedByteBuffer.readData(length: serializedByteBuffer.readableBytes) ?? Data()
    }

    func serialize(into buffer: inout ByteBuffer) throws {
        buffer.writeBytes(self.data)
    }
}

private struct DialTarget {
    enum Kind {
        case hostPort(String, Int)
        case unix(String)
        case connectedSocket(Int32)
    }

    let kind: Kind
}

private struct ConnectionHandle {
    let group: MultiThreadedEventLoopGroup
    let process: Process?
    let relay: SocketRelay?
    let ephemeral: Bool
}

private final class LineQueue {
    private let lock = NSLock()
    private let semaphore = DispatchSemaphore(value: 0)
    private var lines: [String] = []

    func push(_ line: String) {
        lock.lock()
        lines.append(line)
        lock.unlock()
        semaphore.signal()
    }

    func pop(timeout: TimeInterval) -> String? {
        let result = semaphore.wait(timeout: .now() + timeout)
        guard result == .success else {
            return nil
        }

        lock.lock()
        defer { lock.unlock() }
        guard !lines.isEmpty else {
            return nil
        }
        return lines.removeFirst()
    }
}

private final class StringCollector {
    private let lock = NSLock()
    private var lines: [String] = []

    func append(_ line: String) {
        lock.lock()
        lines.append(line)
        lock.unlock()
    }

    var text: String {
        lock.lock()
        defer { lock.unlock() }
        return lines.joined(separator: "\n")
    }
}

private final class ConnectionDiagnostics: NSObject, ClientErrorDelegate, ConnectivityStateDelegate {
    private let collector = StringCollector()

    var text: String {
        collector.text
    }

    func didCatchError(_ error: Error, logger: Logger, file: StaticString, line: Int) {
        _ = logger
        collector.append("client error: \(error) @\(file):\(line)")
    }

    func connectivityStateDidChange(from oldState: ConnectivityState, to newState: ConnectivityState) {
        collector.append("connectivity: \(oldState) -> \(newState)")
    }

    func connectionStartedQuiescing() {
        collector.append("connectivity: quiescing")
    }
}

private final class SocketRelay {
    private let stateLock = NSLock()
    private let listener: TCPRuntimeListener
    private let upstreamFD: Int32
    private var closed = false
    private var connection: RuntimeConnection?
    private var upstreamPreview = Data()
    private var connectionPreview = Data()

    var boundURI: String {
        listener.boundURI
    }

    var debugSummary: String {
        stateLock.lock()
        defer { stateLock.unlock() }
        return "upstream=\(hexPreview(upstreamPreview)) client=\(hexPreview(connectionPreview))"
    }

    init(upstreamFD: Int32) throws {
        self.listener = try TCPRuntimeListener(host: "127.0.0.1", port: 0)
        self.upstreamFD = upstreamFD

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
        let connection = self.connection
        self.connection = nil
        stateLock.unlock()

        _ = sysClose(upstreamFD)
        try? connection?.close()
        try? listener.close()
    }

    private func acceptAndRelay() {
        let accepted: RuntimeConnection
        do {
            accepted = try listener.accept()
        } catch {
            close()
            return
        }

        stateLock.lock()
        if closed {
            stateLock.unlock()
            try? accepted.close()
            return
        }
        connection = accepted
        stateLock.unlock()

        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            self?.forwardUpstream(to: accepted)
        }
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            self?.forwardConnection(from: accepted)
        }
    }

    private func forwardUpstream(to connection: RuntimeConnection) {
        var buffer = [UInt8](repeating: 0, count: 16 * 1024)

        while true {
            let readCount = buffer.withUnsafeMutableBytes { ptr in
                sysRead(upstreamFD, ptr.baseAddress, ptr.count)
            }

            if readCount > 0 {
                do {
                    let chunk = Data(buffer.prefix(Int(readCount)))
                    appendPreview(&upstreamPreview, bytes: chunk)
                    try connection.write(chunk)
                } catch {
                    close()
                    return
                }
            } else if readCount == 0 {
                close()
                return
            } else if currentErrno() == EINTR {
                continue
            } else {
                close()
                return
            }
        }
    }

    private func forwardConnection(from connection: RuntimeConnection) {
        while true {
            do {
                let data = try connection.read(maxBytes: 16 * 1024)
                if data.isEmpty {
                    close()
                    return
                }

                appendPreview(&connectionPreview, bytes: data)
                try data.withUnsafeBytes { ptr in
                    guard let base = ptr.baseAddress else {
                        return
                    }
                    try writeAll(fd: upstreamFD, base: base, count: ptr.count)
                }
            } catch {
                close()
                return
            }
        }
    }

    private func appendPreview(_ preview: inout Data, bytes: Data) {
        stateLock.lock()
        defer { stateLock.unlock() }
        guard preview.count < 64 else {
            return
        }

        let remaining = 64 - preview.count
        preview.append(bytes.prefix(remaining))
    }

    private func hexPreview(_ data: Data) -> String {
        if data.isEmpty {
            return "-"
        }
        return data.map { String(format: "%02x", $0) }.joined()
    }
}

private let connectStateLock = NSLock()
private var connectHandles: [ObjectIdentifier: ConnectionHandle] = [:]

public func connect(_ target: String) throws -> GRPCChannel {
    try connectInternal(target, options: ConnectOptions())
}

public func connect(_ target: String, options: ConnectOptions) throws -> GRPCChannel {
    try connectInternal(target, options: options)
}

public func disconnect(_ channel: GRPCChannel) throws {
    guard let connection = channel as? ClientConnection else {
        try waitForClose(channel.close())
        return
    }

    let key = ObjectIdentifier(connection)

    connectStateLock.lock()
    let handle = connectHandles.removeValue(forKey: key)
    connectStateLock.unlock()

    var firstError: Error?

    do {
        try waitForClose(connection.close())
    } catch {
        firstError = error
    }

    handle?.relay?.close()

    if let group = handle?.group {
        do {
            try group.syncShutdownGracefully()
        } catch {
            if firstError == nil {
                firstError = error
            }
        }
    }

    if let process = handle?.process {
        do {
            if handle?.ephemeral == true {
                try stopProcess(process)
            } else {
                reapProcess(process)
            }
        } catch {
            if firstError == nil {
                firstError = error
            }
        }
    }

    if let firstError {
        throw firstError
    }
}

private func connectInternal(_ target: String, options: ConnectOptions) throws -> GRPCChannel {
    let trimmed = target.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmed.isEmpty else {
        throw ConnectError.targetRequired
    }

    let timeout = options.timeout > 0 ? options.timeout : 5.0
    let transport = try normalizedTransport(options.transport)

    if isDirectTarget(trimmed) {
        return try dialReady(
            target: try normalizeDialTarget(trimmed),
            timeout: timeout,
            process: nil,
            ephemeral: false,
            stderr: nil
        )
    }

    guard let entry = try findBySlug(trimmed) else {
        throw ConnectError.holonNotFound(trimmed)
    }

    let portFile = normalizedPortFilePath(options.portFile, slug: entry.slug)
    if let reusable = try usablePortFile(portFile, timeout: timeout) {
        return try dialReady(
            target: try normalizeDialTarget(reusable),
            timeout: timeout,
            process: nil,
            ephemeral: false,
            stderr: nil
        )
    }

    guard options.start else {
        throw ConnectError.holonNotRunning(trimmed)
    }

    let binaryPath = try resolveBinaryPath(entry)

    switch transport {
    case "stdio":
        return try connectStdioHolon(
            binaryPath: binaryPath,
            workingDirectory: entry.dir.path,
            timeout: timeout
        )

    case "tcp":
        let started = try startTCPHolon(
            binaryPath: binaryPath,
            workingDirectory: entry.dir.path,
            timeout: timeout
        )
        do {
            let channel = try dialReady(
                target: try normalizeDialTarget(started.uri),
                timeout: timeout,
                process: started.process,
                ephemeral: false,
                stderr: started.stderr
            )
            do {
                try writePortFile(path: portFile, uri: started.uri)
            } catch {
                try? disconnect(channel)
                try? stopProcess(started.process)
                throw error
            }
            return channel
        } catch {
            throw error
        }

    default:
        throw ConnectError.unsupportedTransport(transport)
    }
}

private func normalizedTransport(_ transport: String) throws -> String {
    let normalized = transport.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    if normalized.isEmpty {
        return "stdio"
    }

    switch normalized {
    case "stdio", "tcp":
        return normalized
    default:
        throw ConnectError.unsupportedTransport(transport)
    }
}

private func dialReady(
    target: DialTarget,
    timeout: TimeInterval,
    process: Process?,
    relay: SocketRelay? = nil,
    ephemeral: Bool,
    stderr: StringCollector?
) throws -> GRPCChannel {
    let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let diagnostics = ConnectionDiagnostics()
    let connection: ClientConnection
    do {
        connection = try makeConnection(target: target, group: group, diagnostics: diagnostics)
    } catch {
        closeConnectedSocketIfNeeded(target)
        try? group.syncShutdownGracefully()
        throw error
    }

    do {
        try waitForReady(
            channel: connection,
            timeout: timeout,
            process: process,
            stderr: stderr,
            diagnostics: diagnostics,
            relay: relay
        )
    } catch {
        relay?.close()
        try? waitForClose(connection.close())
        try? group.syncShutdownGracefully()
        if let process {
            try? stopProcess(process)
        }
        throw error
    }

    let handle = ConnectionHandle(
        group: group,
        process: process,
        relay: relay,
        ephemeral: ephemeral
    )

    connectStateLock.lock()
    connectHandles[ObjectIdentifier(connection)] = handle
    connectStateLock.unlock()

    return connection
}

private func makeConnection(
    target: DialTarget,
    group: MultiThreadedEventLoopGroup,
    diagnostics: ConnectionDiagnostics? = nil
) throws -> ClientConnection {
    switch target.kind {
    case let .hostPort(host, port):
        return ClientConnection.insecure(group: group)
            .withErrorDelegate(diagnostics)
            .withConnectivityStateDelegate(diagnostics)
            .connect(host: host, port: port)
    case let .unix(path):
        var configuration = ClientConnection.Configuration.default(
            target: .unixDomainSocket(path),
            eventLoopGroup: group
        )
        configuration.connectionBackoff = nil
        configuration.errorDelegate = diagnostics
        configuration.connectivityStateDelegate = diagnostics
        return ClientConnection(configuration: configuration)
    case let .connectedSocket(socket):
        return ClientConnection.insecure(group: group)
            .withConnectionReestablishment(enabled: false)
            .withErrorDelegate(diagnostics)
            .withConnectivityStateDelegate(diagnostics)
            .withConnectedSocket(socket)
    }
}

private func waitForReady(
    channel: ClientConnection,
    timeout: TimeInterval,
    process: Process?,
    stderr: StringCollector?,
    diagnostics: ConnectionDiagnostics?,
    relay: SocketRelay?
) throws {
    do {
        _ = try describe(channel: channel, timeout: timeout)
        return
    } catch {
        if let process, !process.isRunning {
            let stderrText = stderr?.text.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            if !stderrText.isEmpty {
                throw ConnectError.startupFailed("holon exited before becoming ready: \(stderrText)")
            }
            throw ConnectError.startupFailed("holon exited before becoming ready")
        }
        let stderrText = stderr?.text.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let diagnosticsText = diagnostics?.text.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let relayText = relay?.debugSummary ?? ""
        if !stderrText.isEmpty {
            throw ConnectError.readinessFailed("timed out waiting for holon readiness: \(error) [stderr: \(stderrText)] [client: \(diagnosticsText)] [relay: \(relayText)]")
        }
        if !diagnosticsText.isEmpty {
            throw ConnectError.readinessFailed("timed out waiting for holon readiness: \(error) [client: \(diagnosticsText)] [relay: \(relayText)]")
        }
        if !relayText.isEmpty {
            throw ConnectError.readinessFailed("timed out waiting for holon readiness: \(error) [relay: \(relayText)]")
        }
        throw ConnectError.readinessFailed("timed out waiting for holon readiness: \(error)")
    }
}

private func describe(channel: GRPCChannel, timeout: TimeInterval) throws -> RawBytesPayload {
    let call = channel.makeUnaryCall(
        path: "/holonmeta.v1.HolonMeta/Describe",
        request: RawBytesPayload(),
        callOptions: CallOptions(
            timeLimit: .timeout(.nanoseconds(Int64(timeout * 1_000_000_000)))
        )
    ) as UnaryCall<RawBytesPayload, RawBytesPayload>
    return try call.response.wait()
}

private struct StartedTCPHolon {
    let uri: String
    let process: Process
    let stderr: StringCollector
}

private func startTCPHolon(
    binaryPath: String,
    workingDirectory: String?,
    timeout: TimeInterval
) throws -> StartedTCPHolon {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: binaryPath)
    process.arguments = ["serve", "--listen", "tcp://127.0.0.1:0"]
    if let workingDirectory, !workingDirectory.isEmpty {
        process.currentDirectoryURL = URL(fileURLWithPath: workingDirectory, isDirectory: true)
    }

    let stdout = Pipe()
    let stderr = Pipe()
    let stderrCollector = StringCollector()
    let lineQueue = LineQueue()

    process.standardOutput = stdout
    process.standardError = stderr

    try process.run()

    startLineReader(handle: stdout.fileHandleForReading, queue: lineQueue, collector: nil)
    startLineReader(handle: stderr.fileHandleForReading, queue: lineQueue, collector: stderrCollector)

    let deadline = Date().addingTimeInterval(timeout)
    while Date() < deadline {
        if !process.isRunning {
            let stderrText = stderrCollector.text.trimmingCharacters(in: .whitespacesAndNewlines)
            if !stderrText.isEmpty {
                throw ConnectError.startupFailed("holon exited before advertising an address: \(stderrText)")
            }
            throw ConnectError.startupFailed("holon exited before advertising an address")
        }

        if let line = lineQueue.pop(timeout: 0.05), let uri = firstURI(in: line) {
            return StartedTCPHolon(uri: uri, process: process, stderr: stderrCollector)
        }
    }

    try? stopProcess(process)
    let stderrText = stderrCollector.text.trimmingCharacters(in: .whitespacesAndNewlines)
    if !stderrText.isEmpty {
        throw ConnectError.startupFailed("timed out waiting for holon startup: \(stderrText)")
    }
    throw ConnectError.startupFailed("timed out waiting for holon startup")
}

private func connectStdioHolon(
    binaryPath: String,
    workingDirectory: String?,
    timeout: TimeInterval
) throws -> GRPCChannel {
    let sockets = try makeSocketPair()
    let childInputFD = try duplicateDescriptor(sockets.child)
    let childOutputFD = try duplicateDescriptor(sockets.child)
    let childInput = FileHandle(fileDescriptor: childInputFD, closeOnDealloc: true)
    let childOutput = FileHandle(fileDescriptor: childOutputFD, closeOnDealloc: true)
    let stderrPipe = Pipe()
    let stderrCollector = StringCollector()
    let relay: SocketRelay
    do {
        relay = try SocketRelay(upstreamFD: sockets.client)
    } catch {
        try? childInput.close()
        try? childOutput.close()
        _ = sysClose(sockets.child)
        _ = sysClose(sockets.client)
        throw error
    }

    let process = Process()
    process.executableURL = URL(fileURLWithPath: binaryPath)
    process.arguments = ["serve", "--listen", "stdio://"]
    if let workingDirectory, !workingDirectory.isEmpty {
        process.currentDirectoryURL = URL(fileURLWithPath: workingDirectory, isDirectory: true)
    }
    process.standardInput = childInput
    process.standardOutput = childOutput
    process.standardError = stderrPipe

    do {
        try process.run()
    } catch {
        relay.close()
        try? childInput.close()
        try? childOutput.close()
        _ = sysClose(sockets.child)
        throw error
    }

    startLineReader(handle: stderrPipe.fileHandleForReading, queue: nil, collector: stderrCollector)
    try? childInput.close()
    try? childOutput.close()
    _ = sysClose(sockets.child)
    do {
        return try dialReady(
            target: try normalizeDialTarget(relay.boundURI),
            timeout: timeout,
            process: process,
            relay: relay,
            ephemeral: true,
            stderr: stderrCollector
        )
    } catch {
        relay.close()
        try? stopProcess(process)
        throw error
    }
}

private func usablePortFile(_ path: String, timeout: TimeInterval) throws -> String? {
    guard let data = FileManager.default.contents(atPath: path),
          let rawTarget = String(data: data, encoding: .utf8)?
            .trimmingCharacters(in: .whitespacesAndNewlines) else {
        return nil
    }

    guard !rawTarget.isEmpty else {
        try? FileManager.default.removeItem(atPath: path)
        return nil
    }

    let probeTimeout = min(max(timeout / 4.0, 0.25), 1.0)
    do {
        let channel = try dialReady(
            target: try normalizeDialTarget(rawTarget),
            timeout: probeTimeout,
            process: nil,
            ephemeral: false,
            stderr: nil
        )
        try disconnect(channel)
        return rawTarget
    } catch {
        try? FileManager.default.removeItem(atPath: path)
        return nil
    }
}

private func resolveBinaryPath(_ entry: HolonEntry) throws -> String {
    guard let manifest = entry.manifest else {
        throw ConnectError.missingManifest(entry.slug)
    }

    let binary = manifest.artifacts.binary.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !binary.isEmpty else {
        throw ConnectError.missingBinary(entry.slug)
    }

    if binary.hasPrefix("/") {
        if FileManager.default.isExecutableFile(atPath: binary) {
            return binary
        }
    }

    let candidate = entry.dir
        .appendingPathComponent(".op")
        .appendingPathComponent("build")
        .appendingPathComponent("bin")
        .appendingPathComponent((binary as NSString).lastPathComponent)

    if FileManager.default.isExecutableFile(atPath: candidate.path) {
        return candidate.path
    }

    if let resolved = which((binary as NSString).lastPathComponent) {
        return resolved
    }

    throw ConnectError.binaryNotFound(entry.slug)
}

private func normalizedPortFilePath(_ override: String?, slug: String) -> String {
    let trimmed = override?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
    if !trimmed.isEmpty {
        return trimmed
    }

    return URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
        .appendingPathComponent(".op")
        .appendingPathComponent("run")
        .appendingPathComponent("\(slug).port")
        .path
}

private func writePortFile(path: String, uri: String) throws {
    let fileURL = URL(fileURLWithPath: path)
    try FileManager.default.createDirectory(
        at: fileURL.deletingLastPathComponent(),
        withIntermediateDirectories: true
    )
    try (uri.trimmingCharacters(in: .whitespacesAndNewlines) + "\n")
        .write(to: fileURL, atomically: true, encoding: .utf8)
}

private func normalizeDialTarget(_ target: String) throws -> DialTarget {
    let trimmed = target.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmed.isEmpty else {
        throw ConnectError.invalidDirectTarget(target)
    }

    guard trimmed.contains("://") else {
        let hostPort = try parseHostPort(trimmed)
        return DialTarget(kind: .hostPort(hostPort.host, hostPort.port))
    }

    let parsed = try Transport.parse(trimmed)
    switch parsed.scheme {
    case "tcp":
        let host = normalizedLoopbackHost(parsed.host ?? "127.0.0.1")
        return DialTarget(kind: .hostPort(host, parsed.port ?? 9090))
    case "unix":
        guard let path = parsed.path, !path.isEmpty else {
            throw ConnectError.invalidDirectTarget(trimmed)
        }
        return DialTarget(kind: .unix(path))
    default:
        throw ConnectError.unsupportedDialTarget(trimmed)
    }
}

private func normalizedLoopbackHost(_ host: String) -> String {
    let trimmed = host.trimmingCharacters(in: .whitespacesAndNewlines)
    switch trimmed {
    case "", "0.0.0.0", "::", "[::]":
        return "127.0.0.1"
    default:
        return trimmed
    }
}

private func parseHostPort(_ target: String) throws -> (host: String, port: Int) {
    if target.hasPrefix("["),
       let end = target.firstIndex(of: "]"),
       target.index(after: end) < target.endIndex,
       target[target.index(after: end)] == ":" {
        let host = String(target[target.index(after: target.startIndex)..<end])
        let portText = String(target[target.index(end, offsetBy: 2)...])
        guard let port = Int(portText) else {
            throw ConnectError.invalidDirectTarget(target)
        }
        return (host, port)
    }

    guard let colon = target.lastIndex(of: ":") else {
        throw ConnectError.invalidDirectTarget(target)
    }

    let host = String(target[..<colon]).trimmingCharacters(in: .whitespacesAndNewlines)
    let portText = String(target[target.index(after: colon)...]).trimmingCharacters(in: .whitespacesAndNewlines)
    guard !host.isEmpty, let port = Int(portText) else {
        throw ConnectError.invalidDirectTarget(target)
    }
    return (host, port)
}

private func isDirectTarget(_ target: String) -> Bool {
    target.contains("://") || target.contains(":")
}

private func firstURI(in line: String) -> String? {
    for field in line.split(whereSeparator: \.isWhitespace) {
        let candidate = field.trimmingCharacters(in: CharacterSet(charactersIn: "\"'()[]{}.,"))
        if candidate.hasPrefix("tcp://") ||
            candidate.hasPrefix("unix://") ||
            candidate.hasPrefix("stdio://") ||
            candidate.hasPrefix("ws://") ||
            candidate.hasPrefix("wss://") {
            return candidate
        }
    }
    return nil
}

private func startLineReader(handle: FileHandle, queue: LineQueue?, collector: StringCollector?) {
    DispatchQueue.global(qos: .utility).async {
        var buffer = Data()

        while true {
            let chunk = handle.availableData
            if chunk.isEmpty {
                if !buffer.isEmpty, let line = String(data: buffer, encoding: .utf8) {
                    collector?.append(line)
                    queue?.push(line)
                }
                return
            }

            buffer.append(chunk)
            while let newline = buffer.firstIndex(of: 0x0A) {
                let lineData = buffer.prefix(upTo: newline)
                buffer.removeSubrange(...newline)
                guard let line = String(data: lineData, encoding: .utf8) else {
                    continue
                }
                collector?.append(line)
                queue?.push(line)
            }
        }
    }
}

private func stopProcess(_ process: Process) throws {
    guard process.isRunning else {
        return
    }

    let pid = process.processIdentifier
    if pid > 0 {
        if sysKill(pid, SIGTERM) != 0 && currentErrno() != ESRCH {
            throw ConnectError.ioFailure("failed to send SIGTERM: \(sysErrnoMessage())")
        }
    }

    let termDeadline = Date().addingTimeInterval(2.0)
    while process.isRunning && Date() < termDeadline {
        Thread.sleep(forTimeInterval: 0.05)
    }

    if process.isRunning, pid > 0 {
        if sysKill(pid, SIGKILL) != 0 && currentErrno() != ESRCH {
            throw ConnectError.ioFailure("failed to send SIGKILL: \(sysErrnoMessage())")
        }
    }

    let killDeadline = Date().addingTimeInterval(2.0)
    while process.isRunning && Date() < killDeadline {
        Thread.sleep(forTimeInterval: 0.05)
    }
}

private func reapProcess(_ process: Process) {
    DispatchQueue.global(qos: .background).async {
        process.waitUntilExit()
    }
}

private func waitForClose(_ future: EventLoopFuture<Void>) throws {
    try future.wait()
}

private func writeAll(fd: Int32, base: UnsafeRawPointer, count: Int) throws {
    var written = 0
    while written < count {
        let result = sysWrite(fd, base.advanced(by: written), count - written)
        if result > 0 {
            written += result
        } else if result < 0 && currentErrno() == EINTR {
            continue
        } else if result < 0 {
            throw ConnectError.ioFailure(sysErrnoMessage())
        } else {
            throw ConnectError.ioFailure("zero-byte write")
        }
    }
}

private func closeConnectedSocketIfNeeded(_ target: DialTarget) {
    guard case let .connectedSocket(socket) = target.kind else {
        return
    }
    _ = sysClose(socket)
}

private func makeSocketPair() throws -> (client: Int32, child: Int32) {
    var fds: [Int32] = [0, 0]
    let rc = fds.withUnsafeMutableBufferPointer { buffer in
        sysSocketPair(AF_UNIX, connectedSocketType, 0, buffer.baseAddress)
    }
    if rc != 0 {
        throw ConnectError.ioFailure("socketpair failed: \(sysErrnoMessage())")
    }
    return (client: fds[0], child: fds[1])
}

private func duplicateDescriptor(_ fd: Int32) throws -> Int32 {
    let duplicated = sysDup(fd)
    if duplicated < 0 {
        throw ConnectError.ioFailure("dup failed: \(sysErrnoMessage())")
    }
    return duplicated
}

private func which(_ executable: String) -> String? {
    let path = ProcessInfo.processInfo.environment["PATH"] ?? ""
    for directory in path.split(separator: ":") {
        let candidate = URL(fileURLWithPath: String(directory), isDirectory: true)
            .appendingPathComponent(executable)
            .path
        if FileManager.default.isExecutableFile(atPath: candidate) {
            return candidate
        }
    }
    return nil
}

private func currentErrno() -> Int32 {
    #if os(Linux)
    return errno
    #else
    return Darwin.errno
    #endif
}

private var connectedSocketType: Int32 {
    #if os(Linux)
    return Int32(SOCK_STREAM.rawValue)
    #else
    return SOCK_STREAM
    #endif
}

private func sysErrnoMessage() -> String {
    String(cString: strerror(currentErrno()))
}

private func sysRead(_ fd: Int32, _ buf: UnsafeMutableRawPointer?, _ count: Int) -> Int {
    #if os(Linux)
    return Glibc.read(fd, buf, count)
    #else
    return Darwin.read(fd, buf, count)
    #endif
}

private func sysWrite(_ fd: Int32, _ buf: UnsafeRawPointer?, _ count: Int) -> Int {
    #if os(Linux)
    return Glibc.write(fd, buf, count)
    #else
    return Darwin.write(fd, buf, count)
    #endif
}

private func sysClose(_ fd: Int32) -> Int32 {
    #if os(Linux)
    return Glibc.close(fd)
    #else
    return Darwin.close(fd)
    #endif
}

private func sysDup(_ fd: Int32) -> Int32 {
    #if os(Linux)
    return Glibc.dup(fd)
    #else
    return Darwin.dup(fd)
    #endif
}

private func sysFcntl(_ fd: Int32, _ command: Int32, _ value: Int32) -> Int32 {
    #if os(Linux)
    return Glibc.fcntl(fd, command, value)
    #else
    return Darwin.fcntl(fd, command, value)
    #endif
}

private func sysSocketPair(_ domain: Int32, _ type: Int32, _ proto: Int32, _ fds: UnsafeMutablePointer<Int32>?) -> Int32 {
    #if os(Linux)
    return Glibc.socketpair(domain, type, proto, fds)
    #else
    return Darwin.socketpair(domain, type, proto, fds)
    #endif
}

private func sysKill(_ pid: Int32, _ signal: Int32) -> Int32 {
    #if os(Linux)
    return Glibc.kill(pid, signal)
    #else
    return Darwin.kill(pid, signal)
    #endif
}
