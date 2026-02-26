import XCTest
@testable import Holons
#if os(Linux)
import Glibc
#else
import Darwin
#endif

final class HolonsTests: XCTestCase {
    func testSchemeExtraction() {
        XCTAssertEqual(Transport.scheme("tcp://:9090"), "tcp")
        XCTAssertEqual(Transport.scheme("unix:///tmp/x.sock"), "unix")
        XCTAssertEqual(Transport.scheme("stdio://"), "stdio")
        XCTAssertEqual(Transport.scheme("mem://"), "mem")
        XCTAssertEqual(Transport.scheme("ws://localhost:8080"), "ws")
        XCTAssertEqual(Transport.scheme("wss://localhost:8443"), "wss")
    }

    func testTransportParse() throws {
        let tcp = try Transport.parse("tcp://127.0.0.1:9000")
        XCTAssertEqual(tcp.scheme, "tcp")
        XCTAssertEqual(tcp.host, "127.0.0.1")
        XCTAssertEqual(tcp.port, 9000)

        let ws = try Transport.parse("ws://127.0.0.1:8080")
        XCTAssertEqual(ws.path, "/grpc")

        let wss = try Transport.parse("wss://example.com:8443/holon")
        XCTAssertEqual(wss.scheme, "wss")
        XCTAssertEqual(wss.path, "/holon")
    }

    func testListenVariants() throws {
        XCTAssertEqual(try Transport.listen("stdio://"), .stdio)
        XCTAssertEqual(try Transport.listen("mem://test"), .mem(name: "test"))
        XCTAssertEqual(try Transport.listen("unix:///tmp/test.sock"), .unix(path: "/tmp/test.sock"))
        XCTAssertEqual(try Transport.listen("tcp://:9090"), .tcp(host: "0.0.0.0", port: 9090))
    }

    func testParseFlags() {
        XCTAssertEqual(Serve.parseFlags(["--listen", "tcp://:8080"]), "tcp://:8080")
        XCTAssertEqual(Serve.parseFlags(["--port", "3000"]), "tcp://:3000")
        XCTAssertEqual(Serve.parseFlags([]), Transport.defaultURI)
    }

    func testIdentityParse() throws {
        let tmp = FileManager.default.temporaryDirectory
            .appendingPathComponent("holons_test_\(UUID().uuidString).md")
        let content = """
        ---
        uuid: \"abc-123\"
        given_name: \"swift-holon\"
        lang: \"swift\"
        parents: [\"a\", \"b\"]
        aliases: [\"s1\"]
        ---
        # Swift Holon
        """

        try content.write(to: tmp, atomically: true, encoding: .utf8)
        defer { try? FileManager.default.removeItem(at: tmp) }

        let id = try Identity.parseHolon(tmp.path)
        XCTAssertEqual(id.uuid, "abc-123")
        XCTAssertEqual(id.givenName, "swift-holon")
        XCTAssertEqual(id.lang, "swift")
        XCTAssertEqual(id.parents, ["a", "b"])
        XCTAssertEqual(id.aliases, ["s1"])
    }

    func testRuntimeTCPRoundTrip() throws {
        do {
            let runtime = try Transport.listenRuntime("tcp://127.0.0.1:0")
            guard case let .tcp(listener) = runtime else {
                XCTFail("expected tcp runtime listener")
                return
            }
            defer { try? listener.close() }

            let clientFD = try connectTCP(host: "127.0.0.1", port: listener.boundPort)
            let client = POSIXRuntimeConnection(
                readFD: clientFD,
                writeFD: clientFD,
                ownsReadFD: true,
                ownsWriteFD: true
            )
            defer { try? client.close() }

            let server = try listener.accept()
            defer { try? server.close() }

            try client.write(Data("ping".utf8))
            let received = try server.read(maxBytes: 4)
            XCTAssertEqual(String(data: received, encoding: .utf8), "ping")
        } catch {
            if isPermissionDeniedError(error) {
                throw XCTSkip("tcp runtime sockets unavailable in this environment: \(error)")
            }
            throw error
        }
    }

    func testRuntimeUnixRoundTrip() throws {
        let socketPath = FileManager.default.temporaryDirectory
            .appendingPathComponent("holons_test_\(UUID().uuidString).sock")
            .path

        do {
            let runtime = try Transport.listenRuntime("unix://\(socketPath)")
            guard case let .unix(listener) = runtime else {
                XCTFail("expected unix runtime listener")
                return
            }
            defer { try? listener.close() }

            let clientFD = try connectUnix(path: socketPath)
            let client = POSIXRuntimeConnection(
                readFD: clientFD,
                writeFD: clientFD,
                ownsReadFD: true,
                ownsWriteFD: true
            )
            defer { try? client.close() }

            let server = try listener.accept()
            defer { try? server.close() }

            try client.write(Data("unix".utf8))
            let received = try server.read(maxBytes: 4)
            XCTAssertEqual(String(data: received, encoding: .utf8), "unix")
        } catch {
            if isPermissionDeniedError(error) {
                throw XCTSkip("unix runtime sockets unavailable in this environment: \(error)")
            }
            throw error
        }
    }

    func testRuntimeStdioSingleAccept() throws {
        let runtime = try Transport.listenRuntime("stdio://")
        guard case let .stdio(listener) = runtime else {
            XCTFail("expected stdio runtime listener")
            return
        }

        _ = try listener.accept()
        XCTAssertThrowsError(try listener.accept())
        try listener.close()
    }

    func testRuntimeMemRoundTrip() throws {
        let runtime = try Transport.listenRuntime("mem://swift-tests")
        guard case let .mem(listener) = runtime else {
            XCTFail("expected mem runtime listener")
            return
        }
        defer { try? listener.close() }

        let client = try listener.dial()
        defer { try? client.close() }

        let server = try listener.accept()
        defer { try? server.close() }

        try client.write(Data("mem".utf8))
        let received = try server.read(maxBytes: 3)
        XCTAssertEqual(String(data: received, encoding: .utf8), "mem")
    }

    func testRuntimeWebSocketUnsupported() {
        XCTAssertThrowsError(try Transport.listenRuntime("ws://127.0.0.1:8080/grpc")) { error in
            guard case let TransportError.runtimeUnsupported(uri, reason) = error else {
                XCTFail("unexpected error: \(error)")
                return
            }
            XCTAssertEqual(uri, "ws://127.0.0.1:8080/grpc")
            XCTAssertFalse(reason.isEmpty)
        }
    }
}

private enum TestConnectionError: Error {
    case connectFailed(String)
}

private func isPermissionDeniedError(_ error: Error) -> Bool {
    let message = String(describing: error).lowercased()
    return message.contains("operation not permitted")
        || message.contains("permission denied")
}

private func connectTCP(host: String, port: Int) throws -> Int32 {
    var hints = addrinfo(
        ai_flags: 0,
        ai_family: AF_UNSPEC,
        ai_socktype: testSocketStreamType,
        ai_protocol: 0,
        ai_addrlen: 0,
        ai_canonname: nil,
        ai_addr: nil,
        ai_next: nil
    )

    let hostCString = strdup(host)
    let portCString = strdup(String(port))
    defer {
        if let hostCString {
            free(hostCString)
        }
        if let portCString {
            free(portCString)
        }
    }

    var infos: UnsafeMutablePointer<addrinfo>?
    let gai = getaddrinfo(hostCString, portCString, &hints, &infos)
    guard gai == 0 else {
        throw TestConnectionError.connectFailed(String(cString: gai_strerror(gai)))
    }
    defer {
        if let infos {
            freeaddrinfo(infos)
        }
    }

    var current = infos
    var lastError = "unable to connect"
    while let infoPtr = current {
        let info = infoPtr.pointee
        let fd = testSocket(info.ai_family, info.ai_socktype, info.ai_protocol)
        if fd < 0 {
            lastError = testErrno()
            current = info.ai_next
            continue
        }

        if testConnect(fd, info.ai_addr, info.ai_addrlen) == 0 {
            return fd
        }

        lastError = testErrno()
        _ = testClose(fd)
        current = info.ai_next
    }

    throw TestConnectionError.connectFailed(lastError)
}

private func connectUnix(path: String) throws -> Int32 {
    let fd = testSocket(AF_UNIX, testSocketStreamType, 0)
    if fd < 0 {
        throw TestConnectionError.connectFailed(testErrno())
    }

    var addr = sockaddr_un()
    addr.sun_family = sa_family_t(AF_UNIX)

    let maxPathLength = MemoryLayout.size(ofValue: addr.sun_path)
    if path.utf8.count >= maxPathLength {
        _ = testClose(fd)
        throw TestConnectionError.connectFailed("unix path too long")
    }

    _ = path.withCString { cString in
        withUnsafeMutablePointer(to: &addr.sun_path) { ptr in
            ptr.withMemoryRebound(to: CChar.self, capacity: maxPathLength) { dest in
                strncpy(dest, cString, maxPathLength - 1)
            }
        }
    }

    let connectResult = withUnsafePointer(to: &addr) { ptr in
        ptr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sockaddrPtr in
            testConnect(fd, sockaddrPtr, socklen_t(MemoryLayout<sockaddr_un>.size))
        }
    }
    if connectResult != 0 {
        let message = testErrno()
        _ = testClose(fd)
        throw TestConnectionError.connectFailed(message)
    }

    return fd
}

private var testSocketStreamType: Int32 {
    #if os(Linux)
    return Int32(SOCK_STREAM.rawValue)
    #else
    return SOCK_STREAM
    #endif
}

private func testErrno() -> String {
    String(cString: strerror(errno))
}

private func testSocket(_ domain: Int32, _ type: Int32, _ proto: Int32) -> Int32 {
    #if os(Linux)
    return Glibc.socket(domain, type, proto)
    #else
    return Darwin.socket(domain, type, proto)
    #endif
}

private func testConnect(_ fd: Int32, _ addr: UnsafePointer<sockaddr>?, _ len: socklen_t) -> Int32 {
    #if os(Linux)
    return Glibc.connect(fd, addr, len)
    #else
    return Darwin.connect(fd, addr, len)
    #endif
}

private func testClose(_ fd: Int32) -> Int32 {
    #if os(Linux)
    return Glibc.close(fd)
    #else
    return Darwin.close(fd)
    #endif
}
