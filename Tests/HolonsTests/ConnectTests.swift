import Foundation
import GRPC
import NIOCore
import XCTest
@testable import Holons

final class ConnectTests: XCTestCase {
    func testConnectDialsDirectTarget() throws {
        let server = try startConnectHelperServer(slug: "direct-connect", listen: "tcp://127.0.0.1:0")
        defer { server.stop() }

        let channel = try connect(server.uri)
        defer { try? disconnect(channel) }

        let slug = try describeSlug(channel, timeout: 2.0)
        XCTAssertEqual(slug, "direct-connect")
    }

    func testConnectStartsSlugEphemerallyAndStopsOnDisconnect() throws {
        let sandbox = try makeSandbox(prefix: "connect-slug")
        defer { try? FileManager.default.removeItem(at: sandbox.root) }

        let fixture = try sandbox.makeHolonFixture(slug: "connect-ephemeral")
        let previousDirectory = FileManager.default.currentDirectoryPath
        defer {
            XCTAssertTrue(FileManager.default.changeCurrentDirectoryPath(previousDirectory))
        }
        XCTAssertTrue(FileManager.default.changeCurrentDirectoryPath(sandbox.root.path))

        let channel = try connect(fixture.slug)
        let pid = try waitForPID(at: fixture.pidFile)
        let slug = try describeSlug(channel, timeout: 2.0)
        XCTAssertEqual(slug, fixture.slug)

        try disconnect(channel)

        try waitForProcessExit(pid)
        XCTAssertFalse(FileManager.default.fileExists(atPath: fixture.portFile.path))
    }

    func testConnectStartsSlugFromHolonDirectory() throws {
        let sandbox = try makeSandbox(prefix: "connect-cwd")
        defer { try? FileManager.default.removeItem(at: sandbox.root) }

        let fixture = try sandbox.makeHolonFixture(slug: "connect-cwd")
        let previousDirectory = FileManager.default.currentDirectoryPath
        defer {
            XCTAssertTrue(FileManager.default.changeCurrentDirectoryPath(previousDirectory))
        }
        XCTAssertTrue(FileManager.default.changeCurrentDirectoryPath(sandbox.root.path))

        let channel = try connect(fixture.slug)
        defer { try? disconnect(channel) }

        _ = try waitForPID(at: fixture.pidFile)
        let childDirectory = URL(
            fileURLWithPath: try waitForFileContents(at: fixture.cwdFile)
                .trimmingCharacters(in: .whitespacesAndNewlines),
            isDirectory: true
        ).resolvingSymlinksInPath().path
        XCTAssertEqual(childDirectory, fixture.holonDir.resolvingSymlinksInPath().path)
    }

    func testConnectWithTCPOptionsWritesPortFileAndReusesServer() throws {
        let sandbox = try makeSandbox(prefix: "connect-port")
        defer { try? FileManager.default.removeItem(at: sandbox.root) }

        let fixture = try sandbox.makeHolonFixture(slug: "connect-persistent")
        let previousDirectory = FileManager.default.currentDirectoryPath
        defer {
            XCTAssertTrue(FileManager.default.changeCurrentDirectoryPath(previousDirectory))
        }
        XCTAssertTrue(FileManager.default.changeCurrentDirectoryPath(sandbox.root.path))

        let initial = try connect(
            fixture.slug,
            options: ConnectOptions(timeout: 5.0, transport: "tcp", start: true)
        )
        let pid = try waitForPID(at: fixture.pidFile)
        XCTAssertEqual(try describeSlug(initial, timeout: 2.0), fixture.slug)
        try disconnect(initial)

        XCTAssertTrue(pidExists(pid))

        let portTarget = try String(contentsOf: fixture.portFile, encoding: .utf8)
            .trimmingCharacters(in: .whitespacesAndNewlines)
        XCTAssertTrue(portTarget.hasPrefix("tcp://127.0.0.1:"))

        let reused = try connect(fixture.slug)
        XCTAssertEqual(try describeSlug(reused, timeout: 2.0), fixture.slug)
        try disconnect(reused)

        XCTAssertTrue(pidExists(pid))
        terminateProcess(pid)
        try waitForProcessExit(pid)
    }

    func testConnectRemovesStalePortFileAndStartsFresh() throws {
        let sandbox = try makeSandbox(prefix: "connect-stale")
        defer { try? FileManager.default.removeItem(at: sandbox.root) }

        let fixture = try sandbox.makeHolonFixture(slug: "connect-stale")
        let previousDirectory = FileManager.default.currentDirectoryPath
        defer {
            XCTAssertTrue(FileManager.default.changeCurrentDirectoryPath(previousDirectory))
        }
        XCTAssertTrue(FileManager.default.changeCurrentDirectoryPath(sandbox.root.path))

        let stalePort = try reserveLoopbackPort()
        try FileManager.default.createDirectory(at: fixture.portFile.deletingLastPathComponent(), withIntermediateDirectories: true)
        try "tcp://127.0.0.1:\(stalePort)\n".write(to: fixture.portFile, atomically: true, encoding: .utf8)

        let channel = try connect(fixture.slug)
        let pid = try waitForPID(at: fixture.pidFile)
        XCTAssertEqual(try describeSlug(channel, timeout: 2.0), fixture.slug)
        XCTAssertFalse(FileManager.default.fileExists(atPath: fixture.portFile.path))

        try disconnect(channel)
        try waitForProcessExit(pid)
    }
}

private struct ConnectSandbox {
    let root: URL
    let goBinary: String
    let helperSource: URL
    let goModuleRoot: URL
    let helperExecutable: URL

    struct Fixture {
        let slug: String
        let pidFile: URL
        let portFile: URL
        let cwdFile: URL
        let holonDir: URL
    }

    func makeHolonFixture(slug: String) throws -> Fixture {
        let holonDir = root
            .appendingPathComponent("holons")
            .appendingPathComponent(slug)
        let binaryDir = holonDir
            .appendingPathComponent(".op")
            .appendingPathComponent("build")
            .appendingPathComponent("bin")
        try FileManager.default.createDirectory(at: binaryDir, withIntermediateDirectories: true)

        let pidFile = root.appendingPathComponent("\(slug).pid")
        let cwdFile = root.appendingPathComponent("\(slug).cwd")
        let wrapper = binaryDir.appendingPathComponent("holon-helper")
        let script = """
        #!/bin/sh
        printf '%s\n' "$$" > \(shellQuote(pidFile.path))
        pwd > \(shellQuote(cwdFile.path))
        exec \(shellQuote(helperExecutable.path)) --slug \(shellQuote(slug)) "$@"
        """
        try script.write(to: wrapper, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: wrapper.path)

        let manifest = """
        uuid: "\(slug)-uuid"
        given_name: "\(slug)"
        family_name: ""
        composer: "connect-tests"
        kind: service
        build:
          runner: go
          main: Tests/HolonsTests/Fixtures/connect-helper-go/main.go
        artifacts:
          binary: "holon-helper"
        """
        try manifest.write(to: holonDir.appendingPathComponent("holon.yaml"), atomically: true, encoding: .utf8)

        return Fixture(
            slug: slug,
            pidFile: pidFile,
            portFile: root
                .appendingPathComponent(".op")
                .appendingPathComponent("run")
                .appendingPathComponent("\(slug).port"),
            cwdFile: cwdFile,
            holonDir: holonDir
        )
    }
}

private struct RunningConnectHelperServer {
    let process: Process
    let stdout: Pipe
    let stderr: Pipe
    let uri: String

    func stop() {
        if process.isRunning {
            process.terminate()
            let deadline = Date().addingTimeInterval(2.0)
            while process.isRunning && Date() < deadline {
                Thread.sleep(forTimeInterval: 0.05)
            }
            if process.isRunning {
                process.interrupt()
            }
        }
    }
}

private func makeSandbox(prefix: String) throws -> ConnectSandbox {
    let root = FileManager.default.temporaryDirectory
        .appendingPathComponent("\(prefix)-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)

    let packageRoot = URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent()
        .deletingLastPathComponent()
        .deletingLastPathComponent()
    let goBinary = try resolveGoBinary()
    let helperExecutable = root.appendingPathComponent("connect-helper")

    let build = Process()
    build.executableURL = URL(fileURLWithPath: goBinary)
    build.arguments = [
        "build",
        "-o",
        helperExecutable.path,
        packageRoot
            .appendingPathComponent("Tests")
            .appendingPathComponent("HolonsTests")
            .appendingPathComponent("Fixtures")
            .appendingPathComponent("connect-helper-go")
            .appendingPathComponent("main.go")
            .path,
    ]
    build.currentDirectoryURL = packageRoot
        .deletingLastPathComponent()
        .appendingPathComponent("go-holons")
    let buildStdout = Pipe()
    let buildStderr = Pipe()
    build.standardOutput = buildStdout
    build.standardError = buildStderr
    try build.run()
    build.waitUntilExit()
    guard build.terminationStatus == 0 else {
        let stderr = String(data: buildStderr.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
        throw XCTSkip("failed to build connect helper: \(stderr)")
    }

    return ConnectSandbox(
        root: root,
        goBinary: goBinary,
        helperSource: packageRoot
            .appendingPathComponent("Tests")
            .appendingPathComponent("HolonsTests")
            .appendingPathComponent("Fixtures")
            .appendingPathComponent("connect-helper-go")
            .appendingPathComponent("main.go"),
        goModuleRoot: packageRoot
            .deletingLastPathComponent()
            .appendingPathComponent("go-holons"),
        helperExecutable: helperExecutable
    )
}

private func startConnectHelperServer(slug: String, listen: String) throws -> RunningConnectHelperServer {
    let sandbox = try makeSandbox(prefix: "connect-direct")
    let process = Process()
    process.executableURL = sandbox.helperExecutable
    process.arguments = [
        "--slug",
        slug,
        "--listen",
        listen,
    ]

    let stdout = Pipe()
    let stderr = Pipe()
    process.standardOutput = stdout
    process.standardError = stderr

    try process.run()

    let uri = try readStartupLine(
        from: stdout.fileHandleForReading,
        stderr: stderr.fileHandleForReading,
        process: process,
        timeout: 5.0
    )

    return RunningConnectHelperServer(process: process, stdout: stdout, stderr: stderr, uri: uri)
}

private func readStartupLine(
    from stdout: FileHandle,
    stderr: FileHandle,
    process: Process,
    timeout: TimeInterval
) throws -> String {
    let deadline = Date().addingTimeInterval(timeout)
    var stdoutBuffer = Data()
    var stderrBuffer = Data()

    while Date() < deadline {
        if process.isRunning == false {
            let stderrText = String(data: stderr.availableData + stderrBuffer, encoding: .utf8) ?? ""
            throw XCTSkip("connect helper exited before startup: \(stderrText)")
        }

        let chunk = stdout.availableData
        if !chunk.isEmpty {
            stdoutBuffer.append(chunk)
            if let newline = stdoutBuffer.firstIndex(of: 0x0A) {
                let lineData = stdoutBuffer.prefix(upTo: newline)
                if let line = String(data: lineData, encoding: .utf8), !line.isEmpty {
                    return line
                }
            }
        }

        let stderrChunk = stderr.availableData
        if !stderrChunk.isEmpty {
            stderrBuffer.append(stderrChunk)
        }

        Thread.sleep(forTimeInterval: 0.05)
    }

    let stderrText = String(data: stderrBuffer, encoding: .utf8) ?? ""
    throw XCTSkip("timed out waiting for connect helper startup: \(stderrText)")
}

private func resolveGoBinary() throws -> String {
    let environment = ProcessInfo.processInfo.environment
    if let configured = environment["GO_BIN"]?.trimmingCharacters(in: .whitespacesAndNewlines),
       !configured.isEmpty {
        return configured
    }

    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
    process.arguments = ["which", "go"]
    let stdout = Pipe()
    process.standardOutput = stdout
    process.standardError = Pipe()

    do {
        try process.run()
    } catch {
        throw XCTSkip("go is required for connect tests")
    }

    process.waitUntilExit()
    guard process.terminationStatus == 0,
          let path = String(data: stdout.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8)?
            .trimmingCharacters(in: .whitespacesAndNewlines),
          !path.isEmpty else {
        throw XCTSkip("go is required for connect tests")
    }
    return path
}

private func describeSlug(_ channel: GRPCChannel, timeout: TimeInterval) throws -> String {
    let call = channel.makeUnaryCall(
        path: "/holonmeta.v1.HolonMeta/Describe",
        request: ConnectTestRawBytesPayload(),
        callOptions: CallOptions(
            timeLimit: .timeout(.nanoseconds(Int64(timeout * 1_000_000_000)))
        )
    ) as UnaryCall<ConnectTestRawBytesPayload, ConnectTestRawBytesPayload>
    let response = try call.response.wait()
    return try parseTopLevelStringField(response.data, fieldNumber: 1)
}

private struct ConnectTestRawBytesPayload: GRPCPayload {
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

private func parseTopLevelStringField(_ data: Data, fieldNumber: UInt64) throws -> String {
    var index = data.startIndex

    while index < data.endIndex {
        let key = try decodeVarint(data, index: &index)
        let wireType = key & 0x07
        let number = key >> 3

        if wireType == 2 {
            let length = try decodeVarint(data, index: &index)
            guard let end = data.index(index, offsetBy: Int(length), limitedBy: data.endIndex) else {
                throw ConnectError.ioFailure("invalid protobuf payload")
            }
            let slice = data[index..<end]
            if number == fieldNumber, let value = String(data: slice, encoding: .utf8) {
                return value
            }
            index = end
            continue
        }

        try skipField(wireType: wireType, data: data, index: &index)
    }

    throw ConnectError.ioFailure("missing field \(fieldNumber)")
}

private func decodeVarint(_ data: Data, index: inout Data.Index) throws -> UInt64 {
    var value: UInt64 = 0
    var shift: UInt64 = 0

    while index < data.endIndex {
        let byte = UInt64(data[index])
        data.formIndex(after: &index)

        value |= (byte & 0x7f) << shift
        if byte & 0x80 == 0 {
            return value
        }

        shift += 7
        if shift >= 64 {
            break
        }
    }

    throw ConnectError.ioFailure("invalid varint")
}

private func skipField(wireType: UInt64, data: Data, index: inout Data.Index) throws {
    switch wireType {
    case 0:
        _ = try decodeVarint(data, index: &index)
    case 1:
        guard let end = data.index(index, offsetBy: 8, limitedBy: data.endIndex) else {
            throw ConnectError.ioFailure("invalid fixed64 field")
        }
        index = end
    case 2:
        let length = try decodeVarint(data, index: &index)
        guard let end = data.index(index, offsetBy: Int(length), limitedBy: data.endIndex) else {
            throw ConnectError.ioFailure("invalid length-delimited field")
        }
        index = end
    case 5:
        guard let end = data.index(index, offsetBy: 4, limitedBy: data.endIndex) else {
            throw ConnectError.ioFailure("invalid fixed32 field")
        }
        index = end
    default:
        throw ConnectError.ioFailure("unsupported wire type \(wireType)")
    }
}

private func waitForPID(at path: URL, timeout: TimeInterval = 5.0) throws -> Int32 {
    let deadline = Date().addingTimeInterval(timeout)
    while Date() < deadline {
        if let raw = try? String(contentsOf: path, encoding: .utf8).trimmingCharacters(in: .whitespacesAndNewlines),
           let pid = Int32(raw),
           pid > 0 {
            return pid
        }
        Thread.sleep(forTimeInterval: 0.025)
    }
    throw ConnectError.ioFailure("timed out waiting for pid file \(path.path)")
}

private func waitForFileContents(at path: URL, timeout: TimeInterval = 5.0) throws -> String {
    let deadline = Date().addingTimeInterval(timeout)
    while Date() < deadline {
        if let raw = try? String(contentsOf: path, encoding: .utf8),
           !raw.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return raw
        }
        Thread.sleep(forTimeInterval: 0.025)
    }
    throw ConnectError.ioFailure("timed out waiting for file \(path.path)")
}

private func pidExists(_ pid: Int32) -> Bool {
    kill(pid, 0) == 0
}

private func waitForProcessExit(_ pid: Int32, timeout: TimeInterval = 2.0) throws {
    let deadline = Date().addingTimeInterval(timeout)
    while Date() < deadline {
        if !pidExists(pid) {
            return
        }
        Thread.sleep(forTimeInterval: 0.025)
    }
    throw ConnectError.ioFailure("process \(pid) did not exit")
}

private func terminateProcess(_ pid: Int32) {
    guard pidExists(pid) else {
        return
    }
    _ = kill(pid, SIGTERM)
    let deadline = Date().addingTimeInterval(2.0)
    while pidExists(pid) && Date() < deadline {
        Thread.sleep(forTimeInterval: 0.025)
    }
    if pidExists(pid) {
        _ = kill(pid, SIGKILL)
    }
}

private func reserveLoopbackPort() throws -> Int {
    let listener = try TCPRuntimeListener(host: "127.0.0.1", port: 0)
    defer { try? listener.close() }
    return listener.boundPort
}

private func shellQuote(_ value: String) -> String {
    "'" + value.replacingOccurrences(of: "'", with: "'\"'\"'") + "'"
}
