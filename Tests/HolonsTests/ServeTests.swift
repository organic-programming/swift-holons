import GRPC
import NIOCore
import NIOPosix
import SwiftProtobuf
import XCTest
@testable import Holons

final class ServeTests: XCTestCase {
    func testRetryableBridgeErrnosCoverTransientReadWriteFailures() {
        XCTAssertTrue(isRetryableBridgeErrno(EINTR))
        XCTAssertTrue(isRetryableBridgeErrno(EAGAIN))
        XCTAssertTrue(isRetryableBridgeErrno(EWOULDBLOCK))
        XCTAssertFalse(isRetryableBridgeErrno(EBADF))
    }

    func testStartWithOptionsRegistersDescribeService() throws {
        let root = try writeEchoHolon()
        defer { try? FileManager.default.removeItem(at: root) }

        let running = try Serve.startWithOptions(
            "tcp://127.0.0.1:0",
            serviceProviders: [],
            options: Serve.Options(
                logger: { _ in },
                protoDir: root.appendingPathComponent("protos").path,
                holonYAMLPath: root.appendingPathComponent("holon.yaml").path
            )
        )

        let parsed = try Transport.parse(running.publicURI)
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)

        let channel = ClientConnection.insecure(group: group)
            .connect(host: parsed.host ?? "127.0.0.1", port: parsed.port ?? 0)
        defer {
            _ = try? channel.close().wait()
            running.stop()
            try? group.syncShutdownGracefully()
        }

        let call = channel.makeUnaryCall(
            path: "/holonmeta.v1.HolonMeta/Describe",
            request: TestProtobufPayload(message: Holonmeta_V1_DescribeRequest()),
            callOptions: CallOptions()
        ) as UnaryCall<
            TestProtobufPayload<Holonmeta_V1_DescribeRequest>,
            TestProtobufPayload<Holonmeta_V1_DescribeResponse>
        >
        let response = try call.response.wait().message

        XCTAssertEqual(response.slug, "echo-server")
        XCTAssertEqual(response.services.count, 1)
        XCTAssertEqual(response.services.first?.name, "echo.v1.Echo")
    }

    func testStartWithOptionsRegistersDescribeServiceOverUnix() throws {
        let root = try writeEchoHolon()
        defer { try? FileManager.default.removeItem(at: root) }

        let socketPath = root.appendingPathComponent("serve.sock").path
        let running = try Serve.startWithOptions(
            "unix://\(socketPath)",
            serviceProviders: [],
            options: Serve.Options(
                logger: { _ in },
                protoDir: root.appendingPathComponent("protos").path,
                holonYAMLPath: root.appendingPathComponent("holon.yaml").path
            )
        )

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        var configuration = ClientConnection.Configuration.default(
            target: .unixDomainSocket(socketPath),
            eventLoopGroup: group
        )
        configuration.connectionBackoff = nil
        let channel = ClientConnection(configuration: configuration)

        defer {
            _ = try? channel.close().wait()
            running.stop()
            try? group.syncShutdownGracefully()
        }

        let call = channel.makeUnaryCall(
            path: "/holonmeta.v1.HolonMeta/Describe",
            request: TestProtobufPayload(message: Holonmeta_V1_DescribeRequest()),
            callOptions: CallOptions()
        ) as UnaryCall<
            TestProtobufPayload<Holonmeta_V1_DescribeRequest>,
            TestProtobufPayload<Holonmeta_V1_DescribeResponse>
        >
        let response = try call.response.wait().message

        XCTAssertEqual(running.publicURI, "unix://\(socketPath)")
        XCTAssertEqual(response.slug, "echo-server")
        XCTAssertEqual(response.services.count, 1)
        XCTAssertEqual(response.services.first?.name, "echo.v1.Echo")
    }

    private func writeEchoHolon() throws -> URL {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("shs_\(UUID().uuidString.prefix(8))", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let protoDir = root.appendingPathComponent("protos/echo/v1", isDirectory: true)
        try FileManager.default.createDirectory(at: protoDir, withIntermediateDirectories: true)

        try """
        given_name: Echo
        family_name: Server
        motto: Reply precisely.
        """.write(to: root.appendingPathComponent("holon.yaml"), atomically: true, encoding: .utf8)

        try """
        syntax = "proto3";
        package echo.v1;

        service Echo {
          rpc Ping(PingRequest) returns (PingResponse);
        }

        message PingRequest {
          string message = 1;
        }

        message PingResponse {
          string message = 1;
        }
        """.write(to: protoDir.appendingPathComponent("echo.proto"), atomically: true, encoding: .utf8)

        return root
    }
}

private struct TestProtobufPayload<MessageType: SwiftProtobuf.Message & Sendable>: GRPCPayload, Sendable {
    let message: MessageType

    init(message: MessageType) {
        self.message = message
    }

    init(serializedByteBuffer: inout ByteBuffer) throws {
        let data = serializedByteBuffer.readData(length: serializedByteBuffer.readableBytes) ?? Data()
        self.message = try MessageType(serializedBytes: data)
    }

    func serialize(into buffer: inout ByteBuffer) throws {
        let bytes: [UInt8] = try message.serializedBytes()
        buffer.writeBytes(bytes)
    }
}
