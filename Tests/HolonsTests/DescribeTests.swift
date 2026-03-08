import XCTest
@testable import Holons

final class DescribeTests: XCTestCase {
    func testBuildDescribeResponseParsesEchoProto() throws {
        let root = try writeEchoHolon()
        defer { try? FileManager.default.removeItem(at: root) }

        let response = try buildDescribeResponse(
            protoDir: root.appendingPathComponent("protos").path,
            holonYAMLPath: root.appendingPathComponent("holon.yaml").path
        )

        XCTAssertEqual(response.slug, "echo-server")
        XCTAssertEqual(response.motto, "Reply precisely.")
        XCTAssertEqual(response.services.count, 1)

        let service = try XCTUnwrap(response.services.first)
        XCTAssertEqual(service.name, "echo.v1.Echo")
        XCTAssertEqual(service.description_p, "Echo echoes request payloads for documentation tests.")
        XCTAssertEqual(service.methods.count, 1)

        let method = try XCTUnwrap(service.methods.first)
        XCTAssertEqual(method.name, "Ping")
        XCTAssertEqual(method.inputType, "echo.v1.PingRequest")
        XCTAssertEqual(method.outputType, "echo.v1.PingResponse")
        XCTAssertEqual(method.exampleInput, #"{"message":"hello","sdk":"go-holons"}"#)

        let field = try XCTUnwrap(method.inputFields.first)
        XCTAssertEqual(field.name, "message")
        XCTAssertEqual(field.type, "string")
        XCTAssertEqual(field.number, 1)
        XCTAssertEqual(field.description_p, "Message to echo back.")
        XCTAssertEqual(field.label, .optional)
        XCTAssertTrue(field.required)
        XCTAssertEqual(field.example, #""hello""#)
    }

    func testProviderReturnsDescribeResponse() throws {
        let root = try writeEchoHolon()
        defer { try? FileManager.default.removeItem(at: root) }

        let provider = try HolonMetaDescribeProvider(
            protoDir: root.appendingPathComponent("protos").path,
            holonYAMLPath: root.appendingPathComponent("holon.yaml").path
        )
        let response = provider.describe()

        XCTAssertEqual(response.slug, "echo-server")
        XCTAssertEqual(response.services.count, 1)
        XCTAssertEqual(response.services.first?.name, "echo.v1.Echo")
        XCTAssertEqual(response.services.first?.methods.first?.name, "Ping")
    }

    func testBuildDescribeResponseHandlesMissingProtoDirectory() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("swift_holons_empty_\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        try """
        given_name: Silent
        family_name: Holon
        motto: Quietly available.
        """.write(to: root.appendingPathComponent("holon.yaml"), atomically: true, encoding: .utf8)

        let response = try buildDescribeResponse(
            protoDir: root.appendingPathComponent("protos").path,
            holonYAMLPath: root.appendingPathComponent("holon.yaml").path
        )

        XCTAssertEqual(response.slug, "silent-holon")
        XCTAssertEqual(response.motto, "Quietly available.")
        XCTAssertTrue(response.services.isEmpty)
    }

    private func writeEchoHolon() throws -> URL {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("swift_holons_describe_\(UUID().uuidString)", isDirectory: true)
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

        // Echo echoes request payloads for documentation tests.
        service Echo {
          // Ping echoes the inbound message.
          // @example {"message":"hello","sdk":"go-holons"}
          rpc Ping(PingRequest) returns (PingResponse);
        }

        message PingRequest {
          // Message to echo back.
          // @required
          // @example "hello"
          string message = 1;

          // SDK marker included in the response.
          // @example "go-holons"
          string sdk = 2;
        }

        message PingResponse {
          // Echoed message.
          string message = 1;

          // SDK marker from the server.
          string sdk = 2;
        }
        """.write(to: protoDir.appendingPathComponent("echo.proto"), atomically: true, encoding: .utf8)

        return root
    }
}
