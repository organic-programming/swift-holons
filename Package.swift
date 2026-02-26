// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "swift-holons",
    platforms: [
        .macOS(.v13)
    ],
    products: [
        .library(
            name: "Holons",
            targets: ["Holons"]
        ),
        .executable(
            name: "echo-client",
            targets: ["EchoClient"]
        ),
        .executable(
            name: "echo-server",
            targets: ["EchoServer"]
        ),
        .executable(
            name: "holon-rpc-server",
            targets: ["HolonRPCServer"]
        )
    ],
    targets: [
        .target(
            name: "Holons"
        ),
        .executableTarget(
            name: "EchoClient",
            dependencies: ["Holons"],
            path: "Sources/echo-client"
        ),
        .executableTarget(
            name: "EchoServer",
            dependencies: ["Holons"],
            path: "Sources/echo-server"
        ),
        .executableTarget(
            name: "HolonRPCServer",
            dependencies: ["Holons"],
            path: "Sources/holon-rpc-server"
        ),
        .testTarget(
            name: "HolonsTests",
            dependencies: ["Holons"]
        )
    ]
)
