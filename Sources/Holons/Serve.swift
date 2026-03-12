import Dispatch
import Foundation
import GRPC
import NIOPosix
#if os(Linux)
import Glibc
#else
import Darwin
#endif

public enum Serve {
    public struct Options {
        public var describe: Bool
        public var logger: (String) -> Void
        public var onListen: ((String) -> Void)?
        public var shutdownGracePeriodSeconds: TimeInterval
        public var protoDir: String?
        public var holonYAMLPath: String?

        public init(
            describe: Bool = true,
            logger: @escaping (String) -> Void = { message in
                fputs("\(message)\n", stderr)
            },
            onListen: ((String) -> Void)? = nil,
            shutdownGracePeriodSeconds: TimeInterval = 10,
            protoDir: String? = nil,
            holonYAMLPath: String? = nil
        ) {
            self.describe = describe
            self.logger = logger
            self.onListen = onListen
            self.shutdownGracePeriodSeconds = shutdownGracePeriodSeconds
            self.protoDir = protoDir
            self.holonYAMLPath = holonYAMLPath
        }
    }

    public final class RunningServer {
        private let server: Server
        private let group: MultiThreadedEventLoopGroup
        private let logger: (String) -> Void
        private let defaultGracePeriodSeconds: TimeInterval
        private let stateLock = NSLock()
        private var stopped = false

        public let publicURI: String

        fileprivate init(
            server: Server,
            group: MultiThreadedEventLoopGroup,
            publicURI: String,
            logger: @escaping (String) -> Void,
            defaultGracePeriodSeconds: TimeInterval
        ) {
            self.server = server
            self.group = group
            self.publicURI = publicURI
            self.logger = logger
            self.defaultGracePeriodSeconds = defaultGracePeriodSeconds
        }

        public func await() throws {
            try server.onClose.wait()
        }

        public func stop(gracePeriodSeconds: TimeInterval? = nil) {
            stateLock.lock()
            if stopped {
                stateLock.unlock()
                return
            }
            stopped = true
            stateLock.unlock()

            _ = gracePeriodSeconds ?? defaultGracePeriodSeconds
            do {
                try server.initiateGracefulShutdown().wait()
            } catch {
                logger("graceful stop failed: \(error); forcing hard stop")
                _ = try? server.close().wait()
            }

            try? group.syncShutdownGracefully()
        }
    }

    public static func parseFlags(_ args: [String]) -> String {
        var idx = 0
        while idx < args.count {
            if args[idx] == "--listen", idx + 1 < args.count {
                return args[idx + 1]
            }
            if args[idx] == "--port", idx + 1 < args.count {
                return "tcp://:\(args[idx + 1])"
            }
            idx += 1
        }
        return Transport.defaultURI
    }

    public static func run(
        _ listenURI: String,
        serviceProviders: [CallHandlerProvider]
    ) throws {
        try runWithOptions(listenURI, serviceProviders: serviceProviders)
    }

    public static func runWithOptions(
        _ listenURI: String,
        serviceProviders: [CallHandlerProvider],
        options: Options = Options()
    ) throws {
        let running = try startWithOptions(
            listenURI,
            serviceProviders: serviceProviders,
            options: options
        )

        signal(SIGTERM, SIG_IGN)
        signal(SIGINT, SIG_IGN)

        let queue = DispatchQueue(label: "holons.serve.signal-forwarding")
        let stopServer = {
            options.logger("shutting down gRPC server")
            running.stop(gracePeriodSeconds: options.shutdownGracePeriodSeconds)
        }

        let termSource = DispatchSource.makeSignalSource(signal: SIGTERM, queue: queue)
        termSource.setEventHandler(handler: stopServer)
        termSource.resume()

        let intSource = DispatchSource.makeSignalSource(signal: SIGINT, queue: queue)
        intSource.setEventHandler(handler: stopServer)
        intSource.resume()

        defer {
            termSource.cancel()
            intSource.cancel()
            signal(SIGTERM, SIG_DFL)
            signal(SIGINT, SIG_DFL)
        }

        try running.await()
    }

    public static func startWithOptions(
        _ listenURI: String,
        serviceProviders: [CallHandlerProvider],
        options: Options = Options()
    ) throws -> RunningServer {
        let parsed = try Transport.parse(listenURI.isEmpty ? Transport.defaultURI : listenURI)
        guard parsed.scheme == "tcp" else {
            throw TransportError.runtimeUnsupported(
                uri: listenURI,
                reason: "Serve.run(...) currently supports tcp:// only"
            )
        }

        let host = parsed.host ?? "0.0.0.0"
        let port = parsed.port ?? 9090
        var providers = serviceProviders
        let describeEnabled = try maybeAddDescribe(&providers, options: options)
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)

        do {
            let server = try Server.insecure(group: group)
                .withServiceProviders(providers)
                .bind(host: host, port: port)
                .wait()
            let actualPort = server.channel.localAddress?.port ?? port
            let publicURI = "tcp://\(advertisedHost(host)):\(actualPort)"
            let mode = describeEnabled ? "Describe ON" : "Describe OFF"

            options.onListen?(publicURI)
            options.logger("gRPC server listening on \(publicURI) (\(mode))")

            return RunningServer(
                server: server,
                group: group,
                publicURI: publicURI,
                logger: options.logger,
                defaultGracePeriodSeconds: options.shutdownGracePeriodSeconds
            )
        } catch {
            try? group.syncShutdownGracefully()
            throw error
        }
    }

    private static func maybeAddDescribe(
        _ providers: inout [CallHandlerProvider],
        options: Options
    ) throws -> Bool {
        guard options.describe else {
            return false
        }

        let holonYAMLPath = options.holonYAMLPath ?? "holon.yaml"
        guard FileManager.default.fileExists(atPath: holonYAMLPath) else {
            return false
        }

        providers.append(
            try HolonMetaDescribeProvider(
                protoDir: options.protoDir ?? "protos",
                holonYAMLPath: holonYAMLPath
            )
        )
        return true
    }
}

private func advertisedHost(_ host: String) -> String {
    switch host {
    case "", "0.0.0.0":
        return "127.0.0.1"
    case "::":
        return "::1"
    default:
        return host
    }
}
