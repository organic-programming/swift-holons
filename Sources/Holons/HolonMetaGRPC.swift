import GRPC
import NIOCore

public protocol Holonmeta_V1_HolonMetaProvider: CallHandlerProvider {
    var interceptors: Holonmeta_V1_HolonMetaServerInterceptorFactoryProtocol? { get }

    func describe(
        request: Holonmeta_V1_DescribeRequest,
        context: StatusOnlyCallContext
    ) -> EventLoopFuture<Holonmeta_V1_DescribeResponse>
}

extension Holonmeta_V1_HolonMetaProvider {
    public var serviceName: Substring {
        Holonmeta_V1_HolonMetaServerMetadata.serviceDescriptor.fullName[...]
    }

    public var interceptors: Holonmeta_V1_HolonMetaServerInterceptorFactoryProtocol? {
        nil
    }

    public func handle(
        method name: Substring,
        context: CallHandlerContext
    ) -> GRPCServerHandlerProtocol? {
        switch name {
        case "Describe":
            return UnaryServerHandler(
                context: context,
                requestDeserializer: ProtobufDeserializer<Holonmeta_V1_DescribeRequest>(),
                responseSerializer: ProtobufSerializer<Holonmeta_V1_DescribeResponse>(),
                interceptors: self.interceptors?.makeDescribeInterceptors() ?? [],
                userFunction: self.describe(request:context:)
            )
        default:
            return nil
        }
    }
}

public protocol Holonmeta_V1_HolonMetaServerInterceptorFactoryProtocol {
    func makeDescribeInterceptors() -> [ServerInterceptor<Holonmeta_V1_DescribeRequest, Holonmeta_V1_DescribeResponse>]
}

public enum Holonmeta_V1_HolonMetaServerMetadata {
    public static let serviceDescriptor = GRPCServiceDescriptor(
        name: "HolonMeta",
        fullName: "holonmeta.v1.HolonMeta",
        methods: [
            Methods.describe,
        ]
    )

    public enum Methods {
        public static let describe = GRPCMethodDescriptor(
            name: "Describe",
            path: "/holonmeta.v1.HolonMeta/Describe",
            type: .unary
        )
    }
}
