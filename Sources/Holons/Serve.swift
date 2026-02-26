import Foundation

public enum Serve {
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
}
