import Foundation

public struct HolonIdentity: Equatable {
    public var uuid: String = ""
    public var givenName: String = ""
    public var familyName: String = ""
    public var motto: String = ""
    public var composer: String = ""
    public var clade: String = ""
    public var status: String = ""
    public var born: String = ""
    public var lang: String = ""
    public var parents: [String] = []
    public var reproduction: String = ""
    public var generatedBy: String = ""
    public var protoStatus: String = ""
    public var aliases: [String] = []

    public init() {}

    public var slug: String {
        let given = givenName.trimmingCharacters(in: .whitespacesAndNewlines)
        let family = familyName
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .replacingOccurrences(of: #"\?$"#, with: "", options: .regularExpression)
        if given.isEmpty && family.isEmpty {
            return ""
        }

        let joined = "\(given)-\(family)"
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
            .replacingOccurrences(of: " ", with: "-")
        return joined.trimmingCharacters(in: CharacterSet(charactersIn: "-"))
    }
}

public enum IdentityError: Error, CustomStringConvertible {
    case invalidMapping(String)

    public var description: String {
        switch self {
        case let .invalidMapping(path):
            return "\(path): holon.yaml must be a YAML mapping"
        }
    }
}

public enum Identity {
    public static func parseHolon(_ path: String) throws -> HolonIdentity {
        let text = try String(contentsOfFile: path, encoding: .utf8)
        return try parseYAML(text, path: path)
    }

    private static func parseYAML(_ content: String, path: String) throws -> HolonIdentity {
        var identity = HolonIdentity()
        var sawMapping = false

        for rawLine in content.split(separator: "\n") {
            let line = rawLine.trimmingCharacters(in: .whitespaces)
            guard !line.isEmpty, !line.hasPrefix("#") else { continue }
            guard let idx = line.firstIndex(of: ":") else { continue }
            sawMapping = true

            let key = String(line[..<idx]).trimmingCharacters(in: .whitespaces)
            var value = String(line[line.index(after: idx)...]).trimmingCharacters(in: .whitespaces)

            if value.hasPrefix("\"") && value.hasSuffix("\"") && value.count >= 2 {
                value.removeFirst()
                value.removeLast()
            }

            switch key {
            case "uuid": identity.uuid = value
            case "given_name": identity.givenName = value
            case "family_name": identity.familyName = value
            case "motto": identity.motto = value
            case "composer": identity.composer = value
            case "clade": identity.clade = value
            case "status": identity.status = value
            case "born": identity.born = value
            case "lang": identity.lang = value
            case "reproduction": identity.reproduction = value
            case "generated_by": identity.generatedBy = value
            case "proto_status": identity.protoStatus = value
            case "parents": identity.parents = parseList(value)
            case "aliases": identity.aliases = parseList(value)
            default: break
            }
        }

        guard sawMapping else {
            throw IdentityError.invalidMapping(path)
        }
        return identity
    }

    private static func parseList(_ value: String) -> [String] {
        let trimmed = value.trimmingCharacters(in: .whitespaces)
        guard trimmed.hasPrefix("[") && trimmed.hasSuffix("]") else {
            return []
        }

        let inner = trimmed.dropFirst().dropLast()
        if inner.trimmingCharacters(in: .whitespaces).isEmpty {
            return []
        }

        return inner
            .split(separator: ",")
            .map { item in
                var s = item.trimmingCharacters(in: .whitespaces)
                if s.hasPrefix("\"") && s.hasSuffix("\"") && s.count >= 2 {
                    s.removeFirst()
                    s.removeLast()
                }
                return s
            }
    }
}
