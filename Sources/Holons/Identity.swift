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
}

public enum IdentityError: Error, CustomStringConvertible {
    case missingFrontmatter(String)
    case unterminatedFrontmatter(String)

    public var description: String {
        switch self {
        case let .missingFrontmatter(path):
            return "\(path): missing YAML frontmatter"
        case let .unterminatedFrontmatter(path):
            return "\(path): unterminated YAML frontmatter"
        }
    }
}

public enum Identity {
    public static func parseHolon(_ path: String) throws -> HolonIdentity {
        let text = try String(contentsOfFile: path, encoding: .utf8)

        guard text.hasPrefix("---") else {
            throw IdentityError.missingFrontmatter(path)
        }

        guard let end = text.range(of: "---", options: [], range: text.index(text.startIndex, offsetBy: 3)..<text.endIndex) else {
            throw IdentityError.unterminatedFrontmatter(path)
        }

        let frontmatter = text[text.index(text.startIndex, offsetBy: 3)..<end.lowerBound]
        return parseFrontmatter(String(frontmatter))
    }

    private static func parseFrontmatter(_ content: String) -> HolonIdentity {
        var identity = HolonIdentity()

        for rawLine in content.split(separator: "\n") {
            let line = rawLine.trimmingCharacters(in: .whitespaces)
            guard !line.isEmpty, !line.hasPrefix("#") else { continue }
            guard let idx = line.firstIndex(of: ":") else { continue }

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
