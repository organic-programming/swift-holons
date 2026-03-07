import Foundation

public struct HolonBuild: Equatable {
    public var runner: String = ""
    public var main: String = ""

    public init() {}
}

public struct HolonArtifacts: Equatable {
    public var binary: String = ""
    public var primary: String = ""

    public init() {}
}

public struct HolonManifest: Equatable {
    public var kind: String = ""
    public var build = HolonBuild()
    public var artifacts = HolonArtifacts()

    public init() {}
}

public struct HolonEntry: Equatable {
    public var slug: String
    public var uuid: String
    public var dir: URL
    public var relativePath: String
    public var origin: String
    public var identity: HolonIdentity
    public var manifest: HolonManifest?

    public init(
        slug: String,
        uuid: String,
        dir: URL,
        relativePath: String,
        origin: String,
        identity: HolonIdentity,
        manifest: HolonManifest?
    ) {
        self.slug = slug
        self.uuid = uuid
        self.dir = dir
        self.relativePath = relativePath
        self.origin = origin
        self.identity = identity
        self.manifest = manifest
    }
}

public enum DiscoverError: Error, CustomStringConvertible {
    case ambiguousSlug(String)
    case ambiguousUUID(String)
    case invalidMapping(String)

    public var description: String {
        switch self {
        case let .ambiguousSlug(slug):
            return "ambiguous holon \"\(slug)\""
        case let .ambiguousUUID(prefix):
            return "ambiguous UUID prefix \"\(prefix)\""
        case let .invalidMapping(path):
            return "\(path): holon.yaml must be a YAML mapping"
        }
    }
}

public func discover(root: URL) throws -> [HolonEntry] {
    try discoverInRoot(root, origin: "local")
}

public func discoverLocal() throws -> [HolonEntry] {
    try discover(root: URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true))
}

public func discoverAll() throws -> [HolonEntry] {
    let roots: [(url: URL, origin: String)] = [
        (currentRootURL(), "local"),
        (opbinURL(), "$OPBIN"),
        (cacheDirURL(), "cache"),
    ]

    var seen = Set<String>()
    var entries: [HolonEntry] = []
    for root in roots {
        for entry in try discoverInRoot(root.url, origin: root.origin) {
            let key = entry.uuid.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                ? entry.dir.path
                : entry.uuid
            if seen.insert(key).inserted {
                entries.append(entry)
            }
        }
    }
    return entries
}

public func findBySlug(_ slug: String) throws -> HolonEntry? {
    let needle = slug.trimmingCharacters(in: .whitespacesAndNewlines)
    if needle.isEmpty {
        return nil
    }

    var matched: HolonEntry?
    for entry in try discoverAll() where entry.slug == needle {
        if let matched, matched.uuid != entry.uuid {
            throw DiscoverError.ambiguousSlug(needle)
        }
        matched = entry
    }
    return matched
}

public func findByUUID(_ prefix: String) throws -> HolonEntry? {
    let needle = prefix.trimmingCharacters(in: .whitespacesAndNewlines)
    if needle.isEmpty {
        return nil
    }

    var matched: HolonEntry?
    for entry in try discoverAll() where entry.uuid.hasPrefix(needle) {
        if let matched, matched.uuid != entry.uuid {
            throw DiscoverError.ambiguousUUID(needle)
        }
        matched = entry
    }
    return matched
}

private func discoverInRoot(_ root: URL, origin: String) throws -> [HolonEntry] {
    let root = normalizedDirectoryURL(root)
    var isDirectory: ObjCBool = false
    guard FileManager.default.fileExists(atPath: root.path, isDirectory: &isDirectory), isDirectory.boolValue else {
        return []
    }

    guard let enumerator = FileManager.default.enumerator(
        at: root,
        includingPropertiesForKeys: [.isDirectoryKey],
        options: [],
        errorHandler: { _, _ in true }
    ) else {
        return []
    }

    var entriesByKey: [String: HolonEntry] = [:]
    var orderedKeys: [String] = []

    while let item = enumerator.nextObject() as? URL {
        let item = item.standardizedFileURL
        let values = try? item.resourceValues(forKeys: [.isDirectoryKey])
        let isDirectory = values?.isDirectory ?? false
        let name = item.lastPathComponent

        if isDirectory {
            if shouldSkipDiscoveryDir(root: root, path: item, name: name) {
                enumerator.skipDescendants()
            }
            continue
        }

        if name != "holon.yaml" {
            continue
        }

        do {
            let identity = try Identity.parseHolon(item.path)
            let manifest = try? parseManifest(item.path)
            let dir = item.deletingLastPathComponent().standardizedFileURL
            let entry = HolonEntry(
                slug: identity.slug,
                uuid: identity.uuid,
                dir: dir,
                relativePath: relativePath(root: root, dir: dir),
                origin: origin,
                identity: identity,
                manifest: manifest
            )

            let key = entry.uuid.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                ? entry.dir.path
                : entry.uuid
            if let existing = entriesByKey[key] {
                if pathDepth(entry.relativePath) < pathDepth(existing.relativePath) {
                    entriesByKey[key] = entry
                }
                continue
            }

            entriesByKey[key] = entry
            orderedKeys.append(key)
        } catch {
            continue
        }
    }

    var entries: [HolonEntry] = []
    for key in orderedKeys {
        if let entry = entriesByKey[key] {
            entries.append(entry)
        }
    }
    entries.sort { left, right in
        if left.relativePath == right.relativePath {
            return left.uuid < right.uuid
        }
        return left.relativePath < right.relativePath
    }
    return entries
}

private func parseManifest(_ path: String) throws -> HolonManifest {
    let text = try String(contentsOfFile: path, encoding: .utf8)
    var manifest = HolonManifest()
    var sawMapping = false
    var section = ""

    for rawLine in text.split(separator: "\n", omittingEmptySubsequences: false) {
        let line = String(rawLine)
        let trimmed = line.trimmingCharacters(in: .whitespaces)
        guard !trimmed.isEmpty, !trimmed.hasPrefix("#") else {
            continue
        }
        guard let index = trimmed.firstIndex(of: ":") else {
            continue
        }

        sawMapping = true
        let indent = line.prefix { $0 == " " || $0 == "\t" }.count
        let key = String(trimmed[..<index]).trimmingCharacters(in: .whitespaces)
        let value = sanitizeValue(String(trimmed[trimmed.index(after: index)...]))

        if indent == 0 {
            switch key {
            case "build" where value.isEmpty, "artifacts" where value.isEmpty:
                section = key
            case "kind":
                manifest.kind = value
                section = ""
            default:
                section = ""
            }
            continue
        }

        switch (section, key) {
        case ("build", "runner"):
            manifest.build.runner = value
        case ("build", "main"):
            manifest.build.main = value
        case ("artifacts", "binary"):
            manifest.artifacts.binary = value
        case ("artifacts", "primary"):
            manifest.artifacts.primary = value
        default:
            break
        }
    }

    if !sawMapping {
        throw DiscoverError.invalidMapping(path)
    }

    return manifest
}

private func sanitizeValue(_ raw: String) -> String {
    var value = raw.trimmingCharacters(in: .whitespaces)
    if let comment = value.firstIndex(of: "#"), !value[..<comment].contains("\"") {
        value = String(value[..<comment]).trimmingCharacters(in: .whitespaces)
    }
    if value.hasPrefix("\""), value.hasSuffix("\""), value.count >= 2 {
        value.removeFirst()
        value.removeLast()
    }
    return value
}

private func shouldSkipDiscoveryDir(root: URL, path: URL, name: String) -> Bool {
    if path.path == root.path {
        return false
    }
    if [".git", ".op", "node_modules", "vendor", "build"].contains(name) {
        return true
    }
    return name.hasPrefix(".")
}

private func relativePath(root: URL, dir: URL) -> String {
    let rootPath = root.path
    let dirPath = dir.path
    if dirPath == rootPath {
        return "."
    }

    let prefix = rootPath.hasSuffix("/") ? rootPath : rootPath + "/"
    if dirPath.hasPrefix(prefix) {
        return String(dirPath.dropFirst(prefix.count))
    }
    return dirPath
}

private func pathDepth(_ relativePath: String) -> Int {
    let trimmed = relativePath.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
    if trimmed.isEmpty || trimmed == "." {
        return 0
    }
    return trimmed.split(separator: "/").count
}

private func currentRootURL() -> URL {
    normalizedDirectoryURL(URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true))
}

private func opPathURL() -> URL {
    if let path = ProcessInfo.processInfo.environment["OPPATH"]?.trimmingCharacters(in: .whitespacesAndNewlines),
       !path.isEmpty {
        return normalizedDirectoryURL(URL(fileURLWithPath: path, isDirectory: true))
    }
    if let home = ProcessInfo.processInfo.environment["HOME"]?.trimmingCharacters(in: .whitespacesAndNewlines),
       !home.isEmpty {
        return normalizedDirectoryURL(URL(fileURLWithPath: home, isDirectory: true).appendingPathComponent(".op", isDirectory: true))
    }
    return normalizedDirectoryURL(URL(fileURLWithPath: ".op", isDirectory: true))
}

private func opbinURL() -> URL {
    if let path = ProcessInfo.processInfo.environment["OPBIN"]?.trimmingCharacters(in: .whitespacesAndNewlines),
       !path.isEmpty {
        return normalizedDirectoryURL(URL(fileURLWithPath: path, isDirectory: true))
    }
    return normalizedDirectoryURL(opPathURL().appendingPathComponent("bin", isDirectory: true))
}

private func cacheDirURL() -> URL {
    normalizedDirectoryURL(opPathURL().appendingPathComponent("cache", isDirectory: true))
}

private func normalizedDirectoryURL(_ url: URL) -> URL {
    url.standardizedFileURL
}
