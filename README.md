# swift-holons

**Swift SDK for Organic Programming** — transport primitives,
serve-flag parsing, `holon.yaml` parsing, filesystem discovery, and a
Holon-RPC client for Swift holons.

## Package

```swift
.package(path: "../swift-holons")
```

## Features

- Runtime transports: `tcp://`, `unix://`, `stdio://`, `mem://`
- Transport metadata for `ws://` and `wss://`
- Standard CLI flag parsing (`--listen`, `--port`)
- `Identity.parseHolon(_:)`
- `discover(root:)`, `discoverLocal()`, `discoverAll()`
- `findBySlug(_:)`, `findByUUID(_:)`
- `HolonRPCClient.connect/invoke/register/close`

## API

- `Transport.defaultURI`
- `Transport.scheme(_:)`
- `Transport.parse(_:)`
- `Transport.listen(_:)`
- `Transport.listenRuntime(_:)`
- `RuntimeListener.accept()`
- `RuntimeListener.close()`
- `MemRuntimeListener.dial()`
- `Serve.parseFlags(_:)`
- `Identity.parseHolon(_:)`
- `discover(root:)`
- `discoverLocal()`
- `discoverAll()`
- `findBySlug(_:)`
- `findByUUID(_:)`
- `HolonRPCClient`

## Current gaps vs Go

- No generic `connect()` helper yet.
- `ws://` and `wss://` do not provide a runtime gRPC listener.
- The SDK does not yet expose a transport-agnostic gRPC channel factory.

## Test

```bash
swift test
```
