---
# Cartouche v1
title: "swift-holons — Swift SDK for Organic Programming"
author:
  name: "B. ALTER"
created: 2026-02-12
revised: 2026-02-13
lang: en-US
access:
  humans: true
  agents: false
status: draft
---
# swift-holons

**Swift SDK for Organic Programming** — transport URI parsing, serve flag parsing,
HOLON.md identity parsing, and Holon-RPC client support.

## Features

- Transport URI surface:
  - `tcp://`
  - `unix://`
  - `stdio://`
  - `mem://`
  - `ws://`
  - `wss://`
- Native runtime listeners:
  - `tcp://` (socket bind + accept)
  - `unix://` (domain socket bind + accept)
  - `stdio://` (single accepted stdio connection)
  - `mem://` (in-process full-duplex dial/accept pair)
- Standard CLI flag parsing (`--listen`, `--port`)
- HOLON.md frontmatter parser
- Holon-RPC client (`holon-rpc` subprotocol, JSON-RPC 2.0, heartbeat, reconnect)

## Package

```swift
.package(path: "../swift-holons")
```

## API

- `Transport.defaultURI`
- `Transport.scheme(_:)`
- `Transport.parse(_:)`
- `Transport.listen(_:)`
- `Transport.listenRuntime(_:)`
- `RuntimeListener.accept()`
- `RuntimeListener.close()`
- `MemRuntimeListener.dial()`
- `HolonRPCClient.connect/invoke/register/close`
- `Serve.parseFlags(_:)`
- `Identity.parseHolon(_:)`

## Parity Notes vs Go Reference

Implemented parity:

- URI parsing and listener dispatch semantics
- Runtime transport primitives for `tcp`, `unix`, `stdio`, `mem`
- Holon-RPC client protocol support over `ws://` / `wss://`
- Standard serve flag parsing
- HOLON identity parsing

Not yet achievable with the current Swift stack (justified gaps):

- `ws://` / `wss://` runtime listener parity:
  - The Go SDK uses `net.Listener` over upgraded WebSocket streams for gRPC.
  - `grpc-swift` does not provide an official WebSocket server transport for HTTP/2 gRPC framing.
  - `Transport.listenRuntime(_:)` therefore throws `TransportError.runtimeUnsupported` for `ws/wss`.
- Transport-agnostic gRPC client helpers (`Dial`, `DialStdio`, `DialMem`, `DialWebSocket`):
  - Go can expose a generic `grpc.ClientConn` factory across schemes.
  - Swift gRPC clients are channel- and event-loop-driven and need scheme-specific channel construction.
  - A faithful helper layer requires a dedicated grpc-swift integration module that is not yet present in this SDK.

## Test

```bash
swift test
```
