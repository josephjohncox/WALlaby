# API reference

The reference pages are generated from the repository source. CI regenerates them and rejects stale output.

## gRPC and Protobuf

The [gRPC and Protobuf reference](generated/grpc.md) covers services, methods, request and response messages, enums, and field numbers from `proto/wallaby/v1`.

## Go packages

WALlaby treats these packages as stable library APIs:

- [`certify`](generated/go/certify.md)
- [`connector`](generated/go/connector.md)
- [`pgstream`](generated/go/pgstream.md)
- [`schemaregistry`](generated/go/schemaregistry.md)
- [`spec`](generated/go/spec.md)
- [`stream`](generated/go/stream.md)
- [`wire`](generated/go/wire.md)

Packages under `internal/`, command implementations, and concrete connectors are not part of this generated stability surface.

## Regenerate locally

```bash
just docs-generate
just docs-verify
```

Edit comments in Go or Protobuf source files. Do not edit files under `docs/reference/generated/` by hand.
