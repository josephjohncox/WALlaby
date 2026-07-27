# hamba-avro-shim

A local Go module that keeps the `github.com/hamba/avro/v2` **import path** but
routes every symbol to the maintained, patched fork
`github.com/iskorotkov/avro/v2`. It exists solely to remediate three advisories
in the archived upstream while preserving source and wire compatibility.

## Why this exists

`github.com/hamba/avro/v2` is archived and carries three unfixed advisories:

| Advisory      | Class                                             |
| ------------- | ------------------------------------------------- |
| GO-2026-5046  | CPU exhaustion via attacker-controlled block count |
| GO-2026-5047  | Integer overflow / narrowing in the decoder        |
| GO-2026-5048  | Unbounded map allocation (DoS)                     |

The Go vulnerability database lists **no fixed version** for
`github.com/hamba/avro/v2` (`Fixed in: N/A`); the fixes live only in the
renamed successor module `github.com/iskorotkov/avro/v2`, fixed in **v2.33.0**
(this repo pins **v2.33.1**).

We consume Avro two ways:

1. `pkg/wire` (our own OCF codec), and
2. `github.com/apache/iceberg-go@v0.5.0`, which reads/writes Iceberg manifests
   and **imports `github.com/hamba/avro/v2` directly**.

### Why not a plain module replace

```
replace github.com/hamba/avro/v2 => github.com/iskorotkov/avro/v2 v2.33.1
```

is rejected by the go tool:

```
github.com/iskorotkov/avro/v2@v2.33.1 used for two different module paths
(github.com/hamba/avro/v2 and github.com/iskorotkov/avro/v2)
```

The fork renamed its module path, so its own packages import
`github.com/iskorotkov/avro/v2/...` internally. Under a plain replace, iceberg-go
would hold `hamba/avro/v2.Schema` while the fork's `ocf` package produced
`iskorotkov/avro/v2.Schema` — **two incompatible Avro type universes** that do
not compile, and, if forced, would silently split the type system.

### Why not bump iceberg-go to v0.6.0

`iceberg-go@v0.6.0` drops Avro-via-hamba for the unrelated
`github.com/twmb/avro`, changing the manifest codec and API surface. That is a
larger, compatibility-affecting change that also abandons the prescribed
`iskorotkov/avro/v2` remediation, so it is out of scope for a security repair.

### Why not fork iceberg-go or vendor a hand patch

Vendoring iceberg-go (160 files, ~3 MB) to rewrite four import lines, or
hand-patching the archived hamba source, is heavier and harder to audit than a
mechanical alias layer. The task also forbids vendoring an *unverifiable* patch.

## What this module does

- Module path stays `github.com/hamba/avro/v2` so iceberg-go and `pkg/wire`
  keep their existing imports.
- Every exported identifier is a **type alias** (`type X = upstream.X`) or a
  **function/var forwarder** (`var F = upstream.F`) to
  `github.com/iskorotkov/avro/v2`. Type aliases preserve type identity, so all
  consumers share **one** Avro type universe — the patched one.
- The **actual compiled decoder/encoder code is iskorotkov v2.33.1**. The shim
  contributes no Avro logic of its own, only aliases.

Because the vulnerable symbols are defined in `iskorotkov/avro/v2@v2.33.1`
(fixed) and never in this shim, `govulncheck` attributes the reachable code to
the fixed module and reports **0 affected vulnerabilities**. The archived
`hamba/avro/v2` version is no longer in the build graph.

## Provenance / regeneration

- Upstream: `github.com/iskorotkov/avro/v2@v2.33.1`
  (`https://github.com/iskorotkov/avro`, tag `v2.33.1`).
- The two `*_shim.go` files are generated from that release's exported API
  (root `avro` package and `ocf` subpackage), excluding test files. They contain
  only aliases/forwarders — no copied implementation — so they are trivially
  auditable against upstream `go doc`.
- Regenerate after an upstream bump by updating the `require` in `go.mod` and
  re-emitting aliases for the new exported surface, then run
  `go build ./...` + `just vulncheck` at the repo root.

## Wiring

Root `go.mod`:

```
require github.com/iskorotkov/avro/v2 v2.33.1 // indirect
replace github.com/hamba/avro/v2 => ./third_party/hamba-avro-shim
```

## Tests

Regression and property coverage lives in `pkg/wire/avro_security_test.go`
(malicious array/map block counts, MinInt64 negation, oversized byte slices,
64-bit long conversion, OCF round-trip, deterministic schema generation).
Iceberg manifest read/write is covered by
`connectors/destinations/iceberg/commit_test.go`.
