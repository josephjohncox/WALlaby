# Shared API and protocol rules

Read the root guidance first. These packages define reusable connector, wire,
stream, schema-registry and verification contracts.

- Make guarantees explicit in types and capabilities. Do not infer durability or
  replay safety from a transport acknowledgement.
- Batch identity must be deterministic and type-sensitive. Checkpoint timestamps
  are observations, not source positions. Test map-order independence and replay.
- Preserve before/after images, unchanged/TOAST values, source positions and
  schema versions across codecs. Exercise JSON/JSONB arrays, generated columns,
  extension types and add/drop/alter/rename evolution when changing wire handling.
- Prefer table-driven and property tests for protocol invariants. A property test
  must exercise the failure path, not just produce successful traces.
- Register new built-in endpoint names as reserved; plugins must not override them.
- Public API/package-comment changes require `mise run docs-generate`. Changes to
  shared contracts need callers, codec tests and generated references updated.
