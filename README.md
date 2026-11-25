# WALlaby

![WALlaby logo](./wallaby-small.png)

WALlaby replicates PostgreSQL changes through a small control plane and per-flow workers. A flow has one PostgreSQL source, one or more destination adapters, a lifecycle state, and a durable checkpoint.

The core contract is narrow:

1. Read a logical-replication batch.
2. Write the required destinations.
3. Persist progress before acknowledging PostgreSQL.
4. Fence workers when lifecycle generation changes.

## Run the first flow

The local tutorial creates three PostgreSQL databases, replicates `public.orders`, verifies the destination row, and removes the slot:

[Replicate your first table](https://josephjohncox.github.io/WALlaby/getting-started/quickstart/)

The source files are under [`examples/quickstart/`](examples/quickstart/).

## Understand the system

- [Core model](https://josephjohncox.github.io/WALlaby/concepts/core-model/)
- [Lifecycle](https://josephjohncox.github.io/WALlaby/concepts/lifecycle/)
- [Delivery and checkpoints](https://josephjohncox.github.io/WALlaby/concepts/delivery/)
- [Architecture](https://josephjohncox.github.io/WALlaby/architecture/)
- [PostgreSQL connector reference](https://josephjohncox.github.io/WALlaby/connectors/postgres/)

## Commands

| Command | Responsibility |
| --- | --- |
| `wallaby` | Run the gRPC control plane. |
| `wallaby-admin` | Create and operate flows, publications, slots, DDL gates, and streams. |
| `wallaby-worker` | Execute one flow against a captured lifecycle generation. |

Verification and benchmark binaries live under `cmd/`; they are development tools, not part of the first-run path.

## Develop

Install [mise](https://mise.jdx.dev/). The public toolchain and workflows live in
`mise.toml`; no private CLI or organization login is required.

```bash
mise trust
mise install
mise run setup
mise run build
mise run proto
mise run format
mise run lint
mise run test
RAPID_CHECKS=100 mise run test-rapid
mise run docs-verify
```

`mise tasks` lists all workflows. `mise run ci-check-test` runs the non-mutating
pre-merge gate. Go tools in ad-hoc commands run through `mise exec --`.
See the root and subtree `AGENTS.md` files for hierarchical contributor guidance.

Kafka/Redpanda and native [Apache Iggy](https://josephjohncox.github.io/WALlaby/connectors/iggy/)
are separate destination adapters. A local Iggy fixture and typed flow example
live under `examples/iggy/` and `examples/flows/postgres_to_iggy.yaml`.

Run the live integration harness with:

```bash
mise run test-integration
```

See the [contributor guide](https://josephjohncox.github.io/WALlaby/development/) for PostgreSQL-backed store tests, TLA+ checks, Helm validation, and generated-file rules.

## Repository map

- `pkg/`: stable connector, stream, wire, and certification interfaces.
- `connectors/`: source and destination adapters.
- `internal/`: lifecycle, dispatch, checkpoint, registry, and runner implementations.
- `proto/`: gRPC wire contracts.
- `specs/`: TLA+ models and action-coverage manifests.
- `examples/`: runnable flow definitions and local fixtures.
- `docs/`: user guide and generated reference.

## License

WALlaby uses the PolyForm Perimeter License 1.0.1. See [LICENSE](LICENSE).
