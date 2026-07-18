# WALlaby

WALlaby reads PostgreSQL logical replication and delivers each batch through explicit source, destination, lifecycle, and checkpoint interfaces. A gRPC control plane stores flow intent. One worker executes each flow.

## Start with one working flow

Follow [replicate your first table](getting-started/quickstart.md). It starts a local PostgreSQL fixture, creates a direct PostgreSQL-to-PostgreSQL flow, writes a row, verifies the destination, and cleans up the replication slot.

## Then choose what you need

| Goal | Read |
| --- | --- |
| Understand the module hierarchy | [Core model](concepts/core-model.md) |
| Automate pause, resume, or stop | [Flow lifecycle](concepts/lifecycle.md) |
| Choose `all` or `primary` acknowledgement | [Delivery and checkpoints](concepts/delivery.md) |
| Configure the PostgreSQL source or destination | [PostgreSQL connectors](connectors/postgres.md) |
| Operate a flow | [Manage flows](guides/flows.md) |
| Choose worker, DBOS, or Kubernetes execution | [Choose a runtime](deployment/index.md) |
| Look up a gRPC or Go method | [API reference](reference/index.md) |
| Build or change WALlaby | [Contributor guide](development/index.md) |

## Product boundary

The core is one PostgreSQL source, a generation-fenced flow lifecycle, a batch runner, durable checkpoint ordering, and adapter seams. Destinations, wire formats, workflow runtimes, and schema registries plug into that core. They do not change its lifecycle or acknowledgement rules.

WALlaby does not promise generic exactly-once delivery. It persists progress before source acknowledgement and requires idempotent destination behavior at replay boundaries. The [delivery guide](concepts/delivery.md) names those boundaries.
