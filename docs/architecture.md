# Architecture

WALlaby has a gRPC control plane and generation-scoped execution attempts. The control plane stores intent. Supervised workers and Kubernetes Jobs run as separate processes; DBOS executes attempts in the server process.

## One flow

A flow contains:

- one PostgreSQL source;
- one or more destination adapters;
- one wire format and delivery policy;
- one public lifecycle state;
- one internal lifecycle generation;
- one durable checkpoint position.

The generation prevents delayed work from crossing a lifecycle transition. A worker created for generation 3 cannot register after a pause and resume creates generation 4.

## Control plane

```text
wallaby-admin or gRPC client
            │
            ▼
        Flow service
            │
            ▼
  Orchestrated lifecycle engine
       │                 │
       ▼                 ▼
workflow store       dispatcher
                         │
             worker / DBOS / Kubernetes Job
```

The workflow store is the source of truth for flow state, lifecycle target, generation, dispatch intent, and active execution leases. Production uses PostgreSQL. The memory store is for development and tests only.

Lifecycle operations follow one rule: record intent before performing an external effect. After a crash, reconciliation reads the durable intent and finishes dispatch or cancellation. Pause and stop do not report a quiescent state while an execution lease can still continue.

Read [flow lifecycle](concepts/lifecycle.md) for the state machine.

## Data plane

```text
PostgreSQL logical replication
            │
            ▼
       source adapter
            │ Batch
            ▼
        stream runner
       │      │       │
       ▼      ▼       ▼
 destination adapters  checkpoint/outbox store
                              │
                              ▼
                    source acknowledgement
```

The source adapter decodes `pgoutput` messages into WALlaby records. The stream runner owns batching, retry policy, destination ordering, schema-change application, checkpoint persistence, and source acknowledgement. Destination adapters translate a batch into the target system.

A connector does not own lifecycle state or dispatch. A workflow runtime does not own record delivery. The runner is the only module that joins those concerns, and it does so through narrow interfaces.

## Delivery order

With acknowledgement policy `all`:

1. Read a source batch.
2. Write every destination.
3. Persist the checkpoint.
4. Acknowledge the source.

With acknowledgement policy `primary`:

1. Write the primary destination.
2. Atomically persist the checkpoint and one outbox row per secondary destination.
3. Acknowledge the source.
4. Drain secondary deliveries and delete each completed outbox row.

Startup restores and drains pending outbox entries before reading new source data. Writes must be idempotent because a crash can replay a batch. Read [delivery and checkpoints](concepts/delivery.md) for the crash boundaries.

## Storage responsibilities

| Store | Owns | Does not own |
| --- | --- | --- |
| Workflow store | Flow definitions, lifecycle intent, generations, execution leases | Record batches |
| Checkpoint store | Last durable source position | Flow lifecycle |
| Checkpoint/outbox store | Atomic primary-ack checkpoint and secondary deliveries | Destination-specific state |
| Schema registry | Schema versions and compatibility metadata | Lifecycle or checkpoints |
| PostgreSQL stream store | Pull/ack message delivery state | Source acknowledgement policy |

A single PostgreSQL cluster can host several stores, but the schemas have separate responsibilities.

## Runtime choices

The data path is the same in every runtime. Only process ownership changes.

- **Supervised worker:** an external supervisor starts one `wallaby-worker` process per running flow.
- **DBOS:** DBOS schedules durable workflow attempts.
- **Kubernetes:** the control plane creates generation-scoped Jobs.

Read [choose a runtime](deployment/index.md) for operational differences.

## Adapter seams

The stable Go seams live under `pkg/`:

- `connector.Source` reads and acknowledges batches.
- `connector.Destination` writes batches.
- `connector.CheckpointStore` restores and persists progress.
- `connector.CheckpointOutboxStore` provides atomic primary-ack durability.
- `wire.Codec` encodes a batch for a destination.

The lifecycle and dispatch interfaces are internal because they define WALlaby's own control-plane invariants rather than a connector extension contract.

Read [the core model](concepts/core-model.md) for the complete interface hierarchy and [the generated Go reference](reference/index.md) for method signatures.
