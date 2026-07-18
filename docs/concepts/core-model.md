# Core model

WALlaby has two paths: a control path and a data path. Keep them separate when you deploy, extend, or debug the system.

## Control path

The control path stores what should run.

```text
CLI or gRPC client
        │
        ▼
   Flow service
        │
        ▼
 Lifecycle engine ──► durable workflow store
        │
        ▼
    Dispatcher ──► worker process, DBOS workflow, or Kubernetes Job
```

A **flow** is the durable definition of one PostgreSQL source, one or more destinations, and its lifecycle state. The lifecycle engine owns state transitions and the current **generation**. A generation is a fence: work created for an older generation cannot register after the flow pauses, resumes, or stops.

A **dispatcher** starts or cancels execution. It does not decide lifecycle policy. The lifecycle engine records intent first, then asks the dispatcher to perform the external action, then reconciles incomplete work after a crash.

## Data path

The data path moves records.

```text
PostgreSQL source
       │ read batch
       ▼
  Stream runner
       │
       ├──► destination adapter
       ├──► destination adapter
       │
       └──► checkpoint and outbox store
                 │
                 └── source acknowledgement
```

The **flow runner** proves that the execution belongs to the current generation. It then constructs a **stream runner** for the flow.

The stream runner owns the delivery order:

1. Read a batch from the source adapter.
2. Write the required destinations.
3. Persist the checkpoint, and persist secondary outbox entries when the acknowledgement policy is `primary`.
4. Acknowledge the source.
5. Drain any durable secondary deliveries.

Source and destination packages are adapters at the data-path seams. They translate WALlaby's batch model to an external system. They do not own lifecycle state, dispatch, or checkpoint policy.

## Abstraction hierarchy

The interfaces form a narrow hierarchy:

| Interface | Caller | Responsibility |
| --- | --- | --- |
| `workflow.Engine` | gRPC control plane | Create, inspect, and change flows |
| `workflow.ControlReader` | control plane and workers | Read the current lifecycle fence |
| `workflow.LifecycleStore` | orchestrated lifecycle engine | Persist lifecycle intent, generations, and execution leases |
| `workflow.Dispatcher` | orchestrated lifecycle engine | Start and cancel one lifecycle generation |
| `grpc.RunOnceDispatcher` | `RunFlowOnce` RPC | Start one uniquely identified attempt against a captured generation |
| `workflow.ExecutionEngine` | flow runner | Register, renew, and finish one execution without control-plane mutation methods |
| `connector.Source` | stream runner | Read and acknowledge source batches |
| `connector.Destination` | stream runner | Write batches and apply supported schema changes |
| `connector.CheckpointStore` | stream runner | Restore and persist flow progress |
| `connector.CheckpointOutboxStore` | primary-ack stream runner | Atomically persist progress and secondary deliveries |

The hierarchy is intentionally asymmetric. The control plane has broad flow-management authority. A worker receives only the lifecycle and execution methods needed to run one flow.

## What is core

The core is:

- one PostgreSQL logical-replication source;
- the flow lifecycle and generation fence;
- the batch runner;
- durable checkpoints and replay-safe acknowledgement ordering;
- source and destination adapter seams;
- OpenTelemetry signals for the control and data paths.

Destination count, wire formats, workflow runtimes, and schema registries are adapter choices around that core. They should not change its ordering or lifecycle rules.

## Where to continue

- [Flow lifecycle](lifecycle.md)
- [Delivery and checkpoints](delivery.md)
- [Architecture](../architecture.md)
- [Generated Go interfaces](../reference/generated/go/connector.md)
