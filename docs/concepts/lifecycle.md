# Flow lifecycle

A flow has one durable lifecycle state. Slot-retention policy is separate configuration and does not create extra lifecycle states.

| State | Meaning | Allowed next states |
| --- | --- | --- |
| `created` | Stored but not running | `running` |
| `running` | Eligible for worker execution | `paused`, `stopping`, `failed` |
| `paused` | Quiesced and resumable | `running`, `stopping`, `failed` |
| `stopping` | Terminal cancellation is in progress | `stopped`, `failed` |
| `stopped` | Terminally stopped | none |
| `failed` | Execution failed | none |

## Pause and resume

`PauseFlow` records durable pause intent, asks the configured dispatcher to cancel active work, and waits for execution to quiesce. The public state remains `running` while cancellation is in progress and changes to `paused` only after WALlaby has established that the affected execution is no longer active. Resume returns a paused flow to `running` and dispatches work again when a dispatcher is configured.

Use pause when you expect to resume the same flow, such as before changing a publication or destination configuration.

## Stop

`StopFlow` is terminal. It first records `stopping`, cancels active DBOS or Kubernetes execution, waits for cancellation, then records `stopped`. If cancellation fails or the request context expires, the flow remains `stopping`. A later stop request can continue the operation.

A stopped flow cannot be started again. Create a new flow when you need a new terminal lifecycle.

## Dispatch recovery and fencing

WALlaby durably records start, resume, pause, and stop intent. The control plane reconciles incomplete intent after transient dispatch or cancellation failures, including after a server restart. Each running incarnation is fenced so delayed work from an older incarnation cannot register as current execution.

These controls do not add public lifecycle states or fields. Observe the states in the table above: pause becomes visible only after quiescence, and stop remains `stopping` until cancellation is complete. Reconciliation never starts a `failed` or `stopped` flow.

## Run once

`RunFlowOnce` asks the dispatcher to execute one run without changing the durable lifecycle state. It requires a configured dispatcher and a runnable flow.

## Processing failures

`failure_mode` controls replication-slot cleanup after a non-cancellation runner error:

- `hold_slot` keeps the slot for investigation and replay.
- `drop_slot` asks sources that support slot deletion to drop it.

The flow state remains the generic `failed` state. Failed is terminal: the control plane does not automatically redispatch it, and start or resume cannot revive it. After diagnosis and cleanup, create a new flow when execution must continue. Cancellation and context deadlines do not trigger slot deletion.
