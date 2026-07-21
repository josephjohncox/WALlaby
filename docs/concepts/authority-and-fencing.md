# Authority and generation fencing

PostgreSQL is the authority for managed flow ownership. A managed mutation carries one `RunFence`:

- immutable flow incarnation UUID;
- lifecycle generation;
- acquisition UUID and execution ID;
- lease epoch.

`internal/authority` acquires the same transaction-scoped flow advisory lock for validation and producer takeover. It then validates all fields and an unexpired lease in the authoritative mutation's transaction. A takeover cannot commit between validation and mutation. A takeover creates a new acquisition and increments the lease epoch; the old owner cannot renew, finish, fail the flow, advance a managed checkpoint, adopt delivery or bootstrap evidence, mutate snapshot progress, publish DDL receipts, change owned source resources, publish an artifact, or authorize source feedback. Default-zero fences are rejected before SQL and fenced rows require positive provenance.

Flow deletion retires the incarnation. Recreating the public flow ID allocates a new incarnation, so old checkpoints and leases do not enter the new flow.

The public lifecycle remains `created`, `running`, `paused`, `stopping`, `stopped`, and `failed`. Acquisition, claim, bootstrap, delivery, and publication phases are private persistence states.

## Current scope

The managed worker path requires an explicit positive generation, acquires `RunFence` before connectors open, binds it to the source registry hook and DDL receipt store, and renews only that producer lease. Lifecycle quiescence counts compatibility executions and the current live producer acquisition. Legacy execution remains available for compatibility. The server and worker share one control pool and ordered migration entrypoint; in-process DBOS receives the same authority, delivery, bootstrap, checkpoint, and registry dependencies. Legacy administrative slot/publication cleanup and reconfiguration fail closed for managed flows rather than bypassing source-resource ownership.
