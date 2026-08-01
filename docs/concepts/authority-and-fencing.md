# Authority and generation fencing

PostgreSQL is the authority for managed flow ownership. A managed mutation carries one `RunFence`:

- immutable flow incarnation UUID;
- lifecycle generation;
- acquisition UUID and execution ID;
- lease epoch.

`internal/authority` acquires the same transaction-scoped flow advisory lock for validation and producer takeover. It then validates all fields and an unexpired lease in the authoritative mutation's transaction. A takeover cannot commit between validation and mutation. A takeover creates a new acquisition and increments the lease epoch; the old owner cannot renew, finish, fail the flow, advance a managed checkpoint, adopt evidence, publish an artifact, or authorize source feedback.

Flow deletion retires the incarnation. Recreating the public flow ID allocates a new incarnation, so old checkpoints and leases do not enter the new flow.

The public lifecycle remains `created`, `running`, `paused`, `stopping`, `stopped`, and `failed`. Acquisition, claim, bootstrap, delivery, and publication phases are private persistence states.

## Current scope

The managed worker path requires an explicit positive generation, acquires `RunFence` before connector construction, and renews only that producer lease. Managed execution does not call the compatibility execution-finishing API. Lifecycle quiescence counts both compatibility executions and the current live producer acquisition. Legacy execution remains available for compatibility. DBOS does not yet construct the new authority and delivery dependencies, so managed admission fails closed there.
