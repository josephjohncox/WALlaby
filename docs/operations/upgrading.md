# Upgrading

WALlaby keeps PostgreSQL authoritative for generation fences, attempts, receipts,
checkpoints, publication, source feedback, quotas, and GC roots. Schema upgrades
to those authoritative tables run through the centralized control-store migration
coordinator under a single advisory lock with checksum-drift detection. This page
is the operator how-to for applying an upgrade safely.

!!! warning "No mixed binaries, no rolling restart"
    Authority and artifact migrations are **not** designed for a rolling upgrade
    with old and new binaries running at the same time. Drain workers, apply the
    upgrade, then start the new binaries. Do not run two WALlaby versions against
    the same control database concurrently.

## Supported upgrade paths

- **PostgreSQL 14, 15, 16, and 17** are supported for the managed PostgreSQL
  profile. The logical decoding protocol is pinned to `pgoutput` v2 with
  `streaming` and `messages`, which all four majors support.
- Every prior shipped schema migrates forward to the current schema for the
  workflow, delivery, checkpoint, registry, artifact publication/consumer, and
  connector-state domains. Migrations are additive and idempotent; a populated
  legacy per-domain ledger is imported into the centralized control-store ledger
  without re-running the underlying SQL.
- Fresh installs apply the full current schema in one pass and are idempotent on
  re-run.

## Before you upgrade

1. **Quiesce the fleet.** Stop or pause every flow and drain all workers. Confirm
   no flow is in `running` or `stopping` and that no execution lease is active:

   ```bash
   wallaby-admin flow list --output json | jq -r '.[] | "\(.id) \(.state)"'
   ```

   Every flow must be in `created`, `paused`, `stopped`, or `failed` before you
   proceed. `running` and `stopping` flows still hold execution leases.

2. **Take a control-database backup.** The migration coordinator fails closed on
   checksum drift, but a backup is still your rollback of record.

3. **Pin one upgrade coordinator.** Run the upgrade from a single process. The
   advisory lock serializes concurrent attempts, but a single coordinator keeps
   the operation legible.

## Applying the upgrade

Point the new binary at the control database and let it apply migrations on
startup, or apply them explicitly. The coordinator:

- takes one advisory lock for the whole migration set,
- verifies the recorded SHA-256 checksum of every already-applied migration and
  **aborts on any drift**,
- imports legacy per-domain ledger rows (`wallaby_checkpoint_migrations`,
  `wallaby_schema_migrations`, `wallaby_registry_migrations`) into
  `wallaby_control_migrations` without re-executing their SQL,
- runs any new migrations, dual-recording each, and
- runs `verifyManagedAuthoritySchema`, which fails closed if any required table,
  column, constraint, index, or trigger is missing.

## The quiesced cutover (authority migrations 006/007)

The authority migrations that reshape execution/lease state require a fully
quiesced fleet. If a running or stopping flow, or an active execution, still
exists when the migration runs, it **aborts with SQLSTATE `55000`
(`object_not_in_prerequisite_state`)** rather than mutating live state.

### If you see SQLSTATE 55000

```
ERROR: cannot apply authority migration while flows are active (SQLSTATE 55000)
```

This is the guard working as intended, not corruption. Remediate:

1. Re-run the quiesce check above and drain any flow still holding a lease.
2. Wait for in-flight executions to finish; do not `kill -9` a worker to force it
   — let the lease expire or the execution complete so no attempt is left
   ambiguous.
3. Re-run the upgrade. The migration is idempotent and resumes cleanly.

Do not attempt to bypass the guard by editing the ledger. The check protects the
generation-fence invariants that keep exactly-one-writer semantics intact.

## After the upgrade

- Start the new binaries and resume flows.
- Confirm the schema verification passed in the coordinator logs
  (`verifyManagedAuthoritySchema ok`).
- The artifact-log migrations (004/005) are documented alongside destination
  configuration in [Configuration](configuration.md); the same drain-first rule
  applies.

## Stack merge order

When upgrading from a stacked change set, merge the base before the dependent
change. The durable-core usability base must land before the managed-durability
change that depends on it; do not retarget or merge the dependent branch ahead of
its base. Apply database migrations only from the merged, released binary — never
from an in-flight feature branch.

## Related

- [Recovery](recovery.md) — recovering stalled attempts and ambiguous commits.
- [Durable recovery runbooks](durable-recovery-runbooks.md) — orphan artifacts,
  catalog conflicts, receipt reconciliation, quota/GC.
- [Runbooks](../runbooks.md) — symptom-first operational guide.
