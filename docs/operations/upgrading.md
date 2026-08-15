# Upgrading

WALlaby keeps PostgreSQL authoritative for generation fences, attempts, receipts,
checkpoints, publication, source feedback, quotas, and GC roots. The centralized
control-store coordinator applies migration domains in a fixed order with
checksum-drift detection. Each domain uses its own transaction and
transaction-scoped advisory lock; the complete cross-domain upgrade is not one
atomic database transaction. This page is the operator how-to for applying an
upgrade safely.

!!! warning "No mixed binaries, no rolling restart"
    Authority and artifact migrations are **not** designed for a rolling upgrade
    with old and new binaries running at the same time. Drain workers, apply the
    upgrade, then start the new binaries. Do not run two WALlaby versions against
    the same control database concurrently.

## Supported upgrade paths

- **PostgreSQL 14, 15, 16, and 17** are supported for the managed PostgreSQL
  profile. The logical decoding protocol is pinned to `pgoutput` v2 with
  `streaming` and `messages`, which all four majors support.
- Current migration domains are checksum-verified and idempotent when rerun from
  a compatible recorded state. Individual migrations can strengthen nullability,
  remove defaults, add exact constraints, or reject data that cannot prove the
  current authority identity; they are not universally additive.
- Incompatible legacy per-domain ledgers are rejected. They are not discovered,
  imported, copied, or dual-written into the centralized control ledger.
- Fresh installs apply the ordered current domains in one coordinator run. A
  rerun validates the committed prefix and continues with unapplied migrations.

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

- visits each migration domain in its fixed order;
- starts one transaction and takes the transaction-scoped advisory lock for that
  domain;
- verifies the recorded SHA-256 checksum of every already-applied migration and
  **aborts on any drift**;
- rejects incompatible legacy per-domain ledgers rather than importing them;
- applies new SQL and records its authoritative history atomically within that
  domain transaction; and
- runs `verifyManagedAuthoritySchema`, which fails closed if any required table,
  column, constraint, index, or trigger is missing.

If domain N fails, transactions already committed for domains 1 through N-1 stay
committed. Keep every WALlaby process stopped, correct the reported incompatibility
or restore the pre-upgrade backup, then rerun the same released binary. A rerun
verifies the committed prefix before continuing. Do not edit the ledger to skip a
failed domain.

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

## Rollback limitations

WALlaby does not provide down migrations. After any new-domain transaction
commits, do not restart an older binary against that control database—even when a
later domain failed. The general rollback procedure is to stop every WALlaby
process and restore the complete quiesced control-database backup. Otherwise,
repair the forward incompatibility and rerun the same new release.

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
