# Durable recovery runbooks

These runbooks cover the durable delivery core: stalled attempts, orphan
artifacts, catalog conflicts, receipt reconciliation, and quota/GC. PostgreSQL is
authoritative for every fence, attempt, receipt, checkpoint, source-feedback
record, quota account, and GC root. S3 holds immutable, versioned artifacts.
WALlaby never claims exactly-once: replays converge by deterministic identity
(at-least-once with idempotent dedupe), and duplicates are bounded. Within the
modeled boundary chain, completed recovery does not omit a modeled position when
PostgreSQL authority, fencing, immutable/reconcilable side effects, and connector
implementations satisfy their stated contracts. This is not proof that every
external service, deployment, connector, or operator action is gap-free.

Prove the cause before you change lifecycle state or delete anything.

## Stalled attempt

**Symptom.** A delivery or publication attempt is prepared but never produced a
durable receipt; the flow makes no forward progress on a position.

1. Identify the position and attempt:

   ```sql
   SELECT
     p.position_id,
     a.consumer_revision_id,
     a.publication_id,
     a.attempt_id,
     a.commit_id,
     a.prepared_at
   FROM artifact_delivery_attempts AS a
   JOIN artifact_publications AS p
     ON p.flow_incarnation_id = a.flow_incarnation_id
    AND p.publication_id = a.publication_id
   LEFT JOIN artifact_delivery_receipts AS r
     ON r.attempt_id = a.attempt_id
   WHERE a.flow_incarnation_id = $1
     AND r.attempt_id IS NULL
   ORDER BY a.consumer_revision_id, p.sequence, a.prepared_at;
   ```

2. Confirm the current owner holds the lease. A stalled attempt from a fenced
   (superseded) worker is expected and harmless — the current owner re-drives it.
   Do **not** start a competing worker to "help"; the generation fence rejects a
   stale writer, and a manual second writer only adds noise.
3. Let the current owner reconcile. Reconciliation reads the external side effect
   and either adopts an already-applied commit or re-applies a not-applied one. A
   truly ambiguous outcome halts fail-closed as **indeterminate** (below) rather
   than guessing.

## Orphan artifact

**Symptom.** An immutable object exists in S3 with no active publication rooting
it (for example, a crash landed a PUT after the attempt row but before the
publication commit).

1. Orphans are collected by the epoch-fenced GC, which marks
   (`state = deleting`) then finalizes the S3 delete and PostgreSQL finalize under
   the cleanup fence. A crash between mark and delete resumes from the marked rows.
2. Confirm the object is genuinely unrooted before intervening:

   ```sql
   SELECT o.artifact_id, o.state
   FROM artifact_objects o
   LEFT JOIN artifact_publication_objects r ON r.artifact_id = o.artifact_id
   WHERE o.flow_incarnation_id = $1 AND r.artifact_id IS NULL;
   ```

3. An in-flight prepared upload attempt pins its bytes and blocks orphan
   collection on purpose. Wait for the upload to resolve; do not delete the object
   by hand. GC reconciles the exact S3 version and fails closed on a version
   conflict.

## Catalog conflict (indeterminate delivery)

**Symptom.** A consumer or managed destination logs
`delivery outcome indeterminate` and stops advancing a specific publication.

1. This is the fail-closed path. The external commit could not be proven applied
   or not-applied (for example, an optimistic catalog-pointer swap conflict, or an
   eventually-consistent load whose status is not yet visible). WALlaby refuses to
   advance a checkpoint, receipt, or source ACK from an indeterminate outcome.
2. Inspect the external system (catalog snapshot, load history, target marker) to
   establish the true outcome.
3. Once the external state is known, let the fenced writer re-run: it reconciles
   by exact publication/content identity. If the commit did land, reconciliation
   adopts it; if it did not, a fresh attempt with the same deterministic commit ID
   re-applies it. The `wallaby.artifact.consumer.outcomes` counter (with
   `outcome=indeterminate`, or the equivalent delivery metric) makes a
   stuck-indeterminate position alertable rather than silent.

## Receipt reconciliation

**Symptom.** An external commit is visible but PostgreSQL has no matching durable
receipt (a crash after the side effect, before adoption).

1. Adoption is idempotent and identity-keyed. The recovering owner reconciles the
   external evidence and adopts it into exactly one receipt, then advances the
   checkpoint and source-ACK intent atomically.
2. Verify there is at most one receipt for each exact consumer revision and
   publication identity after recovery:

   ```sql
   SELECT
     flow_incarnation_id,
     consumer_revision_id,
     publication_id,
     count(*)
   FROM artifact_delivery_receipts
   WHERE flow_incarnation_id = $1
   GROUP BY flow_incarnation_id, consumer_revision_id, publication_id
   HAVING count(*) > 1;
   ```

   This query must return no rows. Different consumer revisions can legitimately
   receipt the same source position, so position-only grouping is not a duplicate
   test. Capture any duplicate exact identity and escalate rather than deleting
   rows.

## Quota and GC

**Symptom.** Rooted bytes look wrong, or retention is not releasing.

1. Recompute the quota account from the surviving roots and compare:

   ```sql
   SELECT rooted_bytes FROM artifact_quota_accounts WHERE flow_incarnation_id = $1;
   ```

   The account must equal a fresh recompute. Retention releases a root only when
   the source ACK receipt exists, the delivery is done, and the root is older than
   the current checkpoint. A marked-but-unreleased root still counts against the
   quota until GC finalizes it.
2. GC never deletes an object referenced by an active publication or pinned by an
   in-flight prepared upload. If GC is not making progress, look for a stuck
   prepared attempt or an indeterminate delivery pinning retention.

## Verifying the protocol model

The deterministic process-failure matrix
([development guide](../development/failure-matrix.md)) starts real child
processes, applies kill/restart/overlapping-takeover faults, and recovers an
fsync-backed **protocol-model** state file at modeled delivery, feedback,
publication, retention, and GC boundaries:

```bash
just test-failure-matrix
```

Its NDJSON records substantiate model transitions across OS-process death. They
do **not** contain production delivery attempts, destination receipts, S3
objects, PostgreSQL WAL, or live connector artifacts, and they do not prove that
a production stalled attempt or orphan object recovered. Use the required
PostgreSQL/MinIO integration profiles and destination-specific live gates for
those claims. This runbook's SQL queries and external-system inspection remain
the authority for an actual incident.

## Related

- [Upgrading](upgrading.md) — quiesced cutover and migration order.
- [Recovery](recovery.md) — worker recovery basics.
- [Runbooks](../runbooks.md) — symptom-first operational guide.
