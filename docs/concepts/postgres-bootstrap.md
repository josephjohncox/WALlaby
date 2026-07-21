# PostgreSQL slot-anchored bootstrap

`internal/bootstrap` and the PostgreSQL source implement the slot-anchored managed bootstrap used by `wallaby-worker` and in-process DBOS execution. The exact maintained `postgresql-to-postgresql-v1` profile requires `bootstrap=required`. Generic managed execution may use `bootstrap=auto|required|never`, but those broader configurations remain experimental.

The bootstrapper creates a bootstrap-generation-qualified logical slot with `EXPORT_SNAPSHOT`. PostgreSQL stores the returned system identity, slot, publication, manifest hash, consistent point, and snapshot name under the producer fence while the exporter connection stays open.

Before slot creation, the coordinator freezes a bounded table selection and holds PostgreSQL relation locks that block DDL but permit DML. It creates or exactly adopts the publication under a fenced source-resource operation, then creates the slot so the publication is visible at the logical-decoding consistent point. It imports the exported snapshot to verify the same relation/schema manifest.

Snapshot workers call `ImportSnapshot` before catalog or row reads. Each worker uses a read-only, repeatable-read transaction and imports the same snapshot. A task claim, destination attempt/evidence, exclusive cursor, immutable receipt, and task completion all carry the current `RunFence`; only the receipt/cursor transaction advances progress. PostgreSQL destination batches go to generation-qualified durable staging tables. Publishing all frozen tables is one target transaction, so CDC never observes a partially published multi-table snapshot.

Exporter lifetime is strict:

- Concurrent task retries inside the same live worker may re-import its still-live exporter snapshot and resume from a receipt-backed exclusive cursor.
- A replacement process cannot resume the old exported snapshot: exporter loss invalidates the unpublished generation, and every task restarts from zero under a new bootstrap generation and physical slot.
- Cleanup first records `abandoning`, then drops the exact slot, then records `abandoned`.
- A failed drop remains retryable.
- A restart allocates a new bootstrap generation and physical slot name, even within the same lifecycle generation.

The bootstrap contract rejects source pools smaller than two sessions, partitioned/partition relations, and destination targets connected by foreign keys. Snapshot concurrency is capped to the source pool sessions left after reserving the schema-barrier session. Those restrictions are part of the exact named profile; configurations outside them are not promoted.

Publication requires all frozen tasks and destination receipts. Handoff locks and reloads the persisted snapshot cut; caller-supplied slot, source, publication revision, manifest, or LSN differences fail as conflicts. PostgreSQL commits the exact persisted checkpoint, frozen relation-schema baselines, ACK intent, and private `streaming` phase together. The exporter then closes and CDC opens the same owned slot at that exact point. If the first pgoutput `Relation` message after a later restart contains a changed schema, the decoder compares it with this delivered baseline and emits structured DDL before dependent DML. Delivery remains at-least-once with reconciliation; this is not an exactly-once claim.

The legacy `mode=backfill` source remains experimental and is not this protocol.
