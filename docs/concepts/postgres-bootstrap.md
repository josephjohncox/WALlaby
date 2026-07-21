# PostgreSQL slot-anchored bootstrap

`internal/bootstrap` contains experimental bootstrap primitives. `wallaby-worker` does not call them; managed admission rejects `bootstrap=auto|required` and admits only `bootstrap=never`.

The bootstrapper creates a bootstrap-generation-qualified logical slot with `EXPORT_SNAPSHOT`. PostgreSQL stores the returned system identity, slot, publication, manifest hash, consistent point, and snapshot name under the producer fence while the exporter connection stays open.

Snapshot workers call `ImportSnapshot` before any catalog or row query. Each worker uses a read-only, repeatable-read transaction and imports the same snapshot. `RecordTaskReceipt` commits a task's durable cursor, immutable receipt hash, and completed state together.

Exporter lifetime is strict:

- A live exporter allows a replacement worker to import the same snapshot.
- Exporter loss invalidates the unpublished generation.
- Cleanup first records `abandoning`, then drops the exact slot, then records `abandoned`.
- A failed drop remains retryable.
- A restart allocates a new bootstrap generation and physical slot name, even within the same lifecycle generation.

Publication requires at least one completed task receipt and no incomplete tasks. Handoff locks and reloads the persisted snapshot cut; caller-supplied slot, source, manifest, or LSN differences fail as conflicts. PostgreSQL commits the exact persisted checkpoint, ACK intent, and private `streaming` phase together.

The legacy `mode=backfill` source remains experimental and is not this protocol.
