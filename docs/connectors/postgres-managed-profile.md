# PostgreSQL managed profile

Status: **experimental**. No maintained-support or exactly-once claim applies.

## Admitted runtime profile

Managed execution requires:

- an explicit positive lifecycle generation;
- a PostgreSQL source with `managed=true`, `bootstrap=never`, `ensure_publication=false`, and `ensure_state=false`;
- `source_system_identifier`, `source_lineage_id`, and `publication_revision` values that match the live PostgreSQL system and publication definition;
- PostgreSQL authority, fenced checkpoints, and the delivery coordinator;
- `ack_policy=all`;
- one PostgreSQL destination revision with `destination_revision_id`;
- target write mode and target batch mode; and
- explicit `synchronous_commit=on` or `remote_apply`.

Managed admission rejects automatic publication changes, publication synchronization, DDL capture, legacy slot-keyed source state, arbitrary `start_lsn`, legacy backfill, generic staging, drop-slot failure mode, ClickHouse mutation delivery, and automatic raw SQL DDL.

`bootstrap=auto|required` also fails admission. The slot-exported bootstrap primitives are not wired into `wallaby-worker`; accepting those values would silently omit rows that existed before slot creation.

## Durability behavior

The source buffers pgoutput changes until `COMMIT`, enforces transaction record and byte limits, and checkpoints only at `TransactionEndLSN`. A new slot's consistent point is persisted with a source ACK intent before destination startup. On restart, the worker verifies the ACK intent before scheduling feedback.

The destination writes DML, metadata, and a deterministic marker in one target transaction. An ambiguous commit remains recoverable: the flow stays `running`, a later producer reconciles the marker, and PostgreSQL adopts matching evidence. Delivery remains at-least-once.

The profile currently admits one table/schema fragment per source transaction. It fails closed on a multi-fragment transaction.
