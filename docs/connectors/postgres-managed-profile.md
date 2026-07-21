# PostgreSQL managed profile

Status: **experimental**. No maintained-support or exactly-once claim applies.

## Admitted runtime profile

Managed execution requires:

- an explicit positive lifecycle generation;
- a PostgreSQL source with `managed=true` and `bootstrap=auto|required|never`; managed runtime state is always PostgreSQL-authoritative even if legacy source-state options remain parseable;
- `source_system_identifier`, `source_lineage_id`, and `publication_revision` values that match the live PostgreSQL system and publication definition;
- PostgreSQL authority, fenced checkpoints, and the delivery coordinator;
- `ack_policy=all`;
- one PostgreSQL destination revision with `destination_revision_id`;
- target write mode and target batch mode; and
- explicit `synchronous_commit=on` or `remote_apply`.

For `auto|required`, the coordinator creates or exactly adopts the frozen publication under the current fence and supplies runtime-only slot/publication/start options. For `never`, publication creation must remain disabled and the operator must supply a compatible initial dataset and slot. Managed admission rejects arbitrary `start_lsn`, legacy backfill, file/disabled snapshot authority, generic staging, drop-slot failure mode, ClickHouse mutation delivery, DDL-capture resource creation, and automatic raw SQL DDL. The experimental bootstrap also rejects `pool_max_conns<2`, partitioned or partition relations, and destination target tables connected by foreign keys. Snapshot workers are capped to the source sessions available after reserving one session for the schema barrier.

## Durability behavior

The source buffers pgoutput changes until `COMMIT`, enforces transaction record and byte limits, and checkpoints only at `TransactionEndLSN`. Managed bootstrap creates the publication before the logical slot cut, imports the slot-exported snapshot in every bounded table task, atomically publishes PostgreSQL destination staging tables, and seeds the CDC checkpoint and ACK intent at the exact consistent point. Exporter loss abandons the whole unpublished generation. On restart, the worker verifies the ACK intent before scheduling feedback.

The destination writes DML, metadata, and a deterministic marker in one target transaction. An ambiguous commit remains recoverable: the flow stays `running`, a later producer reconciles the marker, and PostgreSQL adopts matching evidence. Delivery remains at-least-once.

The profile currently admits one table/schema fragment per source transaction. It fails closed on a multi-fragment transaction.
