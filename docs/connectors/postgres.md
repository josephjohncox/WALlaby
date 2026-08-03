# PostgreSQL connectors

PostgreSQL appears in three different roles. Choose the role before choosing options.

| Type | Role | Writes ordinary tables? | Consumer API |
| --- | --- | --- | --- |
| `postgres` as a source | Read logical replication | No | WALlaby worker |
| `postgres` as a destination | Apply records to PostgreSQL tables | Yes | SQL |
| `pgstream` as a destination | Store an acknowledged message queue in PostgreSQL | No | `stream pull` and `stream ack` |

The first tutorial uses a `postgres` source and a `postgres` destination. Do not use `pgstream` when you expect rows to appear in an application table.

## PostgreSQL source

### Database requirements

The source server must have:

```sql
SHOW wal_level;             -- must return logical
SHOW max_replication_slots; -- must be greater than the active slot count
SHOW max_wal_senders;       -- must be greater than the active sender count
```

The database role needs `REPLICATION` and `SELECT` on published tables. With the default automatic setup, it also needs `CREATE` on the database to create a publication and ownership of each table whose publication membership it changes. A superuser satisfies those requirements but is not the least-privilege choice.

For least privilege, have an administrator create the slot and publication, grant the runtime role `REPLICATION` and table reads, then set:

```json
{
  "ensure_publication": "false",
  "sync_publication": "false",
  "create_slot": "false"
}
```

Updates and deletes need a stable row identity. Prefer a primary key. If a table has no suitable key, configure PostgreSQL replica identity explicitly and test the resulting records before production use.

### Core options

| Option | Required | Default | Meaning |
| --- | --- | --- | --- |
| `dsn` | yes | — | PostgreSQL connection string. Treat it as a secret. |
| `slot` | yes | — | Logical replication slot name. |
| `publication` | yes | — | Publication name. |
| `publication_tables` | recommended | all tables when a new publication has no explicit scope | Comma-separated `schema.table` list. Set this explicitly. |
| `ensure_publication` | no | `true` | Create the publication when absent. |
| `sync_publication` | no | `false` | Reconcile publication membership to the configured table list. |
| `create_slot` | no | `true` | Create the slot when absent. |
| `batch_size` | no | `100` | Maximum records per batch. |
| `batch_timeout` | no | `1s` | Maximum wait before returning a partial batch. |
| `status_interval` | no | `10s` | PostgreSQL standby-status update interval. |
| `streaming_transactions` | no | `true` for managed execution | Use pgoutput protocol v2 streamed transactions. |
| `managed_profile` | no | — | Exact promoted profile name; use `postgresql-to-postgresql-v1` only with its full contract. |
| `resolve_types` | no | `true` | Resolve PostgreSQL type metadata. |
| `emit_empty` | no | `false` | Emit empty polling batches. Lifecycle workers normally leave this disabled. |

If `publication_tables` and `publication_schemas` are both absent, an automatically created publication covers all tables. That default is convenient for a disposable database and dangerous for a shared database. Production flow files should name their table scope.

### Minimal source

```json
{
  "name": "orders-source",
  "type": "postgres",
  "options": {
    "dsn": "postgres://user:password@source:5432/app?sslmode=require",
    "slot": "wallaby_orders",
    "publication": "wallaby_orders",
    "publication_tables": "public.orders",
    "ensure_publication": "true",
    "sync_publication": "true",
    "create_slot": "true"
  }
}
```

## PostgreSQL table destination

The `postgres` destination applies inserts, updates, and deletes to ordinary tables. Create compatible target tables before starting the flow unless your DDL process creates them.

### Core options

| Option | Required | Default | Meaning |
| --- | --- | --- | --- |
| `dsn` | yes | — | Target PostgreSQL connection string. Treat it as a secret. |
| `schema` | no | source schema | Override the destination schema. |
| `table` | no | source table | Route all records to one table. Leave empty to preserve source table names. |
| `write_mode` | no | `target` | `target` applies mutations; `append` appends records. |
| `synchronous_commit` | no | server setting | Transaction durability setting for destination writes. |
| `meta_table_enabled` | no | `true` | Maintain WALlaby's destination metadata table. |
| `managed_profile` | no | — | Must match the source profile for maintained managed execution. |

The destination does not make a non-idempotent database magically idempotent. Use primary keys or another stable conflict strategy, especially with primary acknowledgement where a crash can replay the primary write.

For maintained PostgreSQL-to-PostgreSQL execution, use the exact [managed profile](postgres-managed-profile.md). Generic source/destination options remain experimental and do not inherit that profile's support status.

### Minimal destination

```json
{
  "name": "orders-postgres",
  "type": "postgres",
  "options": {
    "dsn": "postgres://user:password@destination:5432/app?sslmode=require",
    "schema": "public",
    "write_mode": "target",
    "synchronous_commit": "on"
  }
}
```

## PostgreSQL stream destination

The `pgstream` destination stores messages, delivery state, visibility deadlines, and consumer-group acknowledgements in PostgreSQL. Use it when consumers pull and acknowledge messages through WALlaby.

| Option | Required | Default | Meaning |
| --- | --- | --- | --- |
| `dsn` | yes | — | PostgreSQL database used for stream storage. |
| `stream` | yes | — | Stream name. |
| `format` | no | flow wire format | Stored payload format. |

Consume it with:

```bash
wallaby-admin stream pull --stream orders --group worker-a --max 10 --visibility 30
wallaby-admin stream ack --stream orders --group worker-a --ids <returned-ids>
```

Read [consume streams](../streams.md) for visibility and replay behavior.

## Cleanup

Stopping a flow does not decide whether to retain its slot or publication. After a terminal stop, use:

```bash
wallaby-admin flow cleanup --flow-id <flow-id>
```

The default cleanup drops the slot and source-state row but keeps the publication. Review [manage flows](../guides/flows.md) before changing those flags.
