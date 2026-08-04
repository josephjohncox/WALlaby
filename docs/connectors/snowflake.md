# Snowflake destination

WALlaby exposes two Snowflake modes:

| Contract | Status | Delivery contract |
| --- | --- | --- |
| `postgresql-to-snowflake-sql-v1` | experimental | PostgreSQL 16 CDC into one hybrid table, with external-commit reconciliation |
| Generic `snowflake` and `snowpipe` | experimental | Legacy direct-table and file-loading behavior |

The named SQL profile has no reviewed Snowflake service version or deployment cell. Local tests, PostgreSQL tests, mocks, and fakesnow cannot promote it. A maintained declaration requires an unskipped real-service run on the reviewed SHA.

The profile provides **at-least-once delivery with external-commit reconciliation**. It does not claim exactly-once delivery.

## Admission contract

The profile admits only this configuration:

- PostgreSQL 16 with `server_encoding=UTF8` and `integer_datetimes=on`;
- one source relation and one destination;
- `ack_policy=all`, `streaming_transactions=true`, and `bootstrap=never`;
- `slot=managed` and `create_slot=true` for a new flow;
- `toast_fetch=off`;
- one publication that contains the source relation and publishes insert, update, and delete;
- no publication row filter, column list, partition, `FOR ALL TABLES`, truncate, or non-default replica identity;
- an immediate, valid PostgreSQL primary key with immutable values;
- one pre-provisioned hybrid target table and one pre-provisioned hybrid receipt table in the same database;
- a target primary key that matches the PostgreSQL primary key in name, order, nullability, and admitted type;
- unquoted uppercase Snowflake object, role, and column names;
- a dedicated object-owner role and a distinct execution role;
- no task visible to the execution role in the managed schema and no additional direct table grant with write privileges;
- key-pair JWT authentication over verified HTTPS with OCSP fail-closed;
- one configured `CURRENT_VERSION()` value, checked on every acquired session;
- exact target and receipt-table creation identities;
- at most 1,000 records, 128 fragments, and 8 MiB of logical content per PostgreSQL transaction; and
- at most eight Snowflake sessions.

The profile rejects standard Snowflake tables because their primary and unique constraints are not enforced. It also rejects generated columns, generic metadata tables, staging, append mode, disabled transactions, schema drift, DDL, type-mapping overrides, arbitrary start LSNs, and multiple sinks.

Task inspection is limited by Snowflake role visibility. The current runtime rejects tasks visible to the execution role, but that is not proof that another role cannot own hidden automation. Complete task visibility and account-role isolation are explicit missing live promotion gates.

No reviewed account cloud, region, edition, account type, or Snowflake version is recorded yet. The implementation therefore remains experimental even when an operator supplies a matching runtime pin.

## Source cut

Set the source slot to the literal value `managed`:

```json
{
  "managed_profile": "postgresql-to-snowflake-sql-v1",
  "bootstrap": "never",
  "slot": "managed",
  "create_slot": "true",
  "ensure_state": "false",
  "ensure_publication": "false",
  "sync_publication": "false",
  "streaming_transactions": "true",
  "toast_fetch": "off",
  "source_system_identifier": "<pg_control_system.system_identifier>",
  "source_lineage_id": "<stable-lineage-id>",
  "publication_revision": "<publication-fingerprint>",
  "max_transaction_records": "1000",
  "max_transaction_bytes": "8388608",
  "max_transaction_fragments": "128"
}
```

This is a clean-start stream, not a snapshot or preloaded-target protocol. At first start the PostgreSQL relation, Snowflake target, and receipt table must be empty. WALlaby holds a PostgreSQL `SHARE` relation lock, with 60-second lock and 120-second statement timeouts, while proving the source is empty and creating the consistent-point slot. Writes cannot cross the cut. A preloaded source requires a future snapshot profile; it is rejected here.

The bound PostgreSQL run fence derives a slot name from the flow incarnation. WALlaby creates the slot, stores its consistent point as the authoritative checkpoint, stores the slot as an owned source resource, and creates the source ACK intent while holding the flow authority lock. On restart, the runner loads that checkpoint, disables slot creation, and opens the same derived slot at the authoritative LSN.

A controlled failure after positive slot-creation evidence rolls back the authority transaction and drops only that inactive slot. A hard crash or indeterminate `START_REPLICATION` result may leave a flow-derived slot without an authoritative checkpoint. Restart then fails closed; WALlaby does not guess that an unrooted physical resource is owned. An operator must verify that no `authoritative_checkpoints` or `source_resources` row exists for the flow incarnation, verify the slot is inactive, logical, `pgoutput`, and in the expected database, then drop that exact slot before retrying. Never drop an active or identity-mismatched slot.

The publication must already exist:

```sql
CREATE PUBLICATION wallaby_widgets
  FOR TABLE public.widgets
  WITH (publish = 'insert, update, delete');
```

## Supported source types

SQL v1 admits only the type cell below. Generic Snowflake mappings do not widen it.

| PostgreSQL source type | Snowflake target type |
| --- | --- |
| `bigint` / `int8` | `NUMBER(38,0)` |
| `boolean` / `bool` | `BOOLEAN` |
| `bytea` | `BINARY` with width at least the configured transaction-byte bound |
| finite `numeric(p,s)`, where `p <= 38` and `0 <= s <= p` | `NUMBER(p,s)` |
| `text` | `VARCHAR` |
| `timestamp with time zone` / `timestamptz` | `TIMESTAMP_TZ` with precision at least 6 |

JSON, JSONB, arrays, extension types, unbounded numeric, floating-point types, money, date, time, UUID, and character modifiers fail admission. This restriction avoids unproved or lossy coercions, including `PARSE_JSON` conversion of large PostgreSQL JSON numbers to Snowflake `DOUBLE`.

Both `type_mappings` and `type_mappings_file` are rejected.

## Provision Snowflake objects

Run provisioning as a dedicated owner role. The execution role selected by the connector DSN must be different from the owner role.

This target matches a source table with `id bigint PRIMARY KEY`, `value text`, `payload bytea`, `amount numeric(12,2)`, `active boolean`, `event_at timestamptz`, and `extra text`:

```sql
CREATE HYBRID TABLE "DB"."WALLABY"."WIDGETS_V1" (
  "ID" NUMBER(38,0) NOT NULL,
  "VALUE" VARCHAR,
  "PAYLOAD" BINARY(8388608),
  "AMOUNT" NUMBER(12,2),
  "ACTIVE" BOOLEAN,
  "EVENT_AT" TIMESTAMP_TZ,
  "EXTRA" VARCHAR,
  CONSTRAINT "PK_WIDGETS_V1" PRIMARY KEY ("ID")
)
COMMENT = 'wallaby:postgresql-to-snowflake-sql-v1:target:snowflake-widgets-v1:<schema-contract-sha256>:<flow-id-sha256>';
```

Create the receipt table in the same database:

```sql
CREATE HYBRID TABLE "DB"."WALLABY"."WALLABY_RECEIPTS_V1" (
  "PROFILE_VERSION" VARCHAR NOT NULL,
  "FLOW_ID" VARCHAR NOT NULL,
  "FLOW_INCARNATION_ID" VARCHAR NOT NULL,
  "SOURCE_LINEAGE_ID" VARCHAR NOT NULL,
  "DESTINATION_REVISION_ID" VARCHAR NOT NULL,
  "LOGICAL_BATCH_ID" VARCHAR NOT NULL,
  "POSITION_ID" VARCHAR NOT NULL,
  "CONTENT_HASH" VARCHAR NOT NULL,
  "SCHEMA_CONTRACT_HASH" VARCHAR NOT NULL,
  "CATALOG_FINGERPRINT" VARCHAR NOT NULL,
  "MANIFEST_HASH" VARCHAR NOT NULL,
  "EXTERNAL_ID" VARCHAR NOT NULL,
  "GENERATION" NUMBER(38,0) NOT NULL,
  "ACQUISITION_ID" VARCHAR NOT NULL,
  "LEASE_EPOCH" NUMBER(38,0) NOT NULL,
  "TRANSACTION_ID" NUMBER(38,0) NOT NULL,
  "FRAGMENT_COUNT" NUMBER(38,0) NOT NULL,
  "RECORD_COUNT" NUMBER(38,0) NOT NULL,
  "COMMITTED_AT" TIMESTAMP_TZ NOT NULL,
  CONSTRAINT "PK_WALLABY_RECEIPTS_V1"
    PRIMARY KEY (
      "FLOW_INCARNATION_ID",
      "DESTINATION_REVISION_ID",
      "SOURCE_LINEAGE_ID",
      "POSITION_ID"
    ),
  CONSTRAINT "UQ_WALLABY_LOGICAL_V1"
    UNIQUE ("FLOW_INCARNATION_ID", "DESTINATION_REVISION_ID", "LOGICAL_BATCH_ID"),
  CONSTRAINT "UQ_WALLABY_EXTERNAL_V1" UNIQUE ("EXTERNAL_ID")
)
COMMENT = 'wallaby:postgresql-to-snowflake-sql-v1:receipts:snowflake-widgets-v1:<schema-contract-sha256>:<flow-id-sha256>';
```

Grant only the required table privileges:

```sql
GRANT SELECT, INSERT, UPDATE, DELETE
  ON TABLE "DB"."WALLABY"."WIDGETS_V1"
  TO ROLE "WALLABY_EXECUTION";

GRANT SELECT, INSERT
  ON TABLE "DB"."WALLABY"."WALLABY_RECEIPTS_V1"
  TO ROLE "WALLABY_EXECUTION";
```

The owner role should expose only its `OWNERSHIP` grant on these objects. The execution role must not own them. Admission rejects another direct table grant with `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, or `OWNERSHIP`, disables secondary roles on every session, and rejects an execution session whose primary-role hierarchy includes the owner role. A broader account-role and global-privilege isolation check remains a required live promotion gate.

Admission tolerates additional read-only grants (for example `SELECT` to a monitoring role), but the catalog fingerprint embedded in every receipt hashes the full grant map. A benign read-only grant added after a batch commits therefore fails receipt reconciliation closed — it is treated as indeterminate/conflict, never a double apply, and wedges the flow until the grant is removed. Whether to tolerate read-only grant drift or forbid every additional grant at admission is a deferred decision for the live promotion matrix, since choosing correctly requires observing what `SHOW GRANTS` returns for a dedicated managed schema on a reviewed real Snowflake account.

Record each creation identity with the same expression used by admission:

```sql
SELECT TABLE_NAME,
       TO_VARCHAR(CREATED, 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM') AS CREATED_ON
FROM "DB".INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'WALLABY'
  AND TABLE_NAME IN ('WIDGETS_V1', 'WALLABY_RECEIPTS_V1');
```

A replacement under the same object name changes this identity and fails admission.

## Destination configuration

The DSN must use key-pair JWT. Keep the private key in a mounted secret rather than the flow document.

```json
{
  "name": "analytics-snowflake-sql",
  "type": "snowflake",
  "options": {
    "dsn": "wallaby@ACCOUNT/DB/WALLABY?warehouse=WALLABY_WH&role=WALLABY_EXECUTION&authenticator=SNOWFLAKE_JWT&privateKeyFile=/run/secrets/snowflake-key.p8&ocspFailOpen=false&READ_LATEST_WRITES=true&TIMEZONE=UTC",
    "flow_id": "widgets-to-snowflake",
    "managed_profile": "postgresql-to-snowflake-sql-v1",
    "destination_revision_id": "snowflake-widgets-v1",
    "write_mode": "target",
    "batch_mode": "target",
    "batch_resolution": "none",
    "meta_table_enabled": "false",
    "disable_transactions": "false",
    "session_keep_alive": "false",
    "managed_account": "ACCOUNT",
    "managed_database": "DB",
    "managed_schema": "WALLABY",
    "managed_table": "WIDGETS_V1",
    "managed_receipts_table": "WALLABY_RECEIPTS_V1",
    "managed_owner_role": "WALLABY_OWNER",
    "managed_execution_role": "WALLABY_EXECUTION",
    "managed_warehouse": "WALLABY_WH",
    "managed_snowflake_version": "<reviewed CURRENT_VERSION()>",
    "managed_target_created_on": "<target CREATED_ON>",
    "managed_receipts_created_on": "<receipt CREATED_ON>",
    "managed_source_schema": "public",
    "managed_source_table": "widgets",
    "managed_schema_contract": "<connector.Schema JSON>",
    "managed_schema_contract_hash": "<schema-contract-sha256>",
    "managed_max_transaction_rows": "1000",
    "managed_max_transaction_bytes": "8388608",
    "managed_max_transaction_fragments": "128",
    "managed_max_open_conns": "4",
    "managed_statement_timeout_seconds": "120",
    "managed_hybrid_table_lock_timeout_seconds": "60"
  }
}
```

Compute the schema hash with `snowflake.ManagedSchemaContractHash`. Every source column must set `nullability_known=true` and `generated_known=true`. Primary-key columns must also set `primary_key=true`; composite keys require one-based `primary_key_ordinal` values.

`flow_id` must equal the WALlaby flow ID. Its SHA-256 digest is part of both ownership comments, and existing receipts must belong to the same flow incarnation. A destination revision must change when the flow binding, profile version, account, database, schema, target, receipt table, owner role, execution role, warehouse, service version, object creation identity, schema contract, session timeout, or transaction bound changes.

## Session contract

WALlaby pins one `*sql.Conn` for session validation, catalog validation, the explicit transaction, and `COMMIT`. It sets and verifies these parameters on every acquired session:

- `AUTOCOMMIT=TRUE`;
- `TRANSACTION_ABORT_ON_ERROR=TRUE`;
- `ABORT_DETACHED_QUERY=TRUE`;
- `ERROR_ON_NONDETERMINISTIC_MERGE=TRUE`;
- `ERROR_ON_NONDETERMINISTIC_UPDATE=TRUE`;
- `READ_LATEST_WRITES=TRUE`;
- finite `STATEMENT_TIMEOUT_IN_SECONDS`;
- finite `HYBRID_TABLE_LOCK_TIMEOUT`; and
- `CLIENT_SESSION_KEEP_ALIVE=FALSE`.

It first runs `USE SECONDARY ROLES NONE`, then rechecks the account, database, schema, execution role, owner-role isolation, warehouse, and exact Snowflake service version on that session.

## Transaction and recovery protocol

For one committed PostgreSQL transaction, WALlaby:

1. validates the source identity, checkpoint position, immutable primary key, size bounds, and destination revision;
2. builds the ordered SQL plan without external writes;
3. validates the live Snowflake object creation identities, catalog, grants, tasks, columns, and enforced constraints;
4. lets PostgreSQL persist the destination attempt;
5. acquires and validates a pinned Snowflake session;
6. starts one explicit Snowflake transaction and revalidates the catalog;
7. checks for a matching receipt from an earlier owner;
8. inserts the final receipt first, so its enforced keys serialize competing owners;
9. applies each insert, update, and delete in fragment and record order;
10. requires every statement to affect exactly one row;
11. commits once; and
12. lets PostgreSQL adopt the matching Snowflake receipt, advance its authoritative checkpoint, and authorize source feedback.

The marker has the form `sf-marker:v1:<sha256>` and excludes generation, acquisition, lease epoch, and attempt number from its logical identity.

If `COMMIT` returns an error, WALlaby returns `ErrDeliveryIndeterminate` and discards the physical Snowflake session. It never runs reconciliation on that session. A new validated session queries all stable receipt identities:

- one exact match means the external transaction committed;
- no receipt means replay is safe, although a replaying receipt insert may still wait for an older unresolved transaction; and
- a mismatched or duplicate receipt means `ErrDeliveryConflict`.

The Snowflake receipt does not advance source state. PostgreSQL remains authoritative for flow generations, run fences, source checkpoints, delivery attempts, delivery receipts, and ACK intents.

## DDL and schema changes

SQL v1 rejects raw and structured DDL. It does not accept a pre-applied DDL barrier.

To change the schema:

1. pause the flow;
2. provision new target and receipt objects;
3. assign a new destination revision;
4. update the schema contract, schema hash, object creation identities, comments, and configuration; and
5. restart after the source and destination contracts match.

## Telemetry

Managed Snowflake spans may carry logical-batch, marker, operation, and Snowflake query IDs. Metrics use only bounded profile, operation, outcome, and error-class attributes. WALlaby does not put DSNs, private keys, JWTs, markers, query IDs, transaction IDs, or source positions in metric labels.

## Evidence gate

The real-service gate requires a separate provisioning DSN because the execution role cannot own the managed tables:

```bash
WALLABY_TEST_SNOWFLAKE_MANAGED=1 \
WALLABY_TEST_SNOWFLAKE_DSN='...' \
WALLABY_TEST_SNOWFLAKE_PROVISION_DSN='...' \
WALLABY_TEST_SNOWFLAKE_VERSION='<reviewed version>' \
WALLABY_TEST_SNOWFLAKE_REGION='<reviewed AWS region>' \
WALLABY_TEST_SNOWFLAKE_OWNER_ROLE='WALLABY_OWNER' \
TEST_PG_DSN='postgres://...' \
just test-snowflake-managed-profile
```

The recipe rejects skipped or missing required tests. It records the PostgreSQL version, Snowflake version and region, Go driver version, and JWT auth mode. Fakesnow must fail managed admission and cannot satisfy this gate.

The current tests still do not supply reviewed same-SHA evidence for every required network fault, detached-transaction takeover, full worker `SIGKILL`, account edition/type, bounded-load, telemetry, redaction, and cleanup cell. The profile therefore remains experimental.
