# Snowflake destination

WALlaby exposes these Snowflake modes:

| Contract | Status | Delivery contract |
| --- | --- | --- |
| `postgresql-to-snowflake-sql-v1` | experimental | PostgreSQL 16 CDC into one hybrid table, with external-commit reconciliation |
| `postgresql-to-snowflake-staged-append-v1` | experimental | PostgreSQL 16 CDC into one append changelog table via deterministic internal-stage COPY, with landing and target proof |
| `postgresql-to-snowflake-streaming-rest-append-v1` | experimental (fails closed) | PostgreSQL 16 CDC appended to a Snowpipe Streaming channel, adopted only on SQL-observed row completeness. Admission is refused until a reviewed high-performance append transport is linked |
| Generic `snowflake` and `snowpipe` | experimental | Legacy direct-table and file-loading behavior |

## Deployment execution policy and credentials

Snowflake-backed execution is **disabled by default**. The deployment operator must set both:

```yaml
snowflake:
  enabled: true
  account: xy12345
  user: WALLABY_SERVICE
  host: xy12345.snowflakecomputing.com
  private_key_file: /run/secrets/wallaby/snowflake-key.pem
  private_key_secret_name: wallaby-snowflake # required for Kubernetes dispatch
  private_key_secret_key: private-key.pem
```

The equivalent server/worker variable families are `WALLABY_SNOWFLAKE_*` and `WALLABY_WORKER_SNOWFLAKE_*` for `ENABLED`, `ACCOUNT`, `USER`, `HOST`, and `PRIVATE_KEY_FILE`; the server also accepts `WALLABY_SNOWFLAKE_PRIVATE_KEY_SECRET_NAME` and `WALLABY_SNOWFLAKE_PRIVATE_KEY_SECRET_KEY` for dispatched Jobs. This deployment policy covers generic `snowflake`, generic `snowpipe`, and all three managed profiles. Flow options cannot enable or override it. Kubernetes dispatch passes the current policy as authoritative worker arguments, including an explicit `false` when disabled, and mounts the configured Secret key at mode `0400`.

Snowflake DSNs persisted in flows may contain only the reviewed account/database/schema/warehouse/role identity fields and the small managed-session allowlist. Authority passwords, unknown driver controls, custom hosts/proxies/TLS settings, logging/diagnostic controls, private keys, passcodes, tokens, OAuth/client secrets, proxy passwords, credential/secret aliases, repeated aliases, and encoded variants are rejected before persistence. The parsed account, user, canonical host, HTTPS port, JWT authenticator, and fail-closed OCSP mode must equal the deployment policy before execution. Put no PEM or base64 key material in a DSN. The runtime preloads one deployment-owned, absolute, owner-only regular PEM file (PKCS#8 or PKCS#1 RSA, at least 2048 bits) and supplies it directly to `gosnowflake.NewConnector`; it never reconstructs an inline-key DSN. WALlaby pins gosnowflake easy logging to `OFF` with a process-owned client configuration that takes precedence over `SF_CLIENT_CONFIG_FILE` and default easy-logging files, so flow/query data cannot be exposed by driver logging controls.

Disabling the gate blocks new persistence, lifecycle transitions, reconciliation, DBOS recovery, and newly started workers for existing Snowflake-backed flows. It does not reach into a process that already holds an open connection: stop or terminate those workers and roll the deployment when revoking execution. Offline plan/check commands prove only structural and credential-safe flow syntax. `flow plan --endpoint ...` calls the server's policy-aware `ValidateFlow` RPC before producing a diff and fails when the current deployment does not admit the flow.

These are implemented modeled protocol profiles, not blanket support claims for the Snowflake adapters. SQL and staged COPY have no reviewed Snowflake service version or deployment cell with every required same-SHA live recovery gate. Local tests, PostgreSQL tests, mocks, and fakesnow cannot promote them. Streaming additionally has no linked reviewed append transport, so it fails closed before external I/O. A maintained declaration requires complete unskipped real-service evidence on the reviewed SHA; Streaming also requires the concrete reviewed transport.

The SQL profile provides **at-least-once delivery with external-commit reconciliation**. It does not claim exactly-once delivery.

## Admission contract

The profile admits only this configuration:

- PostgreSQL 16 with `server_encoding=UTF8` and `integer_datetimes=on`;
- one source relation and one destination;
- `config.ack_policy=all`, `streaming_transactions=true`, and `bootstrap=BOOTSTRAP_MODE_NEVER`;
- `slot=managed` and `create_slot=true` for a new flow;
- `toast_fetch=TOAST_FETCH_MODE_OFF`;
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

Set the source slot to the literal value `managed` in the typed source branch. Use exact protobuf enums and native booleans and integers:

```json
{
  "name": "postgres-snowflake-source",
  "postgres_source": {
    "mode": "POSTGRES_SOURCE_MODE_CDC",
    "managed_profile": "MANAGED_PROFILE_POSTGRESQL_TO_SNOWFLAKE_SQL_V1",
    "bootstrap": "BOOTSTRAP_MODE_NEVER",
    "slot": "managed",
    "create_slot": true,
    "ensure_state": false,
    "ensure_publication": false,
    "sync_publication": false,
    "streaming_transactions": true,
    "toast_fetch": "TOAST_FETCH_MODE_OFF",
    "source_system_identifier": "<pg_control_system.system_identifier>",
    "source_lineage_id": "<stable-lineage-id>",
    "publication_revision": "<publication-fingerprint>",
    "max_transaction_records": 1000,
    "max_transaction_bytes": 8388608,
    "max_transaction_fragments": 128
  }
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

The managed branch rejects `type_mappings`; built-in endpoint configuration has no file-backed mapping branch.

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
  "snowflake_postgres_sql": {
    "dsn": "wallaby@ACCOUNT/DB/WALLABY?warehouse=WALLABY_WH&role=WALLABY_EXECUTION&authenticator=SNOWFLAKE_JWT&privateKeyFile=/run/secrets/snowflake-key.p8&ocspFailOpen=false&READ_LATEST_WRITES=true&TIMEZONE=UTC",
    "destination_revision_id": "snowflake-widgets-v1",
    "account": "ACCOUNT",
    "database": "DB",
    "schema": "WALLABY",
    "table": "WIDGETS_V1",
    "receipts_table": "WALLABY_RECEIPTS_V1",
    "owner_role": "WALLABY_OWNER",
    "execution_role": "WALLABY_EXECUTION",
    "managed_warehouse": "WALLABY_WH",
    "snowflake_version": "<reviewed CURRENT_VERSION()>",
    "target_created_on": "<target CREATED_ON>",
    "receipts_created_on": "<receipt CREATED_ON>",
    "max_transaction_rows": 1000,
    "max_transaction_bytes": 8388608,
    "max_transaction_fragments": 128,
    "max_open_connections": 4,
    "statement_timeout_seconds": 120,
    "hybrid_table_lock_timeout_seconds": 60
  }
}
```

The runtime derives the flow binding and projected schema contract from the typed flow and destination-scoped table mapping. They are not free-form endpoint fields. Every source column in that derived contract must have known nullability and generation status; primary-key columns must cover the complete ordered source key.

For this exact profile, `flow mappings generate` requires catalog scope to resolve to one relation with a complete ordered source primary key. It emits one exact upsert mapping with that full key and excludes future tables. Append overrides, watermarks, multiple or keyless relations, and partial, reordered, or extra match-column overrides fail before output. Generic Snowflake generation remains append-only.

The runtime flow binding is the WALlaby flow ID. Its SHA-256 digest is part of both ownership comments, and existing receipts must belong to the same flow incarnation. A destination revision must change when the flow binding, profile version, account, database, schema, target, receipt table, owner role, execution role, warehouse, service version, object creation identity, schema contract, session timeout, or transaction bound changes. Managed `UpdateFlow` and `ReconfigureFlow` are both rejected, including name and parallelism changes. Stop the old flow, create/validate/start a replacement with a new flow ID and revision, cut over, and delete the old flow only when safe. Every Terraform update fails; Terraform does not perform this lifecycle.

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

The SQL implementation still lacks reviewed same-SHA evidence for every required network fault, detached-transaction takeover, full worker `SIGKILL`, account edition/type, bounded-load, telemetry, redaction, and cleanup cell. Those are genuine promotion gaps rather than missing modeled-protocol code. The profile therefore remains experimental.

## Staged COPY append profile

`postgresql-to-snowflake-staged-append-v1` is a second, independent experimental managed profile. Instead of transactional DML into a hybrid table, it serializes each committed PostgreSQL transaction into one deterministic, immutable internal-stage object and loads it with a fail-closed `COPY INTO` into an append-only changelog table. It provides **at-least-once delivery**; it never claims exactly-once. Like the SQL profile, fakesnow and mocks prove logic only and never promote it.

### Delivery protocol

A committed transaction becomes one newline-delimited JSON stage object whose byte content is a pure function of the transaction. Each line is a changelog row carrying the full delivery identity (`FLOW_ID`, `FLOW_INCARNATION_ID`, `SOURCE_LINEAGE_ID`, `DESTINATION_REVISION_ID`, `LOGICAL_BATCH_ID`, `CONTENT_HASH`, source position and LSNs, fragment/record ordinals), the operation, the key, before/after images as `VARIANT`, and a per-row `RECORD_HASH`.

The stage-object path is a deterministic, immutable function of the flow incarnation, destination revision, logical batch, plan hash, and logical content hash, rooted under one per-incarnation retention prefix (`wallaby_staged_append_v1/<incarnation>/…`). Delivery proceeds as:

1. **Fence and adopt** — acquire a Snowflake-clock shared runtime lease bound to the current provision epoch, catalog fingerprint, destination revision, flow incarnation, generation, acquisition, and lease epoch. Acquire the deterministic per-batch load claim. A receipt is adopted only when its immutable identity and the current target manifest plus exact row identities still match.
2. **Stage** — `LIST`/`PUT` the immutable bytes. A `PUT` whose response is lost is reconciled by re-reading the stage. Before any load, WALlaby first requires Snowflake `LIST` to report a stored size no larger than the planned plaintext plus a fixed 64 KiB encryption-envelope allowance; only then does it perform Snowflake `GET` into a plan-sized writer and require the decrypted, uncompressed plaintext to be byte-for-byte equal to the deterministic NDJSON plan. The `GET` runs against a private per-call download directory and emits its own `stage_verify` span. Verification is unconditional, including immediately after this process staged the bytes, so every transaction costs one extra bounded download; that egress is the deliberate price of not treating an encrypted-stage `LIST` checksum as an equality oracle. A different LIST MD5 remains an additional collision signal, but a missing checksum is never accepted as the sole evidence. Because `LIST` and `GET` are both prefix-scoped, a foreign object that merely starts with the deterministic path is reported as a conflict during `LIST` rather than silently joining the download. Unavailable GET evidence is indeterminate; oversized or unequal plaintext is a conflict. Every one of these fails closed before `COPY` and before a receipt.
3. **Load landing** — reload the exact live stage, tables, grants, file format, pipe, and pipe count. Recompute the complete catalog fingerprint, then renew the Snowflake-clock lease before `PUT`, `COPY`, or pipe refresh. `COPY INTO` uses the dedicated landing table with `MATCH_BY_COLUMN_NAME=CASE_SENSITIVE ON_ERROR=ABORT_STATEMENT FORCE=FALSE PURGE=FALSE`. Auto-ingest accepts exactly one pipe, uses the same landing table, refreshes only the deterministic batch prefix, and polls delayed landing visibility within a configured bound.
4. **Prove and promote** — compare every landing `RECORD_HASH` with the planned identity set inside the promotion transaction. Partial, duplicate, and foreign identities fail closed. The transaction renews the exact lease and claim, checks affected row counts, appends the exact landing rows, inserts the immutable companion manifest, and clears the landing rows. A zero-row batch still inserts its manifest. `COPY_HISTORY` is diagnostic only.
5. **Receipt** — reload the live catalog and revalidate the shared lease, claim, provision epoch, target manifest, and target-row identities. Insert one guarded durable load receipt. A duplicate key can be adopted only after the current target manifest and every target row identity are revalidated.

`Reconcile` is read-only. It adopts only a matching receipt whose target manifest and target rows remain complete and unique. An absent receipt is *not applied*. Complete target proof without a receipt converges through `ApplyTransaction` without another `COPY` or pipe refresh.

### Provisioned objects

The profile admits, in one dedicated schema owned by a distinct object-owner role:

- an **internal named stage** (execution role granted `READ, WRITE`);
- a **JSON file format** (execution role granted `USAGE`);
- a **standard append changelog table** with the exact wallaby column contract (execution role granted `SELECT, INSERT`); a hybrid target is rejected;
- a standard **landing table** with the same row contract and no key constraints (execution role granted `SELECT, INSERT, DELETE`);
- a hybrid **authority table** containing the provision guard, shared leases, and load claims with one enforced primary key;
- a hybrid **target manifest table** with enforced logical-batch and manifest identities;
- a **hybrid receipt table** with an enforced primary key on `(RECEIPT_KIND, FLOW_INCARNATION_ID, DESTINATION_REVISION_ID, LOGICAL_BATCH_ID)`, a unique `EXTERNAL_ID`, and the exact `PROVISION_EPOCH` plus catalog fingerprint (execution role granted `SELECT, INSERT`); and
- optionally, when typed `auto_ingest=true`, one owned **pipe** with `AUTO_INGEST=TRUE`.

> **Upgrade note.** Inlining the parsing options changes the deterministic COPY plan hash, and therefore the stage path, manifest hash, and external ID of every batch. Drain and acknowledge in-flight batches before upgrading, or assign a new destination revision. An un-acknowledged batch carried across the upgrade is reported as a receipt-identity conflict and requires operator action rather than silently double-loading.

Every object carries an exact creation-identity timestamp and an ownership comment binding the destination revision, schema-contract hash, and flow. Admission executes `DESCRIBE FILE FORMAT` and requires the exact complete JSON property set and effective values: `TYPE=JSON`, empty `FILE_EXTENSION`, `DATE_FORMAT=TIME_FORMAT=TIMESTAMP_FORMAT=AUTO`, `BINARY_FORMAT=HEX`, `TRIM_SPACE=FALSE`, `MULTI_LINE=FALSE`, `NULL_IF=[]`, `COMPRESSION=AUTO`, `ENABLE_OCTAL=FALSE`, `ALLOW_DUPLICATE=FALSE`, `STRIP_OUTER_ARRAY=FALSE`, `STRIP_NULL_VALUES=FALSE`, `IGNORE_UTF8_ERRORS=FALSE`, `REPLACE_INVALID_CHARACTERS=FALSE`, and `SKIP_BYTE_ORDER_MARK=TRUE`. Missing, duplicate, additional, type-changed, or value-changed properties fail closed. Property type, effective value, and reported default are all included in the catalog fingerprint. Because Snowflake currently documents `MULTI_LINE` for JSON but omits it from documented `DESCRIBE FILE FORMAT` output, admission also requires explicit `MULTI_LINE=FALSE` in the bounded `GET_DDL` definition and fingerprints that complete definition.

No task may be visible in the schema. The profile rejects generated columns, generic metadata/staging options, type-mapping overrides, DDL, arbitrary start LSNs, and multiple sinks. Admission requires key-pair JWT over verified HTTPS with OCSP fail-closed, DSN session parameters `READ_LATEST_WRITES=true` and `TIMEZONE=UTC`, and an inline-secret-free DSN.

Owner provisioning uses a durable attempt UUID and exact epoch CAS. DDL leaves the catalog row in `PROVISIONING` until the owner reloads every live object and installs the post-create fingerprint. Runtime leases reject `PROVISIONING` and `ABORTED` states. Use `wallaby-admin snowflake staged provision bootstrap|inspect|start|resume|abort --spec <non-secret.json> --owner-dsn <ephemeral-dsn>`. `install` is an alias for `bootstrap`. Bootstrap creates the current auxiliary objects, reloads the live catalog, validates it, fingerprints the created objects, and installs the first catalog authority row. Omit `managed_landing_created_on`, `managed_authority_created_on`, and `managed_target_manifest_created_on` from the bootstrap specification. The command returns those live creation identities for the runtime endpoint specification. The command never writes the owner DSN to the flow or provision specification. An abort can return to `CURRENT` only when the live fingerprint still equals the stored pre-attempt fingerprint. A crash after DDL remains fail-closed until the exact attempt resumes or aborts.

### Cleanup and retention

Stage objects are released by a bounded cleanup pass. Cleanup requires the current flow incarnation, generation, acquisition, lease epoch, and destination revision. It acquires a Snowflake-clock lease and deterministic claim, recomputes the stage path from the immutable receipt plan, reloads the live catalog, and revalidates target proof. It repeats the live catalog and lease guard immediately before `REMOVE` and before the guarded release receipt. A stale owner, malicious persisted path, changed epoch, or batch without current target proof cannot remove an object.

### Evidence gate

The staged gate reuses the managed Snowflake credentials plus the provisioning DSN and skips closed without them:

```bash
WALLABY_TEST_SNOWFLAKE_MANAGED=1 \
WALLABY_TEST_SNOWFLAKE_DSN='...' \
WALLABY_TEST_SNOWFLAKE_PROVISION_DSN='...' \
WALLABY_TEST_SNOWFLAKE_VERSION='<reviewed version>' \
WALLABY_TEST_SNOWFLAKE_OWNER_ROLE='WALLABY_OWNER' \
go test ./tests/ -run 'TestSnowflakeStagedManagedProfile|TestPostgresToSnowflakeStagedManagedProfileRecoveryContract'
```

Deterministic PUT/GET, lease, claim, landing, promotion, manifest, receipt, ABA, takeover, partial, duplicate, zero-row, target-proof, and cleanup behavior is exercised against an in-memory protocol fake and property tests. Commercial Snowflake evidence for every named gate is still absent. The profile remains experimental until that exact-SHA evidence passes.

## Snowpipe Streaming REST append profile

`postgresql-to-snowflake-streaming-rest-append-v1` is a third, independent experimental managed profile. It appends each committed PostgreSQL transaction to a durable Snowpipe Streaming channel on a pipe that loads an append-only changelog table, and it adopts a batch **only** after the destination's SQL-observed row completeness plus a durable receipt prove full arrival. It provides **at-least-once delivery**; it never claims exactly-once.

### Fail-closed admission

There is no officially supported Go SDK or high-performance REST client for Snowpipe Streaming: the `database/sql` gosnowflake driver speaks the query API, not the channel append protocol. Proving delivery from a build with no append transport would mean trusting local continuation/offset tokens — token theater. WALlaby refuses that. `ManagedStreamingTransportAvailable()` is a compile-time constant that is **false** until a reviewed high-performance append transport is linked, and both runner admission and destination `Open` **fail closed** with `ErrManagedStreamingTransportUnavailable` before any network side effect. The full admission contract (DSN, JWT, session parameters, identifiers, schema contract, limits, and the typed `transport` declaration) is still validated first, so a misconfiguration produces its own precise error rather than the blanket refusal. Flipping the constant is a promotion action that must ship a concrete append transport and pass the same-SHA live recovery matrix.

### Delivery protocol (exercised against the in-memory fake)

A committed transaction becomes an ordered set of deterministic-identity append rows. Every row carries the full delivery identity, the operation, the key, before/after images, the sorted set of unchanged-TOAST columns, a deterministic per-batch `OFFSET_TOKEN`, an `APPEND_ORDINAL`, and a per-row `ROW_HASH`. The `ROW_HASH` — not any transport token — is the identity that SQL observation counts. Delivery proceeds as:

1. **Adopt** — if a durable append receipt for the logical batch already exists, return it; a receipt whose identity or row-content hash differs is a conflict.
2. **Open channel** — open (or reopen) the deterministic per-incarnation channel and persist its exact channel/pipe revision, continuation token, and committed-offset token to an owned channel-state table.
3. **Observe** — read, by `ROW_HASH`, which rows are already durably present for this logical batch. A row observed more than once is a duplicate-identity hazard that fails closed.
4. **Append proven-missing** — append only the rows SQL observation proves are missing. Channel invalidation reopens, re-observes, and re-appends only the still-missing rows; auth expiry refreshes credentials; throttling backs off within a bound; a terminal response with rejected rows or an oversize row fails closed. Never blindly re-append.
5. **Verify** — poll SQL observation until every `ROW_HASH` is present, then read the committed offset token as corroborating evidence (required when this incarnation performed the append). SQL-observed completeness is the adoption authority; the committed token alone is never sufficient.
6. **Receipt** — insert one durable append receipt into an owned receipt table whose enforced primary key serializes concurrent generations; a duplicate key adopts the winning attempt.

Because the durable receipt plus SQL-observed completeness are the joint proof, `Reconcile` is read-only: an absent receipt is *not applied* so a replay converges idempotently (already-present rows are never re-appended), and only a fully matching receipt is *applied*. Complete-unreceipted recovery (rows already present from a prior incarnation, no receipt yet) appends nothing and writes the receipt on the SQL-observed completeness.

### Provisioned objects

The profile admits, in one dedicated schema owned by a distinct object-owner role: an append changelog **target table** with the exact wallaby column contract (including `ROW_HASH` and `OFFSET_TOKEN`), a **pipe** the channel appends through, an owned **receipt table**, and an owned **channel-state table** that persists the channel/pipe revision and token evidence. It requires key-pair JWT over verified HTTPS with OCSP fail-closed, DSN session parameters `READ_LATEST_WRITES=true` and `TIMEZONE=UTC`, an inline-secret-free DSN, `toast_fetch=TOAST_FETCH_MODE_OFF`, and rejects generated columns, generic metadata/staging options, type-mapping overrides, DDL, arbitrary start LSNs, and multiple sinks.

### Cleanup and retention

Channel state is released by a bounded, idempotent cleanup pass keyed on durable append receipts: only fully committed, acknowledged batches older than the retention window write an idempotent release receipt and have their durable channel state removed. A batch without a durable append receipt is never released.

### Evidence gate

The streaming gate reuses the managed Snowflake credentials and skips closed without them. Because no reviewed append transport is linked, the live entrypoints assert the fail-closed refusal rather than proving delivery:

```bash
WALLABY_TEST_SNOWFLAKE_MANAGED=1 \
WALLABY_TEST_SNOWFLAKE_DSN='...' \
WALLABY_TEST_SNOWFLAKE_VERSION='<reviewed version>' \
go test ./tests/ -run 'TestSnowflakeStreamingManagedProfile|TestPostgresToSnowflakeStreamingManagedProfileRecoveryContract'
```

Deterministic channel/append/observe/receipt recovery — reopen after uncommitted rows, append-only-proven-missing, terminal-token rejection, complete-unreceipted recovery, receipt conflicts, channel invalidation, schema evolution, TOAST unchanged fields, auth expiry, throttling, oversize rejection, and bounded cleanup — is exercised against an in-memory protocol fake and property/fuzz tests. This implements the modeled protocol profile only. The genuine runtime gap is the absent reviewed high-performance append transport; without it there can be no live delivery evidence, so the profile remains experimental and **fails closed** at admission.
