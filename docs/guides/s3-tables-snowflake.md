# Query WALlaby Iceberg tables from Snowflake

This workflow publishes PostgreSQL CDC as canonical Parquet, commits derived Iceberg snapshots to Amazon S3 Tables, and exposes those tables to Snowflake through a read-only AWS Glue Iceberg REST catalog integration.

Snowflake queries the Iceberg data in place. It does not copy Parquet through a stage, and it is not WALlaby's delivery or checkpoint authority.

## Support boundary

The configuration is a documented WALlaby materialized-destination preview with these semantics:

- PostgreSQL owns source progress, publication identity, delivery attempts, catalog receipts, consumer checkpoints, quotas, and retention roots.
- An ordinary versioned S3 bucket stores immutable canonical recovery objects.
- Amazon S3 Tables owns the derived Iceberg metadata and managed data files.
- Snowflake is a read-only external catalog consumer.
- Source acknowledgement occurs after PostgreSQL commits canonical publication, before asynchronous Iceberg and Snowflake visibility.
- Tables are append-only CDC changelogs. Updates and deletes are rows identified by `__op`; they do not mutate older rows.
- Delivery is not exactly-once, and a multi-table source transaction is not atomically visible across all Iceberg tables.
- Initial snapshot publication is not yet admitted. Start from a provisioned logical slot/publication and an agreed streaming cut with `bootstrap=never`.

The connector remains classified experimental until the credential-gated AWS S3 Tables and commercial Snowflake readback gates pass on the same commit. The configuration, security boundary, examples, and local REST compatibility gate are supported and versioned now; do not describe an unrun deployment as maintained evidence.

## Architecture

```text
PostgreSQL logical replication
        |
        v
PostgreSQL artifact authority ----> ordinary versioned S3
        |                              immutable canonical Parquet
        v
asynchronous Iceberg consumer ----> Amazon S3 Tables / Glue Iceberg REST
                                             |
                                             v
                                  Snowflake catalog-linked database
                                       read-only, zero-copy query
```

The ordinary S3 artifact bucket and the S3 Tables table bucket must be different resources. S3 Tables maintenance may compact or expire catalog-managed files; it must never manage canonical artifact versions.

## 1. Provision the source cut

Create the PostgreSQL publication and logical slot under the deployment's fenced source-resource procedure. Record:

- PostgreSQL system identifier;
- stable source lineage identifier;
- exact publication fingerprint;
- slot consistent point; and
- destination revision ID.

The flow rejects resource mutation in this streaming-only configuration. Set `bootstrap`, `create_slot`, `ensure_state`, `ensure_publication`, and `sync_publication` exactly as shown in [`examples/flows/postgres_to_iceberg_s3tables.json`](https://github.com/josephjohncox/WALlaby/blob/main/examples/flows/postgres_to_iceberg_s3tables.json).

## 2. Configure canonical artifacts and S3 Tables

Use [`examples/config/postgres_to_iceberg_s3tables.worker.yaml`](https://github.com/josephjohncox/WALlaby/blob/main/examples/config/postgres_to_iceberg_s3tables.worker.yaml) as the worker configuration.

Required deployment settings are:

```yaml
artifacts:
  bucket: wallaby-canonical-artifacts  # versioning must be enabled
  region: us-east-1

iceberg:
  profile: s3tables
  region: us-east-1
  expected_aws_role_arn: arn:aws:iam::123456789012:role/wallaby-iceberg-writer
  warehouse: "123456789012:s3tablescatalog/wallaby-lake"
  s3tables_table_bucket_arn: arn:aws:s3tables:us-east-1:123456789012:bucket/wallaby-lake
  reconciliation_horizon: 24h
  s3tables_configure_maintenance: true
  s3tables_min_snapshots_to_keep: 100
  s3tables_max_snapshot_age_hours: 168
```

WALlaby derives `https://glue.<region>.amazonaws.com/iceberg` and signs it for the `glue` service. Production S3 Tables configuration rejects any other catalog URI. Catalog URI, warehouse, REST prefix, region, table-bucket ARN, S3 FileIO endpoint, and S3 region are deployment-only. Persisted flows containing any of those options are rejected before storage, so ambient AWS, OAuth, or mTLS credentials cannot be redirected by a flow definition.

Use the AWS default credential chain, IRSA, or an assumed role. Set `expected_aws_role_arn` to the writer role: startup resolves `sts:GetCallerIdentity` and fails before catalog recovery when the active identity differs. Never put OAuth tokens, client keys, AWS access keys, or secret keys in endpoint options. WALlaby rejects deployment-owned, unknown, and secret-bearing Iceberg options before persistence.

Snapshot retention must exceed the reconciliation horizon. A failed S3 Tables maintenance job blocks catalog delivery instead of allowing the consumer to lose recovery evidence.

## 3. Create the WALlaby flow

The persisted destination contains target mapping and immutable revision identity only:

```yaml
destinations:
  - name: s3tables-lake
    type: iceberg
    options:
      catalog_profile: s3tables
      destination_revision_id: s3tables-lake-v1
      control_table: __wallaby_control

config:
  table_mappings:
    version: 2
    destinations:
      - destination: s3tables-lake
        future_tables:
          action: exclude
        tables:
          - source_schema: public
            source_table: events
            action: include
            target_schema: wallaby
            target_table: cdc_events
            future_columns:
              action: include
              target_column: "{{ .Column }}"
            columns: []
            write:
              mode: append
              key_columns: []
  ack_policy: materialized
  materialization:
    projection_id: canonical_cdc_parquet_v2
```

Materialized admission requires exactly one Iceberg destination revision. Changing the consumer revision, effective deployment catalog identity, or target mapping for a live incarnation fails closed. Managed `UpdateFlow` and `ReconfigureFlow` are both rejected, including name and parallelism changes. Stop the old flow, create/validate/start a replacement with a new flow ID and destination revision, cut over, and delete the old flow only when safe. Use `wallaby-admin flow mappings generate` for catalog-derived authoring, then review and validate the complete flow. Every Terraform update fails; Terraform cannot perform this lifecycle.

Each Iceberg table uses the already-mapped namespace, table, and selected columns encoded in the v2 canonical publication. Iceberg never reapplies logical target prefixes or qualification. Mappings must remain injective and append-only.

## 4. Integrate S3 Tables with AWS Glue

Enable the regional S3 Tables integration with AWS analytics services so the table bucket appears under the Glue `s3tablescatalog` hierarchy. Configure Lake Formation if your account uses it.

Keep three permission sets separate:

1. **WALlaby canonical artifact role.** Scope it to the ordinary versioned artifact bucket. It needs bucket-versioning inspection and exact-version object put/get/delete permissions used by publication and garbage collection. It must not manage the S3 Tables bucket.
2. **WALlaby Iceberg writer role.** Scope Glue REST and S3 Tables permissions to the intended regional nested catalog, namespace, and table bucket. The implementation calls the equivalent of Glue database/table load/create/update/commit APIs and S3 Tables `GetTable`, `GetTableMaintenanceConfiguration`, `GetTableMaintenanceJobStatus`, and—when `s3tables_configure_maintenance=true`—`PutTableMaintenanceConfiguration`. Grant `lakeformation:GetDataAccess` and matching Lake Formation data permissions when credential vending is governed by Lake Formation. Use the AWS default chain/IRSA/assumed role; do not persist keys in the flow.
3. **Snowflake read role.** This role discovers the catalog and receives temporary read credentials. It must not have create/update/drop authority.

The Snowflake read role needs the catalog-discovery permissions documented by Snowflake, including:

- `glue:GetCatalog`;
- `glue:GetDatabase` and `glue:GetDatabases`;
- `glue:GetTable` and `glue:GetTables`; and
- `lakeformation:GetDataAccess` plus matching Lake Formation data grants when Lake Formation governs the catalog.

Scope resources to the intended account, regional nested catalog, namespace, and tables. Snowflake uses catalog-vended temporary credentials for table data; do not configure the S3 Tables path as an ordinary `s3://` external volume.

Official references:

- [AWS S3 Tables integration overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integration-overview.html)
- [AWS S3 Tables through Glue Iceberg REST](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-glue-endpoint.html)
- [Snowflake catalog-vended credentials, including the Amazon S3 Tables example](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-vended-credentials)
- [Snowflake AWS Glue Iceberg REST setup](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-rest-glue)

## 5. Create the Snowflake read-only catalog link

Run [`examples/sql/snowflake_s3tables_catalog.sql`](https://github.com/josephjohncox/WALlaby/blob/main/examples/sql/snowflake_s3tables_catalog.sql) after replacing its placeholders.

The load-bearing settings are:

- `CATALOG_SOURCE = ICEBERG_REST`;
- Glue URI `https://glue.<region>.amazonaws.com/iceberg`;
- `CATALOG_API_TYPE = AWS_GLUE`;
- the exact Glue REST catalog name exposed by the account's S3 Tables integration; Snowflake's current S3 Tables-specific example uses `<account>:S3tablescatalog/<table-bucket>`, while generic Glue REST integrations use only the account ID, so verify the value through AWS rather than inferring it;
- `ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS`;
- explicit SigV4 role and signing region; and
- `ALLOWED_WRITE_OPERATIONS = NONE` on the catalog-linked database.

`ALLOWED_WRITE_OPERATIONS` otherwise defaults to writable behavior. Snowflake must remain an observer because WALlaby/PostgreSQL own catalog delivery and cleanup.

After `CREATE CATALOG INTEGRATION`, run `DESCRIBE CATALOG INTEGRATION` and copy `GLUE_AWS_IAM_USER_ARN` and `GLUE_AWS_EXTERNAL_ID` into the IAM role trust policy. Replacing the integration can generate a new external ID and break trust.

Validate discovery with:

```sql
SELECT SYSTEM$GET_CATALOG_LINKED_DATABASE_CONFIG('WALLABY_S3TABLES');
SELECT SYSTEM$CATALOG_LINK_STATUS('WALLABY_S3TABLES');
SHOW ICEBERG TABLES IN DATABASE WALLABY_S3TABLES;
```

Inspect `auto_refresh_status` for tables that were discovered but could not initialize. Catalog-link synchronization and table refresh are asynchronous; the example uses 30-second polling intervals.

For promotion evidence, export the AWS/Snowflake variables consumed by the example and run `just test-s3tables-snowflake-live`. The test creates a unique S3 Tables Iceberg table, commits canonical changelog rows, waits for the catalog-linked database to discover it, records `CURRENT_VERSION()`, and verifies exact logical-batch and row counts through Snowflake. A skipped or unrun credential cell is not evidence.

## 6. Query the changelog

Order records inside one source transaction by logical batch and global record ordinal:

```sql
SELECT
  __wallaby_logical_batch_id,
  __wallaby_record_ordinal,
  __wallaby_source_position,
  __wallaby_unchanged,
  __op,
  *
FROM WALLABY_S3TABLES.WALLABY.CDC_WIDGETS
ORDER BY __wallaby_logical_batch_id, __wallaby_record_ordinal;
```

`__wallaby_unchanged` identifies PostgreSQL columns omitted because the source sent unchanged TOAST. Consumers building current state must merge changelog rows using the source key and operation semantics. WALlaby does not supply a Snowflake current-state view in this profile.

For a transaction touching several source tables, PostgreSQL records one publication, but each Iceberg table receives an independent snapshot. Only the PostgreSQL-authoritative artifact delivery receipt and consumer checkpoint written after the complete `CommitResult` prove that every projection group completed. The control table contains barriers committed before data and is not a completion marker. Snowflake alone cannot prove atomic cross-table completeness; do not infer it from the first visible table.

## 7. Operations and recovery

Monitor:

- PostgreSQL artifact backlog count, bytes, and age;
- indeterminate catalog outcomes;
- S3 Tables maintenance failures and retention horizon;
- `SYSTEM$CATALOG_LINK_STATUS`;
- Snowflake `auto_refresh_status`; and
- the lag between PostgreSQL publication, Iceberg consumer checkpoint, and Snowflake refresh.

An ambiguous catalog commit is reconciled by exact publication, manifest, schema, projection-group, and commit identity. A conflicting snapshot halts the consumer and pins canonical artifacts. Operators must resolve the conflict; the source must not skip it.

Snowflake cannot drop an Amazon S3 Table using the purge semantics S3 Tables requires. Teardown is therefore:

1. stop and fence the WALlaby flow;
2. let the Iceberg consumer reach its checkpoint or explicitly resolve pending attempts;
3. drop only the Snowflake catalog-linked database and catalog integration;
4. remove Snowflake IAM/Lake Formation access;
5. retire S3 Tables objects with AWS APIs under fenced ownership; and
6. release canonical artifact roots only through PostgreSQL-authoritative retention and garbage collection.

## What this is not

- **Not Snowpipe or `COPY INTO`.** Those copy files into Snowflake and do not consume Iceberg snapshots.
- **Not a Snowflake external Parquet table.** External tables do not preserve Iceberg catalog transactions, manifests, field IDs, or snapshot visibility.
- **Not the direct Snowflake SQL destination.** That is a separate managed profile and protocol.
- **Not exactly-once or current-state replication.** This is asynchronous, replay-convergent changelog materialization with PostgreSQL authority.
