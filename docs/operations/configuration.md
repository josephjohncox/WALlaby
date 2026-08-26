# Configuration

WALlaby server and worker commands read strict YAML or JSON plus environment variables. A value from the selected configuration file takes precedence over its environment variable. The file is decoded before environment values are parsed, so a malformed lower-precedence environment value is ignored only when that exact file field is present. Defaults apply last.

## Server configuration

The server searches for `wallaby.yaml` or `wallaby.yml` unless you pass `--config` or set `WALLABY_CONFIG`.

A minimal production file is:

```yaml
environment: production
workflow:
  store: postgres
postgres:
  dsn: postgres://user:pass@postgres:5432/wallaby?sslmode=require
api:
  grpc_listen: :8080
```

Do not commit credentials. Supply the DSN through an environment variable or a secret-mounted configuration file.

## Current configuration-file schema

Configuration files use only the lowercase underscore names below. YAML and JSON are decoded strictly: unknown, duplicate, misspelled, hyphenated, or wrong-typed keys fail with their complete path; multiple YAML documents and trailing JSON documents also fail. Durations are strings such as `30s`, integer fields are YAML/JSON integers, and booleans are YAML/JSON booleans. `profiling.enabled` is the profiling switch and `profiling.listen` is the profiling address.

| Section | Current keys |
| --- | --- |
| root | `environment` |
| `api` | `grpc_listen`, `grpc_reflection` |
| `postgres` | `dsn` |
| `workflow` | `store` |
| `telemetry` | `service_name`, `otlp_endpoint`, `otlp_insecure`, `otlp_protocol`, `metrics_endpoint`, `metrics_insecure`, `metrics_protocol`, `traces_endpoint`, `traces_insecure`, `traces_protocol`, `metrics_exporter`, `traces_exporter`, `metrics_interval` |
| `trace` | `path` |
| `profiling` | `enabled`, `listen` |
| `dbos` | `enabled`, `app_name`, `schedule`, `queue`, `max_empty_reads`, `max_retries` |
| `kubernetes` | `enabled`, `kubeconfig_path`, `context`, `api_server`, `bearer_token`, `ca_file`, `ca_data`, `client_cert_file`, `client_key_file`, `insecure_skip_tls`, `namespace`, `job_image`, `job_image_pull_policy`, `job_service_account`, `job_automount_service_account_token`, `job_name_prefix`, `job_ttl_seconds`, `job_backoff_limit`, `job_max_empty_reads`, `job_labels`, `job_annotations`, `job_command`, `job_args`, `job_env`, `job_env_from` |
| `wire` | `format`, `enforce` |
| `ddl` | `catalog_enabled`, `catalog_interval`, `catalog_schemas`, `auto_approve`, `gate`, `auto_apply` |
| `checkpoints` | `backend`, `dsn`, `path` |
| `artifacts` | `bucket`, `region`, `endpoint`, `access_key`, `secret_key`, `session_token`, `force_path_style`, `hard_retained_bytes`, `backlog_batch_high`, `backlog_bytes_high`, `backlog_age_high`, `backpressure_poll_interval`, `orphan_grace`, `retention`, `gc_interval` |
| `iceberg` | `profile`, `uri`, `warehouse`, `prefix`, `control_table`, `region`, `signing_name`, `expected_aws_role_arn`, `sigv4`, `allow_http`, `oauth_token`, `oauth_credential`, `oauth_scope`, `oauth_uri`, `ca_file`, `ca_data`, `client_cert_file`, `client_key_file`, `server_name`, `s3_endpoint`, `s3_region`, `max_commit_retries`, `request_timeout`, `reconciliation_horizon`, `s3tables_table_bucket_arn`, `s3tables_configure_maintenance`, `s3tables_min_snapshots_to_keep`, `s3tables_max_snapshot_age_hours` |

The server environment uses the documented `WALLABY_*` forms (`WALLABY_ENV`, `WALLABY_GRPC_*`, `WALLABY_POSTGRES_DSN`, `WALLABY_WORKFLOW_STORE`, `WALLABY_OTEL_*`, `WALLABY_TRACE_PATH`, `WALLABY_PPROF_*`, `WALLABY_DBOS_*`, `WALLABY_K8S_*`, `WALLABY_WIRE_*`, `WALLABY_DDL_*`, `WALLABY_CHECKPOINT_*`, `WALLABY_ARTIFACT_*`, and `WALLABY_ICEBERG_*`). Worker deployments use the corresponding documented `WALLABY_WORKER_*` forms. Standard OpenTelemetry variables (`OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_*`, `OTEL_METRICS_EXPORTER`, and `OTEL_TRACES_EXPORTER`) remain current. A selected file overrides environment values; environment values override ordinary defaults. Undocumented environment variables do not configure WALlaby.

Worker command flags also retain their normalized `WALLABY_WORKER_*` bindings, including `WALLABY_WORKER_FLOW_ID`, `WALLABY_WORKER_GENERATION`, `WALLABY_WORKER_EXECUTION_BACKEND`, and `WALLABY_WORKER_MAX_EMPTY_READS`. An explicit command-line flag overrides its environment binding. Strict runtime-file mode disables Viper file decoding and legacy key aliases; it does not disable current flag environment bindings.

## Workflow stores

| Store | Environments | Durability |
| --- | --- | --- |
| `postgres` | all | Durable and required for production |
| `memory` | `dev`, `development`, `test` | Process-local and erased on exit |

The default is `postgres`. Memory mode is rejected outside `dev`, `development`, or `test`, and it cannot be combined with DBOS or Kubernetes dispatch. Production and multi-process deployments must use PostgreSQL so lifecycle intent, execution ownership, and dispatch recovery survive process restarts.

## Checkpoint stores

Checkpoint storage can use PostgreSQL, SQLite, or `none`. SQLite makes checkpoints durable for a local worker, but it does not replace the production workflow store. The control plane still needs PostgreSQL unless the explicit development memory mode is active. Lifecycle reconciliation and stale-execution fencing depend on the workflow store, not the checkpoint backend.

```bash
export WALLABY_CHECKPOINT_BACKEND=sqlite
export WALLABY_CHECKPOINT_PATH="$HOME/.wallaby/checkpoints.db"
```

## Snowflake execution admission

All generic Snowflake, Snowpipe, and managed Snowflake profiles are disabled unless the current server or worker deployment opts in:

```yaml
snowflake:
  enabled: true
  account: xy12345
  user: WALLABY_SERVICE
  host: xy12345.snowflakecomputing.com
  private_key_file: /run/secrets/wallaby/snowflake-key.pem
  private_key_secret_name: wallaby-snowflake # required with Kubernetes dispatch
  private_key_secret_key: private-key.pem
```

The private key file must be an absolute, owner-only, regular PKCS#8 or PKCS#1 RSA PEM file with a key of at least 2048 bits and is loaded before the server or worker can mutate lifecycle state. Flow DSNs carry only the reviewed identity/session allowlist; inline credentials, unknown driver controls, custom hosts/proxies/TLS settings, logging/diagnostic controls, repeated aliases, and encoded variants are rejected. Parsed account, user, canonical host, HTTPS port, JWT authentication, and fail-closed OCSP must match the deployment policy. The equivalent server/worker variable families are `WALLABY_SNOWFLAKE_*` and `WALLABY_WORKER_SNOWFLAKE_*` for `ENABLED`, `ACCOUNT`, `USER`, `HOST`, and `PRIVATE_KEY_FILE`; Kubernetes secret-name/key settings are server-only. The Helm chart mounts the same Secret at mode `0400` into the server and dynamically dispatched Jobs. Standalone worker deployments must mount that file themselves and supply the authoritative worker environment.

Disabling the gate prevents create/update/reconfigure, start/resume/run-once, lifecycle redispatch, DBOS recovery, and newly launched worker execution for existing Snowflake-backed flows. It does not revoke an already running network connection inside an unchanged process; stop or terminate those workers and roll the deployment when changing this policy. Offline CLI planning validates only structural and credential-safe syntax. Online `flow plan --endpoint ...` calls the server's policy-aware `ValidateFlow` RPC before diffing and fails when the current deployment does not admit the flow.

## Canonical artifact publication

`ack_policy=materialized` requires an ordinary versioned S3 bucket and the PostgreSQL workflow/checkpoint store. Current mapped Iceberg flows carry `materialization.projection_id=canonical_cdc_parquet_v2`; the durable destination mapping supplies logical target identity, while credentials and operational limits stay in worker deployment configuration. The v1 encoder remains frozen for historical artifacts.

```yaml
artifacts:
  bucket: wallaby-canonical
  region: us-east-1
  endpoint: ""                 # optional MinIO/S3-compatible endpoint
  force_path_style: false
  hard_retained_bytes: 68719476736
  backlog_batch_high: 10000
  backlog_bytes_high: 34359738368
  backlog_age_high: 24h
  backpressure_poll_interval: 1s
  orphan_grace: 1h
  retention: 168h
  metadata_retention: 168h
  metadata_max_publications: 100
  metadata_max_rows: 1000
  gc_interval: 1m
```

Credentials use the AWS default chain unless `artifacts.access_key`, `artifacts.secret_key`, and optional `artifacts.session_token` are supplied by a secret. Every key also has `WALLABY_ARTIFACT_*` and `WALLABY_WORKER_ARTIFACT_*` environment forms, including `WALLABY_ARTIFACT_METADATA_RETENTION`, `WALLABY_ARTIFACT_METADATA_MAX_PUBLICATIONS`, and `WALLABY_ARTIFACT_METADATA_MAX_ROWS` (and their `WALLABY_WORKER_` equivalents).

The projection, retained-byte limit, and backlog count/byte/age thresholds are durable stream admission; changing them for an existing flow incarnation fails closed. If the flow destination is Iceberg, the production worker records the exact `destination_revision_id` in the durable consumer fingerprint and creates delivery rows for that revision. Other materialized destinations record an empty consumer fingerprint. The poll and GC intervals are worker cadence. `orphan_grace`, `retention`, and `metadata_retention` are runtime collection policy. The metadata horizon starts only after all roots, deliveries, receipts, claims, and source/consumer checkpoint dependencies become terminal. Shortening a horizon can make already-eligible evidence permanently collectible sooner and must be a deliberate retention-policy change.

### Iceberg catalog client

Iceberg OAuth, TLS, timeout, and S3 Tables maintenance settings belong to the worker deployment:

```yaml
iceberg:
  profile: rest
  uri: https://catalog.example.com
  warehouse: warehouse
  prefix: ""
  control_table: __wallaby_control
  request_timeout: 30s
  max_commit_retries: 4
  reconciliation_horizon: 24h
  # oauth_token: supplied by a secret
  # oauth_credential: supplied by a secret
  # ca_file: /var/run/secrets/catalog-ca.pem
  # client_cert_file: /var/run/secrets/catalog-client.pem
  # client_key_file: /var/run/secrets/catalog-client-key.pem
```

Use the `WALLABY_ICEBERG_*` or `WALLABY_WORKER_ICEBERG_*` environment forms for deployment secrets. S3 Tables sets `profile: s3tables` and also uses `region`, `s3tables_table_bucket_arn`, `s3tables_configure_maintenance`, `s3tables_min_snapshots_to_keep`, and `s3tables_max_snapshot_age_hours`. Local S3-compatible REST catalogs may use deployment `s3_endpoint` and `s3_region`. AWS authentication uses the default SDK chain, including IRSA and assumed roles.

Catalog identity is deployment-bound. A persisted flow may select only `catalog_profile`, `control_table`, and `destination_revision_id`; logical namespace and table identity come from its mandatory mapping. Catalog URI, warehouse, REST prefix, region, table-bucket ARN, expected AWS role ARN, S3 endpoint/region, behavior controls, and every unknown or secret option are rejected before storage. OAuth, mTLS, ambient AWS, and static AWS credentials therefore cannot be redirected by a flow definition. The S3 Tables profile requires `iceberg.expected_aws_role_arn`; startup calls STS and fails before catalog recovery unless the active caller is that role. For the complete S3 Tables configuration and read-only Snowflake catalog link, see [Query WALlaby Iceberg tables from Snowflake](../guides/s3-tables-snowflake.md).

### Artifact schema upgrade

The monotonic `artifactlog/004_materialized_publication.sql` migration backfills artifact identities and publication sequences, validates new constraints, and creates unique indexes inside the shared migration transaction. `artifactlog/005_iceberg_consumer_receipts.sql` adds deterministic catalog commit identity, multi-snapshot receipts, and monotonic consumer checkpoints. These are not zero-lock rolling migrations for a large pre-existing artifact history. Before upgrading a database that already contains artifact rows:

1. pause artifact-producing workers and catalog consumers;
2. take and verify a PostgreSQL backup;
3. inspect artifact table sizes and ensure no long transactions hold conflicting locks;
4. run exactly one migrator during a maintenance window while monitoring `pg_stat_activity` and `pg_locks`; and
5. restart workers only after the shared migration ledger and managed-schema verification pass.

The coordinator intentionally serializes migrations with an advisory lock and does not impose a statement timeout. Do not deploy mixed binaries or use a rolling worker restart across this migration.

## Managed Snowflake SQL

The typed `snowflake_postgres_sql` branch stores its account, object names and creation identities, owner and execution roles, destination revision, session contract, transaction bounds, and configured Snowflake runtime pin. The flow ID and projected schema contract are derived from the typed flow and table mapping, not authored as endpoint fields. Mount the key-pair private key as a secret and reference it from `dsn`; do not store key material in the flow document. PostgreSQL remains authoritative for generations, attempts, checkpoints, delivery receipts, and source acknowledgements.

Changing the flow binding, target or receipt table, account, schema, role, warehouse, service-version pin, object creation identity, timeout, bound, or schema contract requires a new destination revision. Managed `UpdateFlow` and `ReconfigureFlow` are rejected for every change. Stop the old flow, create/validate/start a replacement with a new flow ID, cut over, and delete the old flow only when safe. Provision a new object pair and update `destination_revision_id`, creation identities, ownership comments, and the destination-scoped table mapping together. Do not reuse a destination revision for different configuration. Every Terraform managed update fails; Terraform does not perform this lifecycle.

See [Snowflake destination](../connectors/snowflake.md) for the exact typed fields, table DDL, and opt-in real-service gate.

## Managed Snowflake staged COPY

The typed `snowflake_postgres_staged` branch names `stage`, `file_format`, and their creation identities (`stage_created_on`, `file_format_created_on`), and writes into the `table` append changelog plus `receipts_table`. The branch and mandatory one-table append mapping select the protocol. Optional native boolean `auto_ingest: true` requires `pipe` and `pipe_created_on`. Load verification uses integer `load_verify_attempts` and `load_verify_interval_millis`; bounded stage cleanup uses integer `cleanup_max_objects` and `cleanup_retention_seconds`. The DSN must set `READ_LATEST_WRITES=true` and `TIMEZONE=UTC`, carry no inline secret, and use key-pair JWT over verified HTTPS with OCSP fail-closed. Changing any object, identity, bound, or schema contract requires a new destination revision. PostgreSQL remains authoritative for generations, attempts, checkpoints, delivery receipts, and source acknowledgements; the profile never claims exactly-once.

## Managed Snowflake Snowpipe Streaming REST append

The typed `snowflake_postgres_streaming` branch names `pipe`, `table`, `receipts_table`, `channel_state_table`, optional `channel_name_prefix`, their corresponding creation-identity fields, and mandatory `transport`. The branch and mandatory one-table append mapping select the protocol. Append retries use integer `append_attempts` and `append_backoff_millis`; SQL-observed completeness polling uses `observe_attempts` and `observe_interval_millis`; `max_row_bytes` bounds each row; channel-state cleanup uses `cleanup_max_objects` and `cleanup_retention_seconds`. The DSN must set `READ_LATEST_WRITES=true` and `TIMEZONE=UTC`, carry no inline secret, and use key-pair JWT over verified HTTPS with OCSP fail-closed; the source must set `toast_fetch: "TOAST_FETCH_MODE_OFF"`. Because no reviewed high-performance append transport is linked, admission **fails closed** with a transport-unavailable error rather than proving delivery from local continuation/offset tokens. Continuation, request, and committed-offset tokens are persisted as evidence but are never sufficient deduplication proof; deterministic `ROW_HASH` identity plus SQL-observed completeness plus a durable receipt gate every PostgreSQL adoption. Changing any object, identity, bound, or schema contract requires a new destination revision. PostgreSQL remains authoritative for generations, attempts, checkpoints, delivery receipts, and source acknowledgements; the profile never claims exactly-once.

## Command-specific files

- `wallaby-admin` reads `wallaby-admin.yaml` or `$HOME/.config/wallaby/wallaby-admin.yaml` and honors `WALLABY_ADMIN_CONFIG`.
- `wallaby-worker` reads `wallaby-worker.yaml` and honors `WALLABY_WORKER_CONFIG`.
- Validation tools expose command-specific environment variables documented in the [configuration reference](../usage.md).
