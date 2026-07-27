# Configuration

WALlaby commands read YAML plus environment variables. A value from the selected configuration file takes precedence over its environment variable. Defaults apply last.

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

## Canonical artifact publication

`ack_policy=materialized` requires an ordinary versioned S3 bucket and the PostgreSQL workflow/checkpoint store. The flow carries only `materialization.projection_id=canonical_cdc_parquet_v1`; credentials and operational limits stay in worker deployment configuration.

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
  gc_interval: 1m
```

Credentials use the AWS default chain unless `artifacts.access_key`, `artifacts.secret_key`, and optional `artifacts.session_token` are supplied by a secret. Every key also has `WALLABY_ARTIFACT_*` and `WALLABY_WORKER_ARTIFACT_*` environment forms, for example `WALLABY_ARTIFACT_BUCKET`, `WALLABY_ARTIFACT_BACKLOG_BYTES_HIGH`, and `WALLABY_ARTIFACT_RETENTION`.

The projection, retained-byte limit, and backlog count/byte/age thresholds are durable stream admission; changing them for an existing flow incarnation fails closed. If the flow destination is Iceberg, the production worker records the exact `destination_revision_id` in the durable consumer fingerprint and creates delivery rows for that revision. Other materialized destinations record an empty consumer fingerprint. The poll and GC intervals are worker cadence. `orphan_grace` and `retention` are runtime collection policy, so shortening either can make an already-eligible object collectible sooner and must be rolled out as a deliberate retention-policy change.

### Iceberg catalog client

Iceberg OAuth, TLS, timeout, and S3 Tables maintenance settings belong to the worker deployment:

```yaml
iceberg:
  uri: https://catalog.example.com
  warehouse: warehouse
  prefix: ""
  namespace: analytics
  table_prefix: cdc_
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

Use the `WALLABY_ICEBERG_*` or `WALLABY_WORKER_ICEBERG_*` environment forms for deployment secrets. S3 Tables also uses `region`, `s3tables_table_bucket_arn`, `s3tables_configure_maintenance`, `s3tables_min_snapshots_to_keep`, and `s3tables_max_snapshot_age_hours`. AWS authentication uses the default SDK chain, including IRSA and assumed roles. The flow endpoint may override only non-secret URI, warehouse, prefix, namespace, table mapping, region, and table-bucket ARN values.

### Artifact schema upgrade

The monotonic `artifactlog/004_materialized_publication.sql` migration backfills artifact identities and publication sequences, validates new constraints, and creates unique indexes inside the shared migration transaction. `artifactlog/005_iceberg_consumer_receipts.sql` adds deterministic catalog commit identity, multi-snapshot receipts, and monotonic consumer checkpoints. These are not zero-lock rolling migrations for a large pre-existing artifact history. Before upgrading a database that already contains artifact rows:

1. pause artifact-producing workers and catalog consumers;
2. take and verify a PostgreSQL backup;
3. inspect artifact table sizes and ensure no long transactions hold conflicting locks;
4. run exactly one migrator during a maintenance window while monitoring `pg_stat_activity` and `pg_locks`; and
5. restart workers only after the shared migration ledger and managed-schema verification pass.

The coordinator intentionally serializes migrations with an advisory lock and does not impose a statement timeout. Do not deploy mixed binaries or use a rolling worker restart across this migration.

## Command-specific files

- `wallaby-admin` reads `wallaby-admin.yaml` or `$HOME/.config/wallaby/wallaby-admin.yaml` and honors `WALLABY_ADMIN_CONFIG`.
- `wallaby-worker` reads `wallaby-worker.yaml` and honors `WALLABY_WORKER_CONFIG`.
- Validation tools expose command-specific environment variables documented in the [configuration reference](../usage.md).
