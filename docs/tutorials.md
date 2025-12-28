# Tutorials

These tutorials are designed to be copied and adapted. If you change APIs or connector options, update the steps here.

## Tutorial 1: Postgres → Kafka (Arrow)

### 1) Prepare Postgres
```sql
CREATE PUBLICATION wallaby_pub FOR ALL TABLES;
```

Create a logical replication slot (optional if you let WALlaby create it):
```sql
SELECT * FROM pg_create_logical_replication_slot('wallaby_slot', 'pgoutput');
```

### 2) Run the API Server
```bash
export WALLABY_POSTGRES_DSN="postgres://user:pass@localhost:5432/wallaby?sslmode=disable"
./bin/wallaby
```

### 3) Create the Flow
```bash
examples/grpc/create_flow.sh
```

### 4) Verify Kafka Output
Consume the topic with your preferred tool. Each message includes headers for schema name and wire format.

## Tutorial 2: Postgres → S3 (Parquet)

### 1) Create a Flow Spec
Use `examples/flows/postgres_to_s3_parquet.json` as a baseline.

### 2) Create the Flow via gRPC
```bash
grpcurl -plaintext \
  -import-path ./proto \
  -proto wallaby/v1/flow.proto \
  -proto wallaby/v1/types.proto \
  -d @ \
  localhost:8080 wallaby.v1.FlowService/CreateFlow <<'JSON'
{
  "flow": {
    "name": "pg_to_s3",
    "wire_format": "WIRE_FORMAT_PARQUET",
    "source": {
      "name": "pg-source",
      "type": "ENDPOINT_TYPE_POSTGRES",
      "options": {
        "dsn": "postgres://user:pass@localhost:5432/app?sslmode=disable",
        "slot": "wallaby_slot",
        "publication": "wallaby_pub",
        "batch_size": "1000",
        "batch_timeout": "2s",
        "format": "parquet"
      }
    },
    "destinations": [
      {
        "name": "s3-out",
        "type": "ENDPOINT_TYPE_S3",
        "options": {
          "bucket": "my-wallaby-bucket",
          "prefix": "cdc/",
          "region": "us-east-1",
          "format": "parquet",
          "compression": "gzip"
        }
      }
    ]
  },
  "start_immediately": true
}
JSON
```

For full‑row webhooks (rehydrating TOASTed columns), see `examples/flows/postgres_to_http_toast_full.json` and set `"toast_fetch": "full"` on the Postgres source.

## Tutorial 3: Postgres → HTTP Webhook

Use a webhook endpoint to trigger downstream automation:

```bash
grpcurl -plaintext \
  -import-path ./proto \
  -proto wallaby/v1/flow.proto \
  -proto wallaby/v1/types.proto \
  -d @ \
  localhost:8080 wallaby.v1.FlowService/CreateFlow <<'JSON'
{
  "flow": {
    "name": "pg_to_webhook",
    "wire_format": "WIRE_FORMAT_JSON",
    "source": {
      "name": "pg-source",
      "type": "ENDPOINT_TYPE_POSTGRES",
      "options": {
        "dsn": "postgres://user:pass@localhost:5432/app?sslmode=disable",
        "slot": "wallaby_slot",
        "publication": "wallaby_pub",
        "format": "json"
      }
    },
    "destinations": [
      {
        "name": "webhook",
        "type": "ENDPOINT_TYPE_HTTP",
        "options": {
          "url": "https://api.example.com/ingest",
          "method": "POST",
          "format": "json",
          "headers": "Authorization:Bearer token123",
          "max_retries": "5",
          "backoff_base": "200ms",
          "backoff_max": "5s"
        }
      }
    ]
  },
  "start_immediately": true
}
JSON
```

## Tutorial 4: Postgres → Postgres Stream

```bash
grpcurl -plaintext \
  -import-path ./proto \
  -proto wallaby/v1/flow.proto \
  -proto wallaby/v1/types.proto \
  -d @ \
  localhost:8080 wallaby.v1.FlowService/CreateFlow <<'JSON'
{
  "flow": {
    "name": "pg_to_pgstream",
    "wire_format": "WIRE_FORMAT_JSON",
    "source": {
      "name": "pg-source",
      "type": "ENDPOINT_TYPE_POSTGRES",
      "options": {
        "dsn": "postgres://user:pass@localhost:5432/app?sslmode=disable",
        "slot": "wallaby_slot",
        "publication": "wallaby_pub",
        "format": "json"
      }
    },
    "destinations": [
      {
        "name": "stream-out",
        "type": "ENDPOINT_TYPE_PGSTREAM",
        "options": {
          "dsn": "postgres://user:pass@localhost:5432/wallaby?sslmode=disable",
          "stream": "orders",
          "format": "json"
        }
      }
    ]
  },
  "start_immediately": true
}
JSON
```

Consume the stream:

```bash
grpcurl -plaintext \
  -import-path ./proto \
  -proto wallaby/v1/stream.proto \
  -d '{"stream":"orders","consumer_group":"search","max_messages":5,"visibility_timeout_seconds":30}' \
  localhost:8080 wallaby.v1.StreamService/Pull
```

## Tutorial 5: DDL Gating + Approval

### 1) Enable gating
```bash
export WALLABY_DDL_GATE="true"
export WALLABY_DDL_AUTO_APPROVE="false"
export WALLABY_DDL_AUTO_APPLY="false"
```

### 2) Run a schema change in Postgres
Apply a DDL change (e.g., `ALTER TABLE ... ADD COLUMN`).

### 3) Approve the DDL
```bash
./bin/wallaby-admin ddl list -status pending
./bin/wallaby-admin ddl approve -id <id>
./bin/wallaby-admin ddl apply -id <id>
```

## Tutorial 6: Worker Mode + DBOS Scheduling

### 1) Start DBOS scheduling
```bash
export WALLABY_DBOS_ENABLED="true"
export WALLABY_DBOS_APP="wallaby"
export WALLABY_DBOS_SCHEDULE="*/30 * * * * *"
./bin/wallaby
```

### 2) Or run a worker directly
```bash
./bin/wallaby-worker -flow-id "<flow-id>" -max-empty-reads 1
```
