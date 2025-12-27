# Tutorials

These tutorials are designed to be copied and adapted. If you change APIs or connector options, update the steps here.

## Tutorial 1: Postgres → Kafka (Arrow)

### 1) Prepare Postgres
```sql
CREATE PUBLICATION ductstream_pub FOR ALL TABLES;
```

Create a logical replication slot (optional if you let DuctStream create it):
```sql
SELECT * FROM pg_create_logical_replication_slot('ductstream_slot', 'pgoutput');
```

### 2) Run the API Server
```bash
export DUCTSTREAM_POSTGRES_DSN="postgres://user:pass@localhost:5432/ductstream?sslmode=disable"
./bin/ductstream
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
  -proto ductstream/v1/flow.proto \
  -proto ductstream/v1/types.proto \
  -d @ \
  localhost:8080 ductstream.v1.FlowService/CreateFlow <<'JSON'
{
  "flow": {
    "name": "pg_to_s3",
    "wire_format": "WIRE_FORMAT_PARQUET",
    "source": {
      "name": "pg-source",
      "type": "ENDPOINT_TYPE_POSTGRES",
      "options": {
        "dsn": "postgres://user:pass@localhost:5432/app?sslmode=disable",
        "slot": "ductstream_slot",
        "publication": "ductstream_pub",
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
          "bucket": "my-ductstream-bucket",
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

## Tutorial 3: DDL Gating + Approval

### 1) Enable gating
```bash
export DUCTSTREAM_DDL_GATE="true"
export DUCTSTREAM_DDL_AUTO_APPROVE="false"
export DUCTSTREAM_DDL_AUTO_APPLY="false"
```

### 2) Run a schema change in Postgres
Apply a DDL change (e.g., `ALTER TABLE ... ADD COLUMN`).

### 3) Approve the DDL
```bash
./bin/ductstream-admin ddl list -status pending
./bin/ductstream-admin ddl approve -id <id>
./bin/ductstream-admin ddl apply -id <id>
```

## Tutorial 4: Worker Mode + DBOS Scheduling

### 1) Start DBOS scheduling
```bash
export DUCTSTREAM_DBOS_ENABLED="true"
export DUCTSTREAM_DBOS_APP="ductstream"
export DUCTSTREAM_DBOS_SCHEDULE="*/30 * * * * *"
./bin/ductstream
```

### 2) Or run a worker directly
```bash
./bin/ductstream-worker -flow-id "<flow-id>" -max-empty-reads 1
```
