#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)

# Create a flow via gRPC. Update this payload when Flow/Endpoint schemas change.

grpcurl -plaintext \
  -import-path "$ROOT_DIR/proto" \
  -proto ductstream/v1/flow.proto \
  -proto ductstream/v1/types.proto \
  -d @ \
  localhost:8080 ductstream.v1.FlowService/CreateFlow <<'JSON'
{
  "flow": {
    "name": "pg_to_kafka",
    "wire_format": "WIRE_FORMAT_ARROW",
    "source": {
      "name": "pg-source",
      "type": "ENDPOINT_TYPE_POSTGRES",
      "options": {
        "dsn": "postgres://user:pass@localhost:5432/app?sslmode=disable",
        "slot": "ductstream_slot",
        "publication": "ductstream_pub",
        "batch_size": "500",
        "batch_timeout": "1s",
        "status_interval": "10s",
        "create_slot": "true",
        "format": "arrow"
      }
    },
    "destinations": [
      {
        "name": "kafka-out",
        "type": "ENDPOINT_TYPE_KAFKA",
        "options": {
          "brokers": "localhost:9092",
          "topic": "ductstream.cdc",
          "format": "arrow",
          "compression": "lz4",
          "acks": "all"
        }
      }
    ]
  },
  "start_immediately": true
}
JSON
