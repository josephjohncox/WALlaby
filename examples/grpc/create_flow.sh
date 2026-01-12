#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)

# Create a flow via gRPC. Update this payload when Flow/Endpoint schemas change.

grpcurl -plaintext \
  -import-path "$ROOT_DIR/proto" \
  -proto wallaby/v1/flow.proto \
  -proto wallaby/v1/types.proto \
  -d @ \
  localhost:8080 wallaby.v1.FlowService/CreateFlow <<'JSON'
{
  "flow": {
    "name": "pg_to_kafka",
    "wire_format": "WIRE_FORMAT_ARROW",
    "config": {
      "ack_policy": "ACK_POLICY_PRIMARY",
      "primary_destination": "kafka-out",
      "failure_mode": "FAILURE_MODE_HOLD_SLOT",
      "give_up_policy": "GIVE_UP_POLICY_ON_RETRY_EXHAUSTION"
    },
    "source": {
      "name": "pg-source",
      "type": "ENDPOINT_TYPE_POSTGRES",
      "options": {
        "dsn": "postgres://user:pass@localhost:5432/app?sslmode=disable",
        "slot": "wallaby_slot",
        "publication": "wallaby_pub",
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
          "topic": "wallaby.cdc",
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
