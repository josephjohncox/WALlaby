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
      "give_up_policy":"GIVE_UP_POLICY_ON_RETRY_EXHAUSTION",
      "table_mappings":{"version":2,"destinations":[{"destination":"kafka-out","future_tables":{"action":"MAPPING_ACTION_INCLUDE","target_schema":"{{ .Schema }}","target_table":"{{ .Table }}","future_columns":{"action":"MAPPING_ACTION_INCLUDE","target_column":"{{ .Column }}"},"write":{"mode":"TABLE_WRITE_MODE_APPEND","key_columns":[]}},"tables":[]}]}
    },
    "source": {
      "name": "pg-source",
      "postgres_source": {
          "mode": "POSTGRES_SOURCE_MODE_CDC",
        "connection": {
          "dsn": "postgres://user:pass@localhost:5432/app?sslmode=disable"
        },
        "slot": "wallaby_slot",
        "publication": "wallaby_pub",
        "publication_tables": ["public.events"],
        "batch_size": 500,
        "batch_timeout": "1s",
        "status_interval": "10s",
        "create_slot": true,
        "format": "WIRE_FORMAT_ARROW"
      }
    },
    "destinations": [
      {
        "name": "kafka-out",
        "kafka": {
          "brokers": ["localhost:9092"],
          "topic": "wallaby.cdc",
          "format": "WIRE_FORMAT_ARROW",
          "compression": "COMPRESSION_LZ4",
          "acks": "KAFKA_ACKS_ALL"
        }
      }
    ]
  },
  "start_immediately": true
}
JSON
