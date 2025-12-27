#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)

# List pending DDL events

grpcurl -plaintext \
  -import-path "$ROOT_DIR/proto" \
  -proto ductstream/v1/ddl.proto \
  localhost:8080 ductstream.v1.DDLService/ListPendingDDL

# Approve a DDL event by ID (replace 1 with the event id)

grpcurl -plaintext \
  -import-path "$ROOT_DIR/proto" \
  -proto ductstream/v1/ddl.proto \
  -d '{"id": 1}' \
  localhost:8080 ductstream.v1.DDLService/ApproveDDL
