# Terraform Provider

This folder contains the Terraform provider source and acceptance test harness.

## Example Configuration
See `terraform/examples/basic/main.tf` for a minimal flow definition. Every resource supplies expanded destination-scoped `table_mappings`; there is no persisted local mapping-file attribute. For unmanaged flows, mapping, wire-format, source, and destination changes use controlled pause/reconfigure/resume; unmanaged name-only and parallelism-only changes use the ordinary update RPC. Managed flows reject both `UpdateFlow` and `ReconfigureFlow`, so every managed change—including name and parallelism—fails apply with diagnostics and retains the existing resource state. Terraform does not implement managed cutover. Stop the old flow, create and validate a distinct resource with a new flow ID and immutable destination/publication revision identities, start and verify it, cut over, and delete the old resource only when safe.

## Running the Provider Locally
Build and run the provider from `terraform/provider`:

```bash
cd terraform/provider

go build -o wallaby-tf
```

## Acceptance Tests
Acceptance tests are guarded by the `acceptance` build tag and environment variables.

```bash
export WALLABY_TF_ACC=1
export WALLABY_TF_ENDPOINT="localhost:8080"
export WALLABY_TF_INSECURE="true"
export WALLABY_TF_POSTGRES_DSN="postgres://user:pass@localhost:5432/app?sslmode=disable"
export WALLABY_TF_KAFKA_BROKERS="localhost:9092"
export WALLABY_TF_KAFKA_TOPIC="wallaby.cdc"

cd terraform/provider

go test -tags=acceptance ./...
```

The tests expect a running WALlaby gRPC server and a Postgres instance configured for logical replication.
