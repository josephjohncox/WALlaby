# Terraform Provider

This folder contains the Terraform provider source and acceptance test harness.

## Example Configuration
See `terraform/examples/basic/main.tf` for a minimal flow definition.

## Running the Provider Locally
Build and run the provider from `terraform/provider`:

```bash
cd terraform/provider

go build -o ductstream-tf
```

## Acceptance Tests
Acceptance tests are guarded by the `acceptance` build tag and environment variables.

```bash
export DUCTSTREAM_TF_ACC=1
export DUCTSTREAM_TF_ENDPOINT="localhost:8080"
export DUCTSTREAM_TF_INSECURE="true"
export DUCTSTREAM_TF_POSTGRES_DSN="postgres://user:pass@localhost:5432/app?sslmode=disable"
export DUCTSTREAM_TF_KAFKA_BROKERS="localhost:9092"
export DUCTSTREAM_TF_KAFKA_TOPIC="ductstream.cdc"

cd terraform/provider

go test -tags=acceptance ./...
```

The tests expect a running DuctStream gRPC server and a Postgres instance configured for logical replication.
