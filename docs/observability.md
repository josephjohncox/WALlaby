# Observability

WALlaby exposes OpenTelemetry metrics and tracing plus optional `pprof` profiling.

## OpenTelemetry Configuration

Set standard OTLP environment variables:

- `OTEL_EXPORTER_OTLP_ENDPOINT` (e.g. `http://otel-collector:4317`)
- `OTEL_EXPORTER_OTLP_PROTOCOL` (`grpc` or `http/protobuf`)
- `OTEL_EXPORTER_OTLP_INSECURE` (`true`/`false`)
- `OTEL_SERVICE_NAME` (defaults to `wallaby`)
- `OTEL_METRICS_EXPORTER` (`otlp` to enable, `none` to disable)
- `OTEL_TRACES_EXPORTER` (`otlp` to enable, `none` to disable)
- `WALLABY_OTEL_METRICS_INTERVAL` (e.g. `30s`)

## Metrics

### Stream Runner

| Metric | Type | Labels |
|--------|------|--------|
| `wallaby.records.processed` | Counter | `flow_id` |
| `wallaby.batches.processed` | Counter | `flow_id` |
| `wallaby.batch.latency` | Histogram | `flow_id` |
| `wallaby.destination.write.latency` | Histogram | `flow_id` |
| `wallaby.batch.records` | Histogram | `flow_id` |
| `wallaby.errors.total` | Counter | `error_type` |
| `wallaby.checkpoints.commits` | Counter | `flow_id` |
| `wallaby.ddl.gated_total` | Counter | `flow.id`, `ddl.status` |

Error types include: `source_read`, `source_ack`, `destination_write`, `checkpoint_persist`.

### gRPC API

| Metric | Type | Labels |
|--------|------|--------|
| `wallaby.grpc.requests.total` | Counter | `method`, `status` |
| `wallaby.grpc.request.latency` | Histogram | `method` |
| `wallaby.grpc.errors.total` | Counter | `method`, `code` |

### Workflow Engine

| Metric | Type | Labels |
|--------|------|--------|
| `wallaby.flows.active` | UpDownCounter | - |
| `wallaby.flow.state.transitions` | Counter | `from_state`, `to_state` |
| `wallaby.flow.create.total` | Counter | - |

### Checkpoint Store

| Metric | Type | Labels |
|--------|------|--------|
| `wallaby.checkpoint.get.latency` | Histogram | `backend` |
| `wallaby.checkpoint.put.latency` | Histogram | `backend` |

### Source & Destination

| Metric | Type | Labels |
|--------|------|--------|
| `wallaby.source.replication.lag` | Gauge | `slot` |
| `wallaby.source.read.latency` | Histogram | - |
| `wallaby.destination.write.total` | Counter | `type` |
| `wallaby.destination.ddl.applied` | Counter | `type` |

## Tracing

Key spans emitted:

- `stream.batch` (root for each batch)
- `source.read` (source read latency)
- `source.wait` / `source.process` (Postgres replication wait/process)
- `destination.write` (destination write latency)

Important span attributes:

- `flow.id`, `source.type`
- `batch.records`, `batch.schema`, `batch.latency_ms`
- `destination.write_latency_ms`

DDL gating emits an explicit span event `ddl.gated` and a trace event `ddl_gate` in the trace sink.

## pprof

Enable the built-in profiler with:

- `WALLABY_PPROF_ENABLED=true`
- `WALLABY_PPROF_LISTEN=:6060`

The admin server and workers will expose `/debug/pprof` on the configured address.
