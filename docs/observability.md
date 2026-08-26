# Observability

WALlaby exposes OpenTelemetry metrics and tracing plus optional `pprof` profiling.

## OpenTelemetry Configuration

Set signal-specific OTLP environment variables when metrics and traces use different collectors:

- `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`
- `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL` (`grpc` or `http/protobuf`)
- `WALLABY_OTEL_METRICS_INSECURE` (`true` or `false`)
- `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`
- `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` (`grpc` or `http/protobuf`)
- `WALLABY_OTEL_TRACES_INSECURE` (`true` or `false`)
- `OTEL_METRICS_EXPORTER` (`otlp` to enable, `none` to disable)
- `OTEL_TRACES_EXPORTER` (`otlp` to enable, `none` to disable)
- `WALLABY_OTEL_METRICS_INTERVAL` (for example, `30s`)
- `OTEL_SERVICE_NAME` (defaults to `wallaby`)

`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL`, and `OTEL_EXPORTER_OTLP_INSECURE` remain shared fallbacks for existing deployments.

## Health contracts

WALlaby registers the standard gRPC health service. Kubernetes and external checks can query these service names:

- `wallaby.startup` — process initialization completed.
- `wallaby.readiness` — initialized dependencies are ready to serve API traffic.
- `wallaby.liveness` — the gRPC process is running.

The Helm chart enables native gRPC startup, readiness, and liveness probes by default. Its Helm test queries `wallaby.readiness` rather than checking only whether the TCP port is open.

## Metrics

### Stream Runner

| Metric | Type | Labels |
| -------- | ------ | -------- |
| `wallaby.records.processed` | Counter | `flow_id` |
| `wallaby.batches.processed` | Counter | `flow_id` |
| `wallaby.batch.latency` | Histogram | `flow_id` |
| `wallaby.destination.write.latency` | Histogram | `flow_id` |
| `wallaby.batch.records` | Histogram | `flow_id` |
| `wallaby.errors.total` | Counter | `error_type` |
| `wallaby.checkpoints.commits` | Counter | `flow_id` |
| `wallaby.ddl.gated_total` | Counter | `flow.id`, `ddl.status` |

Error types include: `source_read`, `source_ack`, `destination_write`, `checkpoint_persist`.

### Managed durability and artifacts

| Metric | Type | Labels |
| -------- | ------ | -------- |
| `wallaby.fence.rejections` | Counter | - |
| `wallaby.lease.takeovers` | Counter | - |
| `wallaby.delivery.outcomes` | Counter | bounded `outcome` |
| `wallaby.bootstrap.events` | Counter | bounded `event` |
| `wallaby.bootstrap.rows` | Counter | - |
| `wallaby.bootstrap.batches` | Counter | - |
| `wallaby.bootstrap.claim.renewals` | Counter | bounded `outcome` |
| `wallaby.bootstrap.phase.duration` | Histogram | bounded `phase` |
| `wallaby.bootstrap.exporter.age` | Histogram | bounded `outcome` |
| `wallaby.artifact.transitions` | Counter | bounded `state` |
| `wallaby.artifact.bytes` | Histogram | bounded `state` |
| `wallaby.artifact.consumer.outcomes` | Counter | `outcome` |
| `wallaby.artifact.gc.outcomes` | Counter | `outcome` |
| `wallaby.artifact.metadata_retention.publications` | Counter | bounded `outcome`: `scanned`, `deleted`, `deferred`, `other` |
| `wallaby.artifact.metadata_retention.rows` | Counter | - |

Artifact backlog count/bytes/age, reserved/rooted quota headroom, GC lag, S3 request latency/retries, and recovery-duration gauges are not implemented in this experimental checkpoint. PostgreSQL queries remain the authoritative operational source for those values. A persistent rise in metadata `deferred` without `deleted` means a live root, checkpoint, claim, pending delivery, or unresolved catalog attempt is retaining evidence; diagnose that authority rather than deleting rows manually.

### gRPC API

| Metric | Type | Labels |
| -------- | ------ | -------- |
| `wallaby.grpc.requests.total` | Counter | `method`, `status` |
| `wallaby.grpc.request.latency` | Histogram | `method` |
| `wallaby.grpc.errors.total` | Counter | `method`, `code` |

### Workflow Engine

| Metric | Type | Labels |
| -------- | ------ | -------- |
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
| -------- | ------ | -------- |
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
