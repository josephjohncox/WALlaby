# Operational runbooks

Start with the observed symptom. Prove the cause before changing lifecycle state or deleting a replication slot.

| Symptom | First check | Safe first action |
| --- | --- | --- |
| Flow stays `running` after pause | `wallaby-admin flow get --flow-id <id>` and worker logs | Wait for active execution leases to finish; do not start a competing worker. |
| Flow stays `stopping` | Dispatcher status and execution leases | Let reconciliation finish cancellation; do not report the flow as stopped. |
| Pending DDL event | `wallaby-admin ddl list --status pending` | Approve the event, wait for `paused`, then resume or start the flow as appropriate; the running data plane applies it and records destination receipts. |
| Source WAL grows | `pg_replication_slots` restart LSN and active state | Identify the owning flow and worker before dropping anything. |
| Kubernetes Job disappeared | Flow control state and authoritative Job annotations | Let the control plane reconcile; do not create a replacement Job manually. |
| Primary-ack secondary is behind | Checkpoint outbox and destination logs | Restore destination availability; restart drains the outbox before new reads. |

## DDL gate requires approval

When `WALLABY_DDL_GATE=true`, WALlaby stops the affected execution at an unapproved DDL event and records pause intent. The public flow state remains `running` until active execution is quiescent; only then does it become `paused`.

### Detecting a gate

- The `wallaby.ddl.gated_total` metric increments per gated event.
- The runtime log contains `ddl gate` with the flow and event identifiers.
- Runtime JSONL traces contain a `ddl_gate` event with `spec_action=Pause`.
- A pending DDL event is authoritative while the public flow state may still be `running` during quiescence. After reconciliation completes, the state is `paused`.

### Alerting example (OTel to Prometheus)

If you export OTEL metrics to Prometheus, dots are converted to underscores, so the metric becomes `wallaby_ddl_gated_total`.

Example Prometheus rule:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: wallaby-ddl-gates
spec:
  groups:
    - name: wallaby.ddl
      rules:
        - alert: WallabyDDLGated
          expr: increase(wallaby_ddl_gated_total[5m]) > 0
          for: 1m
          labels:
            severity: warning
          annotations:
            summary: "WALlaby DDL gate triggered"
            description: "A flow encountered a DDL approval gate. Check and approve pending DDL events, then resume or start the flow as appropriate."
```

### Log-to-alert example

If you forward logs to a system like Loki, alert on DDL gate log records rather than relying on the flow already being publicly paused:

```
{app="wallaby"} |= "ddl gate"
```

### Approve DDL

Use the CLI:

```bash
wallaby-admin ddl list --status pending
wallaby-admin ddl approve --id <id>
```

Resume the flow after approval. The runner applies the DDL, records one durable execution receipt per destination, and changes the event to `applied` only after all expected receipts exist. Administrators cannot assert `applied` without those receipts.

Or with gRPC:

```bash
grpcurl -plaintext -d '{"status":"pending"}' localhost:8080 wallaby.v1.DDLService/ListDDL
```

### Resume flow

After approval:

```bash
wallaby-admin flow resume --flow-id <id>
```

If `WALLABY_DDL_AUTO_APPROVE=true` and `WALLABY_DDL_AUTO_APPLY=true`, WALlaby will not pause.

## Recover a flow

### Worker stopped making progress

1. Read the flow and its last checkpoint with `wallaby-admin flow get --flow-id <id>`.
2. Read the worker or dispatcher logs for the same flow and execution ID.
3. Query `pg_replication_slots` for the configured slot. Compare `active`, `restart_lsn`, and `confirmed_flush_lsn`.
4. Check destination availability and the checkpoint outbox before restarting a primary-ack flow.
5. Restart a supervised worker only when the lifecycle target is still `running`. DBOS and Kubernetes deployments should reconcile through their dispatcher.

A `paused` flow is not evidence of a stalled worker. It is an explicit lifecycle state.

### Kubernetes dispatch recovery

The control plane reconciles incomplete dispatch for a flow that is still `running`; do not create a competing Job manually. To request an additional one-off run for a runnable flow, call:

```bash
grpcurl -plaintext -d '{"flow_id":"<id>"}' localhost:8080 wallaby.v1.FlowService/RunFlowOnce
```

A flow in `failed` is terminal and is not automatically redispatched. Diagnose the failure, perform any required slot cleanup, and create a new flow before continuing.

### DBOS recovery

DBOS may retry an individual workflow according to its configured retry policy while the flow remains runnable. WALlaby also reconciles durable dispatch intent after transient failures. Neither mechanism revives or redispatches a flow whose public state is `failed`; create a new flow after diagnosis and cleanup.

### DDL gating recovery

If a flow has pending DDL and is running or paused:

1. List pending DDL events.
2. Approve the event.
3. Wait for the flow to reach `paused` if quiescence is still in progress.
4. Resume the paused flow, or start it if it is in a startable state.
5. Verify that the running data plane applied the DDL and recorded the expected destination receipts.

There is no administrative apply command. See the DDL gating section above.

## Validate source and destination data

Use `wallaby-admin certify` to compare source/destination data with a deterministic
hash + count. This is safe for production when paired with sampling.

Example (using a flow configuration):

```bash
wallaby-admin certify \
  --flow-id <flow_id> \
  --destination <dest_name> \
  --tables public.widgets \
  --sample-rate 0.01 \
  --sample-limit 10000 \
  --json
```

Direct DSN mode (no flow required):

```bash
wallaby-admin certify \
  --source-dsn "postgres://..." \
  --dest-dsn "postgres://..." \
  --table public.widgets \
  --sample-rate 1
```

Notes:

- Sampling is deterministic and based on primary keys (if no PKs, use full scan).
- Values are normalized using the **source** schema so type differences (e.g., numeric vs text)
  still compare reliably.
