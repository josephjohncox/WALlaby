# Operational Runbooks

## DDL Gating (Approval Required)
When `WALLABY_DDL_GATE=true`, WALlaby pauses the flow on DDL events until they are approved.

### Detecting a gate
- Flow state transitions to `paused` with reason `ddl_pending`.
- Metric: `wallaby.ddl.gated_total` increments per gated event.
- Trace events contain `spec_action=ReadDDL` and `flow_state=Paused`.

### Alerting example (OTEL → Prometheus)
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
            description: "A flow paused for DDL approval. Check pending DDL events and approve/apply."
```

### Log-to-alert example
If you forward logs to a system like Loki, alert on paused flows:

```
{app="wallaby"} |= "ddl_pending"
```

### Approve + apply DDL
Use the CLI:

```bash
wallaby-admin ddl list -status pending
wallaby-admin ddl approve -id <id>
wallaby-admin ddl apply -id <id>
```

Or with gRPC:

```bash
grpcurl -plaintext -d '{"status":"pending"}' localhost:8080 wallaby.v1.DDLService/ListDDL
```

### Resume flow
After approval + apply:

```bash
wallaby-admin flow resume -flow-id <id>
```

If `WALLABY_DDL_AUTO_APPROVE=true` and `WALLABY_DDL_AUTO_APPLY=true`, WALlaby will not pause.

## Recovery Playbook

### Flow worker stalled
- Check last checkpoint (`wallaby-admin flow get` or DBOS/K8s logs).
- Verify replication slot status in Postgres (`pg_replication_slots`).
- If the slot is stuck, restart the flow worker with `-max-empty-reads=1` for a quick probe run.

### Kubernetes dispatch recovery
If a job completed or failed, re-dispatch via gRPC:

```bash
grpcurl -plaintext -d '{"flow_id":"<id>"}' localhost:8080 wallaby.v1.FlowService/RunFlowOnce
```

The dispatcher is idempotent: if a job is already active it will return success; if the last job finished, it launches a new attempt.

### DBOS recovery
Use the admin recovery endpoint (if enabled) to re-run failed workflows:

```bash
curl -X POST http://<admin-host>/dbos-workflow-recovery
```

### DDL gating recovery
If a flow is paused with pending DDL:
1) List pending DDL events.
2) Approve/apply the event.
3) Resume the flow.

See the DDL gating section above.
