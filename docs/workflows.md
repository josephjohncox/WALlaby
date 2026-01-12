# Workflow Engines

WALlaby supports three ways to run flows: CLI worker mode, DBOS scheduling, and Kubernetes job dispatch. All three share the same flow definitions and lifecycle state transitions.

## CLI Worker (Per-Flow Process)
Run a single flow in its own process. This is the simplest option and works well for local dev or when a supervisor (systemd, k8s, Nomad) handles process lifecycle.

Example:
```bash
./bin/wallaby-worker -flow-id "flow-1"
```

Recommended flags:
- `-max-empty-reads 1` for periodic runs (exits after one empty poll).
- `-resolve-staging` to apply staged backfill tables in batch mode.

See `examples/workflows/cli.env` for a minimal env setup.

## DBOS Scheduling (Durable Runs)
DBOS enqueues durable runs for flows in `running` state. This is ideal for scheduled batch runs or reliable periodic streaming.

Env configuration:
```bash
export WALLABY_DBOS_ENABLED="true"
export WALLABY_DBOS_APP="wallaby"
export WALLABY_DBOS_QUEUE="wallaby"
export WALLABY_DBOS_SCHEDULE="*/10 * * * * *"
export WALLABY_DBOS_MAX_RETRIES="5"
```

See `examples/workflows/dbos.env` for a full template.

## Kubernetes Job Dispatch
When enabled, the API server can launch per‑flow workers as Kubernetes Jobs on `StartFlow` or `ResumeFlow`.

Env configuration:
```bash
export WALLABY_K8S_ENABLED="true"
export WALLABY_K8S_JOB_IMAGE="ghcr.io/josephjohncox/wallaby:0.1.0"
export WALLABY_K8S_JOB_SERVICE_ACCOUNT="wallaby"
export WALLABY_K8S_JOB_ENV_FROM="secret:wallaby-secrets,configmap:wallaby-config"
```

Optional settings include namespace, kubeconfig, explicit API server credentials, and job labels/annotations.
See `examples/workflows/k8s.env` for a ready‑to‑edit template.
