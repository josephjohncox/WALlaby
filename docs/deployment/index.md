# Choose a runtime

Every runtime executes the same flow runner. Choose based on who owns worker processes and how much recovery machinery you need.

Production always requires a PostgreSQL workflow store. The memory store is limited to development and tests.

## Compare runtimes

| Runtime | Process owner | Dispatch durability | Cancellation proof | Required infrastructure |
| --- | --- | --- | --- | --- |
| Supervised worker | Your process supervisor | Supervisor-specific | Worker execution lease expires or finishes | PostgreSQL plus a supervisor |
| DBOS | DBOS | DBOS workflow storage | Exact execution identity reaches a terminal DBOS state; uncertain crash cases fail closed | PostgreSQL and DBOS |
| Kubernetes Jobs | Kubernetes dispatcher | Durable lifecycle intent plus Kubernetes Job state | Foreground Job deletion plus expired execution lease | PostgreSQL and Kubernetes Jobs RBAC |

Start with a supervised worker. Move to DBOS or Kubernetes only when you need the runtime to create and recover worker attempts.

## Control plane

A production control plane needs durable workflow storage:

```bash
export WALLABY_ENV=production
export WALLABY_WORKFLOW_STORE=postgres
export WALLABY_POSTGRES_DSN='postgres://user:password@postgres:5432/wallaby?sslmode=require'
./wallaby
```

The process rejects a missing PostgreSQL DSN. It also rejects memory workflow storage when DBOS or Kubernetes dispatch is enabled.

## Supervised workers

When an external supervisor owns processes, start one worker for each running flow:

```bash
wallaby-worker --flow-id <flow-id>
```

The worker reads the current lifecycle generation, registers an execution lease, and renews it while running. Pause, stop, or a generation change cancels the runner. A finite worker exit does not change the desired lifecycle state.

After `resume`, start a new worker. A process from the previous generation cannot register.

## DBOS

DBOS schedules durable attempts. WALlaby passes the captured lifecycle generation into every workflow input. Lifecycle dispatch is idempotent per flow generation; `run-once` creates a separate attempt identity.

Pause and stop wait for exact terminal execution identities. If DBOS cannot prove that an execution has unwound after a crash, WALlaby leaves the lifecycle operation incomplete for reconciliation rather than reporting false quiescence.

## Kubernetes Jobs

Kubernetes dispatch creates generation-scoped Jobs. Each Job carries authoritative flow ID, generation, backend, and execution ID metadata. Pause and stop delete matching Jobs with foreground propagation, then wait for execution leases before completing.

The Helm chart uses separate dispatcher and worker service accounts. Worker API-token automount is disabled by default. Enable namespaced Job RBAC with:

```yaml
kubernetesDispatch:
  enabled: true
```

## Helm

Start from `charts/wallaby/values.example.yaml`. Put the PostgreSQL DSN in a Secret or another protected source; do not commit it in values files.

Validate a release configuration before installing:

```bash
helm lint charts/wallaby
helm template wallaby charts/wallaby \
  -f charts/wallaby/values-prod.yaml \
  -f your-values.yaml >/tmp/wallaby-rendered.yaml
```

Read [flow lifecycle](../concepts/lifecycle.md) for state semantics and [architecture](../architecture.md) for the control/data split.
