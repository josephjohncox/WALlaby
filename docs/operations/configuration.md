# Configuration

WALlaby commands read YAML plus environment variables. A value from the selected configuration file takes precedence over its environment variable. Defaults apply last.

## Server configuration

The server searches for `wallaby.yaml` or `wallaby.yml` unless you pass `--config` or set `WALLABY_CONFIG`.

A minimal production file is:

```yaml
environment: production
workflow:
  store: postgres
postgres:
  dsn: postgres://user:pass@postgres:5432/wallaby?sslmode=require
api:
  grpc_listen: :8080
```

Do not commit credentials. Supply the DSN through an environment variable or a secret-mounted configuration file.

## Workflow stores

| Store | Environments | Durability |
| --- | --- | --- |
| `postgres` | all | Durable and required for production |
| `memory` | `dev`, `development`, `test` | Process-local and erased on exit |

The default is `postgres`. Memory mode is rejected outside `dev`, `development`, or `test`, and it cannot be combined with DBOS or Kubernetes dispatch. Production and multi-process deployments must use PostgreSQL so lifecycle intent, execution ownership, and dispatch recovery survive process restarts.

## Checkpoint stores

Checkpoint storage can use PostgreSQL, SQLite, or `none`. SQLite makes checkpoints durable for a local worker, but it does not replace the production workflow store. The control plane still needs PostgreSQL unless the explicit development memory mode is active. Lifecycle reconciliation and stale-execution fencing depend on the workflow store, not the checkpoint backend.

```bash
export WALLABY_CHECKPOINT_BACKEND=sqlite
export WALLABY_CHECKPOINT_PATH="$HOME/.wallaby/checkpoints.db"
```

## Command-specific files

- `wallaby-admin` reads `wallaby-admin.yaml` or `$HOME/.config/wallaby/wallaby-admin.yaml` and honors `WALLABY_ADMIN_CONFIG`.
- `wallaby-worker` reads `wallaby-worker.yaml` and honors `WALLABY_WORKER_CONFIG`.
- Validation tools expose command-specific environment variables documented in the [configuration reference](../usage.md).
