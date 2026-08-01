# Manage flows

Use `wallaby-admin` for routine flow operations. Server-backed flow commands accept the common gRPC flags
`--endpoint` and `--insecure`; the local-only `flow validate` and `flow dry-run` commands do not. AWS RDS IAM
flags are command-specific: they are available on the `slot`, `publication`, and `certify` commands that connect
directly to PostgreSQL, not as common `flow` flags. All commands accept the global `--config` flag.

## Validate before writing

```bash
wallaby-admin flow validate --file flow.json
wallaby-admin flow dry-run --file flow.json
wallaby-admin flow check --file flow.json --endpoint localhost:8080
```

Validation checks the document shape. Dry run shows normalized configuration without creating a flow. Check also tests connector configuration against the server.

## Create and inspect

```bash
wallaby-admin flow create --file flow.json --start
wallaby-admin flow list --state running
wallaby-admin flow get --flow-id <flow-id>
```

Use `flow wait` in automation:

```bash
wallaby-admin flow wait \
  --flow-id <flow-id> \
  --state running \
  --timeout 60s
```

## Pause and resume

Pause is resumable:

```bash
wallaby-admin flow pause --flow-id <flow-id>
wallaby-admin flow resume --flow-id <flow-id>
```

For a configuration change, use `flow reconfigure` so the server can coordinate pause, update, publication synchronization, and resume.

## Stop

Stop is terminal:

```bash
wallaby-admin flow stop --flow-id <flow-id>
wallaby-admin flow wait --flow-id <flow-id> --state stopped
```

The intermediate `stopping` state means dispatcher cancellation has not finished. Do not treat it as stopped.

## Clean up source resources

After a flow has stopped, cleanup drops the replication slot and source-state row by default while retaining
the publication:

```bash
wallaby-admin flow cleanup --flow-id <flow-id>
```

The defaults are `--drop-slot=true`, `--drop-publication=false`, and `--drop-source-state=true`. Use explicit
boolean forms to change retention, for example:

```bash
wallaby-admin flow cleanup \
  --flow-id <flow-id> \
  --drop-slot=false \
  --drop-publication=true \
  --drop-source-state=false
```

Cleanup is separate from lifecycle stop so operators can choose the retention policy.
