# Documentation rules

Read the root guidance first. Keep user-facing instructions runnable from a
public checkout with mise and explicitly documented external services.

- Use `mise run <task>` for workflows and `mise exec -- <command>` for ad-hoc
  pinned tools. Keep task names and defaults aligned with mise.toml.
- Put connector behavior/limits in connectors/, contributor tasks in development/,
  procedures in operations/ or guides/, and runnable fixtures in ../examples/.
  Add new pages to ../mkdocs.yml and check relative links.
- State support levels and delivery limits accurately. Do not present a fake,
  model, compile check or skipped service test as a production guarantee.
- Use localhost, reserved example domains and placeholder secrets. Do not copy
  employer-specific setup, accounts, hostnames, tooling or local absolute paths.
- Generated files under reference/ come from public Go symbols and Protobuf.
  Run `mise run docs-generate` rather than editing them by hand.
- Run `mise run docs-check`; run `mise run docs-verify` to check generated drift.
