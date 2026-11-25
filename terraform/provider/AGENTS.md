# Terraform provider rules

Read the root guidance first. This is a separate Go module that replaces the
root module with the checkout. Root `go test ./...` does not test this module.

- Use `mise exec -- go -C terraform/provider test ./...` from the repository root,
  or `mise run test` to cover both application modules.
- Endpoint schemas derive from Protobuf descriptors. New branches also need a
  field in endpointModel, branchObjects and setBranchObject; test typed round
  trips, optional zero/false presence and sensitivity.
- Preserve nested oneof validation. Reject unknown values before apply rather
  than dropping fields or selecting an arbitrary branch.
- Mark passwords, tokens, DSNs and private keys sensitive; do not print them in
  diagnostics or examples. Use reserved example domains and placeholder values.
- Update and tidy this go.mod/go.sum with root dependency changes. Keep Go pins
  aligned with root go.mod, Dockerfile and mise.toml.
