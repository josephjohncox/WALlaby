# Protobuf rules

Read the root guidance first. `wallaby/v1/*.proto` is the source; do not hand-edit
`gen/go/` or generated API documentation.

- Use snake_case file/field names and UpperCamel service/RPC names.
- Built-in endpoints have typed config messages and a branch in Endpoint.config.
  Only custom plugins may expose arbitrary string options.
- Use optional scalar presence when omitted differs from an explicit zero/false.
  Validate unknown enums, invalid durations and wrong endpoint roles at the codec
  boundary. Document units, defaults and secret-bearing fields.
- Add endpointcodec round-trip/rejection tests and Terraform model/schema tests
  for new branches. Keep credential fields sensitive and redacted.
- Run `mise run proto`, `mise run proto-lint` and relevant Go tests.
  Run `mise run docs-generate` for API reference changes.
- Keep breaking checks strict. Do not extend the breaking allowlist just to pass
  CI; intentional wire breaks need an explicit policy decision.
