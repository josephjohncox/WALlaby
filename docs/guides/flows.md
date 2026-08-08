# Manage flows and table mappings

Every persisted flow contains a versioned, destination-scoped `config.table_mappings` document. Endpoint options configure transport and physical ownership only. The mapping is the sole logical contract for table selection, names, columns, write behavior, keys, and freshness.

## Mapping model

Each destination name has exactly one mapping. `future_tables` handles source relations without an exact rule; `tables` contains exact source-schema/source-table rules. An exact table rule contains a `future_columns` policy plus ordered exact `columns` rules.

Every action is explicit:

- `include` requires a target name or template;
- `exclude` has no target, nested column policy, or write policy;
- exact rules override future rules; and
- exact column rules override `future_columns`.

Future table and column targets preserve identifier components independently using a restricted Go `text/template` contract. `target_schema` contains exactly one `{{ .Schema }}` action, `target_table` exactly one `{{ .Table }}` action, and `target_column` exactly one `{{ .Column }}` action. Literal prefixes and suffixes such as `raw_{{ .Schema }}` are valid. WALlaby executes the template once with typed `Schema`, `Table`, and `Column` string data; injected bytes, including braces and template-looking text, are never interpreted recursively. Cross-field, repeated, nested, or otherwise executable actions are rejected. Gomplate functions and datasources, Sprig, environment access, file or network access, conditions, loops, functions, pipelines, variables, and template includes are unsupported. This component-wise contract is injective: ambiguous-looking source pairs cannot collapse through dot joining. Validation also inverses exact targets through future templates and rejects any exact/future table or column collision unless the inverse source is explicitly overridden by an include or exclude rule. Exact-rule precedence therefore remains safe without leaving an unruled future source able to reuse the same target. Exact targets are literal identifiers and may rename or exclude individual tables and columns; only executable Go-template actions are rejected, while ordinary single braces remain literal.

```yaml
version: 2
destinations:
  - destination: warehouse
    future_tables:
      action: include
      target_schema: "{{ .Schema }}"
      target_table: "{{ .Table }}"
      future_columns:
        action: include
        target_column: "{{ .Column }}"
      write:
        mode: append
        key_columns: []
    tables:
      - source_schema: public
        source_table: customers
        action: include
        target_schema: analytics
        target_table: accounts
        future_columns:
          action: include
          target_column: "{{ .Column }}"
        columns:
          - source_column: id
            action: include
            target_column: account_id
          - source_column: secret
            action: exclude
          # Prevent future source account_id from also mapping to target account_id.
          - source_column: account_id
            action: exclude
        write:
          mode: upsert
          key_columns: [id]
      # Prevent a future analytics.accounts source from reusing this exact target.
      - source_schema: analytics
        source_table: accounts
        action: exclude
```

## Generate from the PostgreSQL catalog

Catalog generation is read-only and requires an explicit scope. Select one or more exact tables, schemas, or publications; there is no implicit search-path scan.

```bash
wallaby-admin flow mappings generate \
  --file flow.json \
  --publication app_publication \
  --output mappings.json \
  --format json
```

Use `--schema` or `--table` instead of `--publication` when appropriate. PostgreSQL IAM flags use the same source connection seam as other direct catalog commands. Existing output is never overwritten without `--force`.

Generation preserves PostgreSQL identifiers and deterministic catalog order. `mappinggen` derives the ordered source-primary-key default, but the CLI resolves it against each exact configured destination and managed profile: a primary-key table defaults to `upsert` only when that destination/profile admits explicit-key upsert; append-only destinations automatically receive `append`. Keyless tables and unknown future tables always default to `append`. Multi-destination full-flow output therefore can contain different policies for the same source table.

Use repeatable `--match-column public.orders=tenant_id,id` overrides to replace, rather than extend, source-primary-key columns. Use `--write-mode public.orders=append` to choose append on an upsert-capable destination, or `--write-mode public.orders=upsert` to request upsert. An upsert override fails unless the exact destination/profile admits explicit-key upsert and the table has an explicit match override or source primary key. Use `--watermark public.orders=updated_at` independently: append retains it as projected metadata, while upsert uses it as a freshness guard subject to destination capability. With multiple flow destinations, `--destination` selects mapping-only output; full-flow mode generates capability-aware policies separately for every missing destination and applies explicit write-mode overrides to existing exact included mappings while preserving all other valid existing mapping fields.

The exact `postgresql-to-snowflake-sql-v1` profile has a stricter generator contract. Catalog scope must resolve to exactly one relation with a nonempty complete ordered source primary key. Generation emits one exact upsert mapping using that full key and sets `future_tables.action: exclude`. It rejects zero or multiple relations, a keyless relation, append overrides, watermarks, and any match override that is partial, reordered, or contains extra columns. Generic Snowflake remains append-only.

`--output-mode mappings` writes only the mapping document and contains no endpoint options, DSNs, credentials, or local paths. `--output-mode flow` emits a lossless expanded copy of the input flow. That full-flow output can contain every secret from the input; protect it like the original and do not send it to logs or review systems.

## Strict files and local mapping imports

Flow JSON uses unknown-field rejection and requires exactly one JSON value. YAML uses known-field decoding and requires exactly one document. Misspelled or trailing fields fail instead of being ignored.

A local authoring file may set `config.table_mappings_file`. The admin client resolves a relative path against the lexical directory containing the flow file, strictly decodes it, expands it into `config.table_mappings`, and erases the path before protobuf conversion. The path is never sent, displayed as persisted configuration, or stored by the server. A flow cannot set both inline mappings and a mapping file.

## Review, validate, plan, and apply

Review generated names, exclusions, write modes, key order, and future rules before creating a flow:

```bash
wallaby-admin flow validate --file flow.json
wallaby-admin flow dry-run --file flow.json
wallaby-admin flow check --file flow.json --endpoint localhost:8080
wallaby-admin flow plan --file flow.json --flow-id <flow-id> --endpoint localhost:8080
```

Validation and comparison use raw values, while presentation output redacts credentials and capability-bearing endpoints. Plan compares the complete desired definition, including ordered destinations and expanded mappings; runtime state is not desired configuration. `--flow-id` lets an ID-less authoring file select the persisted flow. A matching file ID is accepted, but a different file ID and `--flow-id` conflict and fail. Remote comparison requires `--endpoint` plus either source of ID.

Create only after review:

```bash
wallaby-admin flow create --file flow.json --start
wallaby-admin flow get --flow-id <flow-id>
```

For unmanaged flows, use `flow update` only for ordinary nonidentity changes. Mapping, wire-format, source, destination, and other identity-affecting changes create a new incarnation and use controlled reconfiguration:

```bash
wallaby-admin flow reconfigure \
  --flow-id <flow-id> \
  --file flow.json \
  --pause=true \
  --resume=true
```

For an unmanaged flow, the server fences new work, quiesces a running generation, installs the new definition, and resumes only after the new incarnation is authoritative. Managed flows reject both `UpdateFlow` and `ReconfigureFlow`, including name-only and parallelism-only changes. For any managed change, assign a new flow ID and immutable destination/publication revision identities, stop the old flow, create and validate the replacement, start and verify it, cut over, then delete and clean up the old flow only when it is safe. Terraform exposes the nested mapping model but every managed update fails apply and retains the old state; it cannot perform this cutover lifecycle. Model the replacement as a distinct resource/revision. Only unmanaged name-only and parallelism-only changes use the ordinary update RPC.

## Write behavior and recovery

`append` records every source event and adds `__wallaby_operation`, `__wallaby_deleted`, and `__wallaby_source_position`. A configured append watermark is projected metadata only; it does not suppress events and does not require destination watermark-guard capability. Kafka, Redpanda, HTTP, gRPC, S3, Snowpipe, ClickHouse, DuckDB, DuckLake, pgstream, generic Snowflake, and Iceberg are append-only mapping destinations.

`upsert` requires a nonempty ordered `key_columns` list. The list is the complete user-selected match identity. It is not combined with source primary-key metadata. PostgreSQL supports explicit-key upsert and durable watermark-guarded upsert. Generic Snowflake is append-only. The exact `postgresql-to-snowflake-sql-v1` profile supports current-state explicit-key upsert only and requires exactly one mapped source relation whose keys equal the complete ordered source primary key; every key component must survive projection. That profile rejects watermarks and unknown future tables.

An upsert watermark establishes freshness, not identity. PostgreSQL watermark-guarded upsert requires an independent explicit key, a nonnullable admitted orderable watermark type, and watermark availability in old rows. Configured upsert key and watermark columns must be included and available from replica identity or `REPLICA IDENTITY FULL`; otherwise key changes and deletes fail closed. An append watermark is metadata only: it does not require nonnullability, ordering, replica identity, or a watermark-guard capability. PostgreSQL canonicalizes values through projected casts and keeps durable tombstones in `wallaby.watermark_state`. Greater watermarks advance. Equal watermarks advance only at a greater canonical source position. Equal watermark and position with identical content is idempotent; different content is a delivery conflict. Tombstones prevent stale updates from resurrecting deleted rows.

## Mapped materialization

The historical `canonical_cdc_parquet_v1` projection remains byte-for-byte frozen for historical artifact verification. It is not admitted for current mapped materialization. Mapped Iceberg flows use exactly `canonical_cdc_parquet_v2`, bind artifacts and catalog commits to mapping and projection identity, and remain append-only.

## Lifecycle operations

```bash
wallaby-admin flow wait --flow-id <flow-id> --state running --timeout 60s
wallaby-admin flow pause --flow-id <flow-id>
wallaby-admin flow resume --flow-id <flow-id>
wallaby-admin flow stop --flow-id <flow-id>
wallaby-admin flow wait --flow-id <flow-id> --state stopped --timeout 60s
wallaby-admin flow cleanup --flow-id <flow-id>
```

Pause is resumable. Stop is terminal. Cleanup remains separate so operators explicitly choose source-resource retention.
