package snowflake

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/snowflakedb/gosnowflake"
)

func stagedValidOptions(t *testing.T) (string, map[string]string) {
	t.Helper()
	dsn := managedSnowflakeTestDSN(t, nil)
	schema := managedTestSchema()
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := ManagedSchemaContractHash(schema)
	if err != nil {
		t.Fatal(err)
	}
	created := "2026-01-01T00:00:00.000000000+00:00"
	options := map[string]string{
		"dsn": dsn, "flow_id": "flow-1", "managed_profile": connector.ManagedProfilePostgresToSnowflakeStagedAppendV1,
		"destination_revision_id": "snowflake-staged-v1", "batch_mode": "target", "batch_resolution": "none",
		"meta_table_enabled": "false", "disable_transactions": "false", "session_keep_alive": "false",
		"managed_account": "ACCOUNT", "managed_database": "DB", "managed_schema": "PUBLIC", "managed_stage": "WALLABY_STAGE",
		"managed_table": "WALLABY_CHANGELOG", "managed_receipts_table": "WALLABY_RECEIPTS", "managed_file_format": "WALLABY_JSON",
		"managed_owner_role": "WALLABY_OWNER", "managed_execution_role": "ROLE", "managed_warehouse": "WH",
		"managed_snowflake_version": "8.0.0", "managed_stage_created_on": created, "managed_target_created_on": created,
		"managed_receipts_created_on": created, "managed_file_format_created_on": created,
		"managed_source_schema": "public", "managed_source_table": "widgets",
		"managed_schema_contract": string(schemaJSON), "managed_schema_contract_hash": hash,
		"managed_max_transaction_rows": "1000", "managed_max_transaction_bytes": "8388608",
		"managed_max_transaction_fragments": "128", "managed_max_open_conns": "4",
		"managed_statement_timeout_seconds": "600", "managed_load_verify_attempts": "10",
		"managed_load_verify_interval_ms": "1000", "managed_cleanup_max_objects": "1000",
		"managed_cleanup_retention_seconds": "2592000",
	}
	return dsn, options
}

func TestStagedConfigFromSpecAdmitsValidSpec(t *testing.T) {
	t.Parallel()
	dsn, options := stagedValidOptions(t)
	cfg, err := stagedConfigFromSpec(dsn, connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: options})
	if err != nil {
		t.Fatalf("valid staged spec rejected: %v", err)
	}
	if cfg.stage != "WALLABY_STAGE" || cfg.table != "WALLABY_CHANGELOG" || cfg.fileFormat != "WALLABY_JSON" {
		t.Fatalf("staged config identity=%+v", cfg)
	}
	if cfg.autoIngest {
		t.Fatal("auto-ingest must default to disabled")
	}
}

func TestStagedConfigRejectsLossyAndUnsafeOptions(t *testing.T) {
	t.Parallel()
	cases := map[string]func(map[string]string){
		"obsolete write mode": func(o map[string]string) { o["write_mode"] = "staged_append" },
		"batch mode":          func(o map[string]string) { o["batch_mode"] = "staged" },
		"meta table":          func(o map[string]string) { o["meta_table_enabled"] = "true" },
		"keep alive":          func(o map[string]string) { o["session_keep_alive"] = "true" },
		"type override":       func(o map[string]string) { o["type_mappings"] = "text=STRING" },
		"generic staging":     func(o map[string]string) { o["staging_table"] = "X" },
		"same role":           func(o map[string]string) { o["managed_execution_role"] = o["managed_owner_role"] },
		"unknown option":      func(o map[string]string) { o["nonsense"] = "1" },
		"pipe without ai":     func(o map[string]string) { o["managed_pipe"] = "WALLABY_PIPE" },
		"bad contract":        func(o map[string]string) { o["managed_schema_contract_hash"] = "deadbeef" },
		"missing created":     func(o map[string]string) { o["managed_stage_created_on"] = "" },
		"lowercase ident":     func(o map[string]string) { o["managed_stage"] = "wallaby_stage" },
	}
	for name, mutate := range cases {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dsn, options := stagedValidOptions(t)
			mutate(options)
			if _, err := stagedConfigFromSpec(dsn, connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: options}); err == nil {
				t.Fatalf("staged admission accepted an unsafe spec (%s)", name)
			}
		})
	}
}

func TestManagedAppendConfigsRejectObsoleteWriteModeConsistently(t *testing.T) {
	t.Parallel()
	const want = "managed Snowflake write_mode is obsolete; managed_profile and the mandatory table mapping select the protocol"
	for _, value := range []string{"", "staged_append", "streaming_append", "target"} {
		t.Run(value, func(t *testing.T) {
			_, stagedOptions := stagedValidOptions(t)
			stagedOptions["write_mode"] = value
			if err := ValidateManagedStagedProfileOptions(stagedOptions); err == nil || err.Error() != want {
				t.Fatalf("staged write_mode=%q error=%v, want %q", value, err, want)
			}
			_, streamOptions := streamValidOptions(t)
			streamOptions["write_mode"] = value
			if err := ValidateManagedStreamingProfileOptions(streamOptions); err == nil || err.Error() != want {
				t.Fatalf("streaming write_mode=%q error=%v, want %q", value, err, want)
			}
		})
	}
}

func TestManagedAppendConfigsPreserveExactSourceIdentifiers(t *testing.T) {
	t.Parallel()
	for _, relation := range []struct{ schema, table string }{{" ", " "}, {" leading", "trailing "}, {" both ", " all "}} {
		t.Run(relation.schema+"/"+relation.table, func(t *testing.T) {
			for _, profile := range []string{"staged", "streaming"} {
				t.Run(profile, func(t *testing.T) {
					var dsn string
					var options map[string]string
					if profile == "staged" {
						dsn, options = stagedValidOptions(t)
					} else {
						dsn, options = streamValidOptions(t)
					}
					var contract connector.Schema
					if err := json.Unmarshal([]byte(options["managed_schema_contract"]), &contract); err != nil {
						t.Fatal(err)
					}
					contract.Namespace, contract.Name = relation.schema, relation.table
					encoded, err := json.Marshal(contract)
					if err != nil {
						t.Fatal(err)
					}
					hash, err := ManagedSchemaContractHash(contract)
					if err != nil {
						t.Fatal(err)
					}
					options["managed_source_schema"], options["managed_source_table"] = relation.schema, relation.table
					options["managed_schema_contract"], options["managed_schema_contract_hash"] = string(encoded), hash
					transaction := managedTestTransaction(contract)
					for fragmentIndex := range transaction.Fragments {
						for recordIndex := range transaction.Fragments[fragmentIndex].Batch.Records {
							transaction.Fragments[fragmentIndex].Batch.Records[recordIndex].Table = relation.table
						}
					}
					if profile == "staged" {
						cfg, err := stagedConfigFromSpec(dsn, connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: options})
						if err != nil || cfg.sourceSchema != relation.schema || cfg.sourceTable != relation.table {
							t.Fatalf("staged exact relation=%q.%q config=%q.%q err=%v", relation.schema, relation.table, cfg.sourceSchema, cfg.sourceTable, err)
						}
						if _, err := planManagedStagedTransaction(cfg, stagedTestIntent(t, cfg, transaction), transaction); err != nil {
							t.Fatalf("staged exact relation planner rejected %q.%q: %v", relation.schema, relation.table, err)
						}
					} else {
						cfg, err := streamConfigFromSpec(dsn, connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: options})
						if err != nil || cfg.sourceSchema != relation.schema || cfg.sourceTable != relation.table {
							t.Fatalf("streaming exact relation=%q.%q config=%q.%q err=%v", relation.schema, relation.table, cfg.sourceSchema, cfg.sourceTable, err)
						}
						if _, err := planManagedStreamTransaction(cfg, streamTestIntent(t, cfg, transaction), transaction); err != nil {
							t.Fatalf("streaming exact relation planner rejected %q.%q: %v", relation.schema, relation.table, err)
						}
					}
				})
			}
		})
	}
}

func TestStagedConfigRequiresPipeForAutoIngest(t *testing.T) {
	t.Parallel()
	dsn, options := stagedValidOptions(t)
	options["managed_auto_ingest"] = "true"
	if _, err := stagedConfigFromSpec(dsn, connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: options}); err == nil || !strings.Contains(err.Error(), "requires managed_pipe") {
		t.Fatalf("auto-ingest without a pipe error=%v, want a pipe requirement", err)
	}
	options["managed_pipe"] = "WALLABY_PIPE"
	options["managed_pipe_created_on"] = "2026-01-01T00:00:00.000000000+00:00"
	cfg, err := stagedConfigFromSpec(dsn, connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: options})
	if err != nil {
		t.Fatalf("valid auto-ingest spec rejected: %v", err)
	}
	if !cfg.autoIngest || cfg.pipe != "WALLABY_PIPE" {
		t.Fatalf("auto-ingest config=%+v", cfg)
	}
}

func TestStagedConfigRequiresFailClosedTransport(t *testing.T) {
	t.Parallel()
	_, options := stagedValidOptions(t)
	insecure := managedSnowflakeTestDSN(t, func(cfg *gosnowflake.Config) {
		cfg.OCSPFailOpen = gosnowflake.OCSPFailOpenTrue
	})
	options["dsn"] = insecure
	if _, err := stagedConfigFromSpec(insecure, connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: options}); err == nil || !strings.Contains(err.Error(), "OCSP fail-closed") {
		t.Fatalf("OCSP fail-open admission error=%v, want fail-closed requirement", err)
	}
}

func TestStagedConfigRequiresReadLatestWrites(t *testing.T) {
	t.Parallel()
	_, options := stagedValidOptions(t)
	noReadLatest := managedSnowflakeTestDSN(t, func(cfg *gosnowflake.Config) {
		timezone := "UTC"
		cfg.Params = map[string]*string{"TIMEZONE": &timezone}
	})
	options["dsn"] = noReadLatest
	if _, err := stagedConfigFromSpec(noReadLatest, connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: options}); err == nil || !strings.Contains(err.Error(), "READ_LATEST_WRITES") {
		t.Fatalf("missing READ_LATEST_WRITES error=%v", err)
	}
}
