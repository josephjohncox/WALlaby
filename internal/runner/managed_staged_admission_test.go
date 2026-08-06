package runner

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	snowflakedest "github.com/josephjohncox/wallaby/connectors/destinations/snowflake"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/tablemap"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func managedSnowflakeStagedAdmissionDestinations(t *testing.T) []stream.DestinationConfig {
	t.Helper()
	created := "2026-01-01T00:00:00.000000000+00:00"
	contract := connector.Schema{Name: "widgets", Namespace: "public", Columns: []connector.Column{{Name: "id", Type: "int8", TypeMetadata: map[string]string{
		"primary_key": "true", "primary_key_ordinal": "1", "replica_identity": "true", "nullability_known": "true", "generated_known": "true",
	}}}}
	encoded, err := json.Marshal(contract)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := snowflakedest.ManagedSchemaContractHash(contract)
	if err != nil {
		t.Fatal(err)
	}
	return []stream.DestinationConfig{{
		Spec: connector.Spec{Name: "snowflake-staged", Type: connector.EndpointSnowflake, Options: map[string]string{
			"dsn":                               managedAdmissionSnowflakeDSN(t, nil),
			"flow_id":                           "managed-flow",
			"managed_profile":                   connector.ManagedProfilePostgresToSnowflakeStagedAppendV1,
			"destination_revision_id":           "snowflake-staged-v1",
			"batch_mode":                        "target",
			"batch_resolution":                  "none",
			"meta_table_enabled":                "false",
			"disable_transactions":              "false",
			"session_keep_alive":                "false",
			"managed_account":                   "ACCOUNT",
			"managed_database":                  "DB",
			"managed_schema":                    "PUBLIC",
			"managed_stage":                     "WALLABY_STAGE",
			"managed_table":                     "WALLABY_CHANGELOG",
			"managed_receipts_table":            "WALLABY_RECEIPTS",
			"managed_file_format":               "WALLABY_JSON",
			"managed_owner_role":                "OWNER_ROLE",
			"managed_execution_role":            "ROLE",
			"managed_warehouse":                 "WH",
			"managed_snowflake_version":         "9.99.0",
			"managed_stage_created_on":          created,
			"managed_target_created_on":         created,
			"managed_receipts_created_on":       created,
			"managed_file_format_created_on":    created,
			"managed_source_schema":             "public",
			"managed_source_table":              "widgets",
			"managed_schema_contract":           string(encoded),
			"managed_schema_contract_hash":      hash,
			"managed_max_transaction_rows":      "1000",
			"managed_max_transaction_bytes":     "8388608",
			"managed_max_transaction_fragments": "64",
			"managed_max_open_conns":            "4",
			"managed_statement_timeout_seconds": "600",
			"managed_load_verify_attempts":      "10",
			"managed_load_verify_interval_ms":   "1000",
			"managed_cleanup_max_objects":       "1000",
			"managed_cleanup_retention_seconds": "2592000",
		}},
		Dest: &snowflakedest.Destination{},
	}}
}

func managedStagedAdmissionFlowConfigured() (flow.Flow, StreamRunnerConfig) {
	f := managedAdmissionFlow()
	delete(f.Source.Options, "managed")
	f.Source.Options["managed_profile"] = connector.ManagedProfilePostgresToSnowflakeStagedAppendV1
	f.Source.Options["create_slot"] = "true"
	f.Source.Options["slot"] = "managed"
	f.Source.Options["streaming_transactions"] = "true"
	f.Source.Options["toast_fetch"] = "off"
	f.Source.Options["max_transaction_records"] = "1000"
	f.Source.Options["max_transaction_bytes"] = "8388608"
	f.Source.Options["max_transaction_fragments"] = "64"
	f.Destinations = []connector.Spec{{Name: "snowflake-staged", Type: connector.EndpointSnowflake}}
	f.Config.TableMappings = managedAppendSnowflakeMappings("snowflake-staged")
	fence := managedAdmissionFence()
	cfg := StreamRunnerConfig{Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{}}
	return f, cfg
}

func managedAppendSnowflakeMappings(destination string) flow.TableMappings {
	return flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: []flow.DestinationTableMappings{{
		Destination:  destination,
		FutureTables: flow.FutureTableMapping{Action: flow.MappingActionExclude},
		Tables: []flow.TableMapping{{
			SourceSchema: "public", SourceTable: "widgets", Action: flow.MappingActionInclude,
			TargetSchema: "PUBLIC", TargetTable: "WALLABY_CHANGELOG",
			FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{{ .Column }}"},
			Write:         flow.TableWritePolicy{Mode: flow.TableWriteModeAppend},
		}},
	}}}
}

func assertManagedAppendPersistedAndRuntimeSpecsMatch(t *testing.T, f flow.Flow, destination stream.DestinationConfig) {
	t.Helper()
	persisted := cloneSpec(destination.Spec)
	projector, err := tablemap.New(f.Config.TableMappings, destination.Spec.Name)
	if err != nil {
		t.Fatal(err)
	}
	mapping, _ := f.Config.TableMappings.ForDestination(destination.Spec.Name)
	if err := projectManagedSnowflakeContract(&destination.Spec, mapping, projector); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(destination.Spec, persisted) {
		t.Fatalf("managed append runtime spec differs from persisted spec:\nruntime=%+v\npersisted=%+v", destination.Spec, persisted)
	}
}

func TestManagedAdmissionAcceptsStagedAppendProfileOnlyWithExactContract(t *testing.T) {
	f, cfg := managedStagedAdmissionFlowConfigured()
	destinations := managedSnowflakeStagedAdmissionDestinations(t)
	assertManagedAppendPersistedAndRuntimeSpecsMatch(t, f, destinations[0])
	if _, err := NewStreamRunner(f, &pgsource.Source{}, destinations, cfg); err != nil {
		t.Fatalf("valid staged admission rejected: %v", err)
	}

	tests := []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{name: "wrong flow binding", key: "flow_id", value: "other-flow", want: "does not match flow"},
		{name: "generic profile", key: "managed_profile", value: "", want: "does not match source profile"},
		{name: "obsolete write mode", key: "write_mode", value: "staged_append", want: "write_mode is obsolete"},
		{name: "staging resolution", key: "batch_resolution", value: "replace", want: "batch_resolution=none"},
		{name: "generic metadata", key: "meta_table_enabled", value: "true", want: "meta_table_enabled=false"},
		{name: "missing stage", key: "managed_stage", value: "", want: "managed_stage"},
		{name: "missing file format", key: "managed_file_format", value: "", want: "managed_file_format"},
		{name: "lowercase stage", key: "managed_stage", value: "wallaby_stage", want: "unquoted uppercase"},
		{name: "unpinned version", key: "managed_snowflake_version", value: "", want: "managed_snowflake_version"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			destinations := managedSnowflakeStagedAdmissionDestinations(t)
			destinations[0].Spec.Options[test.key] = test.value
			if _, err := NewStreamRunner(f, &pgsource.Source{}, destinations, cfg); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("staged admission %s error=%v, want %q", test.name, err, test.want)
			}
		})
	}
}

func TestManagedAdmissionStagedAppendRequiresSourceContract(t *testing.T) {
	f, cfg := managedStagedAdmissionFlowConfigured()
	wrongSlot := f
	wrongSlot.Source.Options = cloneStringMap(f.Source.Options)
	wrongSlot.Source.Options["slot"] = "fixed"
	if _, err := NewStreamRunner(wrongSlot, &pgsource.Source{}, managedSnowflakeStagedAdmissionDestinations(t), cfg); err == nil || !strings.Contains(err.Error(), "slot=managed") {
		t.Fatalf("staged admission without a managed slot error=%v", err)
	}
	noToast := f
	noToast.Source.Options = cloneStringMap(f.Source.Options)
	delete(noToast.Source.Options, "toast_fetch")
	if _, err := NewStreamRunner(noToast, &pgsource.Source{}, managedSnowflakeStagedAdmissionDestinations(t), cfg); err == nil || !strings.Contains(err.Error(), "toast_fetch=off") {
		t.Fatalf("staged admission without toast_fetch=off error=%v", err)
	}
}

func TestManagedAdmissionStagedAppendRequiresPipeForAutoIngest(t *testing.T) {
	f, cfg := managedStagedAdmissionFlowConfigured()
	destinations := managedSnowflakeStagedAdmissionDestinations(t)
	destinations[0].Spec.Options["managed_auto_ingest"] = "true"
	if _, err := NewStreamRunner(f, &pgsource.Source{}, destinations, cfg); err == nil || !strings.Contains(err.Error(), "managed_pipe") {
		t.Fatalf("staged auto-ingest admission without a pipe error=%v", err)
	}
}
