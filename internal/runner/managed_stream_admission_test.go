package runner

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	snowflakedest "github.com/josephjohncox/wallaby/connectors/destinations/snowflake"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func managedSnowflakeStreamingAdmissionDestinations(t *testing.T) []stream.DestinationConfig {
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
		Spec: connector.RuntimeSpec{Name: "snowflake-streaming", Type: connector.EndpointSnowflake, Options: map[string]string{
			"dsn":                               managedAdmissionSnowflakeDSN(t, nil),
			"flow_id":                           "managed-flow",
			"managed_profile":                   connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1,
			"destination_revision_id":           "snowflake-streaming-v1",
			"batch_mode":                        "target",
			"batch_resolution":                  "none",
			"meta_table_enabled":                "false",
			"disable_transactions":              "false",
			"session_keep_alive":                "false",
			"managed_streaming_transport":       "snowpipe-streaming-highperf-rest",
			"managed_account":                   "ACCOUNT",
			"managed_database":                  "DB",
			"managed_schema":                    "PUBLIC",
			"managed_pipe":                      "WALLABY_PIPE",
			"managed_table":                     "WALLABY_CHANGELOG",
			"managed_receipts_table":            "WALLABY_RECEIPTS",
			"managed_channel_state_table":       "WALLABY_CHANNELS",
			"managed_channel_name_prefix":       "wallaby_stream",
			"managed_owner_role":                "OWNER_ROLE",
			"managed_execution_role":            "ROLE",
			"managed_warehouse":                 "WH",
			"managed_snowflake_version":         "9.99.0",
			"managed_pipe_created_on":           created,
			"managed_target_created_on":         created,
			"managed_receipts_created_on":       created,
			"managed_channel_state_created_on":  created,
			"managed_source_schema":             "public",
			"managed_source_table":              "widgets",
			"managed_schema_contract":           string(encoded),
			"managed_schema_contract_hash":      hash,
			"managed_max_transaction_rows":      "1000",
			"managed_max_transaction_bytes":     "8388608",
			"managed_max_transaction_fragments": "64",
			"managed_max_row_bytes":             "1048576",
			"managed_max_open_conns":            "4",
			"managed_statement_timeout_seconds": "600",
			"managed_observe_attempts":          "60",
			"managed_observe_interval_ms":       "1000",
			"managed_append_attempts":           "16",
			"managed_append_backoff_ms":         "250",
			"managed_cleanup_max_objects":       "1000",
			"managed_cleanup_retention_seconds": "2592000",
		}},
		Dest: &snowflakedest.Destination{},
	}}
}

func managedStreamingAdmissionFlowConfigured() (flow.Flow, StreamRunnerConfig) {
	f := managedAdmissionFlow()
	setRunnerSourceOptions(&f, map[string]string{
		"managed": "", "managed_profile": connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1,
		"create_slot": "true", "slot": "managed", "streaming_transactions": "true", "toast_fetch": "off",
		"max_transaction_records": "1000", "max_transaction_bytes": "8388608", "max_transaction_fragments": "64",
	})
	f.Destinations = []*wallabypb.Endpoint{runnerTestDestination(connector.RuntimeSpec{Name: "snowflake-streaming", Type: connector.EndpointSnowflake})}
	f.Config.TableMappings = managedAppendSnowflakeMappings("snowflake-streaming")
	fence := managedAdmissionFence()
	cfg := StreamRunnerConfig{Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{}}
	return f, cfg
}

// TestManagedAdmissionStreamingAppendFailsClosedWithoutTransport is the core
// fail-closed proof: an otherwise perfect streaming REST append contract is
// still refused at admission because no reviewed high-performance append
// transport is linked. Startup must not proceed to local-token theater.
func TestManagedAdmissionStreamingAppendFailsClosedWithoutTransport(t *testing.T) {
	if snowflakedest.ManagedStreamingTransportAvailable() {
		t.Skip("a reviewed high-performance append transport is linked; the fail-closed contract no longer applies")
	}
	f, cfg := managedStreamingAdmissionFlowConfigured()
	destinations := managedSnowflakeStreamingAdmissionDestinations(t)
	assertManagedAppendPersistedAndRuntimeSpecsMatch(t, f, destinations[0])
	_, err := NewStreamRunner(f, &pgsource.Source{}, destinations, cfg)
	if err == nil {
		t.Fatal("streaming admission must fail closed without a reviewed append transport")
	}
	if !errors.Is(err, snowflakedest.ErrManagedStreamingTransportUnavailable) {
		t.Fatalf("streaming admission error=%v, want the transport-unavailable refusal", err)
	}
}

// TestManagedAdmissionStreamingAppendRejectsMisconfigBeforeTransportRefusal
// proves the full admission contract is executable: each misconfiguration is
// rejected with its own precise message rather than the generic transport
// refusal, so the fail-closed boundary is exact rather than a blanket denial.
func TestManagedAdmissionStreamingAppendRejectsMisconfigBeforeTransportRefusal(t *testing.T) {
	f, cfg := managedStreamingAdmissionFlowConfigured()
	tests := []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{name: "wrong flow binding", key: "flow_id", value: "other-flow", want: "does not match flow"},
		{name: "generic profile", key: "managed_profile", value: "", want: "does not match source profile"},
		{name: "obsolete write mode", key: "write_mode", value: "streaming_append", want: "write_mode is obsolete"},
		{name: "staging resolution", key: "batch_resolution", value: "replace", want: "batch_resolution=none"},
		{name: "generic metadata", key: "meta_table_enabled", value: "true", want: "meta_table_enabled=false"},
		{name: "missing pipe", key: "managed_pipe", value: "", want: "managed_pipe"},
		{name: "missing channel state table", key: "managed_channel_state_table", value: "", want: "managed_channel_state_table"},
		{name: "lowercase pipe", key: "managed_pipe", value: "wallaby_pipe", want: "unquoted uppercase"},
		{name: "missing transport", key: "managed_streaming_transport", value: "", want: "managed_streaming_transport"},
		{name: "unpinned version", key: "managed_snowflake_version", value: "", want: "managed_snowflake_version"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			destinations := managedSnowflakeStreamingAdmissionDestinations(t)
			destinations[0].Spec.Options[test.key] = test.value
			_, err := NewStreamRunner(f, &pgsource.Source{}, destinations, cfg)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("streaming admission %s error=%v, want %q", test.name, err, test.want)
			}
			if errors.Is(err, snowflakedest.ErrManagedStreamingTransportUnavailable) {
				t.Fatalf("streaming admission %s returned the transport refusal instead of the precise error: %v", test.name, err)
			}
		})
	}
}

func TestManagedAdmissionStreamingAppendRequiresSourceContract(t *testing.T) {
	f, cfg := managedStreamingAdmissionFlowConfigured()
	noToast := flow.Clone(f)
	setRunnerSourceOptions(&noToast, map[string]string{"toast_fetch": ""})
	if _, err := NewStreamRunner(noToast, &pgsource.Source{}, managedSnowflakeStreamingAdmissionDestinations(t), cfg); err == nil || !strings.Contains(err.Error(), "toast_fetch=off") {
		t.Fatalf("streaming admission without toast_fetch=off error=%v", err)
	}
}
