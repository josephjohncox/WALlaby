package snowflake

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/snowflakedb/gosnowflake"
)

func streamValidOptions(t *testing.T) (string, map[string]string) {
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
		"dsn": dsn, "flow_id": "flow-1", "managed_profile": connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1,
		"destination_revision_id": "snowflake-streaming-v1", "batch_mode": "target", "batch_resolution": "none",
		"meta_table_enabled": "false", "disable_transactions": "false", "session_keep_alive": "false",
		"managed_streaming_transport": streamRequiredTransport,
		"managed_account":             "ACCOUNT", "managed_database": "DB", "managed_schema": "PUBLIC", "managed_pipe": "WALLABY_PIPE",
		"managed_table": "WALLABY_CHANGELOG", "managed_receipts_table": "WALLABY_RECEIPTS", "managed_channel_state_table": "WALLABY_CHANNELS",
		"managed_channel_name_prefix": "wallaby_stream", "managed_owner_role": "WALLABY_OWNER", "managed_execution_role": "ROLE", "managed_warehouse": "WH",
		"managed_snowflake_version": "8.0.0", "managed_pipe_created_on": created, "managed_target_created_on": created,
		"managed_receipts_created_on": created, "managed_channel_state_created_on": created, "managed_request_journal_created_on": created,
		"managed_source_schema": "public", "managed_source_table": "widgets",
		"managed_schema_contract": string(schemaJSON), "managed_schema_contract_hash": hash,
		"managed_max_transaction_rows": "1000", "managed_max_transaction_bytes": "4194304",
		"managed_max_transaction_fragments": "128", "managed_max_row_bytes": "1048576", "managed_max_open_conns": "4",
		"managed_statement_timeout_seconds": "600", "managed_observe_attempts": "60", "managed_observe_interval_ms": "1000",
		"managed_append_attempts": "16", "managed_append_backoff_ms": "250",
		"managed_cleanup_max_objects": "1000", "managed_cleanup_retention_seconds": "2592000",
	}
	return dsn, options
}

func TestStreamConfigFromSpecAdmitsValidSpec(t *testing.T) {
	t.Parallel()
	dsn, options := streamValidOptions(t)
	cfg, err := streamConfigFromSpec(dsn, connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: options})
	if err != nil {
		t.Fatalf("valid streaming spec rejected: %v", err)
	}
	if cfg.pipe != "WALLABY_PIPE" || cfg.table != "WALLABY_CHANGELOG" || cfg.channelStateTable != "WALLABY_CHANNELS" || cfg.requestJournalCreatedOn == "" {
		t.Fatalf("streaming config identity=%+v", cfg)
	}
}

func TestStreamConfigRejectsLossyAndUnsafeOptions(t *testing.T) {
	t.Parallel()
	cases := map[string]func(map[string]string){
		"obsolete write mode":             func(o map[string]string) { o["write_mode"] = "streaming_append" },
		"batch mode":                      func(o map[string]string) { o["batch_mode"] = "staging" },
		"meta table":                      func(o map[string]string) { o["meta_table_enabled"] = "true" },
		"keep alive":                      func(o map[string]string) { o["session_keep_alive"] = "true" },
		"type override":                   func(o map[string]string) { o["type_mappings"] = "text=STRING" },
		"generic staging":                 func(o map[string]string) { o["staging_table"] = "X" },
		"same role":                       func(o map[string]string) { o["managed_execution_role"] = o["managed_owner_role"] },
		"unknown option":                  func(o map[string]string) { o["nonsense"] = "1" },
		"missing transport":               func(o map[string]string) { delete(o, "managed_streaming_transport") },
		"wrong transport":                 func(o map[string]string) { o["managed_streaming_transport"] = "some-other-transport" },
		"bad contract":                    func(o map[string]string) { o["managed_schema_contract_hash"] = "deadbeef" },
		"missing created":                 func(o map[string]string) { o["managed_channel_state_created_on"] = "" },
		"missing request journal created": func(o map[string]string) { o["managed_request_journal_created_on"] = "" },
		"lowercase ident":                 func(o map[string]string) { o["managed_pipe"] = "wallaby_pipe" },
		"bad channel prefix":              func(o map[string]string) { o["managed_channel_name_prefix"] = "bad prefix!" },
		"missing channel tbl":             func(o map[string]string) { o["managed_channel_state_table"] = "" },
		"REST transaction overflow":       func(o map[string]string) { o["managed_max_transaction_bytes"] = "4194305" },
		"row exceeds REST transaction":    func(o map[string]string) { o["managed_max_row_bytes"] = "4194305" },
	}
	for name, mutate := range cases {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dsn, options := streamValidOptions(t)
			mutate(options)
			if _, err := streamConfigFromSpec(dsn, connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: options}); err == nil {
				t.Fatalf("streaming admission accepted an unsafe spec (%s)", name)
			}
		})
	}
}

func TestStreamConfigRequiresFailClosedTransport(t *testing.T) {
	t.Parallel()
	_, options := streamValidOptions(t)
	insecure := managedSnowflakeTestDSN(t, func(cfg *gosnowflake.Config) {
		cfg.OCSPFailOpen = gosnowflake.OCSPFailOpenTrue
	})
	options["dsn"] = insecure
	if _, err := streamConfigFromSpec(insecure, connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: options}); err == nil || !strings.Contains(err.Error(), "prohibited credential or connection control") {
		t.Fatalf("OCSP fail-open admission error=%v, want centralized transport rejection", err)
	}
}

func TestStreamConfigRequiresReadLatestWrites(t *testing.T) {
	t.Parallel()
	_, options := streamValidOptions(t)
	noReadLatest := managedSnowflakeTestDSN(t, func(cfg *gosnowflake.Config) {
		timezone := "UTC"
		cfg.Params = map[string]*string{"TIMEZONE": &timezone}
	})
	options["dsn"] = noReadLatest
	if _, err := streamConfigFromSpec(noReadLatest, connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: options}); err == nil || !strings.Contains(err.Error(), "READ_LATEST_WRITES") {
		t.Fatalf("missing READ_LATEST_WRITES error=%v", err)
	}
}

// TestStreamSnowflakeDSNRedactsSecrets proves the dedicated redaction guard: an
// admitted DSN can never carry an inline password, token, or *secret* query
// parameter, so a channel-open, append, or observe error can never leak a
// credential. Key-pair JWT with verified HTTPS, fail-closed OCSP, and only the
// reviewed identity/session allowlist is accepted.
func TestStreamSnowflakeDSNRedactsSecrets(t *testing.T) {
	t.Parallel()
	rejected := map[string]string{
		"inline password": "user@account/db/schema?password=hunter2",
		"inline token":    "user@account/db/schema?token=abc.def.ghi",
		"client secret":   "user@account/db/schema?client_secret=shhh",
		"oauth secret":    "user@account/db/schema?authenticator=oauth&refresh_secret=leak",
	}
	for name, dsn := range rejected {
		name, dsn := name, dsn
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := streamSnowflakeDSNRedactsSecrets(dsn); err == nil {
				t.Fatalf("DSN with an inline secret must be rejected (%s)", name)
			}
		})
	}
	accepted := map[string]string{
		"jwt only":         "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false",
		"safe params only": "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false&READ_LATEST_WRITES=true&TIMEZONE=UTC",
	}
	for name, dsn := range accepted {
		name, dsn := name, dsn
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := streamSnowflakeDSNRedactsSecrets(dsn); err != nil {
				t.Fatalf("DSN without inline secrets must be accepted (%s): %v", name, err)
			}
		})
	}
}

// TestStreamTransportUnavailableFailsClosed proves the executable fail-closed
// invariant: no reviewed high-performance append transport is linked, so
// ManagedStreamingTransportAvailable() is false and a validated spec still
// refuses managed admission at Open without any network side effect.
func TestStreamTransportUnavailableFailsClosed(t *testing.T) {
	t.Parallel()
	if ManagedStreamingTransportAvailable() {
		t.Fatal("no reviewed high-performance append transport should be linked in this build")
	}
	dsn, options := streamValidOptions(t)
	destination := &Destination{deploymentPolicy: snowflakeTestPolicy(t)}
	err := destination.Open(context.Background(), connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: options})
	if !errors.Is(err, ErrManagedStreamingTransportUnavailable) {
		t.Fatalf("streaming Open must fail closed with the transport-unavailable error, got %v", err)
	}
	_ = dsn
}

// TestStreamOpenRejectsInvalidSpecBeforeTransportCheck proves the side-effect-free
// spec validation runs first, so an invalid spec is rejected with a precise
// admission error rather than the generic transport refusal.
func TestStreamOpenRejectsInvalidSpecBeforeTransportCheck(t *testing.T) {
	t.Parallel()
	_, options := streamValidOptions(t)
	options["managed_schema_contract_hash"] = "deadbeef"
	destination := &Destination{deploymentPolicy: snowflakeTestPolicy(t)}
	err := destination.Open(context.Background(), connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: options})
	if err == nil || errors.Is(err, ErrManagedStreamingTransportUnavailable) {
		t.Fatalf("invalid spec must be rejected before the transport refusal, got %v", err)
	}
}
