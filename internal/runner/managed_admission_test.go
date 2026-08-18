package runner

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	icebergdest "github.com/josephjohncox/wallaby/connectors/destinations/iceberg"
	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	snowflakedest "github.com/josephjohncox/wallaby/connectors/destinations/snowflake"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"github.com/snowflakedb/gosnowflake"
)

func TestManagedAdmissionUsesResolvedDeploymentDDLPolicy(t *testing.T) {
	t.Parallel()
	f := managedAdmissionFlow()
	f.Config.DDL.AutoApply = nil
	defaults := flow.DDLPolicyDefaults{AutoApply: true}
	fence := managedAdmissionFence()
	_, err := NewStreamRunner(f, &pgsource.Source{}, managedAdmissionDestinations(), StreamRunnerConfig{
		Checkpoints: managedCheckpointStore{}, RunFence: &fence,
		DeliveryCoordinator: &delivery.Coordinator{}, DDLPolicyDefaults: &defaults,
	})
	if err == nil || !strings.Contains(err.Error(), "rejects automatic raw-SQL DDL") {
		t.Fatalf("managed admission with omitted flow auto_apply and deployment=true error=%v", err)
	}

	disabled := false
	f.Config.DDL.AutoApply = &disabled
	if _, err := NewStreamRunner(f, &pgsource.Source{}, managedAdmissionDestinations(), StreamRunnerConfig{
		Checkpoints: managedCheckpointStore{}, RunFence: &fence,
		DeliveryCoordinator: &delivery.Coordinator{}, DDLPolicyDefaults: &defaults,
	}); err != nil {
		t.Fatalf("explicit flow auto_apply=false did not override deployment=true: %v", err)
	}
}

func TestManagedAdmissionRequiresExactMaterializedContract(t *testing.T) {
	t.Parallel()

	f := managedAdmissionFlow()
	f.Config.AckPolicy = stream.AckPolicyMaterialized
	f.Config.Materialization = flow.MaterializationPolicy{ProjectionID: "canonical_cdc_parquet_v2"}
	destinations := materializedAdmissionDestinations()
	f.Destinations = []*wallabypb.Endpoint{runnerTestDestination(destinations[0].Spec)}
	f.Config.TableMappings = flow.NewTableMappings([]connector.RuntimeSpec{destinations[0].Spec})
	fence := managedAdmissionFence()
	cfg := StreamRunnerConfig{
		Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{},
	}
	if _, err := NewStreamRunner(f, &pgsource.Source{}, destinations, cfg); err == nil || !strings.Contains(err.Error(), "artifact log") {
		t.Fatalf("missing artifact log error=%v", err)
	}
	cfg.ArtifactLog = materializedAdmissionLog{}
	if _, err := NewStreamRunner(f, &pgsource.Source{}, destinations, cfg); err != nil {
		t.Fatal(err)
	}

	f.Config.Materialization.ProjectionID = "parquet"
	if _, err := NewStreamRunner(f, &pgsource.Source{}, destinations, cfg); err == nil || !strings.Contains(err.Error(), "canonical_cdc_parquet_v2") {
		t.Fatalf("wrong projection error=%v", err)
	}
}

func materializedAdmissionDestinations() []stream.DestinationConfig {
	return []stream.DestinationConfig{{
		Spec: connector.RuntimeSpec{Name: "iceberg", Type: connector.EndpointIceberg, Options: map[string]string{
			"destination_revision_id": "iceberg-append-v1", "catalog_profile": "s3tables", "control_table": "wallaby.control",
		}},
		Dest: &icebergdest.Destination{},
	}}
}

type materializedAdmissionLog struct{}

func (materializedAdmissionLog) Recover(context.Context, connector.RunFence) error { return nil }
func (materializedAdmissionLog) RestoreCheckpoint(_ context.Context, _ connector.RunFence, checkpoint connector.Checkpoint) (connector.AckGrant, error) {
	positionID, err := connector.CheckpointPositionID(checkpoint)
	return connector.AckGrant{Checkpoint: checkpoint, PositionID: positionID}, err
}
func (materializedAdmissionLog) WaitForReadAdmission(context.Context, connector.RunFence) error {
	return nil
}
func (materializedAdmissionLog) Append(_ context.Context, _ connector.RunFence, transaction connector.SourceTransaction, _ connector.ManagedSchemaBaselinePayload) (connector.AckGrant, error) {
	positionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
	return connector.AckGrant{Checkpoint: transaction.Checkpoint, PositionID: positionID}, err
}

func TestManagedAdmissionAcceptsAppendOnlyIcebergArtifactConsumer(t *testing.T) {
	t.Parallel()
	f := managedAdmissionFlow()
	f.Config.AckPolicy = stream.AckPolicyMaterialized
	f.Config.Materialization = flow.MaterializationPolicy{ProjectionID: "canonical_cdc_parquet_v2"}
	fence := managedAdmissionFence()
	destinations := materializedAdmissionDestinations()
	f.Destinations = []*wallabypb.Endpoint{runnerTestDestination(destinations[0].Spec)}
	f.Config.TableMappings = flow.NewTableMappings([]connector.RuntimeSpec{destinations[0].Spec})
	if _, err := NewStreamRunner(f, &pgsource.Source{}, destinations, StreamRunnerConfig{
		Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{}, ArtifactLog: materializedAdmissionLog{},
	}); err != nil {
		t.Fatal(err)
	}

	setRunnerSourceOptions(&f, map[string]string{"bootstrap": "auto", "pool_max_conns": "2"})
	if _, err := NewStreamRunner(f, &pgsource.Source{}, destinations, StreamRunnerConfig{
		Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{}, ArtifactLog: materializedAdmissionLog{},
	}); err == nil || !strings.Contains(err.Error(), "bootstrap=never") {
		t.Fatalf("Iceberg bootstrap admission error=%v", err)
	}
}

func TestManagedAdmissionAcceptsInitialPostgresProfile(t *testing.T) {
	for _, bootstrapMode := range []string{"never", "auto", "required"} {
		t.Run(bootstrapMode, func(t *testing.T) {
			f := managedAdmissionFlow()
			values := map[string]string{"bootstrap": bootstrapMode}
			if bootstrapMode != "never" {
				values["ensure_publication"] = "true"
			}
			setRunnerSourceOptions(&f, values)
			fence := managedAdmissionFence()
			_, err := NewStreamRunner(f, &pgsource.Source{}, managedAdmissionDestinations(), StreamRunnerConfig{
				Checkpoints:         managedCheckpointStore{},
				RunFence:            &fence,
				DeliveryCoordinator: &delivery.Coordinator{},
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManagedProfileCannotBypassManagedAdmission(t *testing.T) {
	f := managedAdmissionFlow()
	setRunnerSourceOptions(&f, map[string]string{"managed": "", "managed_profile": connector.ManagedProfilePostgresToPostgresV1, "bootstrap": "required", "streaming_transactions": "true"})
	destinations := managedAdmissionDestinations()
	destinations[0].Spec.Options["managed_profile"] = connector.ManagedProfilePostgresToPostgresV1

	_, err := NewStreamRunner(f, &pgsource.Source{}, destinations, StreamRunnerConfig{Checkpoints: managedCheckpointStore{}})
	if err == nil || !strings.Contains(err.Error(), "PostgreSQL run authority") {
		t.Fatalf("named profile admission error=%v, want managed authority requirement", err)
	}
	fence := managedAdmissionFence()
	runner, err := NewStreamRunner(f, &pgsource.Source{}, destinations, StreamRunnerConfig{
		Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !runner.ManagedProfileEnabled() {
		t.Fatal("named profile passed admission but fell through to generic stream execution")
	}
}

func TestManagedAdmissionAcceptsNamedPostgresProfileOnlyWithExactContract(t *testing.T) {
	f := managedAdmissionFlow()
	setRunnerSourceOptions(&f, map[string]string{"managed_profile": connector.ManagedProfilePostgresToPostgresV1, "bootstrap": "required", "streaming_transactions": "true"})
	destinations := managedAdmissionDestinations()
	destinations[0].Spec.Options["managed_profile"] = connector.ManagedProfilePostgresToPostgresV1
	fence := managedAdmissionFence()
	if _, err := NewStreamRunner(f, &pgsource.Source{}, destinations, StreamRunnerConfig{
		Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{},
	}); err != nil {
		t.Fatal(err)
	}

	setRunnerSourceOptions(&f, map[string]string{"streaming_transactions": "false"})
	if _, err := NewStreamRunner(f, &pgsource.Source{}, destinations, StreamRunnerConfig{
		Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{},
	}); err == nil || !strings.Contains(err.Error(), "streaming_transactions=true") {
		t.Fatalf("named profile error=%v", err)
	}
}

func TestManagedAdmissionAcceptsClickHouseAppendProfileOnlyWithExactContract(t *testing.T) {
	f := managedAdmissionFlow()
	setRunnerSourceOptions(&f, map[string]string{
		"managed": "", "managed_profile": connector.ManagedProfilePostgresToClickHouseAppendV1, "streaming_transactions": "true",
		"max_transaction_records": "100000", "max_transaction_bytes": "134217728", "max_transaction_fragments": "128",
	})
	destinations := managedClickHouseAdmissionDestinations()
	fence := managedAdmissionFence()
	cfg := StreamRunnerConfig{Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{}}
	if _, err := NewStreamRunner(f, &pgsource.Source{}, destinations, cfg); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{name: "staging", key: "batch_mode", value: "staging", want: "batch_mode=target"},
		{name: "metadata mutations", key: "meta_table_enabled", value: "true", want: "meta_table_enabled=false"},
		{name: "async insert", key: "async_insert", value: "true", want: "async_insert=false"},
		{name: "fire and forget", key: "wait_for_async_insert", value: "false", want: "wait_for_async_insert=true"},
		{name: "single-replica quorum", key: "insert_quorum", value: "1", want: "insert_quorum=2"},
		{name: "unmanaged engine", key: "managed_deployment", value: "standalone", want: "self-managed-keeper"},
		{name: "cloud without evidence", key: "managed_deployment", value: "clickhouse-cloud", want: "self-managed-keeper"},
		{name: "generic staging resolution", key: "batch_resolution", value: "replace", want: "batch_resolution=none"},
		{name: "plaintext transport", key: "dsn", value: "clickhouse://localhost:9000/wallaby", want: "verified native TLS"},
		{name: "unverified transport", key: "dsn", value: "clickhouse://localhost:9440/wallaby?secure=true&skip_verify=true", want: "skip_verify"},
		{name: "missing replica endpoint", key: "managed_replica_dsn", value: "", want: "managed_replica_dsn"},
		{name: "plaintext replica", key: "managed_replica_dsn", value: "clickhouse://replica-2:9000/wallaby", want: "verified native TLS"},
		{name: "same replica endpoint", key: "managed_replica_dsn", value: "clickhouse://localhost:9440/wallaby?secure=true", want: "distinct primary and replica"},
		{name: "single replica", key: "managed_replica_names", value: "replica-1", want: "exactly two"},
		{name: "missing Keeper endpoint", key: "managed_keeper_address", value: "", want: "managed_keeper_address"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copyDestinations := managedClickHouseAdmissionDestinations()
			copyDestinations[0].Spec.Options[tt.key] = tt.value
			_, err := NewStreamRunner(f, &pgsource.Source{}, copyDestinations, cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error=%v, want substring %q", err, tt.want)
			}
		})
	}
	for _, tt := range []struct {
		key, value, want string
	}{
		{key: "max_transaction_records", value: "100001", want: "max_transaction_records"},
		{key: "max_transaction_bytes", value: "134217729", want: "max_transaction_bytes"},
		{key: "max_transaction_fragments", value: "129", want: "max_transaction_fragments"},
	} {
		t.Run("source "+tt.key, func(t *testing.T) {
			copyFlow := flow.Clone(f)
			setRunnerSourceOptions(&copyFlow, map[string]string{tt.key: tt.value})
			_, err := NewStreamRunner(copyFlow, &pgsource.Source{}, managedClickHouseAdmissionDestinations(), cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error=%v, want substring %q", err, tt.want)
			}
		})
	}
}

func testSnowflakePolicy(t *testing.T) connector.SnowflakeDeploymentPolicy {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := connector.NewSnowflakeDeploymentPolicyWithPrivateKey("account", "user", "account.snowflakecomputing.com", key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = policy.Close() })
	return policy
}

func TestManagedAdmissionAcceptsSnowflakeSQLProfileOnlyWithExactContract(t *testing.T) {
	f := managedSnowflakeFlowForTest()
	destinations := managedSnowflakeAdmissionDestinations(t)
	persistedContract := destinations[0].Spec.Options["managed_schema_contract"]
	fence := managedAdmissionFence()
	cfg := StreamRunnerConfig{Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{}, SnowflakePolicy: testSnowflakePolicy(t)}
	runner, err := NewStreamRunner(f, &pgsource.Source{}, destinations, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var projected connector.Schema
	if err := json.Unmarshal([]byte(runner.Destinations[0].Spec.Options["managed_schema_contract"]), &projected); err != nil {
		t.Fatal(err)
	}
	if projected.Namespace != "PUBLIC" || projected.Name != "WIDGETS" || runner.Destinations[0].Spec.Options["managed_schema_contract_hash"] == destinations[0].Spec.Options["managed_schema_contract_hash"] {
		t.Fatalf("projected cloned Snowflake contract=%+v", projected)
	}
	if destinations[0].Spec.Options["managed_schema_contract"] != persistedContract {
		t.Fatal("persisted destination spec was mutated")
	}

	tests := []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{name: "wrong flow binding", key: "flow_id", value: "other-flow", want: "does not match flow"},
		{name: "generic profile", key: "managed_profile", value: "", want: "does not match source profile"},
		{name: "staging", key: "batch_mode", value: "staging", want: "batch_mode=target"},
		{name: "staging resolution", key: "batch_resolution", value: "replace", want: "batch_resolution=none"},
		{name: "generic metadata", key: "meta_table_enabled", value: "true", want: "meta_table_enabled=false"},
		{name: "transactions disabled", key: "disable_transactions", value: "true", want: "disable_transactions=false"},
		{name: "session kept alive", key: "session_keep_alive", value: "true", want: "session_keep_alive=false"},
		{name: "http transport", key: "dsn", value: managedAdmissionSnowflakeDSN(t, func(cfg *gosnowflake.Config) { cfg.Protocol = "http" }), want: "prohibited credential or connection control"},
		{name: "OCSP fail-open", key: "dsn", value: managedAdmissionSnowflakeDSN(t, func(cfg *gosnowflake.Config) { cfg.OCSPFailOpen = gosnowflake.OCSPFailOpenTrue }), want: "prohibited credential or connection control"},
		{name: "password authentication", key: "dsn", value: "user:pass@account/DB/PUBLIC?warehouse=WH&role=ROLE&ocspFailOpen=false&READ_LATEST_WRITES=true&TIMEZONE=UTC", want: "prohibited credential"},
		{name: "stale cross-session reads", key: "dsn", value: managedAdmissionSnowflakeDSN(t, func(cfg *gosnowflake.Config) { delete(cfg.Params, "READ_LATEST_WRITES") }), want: "READ_LATEST_WRITES=true"},
		{name: "non-UTC session", key: "dsn", value: managedAdmissionSnowflakeDSN(t, func(cfg *gosnowflake.Config) { value := "local"; cfg.Params["TIMEZONE"] = &value }), want: "TIMEZONE=UTC"},
		{name: "persistent DSN sessions", key: "dsn", value: managedAdmissionSnowflakeDSN(t, func(cfg *gosnowflake.Config) { value := "true"; cfg.Params["CLIENT_SESSION_KEEP_ALIVE"] = &value }), want: "CLIENT_SESSION_KEEP_ALIVE=true"},
		{name: "account mismatch", key: "managed_account", value: "OTHER", want: "DSN account"},
		{name: "database mismatch", key: "managed_database", value: "OTHER", want: "DSN database"},
		{name: "schema mismatch", key: "managed_schema", value: "OTHER", want: "mapped target"},
		{name: "role mismatch", key: "managed_execution_role", value: "OTHER", want: "DSN role"},
		{name: "execution role owns objects", key: "managed_owner_role", value: "ROLE", want: "must not own"},
		{name: "warehouse mismatch", key: "managed_warehouse", value: "OTHER", want: "DSN warehouse"},
		{name: "missing table", key: "managed_table", value: "", want: "managed_table"},
		{name: "qualified table", key: "managed_table", value: "PUBLIC.WIDGETS", want: "unquoted uppercase identifier"},
		{name: "unsafe destination revision", key: "destination_revision_id", value: "revision:two", want: "letters, digits"},
		{name: "missing receipt table", key: "managed_receipts_table", value: "", want: "managed_receipts_table"},
		{name: "missing source table", key: "managed_source_table", value: "", want: "managed_source_table"},
		{name: "missing schema contract", key: "managed_schema_contract", value: "", want: "managed_schema_contract"},
		{name: "invalid schema hash", key: "managed_schema_contract_hash", value: "sha256", want: "64 lowercase hexadecimal"},
		{name: "mismatched schema hash", key: "managed_schema_contract_hash", value: strings.Repeat("b", 64), want: "does not identify"},
		{name: "unpinned Snowflake version", key: "managed_snowflake_version", value: "", want: "managed_snowflake_version"},
		{name: "zero rows", key: "managed_max_transaction_rows", value: "0", want: "positive integer"},
		{name: "oversized transaction", key: "managed_max_transaction_bytes", value: "8388609", want: "bounds exceed"},
		{name: "too many connections", key: "managed_max_open_conns", value: "9", want: "between 1 and 8"},
		{name: "unknown managed option", key: "managed_typo", value: "true", want: "does not allow option managed_typo"},
		{name: "removed type mapping file", key: "type_mappings_file", value: "mappings.json", want: "does not allow option type_mappings_file"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copyDestinations := managedSnowflakeAdmissionDestinations(t)
			copyDestinations[0].Spec.Options[tt.key] = tt.value
			_, err := NewStreamRunner(f, &pgsource.Source{}, copyDestinations, cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error=%v, want substring %q", err, tt.want)
			}
		})
	}

	wrongSlot := flow.Clone(f)
	setRunnerSourceOptions(&wrongSlot, map[string]string{"slot": "operator_slot"})
	if _, err := NewStreamRunner(wrongSlot, &pgsource.Source{}, managedSnowflakeAdmissionDestinations(t), cfg); err == nil || !strings.Contains(err.Error(), "slot=managed") {
		t.Fatalf("non-derived slot error=%v", err)
	}

	for _, toastFetch := range []string{"", "source", "full", "cache"} {
		copyFlow := flow.Clone(f)
		setRunnerSourceOptions(&copyFlow, map[string]string{"toast_fetch": toastFetch})
		if _, err := NewStreamRunner(copyFlow, &pgsource.Source{}, managedSnowflakeAdmissionDestinations(t), cfg); err == nil || !strings.Contains(err.Error(), "toast_fetch=off") {
			t.Errorf("toast_fetch=%q error=%v", toastFetch, err)
		}
	}

	copyFlow := flow.Clone(f)
	setRunnerSourceOptions(&copyFlow, map[string]string{"max_transaction_records": "1001"})
	if _, err := NewStreamRunner(copyFlow, &pgsource.Source{}, managedSnowflakeAdmissionDestinations(t), cfg); err == nil || !strings.Contains(err.Error(), "max_transaction_records") {
		t.Fatalf("source bound error=%v", err)
	}
}

func TestNewStreamRunnerManagedSnowflakePreservesWhitespaceOnlySourceAdmission(t *testing.T) {
	f := managedSnowflakeFlowForTest()
	mapping, _ := f.Config.TableMappings.ForDestination("target")
	mapping.Tables[0].SourceSchema = " "
	mapping.Tables[0].SourceTable = " "
	f.Config.TableMappings.Destinations[0] = mapping
	sourceContract := connector.Schema{Namespace: " ", Name: " ", Columns: []connector.Column{{Name: "id", Type: "int8", TypeMetadata: map[string]string{"primary_key": "true", "primary_key_ordinal": "1", "replica_identity": "true", "nullability_known": "true", "generated_known": "true"}}}}
	destinations := managedSnowflakeAdmissionDestinationsForContract(t, sourceContract)
	destinations[0].Spec.Options["managed_source_schema"] = " "
	destinations[0].Spec.Options["managed_source_table"] = " "
	fence := managedAdmissionFence()
	runner, err := NewStreamRunner(f, &pgsource.Source{}, destinations, StreamRunnerConfig{Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{}, SnowflakePolicy: testSnowflakePolicy(t)})
	if err != nil {
		t.Fatalf("NewStreamRunner rejected whitespace-only exact Snowflake source admission: %v", err)
	}
	options := runner.Destinations[0].Spec.Options
	if options["managed_source_schema"] != " " || options["managed_source_table"] != " " {
		t.Fatalf("NewStreamRunner normalized exact Snowflake source identity: schema/table=%q/%q", options["managed_source_schema"], options["managed_source_table"])
	}
	var projected connector.Schema
	if err := json.Unmarshal([]byte(options["managed_schema_contract"]), &projected); err != nil {
		t.Fatal(err)
	}
	if projected.Namespace != "PUBLIC" || projected.Name != "WIDGETS" {
		t.Fatalf("NewStreamRunner projected contract=%+v, want exact target PUBLIC.WIDGETS", projected)
	}
	for _, key := range []string{"managed_source_schema", "managed_source_table"} {
		invalidDestinations := managedSnowflakeAdmissionDestinationsForContract(t, sourceContract)
		invalidDestinations[0].Spec.Options["managed_source_schema"] = " "
		invalidDestinations[0].Spec.Options["managed_source_table"] = " "
		invalidDestinations[0].Spec.Options[key] = "bad\x00identifier"
		if _, err := NewStreamRunner(f, &pgsource.Source{}, invalidDestinations, StreamRunnerConfig{Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{}, SnowflakePolicy: testSnowflakePolicy(t)}); err == nil {
			t.Fatalf("NewStreamRunner admitted NUL-containing %s", key)
		}
	}
}

func TestManagedAdmissionRejectsUnsafeOptions(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*flow.Flow, *[]stream.DestinationConfig, *StreamRunnerConfig)
		want   string
	}{
		{name: "bootstrap pool capacity before side effects", mutate: func(f *flow.Flow, _ *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			setRunnerSourceOptions(f, map[string]string{"bootstrap": "required", "pool_max_conns": "1"})
		}, want: "pool_max_conns>=2 before connector side effects"},
		{name: "bootstrap never create slot", mutate: func(f *flow.Flow, _ *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			setRunnerSourceOptions(f, map[string]string{"create_slot": "true"})
		}, want: "create_slot=false"},
		{name: "bootstrap never missing sync publication", mutate: func(f *flow.Flow, _ *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			setRunnerSourceOptions(f, map[string]string{"sync_publication": ""})
		}, want: "sync_publication=false"},
		{name: "file snapshot authority", mutate: func(f *flow.Flow, _ *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			setRunnerSourceOptions(f, map[string]string{"snapshot_state_backend": "file"})
		}, want: "snapshot authority"},
		{name: "drop slot", mutate: func(f *flow.Flow, _ *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			f.Config.FailureMode = stream.FailureModeDropSlot
		}, want: "drop_slot"},
		{name: "primary acknowledgement", mutate: func(f *flow.Flow, _ *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			f.Config.AckPolicy = stream.AckPolicyPrimary
		}, want: "requires ack_policy=all"},
		{name: "multiple sinks", mutate: func(_ *flow.Flow, destinations *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			*destinations = append(*destinations, (*destinations)[0])
		}, want: "exactly one destination revision"},
		{name: "staging", mutate: func(_ *flow.Flow, destinations *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			(*destinations)[0].Spec.Options["batch_mode"] = "staging"
		}, want: "batch_mode"},
		{name: "missing revision", mutate: func(_ *flow.Flow, destinations *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			delete((*destinations)[0].Spec.Options, "destination_revision_id")
		}, want: "destination_revision_id"},
		{name: "missing durable commit setting", mutate: func(_ *flow.Flow, destinations *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			delete((*destinations)[0].Spec.Options, "synchronous_commit")
		}, want: "explicit durable synchronous_commit"},
		{name: "non-durable remote write", mutate: func(_ *flow.Flow, destinations *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			(*destinations)[0].Spec.Options["synchronous_commit"] = "remote_write"
		}, want: "explicit durable synchronous_commit"},
		{name: "clickhouse", mutate: func(_ *flow.Flow, destinations *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			(*destinations)[0].Spec.Type = connector.EndpointClickHouse
		}, want: "ClickHouse"},
		{name: "generic staging resolution", mutate: func(_ *flow.Flow, _ *[]stream.DestinationConfig, cfg *StreamRunnerConfig) { cfg.ResolveStaging = true }, want: "staging resolution"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := managedAdmissionFlow()
			destinations := managedAdmissionDestinations()
			fence := managedAdmissionFence()
			cfg := StreamRunnerConfig{Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{}}
			tt.mutate(&f, &destinations, &cfg)
			_, err := NewStreamRunner(f, &pgsource.Source{}, destinations, cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error=%v, want substring %q", err, tt.want)
			}
		})
	}
}

func managedAdmissionFlow() flow.Flow {
	destination := managedAdmissionDestinations()[0].Spec
	definition := flow.Flow{
		ID: "managed-flow",
		Source: runnerTestSource(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: map[string]string{
			"managed": "true", "bootstrap": "never", "create_slot": "false", "ensure_publication": "false", "ensure_state": "false", "sync_publication": "false",
			"source_system_identifier": "system-1", "source_lineage_id": "lineage-1", "publication_revision": "revision-1",
		}}),
		Destinations: []*wallabypb.Endpoint{runnerTestDestination(destination)},
		Config:       flow.Config{AckPolicy: stream.AckPolicyAll},
	}
	definition.Config.TableMappings = flow.NewTableMappings([]connector.RuntimeSpec{destination})
	autoApply := false
	definition.Config.DDL.AutoApply = &autoApply
	return definition
}

func managedAdmissionDestinations() []stream.DestinationConfig {
	return []stream.DestinationConfig{{
		Spec: connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres, Options: map[string]string{
			"batch_mode": "target", "destination_revision_id": "postgres-target-v1", "synchronous_commit": "on",
		}},
		Dest: &pgdest.Destination{},
	}}
}

func managedAdmissionSnowflakeDSN(t *testing.T, mutate func(*gosnowflake.Config)) string {
	t.Helper()
	readLatestWrites := "true"
	timezone := "UTC"
	cfg := &gosnowflake.Config{
		Account: "account", User: "user", Database: "DB", Schema: "PUBLIC", Warehouse: "WH", Role: "ROLE",
		Protocol: "https", Authenticator: gosnowflake.AuthTypeJwt,
		OCSPFailOpen: gosnowflake.OCSPFailOpenFalse,
		Params:       map[string]*string{"READ_LATEST_WRITES": &readLatestWrites, "TIMEZONE": &timezone},
	}
	if mutate != nil {
		mutate(cfg)
	}
	dsn, err := gosnowflake.DSN(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return dsn
}

func TestManagedSnowflakeCompositePrimaryKeyMustBeCompleteOrderedAndPreserved(t *testing.T) {
	contract := connector.Schema{Namespace: "public", Name: "widgets", Columns: []connector.Column{
		{Name: "tenant_id", Type: "text", TypeMetadata: map[string]string{"primary_key": "true", "primary_key_ordinal": "1", "replica_identity": "true", "nullability_known": "true", "generated_known": "true"}},
		{Name: "event_id", Type: "int8", TypeMetadata: map[string]string{"primary_key": "true", "primary_key_ordinal": "2", "replica_identity": "true", "nullability_known": "true", "generated_known": "true"}},
		{Name: "payload", Type: "text", TypeMetadata: map[string]string{"nullability_known": "true", "generated_known": "true"}},
	}}
	newFlow := func(keys []string, excludeSecond bool) flow.Flow {
		f := managedSnowflakeFlowForTest()
		table := &f.Config.TableMappings.Destinations[0].Tables[0]
		table.FutureColumns = flow.FutureColumnMapping{Action: flow.MappingActionExclude}
		table.Columns = []flow.ColumnMapping{{SourceColumn: "tenant_id", Action: flow.MappingActionInclude, TargetColumn: "TENANT_ID"}, {SourceColumn: "event_id", Action: flow.MappingActionInclude, TargetColumn: "EVENT_ID"}, {SourceColumn: "payload", Action: flow.MappingActionInclude, TargetColumn: "PAYLOAD"}}
		if excludeSecond {
			table.Columns[1].Action = flow.MappingActionExclude
			table.Columns[1].TargetColumn = ""
		}
		table.Write.KeyColumns = append([]string(nil), keys...)
		return f
	}
	fence := managedAdmissionFence()
	cfg := StreamRunnerConfig{Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{}, SnowflakePolicy: testSnowflakePolicy(t)}
	runner, err := NewStreamRunner(newFlow([]string{"tenant_id", "event_id"}, false), &pgsource.Source{}, managedSnowflakeAdmissionDestinationsForContract(t, contract), cfg)
	if err != nil {
		t.Fatal(err)
	}
	var projected connector.Schema
	if err := json.Unmarshal([]byte(runner.Destinations[0].Spec.Options["managed_schema_contract"]), &projected); err != nil {
		t.Fatal(err)
	}
	primary, err := orderedManagedSnowflakePrimaryKey(projected, "test")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(primary, []string{"TENANT_ID", "EVENT_ID"}) {
		t.Fatalf("projected composite primary key=%v", primary)
	}
	projectedBatch, decision, err := runner.Destinations[0].Projector.ProjectBatch(connector.Batch{Schema: contract, Records: []connector.Record{{Table: "widgets", Operation: connector.OpInsert, After: map[string]any{"tenant_id": "tenant-1", "event_id": int64(1), "payload": "value"}, SourcePosition: "0/10"}}})
	if err != nil || decision != stream.ProjectionIncluded {
		t.Fatalf("project composite schema decision/error=%v/%v", decision, err)
	}
	if !slices.Equal(projectedBatch.WritePolicy.KeyColumns, primary) {
		t.Fatalf("planner identity keys=%v primary=%v", projectedBatch.WritePolicy.KeyColumns, primary)
	}
	for _, test := range []struct {
		name          string
		keys          []string
		excludeSecond bool
		want          string
	}{
		{name: "missing", keys: nil, want: "at least one key_columns"},
		{name: "partial", keys: []string{"tenant_id"}, want: "complete ordered source primary key"},
		{name: "reordered", keys: []string{"event_id", "tenant_id"}, want: "complete ordered source primary key"},
		{name: "extra", keys: []string{"tenant_id", "event_id", "payload"}, want: "complete ordered source primary key"},
		{name: "excluded_component", keys: []string{"tenant_id", "event_id"}, excludeSecond: true, want: "excluded"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewStreamRunner(newFlow(test.keys, test.excludeSecond), &pgsource.Source{}, managedSnowflakeAdmissionDestinationsForContract(t, contract), cfg)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v want %q", err, test.want)
			}
		})
	}
}

func TestManagedSnowflakeMappingRejectsAppendWatermarkAndFutureDefaults(t *testing.T) {
	fence := managedAdmissionFence()
	cfg := StreamRunnerConfig{Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{}, SnowflakePolicy: testSnowflakePolicy(t)}
	for name, mutate := range map[string]func(*flow.Flow){
		"future_include": func(f *flow.Flow) {
			f.Config.TableMappings.Destinations[0].FutureTables = flow.FutureTableMapping{Action: flow.MappingActionInclude, TargetSchema: "{{ .Schema }}", TargetTable: "{{ .Table }}", FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{{ .Column }}"}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeAppend}}
		},
		"append": func(f *flow.Flow) {
			f.Config.TableMappings.Destinations[0].Tables[0].Write = flow.TableWritePolicy{Mode: flow.TableWriteModeAppend}
		},
		"watermark": func(f *flow.Flow) { f.Config.TableMappings.Destinations[0].Tables[0].Write.WatermarkColumn = "id" },
	} {
		t.Run(name, func(t *testing.T) {
			f := managedSnowflakeFlowForTest()
			mutate(&f)
			if _, err := NewStreamRunner(f, &pgsource.Source{}, managedSnowflakeAdmissionDestinations(t), cfg); err == nil {
				t.Fatal("unsupported managed Snowflake mapping admitted")
			}
		})
	}
}

func managedSnowflakeFlowForTest() flow.Flow {
	f := managedAdmissionFlow()
	setRunnerSourceOptions(&f, map[string]string{
		"managed": "", "managed_profile": connector.ManagedProfilePostgresToSnowflakeSQLV1,
		"create_slot": "true", "slot": "managed", "streaming_transactions": "true", "toast_fetch": "off",
		"max_transaction_records": "1000", "max_transaction_bytes": "8388608", "max_transaction_fragments": "64",
	})
	mapping, _ := f.Config.TableMappings.ForDestination("target")
	mapping.FutureTables = flow.FutureTableMapping{Action: flow.MappingActionExclude}
	mapping.Tables = []flow.TableMapping{{SourceSchema: "public", SourceTable: "widgets", Action: flow.MappingActionInclude, TargetSchema: "PUBLIC", TargetTable: "WIDGETS", FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{{ .Column }}"}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeUpsert, KeyColumns: []string{"id"}}}}
	f.Config.TableMappings.Destinations[0] = mapping
	return f
}

func managedSnowflakeAdmissionDestinations(t *testing.T) []stream.DestinationConfig {
	t.Helper()
	contract := connector.Schema{Name: "widgets", Namespace: "public", Columns: []connector.Column{{Name: "id", Type: "int8", TypeMetadata: map[string]string{"primary_key": "true", "primary_key_ordinal": "1", "replica_identity": "true", "nullability_known": "true", "generated_known": "true"}}}}
	return managedSnowflakeAdmissionDestinationsForContract(t, contract)
}

func managedSnowflakeAdmissionDestinationsForContract(t *testing.T, contract connector.Schema) []stream.DestinationConfig {
	t.Helper()
	encoded, err := json.Marshal(contract)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := snowflakedest.ManagedSchemaContractHash(contract)
	if err != nil {
		t.Fatal(err)
	}
	return []stream.DestinationConfig{{
		Spec: connector.RuntimeSpec{Name: "target", Type: connector.EndpointSnowflake, Options: map[string]string{
			"dsn":                                       managedAdmissionSnowflakeDSN(t, nil),
			"flow_id":                                   "managed-flow",
			"managed_profile":                           connector.ManagedProfilePostgresToSnowflakeSQLV1,
			"destination_revision_id":                   "snowflake-sql-v1",
			"batch_mode":                                "target",
			"batch_resolution":                          "none",
			"meta_table_enabled":                        "false",
			"disable_transactions":                      "false",
			"session_keep_alive":                        "false",
			"managed_account":                           "ACCOUNT",
			"managed_database":                          "DB",
			"managed_schema":                            "PUBLIC",
			"managed_table":                             "WIDGETS",
			"managed_receipts_table":                    "WALLABY_RECEIPTS",
			"managed_owner_role":                        "OWNER_ROLE",
			"managed_execution_role":                    "ROLE",
			"managed_warehouse":                         "WH",
			"managed_snowflake_version":                 "9.99.0",
			"managed_target_created_on":                 "2026-01-01T00:00:00.000000000+00:00",
			"managed_receipts_created_on":               "2026-01-01T00:00:01.000000000+00:00",
			"managed_source_schema":                     "public",
			"managed_source_table":                      "widgets",
			"managed_schema_contract":                   string(encoded),
			"managed_schema_contract_hash":              hash,
			"managed_max_transaction_rows":              "1000",
			"managed_max_transaction_bytes":             "8388608",
			"managed_max_transaction_fragments":         "64",
			"managed_max_open_conns":                    "4",
			"managed_statement_timeout_seconds":         "120",
			"managed_hybrid_table_lock_timeout_seconds": "60",
		}},
		Dest: &pgdest.Destination{},
	}}
}

func managedClickHouseAdmissionDestinations() []stream.DestinationConfig {
	return []stream.DestinationConfig{{
		Spec: connector.RuntimeSpec{Name: "target", Type: connector.EndpointClickHouse, Options: map[string]string{
			"dsn":                               "clickhouse://localhost:9440/wallaby?secure=true",
			"managed_profile":                   connector.ManagedProfilePostgresToClickHouseAppendV1,
			"destination_revision_id":           "clickhouse-append-v1",
			"batch_mode":                        "target",
			"batch_resolution":                  "none",
			"meta_table_enabled":                "false",
			"managed_database":                  "wallaby",
			"managed_changelog_table":           "cdc_log",
			"managed_receipts_table":            "delivery_receipts",
			"managed_final_view":                "cdc_log_final",
			"managed_deployment":                "self-managed-keeper",
			"managed_keeper_path_prefix":        "/clickhouse/tables/01",
			"managed_keeper_address":            "127.0.0.1:9181",
			"managed_replica_dsn":               "clickhouse://replica-2:9440/wallaby?secure=true",
			"managed_replica_names":             "replica-1,replica-2",
			"managed_max_active_parts":          "180",
			"managed_max_transaction_rows":      "100000",
			"managed_max_transaction_bytes":     "134217728",
			"managed_max_transaction_fragments": "128",
			"managed_max_rows_per_batch":        "10000",
			"managed_max_batch_bytes":           "16777216",
			"insert_quorum":                     "2",
			"async_insert":                      "false",
			"wait_for_async_insert":             "true",
		}},
		// Admission is intentionally interface-based and runs before Open. The
		// ClickHouse implementation proves the same interface at compile time.
		Dest: &pgdest.Destination{},
	}}
}

func managedAdmissionFence() authority.RunFence {
	return authority.RunFence{
		FlowID: "managed-flow", FlowIncarnationID: uuid.New(), Generation: 1,
		AcquisitionID: uuid.New(), ExecutionID: "execution", LeaseEpoch: 1,
	}
}

type managedCheckpointStore struct{ testCheckpointOutboxStore }

func (managedCheckpointStore) GetFenced(context.Context, authority.RunFence) (connector.Checkpoint, error) {
	return connector.Checkpoint{}, connector.ErrCheckpointNotFound
}
func (managedCheckpointStore) PutFenced(context.Context, authority.RunFence, connector.Checkpoint) error {
	return nil
}
func (managedCheckpointStore) PersistCheckpointAndOutboxFenced(context.Context, authority.RunFence, connector.Checkpoint, []connector.OutboxEntry) error {
	return nil
}
func (managedCheckpointStore) ListOutboxFenced(context.Context, authority.RunFence) ([]connector.OutboxEntry, error) {
	return nil, nil
}
func (managedCheckpointStore) CompleteOutboxFenced(context.Context, authority.RunFence, string, string) error {
	return nil
}
