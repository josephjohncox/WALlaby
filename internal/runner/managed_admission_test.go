package runner

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestManagedAdmissionRequiresExactMaterializedContract(t *testing.T) {
	t.Parallel()

	f := managedAdmissionFlow()
	f.Config.AckPolicy = stream.AckPolicyMaterialized
	f.Config.Materialization = flow.MaterializationPolicy{ProjectionID: "canonical_cdc_parquet_v1"}
	fence := managedAdmissionFence()
	cfg := StreamRunnerConfig{
		Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{},
	}
	if _, err := NewStreamRunner(f, &pgsource.Source{}, managedAdmissionDestinations(), cfg); err == nil || !strings.Contains(err.Error(), "artifact log") {
		t.Fatalf("missing artifact log error=%v", err)
	}
	cfg.ArtifactLog = materializedAdmissionLog{}
	if _, err := NewStreamRunner(f, &pgsource.Source{}, managedAdmissionDestinations(), cfg); err != nil {
		t.Fatal(err)
	}

	f.Config.Materialization.ProjectionID = "parquet"
	if _, err := NewStreamRunner(f, &pgsource.Source{}, managedAdmissionDestinations(), cfg); err == nil || !strings.Contains(err.Error(), "canonical_cdc_parquet_v1") {
		t.Fatalf("wrong projection error=%v", err)
	}
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
func (materializedAdmissionLog) Append(_ context.Context, _ connector.RunFence, transaction connector.SourceTransaction) (connector.AckGrant, error) {
	positionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
	return connector.AckGrant{Checkpoint: transaction.Checkpoint, PositionID: positionID}, err
}

func TestManagedAdmissionAcceptsInitialPostgresProfile(t *testing.T) {
	for _, bootstrapMode := range []string{"never", "auto", "required"} {
		t.Run(bootstrapMode, func(t *testing.T) {
			f := managedAdmissionFlow()
			f.Source.Options["bootstrap"] = bootstrapMode
			if bootstrapMode != "never" {
				f.Source.Options["ensure_publication"] = "true"
			}
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
	delete(f.Source.Options, "managed")
	f.Source.Options["managed_profile"] = connector.ManagedProfilePostgresToPostgresV1
	f.Source.Options["bootstrap"] = "required"
	f.Source.Options["streaming_transactions"] = "true"
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
	f.Source.Options["managed_profile"] = connector.ManagedProfilePostgresToPostgresV1
	f.Source.Options["bootstrap"] = "required"
	f.Source.Options["streaming_transactions"] = "true"
	destinations := managedAdmissionDestinations()
	destinations[0].Spec.Options["managed_profile"] = connector.ManagedProfilePostgresToPostgresV1
	fence := managedAdmissionFence()
	if _, err := NewStreamRunner(f, &pgsource.Source{}, destinations, StreamRunnerConfig{
		Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{},
	}); err != nil {
		t.Fatal(err)
	}

	f.Source.Options["streaming_transactions"] = "false"
	if _, err := NewStreamRunner(f, &pgsource.Source{}, destinations, StreamRunnerConfig{
		Checkpoints: managedCheckpointStore{}, RunFence: &fence, DeliveryCoordinator: &delivery.Coordinator{},
	}); err == nil || !strings.Contains(err.Error(), "streaming_transactions=true") {
		t.Fatalf("named profile error=%v", err)
	}
}

func TestManagedAdmissionAcceptsClickHouseAppendProfileOnlyWithExactContract(t *testing.T) {
	f := managedAdmissionFlow()
	delete(f.Source.Options, "managed")
	f.Source.Options["managed_profile"] = connector.ManagedProfilePostgresToClickHouseAppendV1
	f.Source.Options["streaming_transactions"] = "true"
	f.Source.Options["max_transaction_records"] = "100000"
	f.Source.Options["max_transaction_bytes"] = "134217728"
	f.Source.Options["max_transaction_fragments"] = "128"
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
		{name: "mutation mode", key: "write_mode", value: "target", want: "write_mode=managed_append"},
		{name: "staging", key: "batch_mode", value: "staging", want: "batch_mode=target"},
		{name: "metadata mutations", key: "meta_table_enabled", value: "true", want: "meta_table_enabled=false"},
		{name: "async insert", key: "async_insert", value: "true", want: "async_insert=false"},
		{name: "fire and forget", key: "wait_for_async_insert", value: "false", want: "wait_for_async_insert=true"},
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
			copyFlow := f
			copyFlow.Source.Options = make(map[string]string, len(f.Source.Options))
			for key, value := range f.Source.Options {
				copyFlow.Source.Options[key] = value
			}
			copyFlow.Source.Options[tt.key] = tt.value
			_, err := NewStreamRunner(copyFlow, &pgsource.Source{}, managedClickHouseAdmissionDestinations(), cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error=%v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestManagedAdmissionRejectsUnsafeOptions(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*flow.Flow, *[]stream.DestinationConfig, *StreamRunnerConfig)
		want   string
	}{
		{name: "arbitrary start lsn", mutate: func(f *flow.Flow, _ *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			f.Source.Options["start_lsn"] = "0/10"
		}, want: "arbitrary start_lsn"},
		{name: "bootstrap pool capacity before side effects", mutate: func(f *flow.Flow, _ *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			f.Source.Options["bootstrap"] = "required"
			f.Source.Options["pool_max_conns"] = "1"
		}, want: "pool_max_conns>=2 before connector side effects"},
		{name: "bootstrap never create slot", mutate: func(f *flow.Flow, _ *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			f.Source.Options["create_slot"] = "true"
		}, want: "create_slot=false"},
		{name: "bootstrap never missing sync publication", mutate: func(f *flow.Flow, _ *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			delete(f.Source.Options, "sync_publication")
		}, want: "sync_publication=false"},
		{name: "legacy backfill", mutate: func(f *flow.Flow, _ *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			f.Source.Options["mode"] = "backfill"
		}, want: "legacy mode=backfill"},
		{name: "file snapshot authority", mutate: func(f *flow.Flow, _ *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			f.Source.Options["snapshot_state_backend"] = "file"
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
	return flow.Flow{
		ID: "managed-flow",
		Source: connector.Spec{Type: connector.EndpointPostgres, Options: map[string]string{
			"managed": "true", "bootstrap": "never", "create_slot": "false", "ensure_publication": "false", "ensure_state": "false", "sync_publication": "false",
			"source_system_identifier": "system-1", "source_lineage_id": "lineage-1", "publication_revision": "revision-1",
		}},
		Config: flow.Config{AckPolicy: stream.AckPolicyAll},
	}
}

func managedAdmissionDestinations() []stream.DestinationConfig {
	return []stream.DestinationConfig{{
		Spec: connector.Spec{Name: "target", Type: connector.EndpointPostgres, Options: map[string]string{
			"write_mode": "target", "batch_mode": "target", "destination_revision_id": "postgres-target-v1", "synchronous_commit": "on",
		}},
		Dest: &pgdest.Destination{},
	}}
}

func managedClickHouseAdmissionDestinations() []stream.DestinationConfig {
	return []stream.DestinationConfig{{
		Spec: connector.Spec{Name: "clickhouse-append", Type: connector.EndpointClickHouse, Options: map[string]string{
			"dsn":                               "clickhouse://localhost:9440/wallaby?secure=true",
			"managed_profile":                   connector.ManagedProfilePostgresToClickHouseAppendV1,
			"destination_revision_id":           "clickhouse-append-v1",
			"write_mode":                        "managed_append",
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
