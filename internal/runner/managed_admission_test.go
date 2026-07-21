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

func TestManagedAdmissionAcceptsInitialPostgresProfile(t *testing.T) {
	fence := managedAdmissionFence()
	_, err := NewStreamRunner(managedAdmissionFlow(), &pgsource.Source{}, managedAdmissionDestinations(), StreamRunnerConfig{
		Checkpoints:         managedCheckpointStore{},
		RunFence:            &fence,
		DeliveryCoordinator: &delivery.Coordinator{},
	})
	if err != nil {
		t.Fatal(err)
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
		{name: "legacy backfill", mutate: func(f *flow.Flow, _ *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			f.Source.Options["mode"] = "backfill"
		}, want: "legacy mode=backfill"},
		{name: "file snapshot authority", mutate: func(f *flow.Flow, _ *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			f.Source.Options["snapshot_state_backend"] = "file"
		}, want: "snapshot authority"},
		{name: "drop slot", mutate: func(f *flow.Flow, _ *[]stream.DestinationConfig, _ *StreamRunnerConfig) {
			f.Config.FailureMode = stream.FailureModeDropSlot
		}, want: "drop_slot"},
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
			"managed": "true", "bootstrap": "never", "ensure_publication": "false", "ensure_state": "false",
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
