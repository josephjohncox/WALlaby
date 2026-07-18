package runner

import (
	"context"
	"testing"

	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestNewStreamRunnerPrecedence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		flowWire        connector.WireFormat
		defaultWire     connector.WireFormat
		flowParallel    int
		defaultParallel int
		wantWire        connector.WireFormat
		wantParallel    int
	}{
		{
			name:            "flow values override defaults",
			flowWire:        connector.WireFormatJSON,
			defaultWire:     connector.WireFormatArrow,
			flowParallel:    8,
			defaultParallel: 2,
			wantWire:        connector.WireFormatJSON,
			wantParallel:    8,
		},
		{
			name:            "defaults fill zero flow values",
			defaultWire:     connector.WireFormatArrow,
			defaultParallel: 3,
			wantWire:        connector.WireFormatArrow,
			wantParallel:    3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := NewStreamRunner(flow.Flow{
				ID:          "flow-1",
				Source:      connector.Spec{Type: connector.EndpointPostgres},
				WireFormat:  tt.flowWire,
				Parallelism: tt.flowParallel,
			}, nil, nil, StreamRunnerConfig{
				DefaultWireFormat:  tt.defaultWire,
				DefaultParallelism: tt.defaultParallel,
			})
			if err != nil {
				t.Fatal(err)
			}
			if got.WireFormat != tt.wantWire {
				t.Fatalf("WireFormat = %q, want %q", got.WireFormat, tt.wantWire)
			}
			if got.Parallelism != tt.wantParallel {
				t.Fatalf("Parallelism = %d, want %d", got.Parallelism, tt.wantParallel)
			}
		})
	}
}

func TestNewStreamRunnerClonesConfigurationAndAppliesPolicies(t *testing.T) {
	t.Parallel()

	f := flow.Flow{
		ID: "flow-1",
		Source: connector.Spec{
			Type:    connector.EndpointPostgres,
			Options: map[string]string{"emit_empty": "false", "source": "original"},
		},
		Config: flow.Config{
			AckPolicy:          stream.AckPolicyPrimary,
			PrimaryDestination: "primary",
			FailureMode:        stream.FailureModeDropSlot,
			GiveUpPolicy:       stream.GiveUpPolicyNever,
		},
	}
	destinations := []stream.DestinationConfig{{
		Spec: connector.Spec{Name: "primary", Options: map[string]string{"dest": "original"}},
	}}

	got, err := NewStreamRunner(f, nil, destinations, StreamRunnerConfig{
		Checkpoints:    testCheckpointOutboxStore{},
		MaxEmptyReads:  1,
		StrictFormat:   true,
		ResolveStaging: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if got.SourceSpec.Options["flow_id"] != f.ID {
		t.Fatalf("flow_id = %q, want %q", got.SourceSpec.Options["flow_id"], f.ID)
	}
	if got.SourceSpec.Options["emit_empty"] != "false" {
		t.Fatalf("explicit emit_empty overwritten: %q", got.SourceSpec.Options["emit_empty"])
	}
	if got.AckPolicy != f.Config.AckPolicy || got.PrimaryDestination != f.Config.PrimaryDestination ||
		got.FailureMode != f.Config.FailureMode || got.GiveUpPolicy != f.Config.GiveUpPolicy {
		t.Fatalf("flow policies not propagated: %+v", got)
	}
	if !got.StrictFormat || !got.ResolveStaging || got.MaxEmptyReads != 1 {
		t.Fatalf("runtime defaults not propagated: %+v", got)
	}

	got.SourceSpec.Options["source"] = "changed"
	got.Destinations[0].Spec.Options["dest"] = "changed"
	got.Destinations = append(got.Destinations, stream.DestinationConfig{})
	if f.Source.Options["source"] != "original" {
		t.Fatalf("source options mutated: %v", f.Source.Options)
	}
	if destinations[0].Spec.Options["dest"] != "original" {
		t.Fatalf("destination options mutated: %v", destinations[0].Spec.Options)
	}
	if len(destinations) != 1 {
		t.Fatalf("destination slice mutated: len=%d", len(destinations))
	}
}

func TestNewStreamRunnerRejectsPrimaryAckWithoutAtomicOutbox(t *testing.T) {
	t.Parallel()

	_, err := NewStreamRunner(flow.Flow{
		ID:     "flow-primary",
		Config: flow.Config{AckPolicy: stream.AckPolicyPrimary},
	}, nil, nil, StreamRunnerConfig{})
	if err == nil {
		t.Fatal("NewStreamRunner() error = nil, want primary-ack durability error")
	}
}

type testCheckpointOutboxStore struct{}

func (testCheckpointOutboxStore) Get(context.Context, string) (connector.Checkpoint, error) {
	return connector.Checkpoint{}, connector.ErrCheckpointNotFound
}
func (testCheckpointOutboxStore) Put(context.Context, string, connector.Checkpoint) error { return nil }
func (testCheckpointOutboxStore) List(context.Context) ([]connector.FlowCheckpoint, error) {
	return nil, nil
}
func (testCheckpointOutboxStore) PersistCheckpointAndOutbox(context.Context, string, connector.Checkpoint, []connector.OutboxEntry) error {
	return nil
}
func (testCheckpointOutboxStore) ListOutbox(context.Context, string) ([]connector.OutboxEntry, error) {
	return nil, nil
}
func (testCheckpointOutboxStore) DeleteOutbox(context.Context, string, string, string) error {
	return nil
}
