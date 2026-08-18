package runner

import (
	"context"
	"errors"
	"strings"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestNewStreamRunnerSnowflakePolicyDeniesBeforeCheckpointAndConnectorUse(t *testing.T) {
	spec := connector.RuntimeSpec{Name: "snowflake", Type: connector.EndpointSnowflake, Options: map[string]string{"dsn": "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false"}}
	definition := mappedRunnerTestFlow(flow.Flow{
		ID: "snowflake-denied", Source: runnerTestSource(connector.RuntimeSpec{Type: connector.EndpointPostgres}),
		Destinations: []*wallabypb.Endpoint{runnerTestDestination(spec)},
	})
	_, err := NewStreamRunner(definition, nil, []stream.DestinationConfig{{Spec: spec, Dest: flowRunnerDestination{}}}, StreamRunnerConfig{})
	if !errors.Is(err, connector.ErrSnowflakeExecutionDisabled) {
		t.Fatalf("NewStreamRunner() error=%v", err)
	}
}

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
			got, err := NewStreamRunner(mappedRunnerTestFlow(flow.Flow{
				ID:          "flow-1",
				Source:      runnerTestSource(connector.RuntimeSpec{Type: connector.EndpointPostgres}),
				WireFormat:  tt.flowWire,
				Parallelism: tt.flowParallel,
			}), nil, nil, StreamRunnerConfig{
				Checkpoints:        testCheckpointOutboxStore{},
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
		Source: runnerTestSource(connector.RuntimeSpec{
			Type:    connector.EndpointPostgres,
			Options: map[string]string{"emit_empty": "false", "dsn": "postgres://original"},
		}),
		Config: flow.Config{
			AckPolicy:          stream.AckPolicyPrimary,
			PrimaryDestination: "primary",
			FailureMode:        stream.FailureModeDropSlot,
			GiveUpPolicy:       stream.GiveUpPolicyNever,
		},
	}
	destinations := []stream.DestinationConfig{{
		Spec: connector.RuntimeSpec{Name: "primary", Type: connector.EndpointPostgres, Options: map[string]string{"dsn": "postgres://original"}},
		Dest: flowRunnerDestination{},
	}}
	f.Destinations = []*wallabypb.Endpoint{runnerTestDestination(destinations[0].Spec)}
	f = mappedRunnerTestFlow(f)

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

	got.SourceSpec.Options["dsn"] = "postgres://changed"
	got.Destinations[0].Spec.Options["dsn"] = "postgres://changed"
	got.Destinations = append(got.Destinations, stream.DestinationConfig{})
	if got := f.Source.GetPostgresSource().GetConnection().GetDsn(); got != "postgres://original" {
		t.Fatalf("source endpoint mutated: %v", f.Source)
	}
	if destinations[0].Spec.Options["dsn"] != "postgres://original" {
		t.Fatalf("destination options mutated: %v", destinations[0].Spec.Options)
	}
	if len(destinations) != 1 {
		t.Fatalf("destination slice mutated: len=%d", len(destinations))
	}
}

func TestNewStreamRunnerNilDDLPolicyUsesShippedAutoApplyDefault(t *testing.T) {
	t.Parallel()
	destination := connector.RuntimeSpec{Name: "destination", Type: connector.EndpointPostgres}
	definition := flow.Flow{ID: "flow-ddl-default", Source: runnerTestSource(connector.RuntimeSpec{Type: connector.EndpointPostgres}), Destinations: []*wallabypb.Endpoint{runnerTestDestination(destination)}}
	definition.Config.TableMappings = flow.NewTableMappings([]connector.RuntimeSpec{destination})
	destinations := []stream.DestinationConfig{{Spec: destination, Dest: flowRunnerDestination{}}}
	_, err := NewStreamRunner(definition, nil, destinations, StreamRunnerConfig{Checkpoints: testCheckpointOutboxStore{}})
	if err == nil || !strings.Contains(err.Error(), "execution receipt storage") {
		t.Fatalf("nil DDL policy construction error=%v, want shipped auto_apply receipt requirement", err)
	}

	disabled := flow.DDLPolicyDefaults{}
	got, err := NewStreamRunner(definition, nil, destinations, StreamRunnerConfig{Checkpoints: testCheckpointOutboxStore{}, DDLPolicyDefaults: &disabled})
	if err != nil {
		t.Fatalf("deployment auto_apply=false construction: %v", err)
	}
	if got.RequireDDLExecution {
		t.Fatal("deployment auto_apply=false resolved to DDL execution")
	}
	resolved := flow.ResolveDDLPolicy(flow.DDLPolicy{}, nil)
	if !resolved.AutoApprove || !resolved.AutoApply || resolved.Gate {
		t.Fatalf("nil effective DDL policy=%+v, want shipped defaults", resolved)
	}
}

func TestNewStreamRunnerRejectsAutoApplyWithoutReceiptStore(t *testing.T) {
	t.Parallel()

	autoApply := true
	_, err := NewStreamRunner(mappedRunnerTestFlow(flow.Flow{
		ID: "flow-ddl", Destinations: []*wallabypb.Endpoint{runnerTestDestination(connector.RuntimeSpec{Name: "destination", Type: connector.EndpointPostgres})},
		Config: flow.Config{DDL: flow.DDLPolicy{AutoApply: &autoApply}},
	}), nil, []stream.DestinationConfig{{
		Spec: connector.RuntimeSpec{Name: "destination"},
		Dest: flowRunnerDestination{},
	}}, StreamRunnerConfig{Checkpoints: testCheckpointOutboxStore{}})
	if err == nil || !strings.Contains(err.Error(), "execution receipt storage") {
		t.Fatalf("NewStreamRunner() error=%v, want receipt storage requirement", err)
	}
}

func TestNewStreamRunnerRejectsUnsupportedTablePolicyBeforeOpen(t *testing.T) {
	destination := connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}
	definition := flow.Flow{ID: "flow-policy", Source: runnerTestSource(connector.RuntimeSpec{Type: connector.EndpointPostgres}), Destinations: []*wallabypb.Endpoint{runnerTestDestination(destination)}}
	definition.Config.TableMappings = flow.NewTableMappings([]connector.RuntimeSpec{destination})
	mapping := &definition.Config.TableMappings.Destinations[0]
	mapping.Tables = []flow.TableMapping{{
		SourceSchema: "public", SourceTable: "widgets", Action: flow.MappingActionInclude,
		TargetSchema: "public", TargetTable: "widgets", FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{{ .Column }}"},
		Write: flow.TableWritePolicy{Mode: flow.TableWriteModeUpsert, KeyColumns: []string{"id"}},
	}}
	_, err := NewStreamRunner(definition, nil, []stream.DestinationConfig{{Spec: destination, Dest: appendOnlyRunnerDestination{}}}, StreamRunnerConfig{Checkpoints: testCheckpointOutboxStore{}})
	if err == nil || !strings.Contains(err.Error(), "upsert") {
		t.Fatalf("error=%v, want pre-open upsert rejection", err)
	}
}

type appendOnlyRunnerDestination struct{ flowRunnerDestination }

func (appendOnlyRunnerDestination) Capabilities() connector.Capabilities {
	return connector.Capabilities{Delivery: connector.DeliverySemantics{}, TableWrites: connector.TableWriteSemantics{Append: true}}
}

func TestNewStreamRunnerRejectsMissingCheckpointStore(t *testing.T) {
	t.Parallel()

	_, err := NewStreamRunner(flow.Flow{ID: "flow-all"}, nil, nil, StreamRunnerConfig{})
	if err == nil {
		t.Fatal("NewStreamRunner() error = nil, want checkpoint durability error")
	}
}

func TestNewStreamRunnerRejectsMissingFlowID(t *testing.T) {
	t.Parallel()

	_, err := NewStreamRunner(flow.Flow{}, nil, nil, StreamRunnerConfig{
		Checkpoints: testCheckpointOutboxStore{},
	})
	if err == nil {
		t.Fatal("NewStreamRunner() error = nil, want durable flow identity error")
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
