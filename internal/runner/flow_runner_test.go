package runner

import (
	"context"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestFlowRunnerPreservesExecutionSourceOverrides(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := workflow.NewMemoryEngine()
	durable := flow.Flow{
		ID: "flow-override",
		Source: connector.Spec{
			Type:    connector.EndpointPostgres,
			Options: map[string]string{"mode": connector.SourceModeCDC, "tables": "durable.table"},
		},
		Destinations: []connector.Spec{{Name: "dest"}},
	}
	if _, err := engine.Create(ctx, durable); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := engine.Start(ctx, durable.ID); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	execution := durable
	execution.Source.Options = map[string]string{
		"mode":             connector.SourceModeBackfill,
		"tables":           "public.accounts",
		"schemas":          "public",
		"snapshot_workers": "4",
		"partition_column": "id",
		"partition_count":  "8",
		"start_lsn":        "0/16B6C50",
	}
	source := &flowRunnerSource{}
	runner := FlowRunner{Engine: engine, Checkpoints: testCheckpointOutboxStore{}}
	err := runner.Run(ctx, execution, source, []stream.DestinationConfig{{
		Spec: connector.Spec{Name: "dest"},
		Dest: flowRunnerDestination{},
	}})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	wantOptions := make(map[string]string, len(execution.Source.Options)+1)
	for key, value := range execution.Source.Options {
		wantOptions[key] = value
	}
	wantOptions["flow_id"] = durable.ID
	if !reflect.DeepEqual(source.openSpec.Options, wantOptions) {
		t.Fatalf("Source.Open() options = %v, want execution overrides %v", source.openSpec.Options, wantOptions)
	}
	control, err := engine.Control(ctx, durable.ID)
	if err != nil {
		t.Fatal(err)
	}
	active, err := engine.ActiveExecutionsThrough(ctx, durable.ID, control.Generation)
	if err != nil || active != 0 {
		t.Fatalf("active executions after Run()=(%d,%v), want zero", active, err)
	}
}

func TestFlowRunnerRegistersProvidedExecutionIdentity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	memory := workflow.NewMemoryEngine()
	engine := &recordingExecutionEngine{MemoryEngine: memory}
	f := flow.Flow{ID: "provided-identity", Source: connector.Spec{Type: connector.EndpointPostgres, Options: map[string]string{"mode": connector.SourceModeBackfill}}}
	if _, err := engine.Create(ctx, f); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Start(ctx, f.ID); err != nil {
		t.Fatal(err)
	}
	control, _ := engine.Control(ctx, f.ID)
	runner := FlowRunner{
		Engine: engine, Checkpoints: testCheckpointOutboxStore{}, ExecutionBackend: "kubernetes",
		ExecutionID: "job-exact-id", ExpectedGeneration: control.Generation,
	}
	if err := runner.Run(ctx, f, &flowRunnerSource{}, []stream.DestinationConfig{{
		Spec: connector.Spec{Name: "dest"}, Dest: flowRunnerDestination{},
	}}); err != nil {
		t.Fatal(err)
	}
	if engine.backend != "kubernetes" || engine.executionID != "job-exact-id" || engine.generation != control.Generation {
		t.Fatalf("registered identity=(%q,%q,%d), want exact provided identity", engine.backend, engine.executionID, engine.generation)
	}
}

func TestFlowRunnerRejectsFencedExpectedGeneration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	engine := workflow.NewMemoryEngine()
	f := flow.Flow{ID: "fenced", Source: connector.Spec{Type: connector.EndpointPostgres}}
	if _, err := engine.Create(ctx, f); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Start(ctx, f.ID); err != nil {
		t.Fatal(err)
	}
	first, _ := engine.Control(ctx, f.ID)
	if _, err := engine.Pause(ctx, f.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Resume(ctx, f.ID); err != nil {
		t.Fatal(err)
	}
	runner := FlowRunner{Engine: engine, ExpectedGeneration: first.Generation}
	if err := runner.Run(ctx, f, &flowRunnerSource{}, nil); err == nil {
		t.Fatal("Run() accepted fenced generation")
	}
}

type recordingExecutionEngine struct {
	*workflow.MemoryEngine
	backend     string
	executionID string
	generation  int64
}

func (e *recordingExecutionEngine) RegisterExecutionGeneration(ctx context.Context, flowID, executionID, backend string, generation int64, lease time.Duration) error {
	e.backend, e.executionID, e.generation = backend, executionID, generation
	return e.MemoryEngine.RegisterExecutionGeneration(ctx, flowID, executionID, backend, generation, lease)
}

type flowRunnerSource struct {
	openSpec connector.Spec
}

func (s *flowRunnerSource) Open(_ context.Context, spec connector.Spec) error {
	s.openSpec = spec
	return nil
}

func (*flowRunnerSource) Read(context.Context) (connector.Batch, error) {
	return connector.Batch{}, io.EOF
}

func (*flowRunnerSource) Ack(context.Context, connector.Checkpoint) error { return nil }
func (*flowRunnerSource) Close(context.Context) error                     { return nil }
func (*flowRunnerSource) Capabilities() connector.Capabilities            { return connector.Capabilities{} }

type flowRunnerDestination struct{}

func (flowRunnerDestination) Open(context.Context, connector.Spec) error   { return nil }
func (flowRunnerDestination) Write(context.Context, connector.Batch) error { return nil }
func (flowRunnerDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return nil
}
func (flowRunnerDestination) TypeMappings() map[string]string { return nil }
func (flowRunnerDestination) Close(context.Context) error     { return nil }
func (flowRunnerDestination) Capabilities() connector.Capabilities {
	return connector.Capabilities{Delivery: connector.DeliverySemantics{
		Declared:           true,
		TransactionalBatch: true,
		IdempotentReplay:   true,
		ReplaySafe:         true,
	}}
}
