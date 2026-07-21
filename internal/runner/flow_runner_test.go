package runner

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/authority"
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

func TestManagedHeartbeatFailureReturnsErrorWithoutFailingFlow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	engine := workflow.NewMemoryEngine()
	f := managedAdmissionFlow()
	if _, err := engine.Create(ctx, f); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Start(ctx, f.ID); err != nil {
		t.Fatal(err)
	}
	control, _ := engine.Control(ctx, f.ID)
	renewFailure := errors.New("control database unavailable")
	authorityStore := &failingRenewAuthority{renewErr: renewFailure}
	deliveries := &blockingManagedDelivery{}
	runner := FlowRunner{
		Engine: engine, Checkpoints: managedCheckpointStore{}, Authority: authorityStore, Deliveries: deliveries,
		ExpectedGeneration: control.Generation, ExecutionID: "managed-heartbeat", ExecutionBackend: "test",
	}
	err := runner.Run(ctx, f, &blockingManagedSource{}, []stream.DestinationConfig{{
		Spec: managedAdmissionDestinations()[0].Spec,
		Dest: blockingManagedDestination{},
	}})
	if !errors.Is(err, renewFailure) {
		t.Fatalf("Run() error=%v, want heartbeat failure", err)
	}
	if authorityStore.failCalls != 0 {
		t.Fatalf("FailFlow calls=%d, heartbeat authority loss must remain recoverable", authorityStore.failCalls)
	}
}

func TestManagedIndeterminateDeliveryStaysRecoverable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	engine := workflow.NewMemoryEngine()
	f := managedAdmissionFlow()
	if _, err := engine.Create(ctx, f); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Start(ctx, f.ID); err != nil {
		t.Fatal(err)
	}
	control, _ := engine.Control(ctx, f.ID)
	authorityStore := &failingRenewAuthority{}
	deliveries := &blockingManagedDelivery{deliverErr: connector.ErrDeliveryIndeterminate}
	runner := FlowRunner{
		Engine: engine, Checkpoints: managedCheckpointStore{}, Authority: authorityStore, Deliveries: deliveries,
		ExpectedGeneration: control.Generation, ExecutionID: "managed-indeterminate", ExecutionBackend: "test",
	}
	err := runner.Run(ctx, f, &singleTransactionManagedSource{}, []stream.DestinationConfig{{
		Spec: managedAdmissionDestinations()[0].Spec,
		Dest: blockingManagedDestination{},
	}})
	if !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("Run() error=%v, want indeterminate delivery", err)
	}
	if authorityStore.failCalls != 0 {
		t.Fatalf("FailFlow calls=%d, indeterminate outcome must be reconciled by a later owner", authorityStore.failCalls)
	}
	current, getErr := engine.Get(ctx, f.ID)
	if getErr != nil || current.State != flow.StateRunning {
		t.Fatalf("flow after indeterminate delivery=(%s,%v), want running", current.State, getErr)
	}
}

type failingRenewAuthority struct {
	renewErr  error
	failCalls int
}

func (*failingRenewAuthority) AcquireProducer(_ context.Context, flowID, executionID, _ string, generation int64, _ time.Duration) (authority.RunFence, error) {
	return authority.RunFence{
		FlowID: flowID, FlowIncarnationID: uuid.New(), Generation: generation,
		AcquisitionID: uuid.New(), ExecutionID: executionID, LeaseEpoch: 1,
	}, nil
}
func (s *failingRenewAuthority) RenewProducer(context.Context, authority.RunFence, time.Duration) error {
	return s.renewErr
}
func (*failingRenewAuthority) FinishProducer(context.Context, authority.RunFence, string) error {
	return nil
}
func (s *failingRenewAuthority) FailFlow(context.Context, authority.RunFence, string) error {
	s.failCalls++
	return nil
}
func (*failingRenewAuthority) AcquireClaim(_ context.Context, fence authority.RunFence, kind authority.ClaimKind, workID string, _ time.Duration) (authority.ClaimFence, error) {
	return authority.ClaimFence{RunFence: fence, Kind: kind, WorkID: workID, ClaimEpoch: 1}, nil
}
func (*failingRenewAuthority) RenewClaim(context.Context, authority.ClaimFence, time.Duration) error {
	return nil
}
func (*failingRenewAuthority) ReleaseClaim(context.Context, authority.ClaimFence) error { return nil }

type blockingManagedDelivery struct{ deliverErr error }

func (*blockingManagedDelivery) RegisterDestinationRevision(context.Context, authority.RunFence, string, string, string) error {
	return nil
}
func (*blockingManagedDelivery) AuthorizeAck(_ context.Context, _ connector.RunFence, checkpoint connector.Checkpoint) (connector.AckGrant, error) {
	position, err := connector.CheckpointPositionID(checkpoint)
	return connector.AckGrant{Checkpoint: checkpoint, PositionID: position}, err
}
func (d *blockingManagedDelivery) Deliver(context.Context, connector.RunFence, connector.DeliveryIntent, connector.Batch, connector.ManagedDestination) (connector.AckGrant, error) {
	if d.deliverErr != nil {
		return connector.AckGrant{}, d.deliverErr
	}
	return connector.AckGrant{}, errors.New("unexpected delivery")
}
func (*blockingManagedDelivery) ValidateAckGrant(context.Context, connector.RunFence, connector.AckGrant) error {
	return nil
}
func (*blockingManagedDelivery) RecordAckReceipt(context.Context, connector.RunFence, connector.AckGrant, string) error {
	return nil
}

type blockingManagedSource struct{}

func (*blockingManagedSource) Open(context.Context, connector.Spec) error { return nil }
func (*blockingManagedSource) Read(ctx context.Context) (connector.Batch, error) {
	<-ctx.Done()
	return connector.Batch{}, ctx.Err()
}
func (*blockingManagedSource) ReadTransaction(ctx context.Context) (connector.SourceTransaction, error) {
	<-ctx.Done()
	return connector.SourceTransaction{}, ctx.Err()
}
func (*blockingManagedSource) InitialCheckpoint() (connector.Checkpoint, bool) {
	return connector.Checkpoint{LSN: "0/10"}, true
}
func (*blockingManagedSource) Ack(context.Context, connector.Checkpoint) error { return nil }
func (*blockingManagedSource) Close(context.Context) error                     { return nil }
func (*blockingManagedSource) Capabilities() connector.Capabilities {
	return connector.Capabilities{Support: connector.SupportExperimental, SupportsStreaming: true}
}

type singleTransactionManagedSource struct{ read bool }

func (*singleTransactionManagedSource) Open(context.Context, connector.Spec) error { return nil }
func (*singleTransactionManagedSource) Read(context.Context) (connector.Batch, error) {
	return connector.Batch{}, io.EOF
}
func (s *singleTransactionManagedSource) ReadTransaction(context.Context) (connector.SourceTransaction, error) {
	if s.read {
		return connector.SourceTransaction{}, io.EOF
	}
	s.read = true
	return connector.SourceTransaction{
		SourceLineageID: "lineage-1", TransactionID: 1, BeginLSN: "0/10", CommitLSN: "0/18", EndLSN: "0/20", Checkpoint: connector.Checkpoint{LSN: "0/20"},
		Fragments: []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{
			Schema:  connector.Schema{Name: "events", Namespace: "public", Version: 1},
			Records: []connector.Record{{Table: "events", Operation: connector.OpInsert, SchemaVersion: 1, After: map[string]any{"id": int64(1)}}},
		}}},
	}, nil
}
func (*singleTransactionManagedSource) InitialCheckpoint() (connector.Checkpoint, bool) {
	return connector.Checkpoint{LSN: "0/10"}, true
}
func (*singleTransactionManagedSource) Ack(context.Context, connector.Checkpoint) error { return nil }
func (*singleTransactionManagedSource) Close(context.Context) error                     { return nil }
func (*singleTransactionManagedSource) Capabilities() connector.Capabilities {
	return connector.Capabilities{Support: connector.SupportExperimental, SupportsStreaming: true}
}

type blockingManagedDestination struct{}

func (blockingManagedDestination) Open(context.Context, connector.Spec) error   { return nil }
func (blockingManagedDestination) Write(context.Context, connector.Batch) error { return nil }
func (blockingManagedDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return nil
}
func (blockingManagedDestination) TypeMappings() map[string]string { return nil }
func (blockingManagedDestination) Close(context.Context) error     { return nil }
func (blockingManagedDestination) Capabilities() connector.Capabilities {
	return connector.Capabilities{Support: connector.SupportExperimental, Delivery: connector.DeliverySemantics{Declared: true, TransactionalBatch: true, IdempotentReplay: true, ReplaySafe: true}}
}
func (blockingManagedDestination) Apply(context.Context, connector.DeliveryIntent, connector.Batch) (connector.DeliveryEvidence, error) {
	return connector.DeliveryEvidence{}, nil
}
func (blockingManagedDestination) Reconcile(context.Context, connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	return connector.DeliveryNotApplied, connector.DeliveryEvidence{}, nil
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
func (flowRunnerDestination) ReconcileDDL(context.Context, connector.Schema, connector.Record) (connector.DDLReconcileResult, error) {
	return connector.DDLReconcileNotApplied, nil
}
func (flowRunnerDestination) TypeMappings() map[string]string { return nil }
func (flowRunnerDestination) Close(context.Context) error     { return nil }
func (flowRunnerDestination) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		Delivery: connector.DeliverySemantics{
			Declared:           true,
			TransactionalBatch: true,
			IdempotentReplay:   true,
			ReplaySafe:         true,
			ExecutesDDL:        true,
		},
		SupportsDDL: true,
	}
}
