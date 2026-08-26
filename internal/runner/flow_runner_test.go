package runner

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestFlowRunnerSnowflakePolicyDeniesBeforeExecutionDependencies(t *testing.T) {
	ctx := context.Background()
	engine := workflow.NewMemoryEngine()
	spec := connector.RuntimeSpec{Name: "snowflake", Type: connector.EndpointSnowflake, Options: map[string]string{"dsn": "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false"}}
	definition := mappedRunnerTestFlow(flow.Flow{
		ID: "snowflake-denied", Source: runnerTestSource(connector.RuntimeSpec{Type: connector.EndpointPostgres}),
		Destinations: []*wallabypb.Endpoint{runnerTestDestination(spec)}, State: flow.StateCreated,
	})
	if _, err := engine.Create(ctx, definition); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Start(ctx, definition.ID); err != nil {
		t.Fatal(err)
	}
	runner := FlowRunner{Engine: engine}
	err := runner.Run(ctx, definition, nil, []stream.DestinationConfig{{Spec: spec, Dest: flowRunnerDestination{}}})
	if !errors.Is(err, connector.ErrSnowflakeExecutionDisabled) {
		t.Fatalf("Run() error=%v", err)
	}
}

func TestFlowRunnerPreservesExecutionSourceOverrides(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := workflow.NewMemoryEngine()
	durable := flow.Flow{
		ID: "flow-override",
		Source: runnerTestSource(connector.RuntimeSpec{
			Type:    connector.EndpointPostgres,
			Options: map[string]string{"mode": connector.SourceModeCDC, "publication_tables": "durable.table"},
		}),
		Destinations: []*wallabypb.Endpoint{runnerTestDestination(connector.RuntimeSpec{Name: "dest", Type: connector.EndpointPostgres})},
	}
	durable = mappedRunnerTestFlow(durable)
	if _, err := engine.Create(ctx, durable); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := engine.Start(ctx, durable.ID); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	execution := flow.Clone(durable)
	execution.Source = runnerTestSource(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: map[string]string{
		"mode": connector.SourceModeBackfill, "tables": "public.accounts", "schemas": "public",
		"snapshot_workers": "4", "partition_column": "id", "partition_count": "8",
	}})
	source := &flowRunnerSource{}
	runner := FlowRunner{Engine: engine, Checkpoints: testCheckpointOutboxStore{}}
	err := runner.Run(ctx, execution, source, []stream.DestinationConfig{{
		Spec: connector.RuntimeSpec{Name: "dest"},
		Dest: flowRunnerDestination{},
	}})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	executionSource, err := execution.DecodeSource(connector.DefaultRegistry)
	if err != nil {
		t.Fatal(err)
	}
	wantOptions := make(map[string]string, len(executionSource.Options)+1)
	for key, value := range executionSource.Options {
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
	f := mappedRunnerTestFlow(flow.Flow{ID: "provided-identity", Source: runnerTestSource(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: map[string]string{"mode": connector.SourceModeBackfill}})})
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
		Spec: connector.RuntimeSpec{Name: "dest"}, Dest: flowRunnerDestination{},
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
	f := mappedRunnerTestFlow(flow.Flow{ID: "fenced", Source: runnerTestSource(connector.RuntimeSpec{Type: connector.EndpointPostgres})})
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
		Engine: engine, Checkpoints: managedCheckpointStore{}, Authority: authorityStore, Deliveries: deliveries, SchemaBaselines: flowRunnerSchemaBaselines{},
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

func TestFlowRunnerPinsEffectiveArtifactDestinationFingerprint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	engine := workflow.NewMemoryEngine()
	f := managedAdmissionFlow()
	f.Config.AckPolicy = stream.AckPolicyMaterialized
	f.Config.Materialization = flow.MaterializationPolicy{ProjectionID: "canonical_cdc_parquet_v2"}
	lakeSpec := connector.RuntimeSpec{Name: "lake", Type: connector.EndpointIceberg, Options: map[string]string{"destination_revision_id": "iceberg-v1"}}
	f.Destinations = []*wallabypb.Endpoint{runnerTestDestination(lakeSpec)}
	f.Config.TableMappings = flow.NewTableMappings([]connector.RuntimeSpec{lakeSpec})
	if _, err := engine.Create(ctx, f); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Start(ctx, f.ID); err != nil {
		t.Fatal(err)
	}
	control, _ := engine.Control(ctx, f.ID)
	renewFailure := errors.New("stop after fingerprint registration")
	deliveries := &blockingManagedDelivery{}
	runner := FlowRunner{
		Engine: engine, Checkpoints: managedCheckpointStore{}, Authority: &failingRenewAuthority{renewErr: renewFailure}, Deliveries: deliveries, SchemaBaselines: flowRunnerSchemaBaselines{},
		ExpectedGeneration: control.Generation, ExecutionID: "artifact-fingerprint", ExecutionBackend: "test",
		Artifacts: func(_ context.Context, _ flow.Flow, destinations []stream.DestinationConfig) (stream.ManagedArtifactLog, error) {
			if len(destinations) != 1 || destinations[0].Projector == nil || destinations[0].MappingFingerprint == "" || destinations[0].Projector.Fingerprint() != destinations[0].MappingFingerprint {
				return nil, errors.New("missing immutable materialized projector")
			}
			return &effectiveArtifactLog{fingerprint: "effective-deployment-fingerprint"}, nil
		},
	}
	err := runner.Run(ctx, f, &blockingManagedSource{}, []stream.DestinationConfig{{Spec: lakeSpec, Dest: artifactMarkerDestination{}}})
	if !errors.Is(err, renewFailure) {
		t.Fatalf("Run() error=%v, want controlled heartbeat failure", err)
	}
	projectionFingerprint, err := f.Config.TableMappings.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	want, err := connector.BindProjectionFingerprint("effective-deployment-fingerprint", projectionFingerprint)
	if err != nil {
		t.Fatal(err)
	}
	if deliveries.registeredFingerprint != want {
		t.Fatalf("registered fingerprint=%q, want deployment and projection fingerprint %q", deliveries.registeredFingerprint, want)
	}
}

func TestFlowRunnerUsesProjectionBoundSpecFingerprintWithoutCatalogConsumer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	engine := workflow.NewMemoryEngine()
	f := managedAdmissionFlow()
	f.Config.AckPolicy = stream.AckPolicyMaterialized
	f.Config.Materialization = flow.MaterializationPolicy{ProjectionID: "canonical_cdc_parquet_v2"}
	lakeSpec := connector.RuntimeSpec{Name: "lake", Type: connector.EndpointIceberg, Options: map[string]string{"destination_revision_id": "iceberg-v1"}}
	f.Destinations = []*wallabypb.Endpoint{runnerTestDestination(lakeSpec)}
	f.Config.TableMappings = flow.NewTableMappings([]connector.RuntimeSpec{lakeSpec})
	if _, err := engine.Create(ctx, f); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Start(ctx, f.ID); err != nil {
		t.Fatal(err)
	}
	control, _ := engine.Control(ctx, f.ID)
	renewFailure := errors.New("stop after fingerprint registration")
	deliveries := &blockingManagedDelivery{}
	// A materialized barrier-only publication whose destination is not a canonical
	// artifact consumer has no catalog identity, so the spec fingerprint must stand.
	destination := stream.DestinationConfig{Spec: lakeSpec, Dest: artifactMarkerDestination{}}
	projectionFingerprint, err := f.Config.TableMappings.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	want, err := endpointcodec.DeliveryConfigFingerprint(f.Destinations[0], projectionFingerprint)
	if err != nil {
		t.Fatal(err)
	}
	runner := FlowRunner{
		Engine: engine, Checkpoints: managedCheckpointStore{}, Authority: &failingRenewAuthority{renewErr: renewFailure}, Deliveries: deliveries, SchemaBaselines: flowRunnerSchemaBaselines{},
		ExpectedGeneration: control.Generation, ExecutionID: "artifact-no-catalog", ExecutionBackend: "test",
		Artifacts: func(context.Context, flow.Flow, []stream.DestinationConfig) (stream.ManagedArtifactLog, error) {
			return &effectiveArtifactLog{}, nil
		},
	}
	if err := runner.Run(ctx, f, &blockingManagedSource{}, []stream.DestinationConfig{destination}); !errors.Is(err, renewFailure) {
		t.Fatalf("Run() error=%v, want controlled heartbeat failure rather than empty-fingerprint admission refusal", err)
	}
	if deliveries.registeredFingerprint != want {
		t.Fatalf("registered fingerprint=%q, want spec-derived %q", deliveries.registeredFingerprint, want)
	}
}

func TestManagedDeliveryPruneIsBatchBoundedAndRenewsLease(t *testing.T) {
	t.Parallel()
	deliveries := &saturatedPruneDelivery{blockingManagedDelivery: &blockingManagedDelivery{}}
	renewals := 0
	err := pruneManagedDeliveryState(context.Background(), deliveries, authority.RunFence{}, time.Hour, func(context.Context) error {
		renewals++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := deliveries.pruneCalls.Load(); got != 8 {
		t.Fatalf("prune batches=%d, want bounded maximum 8", got)
	}
	if renewals != 8 {
		t.Fatalf("lease renewals between saturated prune batches=%d, want 8", renewals)
	}
}

func TestManagedDeliveryRetentionRunsDuringLongLivedFlow(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	engine := workflow.NewMemoryEngine()
	f := managedAdmissionFlow()
	f.ID = "managed-periodic-retention"
	f.Source.GetPostgresSource().DeliveryPruneInterval = durationpb.New(10 * time.Millisecond)
	if _, err := engine.Create(ctx, f); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Start(ctx, f.ID); err != nil {
		t.Fatal(err)
	}
	control, _ := engine.Control(ctx, f.ID)
	deliveries := &blockingManagedDelivery{}
	runner := FlowRunner{
		Engine: engine, Checkpoints: managedCheckpointStore{}, Authority: &failingRenewAuthority{}, Deliveries: deliveries, SchemaBaselines: flowRunnerSchemaBaselines{},
		ExpectedGeneration: control.Generation, ExecutionID: "managed-retention", ExecutionBackend: "test",
	}
	err := runner.Run(ctx, f, &blockingManagedSource{}, []stream.DestinationConfig{{
		Spec: managedAdmissionDestinations()[0].Spec,
		Dest: blockingManagedDestination{},
	}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error=%v, want bounded test deadline", err)
	}
	if got := deliveries.pruneCalls.Load(); got < 2 {
		t.Fatalf("delivery retention prune calls=%d, want startup plus periodic sweep", got)
	}
}

func TestManagedIndeterminateDeliveryStaysRecoverable(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 125*time.Millisecond)
	defer cancel()
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
		Engine: engine, Checkpoints: managedCheckpointStore{}, Authority: authorityStore, Deliveries: deliveries, SchemaBaselines: flowRunnerSchemaBaselines{},
		ExpectedGeneration: control.Generation, ExecutionID: "managed-indeterminate", ExecutionBackend: "test",
	}
	err := runner.Run(ctx, f, &singleTransactionManagedSource{}, []stream.DestinationConfig{{
		Spec: managedAdmissionDestinations()[0].Spec,
		Dest: blockingManagedDestination{},
	}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error=%v, want bounded test cancellation while delivery remains indeterminate", err)
	}
	if deliveries.deliverCalls.Load() < 2 {
		t.Fatalf("DeliverTransaction calls=%d, want reconciliation retries before cancellation", deliveries.deliverCalls.Load())
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

type blockingManagedDelivery struct {
	deliverErr            error
	deliverCalls          atomic.Int32
	pruneCalls            atomic.Int32
	registeredFingerprint string
}

type saturatedPruneDelivery struct {
	*blockingManagedDelivery
}

func (d *saturatedPruneDelivery) PruneTerminalDeliveryState(context.Context, authority.RunFence, time.Duration, int) (int64, error) {
	d.pruneCalls.Add(1)
	return 1000, nil
}

func (d *blockingManagedDelivery) RegisterDestinationRevision(_ context.Context, _ authority.RunFence, _, _, fingerprint string) error {
	d.registeredFingerprint = fingerprint
	return nil
}
func (d *blockingManagedDelivery) PruneTerminalDeliveryState(context.Context, authority.RunFence, time.Duration, int) (int64, error) {
	d.pruneCalls.Add(1)
	return 0, nil
}
func (*blockingManagedDelivery) AuthorizeAck(_ context.Context, _ connector.RunFence, checkpoint connector.Checkpoint, _ connector.ManagedSchemaBaselinePayload) (connector.AckGrant, error) {
	position, err := connector.CheckpointPositionID(checkpoint)
	return connector.AckGrant{Checkpoint: checkpoint, PositionID: position}, err
}
func (d *blockingManagedDelivery) DeliverTransaction(context.Context, connector.RunFence, connector.DeliveryIntent, connector.SourceTransaction, connector.ManagedSchemaBaselinePayload, connector.ManagedTransactionDestination) (connector.AckGrant, error) {
	d.deliverCalls.Add(1)
	if d.deliverErr != nil {
		return connector.AckGrant{}, d.deliverErr
	}
	return connector.AckGrant{}, errors.New("unexpected transaction delivery")
}
func (*blockingManagedDelivery) ValidateAckGrant(context.Context, connector.RunFence, connector.AckGrant) error {
	return nil
}
func (*blockingManagedDelivery) RecordAckReceipt(context.Context, connector.RunFence, connector.AckGrant, string) error {
	return nil
}
func (*blockingManagedDelivery) CommitSourceFeedback(ctx context.Context, _ connector.RunFence, grant connector.AckGrant, source connector.FlushEvidenceSource) error {
	_, err := source.AckWithEvidence(ctx, grant.Checkpoint)
	return err
}

type effectiveArtifactLog struct {
	fingerprint string
}

func (l *effectiveArtifactLog) EffectiveDestinationFingerprint() string { return l.fingerprint }
func (*effectiveArtifactLog) Recover(context.Context, connector.RunFence) error {
	return nil
}
func (*effectiveArtifactLog) RestoreCheckpoint(_ context.Context, _ connector.RunFence, checkpoint connector.Checkpoint) (connector.AckGrant, error) {
	position, err := connector.CheckpointPositionID(checkpoint)
	return connector.AckGrant{Checkpoint: checkpoint, PositionID: position}, err
}
func (*effectiveArtifactLog) WaitForReadAdmission(context.Context, connector.RunFence) error {
	return nil
}
func (*effectiveArtifactLog) Append(_ context.Context, _ connector.RunFence, transaction connector.SourceTransaction, _ connector.ManagedSchemaBaselinePayload) (connector.AckGrant, error) {
	position, err := connector.CheckpointPositionID(transaction.Checkpoint)
	return connector.AckGrant{Checkpoint: transaction.Checkpoint, PositionID: position}, err
}

type artifactMarkerDestination struct{}

func (artifactMarkerDestination) Open(context.Context, connector.RuntimeSpec) error { return nil }
func (artifactMarkerDestination) Write(context.Context, connector.Batch) error {
	return errors.New("unexpected direct artifact write")
}
func (artifactMarkerDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return errors.New("unexpected direct artifact DDL")
}
func (artifactMarkerDestination) TypeMappings() map[string]string { return map[string]string{} }
func (artifactMarkerDestination) Close(context.Context) error     { return nil }
func (artifactMarkerDestination) CanonicalArtifactConsumer()      {}
func (artifactMarkerDestination) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		Support:           connector.SupportExperimental,
		Delivery:          connector.DeliverySemantics{IdempotentReplay: true, ReplaySafe: true},
		SupportsStreaming: true, SupportedWireFormats: []connector.WireFormat{connector.WireFormatParquet},
	}
}

type blockingManagedSource struct{}

func (*blockingManagedSource) BindRunFence(connector.RunFence) error             { return nil }
func (*blockingManagedSource) Open(context.Context, connector.RuntimeSpec) error { return nil }
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
func (*blockingManagedSource) AckWithEvidence(_ context.Context, checkpoint connector.Checkpoint) (connector.SourceFlushEvidence, error) {
	return connector.SourceFlushEvidence{ObservedFlushLSN: checkpoint.LSN}, nil
}
func (*blockingManagedSource) Close(context.Context) error { return nil }
func (*blockingManagedSource) Capabilities() connector.Capabilities {
	return connector.Capabilities{Support: connector.SupportExperimental, SupportsStreaming: true}
}

type singleTransactionManagedSource struct{ read bool }

func (*singleTransactionManagedSource) BindRunFence(connector.RunFence) error             { return nil }
func (*singleTransactionManagedSource) Open(context.Context, connector.RuntimeSpec) error { return nil }
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
			Schema:  connector.Schema{Name: "events", Namespace: "public", Version: 1, Columns: []connector.Column{{Name: "id", Type: "int8"}}},
			Records: []connector.Record{{Table: "events", Operation: connector.OpInsert, SchemaVersion: 1, After: map[string]any{"id": int64(1)}}},
		}}},
	}, nil
}
func (*singleTransactionManagedSource) InitialCheckpoint() (connector.Checkpoint, bool) {
	return connector.Checkpoint{LSN: "0/10"}, true
}
func (*singleTransactionManagedSource) Ack(context.Context, connector.Checkpoint) error { return nil }
func (*singleTransactionManagedSource) AckWithEvidence(_ context.Context, checkpoint connector.Checkpoint) (connector.SourceFlushEvidence, error) {
	return connector.SourceFlushEvidence{ObservedFlushLSN: checkpoint.LSN}, nil
}
func (*singleTransactionManagedSource) Close(context.Context) error { return nil }
func (*singleTransactionManagedSource) Capabilities() connector.Capabilities {
	return connector.Capabilities{Support: connector.SupportExperimental, SupportsStreaming: true}
}

type blockingManagedDestination struct{}

func (blockingManagedDestination) Open(context.Context, connector.RuntimeSpec) error { return nil }
func (blockingManagedDestination) Write(context.Context, connector.Batch) error      { return nil }
func (blockingManagedDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return nil
}
func (blockingManagedDestination) TypeMappings() map[string]string { return nil }
func (blockingManagedDestination) Close(context.Context) error     { return nil }
func (blockingManagedDestination) Capabilities() connector.Capabilities {
	return connector.Capabilities{Support: connector.SupportExperimental, TableWrites: connector.TableWriteSemantics{Append: true}, Delivery: connector.DeliverySemantics{TransactionalBatch: true, IdempotentReplay: true, ReplaySafe: true}}
}
func (blockingManagedDestination) Apply(context.Context, connector.DeliveryIntent, connector.Batch) (connector.DeliveryEvidence, error) {
	return connector.DeliveryEvidence{}, nil
}
func (blockingManagedDestination) InitializeManagedDelivery(context.Context) error { return nil }
func (blockingManagedDestination) ValidateTransaction(context.Context, connector.SourceTransaction) error {
	return nil
}
func (blockingManagedDestination) ApplyTransaction(context.Context, connector.DeliveryIntent, connector.SourceTransaction) (connector.DeliveryEvidence, error) {
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

func (e *recordingExecutionEngine) RegisterExecutionFence(ctx context.Context, flowID, executionID, backend string, generation int64, lease time.Duration) (workflow.ExecutionFence, error) {
	e.backend, e.executionID, e.generation = backend, executionID, generation
	return e.MemoryEngine.RegisterExecutionFence(ctx, flowID, executionID, backend, generation, lease)
}

type flowRunnerSource struct {
	openSpec connector.RuntimeSpec
}

func (s *flowRunnerSource) Open(_ context.Context, spec connector.RuntimeSpec) error {
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

func (flowRunnerDestination) Open(context.Context, connector.RuntimeSpec) error { return nil }
func (flowRunnerDestination) Write(context.Context, connector.Batch) error      { return nil }
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
		TableWrites: connector.TableWriteSemantics{Append: true},
		Delivery: connector.DeliverySemantics{
			TransactionalBatch: true,
			IdempotentReplay:   true,
			ReplaySafe:         true,
			ExecutesDDL:        true,
		},
	}
}

type flowRunnerSchemaBaselines struct{}

func (flowRunnerSchemaBaselines) Load(context.Context, connector.RunFence, string) ([]connector.Schema, error) {
	return nil, nil
}

func (flowRunnerSchemaBaselines) Persist(context.Context, connector.RunFence, string, []connector.Schema) error {
	return nil
}
