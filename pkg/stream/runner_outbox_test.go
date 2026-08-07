package stream

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	checkpointstore "github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type testRenameProjector struct{}

func (testRenameProjector) Fingerprint() string { return "projection-v1" }
func (testRenameProjector) ProjectBatch(batch connector.Batch) (connector.Batch, ProjectionDecision, error) {
	batch.Schema.Namespace, batch.Schema.Name = "mapped", "events_v2"
	for i := range batch.Records {
		batch.Records[i].Table = "events_v2"
	}
	batch.WritePolicy = connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend, ProjectionFingerprint: "projection-v1"}
	return batch, ProjectionIncluded, nil
}
func (testRenameProjector) ProjectTransaction(transaction connector.SourceTransaction) (connector.SourceTransaction, ProjectionDecision, error) {
	return transaction, ProjectionIncluded, nil
}

func TestRunnerProjectsBeforeDirectWrite(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteOutboxStore(t, ctx)
	batch := outboxTestBatch("0/10")
	source := &fakeSource{batches: []connector.Batch{batch}, log: &eventLog{}}
	destination := &recordingDest{log: &eventLog{}, name: "mapped"}
	runner := Runner{Source: source, SourceSpec: connector.RuntimeSpec{}, Destinations: []DestinationConfig{{Spec: connector.RuntimeSpec{Name: "mapped"}, Dest: destination, Projector: testRenameProjector{}, MappingFingerprint: "projection-v1"}}, Checkpoints: store, FlowID: "projection-flow", AckPolicy: AckPolicyAll}
	if err := runner.Run(ctx); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if len(destination.writes) != 1 || destination.writes[0].Schema.Namespace != "mapped" || destination.writes[0].Records[0].Table != "events_v2" {
		t.Fatalf("destination writes=%+v, want mapped batch", destination.writes)
	}
}

type filterAllProjector struct{}

func (filterAllProjector) Fingerprint() string { return "filter-all-v1" }
func (filterAllProjector) ProjectBatch(batch connector.Batch) (connector.Batch, ProjectionDecision, error) {
	return connector.Batch{Checkpoint: batch.Checkpoint}, ProjectionFiltered, nil
}
func (filterAllProjector) ProjectTransaction(transaction connector.SourceTransaction) (connector.SourceTransaction, ProjectionDecision, error) {
	return transaction, ProjectionFiltered, nil
}

func TestRunnerPrimaryAckBuffersPositionlessPostgresFragmentsUntilCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteOutboxStore(t, ctx)
	positionless := outboxTestBatch("")
	final := outboxTestBatch("0/20")
	source := &fakeSource{batches: []connector.Batch{positionless, final}, log: &eventLog{}}
	primary := &recordingDest{log: &eventLog{}, name: "primary"}
	secondary := &recordingDest{log: &eventLog{}, name: "secondary"}
	runner := outboxTestRunner(source, secondary, store)
	runner.SourceSpec.Type = connector.EndpointPostgres
	runner.Destinations[0].Dest = primary
	runner.Destinations[0].Projector = testRenameProjector{}
	runner.Destinations[0].MappingFingerprint = "projection-v1"
	runner.Destinations[1].Projector = testRenameProjector{}
	runner.Destinations[1].MappingFingerprint = "projection-v1"
	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(primary.writes) != 2 || len(secondary.writes) != 2 {
		t.Fatalf("primary/secondary writes=%d/%d, want both fragments", len(primary.writes), len(secondary.writes))
	}
	if len(source.acks) != 1 || source.acks[0].LSN != "0/20" {
		t.Fatalf("acks=%+v, want only final checkpoint", source.acks)
	}
	for _, write := range append(primary.writes, secondary.writes...) {
		if write.Schema.Name != "events_v2" {
			t.Fatalf("unprojected positionless write=%+v", write.Schema)
		}
	}
	pending, err := store.ListOutbox(ctx, runner.FlowID)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
}

func TestRunnerPrimaryAckFlushesPositionlessFragmentAtEmptyCommitCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteOutboxStore(t, ctx)
	final := connector.Batch{Checkpoint: connector.Checkpoint{LSN: "0/24"}}
	source := &fakeSource{batches: []connector.Batch{outboxTestBatch(""), final}, log: &eventLog{}}
	secondary := &recordingDest{log: &eventLog{}, name: "secondary"}
	runner := outboxTestRunner(source, secondary, store)
	runner.SourceSpec.Type = connector.EndpointPostgres
	if err := runner.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(secondary.writes) != 1 || len(source.acks) != 1 {
		t.Fatalf("secondary writes/acks=%d/%d", len(secondary.writes), len(source.acks))
	}
}

func TestRunnerRestoresDurablePositionlessFragmentOutbox(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteOutboxStore(t, ctx)
	source := &fakeSource{batches: []connector.Batch{outboxTestBatch(""), outboxTestBatch("0/28")}, log: &eventLog{}}
	failed := &flakySecondaryDest{recordingDest: recordingDest{log: &eventLog{}, name: "secondary"}, failures: 10}
	first := outboxTestRunner(source, failed, store)
	first.SourceSpec.Type = connector.EndpointPostgres
	first.GiveUpPolicy = GiveUpPolicyOnRetryExhaustion
	if err := first.Run(ctx); err == nil {
		t.Fatal("first run unexpectedly drained secondary")
	}
	pending, err := store.ListOutbox(ctx, first.FlowID)
	if err != nil || len(pending) != 2 {
		t.Fatalf("pending fragments=%d err=%v, want 2", len(pending), err)
	}
	recovered := &recordingDest{log: &eventLog{}, name: "secondary"}
	second := outboxTestRunner(&fakeSource{log: &eventLog{}}, recovered, store)
	if err := second.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(recovered.writes) != 2 {
		t.Fatalf("recovered writes=%d, want 2", len(recovered.writes))
	}
}

func TestRunnerPrimaryAckPositionlessFilteredAndPrimaryOnlyProgress(t *testing.T) {
	for name, secondary := range map[string]connector.Destination{"filtered": &recordingDest{log: &eventLog{}, name: "secondary"}, "primary_only": nil} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newSQLiteOutboxStore(t, ctx)
			source := &fakeSource{batches: []connector.Batch{outboxTestBatch(""), outboxTestBatch("0/30")}, log: &eventLog{}}
			primary := &recordingDest{log: &eventLog{}, name: "primary"}
			runner := outboxTestRunner(source, secondary, store)
			runner.SourceSpec.Type = connector.EndpointPostgres
			runner.Destinations[0].Dest = primary
			runner.Destinations[0].Projector = filterAllProjector{}
			runner.Destinations[0].MappingFingerprint = "filter-all-v1"
			if secondary == nil {
				runner.Destinations = runner.Destinations[:1]
			} else {
				runner.Destinations[1].Projector = filterAllProjector{}
				runner.Destinations[1].MappingFingerprint = "filter-all-v1"
			}
			if err := runner.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if len(source.acks) != 1 || len(primary.writes) != 0 {
				t.Fatalf("acks/primary writes=%d/%d", len(source.acks), len(primary.writes))
			}
		})
	}
}

func TestRunnerPrimaryAckRequiresDurableOutbox(t *testing.T) {
	runner := Runner{
		Source: &fakeSource{log: &eventLog{}},
		Destinations: []DestinationConfig{
			{Spec: connector.RuntimeSpec{Name: "primary"}, Dest: &recordingDest{log: &eventLog{}, name: "primary"}},
			{Spec: connector.RuntimeSpec{Name: "secondary"}, Dest: &recordingDest{log: &eventLog{}, name: "secondary"}},
		},
		FlowID: "flow", AckPolicy: AckPolicyPrimary, PrimaryDestination: "primary",
	}
	err := runner.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "atomic outbox") {
		t.Fatalf("Run() error = %v, want durable outbox requirement", err)
	}
}

func TestRunnerPrimaryAckRestartAfterPersistBeforeSourceAck(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteOutboxStore(t, ctx)
	batch := outboxTestBatch("1")
	firstSource := &fakeSource{batches: []connector.Batch{batch}, log: &eventLog{}, ackErr: errors.New("crash before ack")}
	firstSecondary := &recordingDest{log: &eventLog{}, name: "secondary"}
	first := outboxTestRunner(firstSource, firstSecondary, store)
	if err := first.Run(ctx); err == nil {
		t.Fatal("first run unexpectedly succeeded")
	}
	pending, err := store.ListOutbox(ctx, "outbox-flow")
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending after failed ack = %+v, %v", pending, err)
	}
	if len(firstSource.acks) != 0 || len(firstSecondary.writes) != 0 {
		t.Fatalf("before restart acks=%d secondary writes=%d", len(firstSource.acks), len(firstSecondary.writes))
	}

	secondSource := &fakeSource{log: &eventLog{}}
	secondSecondary := &recordingDest{log: &eventLog{}, name: "secondary"}
	second := outboxTestRunner(secondSource, secondSecondary, store)
	if err := second.Run(ctx); err != nil {
		t.Fatalf("restart run: %v", err)
	}
	if len(secondSecondary.writes) != 1 || len(secondSource.acks) != 1 {
		t.Fatalf("restart writes=%d restore acks=%d", len(secondSecondary.writes), len(secondSource.acks))
	}
	pending, err = store.ListOutbox(ctx, "outbox-flow")
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after restart = %+v, %v", pending, err)
	}
}

func TestRunnerPrimaryAckRestartAfterAckBeforeDrain(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteOutboxStore(t, ctx)
	batch := outboxTestBatch("2")
	firstSource := &fakeSource{batches: []connector.Batch{batch}, log: &eventLog{}}
	firstSecondary := &flakySecondaryDest{
		recordingDest: recordingDest{log: &eventLog{}, name: "secondary"}, failures: 10,
	}
	first := outboxTestRunner(firstSource, firstSecondary, store)
	first.GiveUpPolicy = GiveUpPolicyOnRetryExhaustion
	if err := first.Run(ctx); err == nil {
		t.Fatal("first run unexpectedly succeeded")
	}
	if len(firstSource.acks) != 1 {
		t.Fatalf("source acks = %d, want 1", len(firstSource.acks))
	}
	pending, err := store.ListOutbox(ctx, "outbox-flow")
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending after drain failure = %+v, %v", pending, err)
	}

	secondSecondary := &recordingDest{log: &eventLog{}, name: "secondary"}
	second := outboxTestRunner(&fakeSource{log: &eventLog{}}, secondSecondary, store)
	if err := second.Run(ctx); err != nil {
		t.Fatalf("restart run: %v", err)
	}
	if len(secondSecondary.writes) != 1 {
		t.Fatalf("restart secondary writes = %d, want 1", len(secondSecondary.writes))
	}
}

func TestRunnerPrimaryAckPreservesOnlyFailedSecondary(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteOutboxStore(t, ctx)
	batch := outboxTestBatch("3")
	source := &fakeSource{batches: []connector.Batch{batch}, log: &eventLog{}}
	completed := &recordingDest{log: &eventLog{}, name: "secondary-a"}
	failed := &flakySecondaryDest{recordingDest: recordingDest{log: &eventLog{}, name: "secondary-b"}, failures: 10}
	runner := Runner{
		Source: source, SourceSpec: connector.RuntimeSpec{Options: map[string]string{"mode": "backfill"}}, Checkpoints: store, CheckpointOutbox: store,
		Destinations: []DestinationConfig{
			{Spec: connector.RuntimeSpec{Name: "primary"}, Dest: &recordingDest{log: &eventLog{}, name: "primary"}, MappingFingerprint: "projection-primary"},
			{Spec: connector.RuntimeSpec{Name: "secondary-a"}, Dest: completed, MappingFingerprint: "projection-a"},
			{Spec: connector.RuntimeSpec{Name: "secondary-b"}, Dest: failed, MappingFingerprint: "projection-b"},
		},
		FlowID: "outbox-flow", AckPolicy: AckPolicyPrimary, PrimaryDestination: "primary", GiveUpPolicy: GiveUpPolicyOnRetryExhaustion,
	}
	if err := runner.Run(ctx); err == nil {
		t.Fatal("run unexpectedly succeeded")
	}
	pending, err := store.ListOutbox(ctx, "outbox-flow")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Destination != "secondary-b" {
		t.Fatalf("pending = %+v, want only secondary-b", pending)
	}
}

func TestRunnerRejectsUnknownRestoredOutboxDestination(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteOutboxStore(t, ctx)
	batch := outboxTestBatch("4")
	position, _ := connector.CheckpointPositionID(batch.Checkpoint)
	if err := store.PersistCheckpointAndOutbox(ctx, "outbox-flow", batch.Checkpoint, []connector.OutboxEntry{{
		Destination: "removed-secondary", PositionID: position, ProjectionFingerprint: "projection-secondary", Batch: batch,
	}}); err != nil {
		t.Fatal(err)
	}
	runner := outboxTestRunner(&fakeSource{log: &eventLog{}}, &recordingDest{log: &eventLog{}, name: "new-secondary"}, store)
	runner.Destinations[1].Spec.Name = "new-secondary"
	err := runner.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "removed-secondary") || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("Run() error = %v, want actionable unknown destination", err)
	}
}

func TestRunnerRejectsRestoredOutboxProjectionMismatch(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteOutboxStore(t, ctx)
	batch := outboxTestBatch("40")
	position, _ := connector.CheckpointPositionID(batch.Checkpoint)
	if err := store.PersistCheckpointAndOutbox(ctx, "outbox-flow", batch.Checkpoint, []connector.OutboxEntry{{
		Destination: "secondary", PositionID: position, ProjectionFingerprint: "old-projection", Batch: batch,
	}}); err != nil {
		t.Fatal(err)
	}
	runner := outboxTestRunner(&fakeSource{log: &eventLog{}}, &recordingDest{log: &eventLog{}, name: "secondary"}, store)
	err := runner.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "projection fingerprint") {
		t.Fatalf("Run() error=%v, want projection fingerprint mismatch", err)
	}
}

func TestRunnerDoesNotAckWhenAtomicOutboxPersistenceFails(t *testing.T) {
	persistErr := errors.New("outbox unavailable")
	store := &recordingCheckpointStore{putErr: persistErr}
	batch := outboxTestBatch("5")
	source := &fakeSource{batches: []connector.Batch{batch}, log: &eventLog{}}
	runner := outboxTestRunner(source, &recordingDest{log: &eventLog{}, name: "secondary"}, store)
	err := runner.Run(context.Background())
	if !errors.Is(err, persistErr) {
		t.Fatalf("Run() error = %v, want %v", err, persistErr)
	}
	if len(source.acks) != 0 {
		t.Fatalf("source acks = %d, want 0", len(source.acks))
	}
}

func newSQLiteOutboxStore(t *testing.T, ctx context.Context) *checkpointstore.SQLiteStore {
	t.Helper()
	store, err := checkpointstore.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "checkpoints.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func outboxTestRunner(source connector.Source, secondary connector.Destination, store connector.CheckpointOutboxStore) Runner {
	return Runner{
		Source: source, SourceSpec: connector.RuntimeSpec{Options: map[string]string{"mode": "backfill"}}, Checkpoints: store, CheckpointOutbox: store,
		Destinations: []DestinationConfig{
			{Spec: connector.RuntimeSpec{Name: "primary"}, Dest: &recordingDest{log: &eventLog{}, name: "primary"}, MappingFingerprint: "projection-primary"},
			{Spec: connector.RuntimeSpec{Name: "secondary"}, Dest: secondary, MappingFingerprint: "projection-secondary"},
		},
		FlowID: "outbox-flow", AckPolicy: AckPolicyPrimary, PrimaryDestination: "primary", GiveUpPolicy: GiveUpPolicyNever,
	}
}

func outboxTestBatch(lsn string) connector.Batch {
	return connector.Batch{
		Checkpoint: connector.Checkpoint{LSN: lsn, Metadata: map[string]string{"seq": lsn}},
		Schema:     connector.Schema{Name: "events", Namespace: "public"},
		Records:    []connector.Record{{Table: "events", Operation: connector.OpInsert, Payload: []byte(`{"id":` + lsn + `}`)}},
	}
}
