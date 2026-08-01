package stream

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	checkpointstore "github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestRunnerPrimaryAckRequiresDurableOutbox(t *testing.T) {
	runner := Runner{
		Source: &fakeSource{log: &eventLog{}},
		Destinations: []DestinationConfig{
			{Spec: connector.Spec{Name: "primary"}, Dest: &recordingDest{log: &eventLog{}, name: "primary"}},
			{Spec: connector.Spec{Name: "secondary"}, Dest: &recordingDest{log: &eventLog{}, name: "secondary"}},
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
		Source: source, SourceSpec: connector.Spec{Options: map[string]string{"mode": "backfill"}}, Checkpoints: store, CheckpointOutbox: store,
		Destinations: []DestinationConfig{
			{Spec: connector.Spec{Name: "primary"}, Dest: &recordingDest{log: &eventLog{}, name: "primary"}},
			{Spec: connector.Spec{Name: "secondary-a"}, Dest: completed},
			{Spec: connector.Spec{Name: "secondary-b"}, Dest: failed},
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
		Destination: "removed-secondary", PositionID: position, Batch: batch,
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
		Source: source, SourceSpec: connector.Spec{Options: map[string]string{"mode": "backfill"}}, Checkpoints: store, CheckpointOutbox: store,
		Destinations: []DestinationConfig{
			{Spec: connector.Spec{Name: "primary"}, Dest: &recordingDest{log: &eventLog{}, name: "primary"}},
			{Spec: connector.Spec{Name: "secondary"}, Dest: secondary},
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
