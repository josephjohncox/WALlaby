package stream

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestRunnerRejectsMissingCheckpointStore(t *testing.T) {
	t.Parallel()

	source := &fakeSource{batches: []connector.Batch{{Checkpoint: connector.Checkpoint{LSN: "1"}}}}
	runner := Runner{
		Source:     source,
		SourceSpec: connector.Spec{Options: map[string]string{"mode": connector.SourceModeBackfill}},
		Destinations: []DestinationConfig{{
			Spec: connector.Spec{Name: "dest"},
			Dest: &recordingDest{name: "dest"},
		}},
		FlowID: "flow-no-checkpoint-store",
	}

	err := runner.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "checkpoint") {
		t.Fatalf("Run() error = %v, want checkpoint durability error", err)
	}
	if len(source.acks) != 0 {
		t.Fatalf("source acknowledgements = %v, want none", source.acks)
	}
}

func TestRunnerPersistsBeforeSourceAck(t *testing.T) {
	t.Parallel()

	checkpoint := connector.Checkpoint{
		LSN: "10",
		Metadata: map[string]string{
			"control": "true",
			"seq":     "10",
		},
	}
	log := &eventLog{}
	source := &fakeSource{batches: []connector.Batch{{Checkpoint: checkpoint}}, log: log}
	checkpoints := &recordingCheckpointStore{}
	traceSink := &MemoryTraceSink{}
	runner := checkpointTestRunner(source, checkpoints, traceSink)

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	var got []string
	for _, event := range traceSink.Events() {
		if event.LSN == checkpoint.LSN {
			got = append(got, event.Kind)
		}
	}
	want := []string{"read", "deliver", "checkpoint", "ack", "control_checkpoint"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("trace order = %v, want %v", got, want)
	}
	if len(source.acks) != 1 || source.acks[0].LSN != checkpoint.LSN {
		t.Fatalf("source acknowledgements = %v, want checkpoint %s", source.acks, checkpoint.LSN)
	}
	if len(checkpoints.puts) != 1 || checkpoints.puts[0].LSN != checkpoint.LSN {
		t.Fatalf("persisted checkpoints = %v, want checkpoint %s", checkpoints.puts, checkpoint.LSN)
	}
}

func TestRunnerIgnoresEmptyHeartbeatWithoutCheckpointPosition(t *testing.T) {
	t.Parallel()

	source := &fakeSource{batches: []connector.Batch{{}}, log: &eventLog{}}
	checkpoints := &recordingCheckpointStore{}
	traceSink := &MemoryTraceSink{}
	runner := checkpointTestRunner(source, checkpoints, traceSink)

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(source.acks) != 0 {
		t.Fatalf("source acknowledgements = %v, want none", source.acks)
	}
	if len(checkpoints.puts) != 0 {
		t.Fatalf("persisted checkpoints = %v, want none", checkpoints.puts)
	}
	if len(traceSink.Events()) != 0 {
		t.Fatalf("trace events = %v, want none", traceSink.Events())
	}
}

func TestRunnerAdvancesEmptyBatchWithDurableSourcePosition(t *testing.T) {
	t.Parallel()

	checkpoint := connector.Checkpoint{LSN: "0/20"}
	source := &fakeSource{batches: []connector.Batch{{Checkpoint: checkpoint}}, log: &eventLog{}}
	checkpoints := &recordingCheckpointStore{}
	traceSink := &MemoryTraceSink{}
	runner := checkpointTestRunner(source, checkpoints, traceSink)

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(source.acks) != 1 || source.acks[0].LSN != checkpoint.LSN {
		t.Fatalf("source acknowledgements = %v, want empty-batch position %s", source.acks, checkpoint.LSN)
	}
	if len(checkpoints.puts) != 1 || checkpoints.puts[0].LSN != checkpoint.LSN {
		t.Fatalf("persisted checkpoints = %v, want empty-batch position %s", checkpoints.puts, checkpoint.LSN)
	}
	var got []string
	for _, event := range traceSink.Events() {
		got = append(got, event.Kind)
	}
	want := []string{"read", "deliver", "checkpoint", "ack"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("trace order = %v, want %v", got, want)
	}
}

func TestRunnerTraceValidatesActualBackfillCheckpointShape(t *testing.T) {
	t.Parallel()

	checkpoint := connector.Checkpoint{Metadata: map[string]string{
		"mode":  connector.SourceModeBackfill,
		"table": "public.accounts",
		"done":  "true",
	}}
	source := &fakeSource{batches: []connector.Batch{{
		Schema:     connector.Schema{Name: "accounts", Namespace: "public"},
		Records:    []connector.Record{{Table: "accounts", Operation: connector.OpLoad}},
		Checkpoint: checkpoint,
	}}, log: &eventLog{}}
	traceSink := &MemoryTraceSink{}
	runner := checkpointTestRunner(source, &recordingCheckpointStore{}, traceSink)

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if err := ValidateTrace(traceSink.Events(), TraceValidationOptions{}); err != nil {
		t.Fatalf("ValidateTrace(actual backfill checkpoint) error = %v; events=%+v", err, traceSink.Events())
	}
	if len(source.acks) != 1 || !reflect.DeepEqual(source.acks[0].Metadata, checkpoint.Metadata) {
		t.Fatalf("source acknowledgements = %+v, want actual backfill checkpoint", source.acks)
	}
}

func TestRunnerBackfillTraceIdentitySurvivesRestart(t *testing.T) {
	t.Parallel()

	store := &recordingCheckpointStore{}
	traceSink := &MemoryTraceSink{}
	firstCheckpoint := connector.Checkpoint{Metadata: map[string]string{
		"mode": connector.SourceModeBackfill, "table": "public.accounts", "partition_index": "1", "partition_count": "4", "partition": "1/4", "cursor": "100",
	}}
	firstSource := &fakeSource{batches: []connector.Batch{{
		Schema: connector.Schema{Name: "accounts", Namespace: "public"}, Records: []connector.Record{{Table: "accounts", Operation: connector.OpLoad}}, Checkpoint: firstCheckpoint,
	}}, log: &eventLog{}}
	first := checkpointTestRunner(firstSource, store, traceSink)
	if err := first.Run(context.Background()); err != nil {
		t.Fatalf("first Run() error = %v", err)
	}

	secondCheckpoint := connector.Checkpoint{Metadata: map[string]string{
		"cursor": "200", "partition": "1/4", "partition_count": "4", "partition_index": "1", "table": "public.accounts", "mode": connector.SourceModeBackfill, "done": "true",
	}}
	secondSource := &fakeSource{batches: []connector.Batch{{
		Schema: connector.Schema{Name: "accounts", Namespace: "public"}, Records: []connector.Record{{Table: "accounts", Operation: connector.OpLoad}}, Checkpoint: secondCheckpoint,
	}}, log: &eventLog{}}
	second := checkpointTestRunner(secondSource, store, traceSink)
	if err := second.Run(context.Background()); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}

	firstID, _ := connector.CheckpointPositionID(firstCheckpoint)
	secondID, _ := connector.CheckpointPositionID(secondCheckpoint)
	if firstID == secondID {
		t.Fatalf("distinct backfill cursors share identity %q", firstID)
	}
	var restored, restoredAck string
	for _, event := range traceSink.Events() {
		switch event.Kind {
		case "restore_checkpoint":
			restored = event.Position
		case "restore_ack":
			restoredAck = event.Position
		}
	}
	if restored != firstID || restoredAck != firstID {
		t.Fatalf("restored positions checkpoint=%q ack=%q, want original %q", restored, restoredAck, firstID)
	}
	if err := ValidateTrace(traceSink.Events(), TraceValidationOptions{}); err != nil {
		t.Fatalf("combined restart trace invalid: %v; events=%+v", err, traceSink.Events())
	}
}

func TestRunnerDoesNotAckWhenCheckpointPersistenceFails(t *testing.T) {
	t.Parallel()

	persistErr := errors.New("checkpoint unavailable")
	checkpoint := connector.Checkpoint{LSN: "10", Metadata: map[string]string{"control": "true"}}
	source := &fakeSource{batches: []connector.Batch{{Checkpoint: checkpoint}}, log: &eventLog{}}
	store := &recordingCheckpointStore{putErr: persistErr}
	traceSink := &MemoryTraceSink{}
	runner := checkpointTestRunner(source, store, traceSink)

	err := runner.Run(context.Background())
	if !errors.Is(err, persistErr) {
		t.Fatalf("Run() error = %v, want persistence error", err)
	}
	if len(source.acks) != 0 {
		t.Fatalf("source acknowledgements = %v, want none", source.acks)
	}
	assertTraceKinds(t, traceSink.Events(), "checkpoint_error")
}

func TestRunnerLeavesDurableCheckpointWhenSourceAckFails(t *testing.T) {
	t.Parallel()

	ackErr := errors.New("source ack unavailable")
	checkpoint := connector.Checkpoint{LSN: "10", Metadata: map[string]string{"control": "true"}}
	source := &fakeSource{batches: []connector.Batch{{Checkpoint: checkpoint}}, log: &eventLog{}, ackErr: ackErr}
	store := &recordingCheckpointStore{}
	traceSink := &MemoryTraceSink{}
	runner := checkpointTestRunner(source, store, traceSink)

	err := runner.Run(context.Background())
	if !errors.Is(err, ackErr) {
		t.Fatalf("Run() error = %v, want ack error", err)
	}
	if len(store.puts) != 1 || store.puts[0].LSN != checkpoint.LSN {
		t.Fatalf("persisted checkpoints = %v, want %s", store.puts, checkpoint.LSN)
	}
	assertTraceKinds(t, traceSink.Events(), "checkpoint", "ack_error")
}

func TestRunnerRestoresCheckpointOnceAndAcknowledgesIdempotently(t *testing.T) {
	t.Parallel()

	restored := connector.Checkpoint{LSN: "0/16B6C50"}
	source := &fakeSource{log: &eventLog{}}
	store := &recordingCheckpointStore{checkpoint: restored}
	traceSink := &MemoryTraceSink{}
	runner := checkpointTestRunner(source, store, traceSink)

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if store.gets != 1 {
		t.Fatalf("checkpoint reads = %d, want 1", store.gets)
	}
	if got := source.openSpec.Options["start_lsn"]; got != restored.LSN {
		t.Fatalf("source start_lsn = %q, want %q", got, restored.LSN)
	}
	if len(source.acks) != 1 || source.acks[0].LSN != restored.LSN {
		t.Fatalf("restore acknowledgements = %v, want %s", source.acks, restored.LSN)
	}
	assertTraceKinds(t, traceSink.Events(), "restore_ack")
}

func TestRunnerExplicitStartLSNTakesPrecedenceWithoutAckingConflictingRestore(t *testing.T) {
	t.Parallel()

	restored := connector.Checkpoint{LSN: "0/16B6C50"}
	explicit := "0/16B6D00"
	source := &fakeSource{log: &eventLog{}}
	store := &recordingCheckpointStore{checkpoint: restored}
	traceSink := &MemoryTraceSink{}
	runner := checkpointTestRunner(source, store, traceSink)
	runner.SourceSpec.Options["start_lsn"] = explicit

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := source.openSpec.Options["start_lsn"]; got != explicit {
		t.Fatalf("source start_lsn = %q, want explicit %q", got, explicit)
	}
	if len(source.acks) != 0 {
		t.Fatalf("restore acknowledgements = %v, want none for conflicting explicit start", source.acks)
	}
	for _, event := range traceSink.Events() {
		if event.Kind == "restore_ack" {
			t.Fatalf("unexpected restore_ack trace: %+v", event)
		}
	}
}

func TestRunnerPropagatesCheckpointRestoreErrors(t *testing.T) {
	t.Parallel()

	restoreErr := errors.New("checkpoint database unavailable")
	source := &fakeSource{log: &eventLog{}}
	store := &recordingCheckpointStore{getErr: restoreErr}
	runner := checkpointTestRunner(source, store, &MemoryTraceSink{})

	err := runner.Run(context.Background())
	if !errors.Is(err, restoreErr) || !strings.Contains(err.Error(), "restore checkpoint") {
		t.Fatalf("Run() error = %v, want wrapped restore error", err)
	}
	if store.gets != 1 {
		t.Fatalf("checkpoint reads = %d, want 1", store.gets)
	}
	if source.openSpec.Options != nil {
		t.Fatalf("source opened despite restore error: %+v", source.openSpec)
	}
}

func TestRunnerPropagatesRestoredCheckpointAckErrors(t *testing.T) {
	t.Parallel()

	ackErr := errors.New("restore ack unavailable")
	source := &fakeSource{log: &eventLog{}, ackErr: ackErr}
	store := &recordingCheckpointStore{checkpoint: connector.Checkpoint{LSN: "0/16B6C50"}}
	traceSink := &MemoryTraceSink{}
	runner := checkpointTestRunner(source, store, traceSink)

	err := runner.Run(context.Background())
	if !errors.Is(err, ackErr) || !strings.Contains(err.Error(), "ack restored checkpoint") {
		t.Fatalf("Run() error = %v, want wrapped restore ack error", err)
	}
	assertTraceKinds(t, traceSink.Events(), "restore_ack_error")
}

func checkpointTestRunner(source connector.Source, store connector.CheckpointStore, traceSink TraceSink) Runner {
	return Runner{
		Source:     source,
		SourceSpec: connector.Spec{Options: map[string]string{"mode": "backfill"}},
		Destinations: []DestinationConfig{{
			Spec: connector.Spec{Name: "dest"},
			Dest: &recordingDest{log: &eventLog{}, name: "dest"},
		}},
		Checkpoints: store,
		FlowID:      "flow-checkpoint",
		TraceSink:   traceSink,
	}
}

func assertTraceKinds(t *testing.T, events []TraceEvent, want ...string) {
	t.Helper()
	seen := make(map[string]bool, len(events))
	for _, event := range events {
		seen[event.Kind] = true
	}
	for _, kind := range want {
		if !seen[kind] {
			t.Fatalf("trace kinds = %v, want %q", seen, kind)
		}
	}
}
