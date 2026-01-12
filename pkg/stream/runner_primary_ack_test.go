package stream

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

type flakySecondaryDest struct {
	recordingDest
	failures int
}

func (d *flakySecondaryDest) Write(ctx context.Context, batch connector.Batch) error {
	if d.failures > 0 {
		d.failures--
		return fmt.Errorf("write failed")
	}
	return d.recordingDest.Write(ctx, batch)
}

type failingDest struct {
	err error
}

func (d *failingDest) Open(context.Context, connector.Spec) error { return nil }
func (d *failingDest) Write(context.Context, connector.Batch) error {
	if d.err == nil {
		return errors.New("write failed")
	}
	return d.err
}
func (d *failingDest) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return nil
}
func (d *failingDest) TypeMappings() map[string]string { return nil }
func (d *failingDest) Close(context.Context) error     { return nil }
func (d *failingDest) Capabilities() connector.Capabilities {
	return connector.Capabilities{SupportsStreaming: true}
}

type dropSource struct {
	*fakeSource
	dropped bool
}

func (s *dropSource) DropSlot(context.Context) error {
	s.dropped = true
	return nil
}

func TestRunnerPrimaryAckQueuesSecondary(t *testing.T) {
	ctx := context.Background()
	log := &eventLog{}
	schema := connector.Schema{Name: "events", Namespace: "public", Version: 1}

	batch1 := connector.Batch{
		Schema: schema,
		Records: []connector.Record{{
			Table:     "events",
			Operation: connector.OpInsert,
			After:     map[string]any{"id": int64(1)},
		}},
		Checkpoint: connector.Checkpoint{LSN: "1", Metadata: map[string]string{"seq": "1"}},
	}
	batch2 := connector.Batch{
		Schema: schema,
		Records: []connector.Record{{
			Table:     "events",
			Operation: connector.OpInsert,
			After:     map[string]any{"id": int64(2)},
		}},
		Checkpoint: connector.Checkpoint{LSN: "2", Metadata: map[string]string{"seq": "2"}},
	}

	source := &fakeSource{batches: []connector.Batch{batch1, batch2}, log: log}
	primaryDest := &recordingDest{log: log, name: "primary"}
	secondaryDest := &flakySecondaryDest{
		recordingDest: recordingDest{log: log, name: "secondary"},
		failures:      1,
	}

	runner := Runner{
		Source:     source,
		SourceSpec: connector.Spec{Options: map[string]string{"mode": "backfill"}},
		Destinations: []DestinationConfig{
			{Spec: connector.Spec{Name: "primary"}, Dest: primaryDest},
			{Spec: connector.Spec{Name: "secondary"}, Dest: secondaryDest},
		},
		FlowID:             "flow-test",
		AckPolicy:          AckPolicyPrimary,
		PrimaryDestination: "primary",
		GiveUpPolicy:       GiveUpPolicyNever,
	}

	if err := runner.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(source.acks) != 2 {
		t.Fatalf("expected 2 source acks, got %d", len(source.acks))
	}
	if len(secondaryDest.writes) != 2 {
		t.Fatalf("expected 2 secondary writes, got %d", len(secondaryDest.writes))
	}

	events := log.snapshot()
	ackIdx := indexEvent(events, "ack", "1", "")
	secondaryIdx := indexEvent(events, "write", "1", "secondary")
	if ackIdx == -1 || secondaryIdx == -1 || ackIdx > secondaryIdx {
		t.Fatalf("expected ack before secondary write for lsn 1 (ack=%d write=%d)", ackIdx, secondaryIdx)
	}
}

func TestRunnerDropsSlotOnFailure(t *testing.T) {
	ctx := context.Background()
	log := &eventLog{}
	schema := connector.Schema{Name: "events", Namespace: "public", Version: 1}

	batch := connector.Batch{
		Schema: schema,
		Records: []connector.Record{{
			Table:     "events",
			Operation: connector.OpInsert,
			After:     map[string]any{"id": int64(1)},
		}},
		Checkpoint: connector.Checkpoint{LSN: "1", Metadata: map[string]string{"seq": "1"}},
	}

	source := &dropSource{fakeSource: &fakeSource{batches: []connector.Batch{batch}, log: log}}
	dest := &failingDest{err: errors.New("write failed")}

	runner := Runner{
		Source:     source,
		SourceSpec: connector.Spec{Options: map[string]string{"mode": "backfill"}},
		Destinations: []DestinationConfig{{
			Spec: connector.Spec{Name: "dest"},
			Dest: dest,
		}},
		FlowID:       "flow-test",
		FailureMode:  FailureModeDropSlot,
		GiveUpPolicy: GiveUpPolicyOnRetryExhaustion,
	}

	if err := runner.Run(ctx); err == nil {
		t.Fatalf("expected run to fail")
	}
	if !source.dropped {
		t.Fatalf("expected replication slot to be dropped on failure")
	}
}

func indexEvent(events []event, kind, seq, dest string) int {
	for i, evt := range events {
		if evt.kind == kind && evt.seq == seq && evt.dest == dest {
			return i
		}
	}
	return -1
}
