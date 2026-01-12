package stream

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"pgregory.net/rapid"
)

type flakyQueueDest struct {
	recordingDest
	failures map[string]int
}

func (d *flakyQueueDest) Write(ctx context.Context, batch connector.Batch) error {
	seq := seqForCheckpoint(batch.Checkpoint)
	if remaining := d.failures[seq]; remaining > 0 {
		d.failures[seq] = remaining - 1
		return fmt.Errorf("write failed")
	}
	return d.recordingDest.Write(ctx, batch)
}

func TestRunnerPrimaryAckInvariantsRapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ctx := context.Background()
		log := &eventLog{}

		batchCount := rapid.IntRange(1, 6).Draw(t, "batchCount")
		schema := connector.Schema{Name: "events", Namespace: "public", Version: 1}
		batches := make([]connector.Batch, 0, batchCount)
		hasDDL := map[string]bool{}

		for i := 0; i < batchCount; i++ {
			seq := strconv.Itoa(i + 1)
			cp := connector.Checkpoint{
				LSN:      seq,
				Metadata: map[string]string{"seq": seq},
			}
			isDDL := rapid.Bool().Draw(t, fmt.Sprintf("ddl-%d", i))
			var records []connector.Record
			if isDDL {
				records = []connector.Record{{
					Table:     "events",
					Operation: connector.OpDDL,
					DDL:       fmt.Sprintf("ALTER TABLE events ADD COLUMN col_%d text", i),
					Timestamp: time.Unix(0, int64(i)).UTC(),
				}}
				hasDDL[seq] = true
			} else {
				recordCount := rapid.IntRange(1, 3).Draw(t, fmt.Sprintf("records-%d", i))
				records = make([]connector.Record, 0, recordCount)
				for j := 0; j < recordCount; j++ {
					records = append(records, connector.Record{
						Table:     "events",
						Operation: connector.OpInsert,
						After: map[string]any{
							"id":   int64(i*10 + j),
							"name": "value",
						},
						Timestamp: time.Unix(0, int64(i*10+j)).UTC(),
					})
				}
			}

			batches = append(batches, connector.Batch{
				Schema:     schema,
				Records:    records,
				Checkpoint: cp,
			})
		}

		failuresFor := func() map[string]int {
			failures := make(map[string]int, len(batches))
			for _, batch := range batches {
				seq := seqForCheckpoint(batch.Checkpoint)
				failures[seq] = rapid.IntRange(0, 1).Draw(t, "failures")
			}
			return failures
		}

		source := &fakeSource{batches: batches, log: log}
		primaryDest := &recordingDest{log: log, name: "primary"}
		secondaryDestA := &flakyQueueDest{
			recordingDest: recordingDest{log: log, name: "secondary-a"},
			failures:      failuresFor(),
		}
		secondaryDestB := &flakyQueueDest{
			recordingDest: recordingDest{log: log, name: "secondary-b"},
			failures:      failuresFor(),
		}

		runner := Runner{
			Source:     source,
			SourceSpec: connector.Spec{Options: map[string]string{"mode": "backfill"}},
			Destinations: []DestinationConfig{
				{Spec: connector.Spec{Name: "primary"}, Dest: primaryDest},
				{Spec: connector.Spec{Name: "secondary-a"}, Dest: secondaryDestA},
				{Spec: connector.Spec{Name: "secondary-b"}, Dest: secondaryDestB},
			},
			FlowID:             "flow-primary-ack",
			AckPolicy:          AckPolicyPrimary,
			PrimaryDestination: "primary",
			GiveUpPolicy:       GiveUpPolicyNever,
			DDLApplied: func(_ context.Context, flowID string, lsn string, _ string) error {
				log.add("ddl_applied", lsn, "")
				_ = flowID
				return nil
			},
		}

		if err := runner.Run(ctx); err != nil {
			t.Fatalf("run: %v", err)
		}

		events := log.snapshot()
		ackIndex := map[string]int{}
		primaryIndex := map[string]int{}
		secondaryIndex := map[string]map[string]int{}
		ddlAppliedIndex := map[string]int{}
		secondaryNames := map[string]struct{}{"secondary-a": {}, "secondary-b": {}}
		for idx, evt := range events {
			switch evt.kind {
			case "ack":
				ackIndex[evt.seq] = idx
			case "write":
				if evt.dest == "primary" {
					primaryIndex[evt.seq] = idx
				} else {
					if secondaryIndex[evt.dest] == nil {
						secondaryIndex[evt.dest] = map[string]int{}
					}
					secondaryIndex[evt.dest][evt.seq] = idx
				}
			case "ddl_applied":
				ddlAppliedIndex[evt.seq] = idx
			}
		}

		pending := map[string]int{"secondary-a": 0, "secondary-b": 0}
		maxPending := map[string]int{"secondary-a": 0, "secondary-b": 0}
		for _, evt := range events {
			switch evt.kind {
			case "ack":
				for dest := range pending {
					pending[dest]++
					if pending[dest] > maxPending[dest] {
						maxPending[dest] = pending[dest]
					}
				}
			case "write":
				if _, ok := secondaryNames[evt.dest]; ok {
					pending[evt.dest]--
					if pending[evt.dest] < 0 {
						t.Fatalf("secondary queue went negative for %s", evt.dest)
					}
				}
			}
		}
		for dest, count := range pending {
			if count != 0 {
				t.Fatalf("secondary queue not drained for %s", dest)
			}
		}
		for dest, max := range maxPending {
			if max > len(batches) {
				t.Fatalf("secondary queue unbounded for %s: %d", dest, max)
			}
		}

		for _, batch := range batches {
			seq := seqForCheckpoint(batch.Checkpoint)
			ackIdx, ok := ackIndex[seq]
			if !ok {
				t.Fatalf("missing ack for %s", seq)
			}
			primaryIdx, ok := primaryIndex[seq]
			if !ok {
				t.Fatalf("missing primary write for %s", seq)
			}
			if ackIdx <= primaryIdx {
				t.Fatalf("ack should follow primary write for %s", seq)
			}
			for dest := range secondaryNames {
				secIdx, ok := secondaryIndex[dest][seq]
				if !ok {
					t.Fatalf("missing secondary write for %s on %s", seq, dest)
				}
				if ackIdx >= secIdx {
					t.Fatalf("ack should precede secondary write for %s on %s", seq, dest)
				}
			}
			if hasDDL[seq] {
				ddlIdx, ok := ddlAppliedIndex[seq]
				if !ok {
					t.Fatalf("missing ddl applied for %s", seq)
				}
				for dest := range secondaryNames {
					secIdx := secondaryIndex[dest][seq]
					if ddlIdx <= secIdx {
						t.Fatalf("ddl applied before secondary write for %s on %s", seq, dest)
					}
				}
			}
		}
	})
}
