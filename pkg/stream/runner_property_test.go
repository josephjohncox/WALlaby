package stream

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"testing/quick"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

type batchPlan struct {
	Batches []batchSpec
}

type batchSpec struct {
	Kind    uint8 // 0=data, 1=control checkpoint, 2=empty read, 3=ddl
	Records int
}

func (batchPlan) Generate(r *rand.Rand, size int) reflect.Value {
	count := r.Intn(8) + 1
	specs := make([]batchSpec, 0, count)
	for i := 0; i < count; i++ {
		kind := uint8(r.Intn(4))
		records := 0
		if kind == 0 {
			records = r.Intn(3) + 1
		}
		if kind == 3 {
			records = 1
		}
		specs = append(specs, batchSpec{Kind: kind, Records: records})
	}
	return reflect.ValueOf(batchPlan{Batches: specs})
}

type eventLog struct {
	mu     sync.Mutex
	events []event
}

type event struct {
	kind string
	seq  string
	dest string
}

func (l *eventLog) add(kind, seq, dest string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event{kind: kind, seq: seq, dest: dest})
}

func (l *eventLog) snapshot() []event {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]event, len(l.events))
	copy(out, l.events)
	return out
}

type fakeSource struct {
	batches []connector.Batch
	idx     int
	acks    []connector.Checkpoint
	log     *eventLog
}

func (s *fakeSource) Open(context.Context, connector.Spec) error { return nil }

func (s *fakeSource) Read(context.Context) (connector.Batch, error) {
	if s.idx >= len(s.batches) {
		return connector.Batch{}, io.EOF
	}
	batch := s.batches[s.idx]
	s.idx++
	return batch, nil
}

func (s *fakeSource) Ack(_ context.Context, checkpoint connector.Checkpoint) error {
	seq := seqForCheckpoint(checkpoint)
	s.log.add("ack", seq, "")
	s.acks = append(s.acks, checkpoint)
	return nil
}

func (s *fakeSource) Close(context.Context) error { return nil }

func (s *fakeSource) Capabilities() connector.Capabilities {
	return connector.Capabilities{SupportsStreaming: true}
}

type recordingDest struct {
	log    *eventLog
	writes []connector.Batch
	name   string
}

func (d *recordingDest) Open(context.Context, connector.Spec) error { return nil }

func (d *recordingDest) Write(_ context.Context, batch connector.Batch) error {
	seq := seqForCheckpoint(batch.Checkpoint)
	d.log.add("write", seq, d.name)
	d.writes = append(d.writes, batch)
	return nil
}

func (d *recordingDest) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return nil
}

func (d *recordingDest) TypeMappings() map[string]string { return nil }

func (d *recordingDest) Close(context.Context) error { return nil }

func (d *recordingDest) Capabilities() connector.Capabilities {
	return connector.Capabilities{SupportsStreaming: true, SupportsDDL: true}
}

type recordingCheckpointStore struct {
	puts []connector.Checkpoint
}

func (s *recordingCheckpointStore) Get(context.Context, string) (connector.Checkpoint, error) {
	return connector.Checkpoint{}, fmt.Errorf("no checkpoint")
}

func (s *recordingCheckpointStore) Put(_ context.Context, _ string, checkpoint connector.Checkpoint) error {
	s.puts = append(s.puts, checkpoint)
	return nil
}

func (s *recordingCheckpointStore) List(context.Context) ([]connector.FlowCheckpoint, error) {
	return nil, nil
}

func TestRunnerProtocolInvariantsQuick(t *testing.T) {
	config := &quick.Config{MaxCount: 200}
	if err := quick.Check(func(plan batchPlan) bool {
		ctx := context.Background()
		log := &eventLog{}
		schema := connector.Schema{Name: "events", Namespace: "public", Version: 1}

		batches := make([]connector.Batch, 0, len(plan.Batches))
		for i, spec := range plan.Batches {
			cp := connector.Checkpoint{
				LSN:      fmt.Sprintf("%d", i+1),
				Metadata: map[string]string{"seq": strconv.Itoa(i)},
			}
			if spec.Kind == 1 {
				cp.Metadata["control"] = "true"
			}
			var records []connector.Record
			switch spec.Kind {
			case 0:
				records = make([]connector.Record, 0, spec.Records)
				for j := 0; j < spec.Records; j++ {
					records = append(records, connector.Record{
						Table:     "events",
						Operation: connector.OpInsert,
						After: map[string]any{
							"id":   int64(i*10 + j),
							"name": "value",
						},
						Timestamp: time.Now().UTC(),
					})
				}
			case 3:
				records = []connector.Record{{
					Table:     "events",
					Operation: connector.OpDDL,
					DDL:       fmt.Sprintf("ALTER TABLE events ADD COLUMN col_%d text", i),
					Timestamp: time.Now().UTC(),
				}}
			}
			batches = append(batches, connector.Batch{
				Schema:     schema,
				Records:    records,
				Checkpoint: cp,
			})
		}

		source := &fakeSource{batches: batches, log: log}
		dest := &recordingDest{log: log, name: "dest"}
		checkpoints := &recordingCheckpointStore{}
		traceSink := &MemoryTraceSink{}
		ddlApplied := make([]string, 0)

		runner := Runner{
			Source:     source,
			SourceSpec: connector.Spec{Options: map[string]string{"mode": "backfill"}},
			Destinations: []DestinationConfig{{
				Spec: connector.Spec{Name: "dest"},
				Dest: dest,
			}},
			Checkpoints: checkpoints,
			FlowID:      "flow-test",
			Parallelism: 1,
			TraceSink:   traceSink,
			DDLApplied: func(_ context.Context, flowID string, lsn string, ddl string) error {
				if lsn == "" || ddl == "" {
					return fmt.Errorf("invalid ddl applied event")
				}
				_ = flowID
				ddlApplied = append(ddlApplied, lsn)
				return nil
			},
		}

		if err := runner.Run(ctx); err != nil {
			return false
		}

		expectedAck := make([]string, 0)
		expectedCheckpoint := make([]string, 0)
		expectedDDL := 0
		for _, batch := range batches {
			if len(batch.Records) > 0 || isControlCheckpoint(batch.Checkpoint) {
				expectedAck = append(expectedAck, seqForCheckpoint(batch.Checkpoint))
				if shouldPersistCheckpoint(batch.Checkpoint) {
					expectedCheckpoint = append(expectedCheckpoint, seqForCheckpoint(batch.Checkpoint))
				}
			}
			for _, record := range batch.Records {
				if record.Operation == connector.OpDDL || record.DDL != "" {
					expectedDDL++
				}
			}
		}

		if len(source.acks) != len(expectedAck) {
			return false
		}
		for i, cp := range source.acks {
			if seqForCheckpoint(cp) != expectedAck[i] {
				return false
			}
		}

		if len(checkpoints.puts) != len(expectedCheckpoint) {
			return false
		}
		for i, cp := range checkpoints.puts {
			if seqForCheckpoint(cp) != expectedCheckpoint[i] {
				return false
			}
		}

		if len(ddlApplied) != expectedDDL {
			return false
		}

		events := log.snapshot()
		writeIndex := map[string]int{}
		ackIndex := map[string]int{}
		for idx, evt := range events {
			if evt.kind == "write" {
				writeIndex[evt.seq] = idx
			}
			if evt.kind == "ack" {
				ackIndex[evt.seq] = idx
			}
		}

		for _, batch := range batches {
			if len(batch.Records) == 0 {
				continue
			}
			seq := seqForCheckpoint(batch.Checkpoint)
			wIdx, wOk := writeIndex[seq]
			aIdx, aOk := ackIndex[seq]
			if !wOk || !aOk || aIdx <= wIdx {
				return false
			}
		}

		if err := ValidateTrace(traceSink.Events(), TraceValidationOptions{}); err != nil {
			return false
		}

		return true
	}, config); err != nil {
		t.Fatalf("property test failed: %v", err)
	}
}

func seqForCheckpoint(cp connector.Checkpoint) string {
	if cp.Metadata == nil {
		return ""
	}
	return cp.Metadata["seq"]
}

type failurePlan struct {
	Plan      batchPlan
	FailIndex int
}

func (failurePlan) Generate(r *rand.Rand, size int) reflect.Value {
	plan := batchPlan{}.Generate(r, size).Interface().(batchPlan)
	specs := plan.Batches
	candidates := make([]int, 0)
	for i, spec := range specs {
		if spec.Kind == 0 || spec.Kind == 3 {
			candidates = append(candidates, i)
		}
	}
	if len(candidates) == 0 {
		specs[0] = batchSpec{Kind: 0, Records: 1}
		candidates = append(candidates, 0)
	}
	failIndex := candidates[r.Intn(len(candidates))]
	return reflect.ValueOf(failurePlan{Plan: batchPlan{Batches: specs}, FailIndex: failIndex})
}

type flakyDest struct {
	log     *eventLog
	failSeq string
	name    string
}

func (d *flakyDest) Open(context.Context, connector.Spec) error { return nil }

func (d *flakyDest) Write(_ context.Context, batch connector.Batch) error {
	seq := seqForCheckpoint(batch.Checkpoint)
	d.log.add("write", seq, d.name)
	if seq == d.failSeq {
		return fmt.Errorf("boom")
	}
	return nil
}

func (d *flakyDest) ApplyDDL(context.Context, connector.Schema, connector.Record) error { return nil }

func (d *flakyDest) TypeMappings() map[string]string { return nil }

func (d *flakyDest) Close(context.Context) error { return nil }

func (d *flakyDest) Capabilities() connector.Capabilities {
	return connector.Capabilities{SupportsStreaming: true, SupportsDDL: true}
}

func TestRunnerStopsOnWriteFailureQuick(t *testing.T) {
	config := &quick.Config{MaxCount: 200}
	if err := quick.Check(func(plan failurePlan) bool {
		ctx := context.Background()
		log := &eventLog{}
		schema := connector.Schema{Name: "events", Namespace: "public", Version: 1}

		batches := make([]connector.Batch, 0, len(plan.Plan.Batches))
		for i, spec := range plan.Plan.Batches {
			cp := connector.Checkpoint{
				LSN:      fmt.Sprintf("%d", i+1),
				Metadata: map[string]string{"seq": strconv.Itoa(i)},
			}
			if spec.Kind == 1 {
				cp.Metadata["control"] = "true"
			}
			var records []connector.Record
			switch spec.Kind {
			case 0:
				records = make([]connector.Record, 0, spec.Records)
				for j := 0; j < spec.Records; j++ {
					records = append(records, connector.Record{
						Table:     "events",
						Operation: connector.OpInsert,
						After: map[string]any{
							"id":   int64(i*10 + j),
							"name": "value",
						},
						Timestamp: time.Now().UTC(),
					})
				}
			case 3:
				records = []connector.Record{{
					Table:     "events",
					Operation: connector.OpDDL,
					DDL:       fmt.Sprintf("ALTER TABLE events ADD COLUMN col_%d text", i),
					Timestamp: time.Now().UTC(),
				}}
			}
			batches = append(batches, connector.Batch{
				Schema:     schema,
				Records:    records,
				Checkpoint: cp,
			})
		}

		failSeq := seqForCheckpoint(batches[plan.FailIndex].Checkpoint)
		source := &fakeSource{batches: batches, log: log}
		dest := &flakyDest{log: log, failSeq: failSeq, name: "dest"}
		traceSink := &MemoryTraceSink{}

		runner := Runner{
			Source:     source,
			SourceSpec: connector.Spec{Options: map[string]string{"mode": "backfill"}},
			Destinations: []DestinationConfig{{
				Spec: connector.Spec{Name: "dest"},
				Dest: dest,
			}},
			FlowID:    "flow-fail",
			TraceSink: traceSink,
		}

		if err := runner.Run(ctx); err == nil {
			return false
		}

		expectedAck := make([]string, 0)
		for idx, batch := range batches {
			if idx == plan.FailIndex {
				break
			}
			if len(batch.Records) > 0 || isControlCheckpoint(batch.Checkpoint) {
				expectedAck = append(expectedAck, seqForCheckpoint(batch.Checkpoint))
			}
		}

		if len(source.acks) != len(expectedAck) {
			return false
		}
		for i, cp := range source.acks {
			if seqForCheckpoint(cp) != expectedAck[i] {
				return false
			}
		}

		if err := ValidateTrace(traceSink.Events(), TraceValidationOptions{}); err != nil {
			return false
		}

		return true
	}, config); err != nil {
		t.Fatalf("property test failed: %v", err)
	}
}

func TestRunnerMultiDestAckOrderingQuick(t *testing.T) {
	config := &quick.Config{MaxCount: 200}
	if err := quick.Check(func(plan batchPlan) bool {
		ctx := context.Background()
		log := &eventLog{}
		schema := connector.Schema{Name: "events", Namespace: "public", Version: 1}

		destCount := 2 + (len(plan.Batches) % 2)
		destinations := make([]DestinationConfig, 0, destCount)
		for i := 0; i < destCount; i++ {
			destinations = append(destinations, DestinationConfig{
				Spec: connector.Spec{Name: fmt.Sprintf("dest-%d", i)},
				Dest: &recordingDest{log: log, name: fmt.Sprintf("dest-%d", i)},
			})
		}

		batches := make([]connector.Batch, 0, len(plan.Batches))
		for i, spec := range plan.Batches {
			cp := connector.Checkpoint{
				LSN:      fmt.Sprintf("%d", i+1),
				Metadata: map[string]string{"seq": strconv.Itoa(i)},
			}
			if spec.Kind == 1 {
				cp.Metadata["control"] = "true"
			}
			var records []connector.Record
			switch spec.Kind {
			case 0:
				records = make([]connector.Record, 0, spec.Records)
				for j := 0; j < spec.Records; j++ {
					records = append(records, connector.Record{
						Table:     "events",
						Operation: connector.OpInsert,
						After: map[string]any{
							"id":   int64(i*10 + j),
							"name": "value",
						},
						Timestamp: time.Unix(0, 0).UTC(),
					})
				}
			case 3:
				records = []connector.Record{{
					Table:     "events",
					Operation: connector.OpDDL,
					DDL:       fmt.Sprintf("ALTER TABLE events ADD COLUMN col_%d text", i),
					Timestamp: time.Unix(0, 0).UTC(),
				}}
			}
			batches = append(batches, connector.Batch{
				Schema:     schema,
				Records:    records,
				Checkpoint: cp,
			})
		}

		source := &fakeSource{batches: batches, log: log}
		runner := Runner{
			Source:       source,
			SourceSpec:   connector.Spec{Options: map[string]string{"mode": "backfill"}},
			Destinations: destinations,
			FlowID:       "flow-multi",
		}

		if err := runner.Run(ctx); err != nil {
			return false
		}

		events := log.snapshot()
		writeIndex := map[string]map[string]int{}
		ackIndex := map[string]int{}
		for idx, evt := range events {
			switch evt.kind {
			case "write":
				if writeIndex[evt.seq] == nil {
					writeIndex[evt.seq] = map[string]int{}
				}
				writeIndex[evt.seq][evt.dest] = idx
			case "ack":
				ackIndex[evt.seq] = idx
			}
		}

		for _, batch := range batches {
			if len(batch.Records) == 0 {
				continue
			}
			seq := seqForCheckpoint(batch.Checkpoint)
			ackIdx, ok := ackIndex[seq]
			if !ok {
				return false
			}
			writes := writeIndex[seq]
			if len(writes) != destCount {
				return false
			}
			maxWrite := -1
			for _, idx := range writes {
				if idx > maxWrite {
					maxWrite = idx
				}
			}
			if ackIdx <= maxWrite {
				return false
			}
		}

		return true
	}, config); err != nil {
		t.Fatalf("property test failed: %v", err)
	}
}
