package stream

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/josephjohncox/wallaby/pkg/spec"
)

// TraceEvent captures a runner event for offline validation.
type TraceEvent struct {
	Time        time.Time     `json:"time"`
	Kind        string        `json:"kind"`
	Spec        spec.SpecName `json:"spec,omitempty"`
	SpecAction  spec.Action   `json:"spec_action,omitempty"`
	LSN         string        `json:"lsn,omitempty"`
	FlowID      string        `json:"flow_id,omitempty"`
	Destination string        `json:"destination,omitempty"`
	DDL         string        `json:"ddl,omitempty"`
	EventID     int64         `json:"event_id,omitempty"`
	Error       string        `json:"error,omitempty"`
	Detail      string        `json:"detail,omitempty"`
}

// TraceSink receives trace events.
type TraceSink interface {
	Emit(ctx context.Context, event TraceEvent)
}

// JSONTraceSink writes JSONL trace events.
type JSONTraceSink struct {
	mu  sync.Mutex
	enc *json.Encoder
}

// NewJSONTraceSink returns a JSONL trace sink.
func NewJSONTraceSink(w io.Writer) *JSONTraceSink {
	enc := json.NewEncoder(w)
	return &JSONTraceSink{enc: enc}
}

// Emit writes a single trace event.
func (s *JSONTraceSink) Emit(_ context.Context, event TraceEvent) {
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.enc.Encode(event)
}

// MemoryTraceSink stores trace events in memory (useful for tests).
type MemoryTraceSink struct {
	mu     sync.Mutex
	events []TraceEvent
}

// Emit stores a trace event.
func (s *MemoryTraceSink) Emit(_ context.Context, event TraceEvent) {
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

// Events returns a snapshot of captured events.
func (s *MemoryTraceSink) Events() []TraceEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]TraceEvent, len(s.events))
	copy(out, s.events)
	return out
}
