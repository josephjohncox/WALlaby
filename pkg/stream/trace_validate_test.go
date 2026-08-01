package stream

import (
	"errors"
	"fmt"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/spec"
	"pgregory.net/rapid"
)

func TestValidateTraceAckAndCheckpointOrdering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		events        []TraceEvent
		wantInvariant spec.Invariant
	}{
		{
			name: "durable checkpoint precedes source acknowledgement",
			events: []TraceEvent{
				{Kind: "deliver", LSN: "1"},
				{Kind: "checkpoint", LSN: "1"},
				{Kind: "ack", LSN: "1"},
				{Kind: "deliver", LSN: "2"},
				{Kind: "checkpoint", LSN: "2"},
				{Kind: "ack", LSN: "1"},
				{Kind: "ack", LSN: "2"},
			},
		},
		{
			name: "source acknowledgement before durable checkpoint",
			events: []TraceEvent{
				{Kind: "deliver", LSN: "1"},
				{Kind: "ack", LSN: "1"},
				{Kind: "checkpoint", LSN: "1"},
			},
			wantInvariant: spec.InvCheckpointMonotonic,
		},
		{
			name: "acknowledgement regression after duplicate",
			events: []TraceEvent{
				{Kind: "deliver", LSN: "1"},
				{Kind: "deliver", LSN: "2"},
				{Kind: "ack", LSN: "1"},
				{Kind: "ack", LSN: "1"},
				{Kind: "ack", LSN: "2"},
				{Kind: "ack", LSN: "1"},
			},
			wantInvariant: spec.InvAckMonotonic,
		},
		{
			name: "acknowledgement skips delivered predecessor",
			events: []TraceEvent{
				{Kind: "deliver", LSN: "1"},
				{Kind: "deliver", LSN: "2"},
				{Kind: "deliver", LSN: "3"},
				{Kind: "ack", LSN: "1"},
				{Kind: "ack", LSN: "3"},
			},
			wantInvariant: spec.InvAckMonotonic,
		},
		{
			name: "checkpoint regression",
			events: []TraceEvent{
				{Kind: "deliver", LSN: "1"},
				{Kind: "checkpoint", LSN: "1"},
				{Kind: "ack", LSN: "1"},
				{Kind: "deliver", LSN: "2"},
				{Kind: "checkpoint", LSN: "2"},
				{Kind: "ack", LSN: "2"},
				{Kind: "checkpoint", LSN: "1"},
			},
			wantInvariant: spec.InvCheckpointMonotonic,
		},
		{
			name: "PostgreSQL LSN regression",
			events: []TraceEvent{
				{Kind: "deliver", LSN: "1/0"},
				{Kind: "deliver", LSN: "0/FFFFFFFF"},
			},
			wantInvariant: spec.InvAckMonotonic,
		},
		{
			name: "mixed PostgreSQL and ordinal positions",
			events: []TraceEvent{
				{Kind: "deliver", LSN: "1"},
				{Kind: "deliver", LSN: "0/2"},
			},
			wantInvariant: spec.InvAckMonotonic,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateTrace(tt.events, TraceValidationOptions{})
			if tt.wantInvariant == "" {
				if err != nil {
					t.Fatalf("ValidateTrace() error = %v", err)
				}
				return
			}
			assertTraceViolation(t, err, tt.wantInvariant)
		})
	}
}

func TestValidateTracePartitionsStateByFlow(t *testing.T) {
	t.Parallel()

	events := []TraceEvent{
		{Kind: "deliver", FlowID: "alpha", LSN: "1"},
		{Kind: "checkpoint", FlowID: "alpha", LSN: "1"},
		{Kind: "deliver", FlowID: "beta", LSN: "100"},
		{Kind: "checkpoint", FlowID: "beta", LSN: "100"},
		{Kind: "ack", FlowID: "beta", LSN: "100"},
		{Kind: "ack", FlowID: "alpha", LSN: "1"},
	}
	if err := ValidateTrace(events, TraceValidationOptions{}); err != nil {
		t.Fatalf("ValidateTrace() error = %v", err)
	}
}

func TestValidateTraceAcceptsDuplicateRestoreAck(t *testing.T) {
	t.Parallel()

	events := []TraceEvent{
		{Kind: "restore_checkpoint", FlowID: "flow", LSN: "0/16B6C50"},
		{Kind: "restore_ack", FlowID: "flow", LSN: "0/16B6C50", SpecAction: spec.ActionRestoreAck},
		{Kind: "restore_ack", FlowID: "flow", LSN: "0/16B6C50", SpecAction: spec.ActionRestoreAck},
	}
	if err := ValidateTrace(events, TraceValidationOptions{}); err != nil {
		t.Fatalf("ValidateTrace() error = %v", err)
	}
}

func TestValidateTraceAcceptsAbstractBackfillPosition(t *testing.T) {
	t.Parallel()

	events := []TraceEvent{
		{Kind: "read", Position: "batch:1"},
		{Kind: "deliver", Position: "batch:1"},
		{Kind: "ack", Position: "batch:1"},
		{Kind: "read", Position: "batch:2"},
		{Kind: "deliver", Position: "batch:2"},
		{Kind: "ack", Position: "batch:2"},
	}
	if err := ValidateTrace(events, TraceValidationOptions{}); err != nil {
		t.Fatalf("ValidateTrace() error = %v", err)
	}
}

func TestValidateTraceRejectsRestoreAckWithoutDurableEvidence(t *testing.T) {
	t.Parallel()

	err := ValidateTrace([]TraceEvent{{
		Kind:       "restore_ack",
		FlowID:     "flow",
		LSN:        "0/16B6C50",
		SpecAction: spec.ActionRestoreAck,
	}}, TraceValidationOptions{})
	assertTraceViolation(t, err, spec.InvCheckpointMonotonic)
}

func TestValidateTraceRejectsAckRegressionRapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		count := rapid.IntRange(2, 20).Draw(t, "count")
		events := make([]TraceEvent, 0, count*2+1)
		for i := 1; i <= count; i++ {
			events = append(events, TraceEvent{Kind: "deliver", LSN: fmt.Sprint(i)})
		}
		for i := 1; i <= count; i++ {
			events = append(events, TraceEvent{Kind: "ack", LSN: fmt.Sprint(i)})
		}
		regressed := rapid.IntRange(1, count-1).Draw(t, "regressed")
		events = append(events, TraceEvent{Kind: "ack", LSN: fmt.Sprint(regressed)})

		assertTraceViolation(t, ValidateTrace(events, TraceValidationOptions{}), spec.InvAckMonotonic)
	})
}

type traceTestingT interface {
	Helper()
	Fatalf(format string, args ...any)
}

func assertTraceViolation(t traceTestingT, err error, invariant spec.Invariant) {
	t.Helper()
	if err == nil {
		t.Fatalf("ValidateTrace() error = nil, want %s violation", invariant)
	}
	var validationErr *TraceValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("ValidateTrace() error type = %T, want *TraceValidationError", err)
	}
	for _, violation := range validationErr.Violations {
		if violation.Invariant == string(invariant) {
			return
		}
	}
	t.Fatalf("ValidateTrace() violations = %v, want %s", validationErr.Violations, invariant)
}
