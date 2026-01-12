package stream

import (
	"fmt"
	"strings"

	"github.com/josephjohncox/wallaby/pkg/spec"
)

// TraceValidationOptions controls optional checks.
type TraceValidationOptions struct {
	RequireDDLApproval bool
}

// TraceCoverage reports exercised actions/invariants.
type TraceCoverage struct {
	Actions    map[spec.Action]int
	Invariants map[spec.Invariant]int
}

func newTraceCoverage() TraceCoverage {
	return TraceCoverage{
		Actions:    make(map[spec.Action]int),
		Invariants: make(map[spec.Invariant]int),
	}
}

// TraceViolation describes a failed invariant.
type TraceViolation struct {
	Invariant string
	Detail    string
}

// TraceValidationError groups invariant violations.
type TraceValidationError struct {
	Violations []TraceViolation
}

func (e *TraceValidationError) Error() string {
	if len(e.Violations) == 0 {
		return "trace validation failed"
	}
	parts := make([]string, 0, len(e.Violations))
	for _, v := range e.Violations {
		parts = append(parts, fmt.Sprintf("%s: %s", v.Invariant, v.Detail))
	}
	return "trace validation failed: " + strings.Join(parts, "; ")
}

// EvaluateTrace checks trace invariants and reports coverage.
func EvaluateTrace(events []TraceEvent, opts TraceValidationOptions, manifest *spec.Manifest) (TraceCoverage, error) {
	coverage := newTraceCoverage()
	var actionSet map[spec.Action]struct{}
	if manifest != nil {
		actionSet = manifest.ActionSet()
	}

	deliveredOrder := make([]string, 0)
	deliveredIndex := make(map[string]int)
	ackedOrder := make([]string, 0)
	ackIndex := make(map[string]int)
	lastCheckpointAck := -1

	pendingDDL := make(map[string]struct{})
	approvedDDL := make(map[string]struct{})

	violations := make([]TraceViolation, 0)

	addViolation := func(invariant, detail string) {
		violations = append(violations, TraceViolation{Invariant: invariant, Detail: detail})
	}

	for idx, evt := range events {
		if evt.SpecAction != spec.ActionNone {
			if actionSet != nil {
				if _, ok := actionSet[evt.SpecAction]; !ok {
					addViolation("SpecActionKnown", fmt.Sprintf("spec action %s not in manifest", evt.SpecAction))
				}
			}
			coverage.Actions[evt.SpecAction]++
		}
		switch evt.Kind {
		case "deliver":
			if evt.LSN == "" {
				addViolation(string(spec.InvNoAckWithoutDeliver), "deliver missing lsn")
				continue
			}
			if _, ok := deliveredIndex[evt.LSN]; !ok {
				deliveredIndex[evt.LSN] = len(deliveredOrder)
				deliveredOrder = append(deliveredOrder, evt.LSN)
			}
		case "ack":
			if evt.LSN == "" {
				addViolation(string(spec.InvNoAckWithoutDeliver), "ack missing lsn")
				continue
			}
			if _, ok := deliveredIndex[evt.LSN]; !ok {
				addViolation(string(spec.InvNoAckWithoutDeliver), fmt.Sprintf("ack without deliver lsn=%s", evt.LSN))
				continue
			}
			expectedIdx := len(ackedOrder)
			if expectedIdx < len(deliveredOrder) && deliveredOrder[expectedIdx] != evt.LSN {
				addViolation(string(spec.InvAckMonotonic), fmt.Sprintf("ack out of order lsn=%s expected=%s", evt.LSN, deliveredOrder[expectedIdx]))
			}
			ackIndex[evt.LSN] = len(ackedOrder)
			ackedOrder = append(ackedOrder, evt.LSN)
			coverage.Invariants[spec.InvNoAckWithoutDeliver]++
			coverage.Invariants[spec.InvAckMonotonic]++
		case "checkpoint":
			if evt.LSN == "" {
				addViolation(string(spec.InvCheckpointMonotonic), "checkpoint missing lsn")
				continue
			}
			ackIdx, ok := ackIndex[evt.LSN]
			if !ok {
				addViolation(string(spec.InvCheckpointMonotonic), fmt.Sprintf("checkpoint without ack lsn=%s", evt.LSN))
				continue
			}
			if ackIdx < lastCheckpointAck {
				addViolation(string(spec.InvCheckpointMonotonic), fmt.Sprintf("checkpoint regressed lsn=%s", evt.LSN))
			}
			lastCheckpointAck = ackIdx
			coverage.Invariants[spec.InvCheckpointMonotonic]++
		case "ddl_pending":
			if evt.LSN != "" {
				pendingDDL[evt.LSN] = struct{}{}
			}
		case "ddl_approved":
			if evt.LSN != "" {
				approvedDDL[evt.LSN] = struct{}{}
				delete(pendingDDL, evt.LSN)
			}
		case "ddl_applied":
			if opts.RequireDDLApproval {
				if _, ok := approvedDDL[evt.LSN]; !ok {
					addViolation(string(spec.InvDDLAppliedAfter), fmt.Sprintf("ddl applied without approval lsn=%s", evt.LSN))
				} else {
					coverage.Invariants[spec.InvDDLAppliedAfter]++
				}
			} else if evt.LSN != "" {
				if _, ok := approvedDDL[evt.LSN]; ok {
					coverage.Invariants[spec.InvDDLAppliedAfter]++
				}
			}
		case "read_error", "write_error", "ack_error", "ddl_error", "read", "flow_state", "ddl_gate", "write", "control_checkpoint":
			// Informational for now.
		default:
			if evt.Kind != "" {
				addViolation("KnownKinds", fmt.Sprintf("unknown event kind=%s index=%d", evt.Kind, idx))
			}
		}
	}

	if len(violations) > 0 {
		return coverage, &TraceValidationError{Violations: violations}
	}
	return coverage, nil
}

// ValidateTrace checks trace invariants (mirrors the TLA+ model).
func ValidateTrace(events []TraceEvent, opts TraceValidationOptions) error {
	_, err := EvaluateTrace(events, opts, nil)
	return err
}
