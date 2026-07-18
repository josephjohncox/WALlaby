package stream

import (
	"fmt"
	"strings"

	"github.com/josephjohncox/wallaby/pkg/connector"
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

type traceFlowState struct {
	delivered             []string
	persistedDelivery     map[int]struct{}
	lastDelivered         string
	lastPersisted         string
	lastAcked             string
	lastPersistedDelivery int
	lastAckedDelivery     int
	hasDurableCheckpoints bool
	pendingDDL            map[string]struct{}
	approvedDDL           map[string]struct{}
	restoreEvidence       map[string]struct{}
}

func newTraceFlowState(hasDurableCheckpoints bool) *traceFlowState {
	return &traceFlowState{
		persistedDelivery:     make(map[int]struct{}),
		lastPersistedDelivery: -1,
		lastAckedDelivery:     -1,
		hasDurableCheckpoints: hasDurableCheckpoints,
		pendingDDL:            make(map[string]struct{}),
		approvedDDL:           make(map[string]struct{}),
		restoreEvidence:       make(map[string]struct{}),
	}
}

// EvaluateTrace checks trace invariants and reports coverage. Ordering state is
// isolated per flow. PostgreSQL LSNs use their native hexadecimal ordering;
// decimal positions are treated as abstract batch ordinals and cannot be mixed
// with PostgreSQL LSNs in one flow.
func EvaluateTrace(events []TraceEvent, opts TraceValidationOptions, manifest *spec.Manifest) (TraceCoverage, error) {
	coverage := newTraceCoverage()
	var actionSet map[spec.Action]struct{}
	if manifest != nil {
		actionSet = manifest.ActionSet()
	}

	durableFlows := make(map[string]bool)
	for _, event := range events {
		if event.Kind == "checkpoint" {
			durableFlows[traceFlowKey(event.FlowID)] = true
		}
	}
	states := make(map[string]*traceFlowState)
	violations := make([]TraceViolation, 0)
	addViolation := func(invariant, flowID, detail string) {
		if flowID == "" {
			flowID = "<default>"
		}
		violations = append(violations, TraceViolation{
			Invariant: invariant,
			Detail:    fmt.Sprintf("flow=%s %s", flowID, detail),
		})
	}

	for idx, event := range events {
		flowKey := traceFlowKey(event.FlowID)
		state := states[flowKey]
		if state == nil {
			state = newTraceFlowState(durableFlows[flowKey])
			states[flowKey] = state
		}

		if event.SpecAction != spec.ActionNone {
			if actionSet != nil {
				if _, ok := actionSet[event.SpecAction]; !ok {
					addViolation("SpecActionKnown", event.FlowID, fmt.Sprintf("spec action %s not in manifest", event.SpecAction))
				}
			}
			coverage.Actions[event.SpecAction]++
		}

		switch event.Kind {
		case "deliver":
			position, ok := traceEventPosition(event)
			if !ok {
				addViolation(string(spec.InvNoAckWithoutDeliver), event.FlowID, "deliver missing position")
				continue
			}
			if deliveredPosition(state.delivered, position) >= 0 {
				continue
			}
			if event.LSN != "" && state.lastDelivered != "" {
				cmp, err := connector.CompareCheckpointLSN(event.LSN, state.lastDelivered)
				if err != nil {
					addViolation(string(spec.InvAckMonotonic), event.FlowID, err.Error())
					continue
				}
				if cmp < 0 {
					addViolation(string(spec.InvAckMonotonic), event.FlowID, fmt.Sprintf("delivery regressed lsn=%s after=%s", event.LSN, state.lastDelivered))
					continue
				}
			}
			state.delivered = append(state.delivered, position)
			if event.LSN != "" {
				state.lastDelivered = event.LSN
			}

		case "checkpoint":
			position, ok := traceEventPosition(event)
			if !ok {
				addViolation(string(spec.InvCheckpointMonotonic), event.FlowID, "checkpoint missing position")
				continue
			}
			deliveryIndex := deliveredPosition(state.delivered, position)
			if deliveryIndex < 0 {
				addViolation(string(spec.InvCheckpointMonotonic), event.FlowID, fmt.Sprintf("checkpoint without delivery lsn=%s", event.LSN))
				continue
			}
			if !advanceDeliveredPosition(state.lastPersistedDelivery, deliveryIndex) {
				addViolation(string(spec.InvCheckpointMonotonic), event.FlowID, fmt.Sprintf("checkpoint skipped or regressed lsn=%s", event.LSN))
				continue
			}
			if event.LSN != "" && state.lastPersisted != "" {
				cmp, err := connector.CompareCheckpointLSN(event.LSN, state.lastPersisted)
				if err != nil || cmp < 0 {
					if err != nil {
						addViolation(string(spec.InvCheckpointMonotonic), event.FlowID, err.Error())
					} else {
						addViolation(string(spec.InvCheckpointMonotonic), event.FlowID, fmt.Sprintf("checkpoint regressed lsn=%s after=%s", event.LSN, state.lastPersisted))
					}
					continue
				}
			}
			state.persistedDelivery[deliveryIndex] = struct{}{}
			if deliveryIndex > state.lastPersistedDelivery {
				state.lastPersistedDelivery = deliveryIndex
				if event.LSN != "" {
					state.lastPersisted = event.LSN
				}
			}
			coverage.Invariants[spec.InvCheckpointMonotonic]++

		case "ack":
			position, ok := traceEventPosition(event)
			if !ok {
				addViolation(string(spec.InvNoAckWithoutDeliver), event.FlowID, "ack missing position")
				continue
			}
			deliveryIndex := deliveredPosition(state.delivered, position)
			if deliveryIndex < 0 {
				addViolation(string(spec.InvNoAckWithoutDeliver), event.FlowID, fmt.Sprintf("ack without delivery lsn=%s", event.LSN))
				continue
			}
			if state.hasDurableCheckpoints {
				if _, ok := state.persistedDelivery[deliveryIndex]; !ok {
					addViolation(string(spec.InvCheckpointMonotonic), event.FlowID, fmt.Sprintf("source ack before durable checkpoint position=%s", position))
					continue
				}
			}
			if !advanceDeliveredPosition(state.lastAckedDelivery, deliveryIndex) {
				addViolation(string(spec.InvAckMonotonic), event.FlowID, fmt.Sprintf("ack skipped or regressed position=%s", position))
				continue
			}
			if event.LSN != "" && state.lastAcked != "" {
				cmp, err := connector.CompareCheckpointLSN(event.LSN, state.lastAcked)
				if err != nil || cmp < 0 {
					if err != nil {
						addViolation(string(spec.InvAckMonotonic), event.FlowID, err.Error())
					} else {
						addViolation(string(spec.InvAckMonotonic), event.FlowID, fmt.Sprintf("ack regressed lsn=%s after=%s", event.LSN, state.lastAcked))
					}
					continue
				}
			}
			if deliveryIndex > state.lastAckedDelivery {
				state.lastAckedDelivery = deliveryIndex
				if event.LSN != "" {
					state.lastAcked = event.LSN
				}
			}
			coverage.Invariants[spec.InvNoAckWithoutDeliver]++
			coverage.Invariants[spec.InvAckMonotonic]++

		case "restore_checkpoint":
			position, ok := traceEventPosition(event)
			if !ok {
				addViolation(string(spec.InvCheckpointMonotonic), event.FlowID, "restored checkpoint missing position")
				continue
			}
			state.restoreEvidence[position] = struct{}{}
		case "restore_ack":
			position, ok := traceEventPosition(event)
			if !ok {
				addViolation(string(spec.InvCheckpointMonotonic), event.FlowID, "restore acknowledgement missing position")
				continue
			}
			if _, ok := state.restoreEvidence[position]; !ok {
				addViolation(string(spec.InvCheckpointMonotonic), event.FlowID, fmt.Sprintf("restore acknowledgement without durable evidence position=%s", position))
				continue
			}
			if event.LSN != "" && state.lastAcked != "" {
				cmp, err := connector.CompareCheckpointLSN(event.LSN, state.lastAcked)
				if err != nil || cmp < 0 {
					addViolation(string(spec.InvAckMonotonic), event.FlowID, fmt.Sprintf("restore acknowledgement regressed lsn=%s", event.LSN))
					continue
				}
			}
			if event.LSN != "" {
				state.lastAcked = event.LSN
			}
			coverage.Invariants[spec.InvCheckpointMonotonic]++

		case "ddl_pending":
			if event.LSN != "" {
				state.pendingDDL[event.LSN] = struct{}{}
			}
		case "ddl_approved":
			if event.LSN != "" {
				state.approvedDDL[event.LSN] = struct{}{}
				delete(state.pendingDDL, event.LSN)
			}
		case "ddl_applied":
			if opts.RequireDDLApproval {
				if _, ok := state.approvedDDL[event.LSN]; !ok {
					addViolation(string(spec.InvDDLAppliedAfter), event.FlowID, fmt.Sprintf("ddl applied without approval lsn=%s", event.LSN))
				} else {
					coverage.Invariants[spec.InvDDLAppliedAfter]++
				}
			} else if event.LSN != "" {
				if _, ok := state.approvedDDL[event.LSN]; ok {
					coverage.Invariants[spec.InvDDLAppliedAfter]++
				}
			}
		case "read_error", "write_error", "ack_error", "restore_ack_error", "checkpoint_error", "ddl_error", "read", "flow_state", "ddl_gate", "write", "control_checkpoint":
			// Informational; failures are represented by the absence of successor actions.
		default:
			if event.Kind != "" {
				addViolation("KnownKinds", event.FlowID, fmt.Sprintf("unknown event kind=%s index=%d", event.Kind, idx))
			}
		}
	}

	if len(violations) > 0 {
		return coverage, &TraceValidationError{Violations: violations}
	}
	return coverage, nil
}

func traceFlowKey(flowID string) string {
	if flowID == "" {
		return "\x00default"
	}
	return flowID
}

func traceEventPosition(event TraceEvent) (string, bool) {
	if event.LSN != "" {
		if _, err := connector.CompareCheckpointLSN(event.LSN, event.LSN); err != nil {
			return "", false
		}
		return "lsn:" + event.LSN, true
	}
	if event.Position == "" {
		return "", false
	}
	return "abstract:" + event.Position, true
}

func deliveredPosition(delivered []string, position string) int {
	for index, candidate := range delivered {
		if tracePositionsEqual(position, candidate) {
			return index
		}
	}
	return -1
}

func tracePositionsEqual(left, right string) bool {
	if strings.HasPrefix(left, "lsn:") && strings.HasPrefix(right, "lsn:") {
		cmp, err := connector.CompareCheckpointLSN(strings.TrimPrefix(left, "lsn:"), strings.TrimPrefix(right, "lsn:"))
		return err == nil && cmp == 0
	}
	return left == right
}

func advanceDeliveredPosition(last, next int) bool {
	return next == last || next == last+1
}

// ValidateTrace checks trace invariants (mirrors the TLA+ model).
func ValidateTrace(events []TraceEvent, opts TraceValidationOptions) error {
	_, err := EvaluateTrace(events, opts, nil)
	return err
}
