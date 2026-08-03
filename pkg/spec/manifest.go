package spec

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type SpecName string

type Action string
type Invariant string

const (
	SpecCDCFlow             SpecName = "CDCFlow"
	SpecFlowState           SpecName = "FlowStateMachine"
	SpecCDCFlowFanout       SpecName = "CDCFlowFanout"
	SpecDDLExecution        SpecName = "DDLExecution"
	SpecLifecycleGeneration SpecName = "LifecycleGeneration"
	SpecSnapshotTransition  SpecName = "SnapshotTransition"
	SpecUnknown             SpecName = ""
)

const (
	ActionNone                   Action = ""
	ActionStart                  Action = "Start"
	ActionPause                  Action = "Pause"
	ActionResume                 Action = "Resume"
	ActionStop                   Action = "Stop"
	ActionStopBegin              Action = "StopBegin"
	ActionStopComplete           Action = "StopComplete"
	ActionFail                   Action = "Fail"
	ActionRunOnce                Action = "RunOnce"
	ActionReadBatch              Action = "ReadBatch"
	ActionReadDDL                Action = "ReadDDL"
	ActionReadFail               Action = "ReadFail"
	ActionReadGiveUp             Action = "ReadGiveUp"
	ActionDeliver                Action = "Deliver"
	ActionWriteFail              Action = "WriteFail"
	ActionWriteGiveUp            Action = "WriteGiveUp"
	ActionCheckpointFail         Action = "CheckpointFail"
	ActionPersistCheckpoint      Action = "PersistCheckpoint"
	ActionAck                    Action = "Ack"
	ActionRestoreAck             Action = "RestoreAck"
	ActionCrash                  Action = "Crash"
	ActionRestart                Action = "Restart"
	ActionAckDest                Action = "AckDest"
	ActionAckSource              Action = "AckSource"
	ActionApproveDDL             Action = "ApproveDDL"
	ActionApplyDDL               Action = "ApplyDDL"
	ActionResumeAfter            Action = "ResumeAfterDDL"
	ActionIdle                   Action = "Idle"
	ActionPrepare                Action = "Prepare"
	ActionApply                  Action = "Apply"
	ActionRecordReceipt          Action = "RecordReceipt"
	ActionAcquireExecutionLock   Action = "AcquireExecutionLock"
	ActionReleaseExecutionLock   Action = "ReleaseExecutionLock"
	ActionReconcileApplied       Action = "ReconcileApplied"
	ActionReconcileNotApplied    Action = "ReconcileNotApplied"
	ActionReconcileIndeterminate Action = "ReconcileIndeterminate"
	ActionPauseIntent            Action = "PauseIntent"
	ActionStopIntent             Action = "StopIntent"
	ActionExecutionFinished      Action = "ExecutionFinished"
	ActionPauseComplete          Action = "PauseComplete"
	ActionRestartExecution       Action = "RestartExecution"
	ActionReadSnapshot           Action = "ReadSnapshot"
	ActionPersistPartition       Action = "PersistPartition"
	ActionCompleteSnapshot       Action = "CompleteSnapshot"
	ActionStartStreaming         Action = "StartStreaming"
	ActionReadStream             Action = "ReadStream"
)

const (
	InvTypeInvariant                 Invariant = "TypeInvariant"
	InvNoAckWithoutDeliver           Invariant = "NoAckWithoutDeliver"
	InvAckMonotonic                  Invariant = "AckMonotonic"
	InvCheckpointMonotonic           Invariant = "CheckpointMonotonic"
	InvReadAheadBounded              Invariant = "ReadAheadBounded"
	InvRetryBounds                   Invariant = "RetryBounds"
	InvDDLAppliedAfter               Invariant = "DDLAppliedAfterApproval"
	InvDDLGatedPausesFlow            Invariant = "DDLGatedPausesFlow"
	InvFlowTransitionsValid          Invariant = "FlowTransitionsValid"
	InvAckedImpliesDelivered         Invariant = "AckedImpliesDelivered"
	InvSourceAckRequires             Invariant = "SourceAckRequiresPolicy"
	InvSourceFlushRequiresCheckpoint Invariant = "SourceFlushRequiresCheckpoint"
	InvExternalCommitRequiresAttempt Invariant = "ExternalCommitRequiresAttempt"
	InvReceiptRequiresExternalCommit Invariant = "ReceiptRequiresExternalCommit"
	InvExternalCommitExactlyOnce     Invariant = "ExternalCommitExactlyOnce"
	InvCommitCountMatchesState       Invariant = "CommitCountMatchesState"
	InvLeaseMatchesExecution         Invariant = "LeaseMatchesExecution"
	InvQuiescentTerminalState        Invariant = "QuiescentTerminalState"
	InvRegistrationCurrentGeneration Invariant = "RegistrationUsesCurrentGeneration"
	InvPendingPauseIsNotPaused       Invariant = "PendingPauseIsNotPaused"
	InvPendingStopIsStopping         Invariant = "PendingStopIsStopping"
	InvRowsStayAssignedPartition     Invariant = "RowsStayInAssignedPartition"
	InvDurableRowsWereScanned        Invariant = "DurableRowsWereScanned"
	InvTransitionCompleteSnapshot    Invariant = "TransitionRequiresCompleteSnapshot"
	InvStreamingSnapshotBoundary     Invariant = "StreamingStartsAtSnapshotBoundary"
)

var DefaultManifestFiles = map[SpecName]string{
	SpecCDCFlow:             "coverage.json",
	SpecFlowState:           "coverage.flow_state.json",
	SpecCDCFlowFanout:       "coverage.fanout.json",
	SpecDDLExecution:        "coverage.ddl_execution.json",
	SpecLifecycleGeneration: "coverage.lifecycle_generation.json",
	SpecSnapshotTransition:  "coverage.snapshot_transition.json",
}

var CDCFlowActions = []Action{
	ActionStart,
	ActionPause,
	ActionResume,
	ActionStopBegin,
	ActionStopComplete,
	ActionFail,
	ActionReadBatch,
	ActionReadDDL,
	ActionReadFail,
	ActionReadGiveUp,
	ActionDeliver,
	ActionWriteFail,
	ActionWriteGiveUp,
	ActionCheckpointFail,
	ActionPersistCheckpoint,
	ActionAck,
	ActionRestoreAck,
	ActionCrash,
	ActionRestart,
	ActionApproveDDL,
	ActionApplyDDL,
	ActionResumeAfter,
	ActionIdle,
}

var CDCFlowInvariants = []Invariant{
	InvTypeInvariant,
	InvNoAckWithoutDeliver,
	InvAckMonotonic,
	InvReadAheadBounded,
	InvRetryBounds,
	InvCheckpointMonotonic,
	InvDDLAppliedAfter,
	InvDDLGatedPausesFlow,
	InvFlowTransitionsValid,
}

var FlowStateActions = []Action{
	ActionStart,
	ActionPause,
	ActionResume,
	ActionStopBegin,
	ActionStopComplete,
	ActionFail,
	ActionRunOnce,
}

var FlowStateInvariants = []Invariant{
	InvTypeInvariant,
}

var FanoutActions = []Action{
	ActionStart,
	ActionPause,
	ActionResume,
	ActionStop,
	ActionFail,
	ActionReadBatch,
	ActionDeliver,
	ActionAckDest,
	ActionPersistCheckpoint,
	ActionCheckpointFail,
	ActionAckSource,
	ActionCrash,
	ActionRestart,
	ActionIdle,
}

var FanoutInvariants = []Invariant{
	InvTypeInvariant,
	InvAckedImpliesDelivered,
	InvSourceAckRequires,
	InvCheckpointMonotonic,
}

var DDLExecutionActions = []Action{
	ActionAcquireExecutionLock,
	ActionReleaseExecutionLock,
	ActionPrepare,
	ActionApply,
	ActionRecordReceipt,
	ActionCrash,
	ActionRestart,
	ActionReconcileApplied,
	ActionReconcileNotApplied,
	ActionReconcileIndeterminate,
}

var DDLExecutionInvariants = []Invariant{
	InvTypeInvariant,
	InvExternalCommitRequiresAttempt,
	InvReceiptRequiresExternalCommit,
	InvExternalCommitExactlyOnce,
	InvCommitCountMatchesState,
}

var LifecycleGenerationActions = []Action{
	ActionStart,
	ActionPauseIntent,
	ActionStopIntent,
	ActionExecutionFinished,
	ActionPauseComplete,
	ActionStopComplete,
	ActionRestartExecution,
	ActionFail,
	ActionRunOnce,
}

var LifecycleGenerationInvariants = []Invariant{
	InvTypeInvariant,
	InvLeaseMatchesExecution,
	InvQuiescentTerminalState,
	InvRegistrationCurrentGeneration,
	InvPendingPauseIsNotPaused,
	InvPendingStopIsStopping,
}

var SnapshotTransitionActions = []Action{
	ActionReadSnapshot,
	ActionPersistPartition,
	ActionCrash,
	ActionRestart,
	ActionCompleteSnapshot,
	ActionStartStreaming,
	ActionReadStream,
	ActionIdle,
}

var SnapshotTransitionInvariants = []Invariant{
	InvTypeInvariant,
	InvRowsStayAssignedPartition,
	InvDurableRowsWereScanned,
	InvTransitionCompleteSnapshot,
	InvStreamingSnapshotBoundary,
}

// CDCFlowTraceUnreachableActions documents actions not emitted by the trace suite.
var CDCFlowTraceUnreachableActions = []Action{
	ActionStart,
	ActionPause,
	ActionResume,
	ActionStopBegin,
	ActionStopComplete,
	ActionFail,
	ActionReadFail,
	ActionReadGiveUp,
	ActionWriteFail,
	ActionWriteGiveUp,
	ActionCheckpointFail,
	ActionRestoreAck,
	ActionCrash,
	ActionRestart,
	ActionApproveDDL,
	ActionResumeAfter,
	ActionIdle,
}

// CDCFlowTraceUnreachableInvariants documents invariants not covered by trace checks.
var CDCFlowTraceUnreachableInvariants = []Invariant{
	InvTypeInvariant,
	InvReadAheadBounded,
	InvRetryBounds,
	InvDDLGatedPausesFlow,
	InvFlowTransitionsValid,
}

// FlowStateTraceUnreachableActions documents actions not emitted by trace suite.
var FlowStateTraceUnreachableActions = append([]Action(nil), FlowStateActions...)

// FlowStateTraceUnreachableInvariants documents invariants not covered by trace checks.
var FlowStateTraceUnreachableInvariants = append([]Invariant(nil), FlowStateInvariants...)

// FanoutTraceUnreachableActions documents actions not emitted by trace suite.
var FanoutTraceUnreachableActions = append([]Action(nil), FanoutActions...)

// FanoutTraceUnreachableInvariants documents invariants not covered by trace checks.
var FanoutTraceUnreachableInvariants = append([]Invariant(nil), FanoutInvariants...)

var DDLExecutionTraceUnreachableActions = append([]Action(nil), DDLExecutionActions...)
var DDLExecutionTraceUnreachableInvariants = append([]Invariant(nil), DDLExecutionInvariants...)
var LifecycleGenerationTraceUnreachableActions = append([]Action(nil), LifecycleGenerationActions...)
var LifecycleGenerationTraceUnreachableInvariants = append([]Invariant(nil), LifecycleGenerationInvariants...)
var SnapshotTransitionTraceUnreachableActions = append([]Action(nil), SnapshotTransitionActions...)
var SnapshotTransitionTraceUnreachableInvariants = append([]Invariant(nil), SnapshotTransitionInvariants...)

// Manifest defines the spec coverage contract shared by TLC and Go tests.
type Manifest struct {
	Spec                  SpecName          `json:"spec"`
	Actions               []Action          `json:"actions"`
	Invariants            []Invariant       `json:"invariants"`
	MinActions            map[Action]int    `json:"min_actions,omitempty"`
	MinInvariants         map[Invariant]int `json:"min_invariants,omitempty"`
	UnreachableActions    []Action          `json:"unreachable_actions,omitempty"`
	UnreachableInvariants []Invariant       `json:"unreachable_invariants,omitempty"`
}

// TraceSuiteManifest returns the CDC flow manifest used by trace suite tests.
func TraceSuiteManifest() Manifest {
	manifest, _ := ManifestForSpec(SpecCDCFlow)
	return manifest
}

// AllManifests returns manifests for all known specs.
func AllManifests() []Manifest {
	manifests := make([]Manifest, 0, 6)
	for _, specName := range []SpecName{
		SpecCDCFlow,
		SpecFlowState,
		SpecCDCFlowFanout,
		SpecDDLExecution,
		SpecLifecycleGeneration,
		SpecSnapshotTransition,
	} {
		manifest, ok := ManifestForSpec(specName)
		if !ok {
			continue
		}
		manifests = append(manifests, manifest)
	}
	return manifests
}

// ManifestForSpec builds a manifest for the named spec.
func ManifestForSpec(spec SpecName) (Manifest, bool) {
	switch spec {
	case SpecCDCFlow:
		return newManifest(spec, CDCFlowActions, CDCFlowInvariants, CDCFlowTraceUnreachableActions, CDCFlowTraceUnreachableInvariants), true
	case SpecFlowState:
		return newManifest(spec, FlowStateActions, FlowStateInvariants, FlowStateTraceUnreachableActions, FlowStateTraceUnreachableInvariants), true
	case SpecCDCFlowFanout:
		return newManifest(spec, FanoutActions, FanoutInvariants, FanoutTraceUnreachableActions, FanoutTraceUnreachableInvariants), true
	case SpecDDLExecution:
		return newManifest(spec, DDLExecutionActions, DDLExecutionInvariants, DDLExecutionTraceUnreachableActions, DDLExecutionTraceUnreachableInvariants), true
	case SpecLifecycleGeneration:
		return newManifest(spec, LifecycleGenerationActions, LifecycleGenerationInvariants, LifecycleGenerationTraceUnreachableActions, LifecycleGenerationTraceUnreachableInvariants), true
	case SpecSnapshotTransition:
		return newManifest(spec, SnapshotTransitionActions, SnapshotTransitionInvariants, SnapshotTransitionTraceUnreachableActions, SnapshotTransitionTraceUnreachableInvariants), true
	default:
		return Manifest{}, false
	}
}

func newManifest(spec SpecName, actions []Action, invariants []Invariant, unreachableActions []Action, unreachableInvariants []Invariant) Manifest {
	unreachableActionSet := toActionSet(unreachableActions)
	unreachableInvariantSet := toInvariantSet(unreachableInvariants)
	minActions := make(map[Action]int)
	for _, action := range actions {
		if _, skip := unreachableActionSet[action]; skip {
			continue
		}
		minActions[action] = 1
	}
	minInvariants := make(map[Invariant]int)
	for _, inv := range invariants {
		if _, skip := unreachableInvariantSet[inv]; skip {
			continue
		}
		minInvariants[inv] = 1
	}
	return Manifest{
		Spec:                  spec,
		Actions:               actions,
		Invariants:            invariants,
		MinActions:            minActions,
		MinInvariants:         minInvariants,
		UnreachableActions:    unreachableActions,
		UnreachableInvariants: unreachableInvariants,
	}
}

// LoadManifest loads a coverage manifest from disk.
func LoadManifest(path string) (Manifest, error) {
	// #nosec G304 -- manifest path is controlled by the caller.
	data, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, fmt.Errorf("read coverage manifest: %w", err)
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("parse coverage manifest: %w", err)
	}
	if manifest.Spec == SpecUnknown {
		if specName, ok := inferSpecFromFilename(filepath.Base(path)); ok {
			manifest.Spec = specName
		}
	}
	if manifest.Spec != SpecUnknown {
		if _, ok := ParseSpecName(string(manifest.Spec)); !ok {
			return Manifest{}, fmt.Errorf("unknown spec %q in manifest", manifest.Spec)
		}
	}
	return manifest, nil
}

// LoadManifests loads one or more manifests from a file or directory.
func LoadManifests(path string) (map[SpecName]Manifest, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		manifest, err := LoadManifest(path)
		if err != nil {
			return nil, err
		}
		return map[SpecName]Manifest{manifest.Spec: manifest}, nil
	}

	entries, err := filepath.Glob(filepath.Join(path, "coverage*.json"))
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("no manifests found in %s", path)
	}

	manifests := make(map[SpecName]Manifest)
	for _, entry := range entries {
		manifest, err := LoadManifest(entry)
		if err != nil {
			return nil, err
		}
		specName := manifest.Spec
		if specName == SpecUnknown {
			return nil, fmt.Errorf("manifest %s missing spec", entry)
		}
		if _, exists := manifests[specName]; exists {
			return nil, fmt.Errorf("duplicate manifest for spec %s", specName)
		}
		manifests[specName] = manifest
	}
	return manifests, nil
}

func ManifestPath(dir string, spec SpecName) string {
	name := DefaultManifestFiles[spec]
	if name == "" {
		name = "coverage." + strings.ToLower(string(spec)) + ".json"
	}
	return filepath.Join(dir, name)
}

func ParseSpecName(value string) (SpecName, bool) {
	switch canonicalSpecName(value) {
	case "cdcflow":
		return SpecCDCFlow, true
	case "flowstate", "flowstatemachine":
		return SpecFlowState, true
	case "fanout", "cdcflowfanout":
		return SpecCDCFlowFanout, true
	case "ddl", "ddlexecution":
		return SpecDDLExecution, true
	case "lifecycle", "lifecyclegeneration":
		return SpecLifecycleGeneration, true
	case "snapshot", "snapshottransition":
		return SpecSnapshotTransition, true
	default:
		return SpecUnknown, false
	}
}

func (m Manifest) ActionSet() map[Action]struct{} {
	out := make(map[Action]struct{}, len(m.Actions))
	for _, action := range m.Actions {
		out[action] = struct{}{}
	}
	return out
}

func (m Manifest) InvariantSet() map[Invariant]struct{} {
	out := make(map[Invariant]struct{}, len(m.Invariants))
	for _, inv := range m.Invariants {
		out[inv] = struct{}{}
	}
	return out
}

func (m Manifest) UnreachableActionSet() map[Action]struct{} {
	return toActionSet(m.UnreachableActions)
}

func (m Manifest) UnreachableInvariantSet() map[Invariant]struct{} {
	return toInvariantSet(m.UnreachableInvariants)
}

func (m Manifest) ActionMin(action Action) int {
	if m.MinActions != nil {
		if min, ok := m.MinActions[action]; ok {
			return min
		}
	}
	return 1
}

func (m Manifest) InvariantMin(inv Invariant) int {
	if m.MinInvariants != nil {
		if min, ok := m.MinInvariants[inv]; ok {
			return min
		}
	}
	return 1
}

func inferSpecFromFilename(name string) (SpecName, bool) {
	if specName, ok := ParseSpecName(strings.TrimSuffix(strings.TrimPrefix(name, "coverage."), ".json")); ok && specName != SpecUnknown {
		return specName, true
	}
	if name == "coverage.json" {
		return SpecCDCFlow, true
	}
	return SpecUnknown, false
}

func canonicalSpecName(value string) string {
	trimmed := strings.ToLower(strings.TrimSpace(value))
	trimmed = strings.TrimSuffix(trimmed, ".json")
	trimmed = strings.ReplaceAll(trimmed, "_", "")
	trimmed = strings.ReplaceAll(trimmed, "-", "")
	return trimmed
}

func toActionSet(items []Action) map[Action]struct{} {
	out := make(map[Action]struct{}, len(items))
	for _, item := range items {
		out[item] = struct{}{}
	}
	return out
}

func toInvariantSet(items []Invariant) map[Invariant]struct{} {
	out := make(map[Invariant]struct{}, len(items))
	for _, item := range items {
		out[item] = struct{}{}
	}
	return out
}

func SortedSpecs() []SpecName {
	out := []SpecName{SpecCDCFlow, SpecFlowState, SpecCDCFlowFanout}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
