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
	SpecCDCFlow       SpecName = "CDCFlow"
	SpecFlowState     SpecName = "FlowStateMachine"
	SpecCDCFlowFanout SpecName = "CDCFlowFanout"
	SpecUnknown       SpecName = ""
)

const (
	ActionNone        Action = ""
	ActionStart       Action = "Start"
	ActionPause       Action = "Pause"
	ActionResume      Action = "Resume"
	ActionStop        Action = "Stop"
	ActionFail        Action = "Fail"
	ActionRunOnce     Action = "RunOnce"
	ActionReadBatch   Action = "ReadBatch"
	ActionReadDDL     Action = "ReadDDL"
	ActionReadFail    Action = "ReadFail"
	ActionReadGiveUp  Action = "ReadGiveUp"
	ActionDeliver     Action = "Deliver"
	ActionWriteFail   Action = "WriteFail"
	ActionWriteGiveUp Action = "WriteGiveUp"
	ActionAck         Action = "Ack"
	ActionAckDest     Action = "AckDest"
	ActionAckSource   Action = "AckSource"
	ActionApproveDDL  Action = "ApproveDDL"
	ActionApplyDDL    Action = "ApplyDDL"
	ActionResumeAfter Action = "ResumeAfterDDL"
	ActionIdle        Action = "Idle"
)

const (
	InvTypeInvariant         Invariant = "TypeInvariant"
	InvNoAckWithoutDeliver   Invariant = "NoAckWithoutDeliver"
	InvAckMonotonic          Invariant = "AckMonotonic"
	InvCheckpointMonotonic   Invariant = "CheckpointMonotonic"
	InvReadAheadBounded      Invariant = "ReadAheadBounded"
	InvRetryBounds           Invariant = "RetryBounds"
	InvDDLAppliedAfter       Invariant = "DDLAppliedAfterApproval"
	InvDDLGatedPausesFlow    Invariant = "DDLGatedPausesFlow"
	InvFlowTransitionsValid  Invariant = "FlowTransitionsValid"
	InvAckedImpliesDelivered Invariant = "AckedImpliesDelivered"
	InvSourceAckRequires     Invariant = "SourceAckRequiresPolicy"
)

var DefaultManifestFiles = map[SpecName]string{
	SpecCDCFlow:       "coverage.json",
	SpecFlowState:     "coverage.flow_state.json",
	SpecCDCFlowFanout: "coverage.fanout.json",
}

var CDCFlowActions = []Action{
	ActionStart,
	ActionPause,
	ActionResume,
	ActionStop,
	ActionFail,
	ActionReadBatch,
	ActionReadDDL,
	ActionReadFail,
	ActionReadGiveUp,
	ActionDeliver,
	ActionWriteFail,
	ActionWriteGiveUp,
	ActionAck,
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
	ActionStop,
	ActionResume,
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
	ActionAckSource,
	ActionIdle,
}

var FanoutInvariants = []Invariant{
	InvTypeInvariant,
	InvAckedImpliesDelivered,
	InvSourceAckRequires,
	InvCheckpointMonotonic,
}

// CDCFlowTraceUnreachableActions documents actions not emitted by the trace suite.
var CDCFlowTraceUnreachableActions = []Action{
	ActionStart,
	ActionPause,
	ActionResume,
	ActionStop,
	ActionFail,
	ActionReadFail,
	ActionReadGiveUp,
	ActionWriteFail,
	ActionWriteGiveUp,
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
	return []Manifest{
		mustManifest(SpecCDCFlow),
		mustManifest(SpecFlowState),
		mustManifest(SpecCDCFlowFanout),
	}
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
	default:
		return Manifest{}, false
	}
}

func mustManifest(spec SpecName) Manifest {
	manifest, ok := ManifestForSpec(spec)
	if !ok {
		panic(fmt.Sprintf("unknown spec %q", spec))
	}
	return manifest
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
