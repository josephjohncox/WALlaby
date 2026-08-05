package failmatrix

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"syscall"
	"time"
)

const (
	// MinimumProcessCyclesPerCell is the required no-skip evidence floor.
	MinimumProcessCyclesPerCell = 100
	defaultProcessCycleTimeout  = 5 * time.Second
	defaultMaxStateBytes        = 1 << 20
)

// durableRevisionObservation records one completed fsync-backed state-store
// operation in its deterministic parent-observed order.
type durableRevisionObservation struct {
	Order      int    `json:"order"`
	Operation  string `json:"operation"`
	Generation int64  `json:"generation"`
	Revision   int64  `json:"revision"`
	SHA256     string `json:"sha256"`
}

// ProcessCycleResult is machine-readable evidence from independent child OS
// processes operating on durable protocol-model state.
type ProcessCycleResult struct {
	EvidenceType           string                       `json:"evidence_type"`
	Cycle                  int                          `json:"cycle"`
	Seed                   int64                        `json:"seed"`
	Profile                string                       `json:"profile"`
	Kind                   string                       `json:"kind"`
	Boundary               string                       `json:"boundary"`
	Fault                  string                       `json:"fault"`
	Schedule               string                       `json:"schedule"`
	ScheduleHash           string                       `json:"schedule_hash"`
	ObservedEvents         []string                     `json:"observed_events"`
	ObservedEventsHash     string                       `json:"observed_events_hash"`
	ObservedRevisions      []durableRevisionObservation `json:"observed_revisions"`
	InitialPID             int                          `json:"initial_pid"`
	RecoveryPID            int                          `json:"recovery_pid"`
	ChildPIDs              []int                        `json:"child_pids"`
	FaultApplied           bool                         `json:"fault_applied"`
	SIGKILLObserved        bool                         `json:"sigkill_observed"`
	RestartObserved        bool                         `json:"restart_observed"`
	OverlapObserved        bool                         `json:"overlap_observed"`
	BoundaryReached        bool                         `json:"boundary_reached"`
	Recovered              bool                         `json:"recovered"`
	Converged              bool                         `json:"converged"`
	FailClosed             bool                         `json:"fail_closed"`
	StaleRejected          bool                         `json:"stale_rejected"`
	StaleMutationAttempted bool                         `json:"stale_mutation_attempted"`
	StaleStateUnchanged    bool                         `json:"stale_state_unchanged"`
	StaleRevisionBefore    int64                        `json:"stale_revision_before"`
	StaleRevisionAfter     int64                        `json:"stale_revision_after"`
	StaleSHA256Before      string                       `json:"stale_sha256_before"`
	StaleSHA256After       string                       `json:"stale_sha256_after"`
	ExternalApplies        int                          `json:"external_applies"`
	Adoptions              int                          `json:"adoptions"`
	CheckpointLSN          int64                        `json:"checkpoint_lsn"`
	SourceFlushLSN         int64                        `json:"source_flush_lsn"`
	DurableWrites          int                          `json:"durable_writes"`
	DurableStateBytes      int64                        `json:"durable_state_bytes"`
	FinalStateSHA256       string                       `json:"final_state_sha256"`
	FinalState             processState                 `json:"final_state"`
	DurationMS             int64                        `json:"duration_ms"`
	ChildUserCPUms         int64                        `json:"child_user_cpu_ms"`
	ChildSystemCPUms       int64                        `json:"child_system_cpu_ms"`
	Violations             []string                     `json:"violations,omitempty"`
}

// Ok reports whether the process cycle produced non-vacuous passing evidence.
func (r ProcessCycleResult) Ok() bool { return len(r.Violations) == 0 }

// ProcessConfig configures an OS-process matrix. RequireCoverage enforces the
// public >=100 cycles per cell contract; focused tests may run individual cycles
// directly without weakening that gate.
type ProcessConfig struct {
	WorkerExecutable  string
	WorkRoot          string
	CyclesPerBoundary int
	Seed              int64
	Profiles          []Profile
	Boundaries        []Boundary
	CycleTimeout      time.Duration
	MaxStateBytes     int64
	RequireCoverage   bool
}

// ProcessSummary is the no-skip, no-vacuity roll-up for OS-process evidence.
type ProcessSummary struct {
	EvidenceType           string               `json:"evidence_type"`
	WorkerExecutable       string               `json:"worker_executable"`
	WorkerSHA256           string               `json:"worker_sha256"`
	GOOS                   string               `json:"goos"`
	GOARCH                 string               `json:"goarch"`
	Seed                   int64                `json:"seed"`
	PlanSHA256             string               `json:"plan_sha256"`
	CyclesPerBoundary      int                  `json:"cycles_per_boundary"`
	MinimumCyclesPerCell   int                  `json:"minimum_cycles_per_cell"`
	TotalCycles            int                  `json:"total_cycles"`
	ExpectedCycles         int                  `json:"expected_cycles"`
	Passed                 int                  `json:"passed"`
	Failed                 int                  `json:"failed"`
	Skipped                int                  `json:"skipped"`
	FailClosedCycles       int                  `json:"fail_closed_cycles"`
	NegativeExpectedCycles int                  `json:"negative_expected_cycles"`
	NegativeCycles         int                  `json:"negative_cycles"`
	CoverageOK             bool                 `json:"coverage_ok"`
	ResourceBoundsOK       bool                 `json:"resource_bounds_ok"`
	ElapsedMS              int64                `json:"elapsed_ms"`
	MaxCycleDurationMS     int64                `json:"max_cycle_duration_ms"`
	MaxCycleCPUms          int64                `json:"max_cycle_cpu_ms"`
	MaxDurableStateBytes   int64                `json:"max_durable_state_bytes"`
	MaxChildrenPerCycle    int                  `json:"max_children_per_cycle"`
	PerBoundary            map[string]int       `json:"per_boundary"`
	PerProfile             map[string]int       `json:"per_profile"`
	PerCell                map[string]int       `json:"per_cell"`
	PerFault               map[string]int       `json:"per_fault"`
	PerCellFault           map[string]int       `json:"per_cell_fault"`
	PerCellSchedule        map[string]int       `json:"per_cell_schedule"`
	PerNegativeCell        map[string]int       `json:"per_negative_cell"`
	PerNegativeCellFault   map[string]int       `json:"per_negative_cell_fault"`
	Violations             []ProcessCycleResult `json:"violations,omitempty"`
}

// RunProcessMatrix runs the required matrix with a prebuilt child executable.
func RunProcessMatrix(cfg ProcessConfig, record func(ProcessCycleResult)) ProcessSummary {
	started := time.Now()
	profiles := cfg.Profiles
	if len(profiles) == 0 {
		profiles = SupportedProfiles()
	}
	boundaries := cfg.Boundaries
	if len(boundaries) == 0 {
		boundaries = RequiredBoundaries()
	}
	if cfg.CyclesPerBoundary <= 0 {
		cfg.CyclesPerBoundary = MinimumProcessCyclesPerCell
	}
	if cfg.CycleTimeout <= 0 {
		cfg.CycleTimeout = defaultProcessCycleTimeout
	}
	if cfg.MaxStateBytes <= 0 {
		cfg.MaxStateBytes = defaultMaxStateBytes
	}
	summary := ProcessSummary{
		EvidenceType: "os_process_protocol_evidence", WorkerExecutable: cfg.WorkerExecutable,
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, Seed: cfg.Seed,
		CyclesPerBoundary: cfg.CyclesPerBoundary, MinimumCyclesPerCell: MinimumProcessCyclesPerCell,
		ExpectedCycles: len(profiles) * len(boundaries) * cfg.CyclesPerBoundary,
		PerBoundary:    map[string]int{}, PerProfile: map[string]int{}, PerCell: map[string]int{},
		PerFault: map[string]int{}, PerCellFault: map[string]int{}, PerCellSchedule: map[string]int{}, PerNegativeCell: map[string]int{},
		PerNegativeCellFault: map[string]int{}, ResourceBoundsOK: true,
	}
	for _, profile := range profiles {
		for _, boundary := range boundaries {
			if isNegativeFailClosedCell(profile, boundary) {
				summary.NegativeExpectedCycles += cfg.CyclesPerBoundary
			}
		}
	}
	if err := validateWorkerExecutable(cfg.WorkerExecutable); err != nil {
		r := ProcessCycleResult{EvidenceType: summary.EvidenceType, Violations: []string{err.Error()}}
		summary.Failed = 1
		summary.Violations = append(summary.Violations, r)
		summary.ElapsedMS = time.Since(started).Milliseconds()
		return summary
	}
	if payload, err := os.ReadFile(cfg.WorkerExecutable); err == nil { // #nosec G304 -- validated explicit executable path.
		sum := sha256.Sum256(payload)
		summary.WorkerSHA256 = hex.EncodeToString(sum[:])
	} else {
		r := ProcessCycleResult{EvidenceType: summary.EvidenceType, Violations: []string{"hash prebuilt worker executable: " + err.Error()}}
		summary.Failed = 1
		summary.Violations = append(summary.Violations, r)
		return summary
	}
	if cfg.WorkRoot == "" {
		root, err := os.MkdirTemp("", "wallaby-failmatrix-")
		if err != nil {
			r := ProcessCycleResult{EvidenceType: summary.EvidenceType, Violations: []string{err.Error()}}
			summary.Failed = 1
			summary.Violations = append(summary.Violations, r)
			return summary
		}
		defer func() { _ = os.RemoveAll(root) }()
		cfg.WorkRoot = root
	}

	faults := []FaultKind{FaultKill, FaultRestart, FaultOverlappingTakeover}
	planDigest := sha256.New()
	cycleID := 0
	for _, profile := range profiles {
		for _, boundary := range boundaries {
			cellSeed := deriveSeed(cfg.Seed, profile.Name, string(boundary), "os-process")
			// #nosec G404 -- reproducible test scheduling, not security.
			rng := rand.New(rand.NewSource(cellSeed))
			offset := rng.Intn(len(faults))
			scheduleOffset := rng.Intn(len(processScheduleVariants))
			for i := 0; i < cfg.CyclesPerBoundary; i++ {
				fault := faults[(i+offset)%len(faults)]
				desiredSchedule := processScheduleVariants[(i+scheduleOffset)%len(processScheduleVariants)]
				cycleSeed := nextSeedForSchedule(rng, fault, desiredSchedule)
				cycleDir := filepath.Join(cfg.WorkRoot, fmt.Sprintf("cycle-%06d", cycleID))
				scheduleHash := processScheduleFor(cycleSeed, fault).hash()
				_, _ = fmt.Fprintf(planDigest, "%d|%d|%s|%s|%s|%s\n", cycleID, cycleSeed, profile.Name, boundary, fault, scheduleHash)
				result := RunProcessCycle(ProcessCycleConfig{
					WorkerExecutable: cfg.WorkerExecutable, CycleDir: cycleDir,
					Cycle: cycleID, Seed: cycleSeed, Profile: profile,
					Boundary: boundary, Fault: fault, Timeout: cfg.CycleTimeout,
					MaxStateBytes: cfg.MaxStateBytes,
				})
				cycleID++
				summary.TotalCycles++
				summary.PerProfile[profile.Name]++
				cellKey := profile.Name + "|" + string(boundary)
				if isNegativeFailClosedCell(profile, boundary) {
					summary.NegativeCycles++
					summary.PerNegativeCell[cellKey]++
					summary.PerNegativeCellFault[cellKey+"|"+string(fault)]++
				} else {
					summary.PerBoundary[string(boundary)]++
					summary.PerCell[cellKey]++
					summary.PerCellFault[cellKey+"|"+string(fault)]++
				}
				summary.PerFault[string(fault)]++
				summary.PerCellSchedule[cellKey+"|"+result.Schedule]++
				if result.FailClosed {
					summary.FailClosedCycles++
				}
				if result.Ok() {
					summary.Passed++
				} else {
					summary.Failed++
					if len(summary.Violations) < 64 {
						summary.Violations = append(summary.Violations, result)
					}
				}
				if result.DurationMS > summary.MaxCycleDurationMS {
					summary.MaxCycleDurationMS = result.DurationMS
				}
				cycleCPUms := result.ChildUserCPUms + result.ChildSystemCPUms
				if cycleCPUms > summary.MaxCycleCPUms {
					summary.MaxCycleCPUms = cycleCPUms
				}
				if result.DurableStateBytes > summary.MaxDurableStateBytes {
					summary.MaxDurableStateBytes = result.DurableStateBytes
				}
				if len(result.ChildPIDs) > summary.MaxChildrenPerCycle {
					summary.MaxChildrenPerCycle = len(result.ChildPIDs)
				}
				if result.DurationMS > cfg.CycleTimeout.Milliseconds() || cycleCPUms > 2*cfg.CycleTimeout.Milliseconds() || result.DurableStateBytes > cfg.MaxStateBytes || len(result.ChildPIDs) > 2 {
					summary.ResourceBoundsOK = false
				}
				if record != nil {
					record(result)
				}
				_ = os.RemoveAll(cycleDir)
			}
		}
	}
	summary.PlanSHA256 = hex.EncodeToString(planDigest.Sum(nil))
	coverage := summary.TotalCycles == summary.ExpectedCycles && summary.Skipped == 0 && summary.Failed == 0
	for _, profile := range profiles {
		for _, boundary := range boundaries {
			cellKey := profile.Name + "|" + string(boundary)
			counts := summary.PerCell
			faultCounts := summary.PerCellFault
			if isNegativeFailClosedCell(profile, boundary) {
				counts = summary.PerNegativeCell
				faultCounts = summary.PerNegativeCellFault
			}
			if counts[cellKey] != cfg.CyclesPerBoundary {
				coverage = false
			}
			for _, fault := range faults {
				if faultCounts[cellKey+"|"+string(fault)] == 0 {
					coverage = false
				}
			}
			for _, schedule := range processScheduleVariants {
				if summary.PerCellSchedule[cellKey+"|"+schedule] == 0 {
					coverage = false
				}
			}
		}
	}
	if summary.NegativeCycles != summary.NegativeExpectedCycles {
		coverage = false
	}
	for _, fault := range faults {
		if summary.PerFault[string(fault)] == 0 {
			coverage = false
		}
	}
	if cfg.RequireCoverage && cfg.CyclesPerBoundary < MinimumProcessCyclesPerCell {
		coverage = false
	}
	summary.CoverageOK = coverage && summary.ResourceBoundsOK
	summary.ElapsedMS = time.Since(started).Milliseconds()
	return summary
}

func isNegativeFailClosedCell(profile Profile, _ Boundary) bool {
	return isStreaming(profile) && !profile.StreamingTransportLinked
}

// ProcessCycleConfig configures one focused OS-process fault cycle.
type ProcessCycleConfig struct {
	WorkerExecutable string
	CycleDir         string
	Cycle            int
	Seed             int64
	Profile          Profile
	Boundary         Boundary
	Fault            FaultKind
	Timeout          time.Duration
	MaxStateBytes    int64
}

type processSchedule struct {
	Variant string `json:"variant"`
}

type expectedDurableOperation struct {
	Operation  string
	Generation int64
	Revision   int64
}

type expectedProcessSchedule struct {
	Generation    int64
	LeaseEpoch    int64
	Revision      int64
	DurableWrites int
	Operations    []expectedDurableOperation
}

var processScheduleVariants = []string{"probe_before_transition", "probe_after_transition", "probe_both_sides"}

// expectedProcessScheduleFor is the single attestation model for the actual
// fsync-backed handshake. Every entry corresponds to one successful
// storeProcessState call, including initial creation and takeover's distinct
// final recovery persist.
func expectedProcessScheduleFor(fault FaultKind, schedule processSchedule) (expectedProcessSchedule, error) {
	transition := "recovery"
	transitionGeneration := int64(1)
	switch fault {
	case FaultKill, FaultRestart:
	case FaultOverlappingTakeover:
		transition = "takeover"
		transitionGeneration = 2
	default:
		return expectedProcessSchedule{}, fmt.Errorf("unsupported fault %q", fault)
	}
	before, after := false, false
	switch schedule.Variant {
	case "probe_before_transition":
		before = true
	case "probe_after_transition":
		after = true
	case "probe_both_sides":
		before, after = true, true
	default:
		return expectedProcessSchedule{}, fmt.Errorf("unsupported process schedule %q", schedule.Variant)
	}
	expected := expectedProcessSchedule{Generation: transitionGeneration, LeaseEpoch: transitionGeneration}
	appendOperation := func(operation string, generation int64) {
		expected.Revision++
		expected.DurableWrites++
		expected.Operations = append(expected.Operations, expectedDurableOperation{
			Operation: operation, Generation: generation, Revision: expected.Revision,
		})
	}
	appendOperation("initial:child_persist", 1)
	if before {
		appendOperation(transition+":parent_probe_before", 1)
	}
	appendOperation(transition+":child_transition", transitionGeneration)
	if after {
		appendOperation(transition+":parent_probe_after", transitionGeneration)
	}
	if fault == FaultOverlappingTakeover {
		appendOperation("takeover:child_final_recovery", transitionGeneration)
	}
	return expected, nil
}

func processScheduleFor(seed int64, fault FaultKind) processSchedule {
	derived := deriveSeed(seed, string(fault), "schedule")
	return processSchedule{Variant: processScheduleVariants[int(derived%int64(len(processScheduleVariants)))]}
}

func nextSeedForSchedule(rng *rand.Rand, fault FaultKind, desired string) int64 {
	for {
		seed := rng.Int63()
		if processScheduleFor(seed, fault).Variant == desired {
			return seed
		}
	}
}

func (p processSchedule) hash() string {
	digest := sha256.Sum256([]byte("process-schedule-v1\x00" + p.Variant))
	return hex.EncodeToString(digest[:])
}

// RunProcessCycle starts real child processes, interrupts the initial PID, and
// recovers from the fsync-backed state with a distinct child PID.
func RunProcessCycle(cfg ProcessCycleConfig) ProcessCycleResult {
	started := time.Now()
	schedule := processScheduleFor(cfg.Seed, cfg.Fault)
	r := ProcessCycleResult{
		EvidenceType: "os_process_protocol_evidence", Cycle: cfg.Cycle, Seed: cfg.Seed,
		Profile: cfg.Profile.Name, Kind: string(cfg.Profile.Kind), Boundary: string(cfg.Boundary),
		Fault: string(cfg.Fault), Schedule: schedule.Variant, ScheduleHash: schedule.hash(),
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultProcessCycleTimeout
	}
	if cfg.MaxStateBytes <= 0 {
		cfg.MaxStateBytes = defaultMaxStateBytes
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()
	if err := os.MkdirAll(cfg.CycleDir, 0o750); err != nil {
		r.Violations = append(r.Violations, err.Error())
		return finishProcessResult(r, started)
	}
	statePath := filepath.Join(cfg.CycleDir, "state.json")
	initialMode := ProcessWorkerInitial
	if cfg.Fault == FaultRestart {
		initialMode = ProcessWorkerInitialRestart
	}
	initial := workerCommand(ctx, cfg.WorkerExecutable, initialMode, statePath, cfg.Profile.Name, cfg.Boundary, 1, "")
	if err := initial.Start(); err != nil {
		r.Violations = append(r.Violations, "start initial child: "+err.Error())
		return finishProcessResult(r, started)
	}
	r.InitialPID = initial.Process.Pid
	r.ChildPIDs = append(r.ChildPIDs, r.InitialPID)
	state, err := waitForProcessPhase(ctx, statePath, processPhaseReady)
	if err != nil {
		_ = initial.Process.Kill()
		_ = initial.Wait()
		r.Violations = append(r.Violations, "wait initial durable boundary: "+err.Error())
		return finishProcessResult(r, started)
	}
	r.BoundaryReached = state.BoundaryReached
	if err := observeDurableRevision(&r, "initial:child_persist", statePath); err != nil {
		killAndWait(initial)
		r.Violations = append(r.Violations, err.Error())
		return finishProcessResult(r, started)
	}

	switch cfg.Fault {
	case FaultKill:
		err = initial.Process.Signal(os.Kill)
		waitErr := initial.Wait()
		recordProcessUsage(&r, initial)
		r.FaultApplied = err == nil
		r.SIGKILLObserved = wasSIGKILL(waitErr)
		if err != nil {
			r.Violations = append(r.Violations, "send SIGKILL: "+err.Error())
		}
		if !r.SIGKILLObserved {
			r.Violations = append(r.Violations, fmt.Sprintf("initial PID %d did not exit from SIGKILL: %v", r.InitialPID, waitErr))
		}
		err = runRecoveryChild(ctx, cfg, statePath, 1, ProcessWorkerRecover, schedule, &r)
	case FaultRestart:
		waitErr := initial.Wait()
		recordProcessUsage(&r, initial)
		r.FaultApplied = exitCode(waitErr) == PlannedRestartExitCode
		if !r.FaultApplied {
			r.Violations = append(r.Violations, fmt.Sprintf("initial PID %d restart exit=%v", r.InitialPID, waitErr))
		}
		err = runRecoveryChild(ctx, cfg, statePath, 1, ProcessWorkerRecover, schedule, &r)
	case FaultOverlappingTakeover:
		err = runOverlappingTakeover(ctx, cfg, statePath, initial, schedule, &r)
	default:
		_ = initial.Process.Kill()
		_ = initial.Wait()
		err = fmt.Errorf("unsupported fault %q", cfg.Fault)
	}
	if err != nil {
		r.Violations = append(r.Violations, err.Error())
	}

	finalState, loadErr := loadProcessState(statePath)
	if loadErr != nil {
		r.Violations = append(r.Violations, "load final durable state: "+loadErr.Error())
		return finishProcessResult(r, started)
	}
	e, engineErr := finalState.engine()
	if engineErr != nil {
		r.Violations = append(r.Violations, engineErr.Error())
		return finishProcessResult(r, started)
	}
	r.FailClosed = finalState.FailClosed
	r.FinalState = finalState
	r.Recovered = finalState.Phase == processPhaseComplete
	r.ExternalApplies = e.auth.externalApplyCount
	r.Adoptions = e.auth.adoptionCount
	r.CheckpointLSN = e.auth.checkpoint
	r.SourceFlushLSN = e.auth.sourceFlushLSN
	r.DurableWrites = finalState.DurableWrites
	modelResult := CycleResult{Fault: r.Fault, Recovered: r.Recovered, FailClosed: r.FailClosed, StaleRejected: r.StaleRejected}
	r.Violations = append(r.Violations, e.checkInvariants(modelResult)...)
	if !r.FailClosed && !r.BoundaryReached {
		r.Violations = append(r.Violations, "requested boundary was not reached")
	}
	if !r.FaultApplied {
		r.Violations = append(r.Violations, "requested OS-process fault was not applied")
	}
	if r.RecoveryPID == 0 || r.RecoveryPID == r.InitialPID {
		r.Violations = append(r.Violations, "recovery did not use a distinct child PID")
	}
	if cfg.Fault != FaultOverlappingTakeover {
		r.RestartObserved = r.RecoveryPID != 0 && r.RecoveryPID != r.InitialPID
	}
	info, statErr := os.Stat(statePath)
	if statErr != nil {
		r.Violations = append(r.Violations, "stat final durable state: "+statErr.Error())
	} else {
		r.DurableStateBytes = info.Size()
	}
	// The evidence hash is defined over the serialized FinalState field. The
	// durable file must contain those exact canonical bytes; stat/read/marshal
	// failures are cycle violations rather than silently missing evidence.
	serializedFinalState, marshalErr := json.Marshal(r.FinalState)
	if marshalErr != nil {
		r.Violations = append(r.Violations, "serialize final durable state: "+marshalErr.Error())
	} else {
		sum := sha256.Sum256(serializedFinalState)
		r.FinalStateSHA256 = hex.EncodeToString(sum[:])
		// #nosec G304 -- statePath is the runner-owned durable file.
		persisted, readErr := os.ReadFile(statePath)
		switch {
		case readErr != nil:
			r.Violations = append(r.Violations, "read final durable state: "+readErr.Error())
		case !reflect.DeepEqual(persisted, serializedFinalState):
			r.Violations = append(r.Violations, "serialized FinalState does not match durable state bytes")
		default:
			persistedHash := sha256.Sum256(persisted)
			if hex.EncodeToString(persistedHash[:]) != r.FinalStateSHA256 {
				r.Violations = append(r.Violations, "final durable state hash mismatch")
			}
		}
	}
	if r.DurableWrites < 2 {
		r.Violations = append(r.Violations, fmt.Sprintf("durable writes=%d, want at least 2", r.DurableWrites))
	}
	if r.DurableStateBytes <= 0 || r.DurableStateBytes > cfg.MaxStateBytes {
		r.Violations = append(r.Violations, fmt.Sprintf("durable state bytes=%d outside bound 1..%d", r.DurableStateBytes, cfg.MaxStateBytes))
	}
	r.Violations = append(r.Violations, checkProcessResultPredicates(r, cfg.Profile, cfg.Boundary, cfg.Fault)...)
	r.Converged = len(r.Violations) == 0
	return finishProcessResult(r, started)
}

func runRecoveryChild(ctx context.Context, cfg ProcessCycleConfig, statePath string, generation int64, mode ProcessWorkerMode, schedule processSchedule, r *ProcessCycleResult) error {
	cmd := workerCommand(ctx, cfg.WorkerExecutable, mode, statePath, cfg.Profile.Name, cfg.Boundary, generation, schedule.Variant)
	if err := startScheduledTransition(ctx, statePath, "recovery", generation, cmd, schedule, r); err != nil {
		if cmd.Process != nil {
			r.RecoveryPID = cmd.Process.Pid
			r.ChildPIDs = append(r.ChildPIDs, r.RecoveryPID)
		}
		return fmt.Errorf("start recovery child: %w", err)
	}
	r.RecoveryPID = cmd.Process.Pid
	r.ChildPIDs = append(r.ChildPIDs, r.RecoveryPID)
	waitErr := cmd.Wait()
	recordProcessUsage(r, cmd)
	if waitErr != nil {
		return fmt.Errorf("recovery PID %d: %w", r.RecoveryPID, waitErr)
	}
	return nil
}

func runOverlappingTakeover(ctx context.Context, cfg ProcessCycleConfig, statePath string, initial *exec.Cmd, schedule processSchedule, r *ProcessCycleResult) error {
	newChild := workerCommand(ctx, cfg.WorkerExecutable, ProcessWorkerTakeover, statePath, cfg.Profile.Name, cfg.Boundary, 2, schedule.Variant)
	if err := startScheduledTransition(ctx, statePath, "takeover", 2, newChild, schedule, r); err != nil {
		killAndWait(initial)
		if newChild.Process != nil {
			r.RecoveryPID = newChild.Process.Pid
			r.ChildPIDs = append(r.ChildPIDs, r.RecoveryPID)
		}
		return fmt.Errorf("start takeover child: %w", err)
	}
	r.RecoveryPID = newChild.Process.Pid
	r.ChildPIDs = append(r.ChildPIDs, r.RecoveryPID)
	initialReaped := false
	takeoverReaped := false
	defer func() {
		if !initialReaped {
			killAndWait(initial)
		}
		if !takeoverReaped {
			// The takeover child remains live until ownership discharge. Every
			// return before discharge therefore kills and waits it exactly here.
			killAndWait(newChild)
		}
	}()
	if _, err := waitForProcessPhase(ctx, statePath, processPhaseTakeoverReady); err != nil {
		return fmt.Errorf("wait takeover generation: %w", err)
	}
	r.OverlapObserved = processAlive(initial.Process) && processAlive(newChild.Process)
	if err := touchDurable(filepath.Join(cfg.CycleDir, "resume-old")); err != nil {
		return err
	}
	initialErr := initial.Wait()
	initialReaped = true
	recordProcessUsage(r, initial)
	if initialErr != nil {
		return fmt.Errorf("stale PID %d: %w", r.InitialPID, initialErr)
	}
	stale, err := readStaleResult(filepath.Join(cfg.CycleDir, "stale-result.json"))
	if err != nil {
		return err
	}
	r.StaleMutationAttempted = stale.MutationAttempted
	r.StaleStateUnchanged = stale.StateUnchanged
	r.StaleRevisionBefore = stale.RevisionBefore
	r.StaleRevisionAfter = stale.RevisionAfter
	r.StaleSHA256Before = stale.StateSHA256Before
	r.StaleSHA256After = stale.StateSHA256After
	r.StaleRejected = stale.Rejected && stale.ErrorType == "stale_generation" &&
		stale.MutationAttempted && stale.StateUnchanged &&
		stale.RevisionBefore == stale.RevisionAfter &&
		stale.StateSHA256Before != "" && stale.StateSHA256Before == stale.StateSHA256After &&
		stale.PID == r.InitialPID && stale.AttemptGeneration == 1 && stale.DurableGeneration == 2
	if err := touchDurable(filepath.Join(cfg.CycleDir, "continue-new")); err != nil {
		return err
	}
	takeoverErr := newChild.Wait()
	takeoverReaped = true
	recordProcessUsage(r, newChild)
	if takeoverErr != nil {
		return fmt.Errorf("takeover PID %d: %w", r.RecoveryPID, takeoverErr)
	}
	if err := observeDurableRevision(r, "takeover:child_final_recovery", statePath); err != nil {
		return err
	}
	r.FaultApplied = r.OverlapObserved && r.StaleRejected
	if !r.OverlapObserved {
		return errors.New("old and takeover PIDs were not concurrently alive")
	}
	if !r.StaleRejected {
		return errors.New("stale generation durable mutation was not rejected unchanged by locked CAS")
	}
	return nil
}

func workerCommand(ctx context.Context, executable string, mode ProcessWorkerMode, statePath, profile string, boundary Boundary, generation int64, schedule string) *exec.Cmd {
	return exec.CommandContext(ctx, executable,
		"-mode", string(mode), "-state", statePath, "-profile", profile,
		"-boundary", string(boundary), "-generation", fmt.Sprint(generation),
		"-schedule", schedule) // #nosec G204 -- executable is explicitly validated and args are non-shell data.
}

func startScheduledTransition(ctx context.Context, statePath, transition string, transitionGeneration int64, cmd *exec.Cmd, schedule processSchedule, r *ProcessCycleResult) error {
	cycleDir := filepath.Dir(statePath)
	release := filepath.Join(cycleDir, transition+"-release")
	pre := filepath.Join(cycleDir, transition+"-pre")
	post := filepath.Join(cycleDir, transition+"-post")
	ack := filepath.Join(cycleDir, transition+"-ack")
	if schedule.Variant == "probe_before_transition" || schedule.Variant == "probe_both_sides" {
		state, err := loadProcessState(statePath)
		if err != nil {
			return fmt.Errorf("load pre-transition probe state: %w", err)
		}
		if err := storeProcessState(statePath, state, state.Generation); err != nil {
			return fmt.Errorf("persist pre-transition CAS probe: %w", err)
		}
		if err := observeDurableRevision(r, transition+":parent_probe_before", statePath); err != nil {
			return err
		}
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	if err := waitForFileContext(ctx, pre); err != nil {
		killAndWait(cmd)
		return err
	}
	if err := touchDurable(release); err != nil {
		killAndWait(cmd)
		return err
	}
	if err := waitForFileContext(ctx, post); err != nil {
		killAndWait(cmd)
		return err
	}
	if err := observeDurableRevision(r, transition+":child_transition", statePath); err != nil {
		killAndWait(cmd)
		return err
	}
	if schedule.Variant == "probe_after_transition" || schedule.Variant == "probe_both_sides" {
		state, err := loadProcessState(statePath)
		if err != nil {
			killAndWait(cmd)
			return fmt.Errorf("load post-transition probe state: %w", err)
		}
		if state.Generation != transitionGeneration {
			killAndWait(cmd)
			return fmt.Errorf("post-transition generation=%d, want %d", state.Generation, transitionGeneration)
		}
		if err := storeProcessState(statePath, state, transitionGeneration); err != nil {
			killAndWait(cmd)
			return fmt.Errorf("persist post-transition CAS probe: %w", err)
		}
		if err := observeDurableRevision(r, transition+":parent_probe_after", statePath); err != nil {
			killAndWait(cmd)
			return err
		}
	}
	if err := touchDurable(ack); err != nil {
		killAndWait(cmd)
		return err
	}
	return nil
}

func observeDurableRevision(result *ProcessCycleResult, operation, statePath string) error {
	snapshot, err := snapshotProcessState(statePath)
	if err != nil {
		return fmt.Errorf("observe durable revision after %s: %w", operation, err)
	}
	result.ObservedEvents = append(result.ObservedEvents, operation)
	result.ObservedRevisions = append(result.ObservedRevisions, durableRevisionObservation{
		Order: len(result.ObservedRevisions) + 1, Operation: operation,
		Generation: snapshot.Generation, Revision: snapshot.Revision, SHA256: snapshot.SHA256,
	})
	return nil
}

func checkProcessResultPredicates(result ProcessCycleResult, profile Profile, boundary Boundary, fault FaultKind) []string {
	var violations []string
	state := result.FinalState
	if result.FinalStateSHA256 == "" {
		violations = append(violations, "final state hash is empty")
	}
	if state.Profile != profile.Name || state.RequestedBoundary != string(boundary) ||
		state.BoundaryReached != result.BoundaryReached || state.FailClosed != result.FailClosed ||
		state.Authority.ExternalApplyCount != result.ExternalApplies || state.Authority.AdoptionCount != result.Adoptions ||
		state.Authority.Checkpoint != result.CheckpointLSN || state.Authority.SourceFlushLSN != result.SourceFlushLSN ||
		state.DurableWrites != result.DurableWrites {
		violations = append(violations, "final evidence does not match durable final state")
	}
	if result.Fault != string(fault) {
		violations = append(violations, fmt.Sprintf("result fault=%q, want %q", result.Fault, fault))
	}
	derivedSchedule := processScheduleFor(result.Seed, fault)
	if result.Schedule != derivedSchedule.Variant {
		violations = append(violations, fmt.Sprintf("result schedule=%q does not match seed-derived schedule %q", result.Schedule, derivedSchedule.Variant))
	}
	if result.ScheduleHash != derivedSchedule.hash() {
		violations = append(violations, "result schedule hash does not match seed-derived schedule")
	}
	expectedSchedule, scheduleErr := expectedProcessScheduleFor(fault, derivedSchedule)
	if scheduleErr != nil {
		violations = append(violations, scheduleErr.Error())
	} else {
		if state.Generation != expectedSchedule.Generation {
			violations = append(violations, fmt.Sprintf("final generation=%d, want %d", state.Generation, expectedSchedule.Generation))
		}
		if state.Authority.LeaseEpoch != expectedSchedule.LeaseEpoch {
			violations = append(violations, fmt.Sprintf("final lease epoch=%d, want %d", state.Authority.LeaseEpoch, expectedSchedule.LeaseEpoch))
		}
		if state.Revision != expectedSchedule.Revision {
			violations = append(violations, fmt.Sprintf("final revision=%d, want %d", state.Revision, expectedSchedule.Revision))
		}
		if state.DurableWrites != expectedSchedule.DurableWrites || result.DurableWrites != expectedSchedule.DurableWrites {
			violations = append(violations, fmt.Sprintf("durable writes state/result=%d/%d, want %d", state.DurableWrites, result.DurableWrites, expectedSchedule.DurableWrites))
		}
		if len(result.ObservedEvents) != len(expectedSchedule.Operations) || len(result.ObservedRevisions) != len(expectedSchedule.Operations) {
			violations = append(violations, fmt.Sprintf("durable observation count events/revisions=%d/%d, want %d", len(result.ObservedEvents), len(result.ObservedRevisions), len(expectedSchedule.Operations)))
		}
		for index, expectedOperation := range expectedSchedule.Operations {
			if index >= len(result.ObservedEvents) || index >= len(result.ObservedRevisions) {
				break
			}
			observed := result.ObservedRevisions[index]
			if result.ObservedEvents[index] != expectedOperation.Operation || observed.Order != index+1 || observed.Operation != expectedOperation.Operation || observed.Generation != expectedOperation.Generation || observed.Revision != expectedOperation.Revision || observed.SHA256 == "" {
				violations = append(violations, fmt.Sprintf("durable observation %d does not match expected operation/revision", index+1))
			}
		}
	}
	if isNegativeFailClosedCell(profile, boundary) {
		if !result.FailClosed {
			violations = append(violations, "negative cell did not fail closed")
		}
		if result.BoundaryReached {
			violations = append(violations, "negative cell reached an inapplicable boundary")
		}
		if state.Phase != processPhaseFailClosed {
			violations = append(violations, "negative cell final phase is not fail_closed")
		}
		if result.Recovered {
			violations = append(violations, "negative cell reported recovery completion")
		}
		violations = append(violations, zeroProgressionViolations(state)...)
		return violations
	}
	if !result.Recovered {
		violations = append(violations, "applicable cell did not recover")
	}
	if !result.BoundaryReached {
		violations = append(violations, "applicable cell did not reach requested boundary")
	}
	if state.Phase != processPhaseComplete {
		violations = append(violations, "applicable cell final phase is not complete")
	}
	if result.FailClosed {
		violations = append(violations, "applicable cell unexpectedly failed closed")
	}

	expectedGeneration := int64(1)
	if fault == FaultOverlappingTakeover {
		expectedGeneration = 2
	}
	expectedEngine := newEngine(profile)
	reached, failClosed := expectedEngine.runToBoundary(1, boundaryStage(boundary))
	if fault == FaultOverlappingTakeover {
		expectedEngine.auth.leaseEpoch = expectedGeneration
	}
	completed, recoveredFailClosed := expectedEngine.recover(expectedGeneration)
	expected := stateFromEngine(expectedEngine, boundary, reached, failClosed || recoveredFailClosed, processPhaseComplete, 0)
	if !completed || failClosed || recoveredFailClosed ||
		state.Generation != expected.Generation || state.Authority != expected.Authority ||
		state.Destination != expected.Destination || state.ObjectSeq != expected.ObjectSeq ||
		state.ConfirmLSN != expected.ConfirmLSN {
		violations = append(violations, "final durable protocol state does not match deterministic recovery state")
	}
	return violations
}

func zeroProgressionViolations(state processState) []string {
	var violations []string
	a, d := state.Authority, state.Destination
	checks := []struct {
		advanced bool
		name     string
	}{
		{a.AttemptPrepared, "attempt_prepared"},
		{a.ReceiptAdopted, "receipt_adopted"},
		{a.Checkpoint != 0, "checkpoint"},
		{a.AckIntent, "ack_intent"},
		{a.SourceFlushLSN != 0, "source_flush_lsn"},
		{a.FlushReceipt, "flush_receipt"},
		{a.Publication, "publication"},
		{a.ObjectVersion != 0, "object_version"},
		{a.ConsumerReceipt, "consumer_receipt"},
		{a.RetentionReleased, "retention_released"},
		{a.GCMarked, "gc_marked"},
		{a.GCFinalized, "gc_finalized"},
		{a.ExternalApplyCount != 0, "external_apply_count"},
		{a.AdoptionCount != 0, "adoption_count"},
		{d.Committed, "destination_committed"},
		{d.Reveal != 0, "destination_reveal"},
		{d.ReceiptVisible, "destination_receipt_visible"},
		{d.Version != 0, "destination_version"},
		{d.ApplyAttempts != 0, "destination_apply_attempts"},
		{state.ObjectSeq != 0, "object_seq"},
		{state.ConfirmLSN != 0, "confirm_lsn"},
	}
	for _, check := range checks {
		if check.advanced {
			violations = append(violations, "negative cell advanced "+check.name)
		}
	}
	return violations
}

func waitForFileContext(ctx context.Context, path string) error {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitForProcessPhase(ctx context.Context, path string, phase string) (processState, error) {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := loadProcessState(path)
		if err == nil && state.Phase == phase {
			return state, nil
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return processState{}, err
		}
		select {
		case <-ctx.Done():
			return processState{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func readStaleResult(path string) (staleAttemptResult, error) {
	// #nosec G304 -- path is under the runner-owned cycle directory.
	payload, err := os.ReadFile(path)
	if err != nil {
		return staleAttemptResult{}, fmt.Errorf("read stale result: %w", err)
	}
	var result staleAttemptResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return result, err
	}
	return result, nil
}

func validateWorkerExecutable(path string) error {
	if path == "" {
		return errors.New("prebuilt worker executable is required")
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat prebuilt worker executable: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("worker executable %q is not a regular file", path)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("worker executable %q is not executable", path)
	}
	return nil
}

func wasSIGKILL(err error) bool {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGKILL
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func processAlive(process *os.Process) bool { return process.Signal(syscall.Signal(0)) == nil }

func killAndWait(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
}

func recordProcessUsage(result *ProcessCycleResult, cmd *exec.Cmd) {
	if cmd.ProcessState == nil {
		return
	}
	result.ChildUserCPUms += cmd.ProcessState.UserTime().Milliseconds()
	result.ChildSystemCPUms += cmd.ProcessState.SystemTime().Milliseconds()
}

func finishProcessResult(r ProcessCycleResult, started time.Time) ProcessCycleResult {
	r.DurationMS = time.Since(started).Milliseconds()
	payload, err := json.Marshal(struct {
		Events    []string                     `json:"events"`
		Revisions []durableRevisionObservation `json:"revisions"`
	}{Events: r.ObservedEvents, Revisions: r.ObservedRevisions})
	if err != nil {
		r.Violations = append(r.Violations, "serialize observed durable ordering: "+err.Error())
	} else {
		digest := sha256.Sum256(payload)
		r.ObservedEventsHash = hex.EncodeToString(digest[:])
	}
	if len(r.Violations) > 0 {
		r.Converged = false
		sort.Strings(r.Violations)
	}
	return r
}
