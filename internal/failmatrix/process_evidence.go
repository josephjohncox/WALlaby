package failmatrix

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ProcessEvidenceWriter writes evidence that is explicitly labeled as
// OS-process protocol evidence and never as destination implementation proof.
type ProcessEvidenceWriter struct {
	dir        string
	cycles     *os.File
	normalized *os.File
	startedAt  time.Time
}

type normalizedProcessCycle struct {
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
}

func normalizedProcessCycleBytes(result ProcessCycleResult) ([]byte, error) {
	return json.Marshal(normalizedProcessCycle{
		EvidenceType: result.EvidenceType, Cycle: result.Cycle, Seed: result.Seed,
		Profile: result.Profile, Kind: result.Kind, Boundary: result.Boundary, Fault: result.Fault,
		Schedule: result.Schedule, ScheduleHash: result.ScheduleHash,
		ObservedEvents: result.ObservedEvents, ObservedEventsHash: result.ObservedEventsHash,
		ObservedRevisions: result.ObservedRevisions,
		FaultApplied:      result.FaultApplied, SIGKILLObserved: result.SIGKILLObserved,
		RestartObserved: result.RestartObserved, OverlapObserved: result.OverlapObserved,
		BoundaryReached: result.BoundaryReached, Recovered: result.Recovered,
		Converged: result.Converged, FailClosed: result.FailClosed,
		StaleRejected: result.StaleRejected, StaleMutationAttempted: result.StaleMutationAttempted,
		StaleStateUnchanged: result.StaleStateUnchanged,
		StaleRevisionBefore: result.StaleRevisionBefore, StaleRevisionAfter: result.StaleRevisionAfter,
		StaleSHA256Before: result.StaleSHA256Before, StaleSHA256After: result.StaleSHA256After,
		ExternalApplies: result.ExternalApplies, Adoptions: result.Adoptions,
		CheckpointLSN: result.CheckpointLSN, SourceFlushLSN: result.SourceFlushLSN,
		DurableWrites: result.DurableWrites, DurableStateBytes: result.DurableStateBytes,
		FinalStateSHA256: result.FinalStateSHA256, FinalState: result.FinalState,
	})
}

type normalizedProcessSummary struct {
	EvidenceType           string         `json:"evidence_type"`
	Seed                   int64          `json:"seed"`
	PlanSHA256             string         `json:"plan_sha256"`
	CyclesPerBoundary      int            `json:"cycles_per_boundary"`
	MinimumCyclesPerCell   int            `json:"minimum_cycles_per_cell"`
	TotalCycles            int            `json:"total_cycles"`
	ExpectedCycles         int            `json:"expected_cycles"`
	Passed                 int            `json:"passed"`
	Failed                 int            `json:"failed"`
	Skipped                int            `json:"skipped"`
	FailClosedCycles       int            `json:"fail_closed_cycles"`
	NegativeExpectedCycles int            `json:"negative_expected_cycles"`
	NegativeCycles         int            `json:"negative_cycles"`
	CoverageOK             bool           `json:"coverage_ok"`
	ResourceBoundsOK       bool           `json:"resource_bounds_ok"`
	MaxDurableStateBytes   int64          `json:"max_durable_state_bytes"`
	MaxChildrenPerCycle    int            `json:"max_children_per_cycle"`
	PerBoundary            map[string]int `json:"per_boundary"`
	PerProfile             map[string]int `json:"per_profile"`
	PerCell                map[string]int `json:"per_cell"`
	PerFault               map[string]int `json:"per_fault"`
	PerCellFault           map[string]int `json:"per_cell_fault"`
	PerCellSchedule        map[string]int `json:"per_cell_schedule"`
	PerNegativeCell        map[string]int `json:"per_negative_cell"`
	PerNegativeCellFault   map[string]int `json:"per_negative_cell_fault"`
}

func normalizeProcessSummary(summary ProcessSummary) normalizedProcessSummary {
	return normalizedProcessSummary{
		EvidenceType: summary.EvidenceType, Seed: summary.Seed, PlanSHA256: summary.PlanSHA256,
		CyclesPerBoundary: summary.CyclesPerBoundary, MinimumCyclesPerCell: summary.MinimumCyclesPerCell,
		TotalCycles: summary.TotalCycles, ExpectedCycles: summary.ExpectedCycles,
		Passed: summary.Passed, Failed: summary.Failed, Skipped: summary.Skipped,
		FailClosedCycles:       summary.FailClosedCycles,
		NegativeExpectedCycles: summary.NegativeExpectedCycles, NegativeCycles: summary.NegativeCycles,
		CoverageOK: summary.CoverageOK, ResourceBoundsOK: summary.ResourceBoundsOK,
		MaxDurableStateBytes: summary.MaxDurableStateBytes, MaxChildrenPerCycle: summary.MaxChildrenPerCycle,
		PerBoundary: summary.PerBoundary, PerProfile: summary.PerProfile, PerCell: summary.PerCell,
		PerFault: summary.PerFault, PerCellFault: summary.PerCellFault, PerCellSchedule: summary.PerCellSchedule,
		PerNegativeCell: summary.PerNegativeCell, PerNegativeCellFault: summary.PerNegativeCellFault,
	}
}

// NewProcessEvidenceWriter creates a timestamped OS-process evidence directory.
func NewProcessEvidenceWriter(root string, seed int64) (*ProcessEvidenceWriter, error) {
	started := time.Now().UTC()
	dir := filepath.Join(root, fmt.Sprintf("os_run_%s_seed%d", started.Format("20060102T150405Z"), seed))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create OS-process evidence dir: %w", err)
	}
	// #nosec G304 -- evidence path derives from the explicit output root.
	cycles, err := os.Create(filepath.Join(dir, "cycles.ndjson"))
	if err != nil {
		return nil, fmt.Errorf("create OS-process cycles.ndjson: %w", err)
	}
	// #nosec G304 -- evidence path derives from the explicit output root.
	normalized, err := os.Create(filepath.Join(dir, "normalized.ndjson"))
	if err != nil {
		_ = cycles.Close()
		return nil, fmt.Errorf("create normalized OS-process evidence: %w", err)
	}
	return &ProcessEvidenceWriter{dir: dir, cycles: cycles, normalized: normalized, startedAt: started}, nil
}

// Dir returns the evidence directory.
func (w *ProcessEvidenceWriter) Dir() string { return w.dir }

// Record appends a single process-cycle record.
func (w *ProcessEvidenceWriter) Record(result ProcessCycleResult) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if _, err := w.cycles.Write(append(payload, '\n')); err != nil {
		return err
	}
	normalized, err := normalizedProcessCycleBytes(result)
	if err != nil {
		return err
	}
	if _, err := w.normalized.Write(append(normalized, '\n')); err != nil {
		return err
	}
	return nil
}

// Finish fsyncs the NDJSON stream and writes JSON/text summaries.
func (w *ProcessEvidenceWriter) Finish(summary ProcessSummary) error {
	if err := w.cycles.Sync(); err != nil {
		_ = w.cycles.Close()
		_ = w.normalized.Close()
		return fmt.Errorf("fsync OS-process cycles.ndjson: %w", err)
	}
	if err := w.cycles.Close(); err != nil {
		_ = w.normalized.Close()
		return fmt.Errorf("close OS-process cycles.ndjson: %w", err)
	}
	if err := w.normalized.Sync(); err != nil {
		_ = w.normalized.Close()
		return fmt.Errorf("fsync normalized OS-process evidence: %w", err)
	}
	if err := w.normalized.Close(); err != nil {
		return fmt.Errorf("close normalized OS-process evidence: %w", err)
	}
	payload, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(w.dir, "summary.json"), payload, 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(w.dir, "summary.txt"), []byte(w.renderText(summary)), 0o600); err != nil {
		return err
	}
	normalizedSummary, err := json.MarshalIndent(normalizeProcessSummary(summary), "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(w.dir, "normalized-summary.json"), append(normalizedSummary, '\n'), 0o600); err != nil {
		return err
	}
	return nil
}

func (w *ProcessEvidenceWriter) renderText(summary ProcessSummary) string {
	out := "WALlaby credential-free OS-process protocol evidence\n"
	out += "====================================================\n"
	out += "Evidence scope: real child PIDs and fsync-backed protocol-model state.\n"
	out += "This is not destination implementation or real-service delivery proof.\n\n"
	out += fmt.Sprintf("started_at             : %s\n", w.startedAt.Format(time.RFC3339))
	out += fmt.Sprintf("evidence_type          : %s\n", summary.EvidenceType)
	out += fmt.Sprintf("worker_sha256          : %s\n", summary.WorkerSHA256)
	out += fmt.Sprintf("platform               : %s/%s\n", summary.GOOS, summary.GOARCH)
	out += fmt.Sprintf("seed                   : %d\n", summary.Seed)
	out += fmt.Sprintf("plan_sha256            : %s\n", summary.PlanSHA256)
	out += fmt.Sprintf("cycles_per_cell        : %d (minimum %d)\n", summary.CyclesPerBoundary, summary.MinimumCyclesPerCell)
	out += fmt.Sprintf("total/expected         : %d / %d\n", summary.TotalCycles, summary.ExpectedCycles)
	out += fmt.Sprintf("passed/failed/skipped  : %d / %d / %d\n", summary.Passed, summary.Failed, summary.Skipped)
	out += fmt.Sprintf("fail_closed_cycles     : %d\n", summary.FailClosedCycles)
	out += fmt.Sprintf("negative/expected      : %d / %d (unreachable downstream fail-closed checks)\n", summary.NegativeCycles, summary.NegativeExpectedCycles)
	out += fmt.Sprintf("elapsed_ms             : %d\n", summary.ElapsedMS)
	out += fmt.Sprintf("max_cycle_duration_ms  : %d\n", summary.MaxCycleDurationMS)
	out += fmt.Sprintf("max_cycle_cpu_ms       : %d\n", summary.MaxCycleCPUms)
	out += fmt.Sprintf("max_state_bytes        : %d\n", summary.MaxDurableStateBytes)
	out += fmt.Sprintf("max_children_per_cycle : %d\n", summary.MaxChildrenPerCycle)
	out += fmt.Sprintf("resource_bounds_ok     : %t\n", summary.ResourceBoundsOK)
	out += fmt.Sprintf("coverage_ok            : %t\n", summary.CoverageOK)
	out += "\nper-fault cycles:\n"
	for _, key := range SortedKeys(summary.PerFault) {
		out += fmt.Sprintf("  %-22s %d\n", key, summary.PerFault[key])
	}
	out += "\nper-applicable-cell cycles:\n"
	for _, key := range SortedKeys(summary.PerCell) {
		out += fmt.Sprintf("  %-58s %d\n", key, summary.PerCell[key])
	}
	out += "\nper-unreachable-negative-cell cycles:\n"
	for _, key := range SortedKeys(summary.PerNegativeCell) {
		out += fmt.Sprintf("  %-58s %d\n", key, summary.PerNegativeCell[key])
	}
	return out
}
