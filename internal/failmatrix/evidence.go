package failmatrix

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// EvidenceWriter streams per-cycle NDJSON evidence and a run summary to disk.
// It never emits comparative "winner" claims; it records only self-referential
// correctness and convergence evidence.
type EvidenceWriter struct {
	dir       string
	cycles    *os.File
	startedAt time.Time
}

// NewEvidenceWriter creates a timestamped run directory under root and opens the
// per-cycle NDJSON stream. The caller must Close the writer.
func NewEvidenceWriter(root string, seed int64) (*EvidenceWriter, error) {
	started := time.Now().UTC()
	dir := filepath.Join(root, fmt.Sprintf("run_%s_seed%d", started.Format("20060102T150405Z"), seed))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create failure-matrix evidence dir: %w", err)
	}
	// #nosec G304 -- evidence path derived from a controlled output root.
	cycles, err := os.Create(filepath.Join(dir, "cycles.ndjson"))
	if err != nil {
		return nil, fmt.Errorf("create cycles.ndjson: %w", err)
	}
	return &EvidenceWriter{dir: dir, cycles: cycles, startedAt: started}, nil
}

// Dir returns the run directory path.
func (w *EvidenceWriter) Dir() string { return w.dir }

// Record appends one cycle result as a single NDJSON line.
func (w *EvidenceWriter) Record(result CycleResult) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal cycle result: %w", err)
	}
	if _, err := w.cycles.Write(append(payload, '\n')); err != nil {
		return fmt.Errorf("write cycle result: %w", err)
	}
	return nil
}

// Finish writes the summary JSON and a human-readable summary, then closes the
// NDJSON stream.
func (w *EvidenceWriter) Finish(summary Summary) error {
	if err := w.cycles.Close(); err != nil {
		return fmt.Errorf("close cycles.ndjson: %w", err)
	}
	summaryPayload, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal summary: %w", err)
	}
	if err := os.WriteFile(filepath.Join(w.dir, "summary.json"), summaryPayload, 0o600); err != nil {
		return fmt.Errorf("write summary.json: %w", err)
	}
	report := w.renderText(summary)
	if err := os.WriteFile(filepath.Join(w.dir, "summary.txt"), []byte(report), 0o600); err != nil {
		return fmt.Errorf("write summary.txt: %w", err)
	}
	return nil
}

// WriteSoakReport writes a soak report as timestamped JSON and text evidence
// under root and returns the run directory.
func WriteSoakReport(root string, report SoakReport) (string, error) {
	started := time.Now().UTC()
	dir := filepath.Join(root, fmt.Sprintf("run_%s_seed%d", started.Format("20060102T150405Z"), report.Seed))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", fmt.Errorf("create soak evidence dir: %w", err)
	}
	payload, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal soak report: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "soak.json"), payload, 0o600); err != nil {
		return "", fmt.Errorf("write soak.json: %w", err)
	}
	text := renderSoakText(report)
	if err := os.WriteFile(filepath.Join(dir, "soak.txt"), []byte(text), 0o600); err != nil {
		return "", fmt.Errorf("write soak.txt: %w", err)
	}
	return dir, nil
}

func renderSoakText(r SoakReport) string {
	out := "WALlaby bounded in-process soak (protocol-fake)\n"
	out += "===============================================\n"
	out += fmt.Sprintf("seed                 : %d\n", r.Seed)
	out += fmt.Sprintf("duration_requested   : %s\n", r.DurationRequested)
	out += fmt.Sprintf("duration_actual_ms   : %d\n", r.DurationActualMS)
	out += fmt.Sprintf("total_cycles         : %d\n", r.TotalCycles)
	out += fmt.Sprintf("passed               : %d\n", r.Passed)
	out += fmt.Sprintf("failed               : %d\n", r.Failed)
	out += fmt.Sprintf("fail_closed_cycles   : %d\n", r.FailClosedCycles)
	out += fmt.Sprintf("goroutine_start/end  : %d / %d (max %d)\n", r.GoroutineStart, r.GoroutineEnd, r.GoroutineMax)
	out += fmt.Sprintf("heap_inuse_start/end : %d / %d (max %d) bytes\n", r.HeapStartBytes, r.HeapEndBytes, r.HeapMaxBytes)
	out += fmt.Sprintf("goroutine_growth_ok  : %t\n", r.GoroutineGrowthOK)
	out += fmt.Sprintf("no_violations        : %t\n", r.NoViolations)
	out += fmt.Sprintf("ok                   : %t\n", r.Ok())
	out += "\nNote: bounded in-process soak of the protocol model. Live-service soak\n"
	out += "for the exact maintained profiles (PostgreSQL, ClickHouse, artifact\n"
	out += "publication, Iceberg REST/MinIO) is a separate opt-in recipe.\n"
	return out
}

func (w *EvidenceWriter) renderText(summary Summary) string {
	elapsed := time.Since(w.startedAt)
	out := "WALlaby deterministic process-failure matrix\n"
	out += "============================================\n"
	out += fmt.Sprintf("started_at         : %s\n", w.startedAt.Format(time.RFC3339))
	out += fmt.Sprintf("elapsed            : %s\n", elapsed.Round(time.Millisecond))
	out += fmt.Sprintf("seed               : %d\n", summary.Seed)
	out += fmt.Sprintf("cycles_per_boundary: %d\n", summary.CyclesPerBoundary)
	out += fmt.Sprintf("total_cycles       : %d\n", summary.TotalCycles)
	out += fmt.Sprintf("passed             : %d\n", summary.Passed)
	out += fmt.Sprintf("failed             : %d\n", summary.Failed)
	out += fmt.Sprintf("fail_closed_cycles : %d\n", summary.FailClosedCycles)
	out += fmt.Sprintf("coverage_ok        : %t\n", summary.CoverageOK)
	out += "\nper-boundary cycles:\n"
	for _, k := range SortedKeys(summary.PerBoundary) {
		out += fmt.Sprintf("  %-22s %d\n", k, summary.PerBoundary[k])
	}
	out += "\nper-profile cycles:\n"
	for _, k := range SortedKeys(summary.PerProfile) {
		out += fmt.Sprintf("  %-30s %d\n", k, summary.PerProfile[k])
	}
	if len(summary.Violations) > 0 {
		out += "\nVIOLATIONS (first 64):\n"
		for _, v := range summary.Violations {
			out += fmt.Sprintf("  cycle=%d profile=%s boundary=%s fault=%s -> %v\n",
				v.Cycle, v.Profile, v.Boundary, v.Fault, v.Violations)
		}
	}
	out += "\nNote: this is an in-process protocol-fake model. It is an executable\n"
	out += "specification, not a substitute for the real local-service integration\n"
	out += "harnesses that provide promotion evidence for the exact maintained\n"
	out += "profiles. Experimental cells exercise the protocol only; live commercial\n"
	out += "cells remain credential-gated and are excluded from promotion evidence.\n"
	return out
}
