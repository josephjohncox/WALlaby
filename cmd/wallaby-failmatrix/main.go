// Command wallaby-failmatrix runs credential-free OS-process failure evidence
// by default. It precludes destination-proof claims: child processes operate on
// fsync-backed protocol-model state, not live destination implementations.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/josephjohncox/wallaby/internal/failmatrix"
)

func main() {
	cycles := flag.Int("cycles", 100, "cycles per (profile, boundary); OS-process evidence requires >=100")
	seed := flag.Int64("seed", 20260728, "master seed for deterministic replay")
	out := flag.String("out", "bench/evidence/failure_matrix", "evidence output root")
	worker := flag.String("worker", "", "prebuilt wallaby-failmatrix-worker executable (required for OS-process evidence)")
	requireCoverage := flag.Bool("require-coverage", true, "exit non-zero unless every cell reached the target with zero skips or violations")
	modelOnly := flag.Bool("model-only", false, "run the legacy in-process protocol model instead of OS-process evidence")
	cycleTimeout := flag.Duration("cycle-timeout", 5*time.Second, "maximum duration for one OS-process cycle")
	soak := flag.Duration("soak", 0, "if > 0, run the bounded in-process model soak")
	soakOut := flag.String("soak-out", "bench/evidence/soak", "soak evidence output root")
	flag.Parse()

	if *soak > 0 {
		runSoak(*soak, *seed, *soakOut)
		return
	}
	if *modelOnly {
		runModel(*cycles, *seed, *out, *requireCoverage)
		return
	}
	if !*requireCoverage {
		fmt.Fprintln(os.Stderr, "wallaby-failmatrix: OS-process evidence cannot disable the no-skip coverage gate")
		os.Exit(2)
	}
	if *cycles < failmatrix.MinimumProcessCyclesPerCell {
		fmt.Fprintf(os.Stderr, "wallaby-failmatrix: OS-process evidence requires at least %d cycles per cell\n", failmatrix.MinimumProcessCyclesPerCell)
		os.Exit(2)
	}
	if *worker == "" {
		fmt.Fprintln(os.Stderr, "wallaby-failmatrix: -worker must name the prebuilt child executable")
		os.Exit(2)
	}

	writer, err := failmatrix.NewProcessEvidenceWriter(*out, *seed)
	if err != nil {
		fmt.Fprintln(os.Stderr, "wallaby-failmatrix:", err)
		os.Exit(2)
	}
	var recordErr error
	summary := failmatrix.RunProcessMatrix(failmatrix.ProcessConfig{
		WorkerExecutable: *worker, CyclesPerBoundary: *cycles, Seed: *seed,
		CycleTimeout: *cycleTimeout, RequireCoverage: true,
	}, func(result failmatrix.ProcessCycleResult) {
		if recordErr == nil {
			recordErr = writer.Record(result)
		}
	})
	if recordErr != nil {
		fmt.Fprintln(os.Stderr, "wallaby-failmatrix: record:", recordErr)
		os.Exit(2)
	}
	if err := writer.Finish(summary); err != nil {
		fmt.Fprintln(os.Stderr, "wallaby-failmatrix: finish:", err)
		os.Exit(2)
	}
	fmt.Printf("OS-process protocol evidence: %s\n", writer.Dir())
	fmt.Printf("total=%d expected=%d passed=%d failed=%d skipped=%d fail_closed=%d coverage_ok=%t resource_bounds_ok=%t elapsed_ms=%d\n",
		summary.TotalCycles, summary.ExpectedCycles, summary.Passed, summary.Failed, summary.Skipped,
		summary.FailClosedCycles, summary.CoverageOK, summary.ResourceBoundsOK, summary.ElapsedMS)
	if !summary.CoverageOK {
		fmt.Fprintln(os.Stderr, "wallaby-failmatrix: OS-process no-skip/coverage gate failed")
		os.Exit(1)
	}
}

func runModel(cycles int, seed int64, out string, requireCoverage bool) {
	writer, err := failmatrix.NewEvidenceWriter(out, seed)
	if err != nil {
		fmt.Fprintln(os.Stderr, "wallaby-failmatrix model:", err)
		os.Exit(2)
	}
	var recordErr error
	summary := failmatrix.Run(failmatrix.Config{CyclesPerBoundary: cycles, Seed: seed}, func(r failmatrix.CycleResult) {
		if recordErr == nil {
			recordErr = writer.Record(r)
		}
	})
	if recordErr != nil {
		fmt.Fprintln(os.Stderr, "wallaby-failmatrix model record:", recordErr)
		os.Exit(2)
	}
	if err := writer.Finish(summary); err != nil {
		fmt.Fprintln(os.Stderr, "wallaby-failmatrix model finish:", err)
		os.Exit(2)
	}
	fmt.Printf("in-process model evidence: %s\n", writer.Dir())
	fmt.Printf("total=%d passed=%d failed=%d fail_closed=%d coverage_ok=%t\n",
		summary.TotalCycles, summary.Passed, summary.Failed, summary.FailClosedCycles, summary.CoverageOK)
	if requireCoverage && !summary.CoverageOK {
		os.Exit(1)
	}
}

func runSoak(duration time.Duration, seed int64, out string) {
	report := failmatrix.Soak(failmatrix.SoakConfig{Duration: duration, Seed: seed})
	dir, err := failmatrix.WriteSoakReport(out, report)
	if err != nil {
		fmt.Fprintln(os.Stderr, "wallaby-failmatrix: soak:", err)
		os.Exit(2)
	}
	fmt.Printf("in-process model soak evidence: %s\n", dir)
	fmt.Printf("cycles=%d passed=%d failed=%d goroutines=%d->%d heap_inuse=%d->%d ok=%t\n",
		report.TotalCycles, report.Passed, report.Failed,
		report.GoroutineStart, report.GoroutineEnd,
		report.HeapStartBytes, report.HeapEndBytes, report.Ok())
	if !report.Ok() {
		os.Exit(1)
	}
}
