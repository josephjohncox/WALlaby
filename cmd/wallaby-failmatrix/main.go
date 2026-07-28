// Command wallaby-failmatrix runs the deterministic in-process process-failure
// matrix and writes machine-readable evidence.
//
// The matrix is a protocol-fake executable specification of WALlaby's durable
// delivery boundary chain. It requires no live services and no credentials, so
// it is safe to run as a bounded, required CI gate. It does not replace the real
// local-service integration harnesses, which remain the promotion evidence for
// the exact maintained profiles, and it never emits comparative winner claims.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/josephjohncox/wallaby/internal/failmatrix"
)

func main() {
	cycles := flag.Int("cycles", 100, "crash cycles per (profile, boundary)")
	seed := flag.Int64("seed", 20260728, "master seed for deterministic replay")
	out := flag.String("out", "bench/evidence/failure_matrix", "evidence output root")
	requireCoverage := flag.Bool("require-coverage", true, "exit non-zero unless every cell reached the target with zero violations")
	soak := flag.Duration("soak", 0, "if > 0, run a bounded in-process soak for this duration instead of the matrix")
	soakOut := flag.String("soak-out", "bench/evidence/soak", "soak evidence output root")
	flag.Parse()

	if *soak > 0 {
		runSoak(*soak, *seed, *soakOut)
		return
	}

	writer, err := failmatrix.NewEvidenceWriter(*out, *seed)
	if err != nil {
		fmt.Fprintln(os.Stderr, "wallaby-failmatrix:", err)
		os.Exit(2)
	}

	var recordErr error
	summary := failmatrix.Run(failmatrix.Config{CyclesPerBoundary: *cycles, Seed: *seed}, func(r failmatrix.CycleResult) {
		if recordErr == nil {
			recordErr = writer.Record(r)
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

	fmt.Printf("failure-matrix evidence: %s\n", writer.Dir())
	fmt.Printf("total=%d passed=%d failed=%d fail_closed=%d coverage_ok=%t\n",
		summary.TotalCycles, summary.Passed, summary.Failed, summary.FailClosedCycles, summary.CoverageOK)

	if *requireCoverage && !summary.CoverageOK {
		fmt.Fprintf(os.Stderr, "wallaby-failmatrix: coverage gate failed (failed=%d)\n", summary.Failed)
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
	fmt.Printf("soak evidence: %s\n", dir)
	fmt.Printf("cycles=%d passed=%d failed=%d goroutines=%d->%d heap_inuse=%d->%d ok=%t\n",
		report.TotalCycles, report.Passed, report.Failed,
		report.GoroutineStart, report.GoroutineEnd,
		report.HeapStartBytes, report.HeapEndBytes, report.Ok())
	if !report.Ok() {
		fmt.Fprintln(os.Stderr, "wallaby-failmatrix: soak gate failed")
		os.Exit(1)
	}
}
