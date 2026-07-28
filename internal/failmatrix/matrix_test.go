package failmatrix

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestSoakBoundedResourceGrowth runs a short bounded soak and asserts there are
// no invariant violations and no goroutine growth (no leak).
func TestSoakBoundedResourceGrowth(t *testing.T) {
	report := Soak(SoakConfig{Duration: 400 * time.Millisecond, Seed: 99, SampleInterval: 50 * time.Millisecond})
	if report.Failed != 0 {
		t.Fatalf("soak failed cycles=%d: %+v", report.Failed, report.Violations)
	}
	if !report.GoroutineGrowthOK {
		t.Fatalf("soak goroutine growth not bounded: start=%d end=%d", report.GoroutineStart, report.GoroutineEnd)
	}
	if report.TotalCycles == 0 {
		t.Fatal("soak executed no cycles")
	}
	if !report.Ok() {
		t.Fatalf("soak not ok: %+v", report)
	}
}

// TestEvidenceWriterRoundTrip asserts the matrix emits machine-readable evidence.
func TestEvidenceWriterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewEvidenceWriter(dir, 42)
	if err != nil {
		t.Fatal(err)
	}
	summary := Run(Config{CyclesPerBoundary: 5, Seed: 42}, func(r CycleResult) {
		if err := writer.Record(r); err != nil {
			t.Fatal(err)
		}
	})
	if err := writer.Finish(summary); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"cycles.ndjson", "summary.json", "summary.txt"} {
		if _, err := os.Stat(filepath.Join(writer.Dir(), name)); err != nil {
			t.Fatalf("expected evidence file %s: %v", name, err)
		}
	}
}

// TestFailureMatrixConvergesAllProfilesBoundaries runs at least 100 crash
// cycles per (profile, boundary) for every supported protocol boundary and
// asserts that every cycle satisfies the standing safety invariants. This is
// the core deterministic process-failure matrix requirement.
func TestFailureMatrixConvergesAllProfilesBoundaries(t *testing.T) {
	cfg := Config{CyclesPerBoundary: 100, Seed: 20260728}
	var recorded int
	summary := Run(cfg, func(r CycleResult) {
		recorded++
		if !r.Ok() {
			t.Errorf("cycle %d profile=%s boundary=%s fault=%s violated: %v",
				r.Cycle, r.Profile, r.Boundary, r.Fault, r.Violations)
		}
	})
	if summary.Failed != 0 {
		t.Fatalf("matrix failed cycles=%d, want 0 (violations: %+v)", summary.Failed, summary.Violations)
	}
	if !summary.CoverageOK {
		t.Fatalf("matrix coverage not ok: %+v", summary)
	}
	if recorded != summary.TotalCycles {
		t.Fatalf("recorded=%d != total=%d", recorded, summary.TotalCycles)
	}
	// No-skip accounting: every (profile, boundary) cell must reach the target.
	profiles := SupportedProfiles()
	wantPerBoundary := cfg.CyclesPerBoundary * len(profiles)
	for _, b := range RequiredBoundaries() {
		if got := summary.PerBoundary[string(b)]; got != wantPerBoundary {
			t.Fatalf("boundary %s cycles=%d, want %d (no-skip accounting)", b, got, wantPerBoundary)
		}
	}
	wantPerProfile := cfg.CyclesPerBoundary * len(RequiredBoundaries())
	for _, p := range profiles {
		if got := summary.PerProfile[p.Name]; got != wantPerProfile {
			t.Fatalf("profile %s cycles=%d, want %d (no-skip accounting)", p.Name, got, wantPerProfile)
		}
	}
	t.Logf("matrix: total=%d passed=%d fail_closed=%d", summary.TotalCycles, summary.Passed, summary.FailClosedCycles)
}

// TestFailureMatrixDeterministic asserts the matrix is reproducible: the same
// seed yields byte-identical evidence.
func TestFailureMatrixDeterministic(t *testing.T) {
	cfg := Config{CyclesPerBoundary: 25, Seed: 7}
	var first, second []CycleResult
	Run(cfg, func(r CycleResult) { first = append(first, r) })
	Run(cfg, func(r CycleResult) { second = append(second, r) })
	if len(first) != len(second) {
		t.Fatalf("cycle count differs: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if !equalResult(first[i], second[i]) {
			t.Fatalf("cycle %d not deterministic:\n a=%+v\n b=%+v", i, first[i], second[i])
		}
	}
}

func equalResult(a, b CycleResult) bool {
	if a.Cycle != b.Cycle || a.Seed != b.Seed || a.Profile != b.Profile || a.Boundary != b.Boundary ||
		a.Fault != b.Fault || a.Injected != b.Injected || a.Recovered != b.Recovered ||
		a.Converged != b.Converged || a.FailClosed != b.FailClosed ||
		a.CheckpointLSN != b.CheckpointLSN || a.SourceFlushLSN != b.SourceFlushLSN {
		return false
	}
	if len(a.Violations) != len(b.Violations) {
		return false
	}
	return true
}

// TestFailureMatrixStreamingFailClosed asserts the unlinked streaming profile
// never advances any durable state at any boundary. This is the fail-closed
// guarantee, not a defect.
func TestFailureMatrixStreamingFailClosed(t *testing.T) {
	var profile Profile
	for _, p := range SupportedProfiles() {
		if p.Name == "snowpipe-streaming-v1" {
			profile = p
		}
	}
	if profile.Name == "" {
		t.Fatal("expected an unlinked snowpipe-streaming-v1 profile")
	}
	for _, boundary := range RequiredBoundaries() {
		for _, fault := range []FaultKind{FaultKill, FaultRestart, FaultOverlappingTakeover} {
			r := RunCycle(0, 1, profile, boundary, fault)
			if !r.Ok() {
				t.Fatalf("streaming fail-closed boundary=%s fault=%s violated: %v", boundary, fault, r.Violations)
			}
			if !r.FailClosed {
				t.Fatalf("streaming boundary=%s fault=%s expected fail-closed", boundary, fault)
			}
			if r.CheckpointLSN != 0 || r.Adoptions != 0 {
				t.Fatalf("streaming boundary=%s advanced durable state: checkpoint=%d adoptions=%d", boundary, r.CheckpointLSN, r.Adoptions)
			}
		}
	}
}

// TestFailureMatrixOverlappingTakeoverRejectsStaleWorker asserts a fenced
// (crashed) worker can never commit after an overlapping takeover.
func TestFailureMatrixOverlappingTakeoverRejectsStaleWorker(t *testing.T) {
	for _, profile := range SupportedProfiles() {
		if isStreaming(profile) && !profile.StreamingTransportLinked {
			continue
		}
		for _, boundary := range RequiredBoundaries() {
			r := RunCycle(0, 3, profile, boundary, FaultOverlappingTakeover)
			if !r.StaleRejected {
				t.Fatalf("profile=%s boundary=%s stale worker was not rejected", profile.Name, boundary)
			}
			if !r.Ok() {
				t.Fatalf("profile=%s boundary=%s violated: %v", profile.Name, boundary, r.Violations)
			}
		}
	}
}

// TestInvariantsAreNonVacuous proves that checkInvariants actually detects
// protocol violations, so a green matrix is meaningful rather than decorative.
func TestInvariantsAreNonVacuous(t *testing.T) {
	base := Profile{Name: "postgres-to-postgres-v1", Kind: Maintained, Visibility: Synchronous, StreamingTransportLinked: true}

	cases := []struct {
		name    string
		mutate  func(e *engine)
		wantSub string
	}{
		{
			name:    "receipt without side effect",
			mutate:  func(e *engine) { e.auth.receiptAdopted = true; e.dest.committed = false },
			wantSub: "receipt_without_side_effect",
		},
		{
			name:    "checkpoint without receipt",
			mutate:  func(e *engine) { e.auth.checkpoint = positionLSN; e.auth.receiptAdopted = false },
			wantSub: "checkpoint_without_receipt",
		},
		{
			name: "ack before checkpoint",
			mutate: func(e *engine) {
				e.auth.receiptAdopted = true
				e.dest.committed = true
				e.auth.ackIntent = true
				e.auth.checkpoint = 0
			},
			wantSub: "ack_before_checkpoint",
		},
		{
			name: "source flush exceeds checkpoint",
			mutate: func(e *engine) {
				e.auth.receiptAdopted = true
				e.dest.committed = true
				e.auth.checkpoint = positionLSN
				e.auth.sourceFlushLSN = positionLSN + 5
			},
			wantSub: "source_flush_exceeds_checkpoint",
		},
		{
			name: "retention released without preconditions",
			mutate: func(e *engine) {
				e.auth.retentionReleased = true
			},
			wantSub: "retention_released_without_preconditions",
		},
		{
			name: "gc finalized active root",
			mutate: func(e *engine) {
				e.auth.gcFinalized = true
				e.auth.retentionReleased = false
			},
			wantSub: "gc_finalized_active_root",
		},
		{
			name: "double adoption",
			mutate: func(e *engine) {
				e.dest.committed = true
				e.auth.receiptAdopted = true
				e.auth.adoptionCount = 2
			},
			wantSub: "adoption_not_unique",
		},
		{
			name: "consumer receipt without publication",
			mutate: func(e *engine) {
				e.auth.consumerReceipt = true
				e.auth.publication = false
			},
			wantSub: "consumer_receipt_without_publication",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEngine(base)
			tc.mutate(e)
			violations := e.checkInvariants(CycleResult{Fault: string(FaultKill)})
			if !containsSub(violations, tc.wantSub) {
				t.Fatalf("expected violation containing %q, got %v", tc.wantSub, violations)
			}
		})
	}
}

// TestStaleFenceDetectionIsNonVacuous confirms that a broken fence (stale
// worker allowed to commit) is caught by the overlapping-takeover path.
func TestStaleFenceDetectionIsNonVacuous(t *testing.T) {
	e := newEngine(Profile{Name: "x", Kind: Maintained, Visibility: Synchronous, StreamingTransportLinked: true})
	// Do NOT bump the lease epoch, so the "stale" epoch 1 still matches the
	// authoritative epoch 1 and the mutation is (incorrectly) allowed. The
	// detector must report this as a broken fence.
	if e.staleFencedMutationRejected(1) {
		t.Fatal("expected staleFencedMutationRejected to report a broken fence when epochs match")
	}
	// After a real takeover the stale epoch no longer matches and is rejected.
	e.auth.leaseEpoch = 2
	if !e.staleFencedMutationRejected(1) {
		t.Fatal("expected stale epoch 1 to be rejected after takeover to epoch 2")
	}
}

func containsSub(violations []string, sub string) bool {
	for _, v := range violations {
		if len(sub) > 0 && contains(v, sub) {
			return true
		}
	}
	return false
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
