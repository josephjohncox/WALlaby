package failmatrix

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func buildProcessWorker(t *testing.T) string {
	t.Helper()
	if path := os.Getenv("WALLABY_FAILMATRIX_WORKER"); path != "" {
		return path
	}
	path := filepath.Join(t.TempDir(), "wallaby-failmatrix-worker")
	cmd := exec.Command("go", "build", "-o", path, "../../cmd/wallaby-failmatrix-worker")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build process worker: %v\n%s", err, output)
	}
	return path
}

func TestOSProcessFaultsPersistRecoverAndFence(t *testing.T) {
	worker := buildProcessWorker(t)
	profile, ok := supportedProfile("postgres-to-postgres-v1")
	if !ok {
		t.Fatal("postgres profile missing")
	}
	for i, fault := range []FaultKind{FaultKill, FaultRestart, FaultOverlappingTakeover} {
		t.Run(string(fault), func(t *testing.T) {
			result := RunProcessCycle(ProcessCycleConfig{
				WorkerExecutable: worker,
				CycleDir:         filepath.Join(t.TempDir(), "cycle"),
				Cycle:            i,
				Seed:             int64(100 + i),
				Profile:          profile,
				Boundary:         BoundaryAfterSideEffect,
				Fault:            fault,
				Timeout:          5 * time.Second,
			})
			if !result.Ok() {
				t.Fatalf("process cycle violated: %+v", result)
			}
			if result.InitialPID <= 0 || result.RecoveryPID <= 0 || result.InitialPID == result.RecoveryPID {
				t.Fatalf("expected distinct real child PIDs: %+v", result.ChildPIDs)
			}
			if result.DurableWrites < 2 || result.FinalStateSHA256 == "" {
				t.Fatalf("durable state evidence missing: writes=%d hash=%q", result.DurableWrites, result.FinalStateSHA256)
			}
			serialized, err := json.Marshal(result.FinalState)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(serialized)
			if got := hex.EncodeToString(digest[:]); got != result.FinalStateSHA256 {
				t.Fatalf("final evidence hash=%s, recomputed from FinalState=%s", result.FinalStateSHA256, got)
			}
			if fault == FaultKill && !result.SIGKILLObserved {
				t.Fatal("SIGKILL was not observed")
			}
			if result.ScheduleHash == "" || result.Schedule == "" {
				t.Fatalf("seed-derived schedule evidence missing: %+v", result)
			}
			if fault == FaultOverlappingTakeover {
				if !result.OverlapObserved || !result.StaleRejected || !result.StaleMutationAttempted || !result.StaleStateUnchanged {
					t.Fatalf("overlap/fence evidence missing: %+v", result)
				}
				if result.StaleRevisionBefore != result.StaleRevisionAfter || result.StaleSHA256Before == "" || result.StaleSHA256Before != result.StaleSHA256After {
					t.Fatalf("stale mutation changed durable state: %+v", result)
				}
			}
		})
	}
}

func TestOSProcessUnlinkedStreamingFailsClosed(t *testing.T) {
	worker := buildProcessWorker(t)
	profile, ok := supportedProfile("snowpipe-streaming-v1")
	if !ok {
		t.Fatal("unlinked streaming profile missing")
	}
	result := RunProcessCycle(ProcessCycleConfig{
		WorkerExecutable: worker,
		CycleDir:         filepath.Join(t.TempDir(), "cycle"),
		Profile:          profile,
		Boundary:         BoundaryGC,
		Fault:            FaultKill,
		Timeout:          5 * time.Second,
	})
	if !result.Ok() {
		t.Fatalf("fail-closed process cycle violated: %+v", result)
	}
	if !result.FailClosed || result.BoundaryReached {
		t.Fatalf("expected explicit pre-boundary fail-closed accounting: %+v", result)
	}
	a := result.FinalState.Authority
	d := result.FinalState.Destination
	if a.AttemptPrepared || a.ReceiptAdopted || a.Checkpoint != 0 || a.AckIntent || a.SourceFlushLSN != 0 || a.FlushReceipt || a.Publication || a.ObjectVersion != 0 || a.ConsumerReceipt || a.RetentionReleased || a.GCMarked || a.GCFinalized || a.ExternalApplyCount != 0 || a.AdoptionCount != 0 || d.Committed || d.Reveal != 0 || d.ReceiptVisible || d.Version != 0 || d.ApplyAttempts != 0 || result.FinalState.ObjectSeq != 0 || result.FinalState.ConfirmLSN != 0 {
		t.Fatalf("unlinked streaming advanced durable delivery state: %+v", result.FinalState)
	}
}

func TestSeedDerivesStableDistinctExecutionSchedules(t *testing.T) {
	worker := buildProcessWorker(t)
	profile, _ := supportedProfile("postgres-to-postgres-v1")
	for _, fault := range []FaultKind{FaultKill, FaultRestart, FaultOverlappingTakeover} {
		first := runScheduledTestCycle(t, worker, profile, fault, 77)
		again := runScheduledTestCycle(t, worker, profile, fault, 77)
		if first.ScheduleHash != again.ScheduleHash || first.ObservedEventsHash != again.ObservedEventsHash || !equalStrings(first.ObservedEvents, again.ObservedEvents) || !reflect.DeepEqual(first.ObservedRevisions, again.ObservedRevisions) {
			t.Fatalf("same seed produced different observed %s ordering:\n%+v\n%+v", fault, first, again)
		}
		assertObservedRevisionSequence(t, first)
		different := false
		for seed := int64(78); seed < 128; seed++ {
			candidate := runScheduledTestCycle(t, worker, profile, fault, seed)
			assertObservedRevisionSequence(t, candidate)
			if candidate.ObservedEventsHash != first.ObservedEventsHash && !reflect.DeepEqual(candidate.ObservedRevisions, first.ObservedRevisions) {
				different = true
				break
			}
		}
		if !different {
			t.Fatalf("different seeds did not exercise a distinct actual %s handshake ordering", fault)
		}
	}
}

func assertObservedRevisionSequence(t *testing.T, result ProcessCycleResult) {
	t.Helper()
	expected, err := expectedProcessScheduleFor(FaultKind(result.Fault), processSchedule{Variant: result.Schedule})
	if err != nil {
		t.Fatal(err)
	}
	if result.FinalState.Generation != expected.Generation || result.FinalState.Authority.LeaseEpoch != expected.LeaseEpoch || result.FinalState.Revision != expected.Revision || result.FinalState.DurableWrites != expected.DurableWrites || result.DurableWrites != expected.DurableWrites {
		t.Fatalf("final attestation=%d/%d/%d/%d, want generation/lease/revision/writes=%d/%d/%d/%d", result.FinalState.Generation, result.FinalState.Authority.LeaseEpoch, result.FinalState.Revision, result.DurableWrites, expected.Generation, expected.LeaseEpoch, expected.Revision, expected.DurableWrites)
	}
	if len(result.ObservedEvents) != len(expected.Operations) || len(result.ObservedRevisions) != len(expected.Operations) {
		t.Fatalf("observed operation count=%d/%d, want %d: events=%v revisions=%+v", len(result.ObservedEvents), len(result.ObservedRevisions), len(expected.Operations), result.ObservedEvents, result.ObservedRevisions)
	}
	for index, operation := range expected.Operations {
		observation := result.ObservedRevisions[index]
		if result.ObservedEvents[index] != operation.Operation || observation.Order != index+1 || observation.Operation != operation.Operation || observation.Generation != operation.Generation || observation.Revision != operation.Revision || observation.SHA256 == "" {
			t.Fatalf("durable observation %d=%+v event=%q, want %+v", index, observation, result.ObservedEvents[index], operation)
		}
	}
}

func TestExpectedProcessScheduleMatchesHandshakeOperations(t *testing.T) {
	for _, fault := range []FaultKind{FaultKill, FaultRestart, FaultOverlappingTakeover} {
		for _, variant := range processScheduleVariants {
			expected, err := expectedProcessScheduleFor(fault, processSchedule{Variant: variant})
			if err != nil {
				t.Fatal(err)
			}
			transition := "recovery"
			wantGeneration := int64(1)
			if fault == FaultOverlappingTakeover {
				transition = "takeover"
				wantGeneration = 2
			}
			wantNames := []string{"initial:child_persist"}
			if variant == "probe_before_transition" || variant == "probe_both_sides" {
				wantNames = append(wantNames, transition+":parent_probe_before")
			}
			wantNames = append(wantNames, transition+":child_transition")
			if variant == "probe_after_transition" || variant == "probe_both_sides" {
				wantNames = append(wantNames, transition+":parent_probe_after")
			}
			if fault == FaultOverlappingTakeover {
				wantNames = append(wantNames, "takeover:child_final_recovery")
			}
			var gotNames []string
			for index, operation := range expected.Operations {
				gotNames = append(gotNames, operation.Operation)
				if operation.Revision != int64(index+1) {
					t.Fatalf("%s/%s operation %d revision=%d, want %d", fault, variant, index, operation.Revision, index+1)
				}
			}
			if !reflect.DeepEqual(gotNames, wantNames) || expected.Generation != wantGeneration || expected.LeaseEpoch != wantGeneration || expected.Revision != int64(len(wantNames)) || expected.DurableWrites != len(wantNames) {
				t.Fatalf("%s/%s expected schedule=%+v names=%v, want generation=%d names=%v", fault, variant, expected, gotNames, wantGeneration, wantNames)
			}
		}
	}
}

func runScheduledTestCycle(t *testing.T, worker string, profile Profile, fault FaultKind, seed int64) ProcessCycleResult {
	t.Helper()
	result := RunProcessCycle(ProcessCycleConfig{
		WorkerExecutable: worker, CycleDir: filepath.Join(t.TempDir(), "cycle"),
		Seed: seed, Profile: profile, Boundary: BoundaryCheckpoint, Fault: fault,
		Timeout: 5 * time.Second,
	})
	if !result.Ok() {
		t.Fatalf("scheduled process cycle failed: %+v", result)
	}
	return result
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func TestNormalizedProcessEvidenceLiteralBytes(t *testing.T) {
	result := ProcessCycleResult{
		EvidenceType: "os_process_protocol_evidence", Cycle: 4, Seed: 9,
		Profile: "p", Kind: "experimental", Boundary: "checkpoint", Fault: "kill",
		Schedule: "validate_before_recovery", ScheduleHash: "abc",
		InitialPID: 111, RecoveryPID: 222, ChildPIDs: []int{111, 222}, DurationMS: 99,
		ChildUserCPUms: 12, ChildSystemCPUms: 13,
	}
	got, err := normalizedProcessCycleBytes(result)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(got, []byte(`"initial_pid"`)) || bytes.Contains(got, []byte(`"duration_ms"`)) || !bytes.Contains(got, []byte(`"final_state"`)) {
		t.Fatalf("normalized evidence contains nondeterminism or omits final state: %s", got)
	}
	result.InitialPID = 333
	result.RecoveryPID = 444
	result.DurationMS = 1000
	again, err := normalizedProcessCycleBytes(result)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, again) {
		t.Fatalf("raw PID/timing changed normalized bytes:\n%s\n%s", got, again)
	}
}

func TestOSProcessMatrixEndToEndNormalizedBytesReproduce(t *testing.T) {
	worker := buildProcessWorker(t)
	profile, _ := supportedProfile("postgres-to-postgres-v1")
	run := func(root string) (normalized, summary []byte) {
		writer, err := NewProcessEvidenceWriter(root, 8181)
		if err != nil {
			t.Fatal(err)
		}
		matrixSummary := RunProcessMatrix(ProcessConfig{
			WorkerExecutable: worker, WorkRoot: filepath.Join(root, "work"),
			CyclesPerBoundary: 3, Seed: 8181, Profiles: []Profile{profile},
			Boundaries: []Boundary{BoundaryCheckpoint}, CycleTimeout: 5 * time.Second,
		}, func(result ProcessCycleResult) {
			if err := writer.Record(result); err != nil {
				t.Fatal(err)
			}
		})
		if !matrixSummary.CoverageOK {
			t.Fatalf("small real process matrix failed: %+v", matrixSummary)
		}
		if err := writer.Finish(matrixSummary); err != nil {
			t.Fatal(err)
		}
		normalized, err = os.ReadFile(filepath.Join(writer.Dir(), "normalized.ndjson"))
		if err != nil {
			t.Fatal(err)
		}
		summary, err = os.ReadFile(filepath.Join(writer.Dir(), "normalized-summary.json"))
		if err != nil {
			t.Fatal(err)
		}
		return normalized, summary
	}
	firstCycles, firstSummary := run(t.TempDir())
	secondCycles, secondSummary := run(t.TempDir())
	if !bytes.Equal(firstCycles, secondCycles) {
		t.Fatalf("normalized real-process NDJSON differs:\n%s\n%s", firstCycles, secondCycles)
	}
	if !bytes.Equal(firstSummary, secondSummary) {
		t.Fatalf("normalized real-process summaries differ:\n%s\n%s", firstSummary, secondSummary)
	}
}

func TestProcessCycleReapsFailingAndTimedOutChildren(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process evidence uses Unix signals and flock")
	}
	profile, _ := supportedProfile("postgres-to-postgres-v1")
	cases := []struct {
		name    string
		script  string
		timeout time.Duration
	}{
		{name: "failing child", script: "#!/bin/sh\nexit 23\n", timeout: 100 * time.Millisecond},
		{name: "timed out child", script: "#!/bin/sh\nwhile :; do :; done\n", timeout: 50 * time.Millisecond},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			worker := filepath.Join(t.TempDir(), "worker")
			if err := os.WriteFile(worker, []byte(test.script), 0o700); err != nil {
				t.Fatal(err)
			}
			result := RunProcessCycle(ProcessCycleConfig{
				WorkerExecutable: worker, CycleDir: filepath.Join(t.TempDir(), "cycle"),
				Profile: profile, Boundary: BoundaryCheckpoint, Fault: FaultKill,
				Timeout: test.timeout,
			})
			if result.Ok() || len(result.ChildPIDs) == 0 {
				t.Fatalf("expected failing cycle with a started child: %+v", result)
			}
			for _, pid := range result.ChildPIDs {
				process, err := os.FindProcess(pid)
				if err != nil {
					t.Fatal(err)
				}
				if processAlive(process) {
					t.Fatalf("child PID %d remains alive/unreaped after error return", pid)
				}
			}
		})
	}

	t.Run("failing recovery child", func(t *testing.T) {
		realWorker := buildProcessWorker(t)
		t.Setenv("WALLABY_REAL_FAILMATRIX_WORKER", realWorker)
		wrapper := filepath.Join(t.TempDir(), "worker")
		script := "#!/bin/sh\nif [ \"$2\" = \"recover\" ]; then exit 23; fi\nexec \"$WALLABY_REAL_FAILMATRIX_WORKER\" \"$@\"\n"
		if err := os.WriteFile(wrapper, []byte(script), 0o700); err != nil {
			t.Fatal(err)
		}
		result := RunProcessCycle(ProcessCycleConfig{
			WorkerExecutable: wrapper, CycleDir: filepath.Join(t.TempDir(), "cycle"),
			Profile: profile, Boundary: BoundaryCheckpoint, Fault: FaultKill,
			Timeout: 200 * time.Millisecond,
		})
		if result.Ok() || len(result.ChildPIDs) != 2 {
			t.Fatalf("expected initial and failing recovery children: %+v", result)
		}
		for _, pid := range result.ChildPIDs {
			process, err := os.FindProcess(pid)
			if err != nil {
				t.Fatal(err)
			}
			if processAlive(process) {
				t.Fatalf("child PID %d remains alive/unreaped after recovery failure", pid)
			}
		}
	})
}

func TestProcessExactAttestationPredicateBranchesAreNonVacuous(t *testing.T) {
	profile, _ := supportedProfile("postgres-to-postgres-v1")
	boundary := BoundaryCheckpoint
	fault := FaultOverlappingTakeover
	base := validProcessPredicateFixture(t, profile, boundary, fault, "probe_both_sides")
	if violations := checkProcessResultPredicates(base, profile, boundary, fault); len(violations) != 0 {
		t.Fatalf("valid exact attestation fixture violated: %v", violations)
	}
	cases := []struct {
		name   string
		mutate func(*ProcessCycleResult)
		want   string
	}{
		{name: "fault identity", mutate: func(r *ProcessCycleResult) { r.Fault = string(FaultRestart) }, want: "result fault"},
		{name: "seed-derived schedule", mutate: func(r *ProcessCycleResult) { r.Schedule = "probe_before_transition" }, want: "seed-derived schedule"},
		{name: "schedule hash", mutate: func(r *ProcessCycleResult) { r.ScheduleHash = "corrupt" }, want: "schedule hash"},
		{name: "generation", mutate: func(r *ProcessCycleResult) { r.FinalState.Generation++ }, want: "final generation"},
		{name: "lease epoch", mutate: func(r *ProcessCycleResult) { r.FinalState.Authority.LeaseEpoch++ }, want: "final lease epoch"},
		{name: "revision", mutate: func(r *ProcessCycleResult) { r.FinalState.Revision++ }, want: "final revision"},
		{name: "durable writes", mutate: func(r *ProcessCycleResult) { r.FinalState.DurableWrites++; r.DurableWrites++ }, want: "durable writes"},
		{name: "missing observation", mutate: func(r *ProcessCycleResult) {
			r.ObservedEvents = r.ObservedEvents[:len(r.ObservedEvents)-1]
			r.ObservedRevisions = r.ObservedRevisions[:len(r.ObservedRevisions)-1]
		}, want: "durable observation count"},
		{name: "reordered observation", mutate: func(r *ProcessCycleResult) {
			r.ObservedEvents[1], r.ObservedEvents[2] = r.ObservedEvents[2], r.ObservedEvents[1]
			r.ObservedRevisions[1], r.ObservedRevisions[2] = r.ObservedRevisions[2], r.ObservedRevisions[1]
		}, want: "durable observation 2"},
		{name: "wrong observation revision", mutate: func(r *ProcessCycleResult) { r.ObservedRevisions[2].Revision++ }, want: "durable observation 3"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneProcessCycleResult(base)
			test.mutate(&candidate)
			if violations := checkProcessResultPredicates(candidate, profile, boundary, fault); !containsSub(violations, test.want) {
				t.Fatalf("mutation did not trigger %q: %v", test.want, violations)
			}
		})
	}
}

func TestProcessNegativePassingPredicateBranchesAreNonVacuous(t *testing.T) {
	profile, _ := supportedProfile("snowpipe-streaming-v1")
	boundary := BoundaryGC
	base := validProcessPredicateFixture(t, profile, boundary, FaultKill, "probe_both_sides")
	if violations := checkProcessResultPredicates(base, profile, boundary, FaultKill); len(violations) != 0 {
		t.Fatalf("valid negative predicate fixture violated: %v", violations)
	}
	cases := []struct {
		name   string
		mutate func(*ProcessCycleResult)
		want   string
	}{
		{name: "fault identity", mutate: func(r *ProcessCycleResult) { r.Fault = string(FaultRestart) }, want: "result fault"},
		{name: "seed-derived schedule", mutate: func(r *ProcessCycleResult) { r.Schedule = "probe_before_transition" }, want: "seed-derived schedule"},
		{name: "schedule hash", mutate: func(r *ProcessCycleResult) { r.ScheduleHash = "corrupt" }, want: "schedule hash"},
		{name: "recovered", mutate: func(r *ProcessCycleResult) { r.Recovered = true }, want: "reported recovery completion"},
		{name: "boundary", mutate: func(r *ProcessCycleResult) { r.BoundaryReached, r.FinalState.BoundaryReached = true, true }, want: "reached an inapplicable boundary"},
		{name: "phase", mutate: func(r *ProcessCycleResult) { r.FinalState.Phase = processPhaseComplete }, want: "final phase is not fail_closed"},
		{name: "hash", mutate: func(r *ProcessCycleResult) { r.FinalStateSHA256 = "" }, want: "final state hash is empty"},
		{name: "generation", mutate: func(r *ProcessCycleResult) { r.FinalState.Generation++ }, want: "final generation"},
		{name: "lease epoch", mutate: func(r *ProcessCycleResult) { r.FinalState.Authority.LeaseEpoch++ }, want: "final lease epoch"},
		{name: "revision", mutate: func(r *ProcessCycleResult) { r.FinalState.Revision++ }, want: "final revision"},
		{name: "durable writes", mutate: func(r *ProcessCycleResult) { r.FinalState.DurableWrites++; r.DurableWrites++ }, want: "durable writes"},
		{name: "missing observation", mutate: func(r *ProcessCycleResult) {
			r.ObservedEvents = r.ObservedEvents[:len(r.ObservedEvents)-1]
			r.ObservedRevisions = r.ObservedRevisions[:len(r.ObservedRevisions)-1]
		}, want: "durable observation count"},
		{name: "reordered observation", mutate: func(r *ProcessCycleResult) {
			r.ObservedEvents[1], r.ObservedEvents[2] = r.ObservedEvents[2], r.ObservedEvents[1]
			r.ObservedRevisions[1], r.ObservedRevisions[2] = r.ObservedRevisions[2], r.ObservedRevisions[1]
		}, want: "durable observation 2"},
		{name: "wrong observation revision", mutate: func(r *ProcessCycleResult) { r.ObservedRevisions[2].Revision++ }, want: "durable observation 3"},
		{name: "attempt prepared", mutate: func(r *ProcessCycleResult) { r.FinalState.Authority.AttemptPrepared = true }, want: "attempt_prepared"},
		{name: "receipt adopted", mutate: func(r *ProcessCycleResult) { r.FinalState.Authority.ReceiptAdopted = true }, want: "receipt_adopted"},
		{name: "checkpoint", mutate: func(r *ProcessCycleResult) { r.FinalState.Authority.Checkpoint = 1 }, want: "checkpoint"},
		{name: "ack intent", mutate: func(r *ProcessCycleResult) { r.FinalState.Authority.AckIntent = true }, want: "ack_intent"},
		{name: "source flush", mutate: func(r *ProcessCycleResult) { r.FinalState.Authority.SourceFlushLSN = 1 }, want: "source_flush_lsn"},
		{name: "flush receipt", mutate: func(r *ProcessCycleResult) { r.FinalState.Authority.FlushReceipt = true }, want: "flush_receipt"},
		{name: "publication", mutate: func(r *ProcessCycleResult) { r.FinalState.Authority.Publication = true }, want: "publication"},
		{name: "object version", mutate: func(r *ProcessCycleResult) { r.FinalState.Authority.ObjectVersion = 1 }, want: "object_version"},
		{name: "consumer receipt", mutate: func(r *ProcessCycleResult) { r.FinalState.Authority.ConsumerReceipt = true }, want: "consumer_receipt"},
		{name: "retention", mutate: func(r *ProcessCycleResult) { r.FinalState.Authority.RetentionReleased = true }, want: "retention_released"},
		{name: "gc marked", mutate: func(r *ProcessCycleResult) { r.FinalState.Authority.GCMarked = true }, want: "gc_marked"},
		{name: "gc finalized", mutate: func(r *ProcessCycleResult) { r.FinalState.Authority.GCFinalized = true }, want: "gc_finalized"},
		{name: "external applies", mutate: func(r *ProcessCycleResult) { r.FinalState.Authority.ExternalApplyCount, r.ExternalApplies = 1, 1 }, want: "external_apply_count"},
		{name: "adoptions", mutate: func(r *ProcessCycleResult) { r.FinalState.Authority.AdoptionCount, r.Adoptions = 1, 1 }, want: "adoption_count"},
		{name: "destination committed", mutate: func(r *ProcessCycleResult) { r.FinalState.Destination.Committed = true }, want: "destination_committed"},
		{name: "destination reveal", mutate: func(r *ProcessCycleResult) { r.FinalState.Destination.Reveal = 1 }, want: "destination_reveal"},
		{name: "destination receipt", mutate: func(r *ProcessCycleResult) { r.FinalState.Destination.ReceiptVisible = true }, want: "destination_receipt_visible"},
		{name: "destination version", mutate: func(r *ProcessCycleResult) { r.FinalState.Destination.Version = 1 }, want: "destination_version"},
		{name: "destination attempts", mutate: func(r *ProcessCycleResult) { r.FinalState.Destination.ApplyAttempts = 1 }, want: "destination_apply_attempts"},
		{name: "object sequence", mutate: func(r *ProcessCycleResult) { r.FinalState.ObjectSeq = 1 }, want: "object_seq"},
		{name: "confirm LSN", mutate: func(r *ProcessCycleResult) { r.FinalState.ConfirmLSN = 1 }, want: "confirm_lsn"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneProcessCycleResult(base)
			test.mutate(&candidate)
			if violations := checkProcessResultPredicates(candidate, profile, boundary, FaultKill); !containsSub(violations, test.want) {
				t.Fatalf("mutation did not trigger %q: %v", test.want, violations)
			}
		})
	}
}

func validProcessPredicateFixture(t *testing.T, profile Profile, boundary Boundary, fault FaultKind, variant string) ProcessCycleResult {
	t.Helper()
	seed := int64(0)
	for processScheduleFor(seed, fault).Variant != variant {
		seed++
	}
	schedule := processScheduleFor(seed, fault)
	expected, err := expectedProcessScheduleFor(fault, schedule)
	if err != nil {
		t.Fatal(err)
	}
	e := newEngine(profile)
	reached, failClosed := e.runToBoundary(1, boundaryStage(boundary))
	if fault == FaultOverlappingTakeover {
		e.auth.leaseEpoch = expected.Generation
	}
	completed, recoveredFailClosed := e.recover(expected.Generation)
	phase := processPhaseComplete
	if failClosed || recoveredFailClosed {
		phase = processPhaseFailClosed
	}
	state := stateFromEngine(e, boundary, reached, failClosed || recoveredFailClosed, phase, expected.DurableWrites)
	state.Revision = expected.Revision
	result := ProcessCycleResult{
		Seed: seed, Profile: profile.Name, Boundary: string(boundary), Fault: string(fault),
		Schedule: schedule.Variant, ScheduleHash: schedule.hash(),
		BoundaryReached: reached, FailClosed: failClosed || recoveredFailClosed, Recovered: completed,
		ExternalApplies: state.Authority.ExternalApplyCount, Adoptions: state.Authority.AdoptionCount,
		CheckpointLSN: state.Authority.Checkpoint, SourceFlushLSN: state.Authority.SourceFlushLSN,
		DurableWrites: expected.DurableWrites, FinalStateSHA256: "nonempty", FinalState: state,
	}
	for index, operation := range expected.Operations {
		result.ObservedEvents = append(result.ObservedEvents, operation.Operation)
		result.ObservedRevisions = append(result.ObservedRevisions, durableRevisionObservation{
			Order: index + 1, Operation: operation.Operation, Generation: operation.Generation,
			Revision: operation.Revision, SHA256: fmt.Sprintf("sha-%d", index+1),
		})
	}
	return result
}

func cloneProcessCycleResult(result ProcessCycleResult) ProcessCycleResult {
	result.ObservedEvents = append([]string(nil), result.ObservedEvents...)
	result.ObservedRevisions = append([]durableRevisionObservation(nil), result.ObservedRevisions...)
	return result
}

func TestOSProcessMatrixNoVacuityAccounting(t *testing.T) {
	worker := buildProcessWorker(t)
	profile, _ := supportedProfile("postgres-to-postgres-v1")
	summary := RunProcessMatrix(ProcessConfig{
		WorkerExecutable:  worker,
		WorkRoot:          t.TempDir(),
		CyclesPerBoundary: 3,
		Seed:              77,
		Profiles:          []Profile{profile},
		Boundaries:        []Boundary{BoundaryCheckpoint},
		CycleTimeout:      5 * time.Second,
	}, nil)
	if !summary.CoverageOK || summary.TotalCycles != 3 || summary.ExpectedCycles != 3 || summary.Skipped != 0 || summary.PlanSHA256 == "" {
		t.Fatalf("unexpected no-vacuity accounting: %+v", summary)
	}
	for _, fault := range []FaultKind{FaultKill, FaultRestart, FaultOverlappingTakeover} {
		if summary.PerFault[string(fault)] != 1 {
			t.Fatalf("fault %s count=%d, want 1", fault, summary.PerFault[string(fault)])
		}
	}
	cell := profile.Name + "|" + string(BoundaryCheckpoint)
	for _, schedule := range processScheduleVariants {
		if summary.PerCellSchedule[cell+"|"+schedule] != 1 {
			t.Fatalf("durable schedule %s count=%d, want 1", schedule, summary.PerCellSchedule[cell+"|"+schedule])
		}
	}
}
