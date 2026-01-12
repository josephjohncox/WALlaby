package stream

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/spec"
)

func TestTraceSuite(t *testing.T) {
	manifestPath := coverageManifestPath(t)
	manifest, err := spec.LoadManifest(manifestPath)
	if err != nil {
		t.Fatalf("load coverage manifest: %v", err)
	}

	cases := envInt("TRACE_CASES", 1000)
	maxBatches := envInt("TRACE_MAX_BATCHES", 10)
	maxRecords := envInt("TRACE_MAX_RECORDS", 3)
	seed := envInt64("TRACE_SEED", 1)

	r := rand.New(rand.NewSource(seed))
	t.Logf("trace suite cases=%d seed=%d", cases, seed)

	coverage := newTraceCoverage()

	coveragePlans := []batchPlan{
		{Batches: []batchSpec{{Kind: 0, Records: 1}}},
		{Batches: []batchSpec{{Kind: 3, Records: 1}}},
	}

	for idx, plan := range coveragePlans {
		traceEvents, err := runPlan(plan)
		if err != nil {
			t.Fatalf("coverage plan %d run: %v", idx, err)
		}
		planCoverage, err := EvaluateTrace(traceEvents, TraceValidationOptions{}, &manifest)
		if err != nil {
			t.Fatalf("coverage plan %d trace invalid: %v", idx, err)
		}
		coverage.merge(planCoverage)
	}

	for i := 0; i < cases; i++ {
		plan := randomPlan(r, maxBatches, maxRecords)
		traceEvents, err := runPlan(plan)
		if err != nil {
			t.Fatalf("case %d run: %v", i, err)
		}
		planCoverage, err := EvaluateTrace(traceEvents, TraceValidationOptions{}, &manifest)
		if err != nil {
			t.Fatalf("case %d trace invalid: %v", i, err)
		}
		coverage.merge(planCoverage)
	}

	assertCoverage(t, manifest, coverage)
	if os.Getenv("TRACE_COVERAGE_REPORT") != "" {
		reportCoverage(t, coverage)
	}
}

func runPlan(plan batchPlan) ([]TraceEvent, error) {
	ctx := context.Background()
	log := &eventLog{}
	traceSink := &MemoryTraceSink{}
	schema := connector.Schema{Name: "events", Namespace: "public", Version: 1}
	checkpoints := &recordingCheckpointStore{}

	batches := make([]connector.Batch, 0, len(plan.Batches))
	for i, spec := range plan.Batches {
		cp := connector.Checkpoint{
			LSN:      fmt.Sprintf("%d", i+1),
			Metadata: map[string]string{"seq": strconv.Itoa(i)},
		}
		if spec.Kind == 1 {
			cp.Metadata["control"] = "true"
		}
		var records []connector.Record
		switch spec.Kind {
		case 0:
			records = make([]connector.Record, 0, spec.Records)
			for j := 0; j < spec.Records; j++ {
				records = append(records, connector.Record{
					Table:     "events",
					Operation: connector.OpInsert,
					After: map[string]any{
						"id":   int64(i*10 + j),
						"name": "value",
					},
					Timestamp: time.Unix(0, 0).UTC(),
				})
			}
		case 3:
			records = []connector.Record{{
				Table:     "events",
				Operation: connector.OpDDL,
				DDL:       fmt.Sprintf("ALTER TABLE events ADD COLUMN col_%d text", i),
				Timestamp: time.Unix(0, 0).UTC(),
			}}
		}
		batches = append(batches, connector.Batch{
			Schema:     schema,
			Records:    records,
			Checkpoint: cp,
		})
	}

	source := &fakeSource{batches: batches, log: log}
	dest := &recordingDest{log: log}
	ddlApplied := make([]string, 0)

	runner := Runner{
		Source:     source,
		SourceSpec: connector.Spec{Options: map[string]string{"mode": "backfill"}},
		Destinations: []DestinationConfig{{
			Spec: connector.Spec{Name: "dest"},
			Dest: dest,
		}},
		Checkpoints: checkpoints,
		FlowID:      "flow-trace",
		TraceSink:   traceSink,
		DDLApplied: func(_ context.Context, flowID string, lsn string, ddl string) error {
			if lsn == "" || ddl == "" {
				return fmt.Errorf("invalid ddl applied event")
			}
			if traceSink != nil {
				traceSink.Emit(context.Background(), TraceEvent{
					Kind:       "ddl_approved",
					Spec:       spec.SpecCDCFlow,
					SpecAction: spec.ActionApproveDDL,
					LSN:        lsn,
					FlowID:     flowID,
					DDL:        ddl,
				})
			}
			ddlApplied = append(ddlApplied, lsn)
			return nil
		},
	}

	if err := runner.Run(ctx); err != nil {
		return nil, err
	}
	_ = ddlApplied
	return traceSink.Events(), nil
}

func randomPlan(r *rand.Rand, maxBatches, maxRecords int) batchPlan {
	if maxBatches <= 0 {
		maxBatches = 1
	}
	if maxRecords <= 0 {
		maxRecords = 1
	}
	count := r.Intn(maxBatches) + 1
	specs := make([]batchSpec, 0, count)
	for i := 0; i < count; i++ {
		kind := uint8(r.Intn(4))
		records := 0
		if kind == 0 {
			records = r.Intn(maxRecords) + 1
		}
		if kind == 3 {
			records = 1
		}
		specs = append(specs, batchSpec{Kind: kind, Records: records})
	}
	return batchPlan{Batches: specs}
}

func envInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return fallback
}

func envInt64(key string, fallback int64) int64 {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			return parsed
		}
	}
	return fallback
}

func coverageManifestPath(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return spec.ManifestPath(filepath.Join(dir, "specs"), spec.SpecCDCFlow)
		}
		next := filepath.Dir(dir)
		if next == dir {
			break
		}
		dir = next
	}
	t.Fatalf("go.mod not found while resolving manifest path")
	return ""
}

func assertCoverage(t *testing.T, manifest spec.Manifest, coverage TraceCoverage) {
	t.Helper()
	unreachableActions := manifest.UnreachableActionSet()
	unreachableInvariants := manifest.UnreachableInvariantSet()

	for _, action := range manifest.Actions {
		if _, skip := unreachableActions[action]; skip {
			continue
		}
		min := manifest.ActionMin(action)
		if coverage.Actions[action] < min {
			t.Fatalf("spec action not exercised: %s (got %d, want >= %d)", action, coverage.Actions[action], min)
		}
	}

	for _, inv := range manifest.Invariants {
		if _, skip := unreachableInvariants[inv]; skip {
			continue
		}
		min := manifest.InvariantMin(inv)
		if coverage.Invariants[inv] < min {
			t.Fatalf("spec invariant not exercised: %s (got %d, want >= %d)", inv, coverage.Invariants[inv], min)
		}
	}
}

func reportCoverage(t *testing.T, coverage TraceCoverage) {
	t.Helper()
	t.Log("Trace coverage report")
	if len(coverage.Actions) > 0 {
		actions := make([]string, 0, len(coverage.Actions))
		for action := range coverage.Actions {
			actions = append(actions, string(action))
		}
		sort.Strings(actions)
		for _, action := range actions {
			t.Logf("  action %-20s %d", action, coverage.Actions[spec.Action(action)])
		}
	}
	if len(coverage.Invariants) > 0 {
		invariants := make([]string, 0, len(coverage.Invariants))
		for inv := range coverage.Invariants {
			invariants = append(invariants, string(inv))
		}
		sort.Strings(invariants)
		for _, inv := range invariants {
			t.Logf("  invariant %-20s %d", inv, coverage.Invariants[spec.Invariant(inv)])
		}
	}
}
