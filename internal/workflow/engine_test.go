package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/internal/flow"
)

func TestMemoryExecutionFenceRejectsStaleGenerationAndRecreatedFlow(t *testing.T) {
	ctx := context.Background()
	engine := NewMemoryEngine()
	created, err := engine.Create(ctx, mappedTestFlow(flow.Flow{ID: "execution-fence"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Start(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	control, _ := engine.Control(ctx, created.ID)
	fence, err := engine.RegisterExecutionFence(ctx, created.ID, "worker", "compat", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	staleGeneration := fence
	staleGeneration.Generation++
	if err := engine.RenewExecutionFence(ctx, staleGeneration, time.Minute); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("stale generation renew error=%v, want ErrInvalidState", err)
	}
	if err := engine.FinishExecutionFence(ctx, fence, "done"); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Stop(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.CompleteStopGeneration(ctx, created.ID, control.Generation); err != nil {
		t.Fatal(err)
	}
	if err := engine.Delete(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Create(ctx, mappedTestFlow(flow.Flow{ID: created.ID})); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Start(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	newControl, _ := engine.Control(ctx, created.ID)
	if _, err := engine.RegisterExecutionFence(ctx, created.ID, "worker", "compat", newControl.Generation, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := engine.FinishExecutionFence(ctx, fence, "stale"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("recreated flow accepted old fence: %v", err)
	}
}

func TestMemoryEngineCopiesAndFencesTableMappings(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	engine := NewMemoryEngine()
	definition := mappedTestFlow(flow.Flow{ID: "mapping-copy"})
	created, err := engine.Create(ctx, definition)
	if err != nil {
		t.Fatal(err)
	}
	originalIncarnation := engine.incarnations[definition.ID]
	definition.Config.TableMappings.Destinations[0].FutureTables.TargetTable = "mutated-outside"
	created.Config.TableMappings.Destinations[0].FutureTables.TargetTable = "mutated-return"
	stored, err := engine.Get(ctx, definition.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := stored.Config.TableMappings.Destinations[0].FutureTables.TargetTable; got != "{table}" {
		t.Fatalf("stored mapping aliased caller: %q", got)
	}
	stored.Config.TableMappings.Destinations[0].FutureTables.TargetTable = "changed-get"
	again, _ := engine.Get(ctx, definition.ID)
	if got := again.Config.TableMappings.Destinations[0].FutureTables.TargetTable; got != "{table}" {
		t.Fatalf("Get returned aliased mapping: %q", got)
	}
	again.Config.TableMappings.Destinations[0].Tables = []flow.TableMapping{}
	again.Config.TableMappings.Destinations[0].FutureTables.Write.KeyColumns = []string{}
	if _, err := engine.Update(ctx, again); err != nil {
		t.Fatal(err)
	}
	if engine.incarnations[definition.ID] != originalIncarnation {
		t.Fatal("nil/empty canonical mapping change rotated memory flow incarnation")
	}
	again, _ = engine.Get(ctx, definition.ID)
	again.Config.TableMappings.Destinations[0].FutureTables.TargetTable = "mapped_{table}"
	if _, err := engine.Update(ctx, again); err != nil {
		t.Fatal(err)
	}
	if engine.incarnations[definition.ID] == originalIncarnation {
		t.Fatal("mapping change did not rotate memory flow incarnation")
	}
	mappingIncarnation := engine.incarnations[definition.ID]
	again, _ = engine.Get(ctx, definition.ID)
	again.WireFormat = "json"
	if _, err := engine.Update(ctx, again); err != nil {
		t.Fatal(err)
	}
	if engine.incarnations[definition.ID] == mappingIncarnation {
		t.Fatal("wire-format change did not rotate memory flow incarnation")
	}
	if _, err := engine.Start(ctx, definition.ID); err != nil {
		t.Fatal(err)
	}
	running, _ := engine.Get(ctx, definition.ID)
	running.Config.TableMappings.Destinations[0].FutureTables.TargetTable = "again_{table}"
	if _, err := engine.Update(ctx, running); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("running mapping update error=%v, want ErrInvalidState", err)
	}
	running, _ = engine.Get(ctx, definition.ID)
	running.WireFormat = "proto"
	if _, err := engine.Update(ctx, running); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("running wire-format update error=%v, want ErrInvalidState", err)
	}
}

func TestMemoryEngineLifecycleReturnsAreMutationIsolated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assertIsolated := func(t *testing.T, engine *MemoryEngine, returned flow.Flow) {
		t.Helper()
		returned.Config.TableMappings.Destinations[0].FutureTables.TargetTable = "caller_mutation_{table}"
		stored, err := engine.Get(ctx, returned.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got := stored.Config.TableMappings.Destinations[0].FutureTables.TargetTable; got != "{table}" {
			t.Fatalf("lifecycle result mutated stored mapping: %q", got)
		}
	}

	engine := NewMemoryEngine()
	created, err := engine.Create(ctx, mappedTestFlow(flow.Flow{ID: "lifecycle-copy-core"}))
	if err != nil {
		t.Fatal(err)
	}
	assertIsolated(t, engine, created)
	started, control, err := engine.PlanStart(ctx, created.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	assertIsolated(t, engine, started)
	idempotentStarted, _, err := engine.PlanStart(ctx, created.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	assertIsolated(t, engine, idempotentStarted)
	pausing, control, err := engine.RequestPause(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertIsolated(t, engine, pausing)
	idempotentPausing, _, err := engine.RequestPause(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertIsolated(t, engine, idempotentPausing)
	paused, err := engine.CompletePause(ctx, created.ID, control.Generation)
	if err != nil {
		t.Fatal(err)
	}
	assertIsolated(t, engine, paused)
	idempotentPaused, _, err := engine.RequestPause(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertIsolated(t, engine, idempotentPaused)
	resumed, control, err := engine.PlanStart(ctx, created.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	assertIsolated(t, engine, resumed)
	stopping, control, err := engine.RequestStop(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertIsolated(t, engine, stopping)
	idempotentStopping, _, err := engine.RequestStop(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertIsolated(t, engine, idempotentStopping)
	stopped, err := engine.CompleteStopGeneration(ctx, created.ID, control.Generation)
	if err != nil {
		t.Fatal(err)
	}
	assertIsolated(t, engine, stopped)
	idempotentStopped, err := engine.CompleteStopGeneration(ctx, created.ID, control.Generation)
	if err != nil {
		t.Fatal(err)
	}
	assertIsolated(t, engine, idempotentStopped)

	wrappers := NewMemoryEngine()
	wrapperCreated, err := wrappers.Create(ctx, mappedTestFlow(flow.Flow{ID: "lifecycle-copy-wrappers"}))
	if err != nil {
		t.Fatal(err)
	}
	wrapperStarted, err := wrappers.Start(ctx, wrapperCreated.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertIsolated(t, wrappers, wrapperStarted)
	wrapperPaused, err := wrappers.Pause(ctx, wrapperCreated.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertIsolated(t, wrappers, wrapperPaused)
	wrapperResumed, err := wrappers.Resume(ctx, wrapperCreated.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertIsolated(t, wrappers, wrapperResumed)
	wrapperStopped, err := wrappers.Stop(ctx, wrapperCreated.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertIsolated(t, wrappers, wrapperStopped)

	failedEngine := NewMemoryEngine()
	failedCreated, err := failedEngine.Create(ctx, mappedTestFlow(flow.Flow{ID: "lifecycle-copy-fail"}))
	if err != nil {
		t.Fatal(err)
	}
	failed, err := failedEngine.Fail(ctx, failedCreated.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertIsolated(t, failedEngine, failed)
}

func TestMemoryEngineLifecycleAndExecutions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	engine := NewMemoryEngine()
	created, err := engine.Create(ctx, mappedTestFlow(flow.Flow{ID: "flow-1"}))
	if err != nil || created.State != flow.StateCreated {
		t.Fatalf("Create() = (%v, %v)", created.State, err)
	}
	if err := engine.RegisterExecutionGeneration(ctx, created.ID, "early", "test", 0, 15*time.Second); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("RegisterExecutionGeneration before start error = %v, want ErrInvalidState", err)
	}
	if _, err := engine.Start(ctx, created.ID); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	control, err := engine.Control(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.RegisterExecutionGeneration(ctx, created.ID, "exec-1", "test", control.Generation, 15*time.Second); err != nil {
		t.Fatalf("RegisterExecutionGeneration() error = %v", err)
	}
	active, err := engine.ActiveExecutionsThrough(ctx, created.ID, control.Generation)
	if err != nil || active != 1 {
		t.Fatalf("ActiveExecutions() = (%d, %v), want (1, nil)", active, err)
	}
	if _, err := engine.Stop(ctx, created.ID); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	stopControl, err := engine.Control(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.CompleteStopGeneration(ctx, created.ID, stopControl.Generation); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("CompleteStopGeneration() with active execution error = %v, want ErrInvalidState", err)
	}
	if err := engine.FinishExecutionReason(ctx, created.ID, "exec-1", "test_done"); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.CompleteStopGeneration(ctx, created.ID, stopControl.Generation); err != nil {
		t.Fatalf("CompleteStopGeneration() error = %v", err)
	}
	if _, err := engine.Start(ctx, created.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("restart stopped flow error = %v, want ErrInvalidState", err)
	}
}

func TestAuthoritativeTerminationRequiresExactIDBackendAndExpiredLease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	engine := NewMemoryEngine()
	if _, err := engine.Create(ctx, mappedTestFlow(flow.Flow{ID: "backend-fence"})); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Start(ctx, "backend-fence"); err != nil {
		t.Fatal(err)
	}
	control, _ := engine.Control(ctx, "backend-fence")
	if err := engine.RegisterExecutionGeneration(ctx, "backend-fence", "kube", "kubernetes", control.Generation, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := engine.RegisterExecutionGeneration(ctx, "backend-fence", "manual", "worker", control.Generation, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := engine.ReconcileTerminatedExecutions(ctx, "backend-fence", control.Generation, "kubernetes", []string{"kube"}, "job_deleted"); err != nil {
		t.Fatal(err)
	}
	active, err := engine.ActiveExecutionsThrough(ctx, "backend-fence", control.Generation)
	if err != nil || active != 2 {
		t.Fatalf("active before lease expiry=(%d,%v), want 2", active, err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := engine.ReconcileTerminatedExecutions(ctx, "backend-fence", control.Generation, "kubernetes", []string{"wrong"}, "job_deleted"); err != nil {
		t.Fatal(err)
	}
	active, _ = engine.ActiveExecutionsThrough(ctx, "backend-fence", control.Generation)
	if active != 2 {
		t.Fatalf("wrong exact id removed an execution; active=%d", active)
	}
	if err := engine.ReconcileTerminatedExecutions(ctx, "backend-fence", control.Generation, "kubernetes", []string{"kube"}, "job_deleted"); err != nil {
		t.Fatal(err)
	}
	active, err = engine.ActiveExecutionsThrough(ctx, "backend-fence", control.Generation)
	if err != nil || active != 1 {
		t.Fatalf("active=(%d,%v), want unmatched worker to remain", active, err)
	}
}

func TestOrchestratedStopWaitsForExecution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	base := NewMemoryEngine()
	_, _ = base.Create(ctx, mappedTestFlow(flow.Flow{ID: "flow-1"}))
	_, _ = base.Start(ctx, "flow-1")
	control, err := base.Control(ctx, "flow-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := base.RegisterExecutionGeneration(ctx, "flow-1", "exec-1", "test", control.Generation, 15*time.Second); err != nil {
		t.Fatal(err)
	}
	engine := NewOrchestratedEngine(base, PassiveDispatcher{}, nil)
	done := make(chan error, 1)
	go func() {
		_, err := engine.Stop(ctx, "flow-1")
		done <- err
	}()
	state, err := base.Get(ctx, "flow-1")
	if err != nil {
		t.Fatal(err)
	}
	for state.State != flow.StateStopping {
		state, err = base.Get(ctx, "flow-1")
		if err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-done:
		t.Fatalf("Stop returned before execution finished: %v", err)
	default:
	}
	if err := base.FinishExecutionReason(ctx, "flow-1", "exec-1", "test_done"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	stopped, _ := base.Get(ctx, "flow-1")
	if stopped.State != flow.StateStopped {
		t.Fatalf("state = %s, want stopped", stopped.State)
	}
}
