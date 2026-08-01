package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/internal/flow"
)

func TestMemoryEngineLifecycleAndExecutions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	engine := NewMemoryEngine()
	created, err := engine.Create(ctx, flow.Flow{ID: "flow-1"})
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
	if _, err := engine.Create(ctx, flow.Flow{ID: "backend-fence"}); err != nil {
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
	_, _ = base.Create(ctx, flow.Flow{ID: "flow-1"})
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
