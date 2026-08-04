package workflow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/internal/flow"
)

func TestMemoryLifecycleStoreContract(t *testing.T) {
	runLifecycleStoreContract(t, NewMemoryEngine(), "memory-lifecycle-contract")
}

func TestPostgresLifecycleStoreContract(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runLifecycleStoreContract(t, store, fmt.Sprintf("postgres-lifecycle-contract-%d", time.Now().UnixNano()))
}

func runLifecycleStoreContract(t *testing.T, store LifecycleStore, flowID string) {
	t.Helper()
	ctx := context.Background()

	created, err := store.Create(ctx, mappedTestFlow(flow.Flow{ID: flowID, Name: "contract"}))
	if err != nil {
		t.Fatal(err)
	}
	if created.State != flow.StateCreated {
		t.Fatalf("created state=%s, want %s", created.State, flow.StateCreated)
	}
	control, err := store.Control(ctx, flowID)
	if err != nil {
		t.Fatal(err)
	}
	if control.Generation != 0 || control.Target != TargetCreated || control.DispatchPending {
		t.Fatalf("created control=%+v", control)
	}
	if _, _, err := store.RequestPause(ctx, flowID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("pause created flow error=%v, want ErrInvalidState", err)
	}

	started, control, err := store.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	if started.State != flow.StateRunning || control.Generation != 1 || control.Target != TargetRunning || !control.DispatchPending {
		t.Fatalf("started=(%s,%+v)", started.State, control)
	}
	if err := store.MarkDispatched(ctx, flowID, control.Generation); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterExecutionGeneration(ctx, flowID, "exec-1", "contract", control.Generation, time.Minute); err != nil {
		t.Fatal(err)
	}

	pausing, pauseControl, err := store.RequestPause(ctx, flowID)
	if err != nil {
		t.Fatal(err)
	}
	if pausing.State != flow.StateRunning || pauseControl.Target != TargetPaused || pauseControl.Generation != control.Generation {
		t.Fatalf("pause intent=(%s,%+v)", pausing.State, pauseControl)
	}
	if err := store.RegisterExecutionGeneration(ctx, flowID, "late", "contract", control.Generation, time.Minute); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("late registration error=%v, want ErrInvalidState", err)
	}
	if _, err := store.CompletePause(ctx, flowID, pauseControl.Generation); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("complete pause with active execution error=%v, want ErrInvalidState", err)
	}
	if err := store.FinishExecutionReason(ctx, flowID, "exec-1", "contract_done"); err != nil {
		t.Fatal(err)
	}
	paused, err := store.CompletePause(ctx, flowID, pauseControl.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if paused.State != flow.StatePaused {
		t.Fatalf("paused state=%s", paused.State)
	}

	resumed, resumeControl, err := store.PlanStart(ctx, flowID, true)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.State != flow.StateRunning || resumeControl.Generation != 2 || !resumeControl.DispatchPending {
		t.Fatalf("resumed=(%s,%+v)", resumed.State, resumeControl)
	}
	stopping, stopControl, err := store.RequestStop(ctx, flowID)
	if err != nil {
		t.Fatal(err)
	}
	if stopping.State != flow.StateStopping || stopControl.Target != TargetStopped || stopControl.Generation != resumeControl.Generation {
		t.Fatalf("stop intent=(%s,%+v)", stopping.State, stopControl)
	}
	stopped, err := store.CompleteStopGeneration(ctx, flowID, stopControl.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.State != flow.StateStopped {
		t.Fatalf("stopped state=%s", stopped.State)
	}
	if _, _, err := store.PlanStart(ctx, flowID, false); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("restart stopped flow error=%v, want ErrInvalidState", err)
	}
	if err := store.Delete(ctx, flowID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, flowID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get deleted flow error=%v, want ErrNotFound", err)
	}
}
