package workflow

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/internal/flow"
	"pgregory.net/rapid"
)

func TestMemoryLifecycleGenerationAndQuiescenceRapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		ctx := context.Background()
		store := NewMemoryEngine()
		flowID := "rapid-lifecycle"
		if _, err := store.Create(ctx, mappedTestFlow(flow.Flow{ID: flowID, Name: "rapid"})); err != nil {
			t.Fatal(err)
		}

		cycles := rapid.IntRange(1, 8).Draw(t, "cycles")
		var previousGeneration int64
		resume := false
		for cycle := range cycles {
			running, control, err := store.PlanStart(ctx, flowID, resume)
			if err != nil {
				t.Fatalf("cycle %d start: %v", cycle, err)
			}
			if running.State != flow.StateRunning || control.Generation != previousGeneration+1 {
				t.Fatalf("cycle %d running=%s generation=%d previous=%d", cycle, running.State, control.Generation, previousGeneration)
			}
			if previousGeneration > 0 {
				err := store.RegisterExecutionGeneration(ctx, flowID, fmt.Sprintf("stale-%d", cycle), "rapid", previousGeneration, time.Minute)
				if !errors.Is(err, ErrInvalidState) {
					t.Fatalf("cycle %d stale generation registration error=%v", cycle, err)
				}
			}

			executionCount := rapid.IntRange(0, 4).Draw(t, fmt.Sprintf("executions_%d", cycle))
			executionIDs := make([]string, 0, executionCount)
			for index := range executionCount {
				executionID := fmt.Sprintf("execution-%d-%d", cycle, index)
				if err := store.RegisterExecutionGeneration(ctx, flowID, executionID, "rapid", control.Generation, time.Minute); err != nil {
					t.Fatalf("cycle %d register %s: %v", cycle, executionID, err)
				}
				executionIDs = append(executionIDs, executionID)
			}

			stop := rapid.Bool().Draw(t, fmt.Sprintf("stop_%d", cycle))
			if cycle == cycles-1 {
				stop = true
			}
			if stop {
				stopping, stopControl, err := store.RequestStop(ctx, flowID)
				if err != nil {
					t.Fatalf("cycle %d stop intent: %v", cycle, err)
				}
				if stopping.State != flow.StateStopping || stopControl.Target != TargetStopped {
					t.Fatalf("cycle %d stop intent=(%s,%s)", cycle, stopping.State, stopControl.Target)
				}
				if executionCount > 0 {
					if _, err := store.CompleteStopGeneration(ctx, flowID, control.Generation); !errors.Is(err, ErrInvalidState) {
						t.Fatalf("cycle %d completed stop with active executions: %v", cycle, err)
					}
				}
				finishExecutionsRapid(t, ctx, store, flowID, executionIDs, cycle)
				stopped, err := store.CompleteStopGeneration(ctx, flowID, control.Generation)
				if err != nil || stopped.State != flow.StateStopped {
					t.Fatalf("cycle %d stopped=%s error=%v", cycle, stopped.State, err)
				}
				active, err := store.ActiveExecutionsThrough(ctx, flowID, control.Generation)
				if err != nil || active != 0 {
					t.Fatalf("cycle %d stopped active=%d error=%v", cycle, active, err)
				}
				return
			}

			pausing, pauseControl, err := store.RequestPause(ctx, flowID)
			if err != nil {
				t.Fatalf("cycle %d pause intent: %v", cycle, err)
			}
			if pausing.State != flow.StateRunning || pauseControl.Target != TargetPaused {
				t.Fatalf("cycle %d pause intent=(%s,%s)", cycle, pausing.State, pauseControl.Target)
			}
			if err := store.RegisterExecutionGeneration(ctx, flowID, fmt.Sprintf("late-%d", cycle), "rapid", control.Generation, time.Minute); !errors.Is(err, ErrInvalidState) {
				t.Fatalf("cycle %d late registration error=%v", cycle, err)
			}
			if executionCount > 0 {
				if _, err := store.CompletePause(ctx, flowID, control.Generation); !errors.Is(err, ErrInvalidState) {
					t.Fatalf("cycle %d completed pause with active executions: %v", cycle, err)
				}
			}
			finishExecutionsRapid(t, ctx, store, flowID, executionIDs, cycle)
			paused, err := store.CompletePause(ctx, flowID, control.Generation)
			if err != nil || paused.State != flow.StatePaused {
				t.Fatalf("cycle %d paused=%s error=%v", cycle, paused.State, err)
			}
			previousGeneration = control.Generation
			resume = true
		}
	})
}

func finishExecutionsRapid(t *rapid.T, ctx context.Context, store LifecycleStore, flowID string, executionIDs []string, cycle int) {
	for len(executionIDs) > 0 {
		index := rapid.IntRange(0, len(executionIDs)-1).Draw(t, fmt.Sprintf("finish_%d_%d", cycle, len(executionIDs)))
		executionID := executionIDs[index]
		if err := store.FinishExecutionReason(ctx, flowID, executionID, "rapid_done"); err != nil {
			t.Fatalf("finish %s: %v", executionID, err)
		}
		executionIDs = append(executionIDs[:index], executionIDs[index+1:]...)
	}
}
