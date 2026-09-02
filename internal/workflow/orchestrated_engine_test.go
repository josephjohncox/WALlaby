package workflow

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"testing"
	"time"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type dispatchCall struct {
	flowID     string
	generation int64
}
type fakeDispatcher struct {
	err         error
	cancelErr   error
	terminal    bool
	terminalIDs []string
	enqueued    []dispatchCall
	cancelled   []dispatchCall
}

type fakeSourceResourceCleaner struct {
	base       *MemoryEngine
	err        error
	calls      int
	generation int64
	state      flow.State
}

func (c *fakeSourceResourceCleaner) CleanupSourceResources(ctx context.Context, f flow.Flow, generation int64) error {
	c.calls++
	c.generation = generation
	c.state = f.State
	if c.base != nil {
		active, err := c.base.ActiveExecutionsThrough(ctx, f.ID, generation)
		if err != nil {
			return err
		}
		if active != 0 {
			return errors.New("cleanup called before execution quiescence")
		}
	}
	return c.err
}

func (d *fakeDispatcher) EnqueueGeneration(_ context.Context, flowID string, generation int64) error {
	if d.err != nil {
		return d.err
	}
	d.enqueued = append(d.enqueued, dispatchCall{flowID, generation})
	return nil
}
func (d *fakeDispatcher) CancelThroughGeneration(_ context.Context, flowID string, generation int64) (CancellationReceipt, error) {
	d.cancelled = append(d.cancelled, dispatchCall{flowID, generation})
	return CancellationReceipt{ThroughGeneration: generation, Terminal: d.terminal, Backend: "fake", TerminalExecutionIDs: d.terminalIDs}, d.cancelErr
}

func newCreatedMemoryFlow(t *testing.T, id string) *MemoryEngine {
	t.Helper()
	base := NewMemoryEngine()
	if _, err := base.Create(context.Background(), mappedTestFlow(flow.Flow{ID: id})); err != nil {
		t.Fatal(err)
	}
	return base
}

func TestOrchestratedEngineStartDispatchesGeneration(t *testing.T) {
	ctx := context.Background()
	base := newCreatedMemoryFlow(t, "flow-1")
	dispatcher := &fakeDispatcher{}
	engine := NewOrchestratedEngine(base, dispatcher, nil)
	started, err := engine.Start(ctx, "flow-1")
	if err != nil {
		t.Fatal(err)
	}
	if started.State != flow.StateRunning {
		t.Fatalf("state=%s", started.State)
	}
	if len(dispatcher.enqueued) != 1 || dispatcher.enqueued[0] != (dispatchCall{"flow-1", 1}) {
		t.Fatalf("enqueued=%v", dispatcher.enqueued)
	}
	control, _ := base.Control(ctx, "flow-1")
	if control.DispatchPending {
		t.Fatal("dispatch remained pending")
	}
}

func workflowTestSnowflakePolicy(t *testing.T) connector.SnowflakeDeploymentPolicy {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := connector.NewSnowflakeDeploymentPolicyWithPrivateKey("account", "user", "account.snowflakecomputing.com", key, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = policy.Close() })
	return policy
}

func TestOrchestratedEngineSnowflakeReconciliationFailsClosedAfterPolicyRestart(t *testing.T) {
	ctx := context.Background()
	destination := connector.RuntimeSpec{Name: "snowflake", Type: connector.EndpointSnowflake, Options: map[string]string{"dsn": "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false"}}
	definition := mappedTestFlow(flow.Flow{ID: "snowflake-reconcile", Destinations: []*wallabypb.Endpoint{workflowTestDestination(destination)}})
	base := NewMemoryEngine()
	if _, err := base.Create(ctx, definition); err != nil {
		t.Fatal(err)
	}
	dispatcher := &fakeDispatcher{err: errors.New("dispatch failed")}
	enabled := NewOrchestratedEngineWithAdmission(base, dispatcher, nil, func(f flow.Flow) error {
		return flow.ValidateSnowflakeDeploymentPolicy(f, connector.DefaultRegistry, workflowTestSnowflakePolicy(t))
	})
	if _, err := enabled.Start(ctx, definition.ID); err == nil {
		t.Fatal("expected initial dispatch failure")
	}
	dispatcher.err = nil
	disabled := NewOrchestratedEngineWithAdmission(base, dispatcher, nil, func(f flow.Flow) error {
		return flow.ValidateSnowflakeDeploymentPolicy(f, connector.DefaultRegistry, connector.SnowflakeDeploymentPolicy{})
	})
	if err := disabled.ReconcileOnce(ctx); !errors.Is(err, connector.ErrSnowflakeExecutionDisabled) {
		t.Fatalf("ReconcileOnce() error=%v", err)
	}
	control, _ := base.Control(ctx, definition.ID)
	if !control.DispatchPending || len(dispatcher.enqueued) != 0 {
		t.Fatalf("disabled reconciliation mutated dispatch: control=%+v calls=%v", control, dispatcher.enqueued)
	}
}

func TestOrchestratedEngineDispatchErrorRemainsRecoverable(t *testing.T) {
	ctx := context.Background()
	base := newCreatedMemoryFlow(t, "flow-2")
	dispatcher := &fakeDispatcher{err: errors.New("dispatch failed")}
	engine := NewOrchestratedEngine(base, dispatcher, nil)
	returned, err := engine.Start(ctx, "flow-2")
	if err == nil {
		t.Fatal("expected error")
	}
	if returned.State != flow.StateRunning {
		t.Fatalf("returned=%s", returned.State)
	}
	control, _ := base.Control(ctx, "flow-2")
	if !control.DispatchPending || control.Generation != 1 {
		t.Fatalf("control=%+v", control)
	}
	dispatcher.err = nil
	if err := engine.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	control, _ = base.Control(ctx, "flow-2")
	if control.DispatchPending {
		t.Fatal("reconcile did not clear pending")
	}
}

func TestPauseRemainsRunningUntilExecutionFinishes(t *testing.T) {
	ctx := context.Background()
	base := newCreatedMemoryFlow(t, "pause")
	engine := NewOrchestratedEngine(base, &fakeDispatcher{}, nil)
	if _, err := engine.Start(ctx, "pause"); err != nil {
		t.Fatal(err)
	}
	control, _ := base.Control(ctx, "pause")
	if err := base.RegisterExecutionGeneration(ctx, "pause", "exec", "test", control.Generation, time.Minute); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := engine.Pause(ctx, "pause"); done <- err }()
	deadline := time.Now().Add(time.Second)
	for {
		control, _ = base.Control(ctx, "pause")
		if control.Target == TargetPaused {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pause intent not stored")
		}
		time.Sleep(time.Millisecond)
	}
	current, _ := base.Get(ctx, "pause")
	if current.State != flow.StateRunning {
		t.Fatalf("published %s before quiescence", current.State)
	}
	if err := base.FinishExecutionReason(ctx, "pause", "exec", "test_done"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	current, _ = base.Get(ctx, "pause")
	if current.State != flow.StatePaused {
		t.Fatalf("state=%s", current.State)
	}
}

func TestGenerationFencesDelayedWorkerAfterResume(t *testing.T) {
	ctx := context.Background()
	base := newCreatedMemoryFlow(t, "fence")
	engine := NewOrchestratedEngine(base, &fakeDispatcher{}, nil)
	if _, err := engine.Start(ctx, "fence"); err != nil {
		t.Fatal(err)
	}
	first, _ := base.Control(ctx, "fence")
	if _, err := engine.Pause(ctx, "fence"); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Resume(ctx, "fence"); err != nil {
		t.Fatal(err)
	}
	second, _ := base.Control(ctx, "fence")
	if second.Generation <= first.Generation {
		t.Fatalf("generations %d -> %d", first.Generation, second.Generation)
	}
	if err := base.RegisterExecutionGeneration(ctx, "fence", "old", "test", first.Generation, time.Minute); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("old generation error=%v", err)
	}
	if err := base.RegisterExecutionGeneration(ctx, "fence", "new", "test", second.Generation, time.Minute); err != nil {
		t.Fatal(err)
	}
}

func TestStopIsIdempotentAndDeleteRejectsIncompleteWork(t *testing.T) {
	ctx := context.Background()
	base := newCreatedMemoryFlow(t, "stop")
	engine := NewOrchestratedEngine(base, &fakeDispatcher{terminal: true}, nil)
	if _, err := engine.Start(ctx, "stop"); err != nil {
		t.Fatal(err)
	}
	stopped, err := engine.Stop(ctx, "stop")
	if err != nil {
		t.Fatal(err)
	}
	if stopped.State != flow.StateStopped {
		t.Fatalf("state=%s", stopped.State)
	}
	stopped, err = engine.Stop(ctx, "stop")
	if err != nil {
		t.Fatal(err)
	}
	if stopped.State != flow.StateStopped {
		t.Fatalf("repeat state=%s", stopped.State)
	}
	if err := engine.Delete(ctx, "stop"); err != nil {
		t.Fatal(err)
	}
}

func TestStopCleansResourcesAfterQuiescenceBeforePublishingStopped(t *testing.T) {
	ctx := context.Background()
	base := newCreatedMemoryFlow(t, "cleanup-stop")
	cleaner := &fakeSourceResourceCleaner{base: base}
	engine := NewOrchestratedEngine(base, &fakeDispatcher{terminal: true}, nil, cleaner)
	if _, err := engine.Start(ctx, "cleanup-stop"); err != nil {
		t.Fatal(err)
	}
	control, _ := base.Control(ctx, "cleanup-stop")
	if err := base.RegisterExecutionGeneration(ctx, "cleanup-stop", "done", "fake", control.Generation, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := base.FinishExecutionReason(ctx, "cleanup-stop", "done", "completed"); err != nil {
		t.Fatal(err)
	}
	stopped, err := engine.Stop(ctx, "cleanup-stop")
	if err != nil {
		t.Fatal(err)
	}
	if stopped.State != flow.StateStopped || cleaner.calls != 1 || cleaner.state != flow.StateStopping || cleaner.generation != control.Generation {
		t.Fatalf("stop=(%s,%v) cleanup=(calls=%d state=%s generation=%d)", stopped.State, err, cleaner.calls, cleaner.state, cleaner.generation)
	}
	if _, err := engine.Stop(ctx, "cleanup-stop"); err != nil {
		t.Fatal(err)
	}
	if cleaner.calls != 1 {
		t.Fatalf("idempotent stopped call repeated cleanup: calls=%d", cleaner.calls)
	}
}

func TestStopCleanupFailureRemainsStoppingAndReconciles(t *testing.T) {
	ctx := context.Background()
	base := newCreatedMemoryFlow(t, "cleanup-retry")
	injected := errors.New("cleanup outcome indeterminate")
	cleaner := &fakeSourceResourceCleaner{base: base, err: injected}
	engine := NewOrchestratedEngine(base, &fakeDispatcher{terminal: true}, nil, cleaner)
	if _, err := engine.Start(ctx, "cleanup-retry"); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Stop(ctx, "cleanup-retry"); !errors.Is(err, injected) {
		t.Fatalf("Stop() error=%v, want cleanup failure", err)
	}
	current, _ := base.Get(ctx, "cleanup-retry")
	if current.State != flow.StateStopping || cleaner.calls != 1 {
		t.Fatalf("failed cleanup state=%s calls=%d", current.State, cleaner.calls)
	}
	cleaner.err = nil
	if err := engine.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	current, _ = base.Get(ctx, "cleanup-retry")
	if current.State != flow.StateStopped || cleaner.calls != 2 {
		t.Fatalf("reconciled cleanup state=%s calls=%d", current.State, cleaner.calls)
	}
}

func TestPauseAndFailureNeverRunTerminalCleanup(t *testing.T) {
	ctx := context.Background()
	base := newCreatedMemoryFlow(t, "cleanup-nonterminal")
	cleaner := &fakeSourceResourceCleaner{base: base}
	engine := NewOrchestratedEngine(base, &fakeDispatcher{}, nil, cleaner)
	if _, err := engine.Start(ctx, "cleanup-nonterminal"); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Pause(ctx, "cleanup-nonterminal"); err != nil {
		t.Fatal(err)
	}
	if cleaner.calls != 0 {
		t.Fatalf("pause ran terminal cleanup: calls=%d", cleaner.calls)
	}
	if _, err := engine.Resume(ctx, "cleanup-nonterminal"); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Fail(ctx, "cleanup-nonterminal"); err != nil {
		t.Fatal(err)
	}
	if cleaner.calls != 0 {
		t.Fatalf("failure ran terminal cleanup: calls=%d", cleaner.calls)
	}
}

func TestTerminalReceiptReconcilesCrashedExecutionOnlyAfterLeaseExpiry(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	base := newCreatedMemoryFlow(t, "crashed")
	dispatcher := &fakeDispatcher{terminal: true, terminalIDs: []string{"dbos-exec"}}
	engine := NewOrchestratedEngine(base, dispatcher, nil)
	if _, err := engine.Start(ctx, "crashed"); err != nil {
		t.Fatal(err)
	}
	control, _ := base.Control(ctx, "crashed")
	if err := base.RegisterExecutionGeneration(ctx, "crashed", "dbos-exec", "fake", control.Generation, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	paused, err := engine.Pause(ctx, "crashed")
	if err != nil {
		t.Fatal(err)
	}
	if paused.State != flow.StatePaused || time.Since(started) < 20*time.Millisecond {
		t.Fatalf("Pause()=(%s,%v), lease was not honored", paused.State, err)
	}
}

func TestTerminalReceiptCannotEraseStillLeasedExecution(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	base := newCreatedMemoryFlow(t, "heartbeating")
	dispatcher := &fakeDispatcher{terminal: true, terminalIDs: []string{"live"}}
	engine := NewOrchestratedEngine(base, dispatcher, nil)
	if _, err := engine.Start(context.Background(), "heartbeating"); err != nil {
		t.Fatal(err)
	}
	control, _ := base.Control(context.Background(), "heartbeating")
	if err := base.RegisterExecutionGeneration(context.Background(), "heartbeating", "live", "fake", control.Generation, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Pause(ctx, "heartbeating"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Pause() error=%v, want deadline while lease is live", err)
	}
	active, err := base.ActiveExecutionsThrough(context.Background(), "heartbeating", control.Generation)
	if err != nil || active != 1 {
		t.Fatalf("active=(%d,%v), still-leased row was erased", active, err)
	}
}

func TestDeleteRejectsPendingDispatchAndActiveExecution(t *testing.T) {
	ctx := context.Background()
	base := newCreatedMemoryFlow(t, "delete")
	dispatcher := &fakeDispatcher{err: errors.New("down")}
	engine := NewOrchestratedEngine(base, dispatcher, nil)
	_, _ = engine.Start(ctx, "delete")
	if err := engine.Delete(ctx, "delete"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("pending delete error=%v", err)
	}
}
