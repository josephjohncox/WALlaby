package workflow

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/telemetry"
)

// OrchestratedEngine serializes lifecycle intent, generation-aware dispatch,
// cancellation proof, and quiescent completion.
// SourceResourceCleaner performs terminal connector cleanup after execution
// quiescence and before the stopped state is published.
type SourceResourceCleaner interface {
	CleanupSourceResources(context.Context, flow.Flow, int64) error
}

type OrchestratedEngine struct {
	base            LifecycleStore
	dispatcher      Dispatcher
	meters          *telemetry.Meters
	resourceCleaner SourceResourceCleaner
}

func NewOrchestratedEngine(base LifecycleStore, dispatcher Dispatcher, meters *telemetry.Meters, cleaners ...SourceResourceCleaner) *OrchestratedEngine {
	if base == nil {
		panic("workflow lifecycle store is required")
	}
	if dispatcher == nil {
		panic("workflow dispatcher is required; use PassiveDispatcher explicitly")
	}
	if len(cleaners) > 1 {
		panic("at most one source resource cleaner may be configured")
	}
	var cleaner SourceResourceCleaner
	if len(cleaners) == 1 {
		cleaner = cleaners[0]
	}
	return &OrchestratedEngine{base: base, dispatcher: dispatcher, meters: meters, resourceCleaner: cleaner}
}

func (o *OrchestratedEngine) Create(ctx context.Context, f flow.Flow) (flow.Flow, error) {
	created, err := o.base.Create(ctx, f)
	if err == nil && o.meters != nil {
		o.meters.RecordFlowCreate(ctx)
	}
	return created, err
}
func (o *OrchestratedEngine) Update(ctx context.Context, f flow.Flow) (flow.Flow, error) {
	return o.base.Update(ctx, f)
}
func (o *OrchestratedEngine) Get(ctx context.Context, flowID string) (flow.Flow, error) {
	return o.base.Get(ctx, flowID)
}
func (o *OrchestratedEngine) List(ctx context.Context) ([]flow.Flow, error) {
	return o.base.List(ctx)
}

func (o *OrchestratedEngine) Start(ctx context.Context, flowID string) (flow.Flow, error) {
	return o.startAndDispatch(ctx, flowID, false)
}
func (o *OrchestratedEngine) Resume(ctx context.Context, flowID string) (flow.Flow, error) {
	return o.startAndDispatch(ctx, flowID, true)
}
func (o *OrchestratedEngine) startAndDispatch(ctx context.Context, flowID string, resume bool) (flow.Flow, error) {
	var result flow.Flow
	acquired, err := o.base.WithFlowLock(ctx, flowID, false, func() error {
		fromState := o.getState(ctx, flowID)
		updated, control, err := o.base.PlanStart(ctx, flowID, resume)
		if err != nil {
			return err
		}
		result = updated
		if !control.DispatchPending {
			return nil
		}
		if err := o.dispatcher.EnqueueGeneration(ctx, flowID, control.Generation); err != nil {
			// Leave dispatch_pending durable so reconciliation can retry.
			return fmt.Errorf("enqueue flow generation %d: %w", control.Generation, err)
		}
		if err := o.base.MarkDispatched(ctx, flowID, control.Generation); err != nil {
			return err
		}
		o.recordTransition(ctx, fromState, string(updated.State))
		if o.meters != nil && fromState != string(flow.StateRunning) {
			o.meters.RecordFlowActive(ctx, 1)
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	if !acquired {
		return flow.Flow{}, errors.New("flow lifecycle operation is busy")
	}
	return result, nil
}

func (o *OrchestratedEngine) Pause(ctx context.Context, flowID string) (flow.Flow, error) {
	var result flow.Flow
	acquired, err := o.base.WithFlowLock(ctx, flowID, false, func() error {
		fromState := o.getState(ctx, flowID)
		current, control, err := o.base.RequestPause(ctx, flowID)
		if err != nil {
			return err
		}
		result = current
		if current.State == flow.StatePaused {
			return nil
		}
		if err := o.cancelAndQuiesce(ctx, flowID, control.Generation); err != nil {
			// Public state remains running; durable target makes this recoverable.
			return err
		}
		paused, err := o.base.CompletePause(ctx, flowID, control.Generation)
		if err != nil {
			return err
		}
		result = paused
		o.recordTransition(ctx, fromState, string(paused.State))
		if o.meters != nil && fromState == string(flow.StateRunning) {
			o.meters.RecordFlowActive(ctx, -1)
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	if !acquired {
		return flow.Flow{}, errors.New("flow lifecycle operation is busy")
	}
	return result, nil
}

func (o *OrchestratedEngine) Stop(ctx context.Context, flowID string) (flow.Flow, error) {
	var result flow.Flow
	acquired, err := o.base.WithFlowLock(ctx, flowID, false, func() error {
		fromState := o.getState(ctx, flowID)
		stopping, control, err := o.base.RequestStop(ctx, flowID)
		if err != nil {
			return err
		}
		result = stopping
		if stopping.State == flow.StateStopped {
			return nil
		}
		if err := o.cancelAndQuiesce(ctx, flowID, control.Generation); err != nil {
			return err
		}
		if o.resourceCleaner != nil {
			if err := o.resourceCleaner.CleanupSourceResources(ctx, stopping, control.Generation); err != nil {
				return err
			}
		}
		stopped, err := o.base.CompleteStopGeneration(ctx, flowID, control.Generation)
		if err != nil {
			return err
		}
		result = stopped
		o.recordTransition(ctx, fromState, string(stopped.State))
		if o.meters != nil && fromState == string(flow.StateRunning) {
			o.meters.RecordFlowActive(ctx, -1)
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	if !acquired {
		return flow.Flow{}, errors.New("flow lifecycle operation is busy")
	}
	return result, nil
}

func (o *OrchestratedEngine) cancelAndQuiesce(ctx context.Context, flowID string, generation int64) error {
	receipt, err := o.dispatcher.CancelThroughGeneration(ctx, flowID, generation)
	if err != nil {
		return fmt.Errorf("cancel flow dispatch: %w", err)
	}
	if receipt.ThroughGeneration < generation {
		return fmt.Errorf("dispatcher cancellation proof covers generation %d, need %d", receipt.ThroughGeneration, generation)
	}
	reason := "dispatcher_terminal"
	if receipt.Terminal {
		if receipt.Backend == "" {
			return errors.New("terminal cancellation receipt is missing backend identity")
		}
		reason = receipt.Backend + "_terminal"
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		// A terminal backend observation alone is insufficient: the exact
		// execution lease must also expire. Re-run this exact reconciliation so
		// a crashed worker can age out while a heartbeating worker stays fenced.
		if receipt.Terminal {
			if err := o.base.ReconcileTerminatedExecutions(ctx, flowID, generation, receipt.Backend, receipt.TerminalExecutionIDs, reason); err != nil {
				return err
			}
		}
		active, err := o.base.ActiveExecutionsThrough(ctx, flowID, generation)
		if err != nil {
			return err
		}
		if active == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (o *OrchestratedEngine) Delete(ctx context.Context, flowID string) error {
	var deleted bool
	_, err := o.base.WithFlowLock(ctx, flowID, false, func() error {
		fromState := o.getState(ctx, flowID)
		if err := o.base.Delete(ctx, flowID); err != nil {
			return err
		}
		deleted = true
		o.recordTransition(ctx, fromState, "deleted")
		return nil
	})
	if err != nil {
		return err
	}
	if !deleted {
		return errors.New("flow lifecycle operation is busy")
	}
	return nil
}

func (o *OrchestratedEngine) Fail(ctx context.Context, flowID string) (flow.Flow, error) {
	var result flow.Flow
	_, err := o.base.WithFlowLock(ctx, flowID, false, func() error {
		var err error
		result, err = o.base.Fail(ctx, flowID)
		return err
	})
	return result, err
}

// ReconcileOnce resumes durable pending dispatch, pause, and stop work. Locks
// are try-acquired so multiple replicas safely skip work owned by a peer.
func (o *OrchestratedEngine) ReconcileOnce(ctx context.Context) error {
	controls, err := o.base.PendingControls(ctx)
	if err != nil {
		return err
	}
	var joined error
	for _, control := range controls {
		control := control
		// A fail-closed standalone execution may remain active indefinitely.
		// Bound each attempt so one stale flow cannot starve reconciliation of
		// unrelated flows; durable intent is retried on the next pass.
		opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := o.base.WithFlowLock(opCtx, control.FlowID, true, func() error {
			latest, err := o.base.Control(opCtx, control.FlowID)
			if err != nil {
				return err
			}
			switch {
			case latest.Target == TargetRunning && latest.DispatchPending:
				if err := o.dispatcher.EnqueueGeneration(opCtx, latest.FlowID, latest.Generation); err != nil {
					return err
				}
				return o.base.MarkDispatched(opCtx, latest.FlowID, latest.Generation)
			case latest.Target == TargetPaused && latest.State == flow.StateRunning:
				if err := o.cancelAndQuiesce(opCtx, latest.FlowID, latest.Generation); err != nil {
					return err
				}
				_, err = o.base.CompletePause(opCtx, latest.FlowID, latest.Generation)
				return err
			case latest.Target == TargetStopped && latest.State == flow.StateStopping:
				if err := o.cancelAndQuiesce(opCtx, latest.FlowID, latest.Generation); err != nil {
					return err
				}
				if o.resourceCleaner != nil {
					stopping, getErr := o.base.Get(opCtx, latest.FlowID)
					if getErr != nil {
						return getErr
					}
					if err := o.resourceCleaner.CleanupSourceResources(opCtx, stopping, latest.Generation); err != nil {
						return err
					}
				}
				_, err = o.base.CompleteStopGeneration(opCtx, latest.FlowID, latest.Generation)
				return err
			default:
				return nil
			}
		})
		cancel()
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("reconcile flow %s: %w", control.FlowID, err))
		}
	}
	return joined
}

func (o *OrchestratedEngine) RunReconciler(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	if err := o.ReconcileOnce(ctx); err != nil && ctx.Err() == nil {
		log.Printf("workflow lifecycle reconciliation: %v", err)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := o.ReconcileOnce(ctx); err != nil && ctx.Err() == nil {
				log.Printf("workflow lifecycle reconciliation: %v", err)
			}
		}
	}
}

// Generation-aware execution methods are delegated without capability probes.
func (o *OrchestratedEngine) WithFlowLock(ctx context.Context, flowID string, try bool, fn func() error) (bool, error) {
	return o.base.WithFlowLock(ctx, flowID, try, fn)
}
func (o *OrchestratedEngine) Control(ctx context.Context, flowID string) (LifecycleControl, error) {
	return o.base.Control(ctx, flowID)
}
func (o *OrchestratedEngine) CheckLegacySourceResourceMutation(ctx context.Context, sourceSystemID, databaseName, resourceKind, physicalName string) error {
	guard, ok := o.base.(interface {
		CheckLegacySourceResourceMutation(context.Context, string, string, string, string) error
	})
	if !ok {
		return nil
	}
	return guard.CheckLegacySourceResourceMutation(ctx, sourceSystemID, databaseName, resourceKind, physicalName)
}
func (o *OrchestratedEngine) RequestPause(ctx context.Context, flowID string) (flow.Flow, LifecycleControl, error) {
	return o.base.RequestPause(ctx, flowID)
}
func (o *OrchestratedEngine) RegisterExecutionGeneration(ctx context.Context, flowID, executionID, backend string, generation int64, lease time.Duration) error {
	return o.base.RegisterExecutionGeneration(ctx, flowID, executionID, backend, generation, lease)
}
func (o *OrchestratedEngine) RegisterExecutionFence(ctx context.Context, flowID, executionID, backend string, generation int64, lease time.Duration) (ExecutionFence, error) {
	return o.base.RegisterExecutionFence(ctx, flowID, executionID, backend, generation, lease)
}
func (o *OrchestratedEngine) RenewExecutionFence(ctx context.Context, fence ExecutionFence, lease time.Duration) error {
	return o.base.RenewExecutionFence(ctx, fence, lease)
}
func (o *OrchestratedEngine) FinishExecutionFence(ctx context.Context, fence ExecutionFence, reason string) error {
	return o.base.FinishExecutionFence(ctx, fence, reason)
}
func (o *OrchestratedEngine) FailExecutionFence(ctx context.Context, fence ExecutionFence, reason string) error {
	return o.base.FailExecutionFence(ctx, fence, reason)
}
func (o *OrchestratedEngine) RenewExecution(ctx context.Context, flowID, executionID string, generation int64, lease time.Duration) error {
	return o.base.RenewExecution(ctx, flowID, executionID, generation, lease)
}
func (o *OrchestratedEngine) FinishExecutionReason(ctx context.Context, flowID, executionID, reason string) error {
	return o.base.FinishExecutionReason(ctx, flowID, executionID, reason)
}
func (o *OrchestratedEngine) recordTransition(ctx context.Context, fromState, toState string) {
	if o.meters == nil || toState == "" {
		return
	}
	if fromState == "" {
		fromState = "unknown"
	}
	o.meters.RecordFlowStateTransition(ctx, fromState, toState)
}
func (o *OrchestratedEngine) getState(ctx context.Context, flowID string) string {
	current, err := o.base.Get(ctx, flowID)
	if err != nil {
		return ""
	}
	return string(current.State)
}

var _ Engine = (*OrchestratedEngine)(nil)
var _ ExecutionEngine = (*OrchestratedEngine)(nil)
