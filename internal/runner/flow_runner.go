package runner

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/registry"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const (
	executionLease     = 15 * time.Second
	executionHeartbeat = 250 * time.Millisecond
)

// FlowRunner executes one already-running flow. It never changes desired
// lifecycle state merely because a finite run completed successfully.
type FlowRunner struct {
	Engine             workflow.ExecutionEngine
	Checkpoints        connector.CheckpointStore
	Tracer             trace.Tracer
	Meters             *telemetry.Meters
	WireFormat         connector.WireFormat
	StrictWire         bool
	MaxEmpty           int
	Parallelism        int
	ResolveStaging     bool
	DDLExecutions      stream.DDLExecutionStore
	TraceSink          stream.TraceSink
	ExecutionBackend   string
	ExecutionID        string
	ExpectedGeneration int64
}

func (r *FlowRunner) Run(ctx context.Context, f flow.Flow, source connector.Source, destinations []stream.DestinationConfig) (runErr error) {
	if r.Engine == nil {
		return errors.New("workflow execution engine is required")
	}
	current, err := r.Engine.Get(ctx, f.ID)
	if err != nil {
		return err
	}
	if current.State != flow.StateRunning {
		return fmt.Errorf("%w: cannot execute flow in state %s", workflow.ErrInvalidState, current.State)
	}
	control, err := r.Engine.Control(ctx, f.ID)
	if err != nil {
		return err
	}
	generation := r.ExpectedGeneration
	if generation == 0 {
		generation = control.Generation
	}

	executionID := r.ExecutionID
	if executionID == "" {
		executionID = uuid.NewString()
	}
	backend := r.ExecutionBackend
	if backend == "" {
		backend = "worker"
	}
	// Validate data-plane dependencies before registering an execution or
	// opening any connector.
	streamRunner, err := NewStreamRunner(f, source, destinations, StreamRunnerConfig{
		Checkpoints:        r.Checkpoints,
		Tracer:             r.Tracer,
		Meters:             r.Meters,
		DefaultWireFormat:  r.WireFormat,
		StrictFormat:       r.StrictWire,
		MaxEmptyReads:      r.MaxEmpty,
		DefaultParallelism: r.Parallelism,
		ResolveStaging:     r.ResolveStaging,
		DDLExecutions:      r.DDLExecutions,
		TraceSink:          r.TraceSink,
	})
	if err != nil {
		return err
	}
	if err := r.Engine.RegisterExecutionGeneration(ctx, f.ID, executionID, backend, generation, executionLease); err != nil {
		return err
	}
	defer func() {
		reason := "completed"
		if runErr != nil {
			reason = "error"
		}
		_ = r.Engine.FinishExecutionReason(context.WithoutCancel(ctx), f.ID, executionID, reason)
	}()

	tracer := r.Tracer
	if tracer == nil {
		tracer = otel.Tracer("wallaby/flow")
	}
	flowSpanCtx, span := tracer.Start(ctx, "flow.run")
	defer span.End()

	runCtx, cancelRun := context.WithCancel(flowSpanCtx)
	watchCtx, stopWatcher := context.WithCancel(flowSpanCtx)
	var lifecycleStopped atomic.Bool
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		ticker := time.NewTicker(executionHeartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
				if renewErr := r.Engine.RenewExecution(watchCtx, f.ID, executionID, generation, executionLease); renewErr != nil {
					lifecycleStopped.Store(true)
					cancelRun()
					return
				}
			}
		}
	}()
	defer func() {
		stopWatcher()
		cancelRun()
		<-watchDone
	}()

	// The durable copy is authoritative only for lifecycle eligibility. The
	// caller's flow is the execution spec and may contain run-scoped overrides.

	if err := streamRunner.Run(runCtx); err != nil {
		if errors.Is(err, registry.ErrApprovalRequired) {
			// Finish first so a concurrent Pause/Stop holding the lifecycle lock
			// cannot deadlock while it waits for this execution to quiesce.
			if finishErr := r.Engine.FinishExecutionReason(context.WithoutCancel(ctx), f.ID, executionID, "ddl_gated"); finishErr != nil {
				return finishErr
			}
			intentCtx, cancelIntent := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelIntent()
			_, lockErr := r.Engine.WithFlowLock(intentCtx, f.ID, false, func() error {
				_, _, requestErr := r.Engine.RequestPause(intentCtx, f.ID)
				return requestErr
			})
			if lockErr != nil {
				return lockErr
			}
			return nil
		}
		if lifecycleStopped.Load() && errors.Is(err, context.Canceled) {
			return nil
		}
		span.RecordError(err)
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			_, _ = r.Engine.Fail(context.WithoutCancel(ctx), f.ID)
		}
		return err
	}
	return nil
}
