package runner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/authority"
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

type managedDeliveryRuntime interface {
	stream.ManagedDeliveryCoordinator
	RegisterDestinationRevision(context.Context, authority.RunFence, string, string, string) error
}

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
	Authority          authority.Store
	Deliveries         managedDeliveryRuntime
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
	managed := managedSourceSpec(f.Source)
	generation := r.ExpectedGeneration
	if managed && generation <= 0 {
		return errors.New("managed flow execution requires an explicit positive lifecycle generation")
	}
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
	var runFence *authority.RunFence
	if managed {
		if r.Authority == nil || r.Deliveries == nil {
			return errors.New("managed flow runner requires PostgreSQL authority and delivery coordinator")
		}
		acquired, err := r.Authority.AcquireProducer(ctx, f.ID, executionID, backend, generation, executionLease)
		if err != nil {
			return err
		}
		runFence = &acquired
	}

	// Validate data-plane dependencies before opening any connector. Managed
	// construction receives the exact acquired fence.
	streamRunner, err := NewStreamRunner(f, source, destinations, StreamRunnerConfig{
		Checkpoints:         r.Checkpoints,
		Tracer:              r.Tracer,
		Meters:              r.Meters,
		DefaultWireFormat:   r.WireFormat,
		StrictFormat:        r.StrictWire,
		MaxEmptyReads:       r.MaxEmpty,
		DefaultParallelism:  r.Parallelism,
		ResolveStaging:      r.ResolveStaging,
		DDLExecutions:       r.DDLExecutions,
		TraceSink:           r.TraceSink,
		RunFence:            runFence,
		DeliveryCoordinator: r.Deliveries,
	})
	if err != nil {
		if runFence != nil {
			_ = r.Authority.FinishProducer(context.WithoutCancel(ctx), *runFence, "admission_rejected")
		}
		return err
	}
	if managed {
		destinationSpec := streamRunner.Destinations[0].Spec
		revisionID := strings.TrimSpace(destinationSpec.Options["destination_revision_id"])
		fingerprint, fingerprintErr := connector.DeliveryConfigFingerprint(destinationSpec)
		if fingerprintErr == nil {
			fingerprintErr = r.Deliveries.RegisterDestinationRevision(ctx, *runFence, revisionID, destinationSpec.Name, fingerprint)
		}
		if fingerprintErr != nil {
			_ = r.Authority.FinishProducer(context.WithoutCancel(ctx), *runFence, "admission_rejected")
			return fingerprintErr
		}
	}
	if !managed {
		if err := r.Engine.RegisterExecutionGeneration(ctx, f.ID, executionID, backend, generation, executionLease); err != nil {
			return err
		}
	}
	defer func() {
		reason := "completed"
		if runErr != nil {
			reason = "error"
		}
		if runFence != nil {
			_ = r.Authority.FinishProducer(context.WithoutCancel(ctx), *runFence, reason)
			return
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
	watchErr := make(chan error, 1)
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
				var renewErr error
				if runFence != nil {
					renewErr = r.Authority.RenewProducer(watchCtx, *runFence, executionLease)
				} else {
					renewErr = r.Engine.RenewExecution(watchCtx, f.ID, executionID, generation, executionLease)
				}
				if renewErr != nil {
					select {
					case watchErr <- renewErr:
					default:
					}
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
			if runFence != nil {
				if finishErr := r.Authority.FinishProducer(context.WithoutCancel(ctx), *runFence, "ddl_gated"); finishErr != nil {
					return finishErr
				}
			}
			if runFence == nil {
				if finishErr := r.Engine.FinishExecutionReason(context.WithoutCancel(ctx), f.ID, executionID, "ddl_gated"); finishErr != nil {
					return finishErr
				}
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
		if errors.Is(err, context.Canceled) {
			select {
			case renewErr := <-watchErr:
				return fmt.Errorf("renew execution authority: %w", renewErr)
			default:
			}
		}
		span.RecordError(err)
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			if runFence != nil {
				if !errors.Is(err, connector.ErrDeliveryIndeterminate) && !errors.Is(err, authority.ErrFenceRejected) && !errors.Is(err, authority.ErrLeaseExpired) {
					_ = r.Authority.FailFlow(context.WithoutCancel(ctx), *runFence, err.Error())
				}
			} else {
				_, _ = r.Engine.Fail(context.WithoutCancel(ctx), f.ID)
			}
		}
		return err
	}
	return nil
}
