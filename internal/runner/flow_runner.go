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
	"github.com/josephjohncox/wallaby/internal/tablemap"
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
	PruneTerminalDeliveryState(context.Context, authority.RunFence, time.Duration, int) (int64, error)
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
	Artifacts          ArtifactLogFactory
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
	managed := connector.IsManagedSourceSpec(f.Source)
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
	var executionFence *workflow.ExecutionFence
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

	ddlExecutions := r.DDLExecutions
	if managed {
		binder, ok := source.(connector.RunFenceBinder)
		if !ok {
			_ = r.Authority.FinishProducer(context.WithoutCancel(ctx), *runFence, "admission_rejected")
			return errors.New("managed source does not accept the acquired RunFence")
		}
		if err := binder.BindRunFence(*runFence); err != nil {
			_ = r.Authority.FinishProducer(context.WithoutCancel(ctx), *runFence, "admission_rejected")
			return fmt.Errorf("bind managed source run fence: %w", err)
		}
		if r.DDLExecutions != nil {
			registryStore, ok := r.DDLExecutions.(*registry.PostgresStore)
			if !ok {
				_ = r.Authority.FinishProducer(context.WithoutCancel(ctx), *runFence, "admission_rejected")
				return errors.New("managed DDL execution store cannot bind the acquired RunFence")
			}
			ddlExecutions, err = registryStore.ForRunFence(*runFence)
			if err != nil {
				_ = r.Authority.FinishProducer(context.WithoutCancel(ctx), *runFence, "admission_rejected")
				return fmt.Errorf("bind managed DDL run fence: %w", err)
			}
		}
	}

	var artifactLog stream.ManagedArtifactLog
	if managed && f.Config.AckPolicy == stream.AckPolicyMaterialized {
		if len(destinations) != 1 || destinations[0].Spec.Type != connector.EndpointIceberg {
			_ = r.Authority.FinishProducer(context.WithoutCancel(ctx), *runFence, "admission_rejected")
			return errors.New("materialized projection requires exactly one Iceberg destination projector")
		}
		destinations = append([]stream.DestinationConfig(nil), destinations...)
		projector, projectErr := tablemap.New(f.Config.TableMappings, destinations[0].Spec.Name)
		if projectErr != nil {
			_ = r.Authority.FinishProducer(context.WithoutCancel(ctx), *runFence, "admission_rejected")
			return fmt.Errorf("construct materialized Iceberg projector: %w", projectErr)
		}
		destinations[0].Projector = projector
		destinations[0].MappingFingerprint = projector.Fingerprint()
		if r.Artifacts == nil {
			_ = r.Authority.FinishProducer(context.WithoutCancel(ctx), *runFence, "admission_rejected")
			return errors.New("ack_policy=materialized requires artifact publication deployment config")
		}
		artifactLog, err = r.Artifacts(ctx, f, destinations)
		if err != nil {
			_ = r.Authority.FinishProducer(context.WithoutCancel(ctx), *runFence, "admission_rejected")
			return fmt.Errorf("build canonical artifact log: %w", err)
		}
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
		DDLExecutions:       ddlExecutions,
		TraceSink:           r.TraceSink,
		RunFence:            runFence,
		DeliveryCoordinator: r.Deliveries,
		ArtifactLog:         artifactLog,
	})
	if err != nil {
		if runFence != nil {
			_ = r.Authority.FinishProducer(context.WithoutCancel(ctx), *runFence, "admission_rejected")
		}
		return err
	}
	deliveryRetention := 7 * 24 * time.Hour
	deliveryPruneInterval := time.Minute
	if managed {
		destinationSpec := streamRunner.Destinations[0].Spec
		revisionID := strings.TrimSpace(destinationSpec.Options["destination_revision_id"])
		fingerprint, fingerprintErr := connector.DeliveryConfigFingerprint(destinationSpec, streamRunner.Destinations[0].MappingFingerprint)
		// A materialized runtime that owns a deployment-merged catalog identity the
		// flow spec cannot express (today: Iceberg) reports it here and it wins. A
		// runtime with no such consumer reports the empty string, and the
		// spec-derived fingerprint remains authoritative; construction already fails
		// closed if a catalog consumer exists without an effective identity.
		if identity, ok := artifactLog.(stream.ManagedArtifactIdentity); ok {
			if effective := strings.TrimSpace(identity.EffectiveDestinationFingerprint()); effective != "" {
				fingerprint, fingerprintErr = connector.BindProjectionFingerprint(effective, streamRunner.Destinations[0].MappingFingerprint)
			}
		}
		if fingerprintErr == nil {
			fingerprintErr = r.Deliveries.RegisterDestinationRevision(ctx, *runFence, revisionID, destinationSpec.Name, fingerprint)
		}
		if fingerprintErr != nil {
			_ = r.Authority.FinishProducer(context.WithoutCancel(ctx), *runFence, "admission_rejected")
			return fingerprintErr
		}
		if raw := strings.TrimSpace(f.Source.Options["delivery_retention"]); raw != "" {
			parsed, parseErr := time.ParseDuration(raw)
			if parseErr != nil || parsed <= 0 {
				_ = r.Authority.FinishProducer(context.WithoutCancel(ctx), *runFence, "admission_rejected")
				return fmt.Errorf("parse managed delivery_retention %q", raw)
			}
			deliveryRetention = parsed
		}
		if raw := strings.TrimSpace(f.Source.Options["delivery_prune_interval"]); raw != "" {
			parsed, parseErr := time.ParseDuration(raw)
			if parseErr != nil || parsed <= 0 {
				_ = r.Authority.FinishProducer(context.WithoutCancel(ctx), *runFence, "admission_rejected")
				return fmt.Errorf("parse managed delivery_prune_interval %q", raw)
			}
			deliveryPruneInterval = parsed
		}
		renewDuringPrune := func(pruneCtx context.Context) error {
			return r.Authority.RenewProducer(pruneCtx, *runFence, executionLease)
		}
		if pruneErr := pruneManagedDeliveryState(ctx, r.Deliveries, *runFence, deliveryRetention, renewDuringPrune); pruneErr != nil {
			_ = r.Authority.FinishProducer(context.WithoutCancel(ctx), *runFence, "admission_rejected")
			return pruneErr
		}
	}
	if !managed {
		registered, err := r.Engine.RegisterExecutionFence(ctx, f.ID, executionID, backend, generation, executionLease)
		if err != nil {
			return err
		}
		executionFence = &registered
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
		if executionFence != nil {
			_ = r.Engine.FinishExecutionFence(context.WithoutCancel(ctx), *executionFence, reason)
		}
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
		heartbeatTicker := time.NewTicker(executionHeartbeat)
		defer heartbeatTicker.Stop()
		var pruneTicker *time.Ticker
		var pruneC <-chan time.Time
		if runFence != nil {
			pruneTicker = time.NewTicker(deliveryPruneInterval)
			pruneC = pruneTicker.C
			defer pruneTicker.Stop()
		}
		fail := func(err error) {
			select {
			case watchErr <- err:
			default:
			}
			cancelRun()
		}
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-heartbeatTicker.C:
				var renewErr error
				if runFence != nil {
					renewErr = r.Authority.RenewProducer(watchCtx, *runFence, executionLease)
				} else if executionFence != nil {
					renewErr = r.Engine.RenewExecutionFence(watchCtx, *executionFence, executionLease)
				}
				if renewErr != nil {
					fail(fmt.Errorf("renew execution authority: %w", renewErr))
					return
				}
			case <-pruneC:
				renewDuringPrune := func(pruneCtx context.Context) error {
					return r.Authority.RenewProducer(pruneCtx, *runFence, executionLease)
				}
				if err := pruneManagedDeliveryState(watchCtx, r.Deliveries, *runFence, deliveryRetention, renewDuringPrune); err != nil {
					fail(fmt.Errorf("prune managed delivery state: %w", err))
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
			if executionFence != nil {
				if finishErr := r.Engine.FinishExecutionFence(context.WithoutCancel(ctx), *executionFence, "ddl_gated"); finishErr != nil {
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
			case watchFailure := <-watchErr:
				return watchFailure
			default:
			}
		}
		span.RecordError(err)
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			if runFence != nil {
				if !errors.Is(err, connector.ErrDeliveryIndeterminate) && !errors.Is(err, authority.ErrFenceRejected) && !errors.Is(err, authority.ErrLeaseExpired) {
					_ = r.Authority.FailFlow(context.WithoutCancel(ctx), *runFence, err.Error())
				}
			} else if executionFence != nil {
				_ = r.Engine.FailExecutionFence(context.WithoutCancel(ctx), *executionFence, err.Error())
			}
		}
		return err
	}
	return nil
}

func pruneManagedDeliveryState(ctx context.Context, deliveries managedDeliveryRuntime, fence authority.RunFence, retention time.Duration, renew func(context.Context) error) error {
	const (
		pruneBatchSize  = 1000
		maxPruneBatches = 8
	)
	for batch := 0; batch < maxPruneBatches; batch++ {
		pruned, err := deliveries.PruneTerminalDeliveryState(ctx, fence, retention, pruneBatchSize)
		if err != nil {
			return err
		}
		if pruned < pruneBatchSize {
			return nil
		}
		if renew != nil {
			if err := renew(ctx); err != nil {
				return fmt.Errorf("renew producer while pruning managed delivery state: %w", err)
			}
		}
	}
	return nil
}
