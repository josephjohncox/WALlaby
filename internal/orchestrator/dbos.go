package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/registry"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Config configures the DBOS orchestrator.
type Config struct {
	AppName       string
	DatabaseURL   string
	Queue         string
	Schedule      string
	MaxEmptyReads int
	DefaultWire   connector.WireFormat
	StrictWire    bool
	Tracer        trace.Tracer
}

// FlowRunInput is the workflow input for running a single flow.
type FlowRunInput struct {
	FlowID        string `json:"flow_id"`
	MaxEmptyReads int    `json:"max_empty_reads,omitempty"`
}

// DBOSOrchestrator schedules flow runs via DBOS.
type DBOSOrchestrator struct {
	ctx           dbos.DBOSContext
	engine        workflow.Engine
	checkpoints   connector.CheckpointStore
	factory       runner.Factory
	queue         string
	maxEmptyReads int
	defaultWire   connector.WireFormat
	strictWire    bool
	tracer        trace.Tracer
}

// NewDBOSOrchestrator builds and launches a DBOS-backed orchestrator.
func NewDBOSOrchestrator(ctx context.Context, cfg Config, engine workflow.Engine, checkpoints connector.CheckpointStore, factory runner.Factory) (*DBOSOrchestrator, error) {
	if engine == nil {
		return nil, errors.New("workflow engine is required")
	}
	if cfg.DatabaseURL == "" {
		return nil, errors.New("dbos database url is required")
	}
	if cfg.AppName == "" {
		return nil, errors.New("dbos app name is required")
	}

	dbosCtx, err := dbos.NewDBOSContext(ctx, dbos.Config{
		AppName:     cfg.AppName,
		DatabaseURL: cfg.DatabaseURL,
	})
	if err != nil {
		return nil, err
	}

	if cfg.Queue != "" {
		dbos.NewWorkflowQueue(dbosCtx, cfg.Queue)
	}

	orchestrator := &DBOSOrchestrator{
		ctx:           dbosCtx,
		engine:        engine,
		checkpoints:   checkpoints,
		factory:       factory,
		queue:         cfg.Queue,
		maxEmptyReads: cfg.MaxEmptyReads,
		defaultWire:   cfg.DefaultWire,
		strictWire:    cfg.StrictWire,
		tracer:        cfg.Tracer,
	}

	orchestrator.registerWorkflows(cfg.Schedule)
	if err := dbosCtx.Launch(); err != nil {
		return nil, err
	}

	return orchestrator, nil
}

// EnqueueFlow schedules a flow run in the DBOS queue.
func (o *DBOSOrchestrator) EnqueueFlow(_ context.Context, flowID string) error {
	if flowID == "" {
		return errors.New("flow id is required")
	}

	input := FlowRunInput{
		FlowID:        flowID,
		MaxEmptyReads: o.maxEmptyReads,
	}

	opts := []dbos.WorkflowOption{
		dbos.WithDeduplicationID(flowID),
	}
	if o.queue != "" {
		opts = append(opts, dbos.WithQueue(o.queue))
	}

	_, err := dbos.RunWorkflow(o.ctx, o.runFlowWorkflow, input, opts...)
	if err == nil {
		return nil
	}

	var dbosErr *dbos.DBOSError
	if errors.As(err, &dbosErr) && dbosErr.Code == dbos.QueueDeduplicated {
		return nil
	}
	return err
}

// Shutdown stops the DBOS runtime.
func (o *DBOSOrchestrator) Shutdown(timeout time.Duration) {
	if o.ctx != nil {
		o.ctx.Shutdown(timeout)
	}
}

func (o *DBOSOrchestrator) registerWorkflows(schedule string) {
	dbos.RegisterWorkflow(o.ctx, o.runFlowWorkflow)
	if schedule != "" {
		dbos.RegisterWorkflow(o.ctx, o.dispatchWorkflow, dbos.WithSchedule(schedule))
	}
}

func (o *DBOSOrchestrator) runFlowWorkflow(ctx dbos.DBOSContext, input FlowRunInput) (string, error) {
	if input.FlowID == "" {
		return "", errors.New("flow id is required")
	}

	f, err := o.engine.Get(ctx, input.FlowID)
	if err != nil {
		return "", err
	}
	if f.State != flow.StateRunning {
		return "skipped", nil
	}

	source, err := o.factory.Source(f.Source)
	if err != nil {
		return "", err
	}

	destinations, err := o.factory.Destinations(f.Destinations)
	if err != nil {
		return "", err
	}

	wireFormat := f.WireFormat
	if wireFormat == "" && o.defaultWire != "" {
		wireFormat = o.defaultWire
	}

	maxEmptyReads := input.MaxEmptyReads
	if maxEmptyReads == 0 {
		maxEmptyReads = o.maxEmptyReads
	}

	sourceSpec := f.Source
	if maxEmptyReads > 0 {
		if sourceSpec.Options == nil {
			sourceSpec.Options = map[string]string{}
		}
		if sourceSpec.Options["emit_empty"] == "" {
			sourceSpec.Options["emit_empty"] = "true"
		}
	}

	tracer := o.tracer
	if tracer == nil {
		tracer = otel.Tracer("wallaby/dbos")
	}

	runner := stream.Runner{
		Source:        source,
		SourceSpec:    sourceSpec,
		Destinations:  destinations,
		Checkpoints:   o.checkpoints,
		FlowID:        f.ID,
		WireFormat:    wireFormat,
		StrictFormat:  o.strictWire,
		MaxEmptyReads: maxEmptyReads,
		Tracer:        tracer,
	}
	if f.Parallelism > 0 {
		runner.Parallelism = f.Parallelism
	}

	if err := runner.Run(ctx); err != nil {
		if errors.Is(err, registry.ErrApprovalRequired) {
			return "ddl gated", nil
		}
		return "", fmt.Errorf("run flow %s: %w", f.ID, err)
	}

	return "ok", nil
}

func (o *DBOSOrchestrator) dispatchWorkflow(ctx dbos.DBOSContext, _ time.Time) (string, error) {
	flows, err := o.engine.List(ctx)
	if err != nil {
		return "", err
	}

	count := 0
	for _, f := range flows {
		if f.State != flow.StateRunning {
			continue
		}
		if err := o.EnqueueFlow(ctx, f.ID); err != nil {
			return "", err
		}
		count++
	}

	return fmt.Sprintf("scheduled %d flows", count), nil
}
