package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/registry"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Config configures the DBOS orchestrator.
type Config struct {
	AppName       string
	DatabaseURL   string
	Queue         string
	Schedule      string
	MaxEmptyReads int
	MaxRetries    int
	MaxRetriesSet bool
	DefaultWire   connector.WireFormat
	StrictWire    bool
	AdminServer   bool
	AdminPort     int
	Tracer        trace.Tracer
	Meters        *telemetry.Meters
	DDLApplied    func(ctx context.Context, flowID string, lsn string, ddl string) error
	TraceSink     stream.TraceSink
	TracePath     string
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
	maxRetries    int
	maxRetriesSet bool
	defaultWire   connector.WireFormat
	strictWire    bool
	tracer        trace.Tracer
	meters        *telemetry.Meters
	ddlApplied    func(ctx context.Context, flowID string, lsn string, ddl string) error
	traceSink     stream.TraceSink
	tracePath     string
}

// FlowWorkflowName returns the fully qualified workflow name used by DBOS recovery.
func FlowWorkflowName() string {
	var o DBOSOrchestrator
	return runtime.FuncForPC(reflect.ValueOf((&o).runFlowWorkflow).Pointer()).Name()
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
		AppName:         cfg.AppName,
		DatabaseURL:     cfg.DatabaseURL,
		AdminServer:     cfg.AdminServer,
		AdminServerPort: cfg.AdminPort,
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
		maxRetries:    cfg.MaxRetries,
		maxRetriesSet: cfg.MaxRetriesSet,
		defaultWire:   cfg.DefaultWire,
		strictWire:    cfg.StrictWire,
		tracer:        cfg.Tracer,
		meters:        cfg.Meters,
		ddlApplied:    cfg.DDLApplied,
		traceSink:     cfg.TraceSink,
		tracePath:     cfg.TracePath,
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
	opts := []dbos.WorkflowRegistrationOption{}
	if o.maxRetriesSet {
		opts = append(opts, dbos.WithMaxRetries(o.maxRetries))
	}
	dbos.RegisterWorkflow(o.ctx, o.runFlowWorkflow, opts...)
	if schedule != "" {
		dispatchOpts := append([]dbos.WorkflowRegistrationOption{}, opts...)
		dispatchOpts = append(dispatchOpts, dbos.WithSchedule(schedule))
		dbos.RegisterWorkflow(o.ctx, o.dispatchWorkflow, dispatchOpts...)
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

	source, err := o.factory.SourceForFlow(f)
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

	// Build destination names for tracing
	destNames := make([]string, len(f.Destinations))
	for i, d := range f.Destinations {
		destNames[i] = d.Name
	}

	// Create parent span for the entire flow run
	flowCtx, flowSpan := tracer.Start(ctx, "flow.run",
		trace.WithAttributes(
			attribute.String("flow.id", f.ID),
			attribute.String("source.type", string(f.Source.Type)),
			attribute.Int("destinations.count", len(f.Destinations)),
			attribute.StringSlice("destinations.names", destNames),
		),
	)
	defer flowSpan.End()

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
		Meters:        o.meters,
	}
	if f.Config.AckPolicy != "" {
		runner.AckPolicy = f.Config.AckPolicy
	}
	if f.Config.PrimaryDestination != "" {
		runner.PrimaryDestination = f.Config.PrimaryDestination
	}
	if f.Config.FailureMode != "" {
		runner.FailureMode = f.Config.FailureMode
	}
	if f.Config.GiveUpPolicy != "" {
		runner.GiveUpPolicy = f.Config.GiveUpPolicy
	}
	traceSink, traceClose, err := o.flowTraceSink(f.ID)
	if err != nil {
		return "", err
	}
	if traceSink != nil {
		runner.TraceSink = traceSink
	}
	if traceClose != nil {
		defer func() { _ = traceClose() }()
	}
	if o.ddlApplied != nil {
		runner.DDLApplied = o.ddlApplied
	}
	if f.Parallelism > 0 {
		runner.Parallelism = f.Parallelism
	}

	if err := runner.Run(flowCtx); err != nil {
		if errors.Is(err, registry.ErrApprovalRequired) {
			_, _ = o.engine.Stop(ctx, f.ID)
			return "ddl gated", nil
		}
		flowSpan.RecordError(err)
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

func (o *DBOSOrchestrator) flowTraceSink(flowID string) (stream.TraceSink, func() error, error) {
	if o.tracePath == "" {
		return o.traceSink, nil, nil
	}
	path := strings.ReplaceAll(o.tracePath, "{flow_id}", flowID)
	if path == o.tracePath {
		path = fmt.Sprintf("%s.%s", o.tracePath, flowID)
	}
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, nil, fmt.Errorf("create trace dir: %w", err)
		}
	}
	// #nosec G304 -- trace path is configured by the operator.
	file, err := os.Create(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open trace file: %w", err)
	}
	return stream.NewJSONTraceSink(file), file.Close, nil
}
