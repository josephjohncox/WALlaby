package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/internal/telemetry"
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

// FlowRunInput is the generation-fenced workflow input for one flow.
type FlowRunInput struct {
	FlowID        string `json:"flow_id"`
	Generation    int64  `json:"generation"`
	MaxEmptyReads int    `json:"max_empty_reads,omitempty"`
}

// DBOSOrchestrator schedules flow runs via DBOS.
type DBOSOrchestrator struct {
	ctx           dbos.DBOSContext
	engine        workflow.LifecycleStore
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
func NewDBOSOrchestrator(ctx context.Context, cfg Config, engine workflow.LifecycleStore, checkpoints connector.CheckpointStore, factory runner.Factory) (*DBOSOrchestrator, error) {
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

// EnqueueGeneration idempotently schedules one (flow,generation).
func (o *DBOSOrchestrator) EnqueueGeneration(_ context.Context, flowID string, generation int64) error {
	if flowID == "" || generation <= 0 {
		return errors.New("flow id and positive generation are required")
	}
	identity := fmt.Sprintf("%sg-%d", flowWorkflowPrefix(flowID), generation)
	return o.enqueueWorkflow(flowID, generation, identity)
}

func (o *DBOSOrchestrator) enqueueWorkflow(flowID string, generation int64, identity string) error {
	input := FlowRunInput{FlowID: flowID, Generation: generation, MaxEmptyReads: o.maxEmptyReads}
	opts := []dbos.WorkflowOption{dbos.WithWorkflowID(identity), dbos.WithDeduplicationID(identity)}
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

// EnqueueRunOnce schedules one uniquely identified attempt against the
// lifecycle generation captured by the caller.
func (o *DBOSOrchestrator) EnqueueRunOnce(_ context.Context, flowID string, generation int64) error {
	if flowID == "" || generation <= 0 {
		return errors.New("flow id and positive generation are required")
	}
	identity := fmt.Sprintf("%sg-%d-r-%s", flowWorkflowPrefix(flowID), generation, uuid.NewString())
	return o.enqueueWorkflow(flowID, generation, identity)
}

// CancelThroughGeneration cancels and then polls every relevant workflow until
// DBOS reports a terminal status. Absence from only the runnable status set is
// not treated as terminal proof.
func (o *DBOSOrchestrator) CancelThroughGeneration(ctx context.Context, flowID string, generation int64) (workflow.CancellationReceipt, error) {
	receipt := workflow.CancellationReceipt{ThroughGeneration: generation, Backend: "dbos"}
	if flowID == "" {
		return receipt, errors.New("flow id is required")
	}
	prefix := flowWorkflowPrefix(flowID)
	for {
		workflows, err := dbos.ListWorkflows(o.ctx, dbos.WithWorkflowIDPrefix(prefix))
		if err != nil {
			return receipt, fmt.Errorf("list dbos flow workflows: %w", err)
		}
		terminalIDs, cancellableIDs, remaining := classifyDBOSWorkflows(prefix, generation, workflows)
		for _, workflowID := range cancellableIDs {
			if err := dbos.CancelWorkflow(o.ctx, workflowID); err != nil {
				return receipt, fmt.Errorf("cancel dbos workflow %s: %w", workflowID, err)
			}
		}
		if remaining == 0 {
			// DBOS can publish CANCELLED before an in-process function unwinds.
			// The lifecycle store therefore additionally requires the exact row's
			// lease to expire before accepting this backend-terminal observation.
			sort.Strings(terminalIDs)
			receipt.Terminal = true
			receipt.TerminalExecutionIDs = terminalIDs
			return receipt, nil
		}
		select {
		case <-ctx.Done():
			return receipt, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
func (o *DBOSOrchestrator) CancelFlow(ctx context.Context, flowID string) error {
	control, err := o.engine.Control(ctx, flowID)
	if err != nil {
		return err
	}
	_, err = o.CancelThroughGeneration(ctx, flowID, control.Generation)
	return err
}

func classifyDBOSWorkflows(prefix string, generation int64, workflows []dbos.WorkflowStatus) (terminalIDs, cancellableIDs []string, remaining int) {
	for _, item := range workflows {
		itemGeneration, ok := dbosWorkflowGeneration(prefix, item.ID)
		if !ok || itemGeneration > generation {
			continue
		}
		switch item.Status {
		case dbos.WorkflowStatusSuccess, dbos.WorkflowStatusError, dbos.WorkflowStatusCancelled, dbos.WorkflowStatusMaxRecoveryAttemptsExceeded:
			terminalIDs = append(terminalIDs, item.ID)
		case dbos.WorkflowStatusPending, dbos.WorkflowStatusEnqueued:
			cancellableIDs = append(cancellableIDs, item.ID)
			remaining++
		default:
			remaining++
		}
	}
	sort.Strings(terminalIDs)
	sort.Strings(cancellableIDs)
	return terminalIDs, cancellableIDs, remaining
}

func dbosWorkflowGeneration(prefix, workflowID string) (int64, bool) {
	var generation int64
	if _, err := fmt.Sscanf(strings.TrimPrefix(workflowID, prefix), "g-%d", &generation); err != nil || generation <= 0 || !strings.HasPrefix(workflowID, prefix) {
		return 0, false
	}
	return generation, true
}

func flowWorkflowPrefix(flowID string) string {
	hash := sha256.Sum256([]byte(flowID))
	return "wallaby-flow-" + sanitizeName(flowID) + "-" + hex.EncodeToString(hash[:8]) + "-"
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
	if input.FlowID == "" || input.Generation <= 0 {
		return "", errors.New("flow id and positive generation are required")
	}
	f, err := o.engine.Get(ctx, input.FlowID)
	if err != nil {
		return "", err
	}
	control, err := o.engine.Control(ctx, input.FlowID)
	if err != nil {
		return "", err
	}
	if f.State != flow.StateRunning || control.Target != workflow.TargetRunning || control.Generation != input.Generation {
		return "fenced", nil
	}
	executionID, err := dbos.GetWorkflowID(ctx)
	if err != nil {
		return "", fmt.Errorf("get dbos workflow id: %w", err)
	}
	source, err := o.factory.SourceForFlow(f)
	if err != nil {
		return "", err
	}
	destinations, err := o.factory.DestinationsForFlow(f)
	if err != nil {
		return "", err
	}
	maxEmptyReads := input.MaxEmptyReads
	if maxEmptyReads == 0 {
		maxEmptyReads = o.maxEmptyReads
	}
	tracer := o.tracer
	if tracer == nil {
		tracer = otel.Tracer("wallaby/dbos")
	}
	traceSink, traceClose, err := o.flowTraceSink(f.ID)
	if err != nil {
		return "", err
	}
	if traceClose != nil {
		defer func() { _ = traceClose() }()
	}
	flowRunner := runner.FlowRunner{
		Engine: o.engine, Checkpoints: o.checkpoints, Tracer: tracer, Meters: o.meters,
		WireFormat: o.defaultWire, StrictWire: o.strictWire, MaxEmpty: maxEmptyReads,
		DDLApplied: o.ddlApplied, TraceSink: traceSink, ExecutionBackend: "dbos",
		ExecutionID: executionID, ExpectedGeneration: input.Generation,
	}
	if err := flowRunner.Run(ctx, f, source, destinations); err != nil {
		return "", fmt.Errorf("run flow %s generation %d: %w", f.ID, input.Generation, err)
	}
	return "ok", nil
}

func (o *DBOSOrchestrator) dispatchWorkflow(ctx dbos.DBOSContext, scheduledAt time.Time) (string, error) {
	flows, err := o.engine.List(ctx)
	if err != nil {
		return "", err
	}

	count := 0
	for _, f := range flows {
		if f.State != flow.StateRunning {
			continue
		}
		control, err := o.engine.Control(ctx, f.ID)
		if err != nil {
			return "", err
		}
		if control.Target != workflow.TargetRunning {
			continue
		}
		// Scheduled finite runs share the lifecycle generation but need a
		// deterministic identity per schedule occurrence.
		identity := fmt.Sprintf("%sg-%d-s-%d", flowWorkflowPrefix(f.ID), control.Generation, scheduledAt.UnixNano())
		if err := o.enqueueWorkflow(f.ID, control.Generation, identity); err != nil {
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
