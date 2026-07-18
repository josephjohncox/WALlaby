package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/spec"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultRetryAttempts = 3
	defaultRetryBackoff  = 50 * time.Millisecond
)

var (
	ddlGatedCounter metric.Int64Counter
	ddlGatedOnce    sync.Once
)

// DestinationConfig binds a destination to its spec.
type DestinationConfig struct {
	Spec connector.Spec
	Dest connector.Destination
}

// StagingResolver is implemented by destinations that can resolve staging tables.
type StagingResolver interface {
	ResolveStaging(ctx context.Context) error
}

// StagingResolverFor lets destinations resolve staging tables for known schemas.
type StagingResolverFor interface {
	ResolveStagingFor(ctx context.Context, schemas []connector.Schema) error
}

// Runner streams data from a source to destinations.
type Runner struct {
	Source              connector.Source
	SourceSpec          connector.Spec
	Destinations        []DestinationConfig
	Checkpoints         connector.CheckpointStore
	CheckpointOutbox    connector.CheckpointOutboxStore
	FlowID              string
	ResolveStaging      bool
	Tracer              trace.Tracer
	Meters              *telemetry.Meters
	BatchTimeout        time.Duration
	MaxEmptyReads       int
	WireFormat          connector.WireFormat
	StrictFormat        bool
	Parallelism         int
	AckPolicy           AckPolicy
	PrimaryDestination  string
	RequireDDLExecution bool
	FailureMode         FailureMode
	GiveUpPolicy        GiveUpPolicy
	DDLApplied          func(ctx context.Context, flowID string, lsn string, ddl string) error
	TraceSink           TraceSink
}

// Run executes the streaming loop until context cancellation or error. It requires
// a stable flow ID and durable checkpoint storage before acknowledging the source.
func (r *Runner) Run(ctx context.Context) (retErr error) {
	if r.Source == nil {
		return errors.New("source is required")
	}
	if len(r.Destinations) == 0 {
		return errors.New("at least one destination is required")
	}
	if r.FlowID == "" {
		return errors.New("a non-empty flow id is required for durable checkpoints")
	}
	if r.effectiveAckPolicy() != AckPolicyPrimary && r.Checkpoints == nil {
		return errors.New("a durable checkpoint store is required before source acknowledgement")
	}
	if err := ValidateDestinationContracts(
		r.Destinations,
		r.effectiveAckPolicy(),
		r.PrimaryDestination,
		r.RequireDDLExecution,
	); err != nil {
		return fmt.Errorf("validate destination contracts: %w", err)
	}

	defer func() {
		if retErr == nil {
			return
		}
		if r.effectiveFailureMode() != FailureModeDropSlot {
			return
		}
		if errors.Is(retErr, context.Canceled) || errors.Is(retErr, context.DeadlineExceeded) {
			return
		}
		if dropper, ok := r.Source.(connector.SlotDropper); ok {
			if err := dropper.DropSlot(ctx); err != nil {
				retErr = fmt.Errorf("%w (drop slot failed: %s)", retErr, err.Error())
			}
		}
	}()

	tracer := r.Tracer
	if tracer == nil {
		tracer = otel.Tracer("wallaby/stream")
	}
	if err := r.normalizeWireFormat(); err != nil {
		return err
	}

	ackPolicy := r.effectiveAckPolicy()
	checkpointStore := r.Checkpoints
	var outbox connector.CheckpointOutboxStore
	var primary DestinationConfig
	var secondary []DestinationConfig
	if ackPolicy == AckPolicyPrimary {
		if r.FlowID == "" {
			return errors.New("primary acknowledgement requires a non-empty flow id")
		}
		outbox = r.CheckpointOutbox
		if outbox == nil {
			return errors.New("primary acknowledgement requires a durable checkpoint store with atomic outbox support")
		}
		checkpointStore = outbox
		var err error
		primary, secondary, err = r.partitionDestinations()
		if err != nil {
			return err
		}
	}
	if r.FlowID != "" {
		if r.SourceSpec.Options == nil {
			r.SourceSpec.Options = map[string]string{}
		}
		if r.SourceSpec.Options["flow_id"] == "" {
			r.SourceSpec.Options["flow_id"] = r.FlowID
		}
		for i := range r.Destinations {
			spec := r.Destinations[i].Spec
			if spec.Options == nil {
				spec.Options = map[string]string{}
			}
			if spec.Options["flow_id"] == "" {
				spec.Options["flow_id"] = r.FlowID
			}
			r.Destinations[i].Spec = spec
		}
	}

	var restoredCheckpoint *connector.Checkpoint
	ackRestoredCheckpoint := false
	explicitStartLSN := ""
	if r.SourceSpec.Options != nil {
		explicitStartLSN = r.SourceSpec.Options["start_lsn"]
	}
	if checkpointStore != nil && r.FlowID != "" {
		cp, err := checkpointStore.Get(ctx, r.FlowID)
		switch {
		case err == nil:
			restoredCheckpoint = &cp
			ackRestoredCheckpoint = explicitStartLSN == "" || checkpointPositionsEqual(explicitStartLSN, cp.LSN)
			if cp.LSN != "" {
				if r.SourceSpec.Options == nil {
					r.SourceSpec.Options = map[string]string{}
				}
				if explicitStartLSN == "" {
					r.SourceSpec.Options["start_lsn"] = cp.LSN
				}
			}
		case errors.Is(err, connector.ErrCheckpointNotFound):
			// A new flow has no restore position yet.
		default:
			return fmt.Errorf("restore checkpoint: %w", err)
		}
	}

	if err := r.Source.Open(ctx, r.SourceSpec); err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer func() { _ = r.Source.Close(ctx) }()

	for _, dest := range r.Destinations {
		if dest.Dest == nil {
			return errors.New("destination is required")
		}
		if err := dest.Dest.Open(ctx, dest.Spec); err != nil {
			return fmt.Errorf("open destination %s: %w", dest.Spec.Name, err)
		}
		defer func() { _ = dest.Dest.Close(ctx) }()
	}

	var secondaryQueues []*secondaryQueue
	if ackPolicy == AckPolicyPrimary {
		var err error
		secondaryQueues, err = r.restoreSecondaryQueues(ctx, outbox, secondary)
		if err != nil {
			return err
		}
		if err := r.flushSecondaryQueues(ctx, secondaryQueues); err != nil {
			return fmt.Errorf("drain restored primary-ack outbox: %w", err)
		}
	}

	if restoredCheckpoint != nil {
		r.emitCheckpointTrace(ctx, "restore_checkpoint", *restoredCheckpoint, "", spec.ActionNone, nil)
	}
	if restoredCheckpoint != nil && ackRestoredCheckpoint {
		if err := r.Source.Ack(ctx, *restoredCheckpoint); err != nil {
			r.emitCheckpointTrace(ctx, "restore_ack_error", *restoredCheckpoint, "", spec.ActionNone, err)
			r.Meters.RecordError(ctx, "source_restore_ack")
			return fmt.Errorf("ack restored checkpoint: %w", err)
		}
		r.emitCheckpointTrace(ctx, "restore_ack", *restoredCheckpoint, "", spec.ActionRestoreAck, nil)
	}

	emptyReads := 0
	readFailures := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if len(secondaryQueues) > 0 {
			if _, err := r.drainSecondaryQueues(ctx, secondaryQueues); err != nil {
				return err
			}
		}

		batchStart := time.Now()
		batchCtx, span := tracer.Start(ctx, "stream.batch",
			trace.WithNewRoot(),
			trace.WithAttributes(
				attribute.String("flow.id", r.FlowID),
				attribute.String("source.type", string(r.SourceSpec.Type)),
			),
		)

		readStart := time.Now()
		readCtx, readSpan := tracer.Start(batchCtx, "source.read")
		batch, err := r.Source.Read(readCtx)
		readSpan.End()
		if r.Meters != nil {
			r.Meters.RecordSourceReadLatency(ctx, float64(time.Since(readStart).Milliseconds()))
			if lagProvider, ok := r.Source.(connector.ReplicationLagProvider); ok {
				if slot, lagBytes, lagErr := lagProvider.ReplicationLag(ctx); lagErr == nil {
					r.Meters.RecordSourceLag(ctx, slot, lagBytes)
				}
			}
		}
		if err != nil {
			if errors.Is(err, connector.ErrDDLApprovalRequired) {
				r.handleDDLGate(batchCtx, span, err)
				span.End()
				return err
			}
			if errors.Is(err, io.EOF) && r.isBackfill() {
				span.End()
				if len(secondaryQueues) > 0 {
					if err := r.flushSecondaryQueues(ctx, secondaryQueues); err != nil {
						return err
					}
				}
				if r.ResolveStaging {
					if err := r.resolveStaging(batchCtx); err != nil {
						return err
					}
				}
				return nil
			}
			r.emitTrace(batchCtx, "read_error", "", "", spec.ActionReadFail, err)
			r.Meters.RecordError(ctx, "source_read")
			span.RecordError(err)
			span.End()
			readFailures++
			if r.shouldGiveUp(readFailures) {
				return err
			}
			if err := r.sleepRetry(ctx); err != nil {
				return err
			}
			continue
		}
		readFailures = 0
		if len(batch.Records) == 0 && !isControlCheckpoint(batch.Checkpoint) {
			// Sources may emit an empty heartbeat before they have a durable
			// position. It carries no data and must not be traced, persisted, or
			// acknowledged as a checkpoint.
			emptyReads++
			span.End()
			if r.MaxEmptyReads > 0 && emptyReads >= r.MaxEmptyReads {
				if len(secondaryQueues) > 0 {
					if err := r.flushSecondaryQueues(ctx, secondaryQueues); err != nil {
						return err
					}
				}
				if r.ResolveStaging && r.isBackfill() {
					if err := r.resolveStaging(batchCtx); err != nil {
						return err
					}
				}
				return nil
			}
			continue
		}

		tracePosition, positionErr := connector.CheckpointPositionID(batch.Checkpoint)
		if positionErr != nil {
			span.RecordError(positionErr)
			span.End()
			return fmt.Errorf("identify checkpoint: %w", positionErr)
		}
		if len(ddlRecordsInBatch(batch)) > 0 {
			r.emitCheckpointTrace(batchCtx, "read", batch.Checkpoint, tracePosition, spec.ActionReadDDL, nil)
		} else {
			r.emitCheckpointTrace(batchCtx, "read", batch.Checkpoint, tracePosition, spec.ActionReadBatch, nil)
		}
		if r.WireFormat != "" {
			batch.WireFormat = r.WireFormat
		}

		if len(batch.Records) == 0 {
			r.emitCheckpointTrace(batchCtx, "deliver", batch.Checkpoint, tracePosition, spec.ActionDeliver, nil)
			var err error
			if ackPolicy == AckPolicyPrimary {
				err = r.ackPrimaryAndOutbox(batchCtx, outbox, batch, nil, tracePosition, true)
			} else {
				err = r.ackAndCheckpoint(batchCtx, batch.Checkpoint, tracePosition, true)
			}
			if err != nil {
				span.RecordError(err)
				span.End()
				return err
			}
			span.End()
			continue
		}
		emptyReads = 0

		span.SetAttributes(
			attribute.Int("batch.records", len(batch.Records)),
			attribute.String("batch.schema", batch.Schema.Name),
		)

		ddlRecords := ddlRecordsInBatch(batch)

		if ackPolicy == AckPolicyPrimary {
			writeStart := time.Now()
			if err := r.writeWithRetry(batchCtx, batch, []DestinationConfig{primary}); err != nil {
				r.emitTrace(batchCtx, "write_error", batch.Checkpoint.LSN, "", spec.ActionWriteFail, err)
				r.Meters.RecordError(ctx, "destination_write")
				span.RecordError(err)
				span.End()
				return err
			}
			r.Meters.RecordDestinationWrite(ctx, r.FlowID, float64(time.Since(writeStart).Milliseconds()))
			r.emitCheckpointTrace(batchCtx, "deliver", batch.Checkpoint, tracePosition, spec.ActionDeliver, nil)
			if err := r.ackPrimaryAndOutbox(batchCtx, outbox, batch, secondary, tracePosition, false); err != nil {
				span.RecordError(err)
				span.End()
				return err
			}
			if len(secondaryQueues) > 0 {
				pending := newPendingBatch(batch, ddlRecords, len(secondaryQueues), tracePosition)
				for _, queue := range secondaryQueues {
					queue.pending = append(queue.pending, pending)
				}
			}
			if _, err := r.drainSecondaryQueues(batchCtx, secondaryQueues); err != nil {
				span.RecordError(err)
				span.End()
				return err
			}
			r.Meters.RecordBatch(ctx, r.FlowID, int64(len(batch.Records)), float64(time.Since(batchStart).Milliseconds()))
			span.End()
			continue
		}

		writeStart := time.Now()
		writeCtx, writeSpan := tracer.Start(batchCtx, "destination.write",
			trace.WithAttributes(
				attribute.Int("destinations.count", len(r.Destinations)),
			),
		)
		if err := r.writeWithRetry(writeCtx, batch, r.Destinations); err != nil {
			r.emitTrace(batchCtx, "write_error", batch.Checkpoint.LSN, "", spec.ActionWriteFail, err)
			r.Meters.RecordError(ctx, "destination_write")
			writeSpan.RecordError(err)
			writeSpan.End()
			span.RecordError(err)
			span.End()
			return err
		}
		writeLatencyMs := float64(time.Since(writeStart).Milliseconds())
		writeSpan.SetAttributes(attribute.Float64("latency_ms", writeLatencyMs))
		writeSpan.End()
		r.Meters.RecordDestinationWrite(ctx, r.FlowID, float64(time.Since(writeStart).Milliseconds()))
		span.SetAttributes(
			attribute.Float64("destination.write_latency_ms", writeLatencyMs),
		)
		if err := r.markDDLApplied(batchCtx, batch.Checkpoint, ddlRecords); err != nil {
			span.RecordError(err)
			span.End()
			return err
		}
		r.emitCheckpointTrace(batchCtx, "deliver", batch.Checkpoint, tracePosition, spec.ActionDeliver, nil)
		if err := r.ackAndCheckpoint(batchCtx, batch.Checkpoint, tracePosition, false); err != nil {
			span.RecordError(err)
			span.End()
			return err
		}

		batchLatencyMs := float64(time.Since(batchStart).Milliseconds())
		r.Meters.RecordBatch(ctx, r.FlowID, int64(len(batch.Records)), batchLatencyMs)
		span.SetAttributes(
			attribute.Float64("batch.latency_ms", batchLatencyMs),
		)
		span.End()
	}
}

func checkpointPositionsEqual(left, right string) bool {
	if left == "" || right == "" {
		return left == right
	}
	cmp, err := connector.CompareCheckpointLSN(left, right)
	return err == nil && cmp == 0
}

func (r *Runner) ackPrimaryAndOutbox(ctx context.Context, outbox connector.OutboxStore, batch connector.Batch, secondary []DestinationConfig, tracePosition string, emitControlCheckpoint bool) error {
	entries := make([]connector.OutboxEntry, 0, len(secondary))
	for _, destination := range secondary {
		entries = append(entries, connector.OutboxEntry{
			FlowID:      r.FlowID,
			Destination: destination.Spec.Name,
			PositionID:  tracePosition,
			Batch:       batch,
		})
	}
	if err := outbox.PersistCheckpointAndOutbox(ctx, r.FlowID, batch.Checkpoint, entries); err != nil {
		r.emitCheckpointTrace(ctx, "checkpoint_error", batch.Checkpoint, tracePosition, spec.ActionCheckpointFail, err)
		r.Meters.RecordError(ctx, "checkpoint_outbox_persist")
		return fmt.Errorf("persist checkpoint and primary-ack outbox: %w", err)
	}
	r.emitCheckpointTrace(ctx, "checkpoint", batch.Checkpoint, tracePosition, spec.ActionPersistCheckpoint, nil)
	r.Meters.RecordCheckpoint(ctx, r.FlowID)
	if err := r.Source.Ack(ctx, batch.Checkpoint); err != nil {
		r.emitCheckpointTrace(ctx, "ack_error", batch.Checkpoint, tracePosition, spec.ActionNone, err)
		r.Meters.RecordError(ctx, "source_ack")
		return fmt.Errorf("ack source: %w", err)
	}
	r.emitCheckpointTrace(ctx, "ack", batch.Checkpoint, tracePosition, spec.ActionAck, nil)
	if emitControlCheckpoint {
		r.emitCheckpointTrace(ctx, "control_checkpoint", batch.Checkpoint, tracePosition, spec.ActionNone, nil)
	}
	return nil
}

func (r *Runner) ackAndCheckpoint(ctx context.Context, checkpoint connector.Checkpoint, tracePosition string, emitControlCheckpoint bool) error {
	if r.Checkpoints != nil && r.FlowID != "" && shouldPersistCheckpoint(checkpoint) {
		if err := r.Checkpoints.Put(ctx, r.FlowID, checkpoint); err != nil {
			r.emitCheckpointTrace(ctx, "checkpoint_error", checkpoint, tracePosition, spec.ActionCheckpointFail, err)
			r.Meters.RecordError(ctx, "checkpoint_persist")
			return fmt.Errorf("persist checkpoint: %w", err)
		}
		r.emitCheckpointTrace(ctx, "checkpoint", checkpoint, tracePosition, spec.ActionPersistCheckpoint, nil)
		r.Meters.RecordCheckpoint(ctx, r.FlowID)
	}
	if err := r.Source.Ack(ctx, checkpoint); err != nil {
		r.emitCheckpointTrace(ctx, "ack_error", checkpoint, tracePosition, spec.ActionNone, err)
		r.Meters.RecordError(ctx, "source_ack")
		return fmt.Errorf("ack source: %w", err)
	}
	r.emitCheckpointTrace(ctx, "ack", checkpoint, tracePosition, spec.ActionAck, nil)
	if emitControlCheckpoint {
		r.emitCheckpointTrace(ctx, "control_checkpoint", checkpoint, tracePosition, spec.ActionNone, nil)
	}
	return nil
}

func (r *Runner) resolveStaging(ctx context.Context) error {
	for _, dest := range r.Destinations {
		if resolver, ok := dest.Dest.(StagingResolver); ok {
			if err := resolver.ResolveStaging(ctx); err != nil {
				return fmt.Errorf("resolve staging for %s: %w", dest.Spec.Name, err)
			}
		}
	}
	return nil
}

func (r *Runner) isBackfill() bool {
	if r.SourceSpec.Options == nil {
		return false
	}
	return r.SourceSpec.Options["mode"] == "backfill"
}

func (r *Runner) effectiveAckPolicy() AckPolicy {
	if r.AckPolicy == "" {
		return AckPolicyAll
	}
	return r.AckPolicy
}

func (r *Runner) effectiveGiveUpPolicy() GiveUpPolicy {
	if r.GiveUpPolicy == "" {
		return GiveUpPolicyOnRetryExhaustion
	}
	return r.GiveUpPolicy
}

func (r *Runner) effectiveFailureMode() FailureMode {
	if r.FailureMode == "" {
		return FailureModeHoldSlot
	}
	return r.FailureMode
}

func (r *Runner) retryLimit() int {
	return defaultRetryAttempts
}

func (r *Runner) retryBackoff() time.Duration {
	return defaultRetryBackoff
}

func (r *Runner) shouldGiveUp(attempts int) bool {
	if r.effectiveGiveUpPolicy() == GiveUpPolicyNever {
		return false
	}
	return attempts >= r.retryLimit()
}

func (r *Runner) sleepRetry(ctx context.Context) error {
	backoff := r.retryBackoff()
	if backoff <= 0 {
		return nil
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *Runner) partitionDestinations() (DestinationConfig, []DestinationConfig, error) {
	if len(r.Destinations) == 0 {
		return DestinationConfig{}, nil, errors.New("at least one destination is required")
	}
	seen := make(map[string]struct{}, len(r.Destinations))
	for _, destination := range r.Destinations {
		if destination.Spec.Name == "" {
			return DestinationConfig{}, nil, errors.New("primary acknowledgement requires a stable name for every destination")
		}
		if _, duplicate := seen[destination.Spec.Name]; duplicate {
			return DestinationConfig{}, nil, fmt.Errorf("duplicate destination identity %q", destination.Spec.Name)
		}
		seen[destination.Spec.Name] = struct{}{}
	}
	if r.PrimaryDestination == "" {
		return r.Destinations[0], append([]DestinationConfig(nil), r.Destinations[1:]...), nil
	}
	for i, dest := range r.Destinations {
		if dest.Spec.Name == r.PrimaryDestination {
			secondary := make([]DestinationConfig, 0, len(r.Destinations)-1)
			secondary = append(secondary, r.Destinations[:i]...)
			secondary = append(secondary, r.Destinations[i+1:]...)
			return dest, secondary, nil
		}
	}
	return DestinationConfig{}, nil, fmt.Errorf("primary destination %q not found", r.PrimaryDestination)
}

func (r *Runner) writeWithRetry(ctx context.Context, batch connector.Batch, dests []DestinationConfig) error {
	attempts := 0
	for {
		if err := r.writeDestinations(ctx, batch, dests); err != nil {
			attempts++
			if r.shouldGiveUp(attempts) {
				return err
			}
			if err := r.sleepRetry(ctx); err != nil {
				return err
			}
			continue
		}
		return nil
	}
}

func (r *Runner) restoreSecondaryQueues(ctx context.Context, outbox connector.OutboxStore, secondary []DestinationConfig) ([]*secondaryQueue, error) {
	queues := make([]*secondaryQueue, 0, len(secondary))
	byDestination := make(map[string]*secondaryQueue, len(secondary))
	for _, destination := range secondary {
		if destination.Spec.Name == "" {
			return nil, errors.New("primary-ack secondary destination name is required")
		}
		if _, duplicate := byDestination[destination.Spec.Name]; duplicate {
			return nil, fmt.Errorf("duplicate secondary destination identity %q", destination.Spec.Name)
		}
		queue := &secondaryQueue{dest: destination, outbox: outbox}
		queues = append(queues, queue)
		byDestination[destination.Spec.Name] = queue
	}
	entries, err := outbox.ListOutbox(ctx, r.FlowID)
	if err != nil {
		return nil, fmt.Errorf("restore primary-ack outbox: %w", err)
	}
	groups := make(map[string]*pendingBatch)
	for _, entry := range entries {
		queue, ok := byDestination[entry.Destination]
		if !ok {
			return nil, fmt.Errorf("primary-ack outbox contains destination %q not configured as a secondary for flow %q; restore or explicitly reconcile that destination", entry.Destination, r.FlowID)
		}
		positionID, err := connector.CheckpointPositionID(entry.Batch.Checkpoint)
		if err != nil {
			return nil, fmt.Errorf("validate restored outbox entry for %s: %w", entry.Destination, err)
		}
		if entry.FlowID != r.FlowID || entry.PositionID != positionID {
			return nil, fmt.Errorf("invalid restored outbox identity flow=%q destination=%q position=%q batch_position=%q", entry.FlowID, entry.Destination, entry.PositionID, positionID)
		}
		if entry.BatchHash == "" {
			return nil, fmt.Errorf("restored outbox entry for %s at %s has no durable batch hash", entry.Destination, entry.PositionID)
		}
		pending := groups[entry.PositionID]
		if pending == nil {
			pending = newPendingBatch(entry.Batch, ddlRecordsInBatch(entry.Batch), 0, entry.PositionID)
			pending.batchHash = entry.BatchHash
			groups[entry.PositionID] = pending
		} else if pending.batchHash != entry.BatchHash {
			return nil, fmt.Errorf("primary-ack outbox position %q contains different batches across destinations", entry.PositionID)
		}
		pending.remaining++
		queue.pending = append(queue.pending, pending)
	}
	return queues, nil
}

func (r *Runner) drainSecondaryQueues(ctx context.Context, queues []*secondaryQueue) (bool, error) {
	progressed := false
	for _, queue := range queues {
		for len(queue.pending) > 0 {
			pending := queue.pending[0]
			if err := r.writeDestination(ctx, queue.dest, pending.batch); err != nil {
				pending.bumpAttempt(queue.dest.Spec.Name)
				if r.shouldGiveUp(pending.attempts[queue.dest.Spec.Name]) {
					return progressed, err
				}
				break
			}
			if pending.remaining == 1 && len(pending.ddlRecords) > 0 {
				if err := r.markDDLApplied(ctx, pending.batch.Checkpoint, pending.ddlRecords); err != nil {
					return progressed, err
				}
			}
			if err := queue.outbox.DeleteOutbox(ctx, r.FlowID, queue.dest.Spec.Name, pending.positionID); err != nil {
				return progressed, fmt.Errorf("delete delivered primary-ack outbox entry for %s: %w", queue.dest.Spec.Name, err)
			}
			queue.pending = queue.pending[1:]
			pending.remaining--
			progressed = true
		}
	}
	return progressed, nil
}

func (r *Runner) flushSecondaryQueues(ctx context.Context, queues []*secondaryQueue) error {
	for {
		empty := true
		for _, queue := range queues {
			if len(queue.pending) > 0 {
				empty = false
				break
			}
		}
		if empty {
			return nil
		}
		progressed, err := r.drainSecondaryQueues(ctx, queues)
		if err != nil {
			return err
		}
		if progressed {
			continue
		}
		if r.effectiveGiveUpPolicy() != GiveUpPolicyNever {
			return fmt.Errorf("secondary destinations failed to catch up")
		}
		if err := r.sleepRetry(ctx); err != nil {
			return err
		}
	}
}

type pendingBatch struct {
	batch      connector.Batch
	ddlRecords []connector.Record
	remaining  int
	positionID string
	batchHash  string
	attempts   map[string]int
}

func newPendingBatch(batch connector.Batch, ddlRecords []connector.Record, remaining int, positionID string) *pendingBatch {
	return &pendingBatch{
		batch:      batch,
		ddlRecords: ddlRecords,
		remaining:  remaining,
		positionID: positionID,
		attempts:   make(map[string]int),
	}
}

func (p *pendingBatch) bumpAttempt(dest string) {
	p.attempts[dest]++
}

type secondaryQueue struct {
	dest    DestinationConfig
	outbox  connector.OutboxStore
	pending []*pendingBatch
}

func (r *Runner) normalizeWireFormat() error {
	if r.WireFormat == "" {
		return nil
	}
	if r.SourceSpec.Options == nil {
		r.SourceSpec.Options = map[string]string{}
	}
	if srcFormat := r.SourceSpec.Options["format"]; srcFormat != "" && connector.WireFormat(srcFormat) != r.WireFormat {
		if r.StrictFormat {
			return fmt.Errorf("source format %s does not match flow format %s", srcFormat, r.WireFormat)
		}
	} else if r.SourceSpec.Options["format"] == "" {
		r.SourceSpec.Options["format"] = string(r.WireFormat)
	}

	for i := range r.Destinations {
		spec := r.Destinations[i].Spec
		if spec.Options == nil {
			spec.Options = map[string]string{}
		}
		if destFormat := spec.Options["format"]; destFormat != "" && connector.WireFormat(destFormat) != r.WireFormat {
			if r.StrictFormat {
				return fmt.Errorf("destination %s format %s does not match flow format %s", spec.Name, destFormat, r.WireFormat)
			}
		} else if spec.Options["format"] == "" {
			spec.Options["format"] = string(r.WireFormat)
		}
		r.Destinations[i].Spec = spec
	}

	return nil
}

func isControlCheckpoint(cp connector.Checkpoint) bool {
	if cp.Metadata == nil {
		return false
	}
	if cp.Metadata["mode"] == "backfill" {
		return true
	}
	if cp.Metadata["done"] == "true" {
		return true
	}
	if cp.Metadata["control"] == "true" {
		return true
	}
	return false
}

func shouldPersistCheckpoint(cp connector.Checkpoint) bool {
	return cp.LSN != "" || len(cp.Metadata) > 0
}

func (r *Runner) writeDestinations(ctx context.Context, batch connector.Batch, dests []DestinationConfig) error {
	if len(dests) == 0 {
		return nil
	}

	parallelism := r.Parallelism
	if parallelism <= 0 {
		parallelism = 1
	}
	if parallelism == 1 || len(dests) == 1 {
		for _, dest := range dests {
			if err := r.writeDestination(ctx, dest, batch); err != nil {
				return err
			}
		}
		return nil
	}

	if parallelism > len(dests) {
		parallelism = len(dests)
	}

	sem := make(chan struct{}, parallelism)
	errCh := make(chan error, len(dests))
	var wg sync.WaitGroup

	for _, dest := range dests {
		sem <- struct{}{}
		wg.Add(1)
		go func(dest DestinationConfig) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := r.writeDestination(ctx, dest, batch); err != nil {
				errCh <- err
			}
		}(dest)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func ddlRecordsInBatch(batch connector.Batch) []connector.Record {
	if len(batch.Records) == 0 {
		return nil
	}
	records := make([]connector.Record, 0)
	for _, record := range batch.Records {
		if record.Operation == connector.OpDDL || record.DDL != "" || len(record.DDLPlan) > 0 {
			records = append(records, record)
		}
	}
	return records
}

func (r *Runner) markDDLApplied(ctx context.Context, checkpoint connector.Checkpoint, records []connector.Record) error {
	if r.DDLApplied == nil || len(records) == 0 {
		return nil
	}
	if checkpoint.LSN == "" {
		return nil
	}
	for _, record := range records {
		ddlText := record.DDL
		if ddlText == "" && len(record.DDLPlan) > 0 {
			ddlText = string(record.DDLPlan)
		}
		if err := r.DDLApplied(ctx, r.FlowID, checkpoint.LSN, ddlText); err != nil {
			if errors.Is(err, connector.ErrDDLApprovalRequired) {
				r.handleDDLGate(ctx, trace.SpanFromContext(ctx), err)
			}
			return err
		}
		if r.TraceSink != nil {
			r.TraceSink.Emit(ctx, TraceEvent{
				Kind:       "ddl_applied",
				Spec:       spec.SpecCDCFlow,
				SpecAction: spec.ActionApplyDDL,
				LSN:        checkpoint.LSN,
				FlowID:     r.FlowID,
				DDL:        ddlText,
			})
		}
	}
	return nil
}

func (r *Runner) handleDDLGate(ctx context.Context, span trace.Span, err error) {
	if span == nil {
		span = trace.SpanFromContext(ctx)
	}
	gate, _ := connector.AsDDLGate(err)
	attrs := []attribute.KeyValue{
		attribute.Bool("ddl.gated", true),
	}
	if r.FlowID != "" {
		attrs = append(attrs, attribute.String("flow.id", r.FlowID))
	}
	if gate != nil {
		if gate.FlowID != "" {
			attrs = append(attrs, attribute.String("ddl.flow_id", gate.FlowID))
		}
		if gate.LSN != "" {
			attrs = append(attrs, attribute.String("ddl.lsn", gate.LSN))
		}
		if gate.Status != "" {
			attrs = append(attrs, attribute.String("ddl.status", gate.Status))
		}
		if gate.EventID != 0 {
			attrs = append(attrs, attribute.Int64("ddl.event_id", gate.EventID))
		}
	}
	span.AddEvent("ddl.gated", trace.WithAttributes(attrs...))

	r.emitDDLGateTrace(ctx, gate, err)
	r.emitDDLGateMetric(ctx, gate)

	if gate != nil {
		log.Printf("ddl gate: flow=%s event_id=%d status=%s lsn=%s", r.FlowID, gate.EventID, gate.Status, gate.LSN)
		return
	}
	log.Printf("ddl gate: flow=%s error=%v", r.FlowID, err)
}

func (r *Runner) emitDDLGateMetric(ctx context.Context, gate *connector.DDLGateError) {
	counter := ddlGatedMetric()
	if counter == nil {
		return
	}
	attrs := []attribute.KeyValue{}
	if r.FlowID != "" {
		attrs = append(attrs, attribute.String("flow.id", r.FlowID))
	}
	if gate != nil && gate.Status != "" {
		attrs = append(attrs, attribute.String("ddl.status", gate.Status))
	}
	counter.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (r *Runner) emitDDLGateTrace(ctx context.Context, gate *connector.DDLGateError, err error) {
	if r.TraceSink == nil {
		return
	}
	event := TraceEvent{
		Kind:       "ddl_gate",
		Spec:       spec.SpecCDCFlow,
		SpecAction: spec.ActionPause,
		FlowID:     r.FlowID,
	}
	if err != nil {
		event.Error = err.Error()
	}
	if gate != nil {
		if gate.FlowID != "" {
			event.FlowID = gate.FlowID
		}
		event.LSN = gate.LSN
		event.DDL = gate.DDL
		event.Detail = gate.PlanJSON
		event.EventID = gate.EventID
	}
	r.TraceSink.Emit(ctx, event)
}

func ddlGatedMetric() metric.Int64Counter {
	ddlGatedOnce.Do(func() {
		meter := otel.Meter("wallaby/stream")
		counter, err := meter.Int64Counter(
			"wallaby.ddl.gated_total",
			metric.WithDescription("Number of times a flow was gated on DDL approval."),
			metric.WithUnit("1"),
		)
		if err == nil {
			ddlGatedCounter = counter
		}
	})
	return ddlGatedCounter
}

func (r *Runner) writeDestination(ctx context.Context, dest DestinationConfig, batch connector.Batch) error {
	if len(batch.Records) > 0 {
		if r.RequireDDLExecution && connector.ResolveDestinationCapabilities(dest.Dest, dest.Spec).ExecutesDDL() {
			for _, record := range batch.Records {
				if record.Operation != connector.OpDDL && record.DDL == "" && len(record.DDLPlan) == 0 {
					continue
				}
				if err := dest.Dest.ApplyDDL(ctx, batch.Schema, record); err != nil {
					r.emitTrace(ctx, "ddl_error", batch.Checkpoint.LSN, dest.Spec.Name, spec.ActionNone, err)
					return fmt.Errorf("apply ddl destination %s: %w", dest.Spec.Name, err)
				}
				r.Meters.RecordDestinationDDL(ctx, string(dest.Spec.Type))
			}
		}
	}

	destBatch := batch
	baseMappings := dest.Dest.TypeMappings()
	if transformed, ok, err := transformBatchForDestination(batch, dest.Spec, baseMappings); err != nil {
		return fmt.Errorf("transform destination %s: %w", dest.Spec.Name, err)
	} else if ok {
		destBatch = transformed
	}

	if err := dest.Dest.Write(ctx, destBatch); err != nil {
		r.emitTrace(ctx, "write_error", batch.Checkpoint.LSN, dest.Spec.Name, spec.ActionWriteFail, err)
		return fmt.Errorf("write destination %s: %w", dest.Spec.Name, err)
	}
	r.emitTrace(ctx, "write", batch.Checkpoint.LSN, dest.Spec.Name, spec.ActionNone, nil)
	r.Meters.RecordDestinationWriteCount(ctx, string(dest.Spec.Type))
	return nil
}

func (r *Runner) emitCheckpointTrace(ctx context.Context, kind string, checkpoint connector.Checkpoint, positionID string, specAction spec.Action, err error) {
	if positionID == "" {
		var positionErr error
		positionID, positionErr = connector.CheckpointPositionID(checkpoint)
		if positionErr != nil {
			return
		}
	}
	if checkpoint.LSN != "" {
		r.emitTracePosition(ctx, kind, positionID, "", "", specAction, err)
		return
	}
	r.emitTracePosition(ctx, kind, "", positionID, "", specAction, err)
}

func (r *Runner) emitTrace(ctx context.Context, kind, lsn, destination string, specAction spec.Action, err error) {
	r.emitTracePosition(ctx, kind, lsn, "", destination, specAction, err)
}

func (r *Runner) emitTracePosition(ctx context.Context, kind, lsn, position, destination string, specAction spec.Action, err error) {
	if r.TraceSink == nil {
		return
	}
	if lsn == "" && position == "" {
		switch kind {
		case "read", "deliver", "ack", "ack_error", "checkpoint", "restore_checkpoint", "restore_ack", "restore_ack_error", "write", "write_error", "ddl_error", "control_checkpoint":
			return
		}
	}
	event := TraceEvent{
		Kind:        kind,
		Spec:        spec.SpecCDCFlow,
		SpecAction:  specAction,
		LSN:         lsn,
		Position:    position,
		FlowID:      r.FlowID,
		Destination: destination,
	}
	if err != nil {
		event.Error = err.Error()
	}
	r.TraceSink.Emit(ctx, event)
}
