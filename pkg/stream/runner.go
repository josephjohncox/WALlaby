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
	Source             connector.Source
	SourceSpec         connector.Spec
	Destinations       []DestinationConfig
	Checkpoints        connector.CheckpointStore
	FlowID             string
	ResolveStaging     bool
	Tracer             trace.Tracer
	Meters             *telemetry.Meters
	BatchTimeout       time.Duration
	MaxEmptyReads      int
	WireFormat         connector.WireFormat
	StrictFormat       bool
	Parallelism        int
	AckPolicy          AckPolicy
	PrimaryDestination string
	FailureMode        FailureMode
	GiveUpPolicy       GiveUpPolicy
	DDLApplied         func(ctx context.Context, flowID string, lsn string, ddl string) error
	TraceSink          TraceSink
}

// Run executes the streaming loop until context cancellation or error.
func (r *Runner) Run(ctx context.Context) (retErr error) {
	if r.Source == nil {
		return errors.New("source is required")
	}
	if len(r.Destinations) == 0 {
		return errors.New("at least one destination is required")
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

	if r.Checkpoints != nil && r.FlowID != "" {
		if cp, err := r.Checkpoints.Get(ctx, r.FlowID); err == nil && cp.LSN != "" {
			if r.SourceSpec.Options == nil {
				r.SourceSpec.Options = map[string]string{}
			}
			if r.SourceSpec.Options["start_lsn"] == "" {
				r.SourceSpec.Options["start_lsn"] = cp.LSN
			}
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

	if r.Checkpoints != nil && r.FlowID != "" {
		if cp, err := r.Checkpoints.Get(ctx, r.FlowID); err == nil {
			_ = r.Source.Ack(ctx, cp)
		}
	}

	ackPolicy := r.effectiveAckPolicy()
	var primary DestinationConfig
	var secondaryQueues []*secondaryQueue
	if ackPolicy == AckPolicyPrimary && len(r.Destinations) > 1 {
		var secondary []DestinationConfig
		var err error
		primary, secondary, err = r.partitionDestinations()
		if err != nil {
			return err
		}
		secondaryQueues = make([]*secondaryQueue, 0, len(secondary))
		for _, dest := range secondary {
			secondaryQueues = append(secondaryQueues, &secondaryQueue{dest: dest})
		}
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

		// Read from source with child span
		readStart := time.Now()
		readCtx, readSpan := tracer.Start(batchCtx, "source.read")
		batch, err := r.Source.Read(readCtx)
		readSpan.End()
		r.Meters.RecordSourceReadLatency(ctx, float64(time.Since(readStart).Milliseconds()))
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
			r.recordError(ctx, "source_read")
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
		if len(ddlRecordsInBatch(batch)) > 0 {
			r.emitTrace(batchCtx, "read", batch.Checkpoint.LSN, "", spec.ActionReadDDL, nil)
		} else {
			r.emitTrace(batchCtx, "read", batch.Checkpoint.LSN, "", spec.ActionReadBatch, nil)
		}
		if r.WireFormat != "" {
			batch.WireFormat = r.WireFormat
		}

		if len(batch.Records) == 0 {
			if isControlCheckpoint(batch.Checkpoint) {
				r.emitTrace(batchCtx, "deliver", batch.Checkpoint.LSN, "", spec.ActionDeliver, nil)
				if err := r.Source.Ack(batchCtx, batch.Checkpoint); err != nil {
					r.emitTrace(batchCtx, "ack_error", batch.Checkpoint.LSN, "", spec.ActionNone, err)
					r.recordError(ctx, "source_ack")
					span.RecordError(err)
					span.End()
					return fmt.Errorf("ack source: %w", err)
				}
				r.emitTrace(batchCtx, "ack", batch.Checkpoint.LSN, "", spec.ActionAck, nil)
				r.emitTrace(batchCtx, "control_checkpoint", batch.Checkpoint.LSN, "", spec.ActionNone, nil)
				if r.Checkpoints != nil && r.FlowID != "" && shouldPersistCheckpoint(batch.Checkpoint) {
					if err := r.Checkpoints.Put(batchCtx, r.FlowID, batch.Checkpoint); err != nil {
						r.recordError(ctx, "checkpoint_persist")
						span.RecordError(err)
						span.End()
						return fmt.Errorf("persist checkpoint: %w", err)
					}
					r.emitTrace(batchCtx, "checkpoint", batch.Checkpoint.LSN, "", spec.ActionNone, nil)
					r.recordCheckpoint(ctx)
				}
				span.End()
				continue
			}

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
		emptyReads = 0

		span.SetAttributes(
			attribute.Int("batch.records", len(batch.Records)),
			attribute.String("batch.schema", batch.Schema.Name),
		)

		ddlRecords := ddlRecordsInBatch(batch)

		ackAndCheckpoint := func() error {
			if err := r.Source.Ack(batchCtx, batch.Checkpoint); err != nil {
				r.emitTrace(batchCtx, "ack_error", batch.Checkpoint.LSN, "", spec.ActionNone, err)
				r.recordError(ctx, "source_ack")
				return fmt.Errorf("ack source: %w", err)
			}
			r.emitTrace(batchCtx, "ack", batch.Checkpoint.LSN, "", spec.ActionAck, nil)
			if r.Checkpoints != nil && r.FlowID != "" && shouldPersistCheckpoint(batch.Checkpoint) {
				if err := r.Checkpoints.Put(batchCtx, r.FlowID, batch.Checkpoint); err != nil {
					r.recordError(ctx, "checkpoint_persist")
					return fmt.Errorf("persist checkpoint: %w", err)
				}
				r.emitTrace(batchCtx, "checkpoint", batch.Checkpoint.LSN, "", spec.ActionNone, nil)
				r.recordCheckpoint(ctx)
			}
			return nil
		}

		if ackPolicy == AckPolicyPrimary && len(secondaryQueues) > 0 {
			writeStart := time.Now()
			if err := r.writeWithRetry(batchCtx, batch, []DestinationConfig{primary}); err != nil {
				r.emitTrace(batchCtx, "write_error", batch.Checkpoint.LSN, "", spec.ActionWriteFail, err)
				r.recordError(ctx, "destination_write")
				span.RecordError(err)
				span.End()
				return err
			}
			r.recordDestinationWrite(ctx, time.Since(writeStart))
			r.emitTrace(batchCtx, "deliver", batch.Checkpoint.LSN, "", spec.ActionDeliver, nil)
			if err := ackAndCheckpoint(); err != nil {
				span.RecordError(err)
				span.End()
				return err
			}
			pending := newPendingBatch(batch, ddlRecords, len(secondaryQueues))
			for _, queue := range secondaryQueues {
				queue.pending = append(queue.pending, pending)
			}
			if _, err := r.drainSecondaryQueues(batchCtx, secondaryQueues); err != nil {
				span.RecordError(err)
				span.End()
				return err
			}
			r.recordBatch(ctx, len(batch.Records), time.Since(batchStart))
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
			r.recordError(ctx, "destination_write")
			writeSpan.RecordError(err)
			writeSpan.End()
			span.RecordError(err)
			span.End()
			return err
		}
		writeLatencyMs := float64(time.Since(writeStart).Milliseconds())
		writeSpan.SetAttributes(attribute.Float64("latency_ms", writeLatencyMs))
		writeSpan.End()
		r.recordDestinationWrite(ctx, time.Since(writeStart))
		span.SetAttributes(
			attribute.Float64("destination.write_latency_ms", writeLatencyMs),
		)
		if err := r.markDDLApplied(batchCtx, batch.Checkpoint, ddlRecords); err != nil {
			span.RecordError(err)
			span.End()
			return err
		}
		r.emitTrace(batchCtx, "deliver", batch.Checkpoint.LSN, "", spec.ActionDeliver, nil)
		if err := ackAndCheckpoint(); err != nil {
			span.RecordError(err)
			span.End()
			return err
		}

		batchLatencyMs := float64(time.Since(batchStart).Milliseconds())
		r.recordBatch(ctx, len(batch.Records), time.Since(batchStart))
		span.SetAttributes(
			attribute.Float64("batch.latency_ms", batchLatencyMs),
		)

		span.End()
	}
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
			queue.pending = queue.pending[1:]
			pending.remaining--
			progressed = true
			if pending.remaining == 0 && len(pending.ddlRecords) > 0 {
				if err := r.markDDLApplied(ctx, pending.batch.Checkpoint, pending.ddlRecords); err != nil {
					return progressed, err
				}
			}
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
	attempts   map[string]int
}

func newPendingBatch(batch connector.Batch, ddlRecords []connector.Record, remaining int) *pendingBatch {
	return &pendingBatch{
		batch:      batch,
		ddlRecords: ddlRecords,
		remaining:  remaining,
		attempts:   make(map[string]int),
	}
}

func (p *pendingBatch) bumpAttempt(dest string) {
	p.attempts[dest]++
}

type secondaryQueue struct {
	dest    DestinationConfig
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
	if cp.LSN != "" {
		return true
	}
	if cp.Metadata == nil {
		return false
	}
	if cp.Metadata["mode"] == "backfill" {
		return false
	}
	return true
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
		if record.Operation == connector.OpDDL || record.DDL != "" {
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
		if err := r.DDLApplied(ctx, r.FlowID, checkpoint.LSN, record.DDL); err != nil {
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
				DDL:        record.DDL,
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
		if dest.Dest.Capabilities().SupportsDDL {
			for _, record := range batch.Records {
				if record.Operation != connector.OpDDL && record.DDL == "" {
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
	r.recordDestinationWriteCount(ctx, string(dest.Spec.Type))
	return nil
}

func (r *Runner) emitTrace(ctx context.Context, kind, lsn, destination string, specAction spec.Action, err error) {
	if r.TraceSink == nil {
		return
	}
	if lsn == "" {
		switch kind {
		case "read", "deliver", "ack", "ack_error", "checkpoint", "write", "write_error", "ddl_error", "control_checkpoint":
			return
		}
	}
	event := TraceEvent{
		Kind:        kind,
		Spec:        spec.SpecCDCFlow,
		SpecAction:  specAction,
		LSN:         lsn,
		FlowID:      r.FlowID,
		Destination: destination,
	}
	if err != nil {
		event.Error = err.Error()
	}
	r.TraceSink.Emit(ctx, event)
}

func (r *Runner) recordError(ctx context.Context, errorType string) {
	if r.Meters != nil {
		r.Meters.RecordError(ctx, errorType)
	}
}

func (r *Runner) recordCheckpoint(ctx context.Context) {
	if r.Meters != nil {
		r.Meters.RecordCheckpoint(ctx, r.FlowID)
	}
}

func (r *Runner) recordDestinationWrite(ctx context.Context, duration time.Duration) {
	if r.Meters != nil {
		r.Meters.RecordDestinationWrite(ctx, r.FlowID, float64(duration.Milliseconds()))
	}
}

func (r *Runner) recordBatch(ctx context.Context, recordCount int, duration time.Duration) {
	if r.Meters != nil {
		r.Meters.RecordBatch(ctx, r.FlowID, int64(recordCount), float64(duration.Milliseconds()))
	}
}

func (r *Runner) recordDestinationWriteCount(ctx context.Context, destType string) {
	if r.Meters != nil {
		r.Meters.RecordDestinationWriteCount(ctx, destType)
	}
}
