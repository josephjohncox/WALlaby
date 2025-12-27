package stream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/josephjohncox/ductstream/pkg/connector"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// DestinationConfig binds a destination to its spec.
type DestinationConfig struct {
	Spec connector.Spec
	Dest connector.Destination
}

// Runner streams data from a source to destinations.
type Runner struct {
	Source        connector.Source
	SourceSpec    connector.Spec
	Destinations  []DestinationConfig
	Checkpoints   connector.CheckpointStore
	FlowID        string
	Tracer        trace.Tracer
	BatchTimeout  time.Duration
	MaxEmptyReads int
}

// Run executes the streaming loop until context cancellation or error.
func (r *Runner) Run(ctx context.Context) error {
	if r.Source == nil {
		return errors.New("source is required")
	}
	if len(r.Destinations) == 0 {
		return errors.New("at least one destination is required")
	}

	tracer := r.Tracer
	if tracer == nil {
		tracer = otel.Tracer("ductstream/stream")
	}

	if err := r.Source.Open(ctx, r.SourceSpec); err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer r.Source.Close(ctx)

	for _, dest := range r.Destinations {
		if dest.Dest == nil {
			return errors.New("destination is required")
		}
		if err := dest.Dest.Open(ctx, dest.Spec); err != nil {
			return fmt.Errorf("open destination %s: %w", dest.Spec.Name, err)
		}
		defer dest.Dest.Close(ctx)
	}

	if r.Checkpoints != nil && r.FlowID != "" {
		if cp, err := r.Checkpoints.Get(ctx, r.FlowID); err == nil {
			_ = r.Source.Ack(ctx, cp)
		}
	}

	emptyReads := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batchCtx, span := tracer.Start(ctx, "stream.batch")
		batch, err := r.Source.Read(batchCtx)
		if err != nil {
			span.RecordError(err)
			span.End()
			return err
		}

		if len(batch.Records) == 0 {
			emptyReads++
			span.End()
			if r.MaxEmptyReads > 0 && emptyReads >= r.MaxEmptyReads {
				return errors.New("max empty reads reached")
			}
			continue
		}
		emptyReads = 0

		span.SetAttributes(
			attribute.Int("records", len(batch.Records)),
			attribute.String("schema", batch.Schema.Name),
		)

		for _, dest := range r.Destinations {
			if err := dest.Dest.Write(batchCtx, batch); err != nil {
				span.RecordError(err)
				span.End()
				return fmt.Errorf("write destination %s: %w", dest.Spec.Name, err)
			}
		}

		if err := r.Source.Ack(batchCtx, batch.Checkpoint); err != nil {
			span.RecordError(err)
			span.End()
			return fmt.Errorf("ack source: %w", err)
		}
		if r.Checkpoints != nil && r.FlowID != "" {
			if err := r.Checkpoints.Put(batchCtx, r.FlowID, batch.Checkpoint); err != nil {
				span.RecordError(err)
				span.End()
				return fmt.Errorf("persist checkpoint: %w", err)
			}
		}

		span.End()
	}
}
