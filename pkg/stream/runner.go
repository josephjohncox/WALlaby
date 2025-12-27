package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
	Source         connector.Source
	SourceSpec     connector.Spec
	Destinations   []DestinationConfig
	Checkpoints    connector.CheckpointStore
	FlowID         string
	ResolveStaging bool
	Tracer         trace.Tracer
	BatchTimeout   time.Duration
	MaxEmptyReads  int
	WireFormat     connector.WireFormat
	StrictFormat   bool
	Parallelism    int
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
			if errors.Is(err, io.EOF) && r.isBackfill() {
				span.End()
				if r.ResolveStaging {
					if err := r.resolveStaging(batchCtx); err != nil {
						return err
					}
				}
				return nil
			}
			span.RecordError(err)
			span.End()
			return err
		}
		if r.WireFormat != "" {
			batch.WireFormat = r.WireFormat
		}

		if len(batch.Records) == 0 {
			if isControlCheckpoint(batch.Checkpoint) {
				if err := r.Source.Ack(batchCtx, batch.Checkpoint); err != nil {
					span.RecordError(err)
					span.End()
					return fmt.Errorf("ack source: %w", err)
				}
				if r.Checkpoints != nil && r.FlowID != "" && shouldPersistCheckpoint(batch.Checkpoint) {
					if err := r.Checkpoints.Put(batchCtx, r.FlowID, batch.Checkpoint); err != nil {
						span.RecordError(err)
						span.End()
						return fmt.Errorf("persist checkpoint: %w", err)
					}
				}
				span.End()
				continue
			}

			emptyReads++
			span.End()
			if r.MaxEmptyReads > 0 && emptyReads >= r.MaxEmptyReads {
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
			attribute.Int("records", len(batch.Records)),
			attribute.String("schema", batch.Schema.Name),
		)

		if err := r.writeDestinations(batchCtx, batch); err != nil {
			span.RecordError(err)
			span.End()
			return err
		}

		if err := r.Source.Ack(batchCtx, batch.Checkpoint); err != nil {
			span.RecordError(err)
			span.End()
			return fmt.Errorf("ack source: %w", err)
		}
		if r.Checkpoints != nil && r.FlowID != "" && shouldPersistCheckpoint(batch.Checkpoint) {
			if err := r.Checkpoints.Put(batchCtx, r.FlowID, batch.Checkpoint); err != nil {
				span.RecordError(err)
				span.End()
				return fmt.Errorf("persist checkpoint: %w", err)
			}
		}

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

func (r *Runner) writeDestinations(ctx context.Context, batch connector.Batch) error {
	if len(r.Destinations) == 0 {
		return nil
	}

	parallelism := r.Parallelism
	if parallelism <= 0 {
		parallelism = 1
	}
	if parallelism == 1 || len(r.Destinations) == 1 {
		for _, dest := range r.Destinations {
			if err := r.writeDestination(ctx, dest, batch); err != nil {
				return err
			}
		}
		return nil
	}

	if parallelism > len(r.Destinations) {
		parallelism = len(r.Destinations)
	}

	sem := make(chan struct{}, parallelism)
	errCh := make(chan error, len(r.Destinations))
	var wg sync.WaitGroup

	for _, dest := range r.Destinations {
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

func (r *Runner) writeDestination(ctx context.Context, dest DestinationConfig, batch connector.Batch) error {
	if len(batch.Records) > 0 {
		if dest.Dest.Capabilities().SupportsDDL {
			for _, record := range batch.Records {
				if record.Operation != connector.OpDDL && record.DDL == "" {
					continue
				}
				if err := dest.Dest.ApplyDDL(ctx, batch.Schema, record); err != nil {
					return fmt.Errorf("apply ddl destination %s: %w", dest.Spec.Name, err)
				}
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
		return fmt.Errorf("write destination %s: %w", dest.Spec.Name, err)
	}
	return nil
}
