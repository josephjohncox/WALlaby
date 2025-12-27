package runner

import (
	"context"
	"fmt"

	"github.com/josephjohncox/ductstream/internal/flow"
	"github.com/josephjohncox/ductstream/internal/workflow"
	"github.com/josephjohncox/ductstream/pkg/connector"
	"github.com/josephjohncox/ductstream/pkg/stream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// FlowRunner executes flows with lifecycle updates.
type FlowRunner struct {
	Engine      workflow.Engine
	Checkpoints connector.CheckpointStore
	Tracer      trace.Tracer
}

func (r *FlowRunner) Run(ctx context.Context, f flow.Flow, source connector.Source, destinations []stream.DestinationConfig) error {
	if r.Engine == nil {
		return fmt.Errorf("workflow engine is required")
	}

	tracer := r.Tracer
	if tracer == nil {
		tracer = otel.Tracer("ductstream/flow")
	}

	flowSpanCtx, span := tracer.Start(ctx, "flow.run")
	defer span.End()

	_, err := r.Engine.Start(flowSpanCtx, f.ID)
	if err != nil {
		span.RecordError(err)
		return err
	}

	runner := stream.Runner{
		Source:       source,
		SourceSpec:   f.Source,
		Destinations: destinations,
		Checkpoints:  r.Checkpoints,
		FlowID:       f.ID,
		Tracer:       tracer,
	}

	if err := runner.Run(flowSpanCtx); err != nil {
		span.RecordError(err)
		_, _ = r.Engine.Stop(flowSpanCtx, f.ID)
		return err
	}

	_, err = r.Engine.Stop(flowSpanCtx, f.ID)
	if err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}
