package runner

import (
	"context"
	"fmt"

	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// FlowRunner executes flows with lifecycle updates.
type FlowRunner struct {
	Engine         workflow.Engine
	Checkpoints    connector.CheckpointStore
	Tracer         trace.Tracer
	Meters         *telemetry.Meters
	WireFormat     connector.WireFormat
	StrictWire     bool
	MaxEmpty       int
	Parallelism    int
	ResolveStaging bool
	DDLApplied     func(ctx context.Context, flowID string, lsn string, ddl string) error
	TraceSink      stream.TraceSink
}

func (r *FlowRunner) Run(ctx context.Context, f flow.Flow, source connector.Source, destinations []stream.DestinationConfig) error {
	if r.Engine == nil {
		return fmt.Errorf("workflow engine is required")
	}

	tracer := r.Tracer
	if tracer == nil {
		tracer = otel.Tracer("wallaby/flow")
	}

	flowSpanCtx, span := tracer.Start(ctx, "flow.run")
	defer span.End()

	_, err := r.Engine.Start(flowSpanCtx, f.ID)
	if err != nil {
		span.RecordError(err)
		return err
	}

	runner := stream.Runner{
		Source:         source,
		SourceSpec:     f.Source,
		Destinations:   destinations,
		Checkpoints:    r.Checkpoints,
		FlowID:         f.ID,
		Tracer:         tracer,
		Meters:         r.Meters,
		ResolveStaging: r.ResolveStaging,
		TraceSink:      r.TraceSink,
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
	if r.DDLApplied != nil {
		runner.DDLApplied = r.DDLApplied
	}
	if runner.SourceSpec.Type == connector.EndpointPostgres {
		if runner.SourceSpec.Options == nil {
			runner.SourceSpec.Options = map[string]string{}
		}
		if runner.SourceSpec.Options["flow_id"] == "" {
			runner.SourceSpec.Options["flow_id"] = f.ID
		}
	}
	if r.MaxEmpty > 0 {
		if runner.SourceSpec.Options == nil {
			runner.SourceSpec.Options = map[string]string{}
		}
		if runner.SourceSpec.Options["emit_empty"] == "" {
			runner.SourceSpec.Options["emit_empty"] = "true"
		}
	}
	if f.WireFormat != "" {
		runner.WireFormat = f.WireFormat
	} else if r.WireFormat != "" {
		runner.WireFormat = r.WireFormat
	}
	if f.Parallelism > 0 {
		runner.Parallelism = f.Parallelism
	} else if r.Parallelism > 0 {
		runner.Parallelism = r.Parallelism
	}
	runner.StrictFormat = r.StrictWire
	runner.MaxEmptyReads = r.MaxEmpty

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
