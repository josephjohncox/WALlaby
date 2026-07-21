package runner

import (
	"fmt"

	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"go.opentelemetry.io/otel/trace"
)

// StreamRunnerConfig contains process-level defaults and dependencies used to
// construct a stream runner for a flow.
type StreamRunnerConfig struct {
	Checkpoints         connector.CheckpointStore
	Tracer              trace.Tracer
	Meters              *telemetry.Meters
	DefaultWireFormat   connector.WireFormat
	StrictFormat        bool
	MaxEmptyReads       int
	DefaultParallelism  int
	ResolveStaging      bool
	DDLExecutions       stream.DDLExecutionStore
	TraceSink           stream.TraceSink
	RunFence            *authority.RunFence
	DeliveryCoordinator stream.ManagedDeliveryCoordinator
}

// NewStreamRunner constructs a stream runner without mutating the flow or
// destination configuration supplied by the caller. Construction fails unless
// the flow has durable checkpoint storage and a stable identity.
func NewStreamRunner(f flow.Flow, source connector.Source, destinations []stream.DestinationConfig, cfg StreamRunnerConfig) (stream.Runner, error) {
	sourceSpec := cloneSpec(f.Source)
	if sourceSpec.Type == connector.EndpointPostgres {
		if sourceSpec.Options == nil {
			sourceSpec.Options = make(map[string]string)
		}
		if sourceSpec.Options["flow_id"] == "" {
			sourceSpec.Options["flow_id"] = f.ID
		}
	}
	if cfg.MaxEmptyReads > 0 {
		if sourceSpec.Options == nil {
			sourceSpec.Options = make(map[string]string)
		}
		if sourceSpec.Options["emit_empty"] == "" {
			sourceSpec.Options["emit_empty"] = "true"
		}
	}

	clonedDestinations := make([]stream.DestinationConfig, len(destinations))
	for i, destination := range destinations {
		clonedDestinations[i] = destination
		clonedDestinations[i].Spec = cloneSpec(destination.Spec)
	}

	wireFormat := f.WireFormat
	if wireFormat == "" {
		wireFormat = cfg.DefaultWireFormat
	}
	parallelism := f.Parallelism
	if parallelism <= 0 {
		parallelism = cfg.DefaultParallelism
	}

	if cfg.Checkpoints == nil {
		return stream.Runner{}, fmt.Errorf("streaming requires a durable checkpoint store before source acknowledgement")
	}
	if f.ID == "" {
		return stream.Runner{}, fmt.Errorf("streaming requires a non-empty flow id for durable checkpoints")
	}

	requireDDLExecution := f.Config.DDL.AutoApply != nil && *f.Config.DDL.AutoApply
	if managedSourceSpec(sourceSpec) {
		if err := validateManagedAdmission(f, source, sourceSpec, clonedDestinations, cfg); err != nil {
			return stream.Runner{}, err
		}
	}

	var checkpointOutbox connector.CheckpointOutboxStore
	if f.Config.AckPolicy == stream.AckPolicyPrimary {
		store, ok := cfg.Checkpoints.(connector.CheckpointOutboxStore)
		if !ok {
			return stream.Runner{}, fmt.Errorf("primary acknowledgement requires a durable checkpoint store with atomic outbox support")
		}
		checkpointOutbox = store
	}

	if err := stream.ValidateDestinationContracts(
		clonedDestinations,
		f.Config.AckPolicy,
		f.Config.PrimaryDestination,
		requireDDLExecution,
	); err != nil {
		return stream.Runner{}, fmt.Errorf("validate flow destination contracts: %w", err)
	}
	if requireDDLExecution && cfg.DDLExecutions == nil {
		return stream.Runner{}, fmt.Errorf("automatic DDL execution requires durable execution receipt storage")
	}

	return stream.Runner{
		Source:              source,
		SourceSpec:          sourceSpec,
		Destinations:        clonedDestinations,
		Checkpoints:         cfg.Checkpoints,
		CheckpointOutbox:    checkpointOutbox,
		FlowID:              f.ID,
		ResolveStaging:      cfg.ResolveStaging,
		Tracer:              cfg.Tracer,
		Meters:              cfg.Meters,
		MaxEmptyReads:       cfg.MaxEmptyReads,
		WireFormat:          wireFormat,
		StrictFormat:        cfg.StrictFormat,
		Parallelism:         parallelism,
		AckPolicy:           f.Config.AckPolicy,
		PrimaryDestination:  f.Config.PrimaryDestination,
		RequireDDLExecution: requireDDLExecution,
		FailureMode:         f.Config.FailureMode,
		GiveUpPolicy:        f.Config.GiveUpPolicy,
		DDLExecutions:       cfg.DDLExecutions,
		TraceSink:           cfg.TraceSink,
		RunFence:            cfg.RunFence,
		DeliveryCoordinator: cfg.DeliveryCoordinator,
	}, nil
}

func cloneSpec(spec connector.Spec) connector.Spec {
	clone := spec
	if spec.Options != nil {
		clone.Options = make(map[string]string, len(spec.Options))
		for key, value := range spec.Options {
			clone.Options[key] = value
		}
	}
	return clone
}
