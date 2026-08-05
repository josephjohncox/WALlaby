package runner

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/replication"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

// Factory builds connectors for flows.
type Factory struct {
	SchemaHook        replication.SchemaHook
	SchemaHookForFlow func(flow.Flow) replication.SchemaHook
	Meters            *telemetry.Meters
	ManagedControl    *pgxpool.Pool
	ManagedAuthority  authority.Store
	BootstrapHooks    bootstrap.Hooks
}

func (f Factory) Source(spec connector.Spec) (connector.Source, error) {
	return f.source(spec, f.SchemaHook)
}

// SourceForFlow builds a source with per-flow configuration.
func (f Factory) SourceForFlow(fdef flow.Flow) (connector.Source, error) {
	hook := f.SchemaHook
	if f.SchemaHookForFlow != nil {
		if candidate := f.SchemaHookForFlow(fdef); candidate != nil {
			hook = candidate
		}
	}
	return f.source(fdef.Source, hook)
}

func (f Factory) source(spec connector.Spec, hook replication.SchemaHook) (connector.Source, error) {
	mode, err := connector.NormalizeSourceMode("")
	if spec.Options != nil {
		mode, err = connector.NormalizeSourceMode(spec.Options["mode"])
	}
	if err != nil {
		return nil, err
	}

	switch spec.Type {
	case connector.EndpointPostgres:
		if mode == connector.SourceModeBackfill {
			return &pgsource.BackfillSource{}, nil
		}
		source := &pgsource.Source{
			SchemaHook: hook, Meters: f.Meters,
			ManagedControl: f.ManagedControl, ManagedAuthority: f.ManagedAuthority,
			BootstrapHooks: f.BootstrapHooks,
		}
		return source, nil
	default:
		return nil, fmt.Errorf("unsupported source type: %s", spec.Type)
	}
}

func (f Factory) Destinations(specs []connector.Spec) ([]stream.DestinationConfig, error) {
	items := make([]stream.DestinationConfig, 0, len(specs))
	for _, spec := range specs {
		dest, err := f.destination(spec)
		if err != nil {
			return nil, err
		}
		items = append(items, stream.DestinationConfig{Spec: spec, Dest: dest})
	}
	return items, nil
}

// DestinationsForFlow builds destinations, applying flow-level defaults.
func (f Factory) DestinationsForFlow(fdef flow.Flow) ([]stream.DestinationConfig, error) {
	specs := flow.ApplyRegistryDefaults(fdef.Destinations, fdef.Config)
	return f.Destinations(specs)
}

func (f Factory) destination(spec connector.Spec) (connector.Destination, error) {
	registration, ok := destinationRegistration(spec.Type)
	if !ok || registration.New == nil {
		return nil, fmt.Errorf("unsupported destination type: %s", spec.Type)
	}
	destination := registration.New()
	if destination == nil {
		return nil, fmt.Errorf("destination constructor returned nil: %s", spec.Type)
	}
	if _, err := registration.ResolveCapabilities(destination, spec); err != nil {
		return nil, fmt.Errorf("destination %s capability profile: %w", spec.Type, err)
	}
	return destination, nil
}
