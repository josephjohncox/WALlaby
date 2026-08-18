package runner

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/connectors/destinations/snowflake"
	"github.com/josephjohncox/wallaby/connectors/destinations/snowpipe"
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
	ConnectorRegistry *connector.Registry
	SnowflakePolicy   connector.SnowflakeDeploymentPolicy
}

func (f Factory) Source(spec connector.RuntimeSpec) (connector.Source, error) {
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
	registry := f.ConnectorRegistry
	if registry == nil {
		registry = connector.DefaultRegistry
	}
	spec, err := fdef.DecodeSource(registry)
	if err != nil {
		return nil, err
	}
	return f.source(spec, hook)
}

func (f Factory) source(spec connector.RuntimeSpec, hook replication.SchemaHook) (connector.Source, error) {
	mode, err := connector.NormalizeSourceMode("")
	if spec.Options != nil {
		mode, err = connector.NormalizeSourceMode(spec.Options["mode"])
	}
	if err != nil {
		return nil, err
	}
	if spec.Type == connector.EndpointPostgres && mode == connector.SourceModeBackfill {
		for _, key := range []string{"publication_tables", "publication_schemas", "sync_publication", "sync_publication_mode"} {
			if spec.Options[key] != "" {
				return nil, fmt.Errorf("postgres backfill source rejects CDC publication option %s", key)
			}
		}
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
		registry := f.ConnectorRegistry
		if registry == nil {
			registry = connector.DefaultRegistry
		}
		return registry.NewSource(spec.Type)
	}
}

func (f Factory) Destinations(specs []connector.RuntimeSpec) ([]stream.DestinationConfig, error) {
	if err := f.SnowflakePolicy.Admit(specs); err != nil {
		return nil, err
	}
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

// DestinationsForFlow builds destinations from detached runtime adapters.
func (f Factory) DestinationsForFlow(fdef flow.Flow) ([]stream.DestinationConfig, error) {
	registry := f.ConnectorRegistry
	if registry == nil {
		registry = connector.DefaultRegistry
	}
	specs, err := fdef.DecodeDestinations(registry)
	if err != nil {
		return nil, err
	}
	return f.Destinations(specs)
}

func (f Factory) destination(spec connector.RuntimeSpec) (connector.Destination, error) {
	if err := f.SnowflakePolicy.Admit([]connector.RuntimeSpec{spec}); err != nil {
		return nil, err
	}
	registration, ok := destinationRegistration(spec.Type)
	if !ok {
		registry := f.ConnectorRegistry
		if registry == nil {
			registry = connector.DefaultRegistry
		}
		return registry.NewDestination(spec.Type)
	}
	if registration.New == nil {
		return nil, fmt.Errorf("unsupported destination type: %s", spec.Type)
	}
	var destination connector.Destination
	switch spec.Type {
	case connector.EndpointSnowflake:
		destination = snowflake.NewDestination(f.SnowflakePolicy)
	case connector.EndpointSnowpipe:
		destination = snowpipe.NewDestination(f.SnowflakePolicy)
	default:
		destination = registration.New()
	}
	if destination == nil {
		return nil, fmt.Errorf("destination constructor returned nil: %s", spec.Type)
	}
	if _, err := registration.ResolveCapabilities(destination, spec); err != nil {
		return nil, fmt.Errorf("destination %s capability profile: %w", spec.Type, err)
	}
	return destination, nil
}
