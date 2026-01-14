package runner

import (
	"fmt"

	"github.com/josephjohncox/wallaby/connectors/destinations/bufstream"
	"github.com/josephjohncox/wallaby/connectors/destinations/clickhouse"
	"github.com/josephjohncox/wallaby/connectors/destinations/duckdb"
	"github.com/josephjohncox/wallaby/connectors/destinations/ducklake"
	grpcdest "github.com/josephjohncox/wallaby/connectors/destinations/grpc"
	httpdest "github.com/josephjohncox/wallaby/connectors/destinations/http"
	"github.com/josephjohncox/wallaby/connectors/destinations/kafka"
	"github.com/josephjohncox/wallaby/connectors/destinations/pgstream"
	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	"github.com/josephjohncox/wallaby/connectors/destinations/s3"
	"github.com/josephjohncox/wallaby/connectors/destinations/snowflake"
	"github.com/josephjohncox/wallaby/connectors/destinations/snowpipe"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/replication"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

// Factory builds connectors for flows.
type Factory struct {
	SchemaHook        replication.SchemaHook
	SchemaHookForFlow func(flow.Flow) replication.SchemaHook
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
	switch spec.Type {
	case connector.EndpointPostgres:
		if spec.Options != nil {
			if mode := spec.Options["mode"]; mode == "backfill" {
				return &pgsource.BackfillSource{}, nil
			}
		}
		source := &pgsource.Source{SchemaHook: hook}
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
	switch spec.Type {
	case connector.EndpointKafka:
		return &kafka.Destination{}, nil
	case connector.EndpointS3:
		return &s3.Destination{}, nil
	case connector.EndpointHTTP:
		return &httpdest.Destination{}, nil
	case connector.EndpointGRPC:
		return &grpcdest.Destination{}, nil
	case connector.EndpointPGStream:
		return &pgstream.Destination{}, nil
	case connector.EndpointSnowflake:
		return &snowflake.Destination{}, nil
	case connector.EndpointSnowpipe:
		return &snowpipe.Destination{}, nil
	case connector.EndpointDuckDB:
		return &duckdb.Destination{}, nil
	case connector.EndpointDuckLake:
		return &ducklake.Destination{}, nil
	case connector.EndpointClickHouse:
		return &clickhouse.Destination{}, nil
	case connector.EndpointPostgres:
		return &pgdest.Destination{}, nil
	case connector.EndpointBufStream:
		return &bufstream.Destination{}, nil
	default:
		return nil, fmt.Errorf("unsupported destination type: %s", spec.Type)
	}
}
