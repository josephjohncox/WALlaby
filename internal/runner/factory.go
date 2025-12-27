package runner

import (
	"fmt"

	httpdest "github.com/josephjohncox/ductstream/connectors/destinations/http"
	"github.com/josephjohncox/ductstream/connectors/destinations/kafka"
	"github.com/josephjohncox/ductstream/connectors/destinations/pgstream"
	"github.com/josephjohncox/ductstream/connectors/destinations/s3"
	"github.com/josephjohncox/ductstream/connectors/sources/postgres"
	"github.com/josephjohncox/ductstream/internal/replication"
	"github.com/josephjohncox/ductstream/pkg/connector"
	"github.com/josephjohncox/ductstream/pkg/stream"
)

// Factory builds connectors for flows.
type Factory struct {
	SchemaHook replication.SchemaHook
}

func (f Factory) Source(spec connector.Spec) (connector.Source, error) {
	switch spec.Type {
	case connector.EndpointPostgres:
		if spec.Options != nil {
			if mode := spec.Options["mode"]; mode == "backfill" {
				return &postgres.BackfillSource{}, nil
			}
		}
		source := &postgres.Source{SchemaHook: f.SchemaHook}
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

func (f Factory) destination(spec connector.Spec) (connector.Destination, error) {
	switch spec.Type {
	case connector.EndpointKafka:
		return &kafka.Destination{}, nil
	case connector.EndpointS3:
		return &s3.Destination{}, nil
	case connector.EndpointHTTP:
		return &httpdest.Destination{}, nil
	case connector.EndpointPGStream:
		return &pgstream.Destination{}, nil
	default:
		return nil, fmt.Errorf("unsupported destination type: %s", spec.Type)
	}
}
