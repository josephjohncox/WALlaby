package workflow

import (
	"github.com/jackc/pgx/v5"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func unmarshalPersistedEndpoint(data []byte, role endpointcodec.Role) (*wallabypb.Endpoint, error) {
	return unmarshalPersistedEndpointWithRegistry(data, role, connector.DefaultRegistry)
}

func marshalFlowFields(f flow.Flow) ([]byte, []byte, []byte, error) {
	return marshalFlowFieldsWithRegistry(f, connector.DefaultRegistry)
}

func scanFlow(row pgx.Row) (flow.Flow, error) {
	return scanFlowWithRegistry(row, connector.DefaultRegistry)
}

func decodeFlow(f *flow.Flow, source, destinations, config []byte, state string, wire *string, parallelism int) error {
	return decodeFlowWithRegistry(f, source, destinations, config, state, wire, parallelism, connector.DefaultRegistry)
}
