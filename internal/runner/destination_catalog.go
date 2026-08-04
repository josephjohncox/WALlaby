package runner

import (
	"fmt"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// DestinationContract is one executable support-matrix row.
type DestinationContract struct {
	Type          connector.EndpointType
	Capabilities  connector.Capabilities
	Runtime       bool
	ReconcilesDDL bool
}

// DestinationContracts returns every declared destination, including endpoint
// constants that intentionally have no runtime adapter.
func DestinationContracts() ([]DestinationContract, error) {
	runtimeTypes := []connector.EndpointType{
		connector.EndpointPostgres,
		connector.EndpointPGStream,
		connector.EndpointKafka,
		connector.EndpointBufStream,
		connector.EndpointS3,
		connector.EndpointHTTP,
		connector.EndpointGRPC,
		connector.EndpointSnowflake,
		connector.EndpointSnowpipe,
		connector.EndpointClickHouse,
		connector.EndpointDuckDB,
		connector.EndpointDuckLake,
		connector.EndpointIceberg,
	}
	contracts := make([]DestinationContract, 0, len(runtimeTypes)+2)
	factory := Factory{}
	for _, endpointType := range runtimeTypes {
		spec := connector.Spec{Name: string(endpointType), Type: endpointType}
		destination, err := factory.destination(spec)
		if err != nil {
			return nil, fmt.Errorf("construct %s destination contract: %w", endpointType, err)
		}
		_, reconcilesDDL := destination.(connector.DDLReconciler)
		contracts = append(contracts, DestinationContract{
			Type:          endpointType,
			Capabilities:  connector.ResolveDestinationCapabilities(destination, spec),
			Runtime:       true,
			ReconcilesDDL: reconcilesDDL,
		})
	}
	for _, endpointType := range []connector.EndpointType{connector.EndpointProto, connector.EndpointParquet} {
		contracts = append(contracts, DestinationContract{
			Type: endpointType,
			Capabilities: connector.Capabilities{
				Support: connector.SupportPlaceholder,
			},
		})
	}
	return contracts, nil
}
