package grpc

import (
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func mappedGRPCTestFlow(definition flow.Flow) flow.Flow {
	if len(definition.Destinations) == 0 {
		definition.Destinations = []connector.Spec{{Name: "test-destination", Type: connector.EndpointPostgres}}
	}
	for index := range definition.Destinations {
		if definition.Destinations[index].Name == "" {
			definition.Destinations[index].Name = "test-destination"
		}
	}
	definition.Config.TableMappings = flow.NewTableMappings(definition.Destinations)
	return definition
}

func addProtoTestMappings(definition *wallabypb.Flow) {
	if len(definition.Destinations) == 0 {
		definition.Destinations = []*wallabypb.Endpoint{{Name: "test-destination", Type: wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES}}
	}
	for index, destination := range definition.Destinations {
		if destination.Name == "" {
			destination.Name = "test-destination"
			if index > 0 {
				destination.Name += "-" + string(rune('a'+index))
			}
		}
	}
	if definition.Config == nil {
		definition.Config = &wallabypb.FlowConfig{}
	}
	model := make([]connector.Spec, 0, len(definition.Destinations))
	for _, destination := range definition.Destinations {
		model = append(model, connector.Spec{Name: destination.Name, Type: endpointTypeFromProto(destination.Type), Options: destination.Options})
	}
	definition.Config.TableMappings = tableMappingsToProto(flow.NewTableMappings(model))
}
