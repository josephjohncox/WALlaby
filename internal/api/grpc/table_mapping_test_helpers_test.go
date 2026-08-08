package grpc

import (
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func testSourceEndpoint(spec connector.RuntimeSpec) *wallabypb.Endpoint {
	if spec.Options == nil {
		spec.Options = map[string]string{}
	}
	if spec.Type == connector.EndpointPostgres && spec.Options["mode"] == "" {
		spec.Options["mode"] = connector.SourceModeCDC
	}
	endpoint, err := endpointcodec.Encode(spec, endpointcodec.RoleSource)
	if err != nil {
		panic(err)
	}
	return endpoint
}

func testDestinationEndpoint(spec connector.RuntimeSpec) *wallabypb.Endpoint {
	endpoint, err := endpointcodec.Encode(spec, endpointcodec.RoleDestination)
	if err != nil {
		panic(err)
	}
	return endpoint
}

func mappedGRPCTestFlow(definition flow.Flow) flow.Flow {
	if len(definition.Destinations) == 0 {
		definition.Destinations = []*wallabypb.Endpoint{testDestinationEndpoint(connector.RuntimeSpec{Name: "test-destination", Type: connector.EndpointPostgres})}
	}
	for index := range definition.Destinations {
		if definition.Destinations[index].Name == "" {
			definition.Destinations[index].Name = "test-destination"
		}
	}
	runtimeDestinations, err := definition.DecodeDestinations(connector.DefaultRegistry)
	if err != nil {
		panic(err)
	}
	definition.Config.TableMappings = flow.NewTableMappings(runtimeDestinations)
	return definition
}

func flowToProtoForTest(definition flow.Flow) *wallabypb.Flow {
	encoded, err := flowToProto(definition, connector.DefaultRegistry)
	if err != nil {
		panic(err)
	}
	return encoded
}

func addProtoTestMappings(definition *wallabypb.Flow) {
	if len(definition.Destinations) == 0 {
		definition.Destinations = []*wallabypb.Endpoint{{Name: "test-destination", Config: &wallabypb.Endpoint_PostgresDestination{PostgresDestination: &wallabypb.PostgresDestinationConfig{}}}}
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
	model := make([]connector.RuntimeSpec, 0, len(definition.Destinations))
	for _, destination := range definition.Destinations {
		spec, err := endpointcodec.Decode(destination, endpointcodec.RoleDestination)
		if err != nil {
			panic(err)
		}
		model = append(model, spec)
	}
	definition.Config.TableMappings = tableMappingsToProto(flow.NewTableMappings(model))
}
