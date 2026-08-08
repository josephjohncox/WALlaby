package workflow

import (
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func workflowTestSource(spec connector.RuntimeSpec) *wallabypb.Endpoint {
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

func workflowTestDestination(spec connector.RuntimeSpec) *wallabypb.Endpoint {
	endpoint, err := endpointcodec.Encode(spec, endpointcodec.RoleDestination)
	if err != nil {
		panic(err)
	}
	return endpoint
}

func mappedTestFlow(definition flow.Flow) flow.Flow {
	if definition.Source == nil {
		definition.Source = workflowTestSource(connector.RuntimeSpec{Name: "test-source", Type: connector.EndpointPostgres})
	}
	if len(definition.Destinations) == 0 {
		definition.Destinations = []*wallabypb.Endpoint{workflowTestDestination(connector.RuntimeSpec{Name: "test-destination", Type: connector.EndpointPostgres})}
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
