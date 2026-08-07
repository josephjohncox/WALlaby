package runner

import (
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func runnerTestSource(spec connector.RuntimeSpec) *wallabypb.Endpoint {
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

func runnerTestDestination(spec connector.RuntimeSpec) *wallabypb.Endpoint {
	endpoint, err := endpointcodec.Encode(spec, endpointcodec.RoleDestination)
	if err != nil {
		panic(err)
	}
	return endpoint
}

func setRunnerSourceOptions(definition *flow.Flow, values map[string]string) {
	spec, err := definition.DecodeSource(connector.DefaultRegistry)
	if err != nil {
		panic(err)
	}
	for key, value := range values {
		if value == "" {
			delete(spec.Options, key)
		} else {
			spec.Options[key] = value
		}
	}
	definition.Source = runnerTestSource(spec)
}

func mappedRunnerTestFlow(definition flow.Flow) flow.Flow {
	if definition.Source == nil {
		definition.Source = runnerTestSource(connector.RuntimeSpec{Type: connector.EndpointPostgres})
	}
	if len(definition.Destinations) == 0 {
		definition.Destinations = []*wallabypb.Endpoint{runnerTestDestination(connector.RuntimeSpec{Name: "dest", Type: connector.EndpointPostgres})}
	}
	for index := range definition.Destinations {
		if definition.Destinations[index].Name == "" {
			definition.Destinations[index].Name = "dest"
		}
	}
	runtimeDestinations, err := definition.DecodeDestinations(connector.DefaultRegistry)
	if err != nil {
		panic(err)
	}
	definition.Config.TableMappings = flow.NewTableMappings(runtimeDestinations)
	if definition.Config.DDL.AutoApply == nil {
		autoApply := false
		definition.Config.DDL.AutoApply = &autoApply
	}
	return definition
}
