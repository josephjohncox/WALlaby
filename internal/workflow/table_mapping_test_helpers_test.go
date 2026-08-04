package workflow

import (
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func mappedTestFlow(definition flow.Flow) flow.Flow {
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
