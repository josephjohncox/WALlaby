package integration_test

import (
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func testFlowSource(spec connector.RuntimeSpec) *wallabypb.Endpoint {
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

func testFlowDestination(spec connector.RuntimeSpec) *wallabypb.Endpoint {
	endpoint, err := endpointcodec.Encode(spec, endpointcodec.RoleDestination)
	if err != nil {
		panic(err)
	}
	return endpoint
}

func testFlowDestinations(specs ...connector.RuntimeSpec) []*wallabypb.Endpoint {
	result := make([]*wallabypb.Endpoint, 0, len(specs))
	for _, spec := range specs {
		result = append(result, testFlowDestination(spec))
	}
	return result
}

func testFlowRuntimeDestinations(endpoints []*wallabypb.Endpoint) []connector.RuntimeSpec {
	result := make([]connector.RuntimeSpec, 0, len(endpoints))
	for _, endpoint := range endpoints {
		spec, err := endpointcodec.Decode(endpoint, endpointcodec.RoleDestination)
		if err != nil {
			panic(err)
		}
		result = append(result, spec)
	}
	return result
}
