package schemabaseline_test

import (
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func schemaBaselineSource(spec connector.RuntimeSpec) *wallabypb.Endpoint {
	if spec.Options == nil {
		spec.Options = map[string]string{}
	}
	if spec.Options["mode"] == "" {
		spec.Options["mode"] = connector.SourceModeCDC
	}
	endpoint, err := endpointcodec.Encode(spec, endpointcodec.RoleSource)
	if err != nil {
		panic(err)
	}
	return endpoint
}

func schemaBaselineDestination(spec connector.RuntimeSpec) *wallabypb.Endpoint {
	endpoint, err := endpointcodec.Encode(spec, endpointcodec.RoleDestination)
	if err != nil {
		panic(err)
	}
	return endpoint
}
