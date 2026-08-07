package main

import (
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func testEndpointConfig(name string, endpointType connector.EndpointType, options map[string]string, role endpointcodec.Role) endpointConfig {
	endpoint, err := endpointcodec.Encode(connector.RuntimeSpec{Name: name, Type: endpointType, Options: options}, role)
	if err != nil {
		panic(err)
	}
	return endpointConfig{endpoint: endpoint}
}

func testSourceEndpoint(name string, options map[string]string) endpointConfig {
	return testEndpointConfig(name, connector.EndpointPostgres, options, endpointcodec.RoleSource)
}

func testDestinationEndpoint(name string, endpointType connector.EndpointType, options map[string]string) endpointConfig {
	return testEndpointConfig(name, endpointType, options, endpointcodec.RoleDestination)
}

func testEndpointName(config endpointConfig) string {
	if config.endpoint == nil {
		return ""
	}
	return config.endpoint.GetName()
}

func testEndpointOptions(config endpointConfig, role endpointcodec.Role) map[string]string {
	spec, err := endpointcodec.Decode(config.endpoint, role)
	if err != nil {
		panic(err)
	}
	return spec.Options
}
