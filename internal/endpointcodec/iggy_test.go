package endpointcodec_test

import (
	"reflect"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestIggyTypedEndpointRoundTrip(t *testing.T) {
	t.Parallel()
	for _, auth := range []map[string]string{{"username": "user", "password": "example-password"}, {"token": "example-token"}} {
		spec := connector.RuntimeSpec{Name: "native-iggy", Type: connector.EndpointIggy, Options: map[string]string{
			"address": "iggy.example.test:8090", "stream": "cdc", "topic": "orders", "partition_id": "0",
			"format": "proto", "tls": "false", "timeout": "3s", "max_message_bytes": "1000000",
			"type_mappings": `{"jsonb":"json"}`,
		}}
		for key, value := range auth {
			spec.Options[key] = value
		}
		endpoint, err := endpointcodec.Encode(spec, endpointcodec.RoleDestination)
		if err != nil {
			t.Fatal(err)
		}
		if endpoint.GetIggy() == nil || endpoint.GetIggy().Tls == nil || endpoint.GetIggy().GetTls() || endpoint.GetIggy().PartitionId == nil {
			t.Fatalf("Iggy endpoint lost its typed branch or explicit false/zero presence: %v", endpoint)
		}
		roundTrip, err := endpointcodec.Decode(endpoint, endpointcodec.RoleDestination)
		if err != nil || !reflect.DeepEqual(roundTrip, spec) {
			t.Fatalf("round trip=%+v err=%v want=%+v", roundTrip, err, spec)
		}
		if _, err := endpointcodec.Decode(endpoint, endpointcodec.RoleSource); err == nil {
			t.Fatal("Iggy destination was accepted as a source")
		}
	}
}

func TestIggyTypedEndpointRejectsUnknownOrInvalidOptions(t *testing.T) {
	t.Parallel()
	for key, value := range map[string]string{
		"brokers": "localhost:9092", "transactional_producer": "true", "partition_id": "-1",
		"max_message_bytes": "4294967296", "timeout": "-1s", "tls": "sometimes", "format": "invalid",
	} {
		if _, err := endpointcodec.Encode(connector.RuntimeSpec{Type: connector.EndpointIggy, Options: map[string]string{key: value}}, endpointcodec.RoleDestination); err == nil {
			t.Errorf("accepted invalid option %s=%s", key, value)
		}
	}
	registry := connector.NewRegistry()
	if err := registry.RegisterDestination("iggy", func() connector.Destination { return nil }); err == nil {
		t.Fatal("custom connector can override the built-in Iggy endpoint")
	}
	endpoint := &wallabypb.Endpoint{Config: &wallabypb.Endpoint_Custom{Custom: &wallabypb.CustomEndpointConfig{ConnectorType: "iggy"}}}
	if _, err := endpointcodec.DecodeWithRegistry(endpoint, endpointcodec.RoleDestination, registry); err == nil {
		t.Fatal("Iggy bypassed its typed configuration through custom.options")
	}
}
