package grpc

import (
	"context"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFlowServiceUsesInjectedCustomRegistrationForAdmission(t *testing.T) {
	registry := connector.NewRegistry()
	if err := registry.RegisterSource("custom-source", func() connector.Source { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterDestination("custom-destination", func() connector.Destination { return nil }); err != nil {
		t.Fatal(err)
	}
	definition := &wallabypb.Flow{
		Id: "custom-flow",
		Source: &wallabypb.Endpoint{Name: "source", Config: &wallabypb.Endpoint_Custom{Custom: &wallabypb.CustomEndpointConfig{
			ConnectorType: "custom-source", Options: map[string]string{"source": "value"},
		}}},
		Destinations: []*wallabypb.Endpoint{{Name: "destination", Config: &wallabypb.Endpoint_Custom{Custom: &wallabypb.CustomEndpointConfig{
			ConnectorType: "custom-destination", Options: map[string]string{"destination": "value"},
		}}}},
		Config: &wallabypb.FlowConfig{TableMappings: &wallabypb.TableMappings{Version: 2, Destinations: []*wallabypb.DestinationTableMappings{{
			Destination:  "destination",
			FutureTables: &wallabypb.FutureTableMapping{Action: wallabypb.MappingAction_MAPPING_ACTION_INCLUDE, TargetSchema: "{{ .Schema }}", TargetTable: "{{ .Table }}", FutureColumns: &wallabypb.FutureColumnMapping{Action: wallabypb.MappingAction_MAPPING_ACTION_INCLUDE, TargetColumn: "{{ .Column }}"}, Write: &wallabypb.TableWritePolicy{Mode: wallabypb.TableWriteMode_TABLE_WRITE_MODE_APPEND}},
		}}}},
	}
	service := NewFlowServiceWithRegistry(workflow.NewMemoryEngineWithRegistry(registry), nil, registry)
	created, err := service.CreateFlow(context.Background(), &wallabypb.CreateFlowRequest{Flow: definition})
	if err != nil {
		t.Fatal(err)
	}
	if created.GetSource().GetCustom().GetConnectorType() != "custom-source" || created.GetDestinations()[0].GetCustom().GetConnectorType() != "custom-destination" {
		t.Fatalf("custom branches not preserved: %+v", created)
	}

	unknown := connector.NewRegistry()
	_, err = NewFlowServiceWithRegistry(workflow.NewMemoryEngine(), nil, unknown).CreateFlow(context.Background(), &wallabypb.CreateFlowRequest{Flow: definition})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown custom connectors error=%v, want InvalidArgument", err)
	}
}
