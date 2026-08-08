package grpc

import (
	"context"
	"strings"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func typedCDCSourceEndpoint(t *testing.T) *wallabypb.Endpoint {
	t.Helper()
	endpoint, err := endpointcodec.Encode(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: map[string]string{"mode": connector.SourceModeCDC}}, endpointcodec.RoleSource)
	if err != nil {
		t.Fatal(err)
	}
	return endpoint
}

func TestFlowServiceRejectsBuiltinCustomBypassBeforePersistence(t *testing.T) {
	t.Parallel()
	service := NewFlowService(workflow.NewMemoryEngine(), nil)
	definition := &wallabypb.Flow{
		Id:     "builtin-custom-bypass",
		Source: typedCDCSourceEndpoint(t),
		Destinations: []*wallabypb.Endpoint{{Config: &wallabypb.Endpoint_Custom{Custom: &wallabypb.CustomEndpointConfig{
			ConnectorType: "iceberg", Options: map[string]string{"aws_session_token": "unsafe"},
		}}}},
	}
	if _, err := service.CreateFlow(context.Background(), &wallabypb.CreateFlowRequest{Flow: definition}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateFlow() error=%v, want InvalidArgument", err)
	}
}

func TestFlowServiceRejectsInvalidTableMappingsBeforePersistence(t *testing.T) {
	t.Parallel()
	service := NewFlowService(workflow.NewMemoryEngine(), nil)
	destination := connector.RuntimeSpec{Name: "warehouse", Type: connector.EndpointPostgres}
	definition := flow.Flow{
		ID:           "invalid-mappings",
		Source:       testSourceEndpoint(connector.RuntimeSpec{Type: connector.EndpointPostgres}),
		Destinations: []*wallabypb.Endpoint{testDestinationEndpoint(destination)},
	}
	definition.Config.TableMappings = flow.NewTableMappings([]connector.RuntimeSpec{destination})
	columns := flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{{ .Column }}"}
	definition.Config.TableMappings.Destinations[0].Tables = []flow.TableMapping{
		{SourceSchema: "public", SourceTable: "left", Action: flow.MappingActionInclude, TargetSchema: "public", TargetTable: "same", FutureColumns: columns, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeAppend}},
		{SourceSchema: "public", SourceTable: "right", Action: flow.MappingActionInclude, TargetSchema: "public", TargetTable: "same", FutureColumns: columns, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeAppend}},
	}
	_, err := service.CreateFlow(context.Background(), &wallabypb.CreateFlowRequest{Flow: flowToProtoForTest(definition)})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "same target") {
		t.Fatalf("CreateFlow() error=%v, want invalid target collision", err)
	}
}

func TestFlowServiceValidatesMaterializationBeforePersistence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	service := NewFlowService(workflow.NewMemoryEngine(), nil)
	base := &wallabypb.Flow{
		Id:     "materialization-validation",
		Source: typedCDCSourceEndpoint(t),
	}
	addProtoTestMappings(base)
	if _, err := service.CreateFlow(ctx, &wallabypb.CreateFlowRequest{Flow: base}); err != nil {
		t.Fatal(err)
	}
	invalid := &wallabypb.Flow{
		Id:           base.Id,
		Source:       base.Source,
		Destinations: base.Destinations,
		Config: &wallabypb.FlowConfig{
			AckPolicy:       wallabypb.AckPolicy_ACK_POLICY_ALL,
			Materialization: &wallabypb.MaterializationPolicy{ProjectionId: "canonical_cdc_parquet_v1"},
			TableMappings:   base.Config.TableMappings,
		},
	}
	if _, err := service.UpdateFlow(ctx, &wallabypb.UpdateFlowRequest{Flow: invalid}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("UpdateFlow() error=%v, want InvalidArgument", err)
	}
	if _, err := service.ReconfigureFlow(ctx, &wallabypb.ReconfigureFlowRequest{Flow: invalid}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ReconfigureFlow() error=%v, want InvalidArgument", err)
	}
}
