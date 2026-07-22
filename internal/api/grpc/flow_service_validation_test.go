package grpc

import (
	"context"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFlowServiceValidatesMaterializationBeforePersistence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	service := NewFlowService(workflow.NewMemoryEngine(), nil)
	base := &wallabypb.Flow{
		Id:     "materialization-validation",
		Source: &wallabypb.Endpoint{Type: wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES},
	}
	if _, err := service.CreateFlow(ctx, &wallabypb.CreateFlowRequest{Flow: base}); err != nil {
		t.Fatal(err)
	}
	invalid := &wallabypb.Flow{
		Id:     base.Id,
		Source: base.Source,
		Config: &wallabypb.FlowConfig{
			AckPolicy:       wallabypb.AckPolicy_ACK_POLICY_ALL,
			Materialization: &wallabypb.MaterializationPolicy{ProjectionId: "canonical_cdc_parquet_v1"},
		},
	}
	if _, err := service.UpdateFlow(ctx, &wallabypb.UpdateFlowRequest{Flow: invalid}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("UpdateFlow() error=%v, want InvalidArgument", err)
	}
	if _, err := service.ReconfigureFlow(ctx, &wallabypb.ReconfigureFlowRequest{Flow: invalid}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ReconfigureFlow() error=%v, want InvalidArgument", err)
	}
}
