package grpc

import (
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestMaterializedAckPolicyAndProjectionRoundTrip(t *testing.T) {
	t.Parallel()

	model := flow.Config{
		AckPolicy:       stream.AckPolicyMaterialized,
		Materialization: flow.MaterializationPolicy{ProjectionID: "canonical_cdc_parquet_v1"},
	}
	wire := flowConfigToProto(model)
	if wire.GetAckPolicy() != wallabypb.AckPolicy_ACK_POLICY_MATERIALIZED {
		t.Fatalf("ack policy=%s, want materialized", wire.GetAckPolicy())
	}
	if wire.GetMaterialization().GetProjectionId() != "canonical_cdc_parquet_v1" {
		t.Fatalf("projection=%q", wire.GetMaterialization().GetProjectionId())
	}
	if roundTrip := flowConfigFromProto(wire); !roundTrip.Equal(model) {
		t.Fatalf("flow config round trip=%+v, want %+v", roundTrip, model)
	}
	if got := int32(wallabypb.AckPolicy_ACK_POLICY_MATERIALIZED); got != 3 {
		t.Fatalf("ACK_POLICY_MATERIALIZED=%d, want wire value 3", got)
	}
}

func TestIcebergEndpointWireValueAndRoundTrip(t *testing.T) {
	t.Parallel()
	wire := endpointTypeToProto(connector.EndpointIceberg)
	if wire != wallabypb.EndpointType_ENDPOINT_TYPE_ICEBERG || int32(wire) != 15 {
		t.Fatalf("Iceberg endpoint wire value=%d", wire)
	}
	if model := endpointTypeFromProto(wire); model != connector.EndpointIceberg {
		t.Fatalf("Iceberg endpoint round trip=%q", model)
	}
}

func TestFlowFromProtoRejectsUnknownAcknowledgementPolicy(t *testing.T) {
	t.Parallel()

	_, err := flowFromProto(&wallabypb.Flow{
		Source: &wallabypb.Endpoint{Type: wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES},
		Config: &wallabypb.FlowConfig{AckPolicy: wallabypb.AckPolicy(99)},
	})
	if err == nil {
		t.Fatal("unknown acknowledgement policy was silently mapped to unspecified")
	}
}

func TestFlowStateRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		model flow.State
		wire  wallabypb.FlowState
	}{
		{flow.StateCreated, wallabypb.FlowState_FLOW_STATE_CREATED},
		{flow.StateRunning, wallabypb.FlowState_FLOW_STATE_RUNNING},
		{flow.StatePaused, wallabypb.FlowState_FLOW_STATE_PAUSED},
		{flow.StateStopping, wallabypb.FlowState_FLOW_STATE_STOPPING},
		{flow.StateFailed, wallabypb.FlowState_FLOW_STATE_FAILED},
		{flow.StateStopped, wallabypb.FlowState_FLOW_STATE_STOPPED},
	}
	for _, tt := range tests {
		if got := flowStateToProto(tt.model); got != tt.wire {
			t.Errorf("flowStateToProto(%s) = %s, want %s", tt.model, got, tt.wire)
		}
		if got := flowStateFromProto(tt.wire); got != tt.model {
			t.Errorf("flowStateFromProto(%s) = %s, want %s", tt.wire, got, tt.model)
		}
	}
	if got := int32(wallabypb.FlowState_FLOW_STATE_STOPPED); got != 6 {
		t.Fatalf("FLOW_STATE_STOPPED = %d, want wire value 6", got)
	}
}
