package main

import (
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestRedpandaEndpointRoundTrip(t *testing.T) {
	t.Parallel()

	wire := endpointTypeToProto("redpanda")
	if wire != wallabypb.EndpointType_ENDPOINT_TYPE_REDPANDA || int32(wire) != 16 {
		t.Fatalf("Redpanda endpoint wire value=%d", wire)
	}
	if model := endpointTypeFromProto(wire); model != connector.EndpointRedpanda {
		t.Fatalf("Redpanda endpoint round trip=%q", model)
	}
}

func TestMaterializedFlowConfigRoundTrip(t *testing.T) {
	input := flowRuntimeConfig{
		AckPolicy: "materialized",
		Materialization: flowMaterializationInfo{
			ProjectionID: artifactlog.ProjectionID,
		},
	}
	pb := flowRuntimeConfigToProto(input)
	if pb == nil || pb.AckPolicy != wallabypb.AckPolicy_ACK_POLICY_MATERIALIZED || pb.Materialization == nil || pb.Materialization.ProjectionId != artifactlog.ProjectionID {
		t.Fatalf("flowRuntimeConfigToProto()=%+v", pb)
	}
	model := flowConfigFromProto(pb)
	if model.AckPolicy != stream.AckPolicyMaterialized || model.Materialization.ProjectionID != artifactlog.ProjectionID {
		t.Fatalf("flowConfigFromProto()=%+v", model)
	}
	detail := flowDetailFromProto(&wallabypb.Flow{Config: pb})
	if detail.Config.AckPolicy != "materialized" || detail.Config.Materialization.ProjectionID != artifactlog.ProjectionID {
		t.Fatalf("flowDetailFromProto().Config=%+v", detail.Config)
	}
}
