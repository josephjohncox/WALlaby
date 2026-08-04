package main

import (
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

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
