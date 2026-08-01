package grpc

import (
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
)

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
