package connector

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestRunFenceValidateRejectsDefaultZeroAuthority(t *testing.T) {
	valid := RunFence{
		FlowIncarnationID: uuid.New(), FlowID: "flow", Generation: 1,
		AcquisitionID: uuid.New(), ExecutionID: "execution", LeaseEpoch: 1,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid fence: %v", err)
	}

	tests := []struct {
		name string
		edit func(*RunFence)
		want string
	}{
		{name: "flow", edit: func(f *RunFence) { f.FlowID = "" }, want: "flow_id"},
		{name: "incarnation", edit: func(f *RunFence) { f.FlowIncarnationID = uuid.Nil }, want: "flow_incarnation_id"},
		{name: "generation", edit: func(f *RunFence) { f.Generation = 0 }, want: "generation"},
		{name: "acquisition", edit: func(f *RunFence) { f.AcquisitionID = uuid.Nil }, want: "acquisition_id"},
		{name: "execution", edit: func(f *RunFence) { f.ExecutionID = "" }, want: "execution_id"},
		{name: "epoch", edit: func(f *RunFence) { f.LeaseEpoch = 0 }, want: "lease_epoch"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fence := valid
			tt.edit(&fence)
			if err := fence.Validate(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate error=%v, want %q", err, tt.want)
			}
		})
	}
}
