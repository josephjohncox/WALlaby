package flow

import "testing"

func TestLifecycleTransitions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		from State
		to   State
		want bool
	}{
		{StateCreated, StateRunning, true},
		{StateRunning, StatePaused, true},
		{StatePaused, StateRunning, true},
		{StateRunning, StateStopping, true},
		{StatePaused, StateStopping, true},
		{StateStopping, StateStopped, true},
		{StateRunning, StateFailed, true},
		{StateStopping, StateFailed, true},
		{StateStopped, StateRunning, false},
		{StateFailed, StateRunning, false},
		{StateCreated, StatePaused, false},
	}
	for _, tt := range tests {
		if got := CanTransition(tt.from, tt.to); got != tt.want {
			t.Errorf("CanTransition(%s, %s) = %v, want %v", tt.from, tt.to, got, tt.want)
		}
	}
}
