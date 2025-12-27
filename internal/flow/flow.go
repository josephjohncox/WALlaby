package flow

import "github.com/josephjohncox/wallaby/pkg/connector"

// State captures lifecycle status for a flow.
type State string

const (
	StateCreated  State = "created"
	StateRunning  State = "running"
	StatePaused   State = "paused"
	StateStopping State = "stopping"
	StateFailed   State = "failed"
)

// Flow defines a CDC pipeline between a source and one or more destinations.
type Flow struct {
	ID           string
	Name         string
	Source       connector.Spec
	Destinations []connector.Spec
	State        State
	WireFormat   connector.WireFormat
	Parallelism  int
}
