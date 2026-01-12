package stream

import "github.com/josephjohncox/wallaby/pkg/spec"

func (c *TraceCoverage) merge(other TraceCoverage) {
	if c == nil {
		return
	}
	if c.Actions == nil {
		c.Actions = make(map[spec.Action]int)
	}
	if c.Invariants == nil {
		c.Invariants = make(map[spec.Invariant]int)
	}
	for action, count := range other.Actions {
		c.Actions[action] += count
	}
	for inv, count := range other.Invariants {
		c.Invariants[inv] += count
	}
}
