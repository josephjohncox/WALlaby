package flow

import (
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

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
	Config       Config
}

// Config captures flow-level runtime behavior.
type Config struct {
	AckPolicy          stream.AckPolicy
	PrimaryDestination string
	FailureMode        stream.FailureMode
	GiveUpPolicy       stream.GiveUpPolicy
	DDL                DDLPolicy
}

// Equal compares flow configs, including optional DDL policy fields.
func (c Config) Equal(other Config) bool {
	if c.AckPolicy != other.AckPolicy {
		return false
	}
	if c.PrimaryDestination != other.PrimaryDestination {
		return false
	}
	if c.FailureMode != other.FailureMode {
		return false
	}
	if c.GiveUpPolicy != other.GiveUpPolicy {
		return false
	}
	return ddlPolicyEqual(c.DDL, other.DDL)
}

func ddlPolicyEqual(a, b DDLPolicy) bool {
	return boolPtrEqual(a.Gate, b.Gate) &&
		boolPtrEqual(a.AutoApprove, b.AutoApprove) &&
		boolPtrEqual(a.AutoApply, b.AutoApply)
}

func boolPtrEqual(a, b *bool) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// DDLPolicy configures DDL approval behavior.
type DDLPolicy struct {
	Gate        *bool `json:"gate,omitempty"`
	AutoApprove *bool `json:"auto_approve,omitempty"`
	AutoApply   *bool `json:"auto_apply,omitempty"`
}

// DDLPolicyDefaults provide global defaults for DDL policy resolution.
type DDLPolicyDefaults struct {
	Gate        bool
	AutoApprove bool
	AutoApply   bool
}

// Resolve merges defaults with per-flow overrides.
func (p DDLPolicy) Resolve(defaults DDLPolicyDefaults) DDLPolicyDefaults {
	resolved := defaults
	if p.Gate != nil {
		resolved.Gate = *p.Gate
	}
	if p.AutoApprove != nil {
		resolved.AutoApprove = *p.AutoApprove
	}
	if p.AutoApply != nil {
		resolved.AutoApply = *p.AutoApply
	}
	return resolved
}
