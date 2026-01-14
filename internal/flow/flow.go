package flow

import (
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
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
	AckPolicy                       stream.AckPolicy
	PrimaryDestination              string
	FailureMode                     stream.FailureMode
	GiveUpPolicy                    stream.GiveUpPolicy
	DDL                             DDLPolicy
	SchemaRegistrySubject           string
	SchemaRegistryProtoTypesSubject string
	SchemaRegistrySubjectMode       string
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
	if c.SchemaRegistrySubject != other.SchemaRegistrySubject {
		return false
	}
	if c.SchemaRegistryProtoTypesSubject != other.SchemaRegistryProtoTypesSubject {
		return false
	}
	if c.SchemaRegistrySubjectMode != other.SchemaRegistrySubjectMode {
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

// ApplyRegistryDefaults applies flow-level schema registry defaults to destination specs.
func ApplyRegistryDefaults(specs []connector.Spec, cfg Config) []connector.Spec {
	if cfg.SchemaRegistrySubject == "" && cfg.SchemaRegistryProtoTypesSubject == "" && cfg.SchemaRegistrySubjectMode == "" {
		return specs
	}
	out := make([]connector.Spec, len(specs))
	for i, spec := range specs {
		out[i] = spec
		opts := copyOptions(spec.Options)
		if cfg.SchemaRegistrySubject != "" && opts[schemaregistry.OptRegistrySubject] == "" {
			opts[schemaregistry.OptRegistrySubject] = cfg.SchemaRegistrySubject
		}
		if cfg.SchemaRegistryProtoTypesSubject != "" && opts[schemaregistry.OptRegistryProtoTypes] == "" {
			opts[schemaregistry.OptRegistryProtoTypes] = cfg.SchemaRegistryProtoTypesSubject
		}
		if cfg.SchemaRegistrySubjectMode != "" && opts[schemaregistry.OptRegistrySubjectMode] == "" {
			opts[schemaregistry.OptRegistrySubjectMode] = cfg.SchemaRegistrySubjectMode
		}
		out[i].Options = opts
	}
	return out
}

func copyOptions(in map[string]string) map[string]string {
	if in == nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
