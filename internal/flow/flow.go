package flow

import (
	"errors"
	"fmt"
	"strings"

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
	StateStopped  State = "stopped"
	StateFailed   State = "failed"
)

// CanTransition reports whether a lifecycle transition is valid. Repeating the
// current state is idempotent; stopped is terminal.
func CanTransition(from, to State) bool {
	if from == to {
		return true
	}
	switch from {
	case StateCreated:
		return to == StateRunning
	case StateRunning:
		return to == StatePaused || to == StateStopping || to == StateFailed
	case StatePaused:
		return to == StateRunning || to == StateStopping || to == StateFailed
	case StateStopping:
		return to == StateStopped || to == StateFailed
	case StateStopped, StateFailed:
		return false
	default:
		return false
	}
}

// ValidState reports whether state is part of the public lifecycle.
func ValidState(state State) bool {
	switch state {
	case StateCreated, StateRunning, StatePaused, StateStopping, StateStopped, StateFailed:
		return true
	default:
		return false
	}
}

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
	AckPolicy                       stream.AckPolicy      `json:"ack_policy,omitempty"`
	PrimaryDestination              string                `json:"primary_destination,omitempty"`
	FailureMode                     stream.FailureMode    `json:"failure_mode,omitempty"`
	GiveUpPolicy                    stream.GiveUpPolicy   `json:"give_up_policy,omitempty"`
	DDL                             DDLPolicy             `json:"ddl,omitempty"`
	SchemaRegistrySubject           string                `json:"schema_registry_subject,omitempty"`
	SchemaRegistryProtoTypesSubject string                `json:"schema_registry_proto_types_subject,omitempty"`
	SchemaRegistrySubjectMode       string                `json:"schema_registry_subject_mode,omitempty"`
	Materialization                 MaterializationPolicy `json:"materialization,omitempty"`
	TableMappings                   TableMappings         `json:"table_mappings"`
}

// MaterializationPolicy selects the frozen canonical projection used by
// ack_policy=materialized. Object-store credentials and operational limits are
// worker deployment configuration, not flow secrets.
type MaterializationPolicy struct {
	ProjectionID string `json:"projection_id,omitempty"`
}

// ValidateDefinition rejects cross-field configurations before they can be
// persisted by any API adapter. Runtime admission repeats capability checks
// after concrete connector construction.
func ValidateDefinition(definition Flow) error {
	if err := definition.Config.TableMappings.Validate(definition.Destinations); err != nil {
		return fmt.Errorf("validate table mappings: %w", err)
	}
	ackPolicy := definition.Config.AckPolicy
	if ackPolicy == "" {
		ackPolicy = stream.AckPolicyAll
	}
	switch ackPolicy {
	case stream.AckPolicyAll, stream.AckPolicyPrimary, stream.AckPolicyMaterialized:
	default:
		return fmt.Errorf("unsupported acknowledgement policy %q", ackPolicy)
	}
	for _, destination := range definition.Destinations {
		if destination.Type == connector.EndpointIceberg {
			if err := connector.ValidatePersistedSpec(destination); err != nil {
				return fmt.Errorf("validate persisted Iceberg destination: %w", err)
			}
		}
		for _, option := range []string{"append_mode", "meta_enabled", "meta_synced_at", "meta_deleted", "meta_watermark", "meta_op", "watermark_source", "soft_delete"} {
			if strings.TrimSpace(destination.Options[option]) != "" {
				return fmt.Errorf("destination %s option %q is superseded by table mappings", destination.Name, option)
			}
		}
		for _, option := range []string{"schema", "table", "database"} {
			if strings.TrimSpace(destination.Options[option]) != "" {
				return fmt.Errorf("destination %s logical option %q is superseded by table mappings", destination.Name, option)
			}
		}
		if strings.TrimSpace(destination.Options["write_mode"]) != "" {
			return fmt.Errorf("destination %s logical option %q is superseded by table mappings", destination.Name, "write_mode")
		}
	}
	materialization := definition.Config.Materialization
	if ackPolicy != stream.AckPolicyMaterialized {
		if materialization != (MaterializationPolicy{}) {
			return errors.New("materialization policy requires ack_policy=materialized")
		}
		return nil
	}
	if materialization.ProjectionID != "canonical_cdc_parquet_v2" {
		return fmt.Errorf("ack_policy=materialized Iceberg requires materialization.projection_id=canonical_cdc_parquet_v2; got %q", materialization.ProjectionID)
	}
	if strings.TrimSpace(definition.Config.PrimaryDestination) != "" {
		return errors.New("primary_destination is not valid with ack_policy=materialized")
	}
	if definition.Source.Type != connector.EndpointPostgres {
		return errors.New("ack_policy=materialized requires a PostgreSQL source")
	}
	if strings.TrimSpace(definition.Source.Options["managed_profile"]) != "" {
		return errors.New("ack_policy=materialized is not admitted by named managed profiles")
	}
	switch strings.ToLower(strings.TrimSpace(definition.Source.Options["managed"])) {
	case "1", "true", "yes", "on":
	default:
		return errors.New("ack_policy=materialized requires managed PostgreSQL transactional execution")
	}
	if !strings.EqualFold(strings.TrimSpace(definition.Source.Options["bootstrap"]), "never") {
		return errors.New("ack_policy=materialized currently requires source.options.bootstrap=never")
	}
	if len(definition.Destinations) != 1 {
		return errors.New("ack_policy=materialized requires exactly one Iceberg destination revision")
	}
	destination := definition.Destinations[0]
	if destination.Type != connector.EndpointIceberg {
		return errors.New("ack_policy=materialized requires an Iceberg destination")
	}
	if strings.TrimSpace(destination.Options["destination_revision_id"]) == "" {
		return errors.New("ack_policy=materialized Iceberg destination requires destination_revision_id")
	}
	for _, option := range []string{"namespace", "table_prefix", "fixed_table", "target_namespace", "target_table"} {
		if strings.TrimSpace(destination.Options[option]) != "" {
			return fmt.Errorf("Iceberg logical option %q is superseded by table mappings", option)
		}
	}
	mapping, ok := definition.Config.TableMappings.ForDestination(destination.Name)
	if !ok {
		return errors.New("materialized Iceberg destination mapping is required")
	}
	validateWrite := func(label string, write TableWritePolicy) error {
		if write.Mode != TableWriteModeAppend {
			return fmt.Errorf("materialized Iceberg %s must use append", label)
		}
		if write.WatermarkColumn != "" {
			return fmt.Errorf("materialized Iceberg %s does not support watermark", label)
		}
		return nil
	}
	if mapping.FutureTables.Action == MappingActionInclude {
		if err := validateWrite("future table mapping", mapping.FutureTables.Write); err != nil {
			return err
		}
	}
	for _, table := range mapping.Tables {
		if table.Action == MappingActionInclude {
			if err := validateWrite(table.SourceSchema+"."+table.SourceTable, table.Write); err != nil {
				return err
			}
		}
	}
	return nil
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
	if c.Materialization != other.Materialization {
		return false
	}
	if !c.TableMappings.Equal(other.TableMappings) {
		return false
	}
	return ddlPolicyEqual(c.DDL, other.DDL)
}

// IsZero reports whether no flow-level behavior was configured.
func (c Config) IsZero() bool {
	return c.AckPolicy == "" && c.PrimaryDestination == "" && c.FailureMode == "" && c.GiveUpPolicy == "" &&
		c.DDL == (DDLPolicy{}) && c.SchemaRegistrySubject == "" && c.SchemaRegistryProtoTypesSubject == "" &&
		c.SchemaRegistrySubjectMode == "" && c.Materialization == (MaterializationPolicy{}) && c.TableMappings.Version == 0 && len(c.TableMappings.Destinations) == 0
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

// ShippedDDLPolicyDefaults is the effective policy when deployment and flow
// configuration both omit DDL settings.
func ShippedDDLPolicyDefaults() DDLPolicyDefaults {
	return DDLPolicyDefaults{Gate: false, AutoApprove: true, AutoApply: true}
}

// ResolveDDLPolicy applies deployment defaults, or the shipped defaults when
// deployment configuration is absent, followed by per-flow overrides.
func ResolveDDLPolicy(policy DDLPolicy, deploymentDefaults *DDLPolicyDefaults) DDLPolicyDefaults {
	defaults := ShippedDDLPolicyDefaults()
	if deploymentDefaults != nil {
		defaults = *deploymentDefaults
	}
	return policy.Resolve(defaults)
}

// Resolve merges explicit defaults with per-flow overrides.
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
