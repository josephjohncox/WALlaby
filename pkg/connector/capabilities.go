package connector

import "fmt"

// SupportLevel states the operational support commitment for a connector.
type SupportLevel string

const (
	SupportMaintained   SupportLevel = "maintained"
	SupportExperimental SupportLevel = "experimental"
	SupportDeprecated   SupportLevel = "deprecated"
	SupportPlaceholder  SupportLevel = "placeholder"
)

// ContractEvidence records the executable contract suites required before a
// connector can be classified as maintained.
type ContractEvidence struct {
	Restart         bool
	Replay          bool
	SchemaEvolution bool
	Integration     bool
}

// Complete reports whether every maintained-connector gate has evidence.
func (e ContractEvidence) Complete() bool {
	return e.Restart && e.Replay && e.SchemaEvolution && e.Integration
}

// DeliverySemantics describes the guarantees a configured destination provides.
// Declared distinguishes an explicit contract from the zero value used by legacy
// third-party adapters.
type DeliverySemantics struct {
	Declared           bool
	TransactionalBatch bool
	IdempotentReplay   bool
	ReplaySafe         bool
	ExecutesDDL        bool
	Lossy              bool
}

// TableWriteSemantics declares which projected logical table policies a
// destination can execute. Undeclared semantics fail admission for mapped flows.
type TableWriteSemantics struct {
	Declared       bool
	Append         bool
	Upsert         bool
	ExplicitKey    bool
	WatermarkGuard bool
}

// SupportsTablePolicy reports whether the destination can execute policy.
func (c Capabilities) SupportsTablePolicy(policy TableWritePolicy) error {
	w := c.TableWrites
	if !w.Declared {
		return fmt.Errorf("destination does not declare table write semantics")
	}
	switch policy.Mode {
	case ResolvedWriteAppend:
		if !w.Append {
			return fmt.Errorf("destination does not support append table writes")
		}
	case ResolvedWriteUpsert:
		if !w.Upsert || !w.ExplicitKey {
			return fmt.Errorf("destination does not support explicit-key upsert table writes")
		}
	default:
		return fmt.Errorf("unsupported table write mode %q", policy.Mode)
	}
	if policy.Mode == ResolvedWriteUpsert && policy.WatermarkColumn != "" && !w.WatermarkGuard {
		return fmt.Errorf("destination does not support watermark-guarded table writes")
	}
	return nil
}

// ConfiguredDestinationCapabilities allows an adapter to refine guarantees that
// depend on options such as append mode or a lossy oversize policy.
type ConfiguredDestinationCapabilities interface {
	CapabilitiesFor(spec Spec) Capabilities
}

// ResolveDestinationCapabilities returns the guarantees for one configured
// destination, falling back to its static declaration.
func ResolveDestinationCapabilities(destination Destination, spec Spec) Capabilities {
	if configured, ok := destination.(ConfiguredDestinationCapabilities); ok {
		return configured.CapabilitiesFor(spec)
	}
	return destination.Capabilities()
}

// ValidateSupport rejects unsupported levels and maintained classifications
// that lack one or more required executable contract suites.
func (c Capabilities) ValidateSupport() error {
	switch c.Support {
	case SupportMaintained:
		if !c.Evidence.Complete() {
			return fmt.Errorf("maintained connector lacks restart, replay, schema-evolution, or integration evidence")
		}
	case SupportExperimental, SupportDeprecated, SupportPlaceholder:
		return nil
	default:
		return fmt.Errorf("connector support level is not declared")
	}
	return nil
}

// ExecutesDDL reports whether ApplyDDL performs a downstream schema mutation.
// Undeclared legacy adapters retain the historical SupportsDDL behavior.
func (c Capabilities) ExecutesDDL() bool {
	if c.Delivery.Declared {
		return c.Delivery.ExecutesDDL
	}
	return c.SupportsDDL
}
