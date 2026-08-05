package connector

import "fmt"

// SupportLevel states the operational support commitment for a connector.
type SupportLevel string

const (
	SupportMaintained   SupportLevel = "maintained"
	SupportExperimental SupportLevel = "experimental"
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
type DeliverySemantics struct {
	TransactionalBatch bool
	IdempotentReplay   bool
	ReplaySafe         bool
	ExecutesDDL        bool
	Lossy              bool
}

// TableWriteSemantics declares which projected logical table policies a
// destination can execute.
type TableWriteSemantics struct {
	Append         bool
	Upsert         bool
	ExplicitKey    bool
	WatermarkGuard bool
}

// SupportsTablePolicy reports whether the destination can execute policy.
func (c Capabilities) SupportsTablePolicy(policy TableWritePolicy) error {
	w := c.TableWrites
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

// CapabilityProfileID identifies one closed configuration-sensitive capability cell.
type CapabilityProfileID string

// ConfiguredDestinationCapabilities classifies every capability-affecting
// configuration into a closed, typed profile before returning its guarantees.
type ConfiguredDestinationCapabilities interface {
	CapabilityProfileIDs() []CapabilityProfileID
	ClassifyCapabilityProfile(spec Spec) (CapabilityProfileID, error)
	CapabilitiesFor(spec Spec) (Capabilities, error)
}

// ResolveDestinationCapabilities returns the guarantees for one validated
// configured destination, falling back to its static declaration. A classifier
// result that is not in the connector's declared closed profile set fails.
func ResolveDestinationCapabilities(destination Destination, spec Spec) (Capabilities, error) {
	configured, ok := destination.(ConfiguredDestinationCapabilities)
	if !ok {
		return destination.Capabilities(), nil
	}
	profileID, err := configured.ClassifyCapabilityProfile(spec)
	if err != nil {
		return Capabilities{}, err
	}
	declared := false
	for _, candidate := range configured.CapabilityProfileIDs() {
		if candidate == profileID {
			declared = true
			break
		}
	}
	if !declared {
		return Capabilities{}, fmt.Errorf("destination capability classifier returned undeclared profile %q", profileID)
	}
	return configured.CapabilitiesFor(spec)
}

// ValidateSupport rejects unsupported levels and maintained classifications
// that lack one or more required executable contract suites.
func (c Capabilities) ValidateSupport() error {
	switch c.Support {
	case SupportMaintained:
		if !c.Evidence.Complete() {
			return fmt.Errorf("maintained connector lacks restart, replay, schema-evolution, or integration evidence")
		}
	case SupportExperimental, SupportPlaceholder:
		return nil
	default:
		return fmt.Errorf("connector support level is not declared")
	}
	return nil
}

// ExecutesDDL reports whether ApplyDDL performs a downstream schema mutation.
func (c Capabilities) ExecutesDDL() bool {
	return c.Delivery.ExecutesDDL
}
