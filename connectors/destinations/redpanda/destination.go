package redpanda

import (
	"context"
	"fmt"

	"github.com/josephjohncox/wallaby/connectors/destinations/kafka"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

const (
	CapabilityProfileBase               connector.CapabilityProfileID = "base"
	CapabilityProfileTransactionalOnly  connector.CapabilityProfileID = "transactional-only"
	CapabilityProfileLossyOnly          connector.CapabilityProfileID = "lossy-only"
	CapabilityProfileTransactionalLossy connector.CapabilityProfileID = "transactional+lossy"
)

// Destination writes batches to Redpanda using Kafka protocol semantics.
type Destination struct {
	inner *kafka.Destination
}

func (d *Destination) protocol() *kafka.Destination {
	if d.inner == nil {
		d.inner = &kafka.Destination{}
	}
	return d.inner
}

func (d *Destination) Open(ctx context.Context, spec connector.RuntimeSpec) error {
	return d.protocol().Open(ctx, spec)
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) error {
	return d.protocol().Write(ctx, batch)
}

func (d *Destination) ApplyDDL(ctx context.Context, schema connector.Schema, record connector.Record) error {
	return d.protocol().ApplyDDL(ctx, schema, record)
}

func (d *Destination) TypeMappings() map[string]string {
	return d.protocol().TypeMappings()
}

func (d *Destination) Close(ctx context.Context) error {
	return d.protocol().Close(ctx)
}

func (d *Destination) Capabilities() connector.Capabilities {
	return d.protocol().Capabilities()
}

// CapabilityProfileIDs returns the complete closed Redpanda capability profile set.
func (*Destination) CapabilityProfileIDs() []connector.CapabilityProfileID {
	return []connector.CapabilityProfileID{
		CapabilityProfileBase,
		CapabilityProfileTransactionalOnly,
		CapabilityProfileLossyOnly,
		CapabilityProfileTransactionalLossy,
	}
}

// ClassifyCapabilityProfile maps the Kafka protocol classifier to Redpanda's
// endpoint-scoped typed profile set.
func (d *Destination) ClassifyCapabilityProfile(spec connector.RuntimeSpec) (connector.CapabilityProfileID, error) {
	profile, err := d.protocol().ClassifyCapabilityProfile(spec)
	if err != nil {
		return "", err
	}
	switch profile {
	case kafka.CapabilityProfileBase:
		return CapabilityProfileBase, nil
	case kafka.CapabilityProfileTransactionalOnly:
		return CapabilityProfileTransactionalOnly, nil
	case kafka.CapabilityProfileLossyOnly:
		return CapabilityProfileLossyOnly, nil
	case kafka.CapabilityProfileTransactionalLossy:
		return CapabilityProfileTransactionalLossy, nil
	default:
		return "", fmt.Errorf("unsupported Kafka protocol capability profile %q", profile)
	}
}

func (d *Destination) CapabilitiesFor(spec connector.RuntimeSpec) (connector.Capabilities, error) {
	profile, err := d.ClassifyCapabilityProfile(spec)
	if err != nil {
		return connector.Capabilities{}, err
	}
	capabilities, err := d.protocol().CapabilitiesFor(spec)
	if err != nil {
		return connector.Capabilities{}, err
	}
	switch profile {
	case CapabilityProfileBase, CapabilityProfileTransactionalOnly, CapabilityProfileLossyOnly, CapabilityProfileTransactionalLossy:
		return capabilities, nil
	default:
		return connector.Capabilities{}, fmt.Errorf("unsupported Redpanda capability profile %q", profile)
	}
}
