// Package iceberg implements append-only Iceberg changelog consumption from
// Wallaby's PostgreSQL-authoritative canonical artifact log.
package iceberg

import (
	"context"
	"errors"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

var ErrArtifactConsumerOnly = errors.New("iceberg is only available as a canonical artifact-log consumer")

// Destination is a flow configuration marker. Catalog I/O is constructed by
// the artifact runtime and never runs through Destination.Write.
type Destination struct{}

func (*Destination) Open(_ context.Context, spec connector.Spec) error {
	_, err := ParseSpec(spec, Config{})
	return err
}

func (*Destination) Write(context.Context, connector.Batch) error { return ErrArtifactConsumerOnly }

func (*Destination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return ErrArtifactConsumerOnly
}

func (*Destination) TypeMappings() map[string]string { return map[string]string{} }
func (*Destination) Close(context.Context) error     { return nil }
func (*Destination) CanonicalArtifactConsumer()      {}

func (*Destination) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		Support:  connector.SupportExperimental,
		Evidence: connector.ContractEvidence{},
		Delivery: connector.DeliverySemantics{
			Declared: true, IdempotentReplay: true, ReplaySafe: true,
		},
		SupportsStreaming:     true,
		SupportsSchemaChanges: true,
		SupportedWireFormats:  []connector.WireFormat{connector.WireFormatParquet},
	}
}
