package bufstream

import (
	"context"

	"github.com/josephjohncox/ductstream/connectors/destinations/kafka"
	"github.com/josephjohncox/ductstream/pkg/connector"
)

// Destination writes batches to a Bufstream deployment using Kafka protocol semantics.
type Destination struct {
	inner kafka.Destination
}

func (d *Destination) Open(ctx context.Context, spec connector.Spec) error {
	return d.inner.Open(ctx, spec)
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) error {
	return d.inner.Write(ctx, batch)
}

func (d *Destination) Close(ctx context.Context) error {
	return d.inner.Close(ctx)
}

func (d *Destination) Capabilities() connector.Capabilities {
	return d.inner.Capabilities()
}
