package stream

import (
	"context"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// Compile-time compatibility contract: checkpoint-1 coordinators that
// implement only the original public seam remain valid adapters. Named managed
// profiles require the separate optional transaction and feedback extensions.
var _ ManagedDeliveryCoordinator = legacyManagedCoordinator{}

type legacyManagedCoordinator struct{}

func (legacyManagedCoordinator) AuthorizeAck(context.Context, connector.RunFence, connector.Checkpoint) (connector.AckGrant, error) {
	return connector.AckGrant{}, nil
}

func (legacyManagedCoordinator) Deliver(context.Context, connector.RunFence, connector.DeliveryIntent, connector.Batch, connector.ManagedDestination) (connector.AckGrant, error) {
	return connector.AckGrant{}, nil
}

func (legacyManagedCoordinator) ValidateAckGrant(context.Context, connector.RunFence, connector.AckGrant) error {
	return nil
}

func (legacyManagedCoordinator) RecordAckReceipt(context.Context, connector.RunFence, connector.AckGrant, string) error {
	return nil
}
