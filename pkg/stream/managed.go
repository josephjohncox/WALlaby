package stream

import (
	"context"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// ManagedDeliveryCoordinator is the public seam used by Runner without
// exposing internal repository implementations in the stable package API.
type ManagedDeliveryCoordinator interface {
	AuthorizeAck(context.Context, connector.RunFence, connector.Checkpoint) (connector.AckGrant, error)
	Deliver(context.Context, connector.RunFence, connector.DeliveryIntent, connector.Batch, connector.ManagedDestination) (connector.AckGrant, error)
	ValidateAckGrant(context.Context, connector.RunFence, connector.AckGrant) error
	RecordAckReceipt(context.Context, connector.RunFence, connector.AckGrant, string) error
}
