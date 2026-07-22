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

// ManagedTransactionDeliveryCoordinator is the optional full-transaction
// extension required by the named PostgreSQL profile. Keeping it separate
// preserves compatibility for existing ManagedDeliveryCoordinator adapters.
type ManagedTransactionDeliveryCoordinator interface {
	DeliverTransaction(context.Context, connector.RunFence, connector.DeliveryIntent, connector.SourceTransaction, connector.ManagedTransactionDestination) (connector.AckGrant, error)
}

// ManagedArtifactLog is the deep publication seam used only by
// ack_policy=materialized. Append returns after immutable objects and the
// PostgreSQL publication/checkpoint/ACK intent commit. The production worker
// registers no catalog consumer, so this seam authorizes canonical publication
// only; direct Publisher users may opt into the experimental queue separately.
type ManagedArtifactLog interface {
	Recover(context.Context, connector.RunFence) error
	RestoreCheckpoint(context.Context, connector.RunFence, connector.Checkpoint) (connector.AckGrant, error)
	WaitForReadAdmission(context.Context, connector.RunFence) error
	Append(context.Context, connector.RunFence, connector.SourceTransaction) (connector.AckGrant, error)
}

// ManagedSourceFeedbackCoordinator is the optional observed-flush extension
// required by the named PostgreSQL profile.
type ManagedSourceFeedbackCoordinator interface {
	CommitSourceFeedback(context.Context, connector.RunFence, connector.AckGrant, connector.FlushEvidenceSource) error
}
