package stream

import (
	"context"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// ManagedDeliveryCoordinator is the complete fenced full-transaction seam used
// by Runner without exposing internal repository implementations.
type ManagedDeliveryCoordinator interface {
	AuthorizeAck(context.Context, connector.RunFence, connector.Checkpoint, connector.ManagedSchemaBaselinePayload) (connector.AckGrant, error)
	DeliverTransaction(context.Context, connector.RunFence, connector.DeliveryIntent, connector.SourceTransaction, connector.ManagedSchemaBaselinePayload, connector.ManagedTransactionDestination) (connector.AckGrant, error)
	ValidateAckGrant(context.Context, connector.RunFence, connector.AckGrant) error
	RecordAckReceipt(context.Context, connector.RunFence, connector.AckGrant, string) error
	CommitSourceFeedback(context.Context, connector.RunFence, connector.AckGrant, connector.FlushEvidenceSource) error
}

// ManagedArtifactLog is the deep publication seam used only by
// ack_policy=materialized. Append returns after immutable objects and the
// PostgreSQL publication/checkpoint/ACK intent commit. Catalog consumers, when
// configured, run asynchronously behind PostgreSQL publication authority and do
// not extend the source-ACK boundary.
type ManagedArtifactLog interface {
	Recover(context.Context, connector.RunFence) error
	RestoreCheckpoint(context.Context, connector.RunFence, connector.Checkpoint) (connector.AckGrant, error)
	WaitForReadAdmission(context.Context, connector.RunFence) error
	Append(context.Context, connector.RunFence, connector.SourceTransaction, connector.ManagedSchemaBaselinePayload) (connector.AckGrant, error)
}

// ManagedArtifactIdentity exposes the non-secret effective destination identity
// after deployment defaults are merged. FlowRunner pins it to the PostgreSQL
// destination revision before catalog recovery or consumption.
type ManagedArtifactIdentity interface {
	EffectiveDestinationFingerprint() string
}
