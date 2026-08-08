package stream

import (
	"context"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

var _ ManagedDeliveryCoordinator = currentManagedCoordinator{}

type currentManagedCoordinator struct{}

func (currentManagedCoordinator) AuthorizeAck(context.Context, connector.RunFence, connector.Checkpoint, connector.ManagedSchemaBaselinePayload) (connector.AckGrant, error) {
	return connector.AckGrant{}, nil
}

func (currentManagedCoordinator) DeliverTransaction(context.Context, connector.RunFence, connector.DeliveryIntent, connector.SourceTransaction, connector.ManagedSchemaBaselinePayload, connector.ManagedTransactionDestination) (connector.AckGrant, error) {
	return connector.AckGrant{}, nil
}

func (currentManagedCoordinator) ValidateAckGrant(context.Context, connector.RunFence, connector.AckGrant) error {
	return nil
}

func (currentManagedCoordinator) RecordAckReceipt(context.Context, connector.RunFence, connector.AckGrant, string) error {
	return nil
}

func (currentManagedCoordinator) CommitSourceFeedback(context.Context, connector.RunFence, connector.AckGrant, connector.FlushEvidenceSource) error {
	return nil
}
