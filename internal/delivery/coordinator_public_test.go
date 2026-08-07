package delivery_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestDeliverTransactionRejectsUntrustedInputBeforeDestinationIO(t *testing.T) {
	t.Parallel()
	fence, transaction, intent := transactionFixture(t)
	coordinator := &delivery.Coordinator{}

	tests := []struct {
		name string
		edit func(*connector.DeliveryIntent, *connector.SourceTransaction)
		want error
	}{
		{
			name: "stale fence",
			edit: func(intent *connector.DeliveryIntent, _ *connector.SourceTransaction) { intent.LeaseEpoch++ },
			want: authority.ErrFenceRejected,
		},
		{
			name: "changed content",
			edit: func(intent *connector.DeliveryIntent, _ *connector.SourceTransaction) { intent.ContentHash = "changed" },
			want: connector.ErrDeliveryConflict,
		},
		{
			name: "changed position",
			edit: func(intent *connector.DeliveryIntent, _ *connector.SourceTransaction) {
				intent.PositionID = "checkpoint:changed"
			},
			want: connector.ErrDeliveryConflict,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidateIntent := intent
			candidateTransaction := transaction
			test.edit(&candidateIntent, &candidateTransaction)
			destination := &recordingManagedDestination{}
			if _, err := coordinator.DeliverTransaction(context.Background(), fence, candidateIntent, candidateTransaction, connector.ManagedSchemaBaselinePayload{}, destination); !errors.Is(err, test.want) {
				t.Fatalf("DeliverTransaction() error=%v, want %v", err, test.want)
			}
			if destination.externalCalls != 0 {
				t.Fatalf("destination calls=%d, want zero before input is trusted", destination.externalCalls)
			}
		})
	}
}

func TestDeliverTransactionRejectsIdentityMismatchBeforeDestinationIO(t *testing.T) {
	t.Parallel()
	fence, transaction, intent := transactionFixture(t)
	intent.LogicalBatchID = "logical-batch:changed"

	destination := &recordingManagedDestination{}
	if _, err := (&delivery.Coordinator{}).DeliverTransaction(context.Background(), fence, intent, transaction, connector.ManagedSchemaBaselinePayload{}, destination); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("DeliverTransaction() error=%v, want delivery conflict", err)
	}
	if destination.externalCalls != 0 {
		t.Fatalf("destination calls=%d, want zero before identity validation", destination.externalCalls)
	}
}

func transactionFixture(t *testing.T) (authority.RunFence, connector.SourceTransaction, connector.DeliveryIntent) {
	t.Helper()
	fence := authority.RunFence{
		FlowID:            "flow",
		FlowIncarnationID: uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Generation:        1,
		AcquisitionID:     uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		LeaseEpoch:        1,
	}
	batch := connector.Batch{
		Schema:     connector.Schema{Namespace: "public", Name: "events", Version: 1},
		Records:    []connector.Record{{Table: "events", Operation: connector.OpInsert, SchemaVersion: 1, After: map[string]any{"id": int64(1)}}},
		Checkpoint: connector.Checkpoint{LSN: "0/20"},
	}
	transaction := connector.SourceTransaction{
		SourceLineageID: "source/publication-v1", TransactionID: 1,
		BeginLSN: "0/10", CommitLSN: "0/18", EndLSN: batch.Checkpoint.LSN, Checkpoint: batch.Checkpoint,
		Fragments: []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{Schema: batch.Schema, Records: batch.Records}}},
	}
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		t.Fatal(err)
	}
	positionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	return fence, transaction, connector.DeliveryIntent{
		FlowID: fence.FlowID, FlowIncarnationID: fence.FlowIncarnationID.String(), SourceLineageID: "source/publication-v1",
		Generation: fence.Generation, AcquisitionID: fence.AcquisitionID.String(), LeaseEpoch: fence.LeaseEpoch,
		DestinationRevisionID: "destination-v1", LogicalBatchID: logicalBatchID, PositionID: positionID, ContentHash: contentHash,
	}
}

type recordingManagedDestination struct{ externalCalls int }

func (*recordingManagedDestination) Open(context.Context, connector.RuntimeSpec) error { return nil }
func (*recordingManagedDestination) Write(context.Context, connector.Batch) error      { return nil }
func (*recordingManagedDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return nil
}
func (*recordingManagedDestination) TypeMappings() map[string]string { return nil }
func (*recordingManagedDestination) Close(context.Context) error     { return nil }
func (*recordingManagedDestination) Capabilities() connector.Capabilities {
	return connector.Capabilities{}
}
func (*recordingManagedDestination) InitializeManagedDelivery(context.Context) error { return nil }
func (d *recordingManagedDestination) Apply(context.Context, connector.DeliveryIntent, connector.Batch) (connector.DeliveryEvidence, error) {
	d.externalCalls++
	return connector.DeliveryEvidence{}, nil
}
func (d *recordingManagedDestination) Reconcile(context.Context, connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	d.externalCalls++
	return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, nil
}
func (d *recordingManagedDestination) ValidateTransaction(context.Context, connector.SourceTransaction) error {
	d.externalCalls++
	return nil
}
func (d *recordingManagedDestination) ApplyTransaction(context.Context, connector.DeliveryIntent, connector.SourceTransaction) (connector.DeliveryEvidence, error) {
	d.externalCalls++
	return connector.DeliveryEvidence{}, nil
}
