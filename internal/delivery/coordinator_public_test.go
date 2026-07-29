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

func TestDeliverRejectsUntrustedInputBeforeDestinationIO(t *testing.T) {
	t.Parallel()
	fence, batch, intent := deliveryFixture(t)
	coordinator := &delivery.Coordinator{}

	tests := []struct {
		name string
		edit func(*connector.DeliveryIntent, *connector.Batch)
		want error
	}{
		{
			name: "stale fence",
			edit: func(intent *connector.DeliveryIntent, _ *connector.Batch) { intent.LeaseEpoch++ },
			want: authority.ErrFenceRejected,
		},
		{
			name: "changed content",
			edit: func(intent *connector.DeliveryIntent, _ *connector.Batch) { intent.ContentHash = "changed" },
			want: connector.ErrDeliveryConflict,
		},
		{
			name: "changed position",
			edit: func(intent *connector.DeliveryIntent, _ *connector.Batch) { intent.PositionID = "checkpoint:changed" },
			want: connector.ErrDeliveryConflict,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidateIntent := intent
			candidateBatch := batch
			test.edit(&candidateIntent, &candidateBatch)
			destination := &recordingManagedDestination{}
			if _, err := coordinator.Deliver(context.Background(), fence, candidateIntent, candidateBatch, destination); !errors.Is(err, test.want) {
				t.Fatalf("Deliver() error=%v, want %v", err, test.want)
			}
			if destination.externalCalls != 0 {
				t.Fatalf("destination calls=%d, want zero before input is trusted", destination.externalCalls)
			}
		})
	}
}

func TestDeliverTransactionRejectsIdentityMismatchBeforeDestinationIO(t *testing.T) {
	t.Parallel()
	fence, batch, intent := deliveryFixture(t)
	transaction := connector.SourceTransaction{
		SourceLineageID: intent.SourceLineageID,
		TransactionID:   1,
		BeginLSN:        "0/10",
		CommitLSN:       "0/18",
		EndLSN:          batch.Checkpoint.LSN,
		Checkpoint:      batch.Checkpoint,
		Fragments: []connector.TransactionFragment{{
			Ordinal: 0,
			Batch:   connector.Batch{Schema: batch.Schema, Records: batch.Records},
		}},
	}
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		t.Fatal(err)
	}
	intent.ContentHash = contentHash
	intent.LogicalBatchID = logicalBatchID
	intent.PositionID, err = connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	intent.LogicalBatchID = "logical-batch:changed"

	destination := &recordingManagedDestination{}
	if _, err := (&delivery.Coordinator{}).DeliverTransaction(context.Background(), fence, intent, transaction, destination); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("DeliverTransaction() error=%v, want delivery conflict", err)
	}
	if destination.externalCalls != 0 {
		t.Fatalf("destination calls=%d, want zero before identity validation", destination.externalCalls)
	}
}

func deliveryFixture(t *testing.T) (authority.RunFence, connector.Batch, connector.DeliveryIntent) {
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
	contentHash, err := connector.BatchContentHash(batch)
	if err != nil {
		t.Fatal(err)
	}
	positionID, err := connector.CheckpointPositionID(batch.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	return fence, batch, connector.DeliveryIntent{
		FlowID: fence.FlowID, FlowIncarnationID: fence.FlowIncarnationID.String(), SourceLineageID: "source/publication-v1",
		Generation: fence.Generation, AcquisitionID: fence.AcquisitionID.String(), LeaseEpoch: fence.LeaseEpoch,
		DestinationRevisionID: "destination-v1", PositionID: positionID, ContentHash: contentHash,
	}
}

type recordingManagedDestination struct{ externalCalls int }

func (*recordingManagedDestination) Open(context.Context, connector.Spec) error   { return nil }
func (*recordingManagedDestination) Write(context.Context, connector.Batch) error { return nil }
func (*recordingManagedDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return nil
}
func (*recordingManagedDestination) TypeMappings() map[string]string { return nil }
func (*recordingManagedDestination) Close(context.Context) error     { return nil }
func (*recordingManagedDestination) Capabilities() connector.Capabilities {
	return connector.Capabilities{}
}
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
