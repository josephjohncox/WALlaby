package bootstrap_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestDeliverTaskBatchRejectsInvalidAuthorityBeforeDestinationIO(t *testing.T) {
	t.Parallel()
	bootstrapID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	schema := connector.Schema{Namespace: "public", Name: "events", Version: 1, Columns: []connector.Column{{Name: "id", Type: "bigint"}}}
	policy := connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend, ProjectionFingerprint: "projection-v1"}
	task := bootstrap.SnapshotTask{
		RelationID: 42, TaskID: "range-0", Namespace: "public", Table: "events", Schema: schema, KeyColumns: []string{"id"},
		Delivery: bootstrap.SnapshotDeliveryContract{Version: bootstrap.SnapshotDeliveryContractV1, Schema: schema, WritePolicy: policy, ProjectionFingerprint: "projection-v1"},
	}
	claim := authority.ClaimFence{
		RunFence: authority.RunFence{
			FlowID: "flow", FlowIncarnationID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
			Generation: 1, AcquisitionID: uuid.MustParse("33333333-3333-3333-3333-333333333333"), ExecutionID: "execution", LeaseEpoch: 1,
		},
		Kind: authority.ClaimSnapshot, WorkID: task.WorkID(bootstrapID), ClaimEpoch: 1,
	}
	snapshot := bootstrap.ExportedSnapshot{BootstrapID: bootstrapID, BootstrapGeneration: 1, SourceLineageID: "source/publication-v1", PublicationRevision: "publication-v1", ManifestHash: "manifest"}
	batch := connector.Batch{
		Schema: schema, WritePolicy: policy,
		Records: []connector.Record{{Table: "events", Operation: connector.OpInsert, SchemaVersion: 1}},
	}

	tests := []struct {
		name    string
		claim   authority.ClaimFence
		ordinal int64
		want    string
	}{
		{name: "non-positive ordinal", claim: claim, ordinal: 0, want: "positive bootstrap batch ordinal"},
		{name: "wrong claim kind", claim: func() authority.ClaimFence {
			candidate := claim
			candidate.Kind = authority.ClaimDelivery
			return candidate
		}(), ordinal: 1, want: "claim does not match"},
		{name: "wrong work identity", claim: func() authority.ClaimFence { candidate := claim; candidate.WorkID = "another-task"; return candidate }(), ordinal: 1, want: "claim does not match"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			destination := &recordingBootstrapDestination{}
			err := (&bootstrap.Bootstrapper{}).DeliverTaskBatch(context.Background(), test.claim, snapshot, task, test.ordinal, nil, false, "destination-v1", batch, destination)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("DeliverTaskBatch() error=%v, want substring %q", err, test.want)
			}
			if destination.externalCalls != 0 {
				t.Fatalf("destination calls=%d, want zero before authority validation", destination.externalCalls)
			}
		})
	}
}

func TestImportSnapshotRejectsMissingSessionBeforeDatabaseAccess(t *testing.T) {
	t.Parallel()
	if _, err := (&bootstrap.Bootstrapper{}).ImportSnapshot(context.Background(), authority.RunFence{}, nil); err == nil || !strings.Contains(err.Error(), "connection is not alive") {
		t.Fatalf("ImportSnapshot(nil) error=%v, want dead exporter rejection", err)
	}
}

type recordingBootstrapDestination struct{ externalCalls int }

func (*recordingBootstrapDestination) Open(context.Context, connector.RuntimeSpec) error { return nil }
func (*recordingBootstrapDestination) Write(context.Context, connector.Batch) error      { return nil }
func (*recordingBootstrapDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return nil
}
func (*recordingBootstrapDestination) TypeMappings() map[string]string { return nil }
func (*recordingBootstrapDestination) Close(context.Context) error     { return nil }
func (*recordingBootstrapDestination) Capabilities() connector.Capabilities {
	return connector.Capabilities{}
}
func (d *recordingBootstrapDestination) Apply(context.Context, connector.DeliveryIntent, connector.Batch) (connector.DeliveryEvidence, error) {
	d.externalCalls++
	return connector.DeliveryEvidence{}, nil
}
func (d *recordingBootstrapDestination) Reconcile(context.Context, connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	d.externalCalls++
	return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, nil
}
func (d *recordingBootstrapDestination) PrepareBootstrap(context.Context, connector.BootstrapIntent, []connector.BootstrapTable) error {
	d.externalCalls++
	return nil
}
func (d *recordingBootstrapDestination) ApplyBootstrap(context.Context, connector.BootstrapIntent, connector.DeliveryIntent, connector.Batch) (connector.DeliveryEvidence, error) {
	d.externalCalls++
	return connector.DeliveryEvidence{}, nil
}
func (d *recordingBootstrapDestination) ReconcileBootstrap(context.Context, connector.BootstrapIntent, connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	d.externalCalls++
	return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, nil
}
func (d *recordingBootstrapDestination) PublishBootstrap(context.Context, connector.BootstrapIntent, []connector.BootstrapTable) (connector.DeliveryEvidence, error) {
	d.externalCalls++
	return connector.DeliveryEvidence{}, nil
}
func (d *recordingBootstrapDestination) AbandonBootstrap(context.Context, connector.BootstrapIntent, []connector.BootstrapTable) error {
	d.externalCalls++
	return nil
}
