package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestPostgresDestinationRevisionIsImmutable(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	coordinator, err := delivery.NewCoordinator(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	flowID := fmt.Sprintf("destination-revision-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: connector.Spec{Name: "source", Type: connector.EndpointPostgres}, Destinations: []connector.Spec{{Name: "target", Type: connector.EndpointPostgres}}, Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.Spec{{Name: "target", Type: connector.EndpointPostgres}})}}); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := authorityStore.AcquireProducer(ctx, flowID, "revision-owner", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	revisionID := fmt.Sprintf("postgres-revision-%d", time.Now().UnixNano())
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM destination_revisions WHERE destination_revision_id=$1", revisionID)
	}()
	if err := coordinator.RegisterDestinationRevision(ctx, fence, revisionID, "target", "fingerprint-a"); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.RegisterDestinationRevision(ctx, fence, revisionID, "target", "fingerprint-a"); err != nil {
		t.Fatalf("identical revision registration: %v", err)
	}
	if err := coordinator.RegisterDestinationRevision(ctx, fence, revisionID, "target", "fingerprint-b"); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("changed revision error=%v, want ErrDeliveryConflict", err)
	}
}

func TestPostgresAckOnlyCheckpointHasIntentAndReceipt(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	coordinator, err := delivery.NewCoordinator(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	flowID := fmt.Sprintf("ack-only-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: connector.Spec{Name: "source", Type: connector.EndpointPostgres}, Destinations: []connector.Spec{{Name: "target", Type: connector.EndpointPostgres}}, Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.Spec{{Name: "target", Type: connector.EndpointPostgres}})}}); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := authorityStore.AcquireProducer(ctx, flowID, "ack-owner", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := coordinator.AuthorizeAck(ctx, fence, connector.Checkpoint{LSN: "0/A0"})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.RecordAckReceipt(ctx, fence, grant, ""); err != nil {
		t.Fatal(err)
	}
	var intents, receipts int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM source_ack_intents WHERE flow_incarnation_id=$1", fence.FlowIncarnationID).Scan(&intents); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM source_ack_receipts WHERE flow_incarnation_id=$1", fence.FlowIncarnationID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if intents != 1 || receipts != 1 {
		t.Fatalf("ack-only intents=%d receipts=%d, want 1/1", intents, receipts)
	}
}

func TestPostgresCommitBeforeReceiptReconciles(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	checkpointStore, err := checkpoint.NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer checkpointStore.Close()
	coordinator, err := delivery.NewCoordinator(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}

	flowID := fmt.Sprintf("delivery-recovery-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: connector.Spec{Name: "source", Type: connector.EndpointPostgres}, Destinations: []connector.Spec{{Name: "target", Type: connector.EndpointPostgres}}, Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.Spec{{Name: "target", Type: connector.EndpointPostgres}})}}); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	oldFence, err := authorityStore.AcquireProducer(ctx, flowID, "delivery-old", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	const tableName = "wallaby_delivery_recovery_test"
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS public.wallaby_delivery_recovery_test; CREATE TABLE public.wallaby_delivery_recovery_test (id bigint PRIMARY KEY, value text)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS public.wallaby_delivery_recovery_test")
	}()
	target := &pgdest.Destination{}
	if err := target.Open(ctx, connector.Spec{Name: "managed-postgres", Options: map[string]string{
		"dsn": dsn, "schema": "public", "managed_profile": connector.ManagedProfilePostgresToPostgresV1, "batch_mode": "target", "synchronous_commit": "on", "meta_table_enabled": "false",
	}}); err != nil {
		t.Fatal(err)
	}
	defer target.Close(ctx)

	batch := connector.Batch{Schema: testManagedUpsertSchema(tableName), Records: []connector.Record{{Table: tableName, Operation: connector.OpInsert, Key: recordKey(t, map[string]any{"id": 1}), After: map[string]any{"id": 1, "value": "committed"}}}, Checkpoint: connector.Checkpoint{LSN: "0/B0"}, WritePolicy: testUpsertPolicy("id")}
	oldIntent := deliveryIntentForFence(t, oldFence, batch)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM destination_revisions WHERE destination_revision_id=$1", oldIntent.DestinationRevisionID)
	}()
	if err := coordinator.RegisterDestinationRevision(ctx, oldFence, oldIntent.DestinationRevisionID, "managed-postgres", "delivery-test-v1"); err != nil {
		t.Fatal(err)
	}
	failing := &commitThenFailDriver{ManagedDestination: target, fail: true}
	if _, err := coordinator.Deliver(ctx, oldFence, oldIntent, batch, failing); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("first Deliver error=%v, want indeterminate external commit", err)
	}

	var attempts, receipts, ackIntents, checkpoints int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM delivery_attempts WHERE flow_incarnation_id=$1`, oldFence.FlowIncarnationID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM delivery_receipts WHERE flow_incarnation_id=$1`, oldFence.FlowIncarnationID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_ack_intents WHERE flow_incarnation_id=$1`, oldFence.FlowIncarnationID).Scan(&ackIntents); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM authoritative_checkpoints WHERE flow_incarnation_id=$1`, oldFence.FlowIncarnationID).Scan(&checkpoints); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || receipts != 0 || ackIntents != 0 || checkpoints != 0 {
		t.Fatalf("after ambiguous commit attempts=%d receipts=%d ackIntents=%d checkpoints=%d, want 1/0/0/0", attempts, receipts, ackIntents, checkpoints)
	}
	conflictingBatch := batch
	conflictingBatch.Records = []connector.Record{{Table: tableName, Operation: connector.OpInsert, Key: recordKey(t, map[string]any{"id": 1}), After: map[string]any{"id": 1, "value": "conflicting"}}}
	conflictingIntent := deliveryIntentForFence(t, oldFence, conflictingBatch)
	if _, err := coordinator.Deliver(ctx, oldFence, conflictingIntent, conflictingBatch, target); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("retry identity conflict error=%v, want ErrDeliveryConflict", err)
	}
	if _, err := coordinator.Recover(ctx, oldFence, conflictingIntent, conflictingBatch.Checkpoint, target); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("recovery identity conflict error=%v, want ErrDeliveryConflict", err)
	}

	if _, err := pool.Exec(ctx, `UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE incarnation_id=$1`, oldFence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	newFence, err := authorityStore.AcquireProducer(ctx, flowID, "delivery-new", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	newIntent := deliveryIntentForFence(t, newFence, batch)
	grant, err := coordinator.Deliver(ctx, newFence, newIntent, batch, target)
	if err != nil {
		t.Fatal(err)
	}
	if grant.Checkpoint.LSN != "0/B0" {
		t.Fatalf("AckGrant checkpoint=%q, want 0/B0", grant.Checkpoint.LSN)
	}
	if err := coordinator.RecordAckReceipt(ctx, newFence, grant, "0/B0"); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.RecordAckReceipt(ctx, oldFence, grant, "0/B0"); !errors.Is(err, authority.ErrFenceRejected) {
		t.Fatalf("stale ack receipt error=%v, want ErrFenceRejected", err)
	}

	var value string
	if err := pool.QueryRow(ctx, "SELECT value FROM public.wallaby_delivery_recovery_test WHERE id=1").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "committed" {
		t.Fatalf("target value=%q, want committed", value)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM delivery_receipts WHERE flow_incarnation_id=$1`, newFence.FlowIncarnationID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_ack_intents WHERE flow_incarnation_id=$1`, newFence.FlowIncarnationID).Scan(&ackIntents); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM authoritative_checkpoints WHERE flow_incarnation_id=$1 AND acquisition_id=$2`, newFence.FlowIncarnationID, newFence.AcquisitionID).Scan(&checkpoints); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 || ackIntents != 1 || checkpoints != 1 {
		t.Fatalf("after reconciliation receipts=%d ackIntents=%d checkpoints=%d, want 1/1/1", receipts, ackIntents, checkpoints)
	}
}

type commitThenFailDriver struct {
	connector.ManagedDestination
	fail bool
}

func (d *commitThenFailDriver) Apply(ctx context.Context, intent connector.DeliveryIntent, batch connector.Batch) (connector.DeliveryEvidence, error) {
	evidence, err := d.ManagedDestination.Apply(ctx, intent, batch)
	if err != nil {
		return evidence, err
	}
	if d.fail {
		d.fail = false
		return evidence, fmt.Errorf("%w: synthetic transport failure after target commit", connector.ErrDeliveryIndeterminate)
	}
	return evidence, nil
}

func deliveryIntentForFence(t *testing.T, fence authority.RunFence, batch connector.Batch) connector.DeliveryIntent {
	t.Helper()
	contentHash, err := connector.BatchContentHash(batch)
	if err != nil {
		t.Fatal(err)
	}
	positionID, err := connector.CheckpointPositionID(batch.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	logicalBatchID, err := connector.DeliveryLogicalBatchID("source-lineage-1", positionID, contentHash)
	if err != nil {
		t.Fatal(err)
	}
	return connector.DeliveryIntent{
		FlowID:                fence.FlowID,
		FlowIncarnationID:     fence.FlowIncarnationID.String(),
		Generation:            fence.Generation,
		AcquisitionID:         fence.AcquisitionID.String(),
		LeaseEpoch:            fence.LeaseEpoch,
		DestinationRevisionID: "postgres-managed-v1", SourceLineageID: "source-lineage-1", LogicalBatchID: logicalBatchID,
		PositionID:  positionID,
		ContentHash: contentHash,
	}
}
