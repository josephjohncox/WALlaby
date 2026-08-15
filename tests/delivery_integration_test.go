package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
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
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}), Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{{Name: "target", Type: connector.EndpointPostgres}})}}); err != nil {
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
	failBeforeAckCommit := true
	coordinator, err := delivery.NewCoordinator(ctx, pool, delivery.WithCoordinatorHooks(delivery.CoordinatorHooks{
		BeforeAuthorizeAckCommit: func(context.Context, authority.RunFence, connector.ManagedSchemaBaselinePayload) error {
			if failBeforeAckCommit {
				failBeforeAckCommit = false
				return errors.New("crash before ack authorization commit")
			}
			return nil
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	flowID := fmt.Sprintf("ack-only-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}), Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{{Name: "target", Type: connector.EndpointPostgres}})}}); err != nil {
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
	baseline, err := connector.NewManagedSchemaBaselinePayload("delivery-test-lineage", []connector.Schema{{Namespace: "public", Name: "filtered_events", Version: 2, Columns: []connector.Column{{Name: "id", Type: "bigint"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.AuthorizeAck(ctx, fence, connector.Checkpoint{LSN: "0/A0"}, baseline); err == nil || !strings.Contains(err.Error(), "crash before ack authorization commit") {
		t.Fatalf("injected authorization crash error=%v", err)
	}
	var crashCheckpoints, crashBaselines int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM authoritative_checkpoints WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&crashCheckpoints); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM managed_schema_baselines WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&crashBaselines); err != nil {
		t.Fatal(err)
	}
	if crashCheckpoints != 0 || crashBaselines != 0 {
		t.Fatalf("crash boundary checkpoint/baseline=%d/%d, want old/old", crashCheckpoints, crashBaselines)
	}
	grant, err := coordinator.AuthorizeAck(ctx, fence, connector.Checkpoint{LSN: "0/A0"}, baseline)
	if err != nil {
		t.Fatal(err)
	}
	for label, observed := range map[string]string{
		"empty": "", "malformed": "not-an-lsn", "mismatched": "0/A1",
	} {
		if err := coordinator.RecordAckReceipt(ctx, fence, grant, observed); err == nil {
			t.Fatalf("%s observed flush %q was accepted", label, observed)
		}
	}
	var rejectedReceipts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_ack_receipts WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&rejectedReceipts); err != nil {
		t.Fatal(err)
	}
	if rejectedReceipts != 0 {
		t.Fatalf("invalid observed flush created %d receipt rows", rejectedReceipts)
	}
	equivalentGrant := grant
	equivalentGrant.Checkpoint.LSN = "0/000000A0"
	if err := coordinator.RecordAckReceipt(ctx, fence, equivalentGrant, "0/A0"); err != nil {
		t.Fatal(err)
	}
	var checkpointLSN, observedFlush string
	if err := pool.QueryRow(ctx, `SELECT checkpoint_lsn,observed_flush_lsn FROM source_ack_receipts WHERE flow_incarnation_id=$1 AND position_id=$2`, fence.FlowIncarnationID, grant.PositionID).Scan(&checkpointLSN, &observedFlush); err != nil {
		t.Fatal(err)
	}
	if checkpointLSN != "0/A0" || observedFlush != "0/A0" {
		t.Fatalf("persisted checkpoint/observed flush=%q/%q, want canonical 0/A0 for both", checkpointLSN, observedFlush)
	}
	var intents, receipts int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM source_ack_intents WHERE flow_incarnation_id=$1", fence.FlowIncarnationID).Scan(&intents); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM source_ack_receipts WHERE flow_incarnation_id=$1", fence.FlowIncarnationID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	var committedCheckpoint, committedBaseline int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM authoritative_checkpoints WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&committedCheckpoint); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM managed_schema_baselines WHERE flow_incarnation_id=$1 AND source_relation='filtered_events'`, fence.FlowIncarnationID).Scan(&committedBaseline); err != nil {
		t.Fatal(err)
	}
	if intents != 1 || receipts != 1 || committedCheckpoint != 1 || committedBaseline != 1 {
		t.Fatalf("ack-only intents/receipts/checkpoint/baseline=%d/%d/%d/%d, want 1/1/1/1", intents, receipts, committedCheckpoint, committedBaseline)
	}
	if _, err := pool.Exec(ctx, `UPDATE source_ack_receipts SET checkpoint_lsn='0/B0' WHERE flow_incarnation_id=$1 AND position_id=$2`, fence.FlowIncarnationID, grant.PositionID); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.RecordAckReceipt(ctx, fence, grant, "0/A0"); err == nil || !strings.Contains(err.Error(), "conflicts with the canonical") {
		t.Fatalf("conflicting durable receipt retry error=%v", err)
	}
}

func TestCoordinatorRecoverAbsentManifestFailsClosedWithoutPoisoningDeliver(t *testing.T) {
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
	flowID := fmt.Sprintf("recover-absent-manifest-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}), Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{{Name: "target", Type: connector.EndpointPostgres}})}}); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := authorityStore.AcquireProducer(ctx, flowID, "recover-absent-owner", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	batch := connector.Batch{Schema: testManagedUpsertSchema("events"), Records: []connector.Record{{Table: "events", Operation: connector.OpInsert, SchemaVersion: 1, After: map[string]any{"id": int64(1)}}}, WritePolicy: testUpsertPolicy("id")}
	transaction := managedDeliveryTransaction(batch)
	transaction.Checkpoint = connector.Checkpoint{LSN: "0/A8", Metadata: map[string]string{"payload": "legitimate"}, Timestamp: time.Unix(1_700_000_000, 0).UTC()}
	transaction.EndLSN = transaction.Checkpoint.LSN
	intent := deliveryIntentForFence(t, fence, transaction)
	intent.DestinationRevisionID = fmt.Sprintf("recover-absent-revision-%d", time.Now().UnixNano())
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM destination_revisions WHERE destination_revision_id=$1", intent.DestinationRevisionID)
	}()
	if err := coordinator.RegisterDestinationRevision(ctx, fence, intent.DestinationRevisionID, "scripted", "recover-absent-v1"); err != nil {
		t.Fatal(err)
	}
	poison := connector.Checkpoint{LSN: transaction.Checkpoint.LSN, Metadata: map[string]string{"payload": "poison"}, Timestamp: time.Unix(1_900_000_000, 0).UTC()}
	driver := &timestampReplayDestination{failFirstApply: true}
	baselines := managedBaselinePayload(t, transaction)
	if _, err := coordinator.Recover(ctx, fence, intent, poison, baselines, driver); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("Recover without manifest error=%v, want ErrDeliveryConflict", err)
	}
	var manifests, attempts, receipts, checkpoints, ackIntents int
	if err := pool.QueryRow(ctx, `
SELECT
  (SELECT count(*) FROM delivery_manifests WHERE flow_incarnation_id=$1),
  (SELECT count(*) FROM delivery_attempts WHERE flow_incarnation_id=$1),
  (SELECT count(*) FROM delivery_receipts WHERE flow_incarnation_id=$1),
  (SELECT count(*) FROM authoritative_checkpoints WHERE flow_incarnation_id=$1),
  (SELECT count(*) FROM source_ack_intents WHERE flow_incarnation_id=$1)`, fence.FlowIncarnationID).Scan(&manifests, &attempts, &receipts, &checkpoints, &ackIntents); err != nil {
		t.Fatal(err)
	}
	if manifests != 0 || attempts != 0 || receipts != 0 || checkpoints != 0 || ackIntents != 0 {
		t.Fatalf("failed Recover persisted manifests/attempts/receipts/checkpoints/ACKs=%d/%d/%d/%d/%d, want all zero", manifests, attempts, receipts, checkpoints, ackIntents)
	}
	if _, err := coordinator.DeliverTransaction(ctx, fence, intent, transaction, baselines, driver); !errors.Is(err, errTimestampReplayFirstApply) {
		t.Fatalf("first delivery error=%v, want injected pre-receipt failure", err)
	}
	var metadataJSON []byte
	var storedTimestamp time.Time
	if err := pool.QueryRow(ctx, `
SELECT checkpoint_metadata,checkpoint_timestamp
FROM delivery_manifests
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND position_id=$3`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.PositionID).Scan(&metadataJSON, &storedTimestamp); err != nil {
		t.Fatal(err)
	}
	var metadata map[string]string
	if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["payload"] != "legitimate" || !storedTimestamp.Equal(transaction.Checkpoint.Timestamp) {
		t.Fatalf("persisted manifest metadata/timestamp=%v/%s, want legitimate payload %v/%s", metadata, storedTimestamp, transaction.Checkpoint.Metadata, transaction.Checkpoint.Timestamp)
	}
	replayed := transaction
	replayed.Checkpoint.Timestamp = transaction.Checkpoint.Timestamp.Add(2 * time.Hour)
	replayedGrant, err := coordinator.DeliverTransaction(ctx, fence, intent, replayed, baselines, driver)
	if err != nil {
		t.Fatalf("replay with a different observation timestamp conflicted: %v", err)
	}
	if !replayedGrant.Checkpoint.Timestamp.Equal(transaction.Checkpoint.Timestamp) {
		t.Fatalf("replayed grant timestamp=%s, want first immutable manifest timestamp=%s", replayedGrant.Checkpoint.Timestamp, transaction.Checkpoint.Timestamp)
	}
	if driver.reconcileCalls != 1 {
		t.Fatalf("timestamp replay reconciliation calls=%d, want one not-applied reconciliation", driver.reconcileCalls)
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
	crashBeforeFinalize := false
	coordinator, err := delivery.NewCoordinator(ctx, pool, delivery.WithCoordinatorHooks(delivery.CoordinatorHooks{
		BeforeFinalizeCommit: func(context.Context, authority.RunFence, connector.DeliveryIntent) error {
			if crashBeforeFinalize {
				crashBeforeFinalize = false
				return errors.New("crash before delivery finalization commit")
			}
			return nil
		},
	}))
	if err != nil {
		t.Fatal(err)
	}

	flowID := fmt.Sprintf("delivery-recovery-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}), Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{{Name: "target", Type: connector.EndpointPostgres}})}}); err != nil {
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
	if err := target.Open(ctx, connector.RuntimeSpec{Name: "managed-postgres", Options: map[string]string{
		"dsn": dsn, "schema": "public", "managed_profile": connector.ManagedProfilePostgresToPostgresV1, "batch_mode": "target", "synchronous_commit": "on", "meta_table_enabled": "false",
	}}); err != nil {
		t.Fatal(err)
	}
	defer target.Close(ctx)

	batch := connector.Batch{Schema: testManagedUpsertSchema(tableName), Records: []connector.Record{{Table: tableName, Operation: connector.OpInsert, Key: recordKey(t, map[string]any{"id": 1}), After: map[string]any{"id": 1, "value": "committed"}}}, WritePolicy: testUpsertPolicy("id")}
	transaction := managedDeliveryTransaction(batch)
	transaction.Checkpoint.Metadata = map[string]string{"prepared": "before-target-commit"}
	transaction.Checkpoint.Timestamp = time.Unix(1_700_000_000, 0).UTC()
	oldIntent := deliveryIntentForFence(t, oldFence, transaction)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM destination_revisions WHERE destination_revision_id=$1", oldIntent.DestinationRevisionID)
	}()
	if err := coordinator.RegisterDestinationRevision(ctx, oldFence, oldIntent.DestinationRevisionID, "managed-postgres", "delivery-test-v1"); err != nil {
		t.Fatal(err)
	}
	failing := &commitThenFailDriver{ManagedTransactionDestination: target, fail: true}
	if _, err := coordinator.DeliverTransaction(ctx, oldFence, oldIntent, transaction, managedBaselinePayload(t, transaction), failing); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("first DeliverTransaction error=%v, want indeterminate external commit", err)
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
	swappedBaseline := managedBaselinePayload(t, transaction)
	swappedBaseline.Schemas[0].QuotedIdentifiers = map[string]bool{"value": true}
	if _, err := coordinator.DeliverTransaction(ctx, oldFence, oldIntent, transaction, swappedBaseline, target); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("delivery retry swapped baseline error=%v, want conflict", err)
	}
	conflictingBatch := batch
	conflictingBatch.Records = []connector.Record{{Table: tableName, Operation: connector.OpInsert, Key: recordKey(t, map[string]any{"id": 1}), After: map[string]any{"id": 1, "value": "conflicting"}}}
	conflictingTransaction := managedDeliveryTransaction(conflictingBatch)
	conflictingIntent := deliveryIntentForFence(t, oldFence, conflictingTransaction)
	if _, err := coordinator.DeliverTransaction(ctx, oldFence, conflictingIntent, conflictingTransaction, managedBaselinePayload(t, conflictingTransaction), target); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("retry identity conflict error=%v, want ErrDeliveryConflict", err)
	}
	if _, err := coordinator.Recover(ctx, oldFence, conflictingIntent, conflictingTransaction.Checkpoint, managedBaselinePayload(t, conflictingTransaction), target); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("recovery identity conflict error=%v, want ErrDeliveryConflict", err)
	}

	if _, err := pool.Exec(ctx, `UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE incarnation_id=$1`, oldFence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	newFence, err := authorityStore.AcquireProducer(ctx, flowID, "delivery-new", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	newIntent := deliveryIntentForFence(t, newFence, transaction)
	alteredReplayCheckpoint := transaction.Checkpoint
	alteredReplayCheckpoint.Metadata = map[string]string{"caller": "must-not-be-trusted"}
	alteredReplayCheckpoint.Timestamp = time.Unix(1_900_000_000, 0).UTC()
	grant, err := coordinator.Recover(ctx, newFence, newIntent, alteredReplayCheckpoint, managedBaselinePayload(t, transaction), target)
	if err != nil {
		t.Fatal(err)
	}
	if grant.Checkpoint.LSN != "0/B0" || grant.Checkpoint.Metadata["prepared"] != "before-target-commit" || !grant.Checkpoint.Timestamp.Equal(transaction.Checkpoint.Timestamp) {
		t.Fatalf("AckGrant checkpoint=%+v, want immutable prepared payload %+v", grant.Checkpoint, transaction.Checkpoint)
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

	secondBatch := batch
	secondBatch.Schema.Version = 2
	secondBatch.Schema.QuotedIdentifiers = map[string]bool{"value": true}
	secondBatch.Records = []connector.Record{{Table: tableName, Operation: connector.OpInsert, Key: recordKey(t, map[string]any{"id": 2}), After: map[string]any{"id": 2, "value": "atomic"}}}
	secondTransaction := connector.SourceTransaction{
		SourceLineageID: transaction.SourceLineageID, TransactionID: 2,
		BeginLSN: "0/B1", CommitLSN: "0/B8", EndLSN: "0/C0",
		Fragments: []connector.TransactionFragment{{Ordinal: 0, Batch: secondBatch}}, Checkpoint: connector.Checkpoint{LSN: "0/C0"},
	}
	secondIntent := deliveryIntentForFence(t, newFence, secondTransaction)
	secondBaselines := managedBaselinePayload(t, secondTransaction)
	crashBeforeFinalize = true
	if _, err := coordinator.DeliverTransaction(ctx, newFence, secondIntent, secondTransaction, secondBaselines, target); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("delivery finalization crash error=%v, want indeterminate", err)
	}
	var crashCheckpoint, crashBaselineFingerprint string
	var oldBaselineFingerprint string
	if err := pool.QueryRow(ctx, `SELECT schema_fingerprint FROM managed_schema_baselines WHERE flow_incarnation_id=$1 AND source_relation=$2`, newFence.FlowIncarnationID, tableName).Scan(&oldBaselineFingerprint); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT lsn FROM authoritative_checkpoints WHERE flow_incarnation_id=$1`, newFence.FlowIncarnationID).Scan(&crashCheckpoint); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT schema_fingerprint FROM managed_schema_baselines WHERE flow_incarnation_id=$1 AND source_relation=$2`, newFence.FlowIncarnationID, tableName).Scan(&crashBaselineFingerprint); err != nil {
		t.Fatal(err)
	}
	if crashCheckpoint != "0/B0" || crashBaselineFingerprint != oldBaselineFingerprint {
		t.Fatalf("delivery crash checkpoint/baseline=%s/%s, want old/old 0/B0/%s", crashCheckpoint, crashBaselineFingerprint, oldBaselineFingerprint)
	}
	if _, err := coordinator.DeliverTransaction(ctx, newFence, secondIntent, secondTransaction, secondBaselines, target); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT lsn FROM authoritative_checkpoints WHERE flow_incarnation_id=$1`, newFence.FlowIncarnationID).Scan(&crashCheckpoint); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT schema_fingerprint FROM managed_schema_baselines WHERE flow_incarnation_id=$1 AND source_relation=$2`, newFence.FlowIncarnationID, tableName).Scan(&crashBaselineFingerprint); err != nil {
		t.Fatal(err)
	}
	if crashCheckpoint != "0/C0" || crashBaselineFingerprint == oldBaselineFingerprint {
		t.Fatalf("delivery retry checkpoint/baseline=%s/%s, want new/new with changed baseline", crashCheckpoint, crashBaselineFingerprint)
	}
}

func TestCoordinatorRecoverReturnsPostgresAuthoritativeCheckpointMetadata(t *testing.T) {
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
	checkpointStore, err := checkpoint.NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer checkpointStore.Close()
	flowID := fmt.Sprintf("delivery-authoritative-metadata-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}), Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{{Name: "target", Type: connector.EndpointPostgres}})}}); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := authorityStore.AcquireProducer(ctx, flowID, "metadata-owner", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	metadataBatchA := metadataTransaction("0/C0", 1, "postgres", time.Unix(1_700_000_000, 0).UTC())
	intent := deliveryTransactionIntentForFence(t, fence, "metadata-revision-placeholder", metadataBatchA)
	intent.DestinationRevisionID = fmt.Sprintf("metadata-revision-%d", time.Now().UnixNano())
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM destination_revisions WHERE destination_revision_id=$1", intent.DestinationRevisionID)
	}()
	if err := coordinator.RegisterDestinationRevision(ctx, fence, intent.DestinationRevisionID, "scripted", "metadata-v1"); err != nil {
		t.Fatal(err)
	}
	driver := &metadataRecoveryDestination{}
	baselineA := managedBaselinePayload(t, metadataBatchA)
	grantA, err := coordinator.DeliverTransaction(ctx, fence, intent, metadataBatchA, baselineA, driver)
	if err != nil {
		t.Fatal(err)
	}
	alteredEqual := metadataBatchA.Checkpoint
	alteredEqual.Metadata = map[string]string{"authority": "caller-equal"}
	alteredEqual.Timestamp = time.Unix(1_900_000_000, 0).UTC()
	equalGrant, err := coordinator.AuthorizeAck(ctx, fence, alteredEqual, baselineA)
	if err != nil {
		t.Fatal(err)
	}
	if equalGrant.Checkpoint.Metadata["authority"] != "postgres" || !equalGrant.Checkpoint.Timestamp.Equal(grantA.Checkpoint.Timestamp) {
		t.Fatalf("equal-LSN AuthorizeAck=%+v, want preserved PostgreSQL payload %+v", equalGrant, grantA)
	}
	immediateRecover, err := coordinator.Recover(ctx, fence, intent, alteredEqual, baselineA, driver)
	if err != nil {
		t.Fatal(err)
	}
	if immediateRecover.PositionID != intent.PositionID || immediateRecover.Checkpoint.Metadata["authority"] != "postgres" || !immediateRecover.Checkpoint.Timestamp.Equal(grantA.Checkpoint.Timestamp) {
		t.Fatalf("immediate Recover replay=%+v, want authoritative A grant %+v", immediateRecover, grantA)
	}
	immediate, err := coordinator.DeliverTransaction(ctx, fence, intent, metadataBatchA, baselineA, driver)
	if err != nil {
		t.Fatal(err)
	}
	if immediate.PositionID != intent.PositionID || immediate.Checkpoint.Metadata["authority"] != "postgres" || !immediate.Checkpoint.Timestamp.Equal(grantA.Checkpoint.Timestamp) {
		t.Fatalf("immediate Deliver replay=%+v, want authoritative A grant %+v", immediate, grantA)
	}

	metadataBatchB := metadataTransaction("0/D0", 2, "postgres-b", time.Unix(1_710_000_000, 0).UTC())
	intentB := deliveryTransactionIntentForFence(t, fence, intent.DestinationRevisionID, metadataBatchB)
	baselineB := managedBaselinePayload(t, metadataBatchB)
	grantB, err := coordinator.DeliverTransaction(ctx, fence, intentB, metadataBatchB, baselineB, driver)
	if err != nil {
		t.Fatal(err)
	}
	fencedAlteredB := metadataBatchB.Checkpoint
	fencedAlteredB.Metadata = map[string]string{"authority": "fenced-caller-equal"}
	fencedAlteredB.Timestamp = time.Unix(1_950_000_000, 0).UTC()
	if err := checkpointStore.PutFenced(ctx, fence, fencedAlteredB); err != nil {
		t.Fatal(err)
	}
	storedB, err := checkpointStore.GetFenced(ctx, fence)
	if err != nil {
		t.Fatal(err)
	}
	if storedB.Metadata["authority"] != "postgres-b" || !storedB.Timestamp.Equal(grantB.Checkpoint.Timestamp) {
		t.Fatalf("equal-LSN PutFenced stored=%+v, want preserved PostgreSQL payload %+v", storedB, grantB.Checkpoint)
	}
	callerCheckpoint := metadataBatchA.Checkpoint
	callerCheckpoint.Metadata = map[string]string{"authority": "caller-a"}
	callerCheckpoint.Timestamp = time.Unix(1_800_000_000, 0).UTC()
	recovered, err := coordinator.Recover(ctx, fence, intent, callerCheckpoint, baselineA, driver)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.PositionID != intentB.PositionID || recovered.Checkpoint.LSN != metadataBatchB.Checkpoint.LSN || recovered.Checkpoint.Metadata["authority"] != "postgres-b" || !recovered.Checkpoint.Timestamp.Equal(grantB.Checkpoint.Timestamp) {
		t.Fatalf("Recover(A after B)=%+v, want current PostgreSQL grant B %+v", recovered, grantB)
	}
	delivered, err := coordinator.DeliverTransaction(ctx, fence, intent, metadataBatchA, baselineA, driver)
	if err != nil {
		t.Fatal(err)
	}
	if delivered.PositionID != intentB.PositionID || delivered.Checkpoint.LSN != metadataBatchB.Checkpoint.LSN || delivered.Checkpoint.Metadata["authority"] != "postgres-b" || !delivered.Checkpoint.Timestamp.Equal(grantB.Checkpoint.Timestamp) {
		t.Fatalf("Deliver(A after B)=%+v, want current PostgreSQL grant B %+v", delivered, grantB)
	}
	if err := coordinator.ValidateAckGrant(ctx, fence, recovered); err != nil {
		t.Fatalf("current grant returned by historical delivery replay is unusable: %v", err)
	}

	transactionA := metadataTransaction("0/E0", 11, "postgres-tx-a", time.Unix(1_720_000_000, 0).UTC())
	transactionIntentA := deliveryTransactionIntentForFence(t, fence, intent.DestinationRevisionID, transactionA)
	baselineTransactionA := managedBaselinePayload(t, transactionA)
	grantTransactionA, err := coordinator.DeliverTransaction(ctx, fence, transactionIntentA, transactionA, baselineTransactionA, driver)
	if err != nil {
		t.Fatal(err)
	}
	immediateTransaction, err := coordinator.DeliverTransaction(ctx, fence, transactionIntentA, transactionA, baselineTransactionA, driver)
	if err != nil {
		t.Fatal(err)
	}
	if immediateTransaction.PositionID != transactionIntentA.PositionID || !immediateTransaction.Checkpoint.Timestamp.Equal(grantTransactionA.Checkpoint.Timestamp) {
		t.Fatalf("immediate DeliverTransaction replay=%+v, want authoritative A grant %+v", immediateTransaction, grantTransactionA)
	}
	transactionB := metadataTransaction("0/F0", 12, "postgres-tx-b", time.Unix(1_730_000_000, 0).UTC())
	transactionIntentB := deliveryTransactionIntentForFence(t, fence, intent.DestinationRevisionID, transactionB)
	baselineTransactionB := managedBaselinePayload(t, transactionB)
	grantTransactionB, err := coordinator.DeliverTransaction(ctx, fence, transactionIntentB, transactionB, baselineTransactionB, driver)
	if err != nil {
		t.Fatal(err)
	}
	alteredTransactionA := transactionA
	alteredTransactionA.Checkpoint.Metadata = map[string]string{"authority": "caller-tx-a"}
	alteredTransactionA.Checkpoint.Timestamp = time.Unix(1_990_000_000, 0).UTC()
	replayedTransaction, err := coordinator.DeliverTransaction(ctx, fence, transactionIntentA, alteredTransactionA, baselineTransactionA, driver)
	if err != nil {
		t.Fatal(err)
	}
	if replayedTransaction.PositionID != transactionIntentB.PositionID || replayedTransaction.Checkpoint.LSN != transactionB.Checkpoint.LSN || replayedTransaction.Checkpoint.Metadata["authority"] != "postgres-tx-b" || !replayedTransaction.Checkpoint.Timestamp.Equal(grantTransactionB.Checkpoint.Timestamp) {
		t.Fatalf("DeliverTransaction(A after B)=%+v, want current PostgreSQL grant B %+v", replayedTransaction, grantTransactionB)
	}
	if err := coordinator.ValidateAckGrant(ctx, fence, replayedTransaction); err != nil {
		t.Fatalf("current transaction grant returned by historical replay is unusable: %v", err)
	}

	// A new fence may rebind equal-LSN authority ownership, but it may not
	// replace the immutable metadata/timestamp payload. Exercise every receipt
	// replay entry point under the new acquisition.
	if _, err := pool.Exec(ctx, `UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE incarnation_id=$1`, fence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	newFence, err := authorityStore.AcquireProducer(ctx, flowID, "metadata-owner-new", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	newIntentA := deliveryTransactionIntentForFence(t, newFence, intent.DestinationRevisionID, metadataBatchA)
	newIntentB := deliveryTransactionIntentForFence(t, newFence, intent.DestinationRevisionID, metadataBatchB)
	newTransactionIntentA := deliveryTransactionIntentForFence(t, newFence, intent.DestinationRevisionID, transactionA)
	newTransactionIntentB := deliveryTransactionIntentForFence(t, newFence, intent.DestinationRevisionID, transactionB)
	newFenceRecover, err := coordinator.Recover(ctx, newFence, newIntentA, callerCheckpoint, baselineA, driver)
	if err != nil {
		t.Fatal(err)
	}
	newFenceDeliver, err := coordinator.DeliverTransaction(ctx, newFence, newIntentA, metadataBatchA, baselineA, driver)
	if err != nil {
		t.Fatal(err)
	}
	newFenceImmediate, err := coordinator.DeliverTransaction(ctx, newFence, newIntentB, metadataBatchB, baselineB, driver)
	if err != nil {
		t.Fatal(err)
	}
	newFenceTransaction, err := coordinator.DeliverTransaction(ctx, newFence, newTransactionIntentA, alteredTransactionA, baselineTransactionA, driver)
	if err != nil {
		t.Fatal(err)
	}
	newFenceTransactionImmediate, err := coordinator.DeliverTransaction(ctx, newFence, newTransactionIntentB, transactionB, baselineTransactionB, driver)
	if err != nil {
		t.Fatal(err)
	}
	for name, replay := range map[string]connector.AckGrant{
		"Recover advanced": newFenceRecover, "Deliver advanced": newFenceDeliver,
		"Deliver immediate": newFenceImmediate, "DeliverTransaction advanced altered": newFenceTransaction,
		"DeliverTransaction immediate": newFenceTransactionImmediate,
	} {
		if replay.PositionID != transactionIntentB.PositionID || replay.Checkpoint.Metadata["authority"] != "postgres-tx-b" || !replay.Checkpoint.Timestamp.Equal(grantTransactionB.Checkpoint.Timestamp) {
			t.Fatalf("%s replay=%+v, want immutable current transaction grant %+v", name, replay, grantTransactionB)
		}
		if err := coordinator.ValidateAckGrant(ctx, newFence, replay); err != nil {
			t.Fatalf("%s returned unusable grant: %v", name, err)
		}
	}
	var checkpointGeneration, checkpointLease, ackGeneration, ackLease int64
	var checkpointAcquisition, ackAcquisition uuid.UUID
	if err := pool.QueryRow(ctx, `
SELECT checkpoint.generation,checkpoint.acquisition_id,checkpoint.lease_epoch,
       intent.generation,intent.acquisition_id,intent.lease_epoch
FROM authoritative_checkpoints AS checkpoint
JOIN source_ack_intents AS intent ON intent.flow_incarnation_id=checkpoint.flow_incarnation_id AND intent.checkpoint_lsn=checkpoint.lsn
WHERE checkpoint.flow_incarnation_id=$1 AND intent.position_id=$2`, newFence.FlowIncarnationID, transactionIntentB.PositionID).Scan(
		&checkpointGeneration, &checkpointAcquisition, &checkpointLease,
		&ackGeneration, &ackAcquisition, &ackLease,
	); err != nil {
		t.Fatal(err)
	}
	if checkpointGeneration != newFence.Generation || checkpointAcquisition != newFence.AcquisitionID || checkpointLease != newFence.LeaseEpoch || ackGeneration != newFence.Generation || ackAcquisition != newFence.AcquisitionID || ackLease != newFence.LeaseEpoch {
		t.Fatalf("delivery equal-LSN replay did not rebind checkpoint/ACK ownership to %+v", newFence)
	}

	fencedEqualNewOwner := transactionB.Checkpoint
	fencedEqualNewOwner.Metadata = map[string]string{"authority": "caller-must-not-replace"}
	fencedEqualNewOwner.Timestamp = time.Unix(2_000_000_000, 0).UTC()
	if err := checkpointStore.PutFenced(ctx, newFence, fencedEqualNewOwner); err != nil {
		t.Fatal(err)
	}
	storedNewOwner, err := checkpointStore.GetFenced(ctx, newFence)
	if err != nil {
		t.Fatal(err)
	}
	if storedNewOwner.Metadata["authority"] != "postgres-tx-b" || !storedNewOwner.Timestamp.Equal(grantTransactionB.Checkpoint.Timestamp) {
		t.Fatalf("new-fence equal-LSN PutFenced replaced payload: got %+v want %+v", storedNewOwner, grantTransactionB.Checkpoint)
	}
	if err := pool.QueryRow(ctx, `SELECT generation,acquisition_id,lease_epoch FROM authoritative_checkpoints WHERE flow_incarnation_id=$1`, newFence.FlowIncarnationID).Scan(&checkpointGeneration, &checkpointAcquisition, &checkpointLease); err != nil {
		t.Fatal(err)
	}
	if checkpointGeneration != newFence.Generation || checkpointAcquisition != newFence.AcquisitionID || checkpointLease != newFence.LeaseEpoch {
		t.Fatalf("new-fence equal-LSN PutFenced ownership=%d/%s/%d, want %d/%s/%d", checkpointGeneration, checkpointAcquisition, checkpointLease, newFence.Generation, newFence.AcquisitionID, newFence.LeaseEpoch)
	}

	// PutFenced can advance authority without authorizing source feedback. A
	// historical delivery receipt must not fabricate a position grant for it.
	checkpointWithoutIntent := connector.Checkpoint{LSN: "0/100", Metadata: map[string]string{"authority": "checkpoint-only"}, Timestamp: time.Unix(1_740_000_000, 0).UTC()}
	if err := checkpointStore.PutFenced(ctx, newFence, checkpointWithoutIntent); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Recover(ctx, newFence, newIntentA, callerCheckpoint, baselineA, driver); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("replay after checkpoint-only advancement error=%v, want DeliveryConflict", err)
	}
	if err := coordinator.ValidateAckGrant(ctx, newFence, grantTransactionB); err == nil {
		t.Fatal("prior delivery grant validated after checkpoint-only authority advancement")
	}
	if driver.reconcileCalls != 0 {
		t.Fatalf("receipt replays reconciled external destination %d times, want 0", driver.reconcileCalls)
	}
}

type metadataRecoveryDestination struct {
	reconcileCalls int
}

func (*metadataRecoveryDestination) Open(context.Context, connector.RuntimeSpec) error { return nil }
func (*metadataRecoveryDestination) Write(context.Context, connector.Batch) error      { return nil }
func (*metadataRecoveryDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return nil
}
func (*metadataRecoveryDestination) TypeMappings() map[string]string { return nil }
func (*metadataRecoveryDestination) Close(context.Context) error     { return nil }
func (*metadataRecoveryDestination) Capabilities() connector.Capabilities {
	return connector.Capabilities{}
}
func (*metadataRecoveryDestination) InitializeManagedDelivery(context.Context) error { return nil }
func (*metadataRecoveryDestination) Apply(_ context.Context, intent connector.DeliveryIntent, _ connector.Batch) (connector.DeliveryEvidence, error) {
	return connector.DeliveryEvidence{ExternalID: "scripted:" + intent.PositionID, ContentHash: intent.ContentHash}, nil
}
func (d *metadataRecoveryDestination) Reconcile(context.Context, connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	d.reconcileCalls++
	return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, errors.New("unexpected reconciliation")
}
func (*metadataRecoveryDestination) ValidateTransaction(context.Context, connector.SourceTransaction) error {
	return nil
}
func (*metadataRecoveryDestination) ApplyTransaction(_ context.Context, intent connector.DeliveryIntent, _ connector.SourceTransaction) (connector.DeliveryEvidence, error) {
	return connector.DeliveryEvidence{ExternalID: "scripted-transaction:" + intent.PositionID, ContentHash: intent.ContentHash}, nil
}

var errTimestampReplayFirstApply = errors.New("injected failure after manifest preparation")

type timestampReplayDestination struct {
	metadataRecoveryDestination
	failFirstApply bool
}

func (d *timestampReplayDestination) ApplyTransaction(ctx context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (connector.DeliveryEvidence, error) {
	if d.failFirstApply {
		d.failFirstApply = false
		return connector.DeliveryEvidence{}, errTimestampReplayFirstApply
	}
	return d.metadataRecoveryDestination.ApplyTransaction(ctx, intent, transaction)
}

func (d *timestampReplayDestination) Reconcile(context.Context, connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	d.reconcileCalls++
	return connector.DeliveryNotApplied, connector.DeliveryEvidence{}, nil
}

func metadataTransaction(lsn string, xid uint32, authorityMetadata string, timestamp time.Time) connector.SourceTransaction {
	return connector.SourceTransaction{
		SourceLineageID: "metadata-transaction-lineage", TransactionID: xid,
		BeginLSN: lsn, CommitLSN: lsn, EndLSN: lsn,
		Checkpoint: connector.Checkpoint{LSN: lsn, Metadata: map[string]string{"authority": authorityMetadata}, Timestamp: timestamp},
		Fragments: []connector.TransactionFragment{{
			Ordinal: 0,
			Batch: connector.Batch{
				Schema:  connector.Schema{Namespace: "public", Name: "events", Version: 1},
				Records: []connector.Record{{Table: "events", Operation: connector.OpInsert, SchemaVersion: 1, After: map[string]any{"id": int64(xid)}}},
			},
		}},
	}
}

func deliveryTransactionIntentForFence(t *testing.T, fence authority.RunFence, revisionID string, transaction connector.SourceTransaction) connector.DeliveryIntent {
	t.Helper()
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		t.Fatal(err)
	}
	positionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	return connector.DeliveryIntent{
		FlowID: fence.FlowID, FlowIncarnationID: fence.FlowIncarnationID.String(),
		SourceLineageID: transaction.SourceLineageID, Generation: fence.Generation,
		AcquisitionID: fence.AcquisitionID.String(), LeaseEpoch: fence.LeaseEpoch,
		DestinationRevisionID: revisionID, LogicalBatchID: logicalBatchID,
		PositionID: positionID, ContentHash: contentHash,
	}
}

type commitThenFailDriver struct {
	connector.ManagedTransactionDestination
	fail bool
}

func (d *commitThenFailDriver) ApplyTransaction(ctx context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (connector.DeliveryEvidence, error) {
	evidence, err := d.ManagedTransactionDestination.ApplyTransaction(ctx, intent, transaction)
	if err != nil {
		return evidence, err
	}
	if d.fail {
		d.fail = false
		return evidence, fmt.Errorf("%w: synthetic transport failure after target commit", connector.ErrDeliveryIndeterminate)
	}
	return evidence, nil
}

func managedDeliveryTransaction(batch connector.Batch) connector.SourceTransaction {
	return connector.SourceTransaction{
		SourceLineageID: "source-lineage-1",
		TransactionID:   1,
		BeginLSN:        "0/A0",
		CommitLSN:       "0/A8",
		EndLSN:          "0/B0",
		Fragments:       []connector.TransactionFragment{{Ordinal: 0, Batch: batch}},
		Checkpoint:      connector.Checkpoint{LSN: "0/B0"},
	}
}

func deliveryIntentForFence(t *testing.T, fence authority.RunFence, transaction connector.SourceTransaction) connector.DeliveryIntent {
	t.Helper()
	contentHash, err := connector.SourceTransactionContentHash(transaction)
	if err != nil {
		t.Fatal(err)
	}
	positionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
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
