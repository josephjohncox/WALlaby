package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestPostgresManagedDeliveryRetryAndRetention(t *testing.T) {
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
	var blockRetention atomic.Bool
	var failAfterTargetApply atomic.Bool
	var cancelAfterTargetApply context.CancelFunc
	retentionEntered := make(chan struct{}, 1)
	retentionRelease := make(chan struct{})
	coordinator, err := delivery.NewCoordinator(ctx, pool, delivery.WithCoordinatorHooks(delivery.CoordinatorHooks{
		AfterTargetApply: func(context.Context, authority.RunFence, connector.DeliveryIntent) error {
			if failAfterTargetApply.CompareAndSwap(true, false) && cancelAfterTargetApply != nil {
				cancelAfterTargetApply()
			}
			return nil
		},
		AfterRetentionRootLock: func(hookCtx context.Context, _ authority.RunFence, _ string) error {
			if !blockRetention.Load() {
				return nil
			}
			select {
			case retentionEntered <- struct{}{}:
			default:
			}
			select {
			case <-retentionRelease:
				return nil
			case <-hookCtx.Done():
				return hookCtx.Err()
			}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	flowID := fmt.Sprintf("delivery-retention-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID}); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := authorityStore.AcquireProducer(ctx, flowID, "retry-retention", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	const table = "wallaby_delivery_retention"
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS public.wallaby_delivery_retention; CREATE TABLE public.wallaby_delivery_retention (id bigint PRIMARY KEY,value text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS public.wallaby_delivery_retention`)
	}()
	target := &pgdest.Destination{}
	if err := target.Open(ctx, connector.Spec{Name: "retry-retention", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": dsn, "write_mode": "target", "batch_mode": "target", "meta_table_enabled": "false", "synchronous_commit": "on",
	}}); err != nil {
		t.Fatal(err)
	}
	defer target.Close(ctx)
	revisionID := "retry-retention-revision"
	defer func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM destination_revisions WHERE destination_revision_id=$1`, revisionID)
	}()
	if err := coordinator.RegisterDestinationRevision(ctx, fence, revisionID, "retry-retention", "profile-v1"); err != nil {
		t.Fatal(err)
	}

	first := retentionTransaction(table, 3001, "0/500", 1)
	firstIntent := transactionIntentForFence(t, fence, revisionID, first)
	if _, err := pool.Exec(ctx, `
INSERT INTO delivery_manifests (
  flow_incarnation_id,destination_revision_id,source_lineage_id,position_id,
  source_transaction_id,content_hash,checkpoint_lsn
) VALUES ($1,$2,$3,$4,$5,$6,$7)`, fence.FlowIncarnationID, revisionID, firstIntent.SourceLineageID, firstIntent.PositionID, firstIntent.SourceLineageID+":"+first.EndLSN, firstIntent.ContentHash, first.EndLSN); err != nil {
		t.Fatalf("checkpoint-1 control writer rejected after additive upgrade: %v", err)
	}
	failing := &failFirstTransactionDriver{ManagedTransactionDestination: target, fail: true}
	if _, err := coordinator.DeliverTransaction(ctx, fence, firstIntent, first, failing); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("checkpoint-1 manifest without immutable payload error=%v, want ErrDeliveryConflict", err)
	}
	// The additive migration accepts an old writer's row, but recovery cannot
	// reconstruct absent historical checkpoint metadata from replay input. Remove
	// the deliberately indeterminate fixture before testing current-writer retry.
	if _, err := pool.Exec(ctx, `
DELETE FROM delivery_manifests
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND position_id=$3`, fence.FlowIncarnationID, revisionID, firstIntent.PositionID); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.DeliverTransaction(ctx, fence, firstIntent, first, failing); err == nil || errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("first deterministic failure=%v", err)
	}
	grant, err := coordinator.DeliverTransaction(ctx, fence, firstIntent, first, failing)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.CommitSourceFeedback(ctx, fence, grant, &flushEvidenceTestSource{}); err != nil {
		t.Fatal(err)
	}
	var adoptedLogicalBatchID string
	if err := pool.QueryRow(ctx, `
SELECT logical_batch_id FROM delivery_manifests
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND position_id=$3`, fence.FlowIncarnationID, revisionID, firstIntent.PositionID).Scan(&adoptedLogicalBatchID); err != nil {
		t.Fatal(err)
	}
	if adoptedLogicalBatchID != firstIntent.LogicalBatchID {
		t.Fatalf("adopted checkpoint-1 control logical batch=%q, want %q", adoptedLogicalBatchID, firstIntent.LogicalBatchID)
	}
	if _, err := pool.Exec(ctx, `
UPDATE delivery_receipts SET logical_batch_id=NULL
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND position_id=$3`, fence.FlowIncarnationID, revisionID, firstIntent.PositionID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE delivery_attempts SET logical_batch_id=NULL
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND position_id=$3`, fence.FlowIncarnationID, revisionID, firstIntent.PositionID); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.DeliverTransaction(ctx, fence, firstIntent, first, target); err != nil {
		t.Fatalf("adopt checkpoint-1 control receipt after upgrade: %v", err)
	}
	var adoptedReceiptID, adoptedAttemptID string
	if err := pool.QueryRow(ctx, `
SELECT receipt.logical_batch_id,attempt.logical_batch_id
FROM delivery_receipts AS receipt
JOIN delivery_attempts AS attempt ON attempt.attempt_id=receipt.attempt_id
WHERE receipt.flow_incarnation_id=$1 AND receipt.destination_revision_id=$2 AND receipt.position_id=$3`, fence.FlowIncarnationID, revisionID, firstIntent.PositionID).Scan(&adoptedReceiptID, &adoptedAttemptID); err != nil {
		t.Fatal(err)
	}
	if adoptedReceiptID != firstIntent.LogicalBatchID || adoptedAttemptID != firstIntent.LogicalBatchID {
		t.Fatalf("adopted checkpoint-1 receipt/attempt=%q/%q, want %q", adoptedReceiptID, adoptedAttemptID, firstIntent.LogicalBatchID)
	}

	postCommit := retentionTransaction(table, 3058, "0/580", 20)
	postCommitIntent := transactionIntentForFence(t, fence, revisionID, postCommit)
	postCommitCtx, cancelPostCommit := context.WithCancel(ctx)
	cancelAfterTargetApply = cancelPostCommit
	failAfterTargetApply.Store(true)
	if _, err := coordinator.DeliverTransaction(postCommitCtx, fence, postCommitIntent, postCommit, target); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		cancelPostCommit()
		t.Fatalf("post-target control failure=%v, want recoverable indeterminate classification", err)
	}
	cancelPostCommit()
	cancelAfterTargetApply = nil
	if failAfterTargetApply.Load() {
		t.Fatal("post-target cancellation hook was not reached")
	}
	if _, err := pool.Exec(ctx, `SELECT 1`); err != nil {
		t.Fatalf("control store did not recover after canceled post-target transaction: %v", err)
	}
	if _, err := coordinator.DeliverTransaction(postCommitCtx, fence, postCommitIntent, postCommit, target); err == nil {
		t.Fatal("canceled delivery context unexpectedly remained usable")
	}
	currentFlow, err := engine.Get(ctx, flowID)
	if err != nil {
		t.Fatal(err)
	}
	if currentFlow.State != flow.StateRunning {
		t.Fatalf("flow state after post-target control failure=%s, want running", currentFlow.State)
	}
	postCommitGrant, err := coordinator.DeliverTransaction(ctx, fence, postCommitIntent, postCommit, target)
	if err != nil {
		t.Fatalf("reconcile post-target control failure: %v", err)
	}
	if err := coordinator.CommitSourceFeedback(ctx, fence, postCommitGrant, &flushEvidenceTestSource{}); err != nil {
		t.Fatal(err)
	}
	var postCommitRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.wallaby_delivery_retention WHERE id=20 AND value='value-20'`).Scan(&postCommitRows); err != nil {
		t.Fatal(err)
	}
	if postCommitRows != 1 {
		t.Fatalf("post-target recovered rows=%d, want exactly one", postCommitRows)
	}

	second := retentionTransaction(table, 3002, "0/600", 2)
	secondIntent := transactionIntentForFence(t, fence, revisionID, second)
	secondGrant, err := coordinator.DeliverTransaction(ctx, fence, secondIntent, second, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.CommitSourceFeedback(ctx, fence, secondGrant, &flushEvidenceTestSource{}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE delivery_manifests SET created_at=clock_timestamp()-interval '2 hours' WHERE flow_incarnation_id=$1 AND logical_batch_id=$2`, fence.FlowIncarnationID, firstIntent.LogicalBatchID); err != nil {
		t.Fatal(err)
	}
	pruned, err := coordinator.PruneTerminalDeliveryState(ctx, fence, time.Hour, 10)
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 1 {
		t.Fatalf("pruned manifests=%d, want one terminal logical batch", pruned)
	}
	var oldCount, currentCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM delivery_manifests WHERE flow_incarnation_id=$1 AND logical_batch_id=$2`, fence.FlowIncarnationID, firstIntent.LogicalBatchID).Scan(&oldCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM delivery_manifests WHERE flow_incarnation_id=$1 AND logical_batch_id=$2`, fence.FlowIncarnationID, secondIntent.LogicalBatchID).Scan(&currentCount); err != nil {
		t.Fatal(err)
	}
	if oldCount != 0 || currentCount != 1 {
		t.Fatalf("retained logical batches old/current=%d/%d, want 0/1", oldCount, currentCount)
	}
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM delivery_attempts WHERE flow_incarnation_id=$1 AND logical_batch_id=$2`, fence.FlowIncarnationID, firstIntent.LogicalBatchID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 {
		t.Fatalf("pruned logical batch attempts=%d, want zero", attempts)
	}

	third := retentionTransaction(table, 3003, "0/700", 3)
	thirdIntent := transactionIntentForFence(t, fence, revisionID, third)
	indeterminate := &reconciliationFailureDriver{ManagedTransactionDestination: target}
	if _, err := coordinator.DeliverTransaction(ctx, fence, thirdIntent, third, indeterminate); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("prepare indeterminate attempt error=%v, want ErrDeliveryIndeterminate", err)
	}
	for attempt := 1; attempt <= 16; attempt++ {
		if _, err := pool.Exec(ctx, `UPDATE delivery_attempts SET next_attempt_at=clock_timestamp() WHERE flow_incarnation_id=$1 AND logical_batch_id=$2`, fence.FlowIncarnationID, thirdIntent.LogicalBatchID); err != nil {
			t.Fatal(err)
		}
		_, err := coordinator.DeliverTransaction(ctx, fence, thirdIntent, third, indeterminate)
		if attempt < 16 {
			if !errors.Is(err, connector.ErrDeliveryIndeterminate) || errors.Is(err, connector.ErrDeliveryRetryExhausted) {
				t.Fatalf("reconciliation attempt %d error=%v, want recoverable indeterminate", attempt, err)
			}
			continue
		}
		if !errors.Is(err, connector.ErrDeliveryRetryExhausted) {
			t.Fatalf("reconciliation attempt %d error=%v, want bounded exhaustion", attempt, err)
		}
	}
	var reconciliationAttempts int
	var lastError string
	if err := pool.QueryRow(ctx, `SELECT reconciliation_attempts,last_error FROM delivery_attempts WHERE flow_incarnation_id=$1 AND logical_batch_id=$2`, fence.FlowIncarnationID, thirdIntent.LogicalBatchID).Scan(&reconciliationAttempts, &lastError); err != nil {
		t.Fatal(err)
	}
	if reconciliationAttempts != 16 || lastError != "deterministic reconciliation transport failure" {
		t.Fatalf("persisted reconciliation attempts/error=%d/%q, want 16/deterministic failure", reconciliationAttempts, lastError)
	}

	observedPositions := make([]string, 0, 6)
	for _, lsn := range []string{"0/800", "0/900", "0/A00", "0/B00", "0/C00"} {
		checkpoint := connector.Checkpoint{LSN: lsn}
		grant, err := coordinator.AuthorizeAck(ctx, fence, checkpoint)
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.CommitSourceFeedback(ctx, fence, grant, &flushEvidenceTestSource{}); err != nil {
			t.Fatal(err)
		}
		observedPositions = append(observedPositions, grant.PositionID)
	}
	unobservedGrant, err := coordinator.AuthorizeAck(ctx, fence, connector.Checkpoint{LSN: "0/D00"})
	if err != nil {
		t.Fatal(err)
	}
	currentGrant, err := coordinator.AuthorizeAck(ctx, fence, connector.Checkpoint{LSN: "0/E00"})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.CommitSourceFeedback(ctx, fence, currentGrant, &flushEvidenceTestSource{}); err != nil {
		t.Fatal(err)
	}
	observedPositions = append(observedPositions, currentGrant.PositionID)
	allPositions := append(append([]string{}, observedPositions...), unobservedGrant.PositionID)
	// Give intents and receipts opposite age order so bounded pruning must use
	// one shared candidate set rather than independently orphaning each side.
	if _, err := pool.Exec(ctx, `
UPDATE source_ack_intents AS intent
SET authorized_at=clock_timestamp()-interval '3 hours'+positions.ordinality*interval '1 minute'
FROM unnest($2::text[]) WITH ORDINALITY AS positions(position_id,ordinality)
WHERE intent.flow_incarnation_id=$1 AND intent.position_id=positions.position_id`, fence.FlowIncarnationID, allPositions); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE source_ack_receipts AS receipt
SET recorded_at=clock_timestamp()-interval '3 hours'+(100-positions.ordinality)*interval '1 minute'
FROM unnest($2::text[]) WITH ORDINALITY AS positions(position_id,ordinality)
WHERE receipt.flow_incarnation_id=$1 AND receipt.position_id=positions.position_id`, fence.FlowIncarnationID, observedPositions); err != nil {
		t.Fatal(err)
	}
	feedbackPruned, err := coordinator.PruneTerminalDeliveryState(ctx, fence, time.Hour, 2)
	if err != nil {
		t.Fatal(err)
	}
	if feedbackPruned != 4 {
		t.Fatalf("bounded feedback rows pruned=%d, want two intents plus two receipts", feedbackPruned)
	}
	var retainedObservedReceipts, retainedObservedIntents, retainedUnobservedIntent int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_ack_receipts WHERE flow_incarnation_id=$1 AND position_id=ANY($2)`, fence.FlowIncarnationID, observedPositions).Scan(&retainedObservedReceipts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_ack_intents WHERE flow_incarnation_id=$1 AND position_id=ANY($2)`, fence.FlowIncarnationID, observedPositions).Scan(&retainedObservedIntents); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_ack_intents WHERE flow_incarnation_id=$1 AND position_id=$2`, fence.FlowIncarnationID, unobservedGrant.PositionID).Scan(&retainedUnobservedIntent); err != nil {
		t.Fatal(err)
	}
	if retainedObservedReceipts != 4 || retainedObservedIntents != 4 || retainedUnobservedIntent != 1 {
		t.Fatalf("bounded feedback retention receipts/intents/unobserved=%d/%d/%d, want 4/4/1", retainedObservedReceipts, retainedObservedIntents, retainedUnobservedIntent)
	}
	for feedbackPruned > 0 {
		feedbackPruned, err = coordinator.PruneTerminalDeliveryState(ctx, fence, time.Hour, 2)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_ack_receipts WHERE flow_incarnation_id=$1 AND position_id=ANY($2)`, fence.FlowIncarnationID, observedPositions).Scan(&retainedObservedReceipts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_ack_intents WHERE flow_incarnation_id=$1 AND position_id=ANY($2)`, fence.FlowIncarnationID, observedPositions).Scan(&retainedObservedIntents); err != nil {
		t.Fatal(err)
	}
	if retainedObservedReceipts != 1 || retainedObservedIntents != 1 {
		t.Fatalf("drained feedback retention receipts/intents=%d/%d, want only current root", retainedObservedReceipts, retainedObservedIntents)
	}

	concurrent := retentionTransaction(table, 3004, "0/F00", 4)
	concurrentIntent := transactionIntentForFence(t, fence, revisionID, concurrent)
	blockingDriver := &blockingApplyTransactionDriver{
		ManagedTransactionDestination: target,
		entered:                       make(chan struct{}, 1),
		release:                       make(chan struct{}),
	}
	var concurrentGrant connector.AckGrant
	concurrentDone := make(chan error, 1)
	go func() {
		grant, deliveryErr := coordinator.DeliverTransaction(ctx, fence, concurrentIntent, concurrent, blockingDriver)
		concurrentGrant = grant
		concurrentDone <- deliveryErr
	}()
	select {
	case <-blockingDriver.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent target delivery did not reach deterministic apply boundary")
	}
	if _, err := pool.Exec(ctx, `UPDATE delivery_manifests SET created_at=clock_timestamp()-interval '2 hours' WHERE flow_incarnation_id=$1 AND logical_batch_id=$2`, fence.FlowIncarnationID, concurrentIntent.LogicalBatchID); err != nil {
		t.Fatal(err)
	}
	blockRetention.Store(true)
	pruneDone := make(chan error, 1)
	go func() {
		_, pruneErr := coordinator.PruneTerminalDeliveryState(ctx, fence, time.Hour, 10)
		pruneDone <- pruneErr
	}()
	select {
	case <-retentionEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("retention did not lock the authoritative checkpoint root")
	}
	close(blockingDriver.release)
	select {
	case deliveryErr := <-concurrentDone:
		t.Fatalf("concurrent finalization bypassed the locked retention root: %v", deliveryErr)
	case <-time.After(100 * time.Millisecond):
	}
	blockRetention.Store(false)
	close(retentionRelease)
	if err := <-pruneDone; err != nil {
		t.Fatal(err)
	}
	if err := <-concurrentDone; err != nil {
		t.Fatal(err)
	}
	if err := coordinator.CommitSourceFeedback(ctx, fence, concurrentGrant, &flushEvidenceTestSource{}); err != nil {
		t.Fatal(err)
	}
	var concurrentManifest, concurrentReceipt int
	var concurrentCheckpoint string
	if err := pool.QueryRow(ctx, `
SELECT
  (SELECT count(*) FROM delivery_manifests WHERE flow_incarnation_id=$1 AND logical_batch_id=$2),
  (SELECT count(*) FROM delivery_receipts WHERE flow_incarnation_id=$1 AND logical_batch_id=$2),
  (SELECT lsn FROM authoritative_checkpoints WHERE flow_incarnation_id=$1)`, fence.FlowIncarnationID, concurrentIntent.LogicalBatchID).Scan(&concurrentManifest, &concurrentReceipt, &concurrentCheckpoint); err != nil {
		t.Fatal(err)
	}
	if concurrentManifest != 1 || concurrentReceipt != 1 || concurrentCheckpoint != concurrent.EndLSN {
		t.Fatalf("concurrent finalization retention safety manifest/receipt/checkpoint=%d/%d/%s, want 1/1/%s", concurrentManifest, concurrentReceipt, concurrentCheckpoint, concurrent.EndLSN)
	}

	if _, err := pool.Exec(ctx, `UPDATE delivery_manifests SET logical_batch_id=NULL,created_at=clock_timestamp()-interval '2 hours' WHERE flow_incarnation_id=$1 AND position_id=$2`, fence.FlowIncarnationID, concurrentIntent.PositionID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE delivery_attempts SET logical_batch_id=NULL WHERE flow_incarnation_id=$1 AND position_id=$2`, fence.FlowIncarnationID, concurrentIntent.PositionID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE delivery_receipts SET logical_batch_id=NULL WHERE flow_incarnation_id=$1 AND position_id=$2`, fence.FlowIncarnationID, concurrentIntent.PositionID); err != nil {
		t.Fatal(err)
	}
	newRoot, err := coordinator.AuthorizeAck(ctx, fence, connector.Checkpoint{LSN: "0/1100"})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.CommitSourceFeedback(ctx, fence, newRoot, &flushEvidenceTestSource{}); err != nil {
		t.Fatal(err)
	}
	legacyPruned, err := coordinator.PruneTerminalDeliveryState(ctx, fence, time.Hour, 10)
	if err != nil {
		t.Fatal(err)
	}
	if legacyPruned < 1 {
		t.Fatalf("unadopted checkpoint-1 terminal rows pruned=%d, want at least manifest", legacyPruned)
	}
	var legacyManifests, legacyAttempts, legacyReceipts int
	if err := pool.QueryRow(ctx, `
SELECT
  (SELECT count(*) FROM delivery_manifests WHERE flow_incarnation_id=$1 AND position_id=$2),
  (SELECT count(*) FROM delivery_attempts WHERE flow_incarnation_id=$1 AND position_id=$2),
  (SELECT count(*) FROM delivery_receipts WHERE flow_incarnation_id=$1 AND position_id=$2)`, fence.FlowIncarnationID, concurrentIntent.PositionID).Scan(&legacyManifests, &legacyAttempts, &legacyReceipts); err != nil {
		t.Fatal(err)
	}
	if legacyManifests != 0 || legacyAttempts != 0 || legacyReceipts != 0 {
		t.Fatalf("unadopted checkpoint-1 terminal rows retained manifest/attempt/receipt=%d/%d/%d", legacyManifests, legacyAttempts, legacyReceipts)
	}
}

type failFirstTransactionDriver struct {
	connector.ManagedTransactionDestination
	fail bool
}

func (d *failFirstTransactionDriver) ApplyTransaction(ctx context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (connector.DeliveryEvidence, error) {
	if d.fail {
		d.fail = false
		return connector.DeliveryEvidence{}, errors.New("deterministic target failure before side effect")
	}
	return d.ManagedTransactionDestination.ApplyTransaction(ctx, intent, transaction)
}

type blockingApplyTransactionDriver struct {
	connector.ManagedTransactionDestination
	entered chan struct{}
	release chan struct{}
}

func (d *blockingApplyTransactionDriver) ApplyTransaction(ctx context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (connector.DeliveryEvidence, error) {
	select {
	case d.entered <- struct{}{}:
	default:
	}
	select {
	case <-d.release:
		return d.ManagedTransactionDestination.ApplyTransaction(ctx, intent, transaction)
	case <-ctx.Done():
		return connector.DeliveryEvidence{}, ctx.Err()
	}
}

type reconciliationFailureDriver struct {
	connector.ManagedTransactionDestination
}

func (*reconciliationFailureDriver) ApplyTransaction(context.Context, connector.DeliveryIntent, connector.SourceTransaction) (connector.DeliveryEvidence, error) {
	return connector.DeliveryEvidence{}, fmt.Errorf("%w: deterministic missing target response", connector.ErrDeliveryIndeterminate)
}

func (*reconciliationFailureDriver) Reconcile(context.Context, connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, errors.New("deterministic reconciliation transport failure")
}

func retentionTransaction(table string, xid uint32, endLSN string, id int64) connector.SourceTransaction {
	return connector.SourceTransaction{
		SourceLineageID: "retention-lineage", TransactionID: xid,
		BeginLSN: "0/400", CommitLSN: endLSN, EndLSN: endLSN, Checkpoint: connector.Checkpoint{LSN: endLSN},
		Fragments: []connector.TransactionFragment{{
			Ordinal: 0,
			Batch: connector.Batch{
				Schema: managedTransactionSchema("public", table, connector.Column{Name: "value", Type: "text"}),
				Records: []connector.Record{{
					Table: table, Operation: connector.OpInsert, SchemaVersion: 1,
					Key:   []byte(fmt.Sprintf(`{"id":%d}`, id)),
					After: map[string]any{"id": id, "value": fmt.Sprintf("value-%d", id)},
				}},
			},
		}},
	}
}

func transactionIntentForFence(t *testing.T, fence authority.RunFence, revisionID string, transaction connector.SourceTransaction) connector.DeliveryIntent {
	t.Helper()
	contentHash, err := connector.SourceTransactionContentHash(transaction)
	if err != nil {
		t.Fatal(err)
	}
	logicalBatchID, err := connector.SourceTransactionLogicalBatchID(transaction)
	if err != nil {
		t.Fatal(err)
	}
	positionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	return connector.DeliveryIntent{
		FlowID: fence.FlowID, FlowIncarnationID: fence.FlowIncarnationID.String(), SourceLineageID: transaction.SourceLineageID,
		Generation: fence.Generation, AcquisitionID: fence.AcquisitionID.String(), LeaseEpoch: fence.LeaseEpoch,
		DestinationRevisionID: revisionID, LogicalBatchID: logicalBatchID, PositionID: positionID, ContentHash: contentHash,
	}
}
