package tests

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestCanonicalArtifactPublicationFailureBoundaries(t *testing.T) {
	deps := newArtifactIntegrationDeps(t)
	config := artifactlog.StreamConfig{
		HardRetainedBytes: 256 << 20, BacklogCountHigh: 1000,
		BacklogBytesHigh: 256 << 20, BacklogAgeHigh: 24 * time.Hour,
		BackpressurePollInterval: 10 * time.Millisecond,
		Consumers:                []string{"destination-v1"},
	}
	boundaries := []string{
		"after_upload_intent_commit",
		"after_object_put",
		"after_upload_evidence",
		"after_object_verified",
		"before_publication_transaction",
		"before_publication_commit",
		"after_publication_commit",
	}
	for _, boundary := range boundaries {
		t.Run(boundary, func(t *testing.T) {
			fence := deps.newFence(t, boundary)
			transaction := artifactSourceTransaction()
			var once sync.Once
			publisher, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, config, artifactlog.WithPublisherHooks(artifactlog.PublisherHooks{
				Boundary: func(_ context.Context, reached string) error {
					if reached != boundary {
						return nil
					}
					injected := false
					once.Do(func() { injected = true })
					if injected {
						return errors.New("injected process loss")
					}
					return nil
				},
			}))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := publisher.Publish(deps.ctx, fence, transaction, managedBaselinePayload(t, transaction)); err == nil {
				t.Fatalf("boundary %s did not interrupt publication", boundary)
			}
			var publications, acknowledgements, baselines int
			if err := deps.pool.QueryRow(deps.ctx, `SELECT count(*) FROM artifact_publications WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&publications); err != nil {
				t.Fatal(err)
			}
			if err := deps.pool.QueryRow(deps.ctx, `SELECT count(*) FROM source_ack_intents WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&acknowledgements); err != nil {
				t.Fatal(err)
			}
			if err := deps.pool.QueryRow(deps.ctx, `SELECT count(*) FROM managed_schema_baselines WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&baselines); err != nil {
				t.Fatal(err)
			}
			if boundary == "after_publication_commit" {
				if publications != 1 || acknowledgements != 1 || baselines != 1 {
					t.Fatalf("ambiguous committed boundary roots/acks/baselines=%d/%d/%d, want new/new/new 1/1/1", publications, acknowledgements, baselines)
				}
			} else if publications != 0 || acknowledgements != 0 || baselines != 0 {
				t.Fatalf("pre-commit boundary %s roots/acks/baselines=%d/%d/%d, want old/old/old 0/0/0", boundary, publications, acknowledgements, baselines)
			}

			recovered, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, config)
			if err != nil {
				t.Fatal(err)
			}
			publication, err := recovered.Publish(deps.ctx, fence, transaction, managedBaselinePayload(t, transaction))
			if err != nil {
				t.Fatal(err)
			}
			if publication.LogicalBatchID == "" || publication.AckGrant.Checkpoint.LSN != transaction.EndLSN {
				t.Fatalf("recovered publication=%+v", publication)
			}
			if err := deps.pool.QueryRow(deps.ctx, `SELECT count(*) FROM artifact_publications WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&publications); err != nil {
				t.Fatal(err)
			}
			if publications != 1 {
				t.Fatalf("recovered publication count=%d, want 1", publications)
			}
			swappedBaseline := managedBaselinePayload(t, transaction)
			swappedBaseline.Schemas[0].QuotedIdentifiers = map[string]bool{"events": true}
			if _, err := recovered.Publish(deps.ctx, fence, transaction, swappedBaseline); !errors.Is(err, connector.ErrDeliveryConflict) {
				t.Fatalf("artifact retry swapped baseline error=%v, want conflict", err)
			}
		})
	}
}

func TestCanonicalArtifactBarrierOnlyDelivery(t *testing.T) {
	deps := newArtifactIntegrationDeps(t)
	fence := deps.newFence(t, "barrier-only")
	config := artifactlog.StreamConfig{
		HardRetainedBytes: 128 << 20, BacklogCountHigh: 100,
		BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour,
		Consumers: []string{"destination-v1"},
	}
	publisher, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, config)
	if err != nil {
		t.Fatal(err)
	}
	transaction := artifactSourceTransaction()
	record := &transaction.Fragments[0].Batch.Records[0]
	record.Operation = connector.OpDDL
	record.After = nil
	record.DDL = "ALTER TABLE public.artifact_events ADD COLUMN note text"
	publication, err := publisher.Publish(deps.ctx, fence, transaction, managedBaselinePayload(t, transaction))
	if err != nil {
		t.Fatal(err)
	}
	if len(publication.Artifacts) != 0 {
		t.Fatalf("barrier-only publication artifacts=%d, want 0", len(publication.Artifacts))
	}
	catalog := &recordingAppendCatalog{snapshotID: "barrier-snapshot"}
	consumer, err := artifactlog.NewConsumer(deps.pool, catalog)
	if err != nil {
		t.Fatal(err)
	}
	consumed, err := consumer.ConsumeNext(deps.ctx, fence, "destination-v1")
	if err != nil {
		t.Fatal(err)
	}
	if !consumed || catalog.objectCount != 0 || catalog.barrierCount != 1 {
		t.Fatalf("barrier delivery consumed/objects/barriers=%t/%d/%d", consumed, catalog.objectCount, catalog.barrierCount)
	}
	replayed, err := publisher.Publish(deps.ctx, fence, transaction, managedBaselinePayload(t, transaction))
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ID != publication.ID || replayed.LogicalBatchID != publication.LogicalBatchID {
		t.Fatalf("barrier replay publication=%s/%s, want %s/%s", replayed.ID, replayed.LogicalBatchID, publication.ID, publication.LogicalBatchID)
	}
}

func TestCanonicalArtifactStalePublisherCannotCommit(t *testing.T) {
	deps := newArtifactIntegrationDeps(t)
	oldFence := deps.newFence(t, "stale")
	config := artifactlog.StreamConfig{
		HardRetainedBytes: 128 << 20, BacklogCountHigh: 100,
		BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour,
		Consumers: []string{"destination-v1"},
	}
	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	publisher, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, config, artifactlog.WithPublisherHooks(artifactlog.PublisherHooks{
		Boundary: func(ctx context.Context, boundary string) error {
			if boundary != "before_publication_transaction" {
				return nil
			}
			once.Do(func() { close(reached) })
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	publishErr := make(chan error, 1)
	go func() {
		transaction := artifactSourceTransaction()
		_, err := publisher.Publish(deps.ctx, oldFence, transaction, managedBaselinePayload(t, transaction))
		publishErr <- err
	}()
	<-reached
	if err := deps.authority.FinishProducer(deps.ctx, oldFence, "takeover"); err != nil {
		t.Fatal(err)
	}
	newFence, err := deps.authority.AcquireProducer(deps.ctx, oldFence.FlowID, "replacement", "test", oldFence.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-publishErr; !errors.Is(err, authority.ErrFenceRejected) {
		t.Fatalf("stale publication error=%v, want fence rejection", err)
	}
	var publications, acknowledgements int
	if err := deps.pool.QueryRow(deps.ctx, `SELECT count(*) FROM artifact_publications WHERE flow_incarnation_id=$1`, oldFence.FlowIncarnationID).Scan(&publications); err != nil {
		t.Fatal(err)
	}
	if err := deps.pool.QueryRow(deps.ctx, `SELECT count(*) FROM source_ack_intents WHERE flow_incarnation_id=$1`, oldFence.FlowIncarnationID).Scan(&acknowledgements); err != nil {
		t.Fatal(err)
	}
	if publications != 0 || acknowledgements != 0 {
		t.Fatalf("stale publisher changed roots/acks=%d/%d", publications, acknowledgements)
	}
	clean, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, config)
	if err != nil {
		t.Fatal(err)
	}
	cleanTransaction := artifactSourceTransaction()
	if _, err := clean.Publish(deps.ctx, newFence, cleanTransaction, managedBaselinePayload(t, cleanTransaction)); err != nil {
		t.Fatal(err)
	}
}

func TestCanonicalArtifactBackpressureAndRootedRetention(t *testing.T) {
	deps := newArtifactIntegrationDeps(t)
	t.Run("restored batch and age watermarks stop reads", func(t *testing.T) {
		fence := deps.newFence(t, "backpressure")
		config := artifactlog.StreamConfig{
			HardRetainedBytes: 128 << 20, BacklogCountHigh: 1,
			BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour,
			BackpressurePollInterval: time.Millisecond, Consumers: []string{"destination-v1"},
		}
		publisher, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, config)
		if err != nil {
			t.Fatal(err)
		}
		transaction := artifactSourceTransaction()
		if _, err := publisher.Publish(deps.ctx, fence, transaction, managedBaselinePayload(t, transaction)); err != nil {
			t.Fatal(err)
		}
		if err := publisher.Recover(deps.ctx, fence); err != nil {
			t.Fatal(err)
		}
		waitCtx, cancel := context.WithCancel(deps.ctx)
		cancel()
		if err := publisher.WaitForReadAdmission(waitCtx, fence); !errors.Is(err, context.Canceled) {
			t.Fatalf("restored batch backpressure error=%v, want cancellation while blocked", err)
		}
		if _, err := deps.pool.Exec(deps.ctx, `UPDATE artifact_deliveries SET delivered_at=clock_timestamp() WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID); err != nil {
			t.Fatal(err)
		}
		if err := publisher.WaitForReadAdmission(deps.ctx, fence); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("restored age watermark stops reads", func(t *testing.T) {
		fence := deps.newFence(t, "age")
		config := artifactlog.StreamConfig{
			HardRetainedBytes: 128 << 20, BacklogCountHigh: 100,
			BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour,
			BackpressurePollInterval: time.Millisecond, Consumers: []string{"destination-v1"},
		}
		publisher, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, config)
		if err != nil {
			t.Fatal(err)
		}
		transaction := artifactSourceTransaction()
		if _, err := publisher.Publish(deps.ctx, fence, transaction, managedBaselinePayload(t, transaction)); err != nil {
			t.Fatal(err)
		}
		if _, err := deps.pool.Exec(deps.ctx, `UPDATE artifact_deliveries SET created_at=clock_timestamp()-interval '2 hours' WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID); err != nil {
			t.Fatal(err)
		}
		waitCtx, cancel := context.WithCancel(deps.ctx)
		cancel()
		if err := publisher.WaitForReadAdmission(waitCtx, fence); !errors.Is(err, context.Canceled) {
			t.Fatalf("restored age backpressure error=%v, want cancellation while blocked", err)
		}
		if _, err := deps.pool.Exec(deps.ctx, `UPDATE artifact_deliveries SET delivered_at=clock_timestamp() WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID); err != nil {
			t.Fatal(err)
		}
		if err := publisher.WaitForReadAdmission(deps.ctx, fence); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("projected byte watermark prevents publication and acknowledgement", func(t *testing.T) {
		fence := deps.newFence(t, "bytes")
		publisher, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, artifactlog.StreamConfig{
			HardRetainedBytes: 128 << 20, BacklogCountHigh: 100,
			BacklogBytesHigh: 1, BacklogAgeHigh: time.Hour, Consumers: []string{"destination-v1"},
		})
		if err != nil {
			t.Fatal(err)
		}
		transaction := artifactSourceTransaction()
		if _, err := publisher.Publish(deps.ctx, fence, transaction, managedBaselinePayload(t, transaction)); !errors.Is(err, artifactlog.ErrBackpressure) {
			t.Fatalf("byte watermark error=%v, want backpressure", err)
		}
		var publications, acknowledgements int
		if err := deps.pool.QueryRow(deps.ctx, `SELECT count(*) FROM artifact_publications WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&publications); err != nil {
			t.Fatal(err)
		}
		if err := deps.pool.QueryRow(deps.ctx, `SELECT count(*) FROM source_ack_intents WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&acknowledgements); err != nil {
			t.Fatal(err)
		}
		if publications != 0 || acknowledgements != 0 {
			t.Fatalf("backpressured publication changed roots/acks=%d/%d", publications, acknowledgements)
		}
	})

	t.Run("rooted retention waits for deliveries ack receipt and newer checkpoint", func(t *testing.T) {
		fence := deps.newFence(t, "retention")
		config := artifactlog.StreamConfig{
			HardRetainedBytes: 128 << 20, BacklogCountHigh: 100,
			BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour, Consumers: []string{"destination-v1"},
		}
		publisher, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, config)
		if err != nil {
			t.Fatal(err)
		}
		firstTransaction := artifactTransactionAt(100, "0/C0", "0/C8", "0/D0", "first")
		first, err := publisher.Publish(deps.ctx, fence, firstTransaction, managedBaselinePayload(t, firstTransaction))
		if err != nil {
			t.Fatal(err)
		}
		if err := deps.delivery.RecordAckReceipt(deps.ctx, fence, first.AckGrant, first.AckGrant.Checkpoint.LSN); err != nil {
			t.Fatal(err)
		}
		if err := consumeArtifactPublication(deps.ctx, deps.pool, fence, "destination-v1"); err != nil {
			t.Fatal(err)
		}
		secondTransaction := artifactTransactionAt(101, "0/D1", "0/D8", "0/E0", "second")
		second, err := publisher.Publish(deps.ctx, fence, secondTransaction, managedBaselinePayload(t, secondTransaction))
		if err != nil {
			t.Fatal(err)
		}
		if second.Sequence <= first.Sequence {
			t.Fatalf("publication sequences=%d then %d", first.Sequence, second.Sequence)
		}
		if _, err := deps.pool.Exec(deps.ctx, `
UPDATE source_ack_intents SET authorized_at=clock_timestamp()-interval '2 hours'
WHERE flow_incarnation_id=$1 AND position_id=$2`, fence.FlowIncarnationID, first.AckGrant.PositionID); err != nil {
			t.Fatal(err)
		}
		if _, err := deps.pool.Exec(deps.ctx, `
UPDATE source_ack_receipts SET recorded_at=clock_timestamp()-interval '2 hours'
WHERE flow_incarnation_id=$1 AND position_id=$2`, fence.FlowIncarnationID, first.AckGrant.PositionID); err != nil {
			t.Fatal(err)
		}
		if _, err := deps.delivery.PruneTerminalDeliveryState(deps.ctx, fence, time.Hour, 100); err != nil {
			t.Fatal(err)
		}
		var retainedAck int
		if err := deps.pool.QueryRow(deps.ctx, `
SELECT count(*) FROM source_ack_receipts
WHERE flow_incarnation_id=$1 AND position_id=$2`, fence.FlowIncarnationID, first.AckGrant.PositionID).Scan(&retainedAck); err != nil {
			t.Fatal(err)
		}
		if retainedAck != 1 {
			t.Fatal("delivery pruning removed a source ACK receipt that still roots retained artifacts")
		}
		if _, err := deps.pool.Exec(deps.ctx, `UPDATE artifact_publications SET published_at=clock_timestamp()-interval '2 hours' WHERE publication_id=$1`, first.ID); err != nil {
			t.Fatal(err)
		}
		collector, err := artifactlog.NewCollector(deps.pool, deps.objects, artifactlog.WithCollectorHooks(artifactlog.CollectorHooks{
			Boundary: func(_ context.Context, boundary string) error {
				if boundary == "after_gc_mark" {
					return errors.New("injected retention collector loss after mark")
				}
				return nil
			},
		}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := collector.CollectRetainedOne(deps.ctx, fence, time.Hour); err == nil {
			t.Fatal("retention collector did not stop after durable mark")
		}
		if err := publisher.RecomputeQuota(deps.ctx, fence); err != nil {
			t.Fatal(err)
		}
		var rootedBeforeRecovery, firstBytes int64
		if err := deps.pool.QueryRow(deps.ctx, `SELECT rooted_bytes FROM artifact_quota_accounts WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&rootedBeforeRecovery); err != nil {
			t.Fatal(err)
		}
		if err := deps.pool.QueryRow(deps.ctx, `SELECT rooted_bytes FROM artifact_publications WHERE publication_id=$1`, first.ID).Scan(&firstBytes); err != nil {
			t.Fatal(err)
		}
		if rootedBeforeRecovery < firstBytes {
			t.Fatalf("quota recompute dropped marked-but-unreleased root: rooted=%d first=%d", rootedBeforeRecovery, firstBytes)
		}
		recovered, err := artifactlog.NewCollector(deps.pool, deps.objects)
		if err != nil {
			t.Fatal(err)
		}
		swept, err := recovered.CollectRetainedOne(deps.ctx, fence, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if !swept {
			t.Fatal("retention collector found no marked rooted artifact on recovery")
		}
		var rootedAfterRecovery int64
		if err := deps.pool.QueryRow(deps.ctx, `SELECT rooted_bytes FROM artifact_quota_accounts WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&rootedAfterRecovery); err != nil {
			t.Fatal(err)
		}
		if rootedAfterRecovery != rootedBeforeRecovery-firstBytes {
			t.Fatalf("retention quota after recovery=%d, want %d", rootedAfterRecovery, rootedBeforeRecovery-firstBytes)
		}
		var state string
		var marked, released bool
		if err := deps.pool.QueryRow(deps.ctx, `
SELECT object.state,root.release_marked_at IS NOT NULL,root.released_at IS NOT NULL
FROM artifact_publication_objects AS root
JOIN artifact_objects AS object ON object.artifact_id=root.artifact_id
WHERE root.publication_id=$1`, first.ID).Scan(&state, &marked, &released); err != nil {
			t.Fatal(err)
		}
		if state != "deleted" || !marked || !released {
			t.Fatalf("retained object state/mark/release=%s/%t/%t", state, marked, released)
		}
	})
}

func TestCanonicalArtifactGCDoesNotClaimActiveUpload(t *testing.T) {
	deps := newArtifactIntegrationDeps(t)
	fence := deps.newFence(t, "active-upload-gc-race")
	config := artifactlog.StreamConfig{
		HardRetainedBytes: 128 << 20, BacklogCountHigh: 100,
		BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour,
	}
	objects := &blockingPutStore{
		ObjectStore: deps.objects,
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	publisher, err := artifactlog.NewPublisher(deps.ctx, deps.pool, objects, config)
	if err != nil {
		t.Fatal(err)
	}
	publishResult := make(chan error, 1)
	go func() {
		transaction := artifactSourceTransaction()
		_, err := publisher.Publish(deps.ctx, fence, transaction, managedBaselinePayload(t, transaction))
		publishResult <- err
	}()
	<-objects.started
	if _, err := deps.pool.Exec(deps.ctx, `UPDATE artifact_objects SET updated_at=clock_timestamp()-interval '2 hours' WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	collector, err := artifactlog.NewCollector(deps.pool, deps.objects)
	if err != nil {
		t.Fatal(err)
	}
	swept, err := collector.CollectOne(deps.ctx, fence, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if swept {
		t.Fatal("GC claimed an artifact while its current fenced upload was active")
	}
	close(objects.release)
	if err := <-publishResult; err != nil {
		t.Fatalf("publish after concurrent GC probe: %v", err)
	}
	var state string
	if err := deps.pool.QueryRow(deps.ctx, `SELECT state FROM artifact_objects WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "rooted" {
		t.Fatalf("artifact state=%s, want rooted", state)
	}
}

func TestCanonicalArtifactGCTakeoverDoesNotOrphanInFlightPut(t *testing.T) {
	deps := newArtifactIntegrationDeps(t)
	oldFence := deps.newFence(t, "stale-upload-gc-race")
	config := artifactlog.StreamConfig{
		HardRetainedBytes: 128 << 20, BacklogCountHigh: 100,
		BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour,
	}
	objects := &blockingPutStore{
		ObjectStore: deps.objects,
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	oldPublisher, err := artifactlog.NewPublisher(deps.ctx, deps.pool, objects, config)
	if err != nil {
		t.Fatal(err)
	}
	oldResult := make(chan error, 1)
	go func() {
		transaction := artifactSourceTransaction()
		_, err := oldPublisher.Publish(deps.ctx, oldFence, transaction, managedBaselinePayload(t, transaction))
		oldResult <- err
	}()
	<-objects.started
	if _, err := deps.pool.Exec(deps.ctx, `
UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second'
WHERE incarnation_id=$1`, oldFence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	newFence, err := deps.authority.AcquireProducer(
		deps.ctx, oldFence.FlowID, "artifact-worker-takeover", "test",
		oldFence.Generation, time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := deps.pool.Exec(deps.ctx, `UPDATE artifact_objects SET updated_at=clock_timestamp()-interval '2 hours' WHERE flow_incarnation_id=$1`, oldFence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	collector, err := artifactlog.NewCollector(deps.pool, deps.objects)
	if err != nil {
		t.Fatal(err)
	}
	swept, err := collector.CollectOne(deps.ctx, newFence, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if swept {
		t.Fatal("takeover GC finalized a reserved object while the old fenced PUT was still in flight")
	}
	close(objects.release)
	if err := <-oldResult; err == nil {
		t.Fatal("stale publisher committed upload evidence after takeover")
	}
	newPublisher, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, config)
	if err != nil {
		t.Fatal(err)
	}
	newTransaction := artifactSourceTransaction()
	publication, err := newPublisher.Publish(deps.ctx, newFence, newTransaction, managedBaselinePayload(t, newTransaction))
	if err != nil {
		t.Fatalf("takeover publisher did not reconcile stale PUT: %v", err)
	}
	if publication.ID == uuid.Nil {
		t.Fatal("takeover publisher returned an empty publication")
	}
	var state string
	if err := deps.pool.QueryRow(deps.ctx, `SELECT state FROM artifact_objects WHERE flow_incarnation_id=$1`, newFence.FlowIncarnationID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "rooted" {
		t.Fatalf("reconciled artifact state=%s, want rooted", state)
	}
}

func TestCanonicalArtifactPublisherGCClaimRevalidation(t *testing.T) {
	deps := newArtifactIntegrationDeps(t)
	fence := deps.newFence(t, "publisher-gc-race")
	config := artifactlog.StreamConfig{
		HardRetainedBytes: 128 << 20, BacklogCountHigh: 100,
		BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour,
	}
	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	publisher, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, config, artifactlog.WithPublisherHooks(artifactlog.PublisherHooks{
		Boundary: func(ctx context.Context, boundary string) error {
			if boundary != "before_publication_transaction" {
				return nil
			}
			once.Do(func() { close(reached) })
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	publishErr := make(chan error, 1)
	go func() {
		transaction := artifactSourceTransaction()
		_, err := publisher.Publish(deps.ctx, fence, transaction, managedBaselinePayload(t, transaction))
		publishErr <- err
	}()
	<-reached
	if _, err := deps.pool.Exec(deps.ctx, `UPDATE artifact_objects SET updated_at=clock_timestamp()-interval '2 hours' WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	collector, err := artifactlog.NewCollector(deps.pool, deps.objects)
	if err != nil {
		t.Fatal(err)
	}
	swept, err := collector.CollectOne(deps.ctx, fence, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !swept {
		t.Fatal("collector did not claim the paused publisher's orphan")
	}
	close(release)
	if err := <-publishErr; err == nil {
		t.Fatal("paused publisher rooted an object after GC mark/sweep")
	}
	var publications, acknowledgements int
	if err := deps.pool.QueryRow(deps.ctx, `SELECT count(*) FROM artifact_publications WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&publications); err != nil {
		t.Fatal(err)
	}
	if err := deps.pool.QueryRow(deps.ctx, `SELECT count(*) FROM source_ack_intents WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&acknowledgements); err != nil {
		t.Fatal(err)
	}
	if publications != 0 || acknowledgements != 0 {
		t.Fatalf("publisher/GC race changed roots/acks=%d/%d", publications, acknowledgements)
	}
	replayTransaction := artifactSourceTransaction()
	replayed, err := publisher.Publish(deps.ctx, fence, replayTransaction, managedBaselinePayload(t, replayTransaction))
	if err != nil {
		t.Fatalf("replay after GC sweep: %v", err)
	}
	if replayed.ID == uuid.Nil {
		t.Fatal("replay after GC sweep returned an empty publication")
	}
	if err := deps.pool.QueryRow(deps.ctx, `SELECT count(*) FROM artifact_publications WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&publications); err != nil {
		t.Fatal(err)
	}
	if err := deps.pool.QueryRow(deps.ctx, `SELECT count(*) FROM source_ack_intents WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&acknowledgements); err != nil {
		t.Fatal(err)
	}
	if publications != 1 || acknowledgements != 1 {
		t.Fatalf("replay after GC sweep roots/acks=%d/%d, want 1/1", publications, acknowledgements)
	}
}

func TestCanonicalArtifactOrphanMarkSweepCrashRecovery(t *testing.T) {
	deps := newArtifactIntegrationDeps(t)
	for _, boundary := range []string{"after_gc_mark", "after_gc_delete"} {
		t.Run(boundary, func(t *testing.T) {
			fence := deps.newFence(t, boundary)
			config := artifactlog.StreamConfig{
				HardRetainedBytes: 128 << 20, BacklogCountHigh: 100,
				BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour,
			}
			publisher, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, config, artifactlog.WithPublisherHooks(artifactlog.PublisherHooks{
				Boundary: func(_ context.Context, reached string) error {
					if reached == "before_publication_transaction" {
						return errors.New("leave verified orphan")
					}
					return nil
				},
			}))
			if err != nil {
				t.Fatal(err)
			}
			transaction := artifactSourceTransaction()
			if _, err := publisher.Publish(deps.ctx, fence, transaction, managedBaselinePayload(t, transaction)); err == nil {
				t.Fatal("publication unexpectedly committed")
			}
			if _, err := deps.pool.Exec(deps.ctx, `UPDATE artifact_objects SET updated_at=clock_timestamp()-interval '2 hours' WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID); err != nil {
				t.Fatal(err)
			}
			var once sync.Once
			collector, err := artifactlog.NewCollector(deps.pool, deps.objects, artifactlog.WithCollectorHooks(artifactlog.CollectorHooks{
				Boundary: func(_ context.Context, reached string) error {
					if reached != boundary {
						return nil
					}
					injected := false
					once.Do(func() { injected = true })
					if injected {
						return errors.New("injected GC process loss")
					}
					return nil
				},
			}))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := collector.CollectOne(deps.ctx, fence, time.Hour); err == nil {
				t.Fatalf("GC boundary %s did not interrupt", boundary)
			}
			recovered, err := artifactlog.NewCollector(deps.pool, deps.objects)
			if err != nil {
				t.Fatal(err)
			}
			swept, err := recovered.CollectOne(deps.ctx, fence, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			if !swept {
				t.Fatal("recovered collector found no marked orphan")
			}
			var state string
			var reserved int64
			if err := deps.pool.QueryRow(deps.ctx, `SELECT state FROM artifact_objects WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&state); err != nil {
				t.Fatal(err)
			}
			if err := deps.pool.QueryRow(deps.ctx, `SELECT reserved_bytes FROM artifact_quota_accounts WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&reserved); err != nil {
				t.Fatal(err)
			}
			if state != "deleted" || reserved != 0 {
				t.Fatalf("recovered orphan state/reserved=%s/%d", state, reserved)
			}
			retryPublisher, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, config)
			if err != nil {
				t.Fatal(err)
			}
			retryTransaction := artifactSourceTransaction()
			publication, err := retryPublisher.Publish(deps.ctx, fence, retryTransaction, managedBaselinePayload(t, retryTransaction))
			if err != nil {
				t.Fatalf("republish swept source transaction: %v", err)
			}
			if publication.ID == uuid.Nil {
				t.Fatal("republish swept source transaction returned an empty publication")
			}
			if err := deps.pool.QueryRow(deps.ctx, `SELECT state FROM artifact_objects WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&state); err != nil {
				t.Fatal(err)
			}
			if err := deps.pool.QueryRow(deps.ctx, `SELECT reserved_bytes FROM artifact_quota_accounts WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&reserved); err != nil {
				t.Fatal(err)
			}
			if state != "rooted" || reserved != 0 {
				t.Fatalf("republished artifact state/reserved=%s/%d, want rooted/0", state, reserved)
			}
		})
	}
}

// TestCanonicalArtifactConsumerReceiptBoundaryRecovery injects a crash after the
// consumer receipt commits and asserts recovery is idempotent: no duplicate
// delivery receipt or consumer checkpoint is produced.
func TestCanonicalArtifactConsumerReceiptBoundaryRecovery(t *testing.T) {
	deps := newArtifactIntegrationDeps(t)
	fence := deps.newFence(t, "consumer-receipt")
	config := artifactlog.StreamConfig{
		HardRetainedBytes: 128 << 20, BacklogCountHigh: 100,
		BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour, Consumers: []string{"destination-v1"},
	}
	publisher, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, config)
	if err != nil {
		t.Fatal(err)
	}
	transaction := artifactSourceTransaction()
	if _, err := publisher.Publish(deps.ctx, fence, transaction, managedBaselinePayload(t, transaction)); err != nil {
		t.Fatal(err)
	}

	var once sync.Once
	consumer, err := artifactlog.NewConsumer(deps.pool, &recordingAppendCatalog{}, artifactlog.WithConsumerHooks(artifactlog.ConsumerHooks{
		Reach: func(_ context.Context, boundary string) error {
			if boundary != "after_consumer_receipt" {
				return nil
			}
			injected := false
			once.Do(func() { injected = true })
			if injected {
				return errors.New("injected process loss after consumer receipt")
			}
			return nil
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.ConsumeNext(deps.ctx, fence, "destination-v1"); err == nil {
		t.Fatal("consumer did not surface the injected loss after receipt")
	}
	// The receipt and checkpoint are already durable because finalize committed
	// before the boundary fired.
	assertArtifactConsumerReceiptCount(t, deps, fence, "destination-v1", 1)

	recovered, err := artifactlog.NewConsumer(deps.pool, &recordingAppendCatalog{})
	if err != nil {
		t.Fatal(err)
	}
	consumed, err := recovered.ConsumeNext(deps.ctx, fence, "destination-v1")
	if err != nil {
		t.Fatal(err)
	}
	if consumed {
		t.Fatal("recovery re-consumed an already delivered publication")
	}
	assertArtifactConsumerReceiptCount(t, deps, fence, "destination-v1", 1)
}

// TestCanonicalArtifactConsumerReconcileBoundaryRecovery injects a crash after
// the external catalog commit (before finalize), then a second crash after the
// reconcile disposition is validated, and asserts that recovery finalizes
// exactly one delivery receipt and consumer checkpoint without duplication.
func TestCanonicalArtifactConsumerReconcileBoundaryRecovery(t *testing.T) {
	deps := newArtifactIntegrationDeps(t)
	fence := deps.newFence(t, "consumer-reconcile")
	config := artifactlog.StreamConfig{
		HardRetainedBytes: 128 << 20, BacklogCountHigh: 100,
		BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour, Consumers: []string{"destination-v1"},
	}
	publisher, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, config)
	if err != nil {
		t.Fatal(err)
	}
	transaction := artifactSourceTransaction()
	if _, err := publisher.Publish(deps.ctx, fence, transaction, managedBaselinePayload(t, transaction)); err != nil {
		t.Fatal(err)
	}

	// First attempt commits externally then loses the process before finalize.
	commitConsumer, err := artifactlog.NewConsumer(deps.pool, &recordingAppendCatalog{}, oneShotConsumerBoundary("after_catalog_commit"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := commitConsumer.ConsumeNext(deps.ctx, fence, "destination-v1"); err == nil {
		t.Fatal("consumer did not surface the injected loss after catalog commit")
	}
	assertArtifactConsumerReceiptCount(t, deps, fence, "destination-v1", 0)

	// Recovery reconciles the prior attempt but loses the process again right
	// after the reconcile disposition is validated, before finalize.
	reconcileConsumer, err := artifactlog.NewConsumer(deps.pool, &recordingAppendCatalog{}, oneShotConsumerBoundary("after_catalog_reconcile"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reconcileConsumer.ConsumeNext(deps.ctx, fence, "destination-v1"); err == nil {
		t.Fatal("consumer did not surface the injected loss after catalog reconcile")
	}
	assertArtifactConsumerReceiptCount(t, deps, fence, "destination-v1", 0)

	// A clean consumer finalizes exactly once via reconciliation.
	recovered, err := artifactlog.NewConsumer(deps.pool, &recordingAppendCatalog{})
	if err != nil {
		t.Fatal(err)
	}
	consumed, err := recovered.ConsumeNext(deps.ctx, fence, "destination-v1")
	if err != nil {
		t.Fatal(err)
	}
	if !consumed {
		t.Fatal("recovery did not reconcile the ambiguous catalog commit")
	}
	assertArtifactConsumerReceiptCount(t, deps, fence, "destination-v1", 1)

	// Idempotent re-run leaves the single receipt intact.
	if consumedAgain, err := recovered.ConsumeNext(deps.ctx, fence, "destination-v1"); err != nil || consumedAgain {
		t.Fatalf("redundant consume changed state consumed=%t err=%v", consumedAgain, err)
	}
	assertArtifactConsumerReceiptCount(t, deps, fence, "destination-v1", 1)
}

// TestCanonicalArtifactRandomizedCrashCycles is the live mirror of the
// deterministic in-process failure matrix. It publishes a distinct position per
// cycle, injects one randomly chosen publisher boundary each cycle against real
// PostgreSQL + MinIO, recovers, and asserts the standing invariants: exactly one
// publication per position and a quota account that equals a fresh recompute.
// The cycle count and seed are overridable so nightly can deepen the sweep.
func TestCanonicalArtifactRandomizedCrashCycles(t *testing.T) {
	deps := newArtifactIntegrationDeps(t)
	cycles := 100
	if v := os.Getenv("WALLABY_DURABLE_CYCLES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cycles = n
		}
	}
	seed := int64(20260728)
	if v := os.Getenv("WALLABY_DURABLE_SEED"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			seed = n
		}
	}
	rng := rand.New(rand.NewSource(seed))
	fence := deps.newFence(t, "randomized-cycles")
	config := artifactlog.StreamConfig{
		HardRetainedBytes: 1 << 30, BacklogCountHigh: int64(cycles) + 100,
		BacklogBytesHigh: 1 << 30, BacklogAgeHigh: 24 * time.Hour, Consumers: []string{"destination-v1"},
	}
	boundaries := []string{
		"after_upload_intent_commit",
		"after_object_put",
		"after_upload_evidence",
		"after_object_verified",
		"before_publication_transaction",
		"before_publication_commit",
		"after_publication_commit",
	}
	t.Logf("randomized crash cycles: cycles=%d seed=%d", cycles, seed)
	for i := 0; i < cycles; i++ {
		boundary := boundaries[rng.Intn(len(boundaries))]
		position := 0x1000 + i
		txn := artifactTransactionAt(uint32(1000+i),
			fmt.Sprintf("0/%X0", position), fmt.Sprintf("0/%X8", position),
			fmt.Sprintf("0/%XF", position), fmt.Sprintf("value-%d", i))

		var once sync.Once
		publisher, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, config, artifactlog.WithPublisherHooks(artifactlog.PublisherHooks{
			Boundary: func(_ context.Context, reached string) error {
				if reached != boundary {
					return nil
				}
				injected := false
				once.Do(func() { injected = true })
				if injected {
					return errors.New("injected process loss")
				}
				return nil
			},
		}))
		if err != nil {
			t.Fatalf("cycle %d: %v", i, err)
		}
		if _, err := publisher.Publish(deps.ctx, fence, txn, managedBaselinePayload(t, txn)); err == nil {
			t.Fatalf("cycle %d boundary %s did not interrupt publication", i, boundary)
		}

		recovered, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, config)
		if err != nil {
			t.Fatalf("cycle %d recovery publisher: %v", i, err)
		}
		if _, err := recovered.Publish(deps.ctx, fence, txn, managedBaselinePayload(t, txn)); err != nil {
			t.Fatalf("cycle %d recovery publish (boundary %s): %v", i, boundary, err)
		}
		// Republishing the identical position must remain idempotent.
		if _, err := recovered.Publish(deps.ctx, fence, txn, managedBaselinePayload(t, txn)); err != nil {
			t.Fatalf("cycle %d idempotent republish (boundary %s): %v", i, boundary, err)
		}

		var publications int
		if err := deps.pool.QueryRow(deps.ctx, `SELECT count(*) FROM artifact_publications WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&publications); err != nil {
			t.Fatalf("cycle %d count publications: %v", i, err)
		}
		if publications != i+1 {
			t.Fatalf("cycle %d publications=%d, want %d (exactly one publication per position)", i, publications, i+1)
		}

		// Drain periodically so the consumer/GC boundaries participate too.
		if i%5 == 4 {
			if err := consumeArtifactPublication(deps.ctx, deps.pool, fence, "destination-v1"); err != nil {
				t.Fatalf("cycle %d consume: %v", i, err)
			}
		}
	}

	// The quota account must equal a fresh recompute over the surviving roots.
	clean, err := artifactlog.NewPublisher(deps.ctx, deps.pool, deps.objects, config)
	if err != nil {
		t.Fatal(err)
	}
	var rootedBefore int64
	if err := deps.pool.QueryRow(deps.ctx, `SELECT rooted_bytes FROM artifact_quota_accounts WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&rootedBefore); err != nil {
		t.Fatal(err)
	}
	if err := clean.RecomputeQuota(deps.ctx, fence); err != nil {
		t.Fatal(err)
	}
	var rootedAfter int64
	if err := deps.pool.QueryRow(deps.ctx, `SELECT rooted_bytes FROM artifact_quota_accounts WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&rootedAfter); err != nil {
		t.Fatal(err)
	}
	if rootedAfter != rootedBefore {
		t.Fatalf("quota drifted from recompute: before=%d after=%d", rootedBefore, rootedAfter)
	}
}

func oneShotConsumerBoundary(target string) artifactlog.ConsumerOption {
	var once sync.Once
	return artifactlog.WithConsumerHooks(artifactlog.ConsumerHooks{
		Reach: func(_ context.Context, boundary string) error {
			if boundary != target {
				return nil
			}
			injected := false
			once.Do(func() { injected = true })
			if injected {
				return fmt.Errorf("injected process loss at %s", target)
			}
			return nil
		},
	})
}

func assertArtifactConsumerReceiptCount(t *testing.T, deps *artifactIntegrationDeps, fence authority.RunFence, consumerRevisionID string, want int) {
	t.Helper()
	var receipts, checkpoints int
	if err := deps.pool.QueryRow(deps.ctx, `SELECT count(*) FROM artifact_delivery_receipts WHERE flow_incarnation_id=$1 AND consumer_revision_id=$2`, fence.FlowIncarnationID, consumerRevisionID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != want {
		t.Fatalf("delivery receipts=%d, want %d", receipts, want)
	}
	if err := deps.pool.QueryRow(deps.ctx, `SELECT count(*) FROM artifact_consumer_checkpoints WHERE flow_incarnation_id=$1 AND consumer_revision_id=$2`, fence.FlowIncarnationID, consumerRevisionID).Scan(&checkpoints); err != nil {
		t.Fatal(err)
	}
	if checkpoints > 1 {
		t.Fatalf("consumer checkpoints=%d, want at most 1", checkpoints)
	}
}

type blockingPutStore struct {
	artifactlog.ObjectStore
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingPutStore) PutImmutable(ctx context.Context, key string, body []byte, digest, projectionID, mappingFingerprint string) (artifactlog.ObjectEvidence, error) {
	s.once.Do(func() { close(s.started) })
	select {
	case <-ctx.Done():
		return artifactlog.ObjectEvidence{}, ctx.Err()
	case <-s.release:
	}
	return s.ObjectStore.PutImmutable(ctx, key, body, digest, projectionID, mappingFingerprint)
}

type artifactIntegrationDeps struct {
	ctx       context.Context
	pool      *pgxpool.Pool
	engine    *workflow.PostgresEngine
	authority *authority.PostgresStore
	delivery  *delivery.Coordinator
	objects   *artifactlog.S3Store
}

func newArtifactIntegrationDeps(t *testing.T) *artifactIntegrationDeps {
	t.Helper()
	endpoint := os.Getenv("WALLABY_TEST_S3_ENDPOINT")
	bucket := os.Getenv("WALLABY_TEST_S3_BUCKET")
	accessKey := os.Getenv("WALLABY_TEST_S3_ACCESS_KEY")
	secretKey := os.Getenv("WALLABY_TEST_S3_SECRET_KEY")
	region := os.Getenv("WALLABY_TEST_S3_REGION")
	dsn := os.Getenv("TEST_PG_DSN")
	if endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" || dsn == "" {
		t.Skip("PostgreSQL and MinIO integration environment is required")
	}
	if region == "" {
		region = "us-east-1"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	client, err := newS3Client(ctx, endpoint, region, accessKey, secretKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if !errors.As(err, &owned) && !errors.As(err, &exists) {
			t.Fatal(err)
		}
	}
	if _, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatal(err)
	}
	objects, err := artifactlog.NewS3Store(ctx, artifactlog.S3Config{
		Bucket: bucket, Region: region, Endpoint: endpoint,
		AccessKey: accessKey, SecretKey: secretKey, ForcePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	checkpointStore, err := checkpoint.NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	checkpointStore.Close()
	deliveryCoordinator, err := delivery.NewCoordinator(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	return &artifactIntegrationDeps{ctx: ctx, pool: pool, engine: engine, authority: authorityStore, delivery: deliveryCoordinator, objects: objects}
}

func (d *artifactIntegrationDeps) newFence(t *testing.T, suffix string) authority.RunFence {
	t.Helper()
	flowID := fmt.Sprintf("artifact-%s-%d", suffix, time.Now().UnixNano())
	t.Cleanup(func() { cleanupAuthorityTest(context.Background(), d.pool, flowID) })
	if _, err := d.engine.Create(d.ctx, flow.Flow{ID: flowID, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}), Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{{Name: "target", Type: connector.EndpointPostgres}})}}); err != nil {
		t.Fatal(err)
	}
	_, control, err := d.engine.PlanStart(d.ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := d.authority.AcquireProducer(d.ctx, flowID, "artifact-worker", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return fence
}

func artifactTransactionAt(xid uint32, beginLSN, commitLSN, endLSN, value string) connector.SourceTransaction {
	transaction := artifactSourceTransaction()
	transaction.TransactionID = xid
	transaction.BeginLSN = beginLSN
	transaction.CommitLSN = commitLSN
	transaction.EndLSN = endLSN
	transaction.Checkpoint.LSN = endLSN
	transaction.Fragments[0].Batch.Records[0].SourcePosition = endLSN
	transaction.Fragments[0].Batch.Records[0].After = map[string]any{"id": int64(xid), "value": value}
	return transaction
}

func consumeArtifactPublication(ctx context.Context, pool *pgxpool.Pool, fence authority.RunFence, consumerRevisionID string) error {
	var publicationID string
	if err := pool.QueryRow(ctx, `
SELECT publication_id::text
FROM artifact_deliveries
WHERE flow_incarnation_id=$1 AND consumer_revision_id=$2 AND delivered_at IS NULL
ORDER BY sequence
LIMIT 1`, fence.FlowIncarnationID, consumerRevisionID).Scan(&publicationID); err != nil {
		return err
	}
	catalog := &recordingAppendCatalog{snapshotID: "snapshot-" + publicationID}
	consumer, err := artifactlog.NewConsumer(pool, catalog)
	if err != nil {
		return err
	}
	consumed, err := consumer.ConsumeNext(ctx, fence, consumerRevisionID)
	if err != nil {
		return err
	}
	if !consumed {
		return errors.New("artifact publication was not consumed")
	}
	return nil
}
