package tests

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/tablemap"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestArtifactMetadataRetentionBoundsConvergesAndPreservesCurrentRecovery(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
	if store, err := checkpoint.NewPostgresStore(ctx, dsn); err != nil {
		t.Fatal(err)
	} else {
		store.Close()
	}
	if _, err := delivery.NewCoordinator(ctx, pool); err != nil {
		t.Fatal(err)
	}
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	flowID := "artifact-metadata-retention-" + uuid.NewString()
	defer cleanupAuthorityTest(context.Background(), pool, flowID)
	destination := connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(destination), Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{destination})}}); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := authorityStore.AcquireProducer(ctx, flowID, "artifact-metadata-retention", "test", control.Generation, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	mappings := flow.NewTableMappings([]connector.RuntimeSpec{{Name: "ice", Type: connector.EndpointIceberg}})
	projector, err := tablemap.New(mappings, "ice")
	if err != nil {
		t.Fatal(err)
	}
	objects := &metadataArtifactStore{objects: map[string]metadataArtifactObject{}}
	committer := &catalogAttemptTestCommitter{}
	runtimeConfig := artifactlog.RuntimeConfig{
		Stream: artifactlog.StreamConfig{
			ProjectionID: artifactlog.ProjectionIDV2, MappingFingerprint: projector.Fingerprint(),
			HardRetainedBytes: 128 << 20, BacklogCountHigh: 100,
			BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour,
		},
		Projector: projector, OrphanGrace: time.Hour, Retention: time.Hour,
		MetadataRetention: 7 * 24 * time.Hour, MetadataMaxPublications: 2,
		MetadataMaxRows: 8, GCInterval: time.Hour,
		Consumers:              []artifactlog.CatalogConsumerConfig{{RevisionID: "ice-retention-v1", Committer: committer}},
		DestinationFingerprint: "retention-destination-v1",
	}
	runtime, err := artifactlog.NewRuntime(ctx, pool, objects, runtimeConfig)
	if err != nil {
		t.Fatal(err)
	}
	var currentGrant connector.AckGrant
	var currentTransaction connector.SourceTransaction
	for index := 0; index < 6; index++ {
		base := 0x100 + index*0x20
		transaction := artifactTransactionAt(uint32(1000+index), lsnForTest(base), lsnForTest(base+8), lsnForTest(base+16), "filtered")
		currentTransaction = transaction
		currentGrant, err = runtime.Append(ctx, fence, transaction, managedBaselinePayload(t, transaction))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := runtime.Recover(ctx, fence); err != nil {
		t.Fatalf("deliver catalog publications before retention: %v", err)
	}
	var publicationsBefore, objectsBefore, rootsBefore, deliveriesBefore, attemptsBefore, receiptsBefore, schemasBefore int
	if err := pool.QueryRow(ctx, `SELECT
 (SELECT count(*) FROM artifact_publications WHERE flow_incarnation_id=$1),
 (SELECT count(*) FROM artifact_objects WHERE flow_incarnation_id=$1),
 (SELECT count(*) FROM artifact_publication_objects AS root JOIN artifact_publications AS publication ON publication.publication_id=root.publication_id WHERE publication.flow_incarnation_id=$1),
 (SELECT count(*) FROM artifact_deliveries WHERE flow_incarnation_id=$1),
 (SELECT count(*) FROM artifact_delivery_attempts WHERE flow_incarnation_id=$1),
 (SELECT count(*) FROM artifact_delivery_receipts WHERE flow_incarnation_id=$1),
 (SELECT count(DISTINCT object.schema_id) FROM artifact_objects AS object WHERE object.flow_incarnation_id=$1)`, fence.FlowIncarnationID).Scan(&publicationsBefore, &objectsBefore, &rootsBefore, &deliveriesBefore, &attemptsBefore, &receiptsBefore, &schemasBefore); err != nil {
		t.Fatal(err)
	}
	if publicationsBefore != 6 || objectsBefore == 0 || rootsBefore == 0 || deliveriesBefore != 6 || attemptsBefore != 6 || receiptsBefore != 6 || schemasBefore == 0 {
		t.Fatalf("metadata graph is vacuous: publications=%d objects=%d roots=%d deliveries=%d attempts=%d receipts=%d schemas=%d", publicationsBefore, objectsBefore, rootsBefore, deliveriesBefore, attemptsBefore, receiptsBefore, schemasBefore)
	}
	admissionPruner, err := artifactlog.NewMetadataPruner(pool)
	if err != nil {
		t.Fatal(err)
	}
	var tamperedAttempt uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT attempt_id FROM artifact_delivery_receipts WHERE flow_incarnation_id=$1 ORDER BY publication_sequence LIMIT 1`, fence.FlowIncarnationID).Scan(&tamperedAttempt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE artifact_delivery_receipts SET content_hash=$2 WHERE attempt_id=$1`, tamperedAttempt, strings.Repeat("f", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := admissionPruner.Prune(ctx, fence, 7*24*time.Hour, 2, 8); err == nil || !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("tampered receipt identity error=%v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE artifact_delivery_receipts AS receipt SET content_hash=attempt.manifest_sha256 FROM artifact_delivery_attempts AS attempt WHERE receipt.attempt_id=$1 AND attempt.attempt_id=receipt.attempt_id`, tamperedAttempt); err != nil {
		t.Fatal(err)
	}
	blockedStats, err := admissionPruner.Prune(ctx, fence, 7*24*time.Hour, 2, 8)
	if err != nil {
		t.Fatal(err)
	}
	if blockedStats != (artifactlog.MetadataPruneStats{}) {
		t.Fatalf("fresh horizon/active roots did not block retention: %+v", blockedStats)
	}
	for _, statement := range []string{
		`UPDATE artifact_publications SET published_at=clock_timestamp()-interval '8 days' WHERE flow_incarnation_id=$1`,
		`UPDATE artifact_publication_objects SET release_marked_at=clock_timestamp()-interval '8 days',released_at=clock_timestamp()-interval '8 days' WHERE publication_id IN (SELECT publication_id FROM artifact_publications WHERE flow_incarnation_id=$1 AND sequence<(SELECT max(sequence) FROM artifact_publications WHERE flow_incarnation_id=$1))`,
		`UPDATE artifact_objects SET state='deleted',updated_at=clock_timestamp()-interval '8 days' WHERE artifact_id IN (SELECT root.artifact_id FROM artifact_publication_objects AS root JOIN artifact_publications AS publication ON publication.publication_id=root.publication_id WHERE publication.flow_incarnation_id=$1 AND publication.sequence<(SELECT max(sequence) FROM artifact_publications WHERE flow_incarnation_id=$1))`,
		`UPDATE source_ack_retention_roots SET released_at=clock_timestamp()-interval '8 days' WHERE flow_incarnation_id=$1 AND root_kind='artifact_publication' AND root_id IN (SELECT publication_id::text FROM artifact_publications WHERE flow_incarnation_id=$1 AND sequence<(SELECT max(sequence) FROM artifact_publications WHERE flow_incarnation_id=$1))`,
		`UPDATE artifact_deliveries SET delivered_at=clock_timestamp()-interval '8 days' WHERE flow_incarnation_id=$1`,
		`UPDATE artifact_delivery_receipts SET committed_at=clock_timestamp()-interval '8 days' WHERE flow_incarnation_id=$1`,
	} {
		if _, err := pool.Exec(ctx, statement, fence.FlowIncarnationID); err != nil {
			t.Fatal(err)
		}
	}
	crashPruner, err := artifactlog.NewMetadataPruner(pool, artifactlog.WithMetadataPrunerHooks(artifactlog.MetadataPrunerHooks{Boundary: func(_ context.Context, boundary string, _ uuid.UUID) error {
		if boundary == "after_metadata_claim" {
			return errors.New("injected crash after durable metadata claim")
		}
		return nil
	}}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := crashPruner.Prune(ctx, fence, 7*24*time.Hour, 2, 8); err == nil {
		t.Fatal("metadata claim crash was not injected")
	}
	var afterCrashPublications, afterCrashClaims int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM artifact_publications WHERE flow_incarnation_id=$1),(SELECT count(*) FROM artifact_metadata_prune_claims WHERE flow_incarnation_id=$1)`, fence.FlowIncarnationID).Scan(&afterCrashPublications, &afterCrashClaims); err != nil {
		t.Fatal(err)
	}
	if afterCrashPublications != 6 || afterCrashClaims != 1 {
		t.Fatalf("durable claim crash publications/claims=%d/%d, want 6/1", afterCrashPublications, afterCrashClaims)
	}
	partialCrash := true
	partialPruner, err := artifactlog.NewMetadataPruner(pool, artifactlog.WithMetadataPrunerHooks(artifactlog.MetadataPrunerHooks{Boundary: func(_ context.Context, boundary string, _ uuid.UUID) error {
		if boundary == "after_metadata_partial_phase" && partialCrash {
			partialCrash = false
			return errors.New("injected crash after metadata partial phase")
		}
		return nil
	}}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partialPruner.Prune(ctx, fence, 7*24*time.Hour, 2, 8); err == nil {
		t.Fatal("metadata partial-phase crash was not injected")
	}
	commitCrash := true
	rollbackPruner, err := artifactlog.NewMetadataPruner(pool, artifactlog.WithMetadataPrunerHooks(artifactlog.MetadataPrunerHooks{Boundary: func(_ context.Context, boundary string, _ uuid.UUID) error {
		if boundary == "before_metadata_commit" && commitCrash {
			commitCrash = false
			return errors.New("injected crash before metadata commit")
		}
		return nil
	}}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rollbackPruner.Prune(ctx, fence, 7*24*time.Hour, 2, 8); err == nil {
		t.Fatal("metadata pre-commit crash was not injected")
	}
	var attemptsAfterRollback, receiptsAfterRollback int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM artifact_delivery_attempts WHERE flow_incarnation_id=$1),(SELECT count(*) FROM artifact_delivery_receipts WHERE flow_incarnation_id=$1)`, fence.FlowIncarnationID).Scan(&attemptsAfterRollback, &receiptsAfterRollback); err != nil {
		t.Fatal(err)
	}
	if attemptsAfterRollback != attemptsBefore || receiptsAfterRollback != receiptsBefore {
		t.Fatalf("rollback removed reconciliation evidence: attempts=%d/%d receipts=%d/%d", attemptsAfterRollback, attemptsBefore, receiptsAfterRollback, receiptsBefore)
	}
	pruner, err := artifactlog.NewMetadataPruner(pool)
	if err != nil {
		t.Fatal(err)
	}
	metadataRows := func() int {
		t.Helper()
		var count int
		if err := pool.QueryRow(ctx, `SELECT
 (SELECT count(*) FROM canonical_schemas)+
 (SELECT count(*) FROM artifact_publications WHERE flow_incarnation_id=$1)+
 (SELECT count(*) FROM artifact_objects WHERE flow_incarnation_id=$1)+
 (SELECT count(*) FROM artifact_upload_attempts AS upload JOIN artifact_objects AS object ON object.artifact_id=upload.artifact_id WHERE object.flow_incarnation_id=$1)+
 (SELECT count(*) FROM artifact_publication_objects AS root JOIN artifact_publications AS publication ON publication.publication_id=root.publication_id WHERE publication.flow_incarnation_id=$1)+
 (SELECT count(*) FROM artifact_barriers AS barrier JOIN artifact_publications AS publication ON publication.publication_id=barrier.publication_id WHERE publication.flow_incarnation_id=$1)+
 (SELECT count(*) FROM artifact_deliveries WHERE flow_incarnation_id=$1)+
 (SELECT count(*) FROM artifact_delivery_attempts WHERE flow_incarnation_id=$1)+
 (SELECT count(*) FROM artifact_delivery_receipts WHERE flow_incarnation_id=$1)+
 (SELECT count(*) FROM artifact_quota_reservations WHERE flow_incarnation_id=$1)+
 (SELECT count(*) FROM source_ack_retention_roots WHERE flow_incarnation_id=$1 AND root_kind='artifact_publication')+
 (SELECT count(*) FROM artifact_metadata_prune_claims WHERE flow_incarnation_id=$1)`, fence.FlowIncarnationID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	for sweep := 0; sweep < 40; sweep++ {
		time.Sleep(2 * time.Millisecond)
		beforeRows := metadataRows()
		stats, err := pruner.Prune(ctx, fence, 7*24*time.Hour, 2, 8)
		if err != nil {
			t.Fatal(err)
		}
		if stats.PublicationsScanned > 2 || stats.RowsDeleted > 8 {
			t.Fatalf("sweep %d exceeded bounds: %+v", sweep, stats)
		}
		afterRows := metadataRows()
		committedDelta := beforeRows - afterRows
		if committedDelta > stats.RowsDeleted || stats.RowsDeleted-committedDelta > stats.PublicationsScanned {
			t.Fatalf("sweep %d reported deletions=%d, committed delta=%d, scanned claims=%d", sweep, stats.RowsDeleted, committedDelta, stats.PublicationsScanned)
		}
		var publications, claims int
		if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM artifact_publications WHERE flow_incarnation_id=$1),(SELECT count(*) FROM artifact_metadata_prune_claims WHERE flow_incarnation_id=$1)`, fence.FlowIncarnationID).Scan(&publications, &claims); err != nil {
			t.Fatal(err)
		}
		if publications == 1 && claims == 0 {
			break
		}
		if sweep == 39 {
			t.Fatalf("metadata did not converge: publications=%d claims=%d", publications, claims)
		}
	}
	fresh, err := artifactlog.NewRuntime(ctx, pool, objects, runtimeConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := fresh.Recover(ctx, fence); err != nil {
		t.Fatalf("fresh runtime recovery after pruning: %v", err)
	}
	if _, err := fresh.RestoreCheckpoint(ctx, fence, currentGrant.Checkpoint); err != nil {
		t.Fatalf("current checkpoint recovery after pruning: %v", err)
	}
	putsBeforeReplay := objects.puts
	if _, err := fresh.Append(ctx, fence, currentTransaction, managedBaselinePayload(t, currentTransaction)); err != nil {
		t.Fatalf("replay current transaction after pruning: %v", err)
	}
	if objects.puts != putsBeforeReplay {
		t.Fatalf("replay duplicated immutable object: puts=%d before=%d", objects.puts, putsBeforeReplay)
	}
}

func lsnForTest(value int) string {
	const hex = "0123456789ABCDEF"
	if value < 0x100 || value > 0xFFF {
		panic("test LSN out of range")
	}
	return "0/" + string([]byte{hex[(value>>8)&0xF], hex[(value>>4)&0xF], hex[value&0xF]})
}
