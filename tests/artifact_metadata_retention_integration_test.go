package tests

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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
	const consumerCount = 3
	runtimeConfig := artifactlog.RuntimeConfig{
		Stream: artifactlog.StreamConfig{
			ProjectionID: artifactlog.ProjectionIDV2, MappingFingerprint: projector.Fingerprint(),
			HardRetainedBytes: 128 << 20, BacklogCountHigh: 100,
			BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour,
		},
		Projector: projector, OrphanGrace: time.Hour, Retention: time.Hour,
		MetadataRetention: 7 * 24 * time.Hour, MetadataMaxPublications: 2,
		MetadataMaxRows: 3, GCInterval: time.Hour,
		Consumers: []artifactlog.CatalogConsumerConfig{
			{RevisionID: "ice-retention-v1", Committer: committer},
			{RevisionID: "ice-retention-v2", Committer: committer},
			{RevisionID: "ice-retention-v3", Committer: committer},
		},
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
	var liveSchemaID string
	if err := pool.QueryRow(ctx, `SELECT object.schema_id FROM artifact_publication_objects AS root JOIN artifact_publications AS publication ON publication.publication_id=root.publication_id JOIN artifact_objects AS object ON object.artifact_id=root.artifact_id WHERE publication.flow_incarnation_id=$1 ORDER BY publication.sequence DESC,root.ordinal LIMIT 1`, fence.FlowIncarnationID).Scan(&liveSchemaID); err != nil {
		t.Fatal(err)
	}
	obsoleteSchemaID := "obsolete-" + uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO canonical_schemas(schema_id,projection_id,schema_json,mapping_fingerprint) SELECT $2,projection_id,schema_json,mapping_fingerprint FROM canonical_schemas WHERE schema_id=$1`, liveSchemaID, obsoleteSchemaID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE artifact_objects SET schema_id=$2 WHERE artifact_id=(SELECT root.artifact_id FROM artifact_publication_objects AS root JOIN artifact_publications AS publication ON publication.publication_id=root.publication_id WHERE publication.flow_incarnation_id=$1 ORDER BY publication.sequence,root.ordinal LIMIT 1)`, fence.FlowIncarnationID, obsoleteSchemaID); err != nil {
		t.Fatal(err)
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
	if publicationsBefore != 6 || objectsBefore == 0 || rootsBefore == 0 || deliveriesBefore != 6*consumerCount || attemptsBefore != 6*consumerCount || receiptsBefore != 6*consumerCount || schemasBefore == 0 {
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
	if _, err := admissionPruner.Prune(ctx, fence, 7*24*time.Hour, 2, 3); err == nil || !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("tampered receipt identity error=%v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE artifact_delivery_receipts AS receipt SET content_hash=attempt.manifest_sha256 FROM artifact_delivery_attempts AS attempt WHERE receipt.attempt_id=$1 AND attempt.attempt_id=receipt.attempt_id`, tamperedAttempt); err != nil {
		t.Fatal(err)
	}
	blockedStats, err := admissionPruner.Prune(ctx, fence, 7*24*time.Hour, 2, 3)
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
	if _, err := crashPruner.Prune(ctx, fence, 7*24*time.Hour, 2, 3); err == nil {
		t.Fatal("metadata claim crash was not injected")
	}
	var afterCrashPublications, afterCrashClaims int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM artifact_publications WHERE flow_incarnation_id=$1),(SELECT count(*) FROM artifact_metadata_prune_claims WHERE flow_incarnation_id=$1)`, fence.FlowIncarnationID).Scan(&afterCrashPublications, &afterCrashClaims); err != nil {
		t.Fatal(err)
	}
	if afterCrashPublications != 6 || afterCrashClaims != 1 {
		t.Fatalf("durable claim crash publications/claims=%d/%d, want 6/1", afterCrashPublications, afterCrashClaims)
	}
	var claimedPublication uuid.UUID
	var evidenceVersion, evidenceConsumers int
	var evidenceComplete bool
	if err := pool.QueryRow(ctx, `SELECT publication_id,(catalog_evidence->>'version')::int,jsonb_array_length(catalog_evidence->'consumers'),
  COALESCE((SELECT bool_and(value ? 'delivery' AND value ? 'attempt' AND value ? 'receipt'
    AND value->'delivery' ? 'consumer_revision_id'
    AND value->'attempt' ? 'attempt_id' AND value->'attempt' ? 'commit_id' AND value->'attempt' ? 'manifest_sha256'
    AND value->'receipt' ? 'attempt_id' AND value->'receipt' ? 'commit_id' AND value->'receipt' ? 'snapshot_ids')
    FROM jsonb_array_elements(catalog_evidence->'consumers')),false)
FROM artifact_metadata_prune_claims WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&claimedPublication, &evidenceVersion, &evidenceConsumers, &evidenceComplete); err != nil {
		t.Fatal(err)
	}
	if evidenceVersion != 1 || evidenceConsumers != consumerCount || !evidenceComplete {
		t.Fatalf("frozen catalog tombstone version/consumers/complete=%d/%d/%v, want 1/%d/true", evidenceVersion, evidenceConsumers, evidenceComplete, consumerCount)
	}
	if _, err := pool.Exec(ctx, `UPDATE artifact_deliveries SET bytes=bytes WHERE publication_id=$1`, claimedPublication); err == nil || !strings.Contains(err.Error(), "under authoritative retention") {
		t.Fatalf("claimed publication accepted new/mutated catalog dependent: %v", err)
	}
	var claimedArtifact string
	if err := pool.QueryRow(ctx, `SELECT artifact_ids->>0 FROM artifact_metadata_prune_claims WHERE publication_id=$1`, claimedPublication).Scan(&claimedArtifact); err != nil {
		t.Fatal(err)
	}
	assertRevalidationDefers := func(name string, install, remove func() error) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			time.Sleep(2 * time.Millisecond)
			installed := false
			racePruner, err := artifactlog.NewMetadataPruner(pool, artifactlog.WithMetadataPrunerHooks(artifactlog.MetadataPrunerHooks{Boundary: func(_ context.Context, boundary string, publicationID uuid.UUID) error {
				if boundary == "before_metadata_revalidation" && publicationID == claimedPublication && !installed {
					installed = true
					return install()
				}
				return nil
			}}))
			if err != nil {
				t.Fatal(err)
			}
			stats, err := racePruner.Prune(ctx, fence, 7*24*time.Hour, 1, 3)
			if err != nil {
				t.Fatal(err)
			}
			if !installed || stats.PublicationsDeferred != 1 || stats.RowsDeleted != 0 {
				t.Fatalf("stale revalidation was destructive: installed=%v stats=%+v", installed, stats)
			}
			if err := remove(); err != nil {
				t.Fatal(err)
			}
		})
	}
	var currentSourceLSN string
	var currentSourceMetadata []byte
	if err := pool.QueryRow(ctx, `SELECT lsn,metadata FROM authoritative_checkpoints WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&currentSourceLSN, &currentSourceMetadata); err != nil {
		t.Fatal(err)
	}
	assertRevalidationDefers("source checkpoint reroot", func() error {
		_, err := pool.Exec(ctx, `UPDATE authoritative_checkpoints SET lsn=(SELECT checkpoint_lsn FROM artifact_publications WHERE publication_id=$2),metadata=jsonb_set(metadata,'{artifact_publication_id}',to_jsonb($2::text)) WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID, claimedPublication)
		return err
	}, func() error {
		_, err := pool.Exec(ctx, `UPDATE authoritative_checkpoints SET lsn=$2,metadata=$3::jsonb WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID, currentSourceLSN, currentSourceMetadata)
		return err
	})
	assertRevalidationDefers("consumer checkpoint reroot", func() error {
		_, err := pool.Exec(ctx, `UPDATE artifact_consumer_checkpoints AS checkpoint SET publication_id=publication.publication_id,publication_sequence=publication.sequence,position_id=publication.position_id,checkpoint_lsn=publication.checkpoint_lsn FROM artifact_publications AS publication WHERE publication.publication_id=$1 AND checkpoint.flow_incarnation_id=$2`, claimedPublication, fence.FlowIncarnationID)
		return err
	}, func() error {
		_, err := pool.Exec(ctx, `WITH latest AS (
  SELECT * FROM artifact_publications WHERE flow_incarnation_id=$1 ORDER BY sequence DESC LIMIT 1
) UPDATE artifact_consumer_checkpoints AS checkpoint SET
  publication_id=latest.publication_id,publication_sequence=latest.sequence,
  position_id=latest.position_id,checkpoint_lsn=latest.checkpoint_lsn,
  commit_id=receipt.commit_id,snapshot_id=receipt.snapshot_id
FROM latest JOIN artifact_delivery_receipts AS receipt ON receipt.publication_id=latest.publication_id
WHERE checkpoint.flow_incarnation_id=$1 AND receipt.consumer_revision_id=checkpoint.consumer_revision_id`, fence.FlowIncarnationID)
		return err
	})
	workID := "retention-race:" + claimedPublication.String()
	assertRevalidationDefers("live work claim", func() error {
		_, err := pool.Exec(ctx, `INSERT INTO work_claims(incarnation_id,claim_kind,work_id,generation,acquisition_id,lease_epoch,claim_epoch,claim_expires_at) VALUES($1,'artifact_delivery',$2,$3,$4,$5,1,clock_timestamp()+interval '5 minutes')`, fence.FlowIncarnationID, workID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch)
		return err
	}, func() error {
		_, err := pool.Exec(ctx, `DELETE FROM work_claims WHERE incarnation_id=$1 AND claim_kind='artifact_delivery' AND work_id=$2`, fence.FlowIncarnationID, workID)
		return err
	})
	assertRevalidationDefers("GC claim", func() error {
		_, err := pool.Exec(ctx, `INSERT INTO artifact_gc_claims(artifact_id,claim_epoch,generation,acquisition_id,lease_epoch,claim_kind,publication_id) VALUES($1,1,$2,$3,$4,'retention',$5)`, claimedArtifact, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, claimedPublication)
		return err
	}, func() error {
		_, err := pool.Exec(ctx, `DELETE FROM artifact_gc_claims WHERE artifact_id=$1`, claimedArtifact)
		return err
	})
	assertRevalidationDefers("prepared upload", func() error {
		_, err := pool.Exec(ctx, `UPDATE artifact_upload_attempts SET attempt_state='prepared',completed_at=NULL WHERE artifact_id=$1`, claimedArtifact)
		return err
	}, func() error {
		_, err := pool.Exec(ctx, `UPDATE artifact_upload_attempts SET attempt_state='verified',completed_at=clock_timestamp() WHERE artifact_id=$1`, claimedArtifact)
		return err
	})
	assertRevalidationDefers("active quota", func() error {
		_, err := pool.Exec(ctx, `UPDATE artifact_quota_reservations SET converted_at=NULL,released_at=NULL WHERE artifact_id=$1`, claimedArtifact)
		return err
	}, func() error {
		_, err := pool.Exec(ctx, `UPDATE artifact_quota_reservations SET converted_at=clock_timestamp(),released_at=NULL WHERE artifact_id=$1`, claimedArtifact)
		return err
	})
	type takeoverResult struct {
		fence authority.RunFence
		err   error
	}
	takeoverDone := make(chan takeoverResult, 1)
	time.Sleep(2 * time.Millisecond)
	takeoverInstalled := false
	takeoverPruner, err := artifactlog.NewMetadataPruner(pool, artifactlog.WithMetadataPrunerHooks(artifactlog.MetadataPrunerHooks{Boundary: func(_ context.Context, boundary string, publicationID uuid.UUID) error {
		if boundary != "before_metadata_revalidation" || publicationID != claimedPublication || takeoverInstalled {
			return nil
		}
		takeoverInstalled = true
		go func() {
			takeoverCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if _, err := pool.Exec(takeoverCtx, `UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE incarnation_id=$1`, fence.FlowIncarnationID); err != nil {
				takeoverDone <- takeoverResult{err: err}
				return
			}
			newFence, err := authorityStore.AcquireProducer(takeoverCtx, flowID, "artifact-metadata-retention-takeover", "test", control.Generation, 5*time.Minute)
			takeoverDone <- takeoverResult{fence: newFence, err: err}
		}()
		select {
		case result := <-takeoverDone:
			return fmt.Errorf("takeover crossed the active metadata authority lock: %v", result.err)
		case <-time.After(20 * time.Millisecond):
			return errors.New("injected rollback after proving takeover serialization")
		}
	}}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := takeoverPruner.Prune(ctx, fence, 7*24*time.Hour, 1, 3); err == nil || !strings.Contains(err.Error(), "proving takeover serialization") {
		t.Fatalf("takeover serialization boundary error=%v", err)
	}
	var takeover takeoverResult
	select {
	case takeover = <-takeoverDone:
	case <-time.After(30 * time.Second):
		t.Fatal("takeover did not complete after metadata transaction released its authority lock")
	}
	if !takeoverInstalled || takeover.err != nil || takeover.fence.AcquisitionID == uuid.Nil {
		t.Fatalf("fence takeover result installed=%v fence=%+v err=%v", takeoverInstalled, takeover.fence, takeover.err)
	}
	if _, err := admissionPruner.Prune(ctx, fence, 7*24*time.Hour, 1, 3); err == nil || !errors.Is(err, authority.ErrFenceRejected) {
		t.Fatalf("old metadata fence remained usable after takeover: %v", err)
	}
	fence = takeover.fence
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
	if _, err := partialPruner.Prune(ctx, fence, 7*24*time.Hour, 2, 3); err == nil {
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
	if _, err := rollbackPruner.Prune(ctx, fence, 7*24*time.Hour, 2, 3); err == nil {
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
		stats, err := pruner.Prune(ctx, fence, 7*24*time.Hour, 2, 3)
		if err != nil {
			t.Fatal(err)
		}
		if stats.PublicationsScanned > 2 || stats.RowsDeleted > 3 {
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
	var obsoleteExists, liveExists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM canonical_schemas WHERE schema_id=$1),EXISTS(SELECT 1 FROM canonical_schemas WHERE schema_id=$2)`, obsoleteSchemaID, liveSchemaID).Scan(&obsoleteExists, &liveExists); err != nil {
		t.Fatal(err)
	}
	if obsoleteExists || !liveExists {
		t.Fatalf("canonical schema pruning obsolete/live=%v/%v, want false/true", obsoleteExists, liveExists)
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

	t.Run("committed metrics survive a later claim failure", func(t *testing.T) {
		// Create three fresh publications so two can become terminal while the
		// newest remains the authoritative checkpoint root.
		for index := 0; index < 3; index++ {
			base := 0x500 + index*0x20
			transaction := artifactTransactionAt(uint32(2000+index), lsnForTest(base), lsnForTest(base+8), lsnForTest(base+16), "metric-partial-failure")
			if _, err := fresh.Append(ctx, fence, transaction, managedBaselinePayload(t, transaction)); err != nil {
				t.Fatal(err)
			}
		}
		if err := fresh.Recover(ctx, fence); err != nil {
			t.Fatalf("deliver metric publications: %v", err)
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

		reader := sdkmetric.NewManualReader()
		meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		defer func() { _ = meterProvider.Shutdown(context.Background()) }()
		metricRecorder, err := telemetry.NewArtifactMetadataPruneRecorder(meterProvider.Meter("wallaby/durable"))
		if err != nil {
			t.Fatal(err)
		}

		injected := errors.New("injected failure after first committed metadata claim")
		claimed := make([]uuid.UUID, 0, 2)
		metricPruner, err := artifactlog.NewMetadataPruner(
			pool,
			artifactlog.WithMetadataPrunerStatsRecorder(func(ctx context.Context, stats artifactlog.MetadataPruneStats) {
				metricRecorder.Record(ctx, int64(stats.PublicationsScanned), int64(stats.PublicationsDeleted), int64(stats.PublicationsDeferred), int64(stats.RowsDeleted))
			}),
			artifactlog.WithMetadataPrunerHooks(artifactlog.MetadataPrunerHooks{Boundary: func(_ context.Context, boundary string, publicationID uuid.UUID) error {
				if boundary != "after_metadata_claim" {
					return nil
				}
				claimed = append(claimed, publicationID)
				if len(claimed) == 2 {
					return injected
				}
				return nil
			}}),
		)
		if err != nil {
			t.Fatal(err)
		}
		stats, pruneErr := metricPruner.Prune(ctx, fence, 7*24*time.Hour, 2, 1000)
		if !errors.Is(pruneErr, injected) {
			t.Fatalf("later claim failure=%v, want injected error", pruneErr)
		}
		if len(claimed) != 2 || stats.PublicationsScanned != 2 || stats.PublicationsDeleted != 1 || stats.PublicationsDeferred != 0 || stats.RowsDeleted <= 0 {
			t.Fatalf("committed first claim stats=%+v claimed=%v", stats, claimed)
		}
		var firstExists, secondClaimExists bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM artifact_publications WHERE publication_id=$1),EXISTS(SELECT 1 FROM artifact_metadata_prune_claims WHERE publication_id=$2)`, claimed[0], claimed[1]).Scan(&firstExists, &secondClaimExists); err != nil {
			t.Fatal(err)
		}
		if firstExists || !secondClaimExists {
			t.Fatalf("claim commit boundary first_exists=%v second_claim_exists=%v", firstExists, secondClaimExists)
		}

		var metrics metricdata.ResourceMetrics
		if err := reader.Collect(ctx, &metrics); err != nil {
			t.Fatal(err)
		}
		outcomes := map[string]int64{}
		var rows int64
		for _, scope := range metrics.ScopeMetrics {
			for _, measurement := range scope.Metrics {
				sum, ok := measurement.Data.(metricdata.Sum[int64])
				if !ok {
					continue
				}
				for _, point := range sum.DataPoints {
					switch measurement.Name {
					case "wallaby.artifact.metadata_retention.publications":
						if value, ok := point.Attributes.Value(attribute.Key("outcome")); ok {
							outcomes[value.AsString()] += point.Value
						}
					case "wallaby.artifact.metadata_retention.rows":
						rows += point.Value
					}
				}
			}
		}
		if outcomes["scanned"] != int64(stats.PublicationsScanned) || outcomes["deleted"] != int64(stats.PublicationsDeleted) || outcomes["deferred"] != int64(stats.PublicationsDeferred) || rows != int64(stats.RowsDeleted) {
			t.Fatalf("deferred-path metrics outcomes=%v rows=%d stats=%+v", outcomes, rows, stats)
		}
	})
}

func lsnForTest(value int) string {
	const hex = "0123456789ABCDEF"
	if value < 0x100 || value > 0xFFF {
		panic("test LSN out of range")
	}
	return "0/" + string([]byte{hex[(value>>8)&0xF], hex[(value>>4)&0xF], hex[value&0xF]})
}
