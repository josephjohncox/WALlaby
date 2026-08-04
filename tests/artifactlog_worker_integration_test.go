package tests

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestWallabyWorkerMaterializedPublicationRecovery(t *testing.T) {
	deps := newArtifactIntegrationDeps(t)
	workerBinary := os.Getenv("WALLABY_WORKER_BINARY")
	if workerBinary == "" {
		t.Fatal("WALLABY_WORKER_BINARY is required for materialized worker evidence")
	}
	dsn := os.Getenv("TEST_PG_DSN")
	t.Setenv("WALLABY_ARTIFACT_BUCKET", os.Getenv("WALLABY_TEST_S3_BUCKET"))
	t.Setenv("WALLABY_ARTIFACT_REGION", os.Getenv("WALLABY_TEST_S3_REGION"))
	t.Setenv("WALLABY_ARTIFACT_ENDPOINT", os.Getenv("WALLABY_TEST_S3_ENDPOINT"))
	t.Setenv("WALLABY_ARTIFACT_ACCESS_KEY", os.Getenv("WALLABY_TEST_S3_ACCESS_KEY"))
	t.Setenv("WALLABY_ARTIFACT_SECRET_KEY", os.Getenv("WALLABY_TEST_S3_SECRET_KEY"))
	t.Setenv("WALLABY_ARTIFACT_FORCE_PATH_STYLE", "true")

	ctx, cancel := context.WithTimeout(deps.ctx, 60*time.Second)
	defer cancel()
	flowID := "wallaby-worker-materialized-" + uuid.NewString()
	publication := "wallaby_worker_materialized_publication"
	if _, err := deps.pool.Exec(ctx, `
DROP PUBLICATION IF EXISTS wallaby_worker_materialized_publication;
DROP TABLE IF EXISTS public.wallaby_worker_materialized_source;
DROP TABLE IF EXISTS public.wallaby_worker_materialized_target;
CREATE TABLE public.wallaby_worker_materialized_source (id bigint PRIMARY KEY, value text);
CREATE TABLE public.wallaby_worker_materialized_target (id bigint PRIMARY KEY, value text);
INSERT INTO public.wallaby_worker_materialized_source VALUES (0,'snapshot');
CREATE PUBLICATION wallaby_worker_materialized_publication FOR TABLE public.wallaby_worker_materialized_source`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = deps.pool.Exec(context.Background(), `
DROP PUBLICATION IF EXISTS wallaby_worker_materialized_publication;
DROP TABLE IF EXISTS public.wallaby_worker_materialized_source;
DROP TABLE IF EXISTS public.wallaby_worker_materialized_target`)
	}()
	defer cleanupAuthorityTest(context.Background(), deps.pool, flowID)
	defer cleanupBootstrapSlotsForFlow(t, deps.pool, flowID)

	var sourceSystemID string
	if err := deps.pool.QueryRow(ctx, `SELECT system_identifier::text FROM pg_control_system()`).Scan(&sourceSystemID); err != nil {
		t.Fatal(err)
	}
	publicationRevision, err := pgsource.PublicationFingerprint(ctx, deps.pool, publication)
	if err != nil {
		t.Fatal(err)
	}
	destinationRevisionID := "wallaby-materialized-" + uuid.NewString()
	definition := flow.Flow{
		ID: flowID,
		Source: connector.Spec{Name: "source", Type: connector.EndpointPostgres, Options: map[string]string{
			"dsn": dsn, "publication": publication, "tables": "public.wallaby_worker_materialized_source",
			"ensure_publication": "false", "managed": "true", "bootstrap": "required",
			"streaming_transactions": "true", "status_interval": "10ms", "batch_timeout": "10ms",
			"ensure_state": "false", "source_system_identifier": sourceSystemID,
			"source_lineage_id":    sourceSystemID + ":" + publication + ":v1",
			"publication_revision": publicationRevision,
		}},
		Destinations: []connector.Spec{{Name: "target", Type: connector.EndpointPostgres, Options: map[string]string{
			"dsn": dsn, "schema": "public", "table": "wallaby_worker_materialized_target",
			"batch_mode": "target", "meta_table_enabled": "false",
			"synchronous_commit": "on", "destination_revision_id": destinationRevisionID,
		}}},
		Config: flow.Config{
			AckPolicy:       stream.AckPolicyMaterialized,
			Materialization: flow.MaterializationPolicy{ProjectionID: "canonical_cdc_parquet_v1"},
		},
	}
	if _, err := deps.engine.Create(ctx, definition); err != nil {
		t.Fatal(err)
	}
	_, control, err := deps.engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}

	first := startWorkerProcess(t, workerBinary, dsn, flowID, control.Generation, "materialized-first")
	defer first.stopAbruptly()
	var slotName string
	waitForWorkerProcessCondition(t, ctx, first, "materialized bootstrap handoff", func() (bool, error) {
		var phase string
		if err := deps.pool.QueryRow(ctx, `
SELECT phase,slot_name FROM source_bootstraps
WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)
ORDER BY bootstrap_generation DESC LIMIT 1`, flowID).Scan(&phase, &slotName); err != nil {
			return false, nil
		}
		return phase == "streaming", nil
	})
	waitForWorkerProcessCondition(t, ctx, first, "materialized startup-cut publication and source ACK", func() (bool, error) {
		var count int
		err := deps.pool.QueryRow(ctx, `
SELECT count(*)
FROM artifact_publications AS publication
JOIN source_ack_receipts AS receipt
  ON receipt.flow_incarnation_id=publication.flow_incarnation_id
 AND receipt.position_id=publication.position_id
WHERE publication.flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)
  AND NOT EXISTS (SELECT 1 FROM artifact_publication_objects AS root WHERE root.publication_id=publication.publication_id)
  AND NOT EXISTS (SELECT 1 FROM artifact_barriers AS barrier WHERE barrier.publication_id=publication.publication_id)`, flowID).Scan(&count)
		return count >= 1, err
	})
	if _, err := deps.pool.Exec(ctx, `INSERT INTO public.wallaby_worker_materialized_source VALUES (1,'first')`); err != nil {
		t.Fatal(err)
	}
	waitForWorkerProcessCondition(t, ctx, first, "first materialized publication and source ACK", func() (bool, error) {
		var count int
		err := deps.pool.QueryRow(ctx, `
SELECT count(*)
FROM artifact_publications AS publication
JOIN source_ack_receipts AS receipt
  ON receipt.flow_incarnation_id=publication.flow_incarnation_id
 AND receipt.position_id=publication.position_id
WHERE publication.flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)
  AND EXISTS (SELECT 1 FROM artifact_publication_objects AS root WHERE root.publication_id=publication.publication_id)`, flowID).Scan(&count)
		return count >= 1, err
	})
	var targetCount int
	if err := deps.pool.QueryRow(ctx, `SELECT count(*) FROM public.wallaby_worker_materialized_target WHERE id=1`).Scan(&targetCount); err != nil {
		t.Fatal(err)
	}
	if targetCount != 0 {
		t.Fatal("materialized CDC worker synchronously committed the downstream table")
	}
	var queuedDeliveries int
	if err := deps.pool.QueryRow(ctx, `
SELECT count(*) FROM artifact_deliveries
WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID).Scan(&queuedDeliveries); err != nil {
		t.Fatal(err)
	}
	if queuedDeliveries != 0 {
		t.Fatalf("production materialized worker queued %d destination deliveries without a consumer runtime", queuedDeliveries)
	}
	first.stopAbruptly()
	var expectedReserved, expectedRooted int64
	if err := deps.pool.QueryRow(ctx, `
SELECT
  COALESCE((SELECT sum(bytes) FROM artifact_quota_reservations
            WHERE flow_incarnation_id=account.flow_incarnation_id
              AND converted_at IS NULL AND released_at IS NULL),0),
  COALESCE((SELECT sum(object.encoded_length)
            FROM artifact_objects AS object
            JOIN artifact_publication_objects AS root ON root.artifact_id=object.artifact_id
            WHERE object.flow_incarnation_id=account.flow_incarnation_id
              AND root.released_at IS NULL),0)
FROM artifact_quota_accounts AS account
WHERE account.flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID).Scan(&expectedReserved, &expectedRooted); err != nil {
		t.Fatal(err)
	}
	if expectedRooted <= 0 {
		t.Fatalf("expected positive rooted bytes before quota recovery, got %d", expectedRooted)
	}
	if _, err := deps.pool.Exec(ctx, `
UPDATE artifact_quota_accounts
SET reserved_bytes=hard_limit_bytes,rooted_bytes=0
WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID); err != nil {
		t.Fatal(err)
	}
	if _, err := deps.pool.Exec(ctx, `
UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second'
WHERE incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID); err != nil {
		t.Fatal(err)
	}

	second := startWorkerProcess(t, workerBinary, dsn, flowID, control.Generation, "materialized-second")
	defer second.stopAbruptly()
	waitForWorkerProcessCondition(t, ctx, second, "PostgreSQL quota recovery before replacement source use", func() (bool, error) {
		var reserved, rooted int64
		err := deps.pool.QueryRow(ctx, `
SELECT reserved_bytes,rooted_bytes FROM artifact_quota_accounts
WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID).Scan(&reserved, &rooted)
		return reserved == expectedReserved && rooted == expectedRooted, err
	})
	waitForWorkerProcessCondition(t, ctx, second, "replacement materialized slot activation", func() (bool, error) {
		var active bool
		err := deps.pool.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name=$1`, slotName).Scan(&active)
		return active, err
	})
	if _, err := deps.pool.Exec(ctx, `INSERT INTO public.wallaby_worker_materialized_source VALUES (2,'second')`); err != nil {
		t.Fatal(err)
	}
	waitForWorkerProcessCondition(t, ctx, second, "replacement materialized publication and source ACK", func() (bool, error) {
		var count, invalidEvidence int
		if err := deps.pool.QueryRow(ctx, `
SELECT count(*),count(*) FILTER (WHERE object.version_id IS NULL OR object.state<>'rooted')
FROM artifact_publications AS publication
JOIN source_ack_receipts AS receipt
  ON receipt.flow_incarnation_id=publication.flow_incarnation_id
 AND receipt.position_id=publication.position_id
JOIN artifact_publication_objects AS root ON root.publication_id=publication.publication_id
JOIN artifact_objects AS object ON object.artifact_id=root.artifact_id
WHERE publication.flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID).Scan(&count, &invalidEvidence); err != nil {
			return false, err
		}
		return count >= 2 && invalidEvidence == 0, nil
	})
	second.stopAbruptly()
	if err := deps.pool.QueryRow(ctx, `SELECT count(*) FROM public.wallaby_worker_materialized_target WHERE id IN (1,2)`).Scan(&targetCount); err != nil {
		t.Fatal(err)
	}
	if targetCount != 0 {
		t.Fatalf("materialized CDC synchronously committed %d target rows", targetCount)
	}

	var evidence artifactlog.ObjectEvidence
	if err := deps.pool.QueryRow(ctx, `
SELECT object.bucket,object.object_key,object.version_id,object.checksum_sha256,
       object.encoded_length,object.encryption_mode,object.object_lock_evidence
FROM artifact_publications AS publication
JOIN artifact_publication_objects AS root ON root.publication_id=publication.publication_id
JOIN artifact_objects AS object ON object.artifact_id=root.artifact_id
WHERE publication.flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)
ORDER BY publication.sequence DESC,root.ordinal DESC
LIMIT 1`, flowID).Scan(
		&evidence.Bucket, &evidence.Key, &evidence.VersionID, &evidence.ChecksumSHA256,
		&evidence.Length, &evidence.EncryptionMode, &evidence.ObjectLock,
	); err != nil {
		t.Fatal(err)
	}
	var ackReceiptsBefore int
	var confirmedFlushBefore string
	if err := deps.pool.QueryRow(ctx, `
SELECT count(*) FROM source_ack_receipts
WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID).Scan(&ackReceiptsBefore); err != nil {
		t.Fatal(err)
	}
	if err := deps.pool.QueryRow(ctx, `SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1`, slotName).Scan(&confirmedFlushBefore); err != nil {
		t.Fatal(err)
	}
	if err := deps.objects.DeleteVersion(ctx, evidence); err != nil {
		t.Fatal(err)
	}
	if _, err := deps.pool.Exec(ctx, `
UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second'
WHERE incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID); err != nil {
		t.Fatal(err)
	}
	third := startWorkerProcess(t, workerBinary, dsn, flowID, control.Generation, "materialized-corrupt-root")
	select {
	case third.err = <-third.done:
		third.exited = true
		if third.err == nil {
			t.Fatalf("replacement worker accepted a missing rooted object version\n%s", third.output.String())
		}
		if !strings.Contains(third.output.String(), "restore canonical artifact checkpoint") {
			t.Fatalf("replacement worker failed for the wrong reason: %v\n%s", third.err, third.output.String())
		}
	case <-time.After(15 * time.Second):
		third.stopAbruptly()
		t.Fatalf("replacement worker did not fail closed on a missing rooted object version\n%s", third.output.String())
	}
	var ackReceiptsAfter int
	var confirmedFlushAfter string
	if err := deps.pool.QueryRow(ctx, `
SELECT count(*) FROM source_ack_receipts
WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID).Scan(&ackReceiptsAfter); err != nil {
		t.Fatal(err)
	}
	if err := deps.pool.QueryRow(ctx, `SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1`, slotName).Scan(&confirmedFlushAfter); err != nil {
		t.Fatal(err)
	}
	if ackReceiptsAfter != ackReceiptsBefore || confirmedFlushAfter != confirmedFlushBefore {
		t.Fatalf("corrupt-root restart advanced source feedback receipts/flush=%d/%s -> %d/%s", ackReceiptsBefore, confirmedFlushBefore, ackReceiptsAfter, confirmedFlushAfter)
	}
}
