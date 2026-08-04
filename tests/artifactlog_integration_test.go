package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestCanonicalArtifactS3AdmissionRequiresEnabledVersioning(t *testing.T) {
	endpoint := os.Getenv("WALLABY_TEST_S3_ENDPOINT")
	accessKey := os.Getenv("WALLABY_TEST_S3_ACCESS_KEY")
	secretKey := os.Getenv("WALLABY_TEST_S3_SECRET_KEY")
	region := os.Getenv("WALLABY_TEST_S3_REGION")
	if endpoint == "" || accessKey == "" || secretKey == "" {
		t.Skip("S3 integration environment is required")
	}
	if region == "" {
		region = "us-east-1"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := newS3Client(ctx, endpoint, region, accessKey, secretKey)
	if err != nil {
		t.Fatal(err)
	}
	bucket := "wallaby-v-" + strings.ToLower(uuid.NewString())
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})
	config := artifactlog.S3Config{
		Bucket: bucket, Region: region, Endpoint: endpoint,
		AccessKey: accessKey, SecretKey: secretKey, ForcePathStyle: true,
	}
	if _, err := artifactlog.NewS3Store(ctx, config); err == nil {
		t.Fatal("unversioned bucket was admitted for immutable artifacts")
	}
	if _, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := artifactlog.NewS3Store(ctx, config); err != nil {
		t.Fatalf("enabled versioning was rejected: %v", err)
	}
	if _, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := artifactlog.NewS3Store(ctx, config); err == nil {
		t.Fatal("versioning-suspended bucket was admitted for immutable artifacts")
	}
}

func TestCanonicalArtifactPublicationRecovery(t *testing.T) {
	endpoint := os.Getenv("WALLABY_TEST_S3_ENDPOINT")
	bucket := os.Getenv("WALLABY_TEST_S3_BUCKET")
	accessKey := os.Getenv("WALLABY_TEST_S3_ACCESS_KEY")
	secretKey := os.Getenv("WALLABY_TEST_S3_SECRET_KEY")
	region := os.Getenv("WALLABY_TEST_S3_REGION")
	dsn := os.Getenv("TEST_PG_DSN")
	if endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" || dsn == "" {
		t.Skip("PostgreSQL and S3 integration environment is required")
	}
	if region == "" {
		region = "us-east-1"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
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
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
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
	checkpointStore.Close()
	if _, err := delivery.NewCoordinator(ctx, pool); err != nil {
		t.Fatal(err)
	}
	flowID := fmt.Sprintf("artifact-publication-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID}); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := authorityStore.AcquireProducer(ctx, flowID, "artifact-worker", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	transaction := artifactSourceTransaction()
	config := artifactlog.StreamConfig{
		HardRetainedBytes: 128 << 20,
		BacklogCountHigh:  100,
		BacklogBytesHigh:  128 << 20,
		Consumers:         []string{"iceberg-append-v1"},
	}
	failingObjects := &failBeforePutStore{ObjectStore: objects, fail: true}
	publisher, err := artifactlog.NewPublisher(ctx, pool, failingObjects, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(ctx, fence, transaction); err == nil {
		t.Fatal("expected injected failure after durable quota reservation and before upload")
	}
	var reservedBefore int64
	if err := pool.QueryRow(ctx, `SELECT reserved_bytes FROM artifact_quota_accounts WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&reservedBefore); err != nil {
		t.Fatal(err)
	}
	if reservedBefore <= 0 {
		t.Fatalf("reserved bytes=%d, want durable positive reservation after crash", reservedBefore)
	}

	publisher, err = artifactlog.NewPublisher(ctx, pool, objects, config)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := publisher.Publish(ctx, fence, transaction)
	if err != nil {
		t.Fatal(err)
	}
	if publication.AckGrant.Checkpoint.LSN != transaction.EndLSN || len(publication.Artifacts) != 1 {
		t.Fatalf("publication=(checkpoint:%s artifacts:%d), want %s/1", publication.AckGrant.Checkpoint.LSN, len(publication.Artifacts), transaction.EndLSN)
	}
	artifact := publication.Artifacts[0]
	var state, versionID string
	var encodedLength, rootedBytes, reservedBytes int64
	if err := pool.QueryRow(ctx, `SELECT state,version_id,encoded_length FROM artifact_objects WHERE artifact_id=$1`, artifact.ID).Scan(&state, &versionID, &encodedLength); err != nil {
		t.Fatal(err)
	}
	if versionID == "" || state != "rooted" {
		t.Fatalf("artifact state/version=(%s,%q), want rooted/exact version", state, versionID)
	}
	if err := pool.QueryRow(ctx, `SELECT rooted_bytes,reserved_bytes FROM artifact_quota_accounts WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&rootedBytes, &reservedBytes); err != nil {
		t.Fatal(err)
	}
	if rootedBytes != encodedLength || reservedBytes != 0 {
		t.Fatalf("quota rooted/reserved=(%d,%d), want (%d,0)", rootedBytes, reservedBytes, encodedLength)
	}
	var deliveryCount, ackCount, checkpointCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM artifact_deliveries WHERE flow_incarnation_id=$1 AND consumer_revision_id='iceberg-append-v1'`, fence.FlowIncarnationID).Scan(&deliveryCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_ack_intents WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&ackCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM authoritative_checkpoints WHERE flow_incarnation_id=$1 AND lsn=$2`, fence.FlowIncarnationID, transaction.EndLSN).Scan(&checkpointCount); err != nil {
		t.Fatal(err)
	}
	if deliveryCount != 1 || ackCount != 1 || checkpointCount != 1 {
		t.Fatalf("publication roots delivery/ack/checkpoint=(%d,%d,%d), want 1/1/1", deliveryCount, ackCount, checkpointCount)
	}

	catalog := &recordingAppendCatalog{snapshotID: "snapshot-1"}
	consumer, err := artifactlog.NewConsumer(pool, catalog, artifactlog.WithConsumerHooks(artifactlog.ConsumerHooks{
		Reach: func(_ context.Context, boundary string) error {
			if boundary == "after_catalog_commit" {
				return errors.New("injected crash after Iceberg commit before PostgreSQL receipt")
			}
			return nil
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if consumed, err := consumer.ConsumeNext(ctx, fence, "iceberg-append-v1"); err == nil || consumed {
		t.Fatalf("commit-before-receipt injection consumed/error=%t/%v", consumed, err)
	}
	consumer, err = artifactlog.NewConsumer(pool, catalog)
	if err != nil {
		t.Fatal(err)
	}
	consumed, err := consumer.ConsumeNext(ctx, fence, "iceberg-append-v1")
	if err != nil {
		t.Fatal(err)
	}
	if !consumed || catalog.appendCalls != 1 {
		t.Fatalf("Iceberg reconciliation consumed=%t append calls=%d, want true/1", consumed, catalog.appendCalls)
	}
	consumed, err = consumer.ConsumeNext(ctx, fence, "iceberg-append-v1")
	if err != nil {
		t.Fatal(err)
	}
	if consumed {
		t.Fatal("artifact consumer replayed an already receipted publication")
	}
	var consumerReceipts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM artifact_delivery_receipts WHERE flow_incarnation_id=$1 AND publication_id=$2`, fence.FlowIncarnationID, publication.ID).Scan(&consumerReceipts); err != nil {
		t.Fatal(err)
	}
	if consumerReceipts != 1 {
		t.Fatalf("artifact consumer receipts=%d, want 1", consumerReceipts)
	}
	var consumerSequence int64
	var consumerPosition, consumerCommitID string
	if err := pool.QueryRow(ctx, `
SELECT publication_sequence,position_id,commit_id
FROM artifact_consumer_checkpoints
WHERE flow_incarnation_id=$1 AND consumer_revision_id=$2`, fence.FlowIncarnationID, "iceberg-append-v1").Scan(
		&consumerSequence, &consumerPosition, &consumerCommitID,
	); err != nil {
		t.Fatal(err)
	}
	if consumerSequence != publication.Sequence || consumerPosition != publication.AckGrant.PositionID || !strings.HasPrefix(consumerCommitID, "wallaby-iceberg-") {
		t.Fatalf("consumer checkpoint=%d/%s/%s", consumerSequence, consumerPosition, consumerCommitID)
	}

	evidence := artifactlog.ObjectEvidence{
		Bucket: bucket, Key: artifact.ObjectKey, VersionID: versionID,
		ChecksumSHA256: artifact.EncodedByteHash, Length: encodedLength,
	}
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(artifact.ObjectKey)}); err != nil {
		t.Fatal(err)
	}
	if _, err := objects.HeadVersion(ctx, evidence); err != nil {
		t.Fatalf("exact rooted version was hidden by a later delete marker: %v", err)
	}
	if err := publisher.RecomputeQuota(ctx, fence); err != nil {
		t.Fatal(err)
	}
	var recomputedRooted, recomputedReserved int64
	if err := pool.QueryRow(ctx, `SELECT rooted_bytes,reserved_bytes FROM artifact_quota_accounts WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&recomputedRooted, &recomputedReserved); err != nil {
		t.Fatal(err)
	}
	if recomputedRooted != rootedBytes || recomputedReserved != 0 {
		t.Fatalf("recomputed quota=(%d,%d), want (%d,0)", recomputedRooted, recomputedReserved, rootedBytes)
	}
	if err := objects.DeleteVersion(ctx, evidence); err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(ctx, fence, transaction); err == nil {
		t.Fatal("retry unexpectedly authorized ACK after rooted exact object version was deleted")
	}
}

type recordingAppendCatalog struct {
	snapshotID   string
	appendCalls  int
	objectCount  int
	barrierCount int
}

func (c *recordingAppendCatalog) Commit(_ context.Context, request artifactlog.CommitRequest) (artifactlog.CommitResult, error) {
	if len(request.Objects) == 0 && len(request.Barriers) == 0 {
		return artifactlog.CommitResult{}, errors.New("commit received no rooted objects or barriers")
	}
	c.appendCalls++
	c.objectCount += len(request.Objects)
	c.barrierCount += len(request.Barriers)
	return c.result(request), nil
}

func (c *recordingAppendCatalog) Reconcile(_ context.Context, request artifactlog.ReconcileRequest) (artifactlog.ReconcileResult, error) {
	return artifactlog.ReconcileResult{Disposition: artifactlog.CommitApplied, Commit: c.result(request)}, nil
}

func (c *recordingAppendCatalog) result(request artifactlog.CommitRequest) artifactlog.CommitResult {
	snapshotID := c.snapshotID
	if snapshotID == "" {
		snapshotID = "snapshot-" + request.PublicationID.String()
	}
	return artifactlog.CommitResult{
		SnapshotID: snapshotID, SnapshotIDs: map[string]string{"test": snapshotID},
		ManifestSHA256: request.ManifestSHA256, CommitID: request.CommitID,
		LogicalBatchID: request.LogicalBatchID,
	}
}

type failBeforePutStore struct {
	artifactlog.ObjectStore
	fail bool
}

func (s *failBeforePutStore) PutImmutable(ctx context.Context, key string, body []byte, digest string) (artifactlog.ObjectEvidence, error) {
	if s.fail {
		return artifactlog.ObjectEvidence{}, errors.New("injected crash before immutable PUT")
	}
	return s.ObjectStore.PutImmutable(ctx, key, body, digest)
}

func artifactSourceTransaction() connector.SourceTransaction {
	return connector.SourceTransaction{
		SourceLineageID: "postgres-system-1/artifact-publication-v1",
		TransactionID:   100,
		BeginLSN:        "0/C0",
		CommitLSN:       "0/C8",
		EndLSN:          "0/D0",
		Checkpoint:      connector.Checkpoint{LSN: "0/D0", Timestamp: time.Unix(1000, 0).UTC()},
		Fragments: []connector.TransactionFragment{{
			Ordinal: 0,
			Batch: connector.Batch{
				Schema: connector.Schema{Namespace: "public", Name: "artifact_events", Version: 1, Columns: []connector.Column{
					{Name: "id", Type: "int8", TypeMetadata: map[string]string{"source_relation_id": "84", "source_column_id": "1"}},
					{Name: "value", Type: "text", TypeMetadata: map[string]string{"source_relation_id": "84", "source_column_id": "2"}},
				}},
				Records: []connector.Record{{Table: "artifact_events", Operation: connector.OpInsert, SchemaVersion: 1, Key: []byte(`{"id":1}`), After: map[string]any{"id": int64(1), "value": "canonical"}, Timestamp: time.Unix(999, 0).UTC()}},
			},
		}},
	}
}
