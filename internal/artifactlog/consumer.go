package artifactlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// RootedArtifact is immutable, exact-version input to one asynchronous catalog
// delivery. Record ordinals let a consumer merge artifacts with ordered
// barriers without depending on object-list order.
type RootedArtifact struct {
	Evidence           ObjectEvidence
	ArtifactID         string
	LogicalBatchID     string
	Namespace          string
	Table              string
	SchemaID           string
	SchemaJSON         []byte
	EncodedByteHash    string
	FragmentOrdinal    uint64
	FirstRecordOrdinal uint64
	RecordCount        uint64
}

// ConsumerHooks exposes named deterministic crash boundaries without changing
// the production protocol.
type ConsumerHooks struct {
	Reach func(context.Context, string) error
}

// ConsumerOption configures optional consumer behavior.
type ConsumerOption func(*Consumer)

// WithConsumerHooks installs deterministic failure injection hooks.
func WithConsumerHooks(hooks ConsumerHooks) ConsumerOption {
	return func(consumer *Consumer) { consumer.hooks = hooks }
}

// Consumer delivers rooted publications through an append-only Iceberg catalog
// seam. It does not implement equality deletes, upserts, or compaction.
type Consumer struct {
	pool      *pgxpool.Pool
	committer ChangelogCommitter
	hooks     ConsumerHooks
}

type claimedPublication struct {
	request    CommitRequest
	attemptID  uuid.UUID
	hasAttempt bool
	claim      authority.ClaimFence
}

func NewConsumer(pool *pgxpool.Pool, committer ChangelogCommitter, options ...ConsumerOption) (*Consumer, error) {
	if pool == nil || committer == nil {
		return nil, errors.New("artifact consumer PostgreSQL pool and changelog committer are required")
	}
	consumer := &Consumer{pool: pool, committer: committer}
	for _, option := range options {
		option(consumer)
	}
	return consumer, nil
}

func (c *Consumer) reach(ctx context.Context, boundary string) error {
	if c.hooks.Reach == nil {
		return nil
	}
	return c.hooks.Reach(ctx, boundary)
}

// ConsumeNext processes one publication in PostgreSQL sequence order. Attempts
// are persisted before catalog I/O; ambiguous commits reconcile by exact
// publication/content identity or fail closed.
func (c *Consumer) ConsumeNext(ctx context.Context, fence authority.RunFence, consumerRevisionID string) (bool, error) {
	publication, err := c.loadNext(ctx, fence, consumerRevisionID)
	if err != nil {
		return false, err
	}
	if publication.request.PublicationID == uuid.Nil {
		telemetry.RecordArtifactConsumerOutcome(ctx, "idle_or_claimed")
		return false, nil
	}
	defer func() {
		store, storeErr := authority.NewPostgresStore(c.pool)
		if storeErr == nil {
			_ = store.ReleaseClaim(context.WithoutCancel(ctx), publication.claim)
		}
	}()

	if publication.hasAttempt {
		reconciliation, reconcileErr := c.committer.Reconcile(ctx, publication.request)
		if reconcileErr != nil {
			telemetry.RecordArtifactConsumerOutcome(ctx, "reconcile_failed")
			return false, reconcileErr
		}
		switch reconciliation.Disposition {
		case CommitApplied:
			if err := validateCommitResult(publication.request, reconciliation.Commit); err != nil {
				return false, err
			}
			if err := c.reach(ctx, "after_catalog_reconcile"); err != nil {
				return false, err
			}
			if err := c.finalize(ctx, fence, publication.request, publication.attemptID, reconciliation.Commit); err != nil {
				return false, err
			}
			telemetry.RecordArtifactConsumerOutcome(ctx, "reconciled")
			return true, c.reach(ctx, "after_consumer_receipt")
		case CommitIndeterminate:
			// The consumer halts on this publication until an operator resolves the
			// ambiguity; the delivery stays undelivered and retention pins its bytes.
			// Emit a distinct outcome so a stuck-indeterminate consumer is alertable
			// rather than hiding inside generic reconcile failures.
			telemetry.RecordArtifactConsumerOutcome(ctx, "indeterminate")
			return false, fmt.Errorf("%w: Iceberg publication %s", connector.ErrDeliveryIndeterminate, publication.request.PublicationID)
		case CommitNotApplied:
			// A fresh attempt with the same deterministic commit ID follows.
		default:
			return false, fmt.Errorf("unknown catalog reconciliation disposition %d", reconciliation.Disposition)
		}
	}

	attemptID := publication.attemptID
	if !publication.hasAttempt {
		var attemptedAt time.Time
		attemptID, attemptedAt, err = c.prepare(ctx, fence, publication.request)
		if err != nil {
			return false, err
		}
		publication.request.AttemptedAt = attemptedAt
	}
	commit, err := c.committer.Commit(ctx, publication.request)
	if err != nil {
		telemetry.RecordArtifactConsumerOutcome(ctx, "commit_failed")
		return false, err
	}
	if err := validateCommitResult(publication.request, commit); err != nil {
		return false, err
	}
	if err := c.reach(ctx, "after_catalog_commit"); err != nil {
		return false, err
	}
	if err := c.finalize(ctx, fence, publication.request, attemptID, commit); err != nil {
		telemetry.RecordArtifactConsumerOutcome(ctx, "finalize_failed")
		return false, err
	}
	telemetry.RecordArtifactConsumerOutcome(ctx, "committed")
	return true, c.reach(ctx, "after_consumer_receipt")
}

func ensurePublicationNotUnderMetadataRetention(ctx context.Context, tx pgx.Tx, publicationID uuid.UUID) error {
	var claimed bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
  SELECT 1 FROM artifact_metadata_prune_claims WHERE publication_id=$1
)`, publicationID).Scan(&claimed); err != nil {
		return err
	}
	if claimed {
		return fmt.Errorf("%w: artifact publication metadata is under authoritative retention", connector.ErrDeliveryConflict)
	}
	return nil
}

func validateCommitResult(request CommitRequest, commit CommitResult) error {
	if commit.SnapshotID == "" || commit.ManifestSHA256 == "" || commit.CommitID == "" || commit.LogicalBatchID == "" {
		return errors.New("catalog commit evidence is incomplete")
	}
	if commit.ManifestSHA256 != request.ManifestSHA256 || commit.CommitID != request.CommitID || commit.LogicalBatchID != request.LogicalBatchID {
		return fmt.Errorf("%w: catalog commit identity differs", connector.ErrDeliveryConflict)
	}
	return nil
}

func (c *Consumer) loadNext(ctx context.Context, fence authority.RunFence, consumerRevisionID string) (claimedPublication, error) {
	var result claimedPublication
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return result, err
	}
	result.request.FlowIncarnationID = fence.FlowIncarnationID
	result.request.ConsumerRevisionID = consumerRevisionID
	var publicationProjection, publicationMapping string
	err = tx.QueryRow(ctx, `
SELECT delivery.publication_id,stream.flow_id,publication.logical_batch_id,
       publication.sequence,publication.position_id,publication.checkpoint_lsn,
       stream.projection_id,stream.mapping_fingerprint,
       publication.projection_id,publication.mapping_fingerprint
FROM artifact_deliveries AS delivery
JOIN artifact_publications AS publication ON publication.publication_id=delivery.publication_id
JOIN artifact_streams AS stream ON stream.flow_incarnation_id=delivery.flow_incarnation_id
WHERE delivery.flow_incarnation_id=$1 AND delivery.consumer_revision_id=$2
  AND delivery.delivered_at IS NULL
ORDER BY delivery.sequence
LIMIT 1
FOR UPDATE OF delivery`, fence.FlowIncarnationID, consumerRevisionID).Scan(
		&result.request.PublicationID, &result.request.FlowID, &result.request.LogicalBatchID,
		&result.request.PublicationSequence, &result.request.PositionID, &result.request.CheckpointLSN,
		&result.request.ProjectionID, &result.request.MappingFingerprint, &publicationProjection, &publicationMapping,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return claimedPublication{}, nil
	}
	if err != nil {
		return result, err
	}
	if publicationProjection != result.request.ProjectionID || publicationMapping != result.request.MappingFingerprint {
		return result, fmt.Errorf("%w: artifact publication projection identity differs from stream", connector.ErrDeliveryConflict)
	}
	if err := ensurePublicationNotUnderMetadataRetention(ctx, tx, result.request.PublicationID); err != nil {
		return result, err
	}

	claimKind := authority.ClaimKind("artifact_delivery")
	workID := consumerRevisionID + ":" + result.request.PublicationID.String()
	claimEpoch := int64(1)
	var previousEpoch int64
	var live bool
	err = tx.QueryRow(ctx, `
SELECT claim.claim_epoch,
       claim.released_at IS NULL
       AND claim.claim_expires_at > clock_timestamp()
       AND producer.acquisition_id=claim.acquisition_id
       AND producer.lease_epoch=claim.lease_epoch
       AND producer.lease_expires_at > clock_timestamp()
FROM work_claims AS claim
LEFT JOIN producer_leases AS producer ON producer.incarnation_id=claim.incarnation_id
WHERE claim.incarnation_id=$1 AND claim.claim_kind=$2 AND claim.work_id=$3
FOR UPDATE OF claim`, fence.FlowIncarnationID, string(claimKind), workID).Scan(&previousEpoch, &live)
	switch {
	case err == nil && live:
		return claimedPublication{}, nil
	case err == nil:
		claimEpoch = previousEpoch + 1
	case !errors.Is(err, pgx.ErrNoRows):
		return result, err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO work_claims (
  incarnation_id,claim_kind,work_id,generation,acquisition_id,lease_epoch,
  claim_epoch,claim_expires_at,released_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,clock_timestamp()+interval '5 minutes',NULL)
ON CONFLICT (incarnation_id,claim_kind,work_id) DO UPDATE SET
  generation=EXCLUDED.generation,acquisition_id=EXCLUDED.acquisition_id,
  lease_epoch=EXCLUDED.lease_epoch,claim_epoch=EXCLUDED.claim_epoch,
  claim_expires_at=EXCLUDED.claim_expires_at,released_at=NULL,updated_at=clock_timestamp()`, fence.FlowIncarnationID, string(claimKind), workID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, claimEpoch); err != nil {
		return result, err
	}
	result.claim = authority.ClaimFence{RunFence: fence, Kind: claimKind, WorkID: workID, ClaimEpoch: claimEpoch}

	rows, err := tx.Query(ctx, `
SELECT object.bucket,object.object_key,object.version_id,object.checksum_sha256,
       object.encoded_length,object.encryption_mode,object.object_lock_evidence,
       object.artifact_id,object.encoded_byte_hash,object.logical_batch_id,
       object.namespace,object.table_name,object.schema_id,schema.schema_json,
       object.fragment_ordinal,object.first_record_ordinal,object.record_count,
       object.projection_id,object.mapping_fingerprint,
       schema.projection_id,schema.mapping_fingerprint
FROM artifact_publication_objects AS item
JOIN artifact_objects AS object ON object.artifact_id=item.artifact_id
JOIN canonical_schemas AS schema ON schema.schema_id=object.schema_id
WHERE item.publication_id=$1 AND item.release_marked_at IS NULL
  AND item.released_at IS NULL AND object.state='rooted'
ORDER BY item.ordinal`, result.request.PublicationID)
	if err != nil {
		return result, err
	}
	hash := sha256.New()
	for rows.Next() {
		var object RootedArtifact
		var fragmentOrdinal, firstRecordOrdinal, recordCount int64
		var objectProjectionID, objectMappingFingerprint, schemaProjectionID, schemaMappingFingerprint string
		if err := rows.Scan(
			&object.Evidence.Bucket, &object.Evidence.Key, &object.Evidence.VersionID,
			&object.Evidence.ChecksumSHA256, &object.Evidence.Length, &object.Evidence.EncryptionMode,
			&object.Evidence.ObjectLock, &object.ArtifactID, &object.EncodedByteHash,
			&object.LogicalBatchID, &object.Namespace, &object.Table, &object.SchemaID,
			&object.SchemaJSON, &fragmentOrdinal, &firstRecordOrdinal, &recordCount, &objectProjectionID, &objectMappingFingerprint, &schemaProjectionID, &schemaMappingFingerprint,
		); err != nil {
			rows.Close()
			return result, err
		}
		if objectProjectionID != result.request.ProjectionID || objectMappingFingerprint != result.request.MappingFingerprint || schemaProjectionID != result.request.ProjectionID || schemaMappingFingerprint != result.request.MappingFingerprint || objectProjectionID != schemaProjectionID || objectMappingFingerprint != schemaMappingFingerprint || object.LogicalBatchID != result.request.LogicalBatchID {
			rows.Close()
			return result, fmt.Errorf("%w: rooted artifact identity differs from publication", connector.ErrDeliveryConflict)
		}
		object.Evidence.ProjectionID = objectProjectionID
		object.Evidence.MappingFingerprint = objectMappingFingerprint
		if fragmentOrdinal < 0 || firstRecordOrdinal < 0 || recordCount <= 0 {
			rows.Close()
			return result, fmt.Errorf("rooted artifact %s has invalid ordinal metadata", object.ArtifactID)
		}
		object.FragmentOrdinal = uint64(fragmentOrdinal)       // #nosec G115 -- nonnegative BIGINT checked above.
		object.FirstRecordOrdinal = uint64(firstRecordOrdinal) // #nosec G115 -- nonnegative BIGINT checked above.
		object.RecordCount = uint64(recordCount)               // #nosec G115 -- positive BIGINT checked above.
		_, _ = hash.Write([]byte(object.ArtifactID))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(object.EncodedByteHash))
		_, _ = hash.Write([]byte{0})
		result.request.Objects = append(result.request.Objects, object)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return result, err
	}
	rows.Close()

	barrierRows, err := tx.Query(ctx, `
SELECT fragment_ordinal,record_ordinal,kind,namespace,table_name,
       COALESCE(schema_id,''),ddl,ddl_plan,content_hash
FROM artifact_barriers
WHERE publication_id=$1
ORDER BY record_ordinal`, result.request.PublicationID)
	if err != nil {
		return result, err
	}
	for barrierRows.Next() {
		var barrier Barrier
		var fragmentOrdinal, recordOrdinal int64
		if err := barrierRows.Scan(
			&fragmentOrdinal, &recordOrdinal, &barrier.Kind, &barrier.Namespace,
			&barrier.Table, &barrier.SchemaID, &barrier.DDL, &barrier.DDLPlan, &barrier.ContentHash,
		); err != nil {
			barrierRows.Close()
			return result, err
		}
		if fragmentOrdinal < 0 || recordOrdinal < 0 {
			barrierRows.Close()
			return result, errors.New("artifact barrier has invalid ordinal metadata")
		}
		barrier.FragmentOrdinal = uint64(fragmentOrdinal) // #nosec G115 -- nonnegative BIGINT checked above.
		barrier.RecordOrdinal = uint64(recordOrdinal)     // #nosec G115 -- nonnegative BIGINT checked above.
		_, _ = hash.Write([]byte("barrier"))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(barrier.ContentHash))
		_, _ = hash.Write([]byte{0})
		result.request.Barriers = append(result.request.Barriers, barrier)
	}
	if err := barrierRows.Err(); err != nil {
		barrierRows.Close()
		return result, err
	}
	barrierRows.Close()
	if len(result.request.Objects) == 0 && len(result.request.Barriers) == 0 {
		return result, errors.New("artifact publication has no rooted objects or barriers")
	}
	result.request.ManifestSHA256 = hex.EncodeToString(hash.Sum(nil))
	result.request.CommitID = DeterministicCommitID(
		result.request.FlowIncarnationID,
		result.request.ConsumerRevisionID,
		result.request.PublicationID,
		result.request.ManifestSHA256,
	)

	var storedManifestSHA256, storedLogicalBatchID string
	err = tx.QueryRow(ctx, `
SELECT attempt.attempt_id,attempt.commit_id,attempt.manifest_sha256,
       attempt.logical_batch_id,attempt.prepared_at
FROM artifact_delivery_attempts AS attempt
LEFT JOIN artifact_delivery_receipts AS receipt ON receipt.attempt_id=attempt.attempt_id
WHERE attempt.flow_incarnation_id=$1 AND attempt.consumer_revision_id=$2
  AND attempt.publication_id=$3 AND receipt.attempt_id IS NULL
ORDER BY attempt.prepared_at DESC,attempt.attempt_id DESC
LIMIT 1`, fence.FlowIncarnationID, consumerRevisionID, result.request.PublicationID).Scan(
		&result.attemptID, &result.request.CommitID, &storedManifestSHA256,
		&storedLogicalBatchID, &result.request.AttemptedAt,
	)
	result.hasAttempt = err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return result, err
	}
	if result.hasAttempt {
		expected := DeterministicCommitID(result.request.FlowIncarnationID, result.request.ConsumerRevisionID, result.request.PublicationID, result.request.ManifestSHA256)
		if result.request.CommitID != expected || storedManifestSHA256 != result.request.ManifestSHA256 || storedLogicalBatchID != result.request.LogicalBatchID {
			return result, fmt.Errorf("%w: prepared catalog commit identity differs", connector.ErrDeliveryConflict)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return result, err
	}
	return result, nil
}

func (c *Consumer) prepare(ctx context.Context, fence authority.RunFence, request CommitRequest) (uuid.UUID, time.Time, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, time.Time{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return uuid.Nil, time.Time{}, err
	}
	if err := ensurePublicationNotUnderMetadataRetention(ctx, tx, request.PublicationID); err != nil {
		return uuid.Nil, time.Time{}, err
	}
	attemptID := uuid.New()
	var preparedAt time.Time
	if err := tx.QueryRow(ctx, `
INSERT INTO artifact_delivery_attempts (
  attempt_id,flow_incarnation_id,consumer_revision_id,publication_id,
  generation,acquisition_id,lease_epoch,commit_id,manifest_sha256,logical_batch_id
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
RETURNING prepared_at`, attemptID, fence.FlowIncarnationID, request.ConsumerRevisionID, request.PublicationID,
		fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, request.CommitID,
		request.ManifestSHA256, request.LogicalBatchID).Scan(&preparedAt); err != nil {
		return uuid.Nil, time.Time{}, fmt.Errorf("prepare artifact consumer attempt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, time.Time{}, err
	}
	return attemptID, preparedAt, nil
}

func (c *Consumer) finalize(ctx context.Context, fence authority.RunFence, request CommitRequest, attemptID uuid.UUID, commit CommitResult) error {
	if err := validateCommitResult(request, commit); err != nil {
		return err
	}
	snapshotIDs, err := json.Marshal(commit.SnapshotIDs)
	if err != nil {
		return fmt.Errorf("encode catalog snapshot receipt: %w", err)
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	if err := ensurePublicationNotUnderMetadataRetention(ctx, tx, request.PublicationID); err != nil {
		return err
	}
	receiptTag, err := tx.Exec(ctx, `
INSERT INTO artifact_delivery_receipts (
  flow_incarnation_id,consumer_revision_id,publication_id,attempt_id,
  snapshot_id,content_hash,acquisition_id,lease_epoch,commit_id,
  logical_batch_id,publication_sequence,position_id,checkpoint_lsn,snapshot_ids
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
ON CONFLICT (flow_incarnation_id,consumer_revision_id,publication_id) DO UPDATE SET
  snapshot_id=EXCLUDED.snapshot_id
WHERE artifact_delivery_receipts.snapshot_id=EXCLUDED.snapshot_id
  AND artifact_delivery_receipts.snapshot_ids=EXCLUDED.snapshot_ids
  AND artifact_delivery_receipts.content_hash=EXCLUDED.content_hash
  AND artifact_delivery_receipts.commit_id=EXCLUDED.commit_id
  AND artifact_delivery_receipts.logical_batch_id=EXCLUDED.logical_batch_id
  AND artifact_delivery_receipts.publication_sequence=EXCLUDED.publication_sequence
  AND artifact_delivery_receipts.position_id=EXCLUDED.position_id
  AND artifact_delivery_receipts.checkpoint_lsn=EXCLUDED.checkpoint_lsn`,
		fence.FlowIncarnationID, request.ConsumerRevisionID, request.PublicationID, attemptID,
		commit.SnapshotID, commit.ManifestSHA256, fence.AcquisitionID, fence.LeaseEpoch,
		commit.CommitID, commit.LogicalBatchID, request.PublicationSequence, request.PositionID,
		request.CheckpointLSN, snapshotIDs)
	if err != nil {
		return fmt.Errorf("record artifact consumer receipt: %w", err)
	}
	if receiptTag.RowsAffected() != 1 {
		return fmt.Errorf("%w: artifact consumer receipt differs", connector.ErrDeliveryConflict)
	}

	var earlierPending bool
	if err := tx.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1 FROM artifact_deliveries
  WHERE flow_incarnation_id=$1 AND consumer_revision_id=$2
    AND sequence < $3 AND delivered_at IS NULL
)`, fence.FlowIncarnationID, request.ConsumerRevisionID, request.PublicationSequence).Scan(&earlierPending); err != nil {
		return err
	}
	if earlierPending {
		return fmt.Errorf("%w: consumer checkpoint would skip an earlier publication", connector.ErrCheckpointRegression)
	}

	tag, err := tx.Exec(ctx, `
UPDATE artifact_deliveries
SET delivered_at=clock_timestamp()
WHERE flow_incarnation_id=$1 AND consumer_revision_id=$2 AND publication_id=$3 AND delivered_at IS NULL`,
		fence.FlowIncarnationID, request.ConsumerRevisionID, request.PublicationID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("artifact delivery is no longer pending")
	}

	checkpointTag, err := tx.Exec(ctx, `
INSERT INTO artifact_consumer_checkpoints (
  flow_incarnation_id,consumer_revision_id,publication_sequence,publication_id,
  position_id,checkpoint_lsn,commit_id,snapshot_id
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (flow_incarnation_id,consumer_revision_id) DO UPDATE SET
  publication_sequence=EXCLUDED.publication_sequence,
  publication_id=EXCLUDED.publication_id,
  position_id=EXCLUDED.position_id,
  checkpoint_lsn=EXCLUDED.checkpoint_lsn,
  commit_id=EXCLUDED.commit_id,
  snapshot_id=EXCLUDED.snapshot_id,
  advanced_at=clock_timestamp()
WHERE artifact_consumer_checkpoints.publication_sequence < EXCLUDED.publication_sequence`,
		fence.FlowIncarnationID, request.ConsumerRevisionID, request.PublicationSequence,
		request.PublicationID, request.PositionID, request.CheckpointLSN, request.CommitID,
		commit.SnapshotID)
	if err != nil {
		return fmt.Errorf("advance artifact consumer checkpoint: %w", err)
	}
	if checkpointTag.RowsAffected() != 1 {
		return fmt.Errorf("%w: artifact consumer checkpoint did not advance", connector.ErrCheckpointRegression)
	}
	return tx.Commit(ctx)
}
