package artifactlog

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/telemetry"
)

// Collector deletes only unpublished, unrooted exact object versions. Rooted
// publication retention remains deliberately unsupported.
type Collector struct {
	pool    *pgxpool.Pool
	objects ObjectStore
}

func NewCollector(pool *pgxpool.Pool, objects ObjectStore) (*Collector, error) {
	if pool == nil || objects == nil {
		return nil, errors.New("artifact GC PostgreSQL pool and object store are required")
	}
	return &Collector{pool: pool, objects: objects}, nil
}

// CollectOne claims and deletes one old uploaded/verified orphan. Reserved
// objects with no exact VersionId are left for reconciliation rather than
// guessed from S3 listing.
func (c *Collector) CollectOne(ctx context.Context, fence authority.RunFence, olderThan time.Duration) (bool, error) {
	if olderThan <= 0 {
		return false, errors.New("artifact GC grace period must be positive")
	}
	artifactID, evidence, claimEpoch, err := c.claim(ctx, fence, olderThan)
	if err != nil || artifactID == "" {
		return false, err
	}
	if err := c.objects.DeleteVersion(ctx, evidence); err != nil {
		telemetry.RecordArtifactGCOutcome(ctx, "delete_failed")
		return false, err
	}
	if err := c.finalize(ctx, fence, artifactID, claimEpoch); err != nil {
		telemetry.RecordArtifactGCOutcome(ctx, "finalize_failed")
		return false, err
	}
	telemetry.RecordArtifactGCOutcome(ctx, "deleted")
	return true, nil
}

func (c *Collector) claim(ctx context.Context, fence authority.RunFence, olderThan time.Duration) (string, ObjectEvidence, int64, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return "", ObjectEvidence{}, 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return "", ObjectEvidence{}, 0, err
	}
	var artifactID, state string
	var evidence ObjectEvidence
	err = tx.QueryRow(ctx, `
SELECT object.artifact_id,object.state,object.bucket,object.object_key,
       object.version_id,object.checksum_sha256,object.encoded_length,
       object.encryption_mode,object.object_lock_evidence
FROM artifact_objects AS object
WHERE object.flow_incarnation_id=$1
  AND object.state IN ('uploaded','verified','deleting')
  AND object.updated_at < clock_timestamp()-$2::interval
  AND NOT EXISTS (
    SELECT 1 FROM artifact_publication_objects AS root
    WHERE root.artifact_id=object.artifact_id
  )
ORDER BY object.updated_at,object.artifact_id
LIMIT 1
FOR UPDATE OF object SKIP LOCKED`, fence.FlowIncarnationID, olderThan.String()).Scan(
		&artifactID,
		&state,
		&evidence.Bucket,
		&evidence.Key,
		&evidence.VersionID,
		&evidence.ChecksumSHA256,
		&evidence.Length,
		&evidence.EncryptionMode,
		&evidence.ObjectLock,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ObjectEvidence{}, 0, nil
	}
	if err != nil {
		return "", ObjectEvidence{}, 0, err
	}
	var priorEpoch int64
	err = tx.QueryRow(ctx, `SELECT claim_epoch FROM artifact_gc_claims WHERE artifact_id=$1 FOR UPDATE`, artifactID).Scan(&priorEpoch)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", ObjectEvidence{}, 0, err
	}
	claimEpoch := priorEpoch + 1
	if _, err := tx.Exec(ctx, `
INSERT INTO artifact_gc_claims(artifact_id,claim_epoch,generation,acquisition_id,lease_epoch)
VALUES($1,$2,$3,$4,$5)
ON CONFLICT (artifact_id) DO UPDATE SET
  claim_epoch=EXCLUDED.claim_epoch,generation=EXCLUDED.generation,
  acquisition_id=EXCLUDED.acquisition_id,lease_epoch=EXCLUDED.lease_epoch,
  claimed_at=clock_timestamp()`, artifactID, claimEpoch, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch); err != nil {
		return "", ObjectEvidence{}, 0, err
	}
	tag, err := tx.Exec(ctx, `
UPDATE artifact_objects SET state='deleting',updated_at=clock_timestamp()
WHERE artifact_id=$1 AND flow_incarnation_id=$2 AND state IN ('uploaded','verified','deleting')
  AND NOT EXISTS (SELECT 1 FROM artifact_publication_objects WHERE artifact_id=$1)`, artifactID, fence.FlowIncarnationID)
	if err != nil {
		return "", ObjectEvidence{}, 0, fmt.Errorf("claim artifact GC object %s: %w", artifactID, err)
	}
	if tag.RowsAffected() != 1 {
		return "", ObjectEvidence{}, 0, fmt.Errorf("claim artifact GC object %s: affected=%d", artifactID, tag.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		return "", ObjectEvidence{}, 0, err
	}
	return artifactID, evidence, claimEpoch, nil
}

func (c *Collector) finalize(ctx context.Context, fence authority.RunFence, artifactID string, claimEpoch int64) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	var claimAcquisition uuid.UUID
	var claimGeneration, claimLeaseEpoch, storedEpoch int64
	if err := tx.QueryRow(ctx, `
SELECT claim_epoch,generation,acquisition_id,lease_epoch
FROM artifact_gc_claims WHERE artifact_id=$1 FOR UPDATE`, artifactID).Scan(&storedEpoch, &claimGeneration, &claimAcquisition, &claimLeaseEpoch); err != nil {
		return err
	}
	if storedEpoch != claimEpoch || claimGeneration != fence.Generation || claimAcquisition != fence.AcquisitionID || claimLeaseEpoch != fence.LeaseEpoch {
		return fmt.Errorf("%w: artifact GC claim changed", authority.ErrFenceRejected)
	}
	tag, err := tx.Exec(ctx, `
UPDATE artifact_objects SET state='deleted',updated_at=clock_timestamp()
WHERE artifact_id=$1 AND flow_incarnation_id=$2 AND state='deleting'
  AND NOT EXISTS (SELECT 1 FROM artifact_publication_objects WHERE artifact_id=$1)`, artifactID, fence.FlowIncarnationID)
	if err != nil {
		return fmt.Errorf("finalize artifact deletion %s: %w", artifactID, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("finalize artifact deletion %s: affected=%d", artifactID, tag.RowsAffected())
	}
	var releasedBytes int64
	err = tx.QueryRow(ctx, `
UPDATE artifact_quota_reservations
SET released_at=clock_timestamp()
WHERE artifact_id=$1 AND converted_at IS NULL AND released_at IS NULL
RETURNING bytes`, artifactID).Scan(&releasedBytes)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if err == nil {
		tag, err = tx.Exec(ctx, `
UPDATE artifact_quota_accounts
SET reserved_bytes=reserved_bytes-$2,updated_at=clock_timestamp()
WHERE flow_incarnation_id=$1 AND reserved_bytes >= $2`, fence.FlowIncarnationID, releasedBytes)
		if err != nil {
			return fmt.Errorf("release artifact quota %s: %w", artifactID, err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("release artifact quota %s: affected=%d", artifactID, tag.RowsAffected())
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM artifact_gc_claims WHERE artifact_id=$1 AND claim_epoch=$2`, artifactID, claimEpoch); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
