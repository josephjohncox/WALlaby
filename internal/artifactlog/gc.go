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

// CollectorHooks exposes deterministic mark/delete/finalize crash boundaries.
type CollectorHooks struct {
	Boundary func(context.Context, string) error
}

// CollectorOption configures optional collector behavior.
type CollectorOption func(*Collector)

// WithCollectorHooks installs deterministic failure injection hooks.
func WithCollectorHooks(hooks CollectorHooks) CollectorOption {
	return func(collector *Collector) { collector.hooks = hooks }
}

// Collector performs PostgreSQL-rooted mark/sweep. Ordinary S3 contains only
// immutable versions; PostgreSQL claims and active publication roots decide
// whether an exact version may be removed.
type Collector struct {
	pool    *pgxpool.Pool
	objects ObjectStore
	hooks   CollectorHooks
}

func NewCollector(pool *pgxpool.Pool, objects ObjectStore, options ...CollectorOption) (*Collector, error) {
	if pool == nil || objects == nil {
		return nil, errors.New("artifact collector requires PostgreSQL and object storage")
	}
	collector := &Collector{pool: pool, objects: objects}
	for _, option := range options {
		option(collector)
	}
	return collector, nil
}

type collectionClaim struct {
	artifactID    string
	publicationID uuid.UUID
	kind          string
	claimEpoch    int64
	evidence      ObjectEvidence
}

// CollectOne claims and deletes one old unpublished orphan. A reserved intent
// without exact-version evidence is eligible only when no prepared PUT attempt
// remains; prepared attempts stay charged until publisher replay reconciles
// them. Conclusive absence then releases the reservation, while a conflicting
// version fails closed.
func (c *Collector) CollectOne(ctx context.Context, fence authority.RunFence, olderThan time.Duration) (bool, error) {
	if olderThan <= 0 {
		return false, errors.New("artifact GC grace period must be positive")
	}
	claim, ok, err := c.claimOrphan(ctx, fence, olderThan)
	if err != nil || !ok {
		return false, err
	}
	return c.sweep(ctx, fence, claim)
}

// CollectRetainedOne marks and sweeps one publication object only after its
// source ACK receipt exists, every delivery is receipted, it is older than the
// retention period, and it is no longer the current checkpoint root.
func (c *Collector) CollectRetainedOne(ctx context.Context, fence authority.RunFence, retention time.Duration) (bool, error) {
	if retention <= 0 {
		return false, errors.New("artifact retention period must be positive")
	}
	claim, ok, err := c.claimRetention(ctx, fence, retention)
	if err != nil || !ok {
		return false, err
	}
	return c.sweep(ctx, fence, claim)
}

func (c *Collector) reach(ctx context.Context, boundary string) error {
	if c.hooks.Boundary == nil {
		return nil
	}
	if err := c.hooks.Boundary(ctx, boundary); err != nil {
		return fmt.Errorf("artifact GC boundary %s: %w", boundary, err)
	}
	return nil
}

func (c *Collector) sweep(ctx context.Context, fence authority.RunFence, claim collectionClaim) (bool, error) {
	if err := c.reach(ctx, "after_gc_mark"); err != nil {
		return false, err
	}
	if claim.evidence.VersionID == "" {
		evidence, err := c.objects.ReconcileVersion(ctx, claim.evidence.Key, claim.evidence.ChecksumSHA256, claim.evidence.Length)
		switch {
		case err == nil:
			if err := c.adoptClaimEvidence(ctx, fence, claim, evidence); err != nil {
				return false, err
			}
			claim.evidence = evidence
		case errors.Is(err, ErrObjectNotFound):
			// A conditional single-part PUT is either complete or absent. Exact
			// absence is the only case in which a reserved intent may be released
			// without an S3 delete.
		default:
			telemetry.RecordArtifactGCOutcome(ctx, "reconcile_failed")
			return false, err
		}
	}
	if claim.evidence.VersionID != "" {
		if err := c.objects.DeleteVersion(ctx, claim.evidence); err != nil {
			telemetry.RecordArtifactGCOutcome(ctx, "delete_failed")
			return false, err
		}
	}
	if err := c.reach(ctx, "after_gc_delete"); err != nil {
		return false, err
	}
	if err := c.finalize(ctx, fence, claim); err != nil {
		telemetry.RecordArtifactGCOutcome(ctx, "finalize_failed")
		return false, err
	}
	telemetry.RecordArtifactGCOutcome(ctx, "deleted_"+claim.kind)
	return true, nil
}

func (c *Collector) claimOrphan(ctx context.Context, fence authority.RunFence, olderThan time.Duration) (collectionClaim, bool, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return collectionClaim{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return collectionClaim{}, false, err
	}
	var claim collectionClaim
	claim.kind = "orphan"
	var versionID *string
	err = tx.QueryRow(ctx, `
SELECT object.artifact_id,object.bucket,object.object_key,object.version_id,
       object.checksum_sha256,object.encoded_length,object.encryption_mode,object.object_lock_evidence
FROM artifact_objects AS object
LEFT JOIN artifact_gc_claims AS claim ON claim.artifact_id=object.artifact_id
WHERE object.flow_incarnation_id=$1
  AND (
    (object.state IN ('reserved','uploaded','verified')
      AND object.updated_at < clock_timestamp()-$2::interval
      AND NOT EXISTS (
        SELECT 1 FROM artifact_upload_attempts AS attempt
        WHERE attempt.artifact_id=object.artifact_id
          AND attempt.attempt_state='prepared'
      ))
    OR (object.state='deleting' AND claim.claim_kind='orphan')
  )
  AND NOT EXISTS (
    SELECT 1 FROM artifact_publication_objects AS root
    WHERE root.artifact_id=object.artifact_id
      AND root.release_marked_at IS NULL AND root.released_at IS NULL
  )
ORDER BY (object.state='deleting') DESC,object.updated_at,object.artifact_id
LIMIT 1
FOR UPDATE OF object SKIP LOCKED`, fence.FlowIncarnationID, olderThan.String()).Scan(
		&claim.artifactID, &claim.evidence.Bucket, &claim.evidence.Key, &versionID,
		&claim.evidence.ChecksumSHA256, &claim.evidence.Length,
		&claim.evidence.EncryptionMode, &claim.evidence.ObjectLock,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return collectionClaim{}, false, nil
	}
	if err != nil {
		return collectionClaim{}, false, err
	}
	if versionID != nil {
		claim.evidence.VersionID = *versionID
	}
	claim.claimEpoch, err = c.nextEpoch(ctx, tx, fence.FlowIncarnationID)
	if err != nil {
		return collectionClaim{}, false, err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO artifact_gc_claims(
  artifact_id,claim_epoch,generation,acquisition_id,lease_epoch,claim_kind,publication_id
) VALUES($1,$2,$3,$4,$5,'orphan',NULL)
ON CONFLICT (artifact_id) DO UPDATE SET
  claim_epoch=EXCLUDED.claim_epoch,generation=EXCLUDED.generation,
  acquisition_id=EXCLUDED.acquisition_id,lease_epoch=EXCLUDED.lease_epoch,
  claim_kind='orphan',publication_id=NULL,claimed_at=clock_timestamp()`,
		claim.artifactID, claim.claimEpoch, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch,
	); err != nil {
		return collectionClaim{}, false, err
	}
	tag, err := tx.Exec(ctx, `
UPDATE artifact_objects SET state='deleting',updated_at=clock_timestamp()
WHERE artifact_id=$1 AND flow_incarnation_id=$2
  AND state IN ('reserved','uploaded','verified','deleting')
  AND NOT EXISTS (
    SELECT 1 FROM artifact_publication_objects
    WHERE artifact_id=$1 AND release_marked_at IS NULL AND released_at IS NULL
  )`, claim.artifactID, fence.FlowIncarnationID)
	if err != nil {
		return collectionClaim{}, false, fmt.Errorf("mark orphan %s: %w", claim.artifactID, err)
	}
	if tag.RowsAffected() != 1 {
		return collectionClaim{}, false, fmt.Errorf("mark orphan %s: affected=%d", claim.artifactID, tag.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		return collectionClaim{}, false, err
	}
	return claim, true, nil
}

func (c *Collector) claimRetention(ctx context.Context, fence authority.RunFence, retention time.Duration) (collectionClaim, bool, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return collectionClaim{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return collectionClaim{}, false, err
	}
	var claim collectionClaim
	claim.kind = "retention"
	var versionID *string
	err = tx.QueryRow(ctx, `
SELECT object.artifact_id,publication.publication_id,object.bucket,object.object_key,
       object.version_id,object.checksum_sha256,object.encoded_length,
       object.encryption_mode,object.object_lock_evidence
FROM artifact_publication_objects AS root
JOIN artifact_publications AS publication ON publication.publication_id=root.publication_id
JOIN artifact_objects AS object ON object.artifact_id=root.artifact_id
JOIN source_ack_receipts AS ack
  ON ack.flow_incarnation_id=publication.flow_incarnation_id
 AND ack.position_id=publication.position_id
JOIN authoritative_checkpoints AS checkpoint
  ON checkpoint.flow_incarnation_id=publication.flow_incarnation_id
LEFT JOIN artifact_gc_claims AS claim ON claim.artifact_id=object.artifact_id
WHERE publication.flow_incarnation_id=$1
  AND root.released_at IS NULL
  AND (
    (root.release_marked_at IS NULL AND object.state='rooted'
      AND publication.published_at < clock_timestamp()-$2::interval
      AND publication.checkpoint_lsn<>checkpoint.lsn
      AND NOT EXISTS (
        SELECT 1 FROM artifact_deliveries AS delivery
        WHERE delivery.publication_id=publication.publication_id AND delivery.delivered_at IS NULL
      ))
    OR (root.release_marked_at IS NOT NULL AND object.state='deleting'
      AND claim.claim_kind='retention' AND claim.publication_id=publication.publication_id)
  )
ORDER BY (root.release_marked_at IS NOT NULL) DESC,publication.sequence,root.ordinal
LIMIT 1
FOR UPDATE OF root,object SKIP LOCKED`, fence.FlowIncarnationID, retention.String()).Scan(
		&claim.artifactID, &claim.publicationID, &claim.evidence.Bucket, &claim.evidence.Key,
		&versionID, &claim.evidence.ChecksumSHA256, &claim.evidence.Length,
		&claim.evidence.EncryptionMode, &claim.evidence.ObjectLock,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return collectionClaim{}, false, nil
	}
	if err != nil {
		return collectionClaim{}, false, err
	}
	if versionID != nil {
		claim.evidence.VersionID = *versionID
	}
	claim.claimEpoch, err = c.nextEpoch(ctx, tx, fence.FlowIncarnationID)
	if err != nil {
		return collectionClaim{}, false, err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO artifact_gc_claims(
  artifact_id,claim_epoch,generation,acquisition_id,lease_epoch,claim_kind,publication_id
) VALUES($1,$2,$3,$4,$5,'retention',$6)
ON CONFLICT (artifact_id) DO UPDATE SET
  claim_epoch=EXCLUDED.claim_epoch,generation=EXCLUDED.generation,
  acquisition_id=EXCLUDED.acquisition_id,lease_epoch=EXCLUDED.lease_epoch,
  claim_kind='retention',publication_id=EXCLUDED.publication_id,claimed_at=clock_timestamp()`,
		claim.artifactID, claim.claimEpoch, fence.Generation, fence.AcquisitionID,
		fence.LeaseEpoch, claim.publicationID,
	); err != nil {
		return collectionClaim{}, false, err
	}
	tag, err := tx.Exec(ctx, `
UPDATE artifact_publication_objects
SET release_marked_at=COALESCE(release_marked_at,clock_timestamp())
WHERE publication_id=$1 AND artifact_id=$2 AND released_at IS NULL`, claim.publicationID, claim.artifactID)
	if err != nil || tag.RowsAffected() != 1 {
		return collectionClaim{}, false, fmt.Errorf("mark retained root %s: affected=%d err=%w", claim.artifactID, tag.RowsAffected(), err)
	}
	tag, err = tx.Exec(ctx, `
UPDATE artifact_objects SET state='deleting',updated_at=clock_timestamp()
WHERE artifact_id=$1 AND flow_incarnation_id=$2 AND state IN ('rooted','deleting')`, claim.artifactID, fence.FlowIncarnationID)
	if err != nil || tag.RowsAffected() != 1 {
		return collectionClaim{}, false, fmt.Errorf("mark retained object %s: affected=%d err=%w", claim.artifactID, tag.RowsAffected(), err)
	}
	if err := tx.Commit(ctx); err != nil {
		return collectionClaim{}, false, err
	}
	return claim, true, nil
}

func (c *Collector) nextEpoch(ctx context.Context, tx pgx.Tx, incarnationID uuid.UUID) (int64, error) {
	var epoch int64
	if err := tx.QueryRow(ctx, `
UPDATE artifact_streams SET gc_epoch=gc_epoch+1
WHERE flow_incarnation_id=$1 RETURNING gc_epoch`, incarnationID).Scan(&epoch); err != nil {
		return 0, fmt.Errorf("allocate artifact GC epoch: %w", err)
	}
	return epoch, nil
}

func (c *Collector) adoptClaimEvidence(ctx context.Context, fence authority.RunFence, claim collectionClaim, evidence ObjectEvidence) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	if err := c.validateClaim(ctx, tx, fence, claim); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
UPDATE artifact_objects
SET version_id=$3,encryption_mode=$4,object_lock_evidence=$5,updated_at=clock_timestamp()
WHERE artifact_id=$1 AND flow_incarnation_id=$2 AND state='deleting'
  AND bucket=$6 AND object_key=$7 AND checksum_sha256=$8 AND encoded_length=$9`,
		claim.artifactID, fence.FlowIncarnationID, evidence.VersionID,
		evidence.EncryptionMode, evidence.ObjectLock, evidence.Bucket, evidence.Key,
		evidence.ChecksumSHA256, evidence.Length,
	)
	if err != nil || tag.RowsAffected() != 1 {
		return fmt.Errorf("adopt GC object evidence %s: affected=%d err=%w", claim.artifactID, tag.RowsAffected(), err)
	}
	return tx.Commit(ctx)
}

func (c *Collector) finalize(ctx context.Context, fence authority.RunFence, claim collectionClaim) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	if err := c.validateClaim(ctx, tx, fence, claim); err != nil {
		return err
	}

	var releasedBytes int64
	switch claim.kind {
	case "orphan":
		tag, err := tx.Exec(ctx, `
UPDATE artifact_objects SET state='deleted',updated_at=clock_timestamp()
WHERE artifact_id=$1 AND flow_incarnation_id=$2 AND state='deleting'
  AND NOT EXISTS (
    SELECT 1 FROM artifact_publication_objects
    WHERE artifact_id=$1 AND release_marked_at IS NULL AND released_at IS NULL
  )`, claim.artifactID, fence.FlowIncarnationID)
		if err != nil || tag.RowsAffected() != 1 {
			return fmt.Errorf("finalize orphan deletion %s: affected=%d err=%w", claim.artifactID, tag.RowsAffected(), err)
		}
		err = tx.QueryRow(ctx, `
UPDATE artifact_quota_reservations SET released_at=clock_timestamp()
WHERE artifact_id=$1 AND converted_at IS NULL AND released_at IS NULL
RETURNING bytes`, claim.artifactID).Scan(&releasedBytes)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if errors.Is(err, pgx.ErrNoRows) {
			releasedBytes = 0
		}
		if releasedBytes > 0 {
			tag, err = tx.Exec(ctx, `
UPDATE artifact_quota_accounts
SET reserved_bytes=reserved_bytes-$2,updated_at=clock_timestamp()
WHERE flow_incarnation_id=$1 AND reserved_bytes >= $2`, fence.FlowIncarnationID, releasedBytes)
			if err != nil || tag.RowsAffected() != 1 {
				return fmt.Errorf("release orphan quota %s: affected=%d err=%w", claim.artifactID, tag.RowsAffected(), err)
			}
		}
	case "retention":
		var safe bool
		if err := tx.QueryRow(ctx, `
SELECT EXISTS(
  SELECT 1
  FROM artifact_publication_objects AS root
  JOIN artifact_publications AS publication ON publication.publication_id=root.publication_id
  JOIN source_ack_receipts AS ack
    ON ack.flow_incarnation_id=publication.flow_incarnation_id
   AND ack.position_id=publication.position_id
  JOIN authoritative_checkpoints AS checkpoint
    ON checkpoint.flow_incarnation_id=publication.flow_incarnation_id
  WHERE root.publication_id=$1 AND root.artifact_id=$2
    AND root.release_marked_at IS NOT NULL AND root.released_at IS NULL
    AND publication.checkpoint_lsn<>checkpoint.lsn
    AND NOT EXISTS (
      SELECT 1 FROM artifact_deliveries AS delivery
      WHERE delivery.publication_id=publication.publication_id AND delivery.delivered_at IS NULL
    )
)`, claim.publicationID, claim.artifactID).Scan(&safe); err != nil {
			return err
		}
		if !safe {
			return fmt.Errorf("retention root %s is no longer safe to sweep", claim.artifactID)
		}
		tag, err := tx.Exec(ctx, `
UPDATE artifact_publication_objects SET released_at=clock_timestamp()
WHERE publication_id=$1 AND artifact_id=$2
  AND release_marked_at IS NOT NULL AND released_at IS NULL`, claim.publicationID, claim.artifactID)
		if err != nil || tag.RowsAffected() != 1 {
			return fmt.Errorf("release retained root %s: affected=%d err=%w", claim.artifactID, tag.RowsAffected(), err)
		}
		err = tx.QueryRow(ctx, `
UPDATE artifact_objects SET state='deleted',updated_at=clock_timestamp()
WHERE artifact_id=$1 AND flow_incarnation_id=$2 AND state='deleting'
  AND NOT EXISTS (
    SELECT 1 FROM artifact_publication_objects
    WHERE artifact_id=$1 AND release_marked_at IS NULL AND released_at IS NULL
  )
RETURNING encoded_length`, claim.artifactID, fence.FlowIncarnationID).Scan(&releasedBytes)
		if err != nil {
			return fmt.Errorf("finalize retained object %s: %w", claim.artifactID, err)
		}
		tag, err = tx.Exec(ctx, `
UPDATE artifact_quota_accounts
SET rooted_bytes=rooted_bytes-$2,updated_at=clock_timestamp()
WHERE flow_incarnation_id=$1 AND rooted_bytes >= $2`, fence.FlowIncarnationID, releasedBytes)
		if err != nil || tag.RowsAffected() != 1 {
			return fmt.Errorf("release retained quota %s: affected=%d err=%w", claim.artifactID, tag.RowsAffected(), err)
		}
		if _, err := tx.Exec(ctx, `
UPDATE source_ack_retention_roots AS ack_root
SET released_at=clock_timestamp()
WHERE ack_root.flow_incarnation_id=$1
  AND ack_root.root_kind='artifact_publication'
  AND ack_root.root_id=$2
  AND ack_root.released_at IS NULL
  AND NOT EXISTS (
    SELECT 1 FROM artifact_publication_objects AS root
    WHERE root.publication_id=$3 AND root.released_at IS NULL
  )`, fence.FlowIncarnationID, claim.publicationID.String(), claim.publicationID); err != nil {
			return fmt.Errorf("release artifact source ACK root %s: %w", claim.publicationID, err)
		}
	default:
		return fmt.Errorf("unknown artifact GC claim kind %q", claim.kind)
	}
	if _, err := tx.Exec(ctx, `
DELETE FROM artifact_gc_claims WHERE artifact_id=$1 AND claim_epoch=$2`, claim.artifactID, claim.claimEpoch); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *Collector) validateClaim(ctx context.Context, tx pgx.Tx, fence authority.RunFence, claim collectionClaim) error {
	var storedKind string
	var storedPublication *uuid.UUID
	var acquisitionID uuid.UUID
	var epoch, generation, leaseEpoch int64
	if err := tx.QueryRow(ctx, `
SELECT claim_epoch,generation,acquisition_id,lease_epoch,claim_kind,publication_id
FROM artifact_gc_claims WHERE artifact_id=$1 FOR UPDATE`, claim.artifactID).Scan(
		&epoch, &generation, &acquisitionID, &leaseEpoch, &storedKind, &storedPublication,
	); err != nil {
		return err
	}
	if epoch != claim.claimEpoch || generation != fence.Generation || acquisitionID != fence.AcquisitionID ||
		leaseEpoch != fence.LeaseEpoch || storedKind != claim.kind {
		return fmt.Errorf("%w: artifact GC claim changed", authority.ErrFenceRejected)
	}
	if claim.kind == "retention" && (storedPublication == nil || *storedPublication != claim.publicationID) {
		return fmt.Errorf("%w: artifact retention publication changed", authority.ErrFenceRejected)
	}
	return nil
}
