package artifactlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// Consumer delivers rooted publications through an append-only Iceberg catalog
// seam. It does not implement equality deletes, upserts, or compaction.
type Consumer struct {
	pool    *pgxpool.Pool
	catalog Catalog
}

func NewConsumer(pool *pgxpool.Pool, catalog Catalog) (*Consumer, error) {
	if pool == nil || catalog == nil {
		return nil, errors.New("artifact consumer PostgreSQL pool and catalog are required")
	}
	return &Consumer{pool: pool, catalog: catalog}, nil
}

// ConsumeNext processes one publication in PostgreSQL sequence order. Attempts
// are persisted before catalog I/O; ambiguous commits reconcile by exact
// publication/content identity or fail closed.
func (c *Consumer) ConsumeNext(ctx context.Context, fence authority.RunFence, consumerRevisionID, table string) (bool, error) {
	publicationID, objects, contentHash, attemptID, hasAttempt, claim, err := c.loadNext(ctx, fence, consumerRevisionID)
	if err != nil {
		return false, err
	}
	if publicationID == uuid.Nil {
		telemetry.RecordArtifactConsumerOutcome(ctx, "idle_or_claimed")
		return false, nil
	}
	defer func() {
		store, storeErr := authority.NewPostgresStore(c.pool)
		if storeErr == nil {
			_ = store.ReleaseClaim(context.WithoutCancel(ctx), claim)
		}
	}()
	if hasAttempt {
		disposition, commit, err := c.catalog.Reconcile(ctx, table, publicationID)
		if err != nil {
			return false, err
		}
		switch disposition {
		case CatalogApplied:
			if commit.ContentHash != contentHash {
				return false, fmt.Errorf("%w: catalog content hash %s, expected %s", connector.ErrDeliveryConflict, commit.ContentHash, contentHash)
			}
			return true, c.finalize(ctx, fence, consumerRevisionID, publicationID, attemptID, commit)
		case CatalogIndeterminate:
			return false, fmt.Errorf("%w: Iceberg publication %s", connector.ErrDeliveryIndeterminate, publicationID)
		case CatalogNotApplied:
			// Prepare a new append-only attempt below.
		}
	}
	attemptID, err = c.prepare(ctx, fence, consumerRevisionID, publicationID)
	if err != nil {
		return false, err
	}
	commit, err := c.catalog.Append(ctx, table, publicationID, objects)
	if err != nil {
		return false, err
	}
	if commit.ContentHash != contentHash {
		return false, fmt.Errorf("%w: catalog commit hash %s, expected %s", connector.ErrDeliveryConflict, commit.ContentHash, contentHash)
	}
	if err := c.finalize(ctx, fence, consumerRevisionID, publicationID, attemptID, commit); err != nil {
		telemetry.RecordArtifactConsumerOutcome(ctx, "finalize_failed")
		return false, err
	}
	telemetry.RecordArtifactConsumerOutcome(ctx, "committed")
	return true, nil
}

func (c *Consumer) loadNext(ctx context.Context, fence authority.RunFence, consumerRevisionID string) (uuid.UUID, []ObjectEvidence, string, uuid.UUID, bool, authority.ClaimFence, error) {
	empty := authority.ClaimFence{}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, nil, "", uuid.Nil, false, empty, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return uuid.Nil, nil, "", uuid.Nil, false, empty, err
	}
	var publicationID uuid.UUID
	err = tx.QueryRow(ctx, `
SELECT publication_id
FROM artifact_deliveries
WHERE flow_incarnation_id=$1 AND consumer_revision_id=$2 AND delivered_at IS NULL
ORDER BY sequence
LIMIT 1
FOR UPDATE`, fence.FlowIncarnationID, consumerRevisionID).Scan(&publicationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, nil, "", uuid.Nil, false, empty, nil
	}
	if err != nil {
		return uuid.Nil, nil, "", uuid.Nil, false, empty, err
	}
	claimKind := authority.ClaimKind("artifact_delivery")
	workID := consumerRevisionID + ":" + publicationID.String()
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
		return uuid.Nil, nil, "", uuid.Nil, false, empty, nil
	case err == nil:
		claimEpoch = previousEpoch + 1
	case !errors.Is(err, pgx.ErrNoRows):
		return uuid.Nil, nil, "", uuid.Nil, false, empty, err
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
		return uuid.Nil, nil, "", uuid.Nil, false, empty, err
	}
	claim := authority.ClaimFence{RunFence: fence, Kind: claimKind, WorkID: workID, ClaimEpoch: claimEpoch}
	rows, err := tx.Query(ctx, `
SELECT object.bucket,object.object_key,object.version_id,object.checksum_sha256,
       object.encoded_length,object.encryption_mode,object.object_lock_evidence,
       object.artifact_id,object.encoded_byte_hash
FROM artifact_publication_objects AS item
JOIN artifact_objects AS object ON object.artifact_id=item.artifact_id
WHERE item.publication_id=$1 AND object.state='rooted'
ORDER BY item.ordinal`, publicationID)
	if err != nil {
		return uuid.Nil, nil, "", uuid.Nil, false, empty, err
	}
	objects := make([]ObjectEvidence, 0)
	hash := sha256.New()
	for rows.Next() {
		var evidence ObjectEvidence
		var artifactID, encodedHash string
		if err := rows.Scan(&evidence.Bucket, &evidence.Key, &evidence.VersionID, &evidence.ChecksumSHA256, &evidence.Length, &evidence.EncryptionMode, &evidence.ObjectLock, &artifactID, &encodedHash); err != nil {
			rows.Close()
			return uuid.Nil, nil, "", uuid.Nil, false, empty, err
		}
		_, _ = hash.Write([]byte(artifactID))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(encodedHash))
		_, _ = hash.Write([]byte{0})
		objects = append(objects, evidence)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return uuid.Nil, nil, "", uuid.Nil, false, empty, err
	}
	rows.Close()
	if len(objects) == 0 {
		return uuid.Nil, nil, "", uuid.Nil, false, empty, errors.New("rooted artifact publication has no objects")
	}
	contentHash := hex.EncodeToString(hash.Sum(nil))
	var attemptID uuid.UUID
	err = tx.QueryRow(ctx, `
SELECT attempt.attempt_id
FROM artifact_delivery_attempts AS attempt
LEFT JOIN artifact_delivery_receipts AS receipt ON receipt.attempt_id=attempt.attempt_id
WHERE attempt.flow_incarnation_id=$1 AND attempt.consumer_revision_id=$2
  AND attempt.publication_id=$3 AND receipt.attempt_id IS NULL
ORDER BY attempt.prepared_at DESC,attempt.attempt_id DESC
LIMIT 1`, fence.FlowIncarnationID, consumerRevisionID, publicationID).Scan(&attemptID)
	hasAttempt := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, nil, "", uuid.Nil, false, empty, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, nil, "", uuid.Nil, false, empty, err
	}
	return publicationID, objects, contentHash, attemptID, hasAttempt, claim, nil
}

func (c *Consumer) prepare(ctx context.Context, fence authority.RunFence, consumerRevisionID string, publicationID uuid.UUID) (uuid.UUID, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return uuid.Nil, err
	}
	attemptID := uuid.New()
	if _, err := tx.Exec(ctx, `
INSERT INTO artifact_delivery_attempts (
  attempt_id,flow_incarnation_id,consumer_revision_id,publication_id,
  generation,acquisition_id,lease_epoch
) VALUES ($1,$2,$3,$4,$5,$6,$7)`, attemptID, fence.FlowIncarnationID, consumerRevisionID, publicationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch); err != nil {
		return uuid.Nil, fmt.Errorf("prepare artifact consumer attempt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, err
	}
	return attemptID, nil
}

func (c *Consumer) finalize(ctx context.Context, fence authority.RunFence, consumerRevisionID string, publicationID, attemptID uuid.UUID, commit CatalogCommit) error {
	if commit.SnapshotID == "" || commit.ContentHash == "" {
		return errors.New("catalog commit evidence is incomplete")
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	receiptTag, err := tx.Exec(ctx, `
INSERT INTO artifact_delivery_receipts (
  flow_incarnation_id,consumer_revision_id,publication_id,attempt_id,
  snapshot_id,content_hash,acquisition_id,lease_epoch
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (flow_incarnation_id,consumer_revision_id,publication_id) DO UPDATE SET
  snapshot_id=EXCLUDED.snapshot_id
WHERE artifact_delivery_receipts.snapshot_id=EXCLUDED.snapshot_id
  AND artifact_delivery_receipts.content_hash=EXCLUDED.content_hash`, fence.FlowIncarnationID, consumerRevisionID, publicationID, attemptID, commit.SnapshotID, commit.ContentHash, fence.AcquisitionID, fence.LeaseEpoch)
	if err != nil {
		return fmt.Errorf("record artifact consumer receipt: %w", err)
	}
	if receiptTag.RowsAffected() != 1 {
		return fmt.Errorf("%w: artifact consumer receipt differs", connector.ErrDeliveryConflict)
	}
	tag, err := tx.Exec(ctx, `
UPDATE artifact_deliveries
SET delivered_at=clock_timestamp()
WHERE flow_incarnation_id=$1 AND consumer_revision_id=$2 AND publication_id=$3 AND delivered_at IS NULL`, fence.FlowIncarnationID, consumerRevisionID, publicationID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("artifact delivery is no longer pending")
	}
	return tx.Commit(ctx)
}
