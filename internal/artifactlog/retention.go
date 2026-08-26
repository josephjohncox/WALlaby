package artifactlog

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/telemetry"
)

// MetadataPruneStats reports one strictly bounded metadata sweep.
type MetadataPruneStats struct {
	PublicationsScanned  int
	PublicationsDeleted  int
	PublicationsDeferred int
	RowsDeleted          int
}

// MetadataPrunerHooks exposes deterministic claim/finalization boundaries.
type MetadataPrunerHooks struct {
	Boundary func(context.Context, string, uuid.UUID) error
}

// MetadataPrunerOption configures deterministic test behavior.
type MetadataPrunerOption func(*MetadataPruner)

// WithMetadataPrunerHooks installs metadata retention failure hooks.
func WithMetadataPrunerHooks(hooks MetadataPrunerHooks) MetadataPrunerOption {
	return func(pruner *MetadataPruner) { pruner.hooks = hooks }
}

// MetadataPruner durably removes terminal artifact control history. A claim is
// deliberately independent of the publication FK: restrictive children and the
// publication can be removed in row-bounded transactions, then the tombstone is
// removed by a later transaction. PostgreSQL remains the sole authority.
type MetadataPruner struct {
	pool  *pgxpool.Pool
	hooks MetadataPrunerHooks
}

func NewMetadataPruner(pool *pgxpool.Pool, options ...MetadataPrunerOption) (*MetadataPruner, error) {
	if pool == nil {
		return nil, errors.New("artifact metadata pruner requires PostgreSQL")
	}
	pruner := &MetadataPruner{pool: pool}
	for _, option := range options {
		option(pruner)
	}
	return pruner, nil
}

type metadataClaim struct {
	publicationID uuid.UUID
	claimEpoch    int64
}

// Prune scans and advances at most maxPublications durable claims and deletes
// at most maxRows PostgreSQL rows. Very small row limits may defer an atomic
// receipt/attempt/delivery evidence bundle, but never exceed the configured cap.
func (p *MetadataPruner) Prune(ctx context.Context, fence authority.RunFence, horizon time.Duration, maxPublications, maxRows int) (MetadataPruneStats, error) {
	var stats MetadataPruneStats
	if horizon <= 0 || maxPublications <= 0 || maxRows <= 0 {
		return stats, errors.New("positive artifact metadata retention, publication, and row limits are required")
	}
	seen := make([]uuid.UUID, 0, maxPublications)
	for stats.PublicationsScanned < maxPublications && stats.RowsDeleted < maxRows {
		claim, ok, err := p.claimNext(ctx, fence, horizon, seen)
		if err != nil {
			return stats, err
		}
		if !ok {
			break
		}
		seen = append(seen, claim.publicationID)
		stats.PublicationsScanned++
		if p.hooks.Boundary != nil {
			if err := p.hooks.Boundary(ctx, "after_metadata_claim", claim.publicationID); err != nil {
				return stats, err
			}
		}
		deleted, publicationDeleted, deferred, err := p.advanceClaim(ctx, fence, claim, horizon, maxRows-stats.RowsDeleted)
		if err != nil {
			return stats, err
		}
		stats.RowsDeleted += deleted
		if publicationDeleted {
			stats.PublicationsDeleted++
		}
		if deferred {
			stats.PublicationsDeferred++
		}
	}
	telemetry.RecordArtifactMetadataRetention(ctx, "scanned", int64(stats.PublicationsScanned))
	telemetry.RecordArtifactMetadataRetention(ctx, "deleted", int64(stats.PublicationsDeleted))
	telemetry.RecordArtifactMetadataRetention(ctx, "deferred", int64(stats.PublicationsDeferred))
	telemetry.RecordArtifactMetadataRows(ctx, int64(stats.RowsDeleted))
	return stats, nil
}

func (p *MetadataPruner) claimNext(ctx context.Context, fence authority.RunFence, horizon time.Duration, excluded []uuid.UUID) (metadataClaim, bool, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return metadataClaim{}, false, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return metadataClaim{}, false, err
	}

	var claim metadataClaim
	// Resume an interrupted claim first. Updating the exact fence makes takeover
	// explicit and monotonic; final safety is still revalidated under locks.
	err = tx.QueryRow(ctx, `
SELECT publication_id,claim_epoch
FROM artifact_metadata_prune_claims
WHERE flow_incarnation_id=$1 AND NOT (publication_id=ANY($2::uuid[]))
ORDER BY claimed_at,publication_id
LIMIT 1
FOR UPDATE SKIP LOCKED`, fence.FlowIncarnationID, excluded).Scan(&claim.publicationID, &claim.claimEpoch)
	if err == nil {
		claim.claimEpoch++
		tag, updateErr := tx.Exec(ctx, `
UPDATE artifact_metadata_prune_claims
SET generation=$2,acquisition_id=$3,lease_epoch=$4,claim_epoch=$5,updated_at=clock_timestamp()
WHERE publication_id=$1`, claim.publicationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, claim.claimEpoch)
		if updateErr != nil || tag.RowsAffected() != 1 {
			return metadataClaim{}, false, fmt.Errorf("adopt artifact metadata claim: affected=%d err=%w", tag.RowsAffected(), updateErr)
		}
		if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
			return metadataClaim{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return metadataClaim{}, false, err
		}
		return claim, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return metadataClaim{}, false, err
	}

	var artifactIDs []byte
	var eligibleAt time.Time
	err = tx.QueryRow(ctx, `
SELECT publication.publication_id,
       GREATEST(
         publication.published_at,
         COALESCE((SELECT max(root.released_at) FROM artifact_publication_objects AS root WHERE root.publication_id=publication.publication_id),publication.published_at),
         COALESCE((SELECT max(delivery.delivered_at) FROM artifact_deliveries AS delivery WHERE delivery.publication_id=publication.publication_id),publication.published_at),
         COALESCE((SELECT max(receipt.committed_at) FROM artifact_delivery_receipts AS receipt WHERE receipt.publication_id=publication.publication_id),publication.published_at),
         (SELECT min(successor.published_at) FROM artifact_publications AS successor WHERE successor.flow_incarnation_id=publication.flow_incarnation_id AND successor.sequence>publication.sequence),
         COALESCE((
           SELECT max(movement.moved_at) FROM (
             SELECT delivery.consumer_revision_id,min(successor_delivery.delivered_at) AS moved_at
             FROM artifact_deliveries AS delivery
             JOIN artifact_deliveries AS successor_delivery
               ON successor_delivery.flow_incarnation_id=delivery.flow_incarnation_id
              AND successor_delivery.consumer_revision_id=delivery.consumer_revision_id
              AND successor_delivery.delivered_at IS NOT NULL
             JOIN artifact_publications AS successor
               ON successor.publication_id=successor_delivery.publication_id
              AND successor.sequence>publication.sequence
             WHERE delivery.publication_id=publication.publication_id
             GROUP BY delivery.consumer_revision_id
           ) AS movement
         ),publication.published_at)
       ) AS eligible_at,
       COALESCE((SELECT jsonb_agg(root.artifact_id ORDER BY root.ordinal) FROM artifact_publication_objects AS root WHERE root.publication_id=publication.publication_id),'[]'::jsonb)
FROM artifact_publications AS publication
JOIN authoritative_checkpoints AS checkpoint
  ON checkpoint.flow_incarnation_id=publication.flow_incarnation_id
JOIN artifact_publications AS current_publication
  ON current_publication.flow_incarnation_id=publication.flow_incarnation_id
 AND current_publication.publication_id::text=checkpoint.metadata->>'artifact_publication_id'
WHERE publication.flow_incarnation_id=$1
  AND NOT (publication.publication_id=ANY($2::uuid[]))
  AND current_publication.sequence>publication.sequence
  AND checkpoint.lsn<>publication.checkpoint_lsn
  AND publication.published_at < clock_timestamp()-$3::interval
  AND EXISTS (
    SELECT 1 FROM artifact_publications AS successor
    WHERE successor.flow_incarnation_id=publication.flow_incarnation_id
      AND successor.sequence>publication.sequence
      AND successor.published_at < clock_timestamp()-$3::interval
  )
  AND NOT EXISTS (
    SELECT 1 FROM artifact_publication_objects AS root
    JOIN artifact_objects AS object ON object.artifact_id=root.artifact_id
    WHERE root.publication_id=publication.publication_id
      AND (root.released_at IS NULL OR root.released_at>=clock_timestamp()-$3::interval OR object.state<>'deleted')
  )
  AND NOT EXISTS (
    SELECT 1 FROM source_ack_retention_roots AS ack_root
    WHERE ack_root.flow_incarnation_id=publication.flow_incarnation_id
      AND ack_root.root_kind='artifact_publication'
      AND ack_root.root_id=publication.publication_id::text
      AND (ack_root.released_at IS NULL OR ack_root.released_at>=clock_timestamp()-$3::interval)
  )
  AND NOT EXISTS (
    SELECT 1 FROM artifact_gc_claims AS gc
    WHERE gc.publication_id=publication.publication_id OR gc.artifact_id IN (
      SELECT root.artifact_id FROM artifact_publication_objects AS root WHERE root.publication_id=publication.publication_id
    )
  )
  AND NOT EXISTS (
    SELECT 1 FROM artifact_deliveries AS delivery
    WHERE delivery.publication_id=publication.publication_id
      AND (delivery.delivered_at IS NULL OR delivery.delivered_at>=clock_timestamp()-$3::interval)
  )
  AND NOT EXISTS (
    SELECT 1 FROM artifact_delivery_receipts AS receipt
    WHERE receipt.publication_id=publication.publication_id
      AND receipt.committed_at>=clock_timestamp()-$3::interval
  )
  AND NOT EXISTS (
    SELECT 1 FROM artifact_delivery_attempts AS attempt
    LEFT JOIN artifact_delivery_receipts AS receipt ON receipt.attempt_id=attempt.attempt_id
    WHERE attempt.publication_id=publication.publication_id AND receipt.attempt_id IS NULL
  )
  AND NOT EXISTS (
    SELECT 1 FROM artifact_deliveries AS delivery
    LEFT JOIN artifact_delivery_attempts AS attempt
      ON attempt.flow_incarnation_id=delivery.flow_incarnation_id
     AND attempt.consumer_revision_id=delivery.consumer_revision_id
     AND attempt.publication_id=delivery.publication_id
    LEFT JOIN artifact_delivery_receipts AS receipt ON receipt.attempt_id=attempt.attempt_id
    WHERE delivery.publication_id=publication.publication_id
      AND (attempt.attempt_id IS NULL OR receipt.attempt_id IS NULL)
  )
  AND NOT EXISTS (
    SELECT 1 FROM artifact_consumer_checkpoints AS consumer_checkpoint
    WHERE consumer_checkpoint.publication_id=publication.publication_id
       OR (consumer_checkpoint.flow_incarnation_id=publication.flow_incarnation_id
           AND consumer_checkpoint.publication_sequence<=publication.sequence
           AND EXISTS (SELECT 1 FROM artifact_deliveries AS delivery WHERE delivery.publication_id=publication.publication_id AND delivery.consumer_revision_id=consumer_checkpoint.consumer_revision_id))
  )
  AND NOT EXISTS (
    SELECT 1 FROM artifact_deliveries AS delivery
    WHERE delivery.publication_id=publication.publication_id
      AND NOT EXISTS (
        SELECT 1 FROM artifact_deliveries AS successor_delivery
        JOIN artifact_publications AS successor ON successor.publication_id=successor_delivery.publication_id
        WHERE successor_delivery.flow_incarnation_id=delivery.flow_incarnation_id
          AND successor_delivery.consumer_revision_id=delivery.consumer_revision_id
          AND successor.sequence>publication.sequence
          AND successor_delivery.delivered_at IS NOT NULL
          AND successor_delivery.delivered_at<clock_timestamp()-$3::interval
      )
  )
  AND NOT EXISTS (
    SELECT 1 FROM work_claims AS claim
    JOIN producer_leases AS producer ON producer.incarnation_id=claim.incarnation_id
    WHERE claim.incarnation_id=publication.flow_incarnation_id
      AND claim.claim_kind='artifact_delivery'
      AND claim.work_id LIKE '%:'||publication.publication_id::text
      AND claim.released_at IS NULL AND claim.claim_expires_at>clock_timestamp()
      AND producer.acquisition_id=claim.acquisition_id AND producer.lease_epoch=claim.lease_epoch
      AND producer.lease_expires_at>clock_timestamp()
  )
  AND NOT EXISTS (
    SELECT 1 FROM artifact_publication_objects AS root
    JOIN artifact_upload_attempts AS upload ON upload.artifact_id=root.artifact_id AND upload.attempt_state='prepared'
    WHERE root.publication_id=publication.publication_id
  )
  AND NOT EXISTS (
    SELECT 1 FROM artifact_publication_objects AS root
    JOIN artifact_quota_reservations AS quota ON quota.artifact_id=root.artifact_id
    WHERE root.publication_id=publication.publication_id
      AND quota.converted_at IS NULL AND quota.released_at IS NULL
  )
ORDER BY publication.published_at,publication.sequence,publication.publication_id
LIMIT 1
FOR UPDATE OF publication SKIP LOCKED`, fence.FlowIncarnationID, excluded, horizon.String()).Scan(&claim.publicationID, &eligibleAt, &artifactIDs)
	if errors.Is(err, pgx.ErrNoRows) {
		return metadataClaim{}, false, nil
	}
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			return metadataClaim{}, false, fmt.Errorf("select artifact metadata candidate at SQL position %d: %w", pgErr.Position, err)
		}
		return metadataClaim{}, false, err
	}
	claim.claimEpoch = 1
	if _, err := tx.Exec(ctx, `
INSERT INTO artifact_metadata_prune_claims(
  publication_id,flow_incarnation_id,generation,acquisition_id,lease_epoch,claim_epoch,artifact_ids,eligible_at
) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8)`, claim.publicationID, fence.FlowIncarnationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, claim.claimEpoch, artifactIDs, eligibleAt); err != nil {
		return metadataClaim{}, false, err
	}
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return metadataClaim{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return metadataClaim{}, false, err
	}
	return claim, true, nil
}

func (p *MetadataPruner) advanceClaim(ctx context.Context, fence authority.RunFence, claim metadataClaim, horizon time.Duration, budget int) (rows int, publicationDeleted, deferred bool, resultErr error) {
	if budget <= 0 {
		return 0, false, true, nil
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, false, false, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return 0, false, false, err
	}
	var epoch, generation, leaseEpoch int64
	var acquisitionID uuid.UUID
	var eligibleAt time.Time
	if err := tx.QueryRow(ctx, `
SELECT claim_epoch,generation,acquisition_id,lease_epoch,eligible_at
FROM artifact_metadata_prune_claims WHERE publication_id=$1 FOR UPDATE`, claim.publicationID).Scan(&epoch, &generation, &acquisitionID, &leaseEpoch, &eligibleAt); err != nil {
		return 0, false, false, err
	}
	if epoch != claim.claimEpoch || generation != fence.Generation || acquisitionID != fence.AcquisitionID || leaseEpoch != fence.LeaseEpoch {
		return 0, false, false, fmt.Errorf("%w: artifact metadata claim changed", authority.ErrFenceRejected)
	}

	var publicationExists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM artifact_publications WHERE publication_id=$1)`, claim.publicationID).Scan(&publicationExists); err != nil {
		return 0, false, false, err
	}
	if !publicationExists {
		tag, err := tx.Exec(ctx, `DELETE FROM artifact_metadata_prune_claims WHERE publication_id=$1 AND claim_epoch=$2`, claim.publicationID, claim.claimEpoch)
		if err != nil || tag.RowsAffected() != 1 {
			return 0, false, false, fmt.Errorf("finish artifact metadata tombstone: affected=%d err=%w", tag.RowsAffected(), err)
		}
		if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
			return 0, false, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, false, false, err
		}
		return 1, false, false, nil
	}
	if _, err := tx.Exec(ctx, `SELECT publication_id FROM artifact_publications WHERE publication_id=$1 FOR UPDATE`, claim.publicationID); err != nil {
		return 0, false, false, err
	}
	// Lock all FK children and cross-domain checkpoint rows before the final
	// predicate. Parent row locking blocks concurrent new FK children.
	for _, lock := range []struct {
		query string
		args  []any
	}{
		{`SELECT artifact_id FROM artifact_publication_objects WHERE publication_id=$1 FOR UPDATE`, []any{claim.publicationID}},
		{`SELECT publication_id FROM artifact_deliveries WHERE publication_id=$1 FOR UPDATE`, []any{claim.publicationID}},
		{`SELECT attempt_id FROM artifact_delivery_attempts WHERE publication_id=$1 FOR UPDATE`, []any{claim.publicationID}},
		{`SELECT publication_id FROM artifact_delivery_receipts WHERE publication_id=$1 FOR UPDATE`, []any{claim.publicationID}},
		{`SELECT publication_id FROM artifact_consumer_checkpoints WHERE flow_incarnation_id=$1 FOR UPDATE`, []any{fence.FlowIncarnationID}},
		{`SELECT flow_incarnation_id FROM authoritative_checkpoints WHERE flow_incarnation_id=$1 FOR UPDATE`, []any{fence.FlowIncarnationID}},
		{`SELECT flow_incarnation_id FROM source_ack_retention_roots WHERE flow_incarnation_id=$1 AND root_kind='artifact_publication' AND root_id=$2::text FOR UPDATE`, []any{fence.FlowIncarnationID, claim.publicationID}},
	} {
		rowsLock, queryErr := tx.Query(ctx, lock.query, lock.args...)
		if queryErr != nil {
			return 0, false, false, queryErr
		}
		rowsLock.Close()
	}
	if p.hooks.Boundary != nil {
		if err := p.hooks.Boundary(ctx, "before_metadata_revalidation", claim.publicationID); err != nil {
			return 0, false, false, err
		}
	}
	var safe bool
	if err := tx.QueryRow(ctx, `
SELECT eligible_at < clock_timestamp()-$3::interval
  AND NOT EXISTS (
    SELECT 1 FROM authoritative_checkpoints AS checkpoint
    WHERE checkpoint.flow_incarnation_id=$2
      AND (checkpoint.lsn=publication.checkpoint_lsn OR checkpoint.metadata->>'artifact_publication_id'=($1::uuid)::text)
  )
  AND NOT EXISTS (SELECT 1 FROM artifact_consumer_checkpoints WHERE publication_id=$1::uuid)
  AND NOT EXISTS (SELECT 1 FROM artifact_publication_objects WHERE publication_id=$1::uuid AND released_at IS NULL)
  AND NOT EXISTS (
    SELECT 1 FROM source_ack_retention_roots
    WHERE flow_incarnation_id=$2 AND root_kind='artifact_publication' AND root_id=($1::uuid)::text AND released_at IS NULL
  )
  AND NOT EXISTS (
    SELECT 1 FROM artifact_gc_claims
    WHERE publication_id=$1::uuid OR artifact_id IN (SELECT value FROM jsonb_array_elements_text(claim.artifact_ids))
  )
  AND NOT EXISTS (SELECT 1 FROM artifact_deliveries WHERE publication_id=$1::uuid AND delivered_at IS NULL)
  AND NOT EXISTS (
    SELECT 1 FROM artifact_delivery_attempts AS attempt
    LEFT JOIN artifact_delivery_receipts AS receipt ON receipt.attempt_id=attempt.attempt_id
    WHERE attempt.publication_id=$1::uuid AND receipt.attempt_id IS NULL
  )
  AND NOT EXISTS (
    SELECT 1 FROM work_claims AS work
    JOIN producer_leases AS producer ON producer.incarnation_id=work.incarnation_id
    WHERE work.incarnation_id=$2 AND work.claim_kind='artifact_delivery'
      AND work.work_id LIKE '%:'||($1::uuid)::text AND work.released_at IS NULL
      AND work.claim_expires_at>clock_timestamp()
      AND producer.acquisition_id=work.acquisition_id AND producer.lease_epoch=work.lease_epoch
      AND producer.lease_expires_at>clock_timestamp()
  )
FROM artifact_metadata_prune_claims AS claim
JOIN artifact_publications AS publication ON publication.publication_id=claim.publication_id
WHERE claim.publication_id=$1::uuid`, claim.publicationID, fence.FlowIncarnationID, horizon.String()).Scan(&safe); err != nil {
		return 0, false, false, err
	}
	if !safe {
		if err := tx.Commit(ctx); err != nil {
			return 0, false, false, err
		}
		return 0, false, true, nil
	}

	remaining := budget
	// Catalog evidence is an atomic reconciliation/conflict bundle.
	for remaining >= 3 {
		var attemptID uuid.UUID
		var consumerRevisionID string
		err := tx.QueryRow(ctx, `
SELECT attempt.attempt_id,delivery.consumer_revision_id
FROM artifact_deliveries AS delivery
JOIN artifact_delivery_attempts AS attempt
  ON attempt.flow_incarnation_id=delivery.flow_incarnation_id
 AND attempt.consumer_revision_id=delivery.consumer_revision_id
 AND attempt.publication_id=delivery.publication_id
JOIN artifact_delivery_receipts AS receipt ON receipt.attempt_id=attempt.attempt_id
WHERE delivery.publication_id=$1 AND delivery.delivered_at IS NOT NULL
ORDER BY delivery.consumer_revision_id LIMIT 1
FOR UPDATE OF delivery,attempt,receipt`, claim.publicationID).Scan(&attemptID, &consumerRevisionID)
		if errors.Is(err, pgx.ErrNoRows) {
			break
		}
		if err != nil {
			return 0, false, false, err
		}
		for _, deletion := range []struct {
			query string
			args  []any
		}{
			{`DELETE FROM artifact_delivery_receipts WHERE attempt_id=$1`, []any{attemptID}},
			{`DELETE FROM artifact_delivery_attempts WHERE attempt_id=$1`, []any{attemptID}},
			{`DELETE FROM artifact_deliveries WHERE publication_id=$1 AND consumer_revision_id=$2 AND delivered_at IS NOT NULL`, []any{claim.publicationID, consumerRevisionID}},
		} {
			tag, deleteErr := tx.Exec(ctx, deletion.query, deletion.args...)
			if deleteErr != nil || tag.RowsAffected() != 1 {
				return 0, false, false, fmt.Errorf("prune artifact catalog evidence: affected=%d err=%w", tag.RowsAffected(), deleteErr)
			}
			rows++
			remaining--
		}
	}
	for _, table := range []string{"artifact_barriers", "source_ack_retention_roots", "artifact_publication_objects"} {
		if remaining == 0 {
			break
		}
		var query string
		switch table {
		case "artifact_barriers":
			query = `WITH doomed AS (SELECT ctid FROM artifact_barriers WHERE publication_id=$1 ORDER BY ordinal LIMIT $2) DELETE FROM artifact_barriers WHERE ctid IN (SELECT ctid FROM doomed)`
		case "source_ack_retention_roots":
			query = `WITH doomed AS (SELECT ctid FROM source_ack_retention_roots WHERE flow_incarnation_id=$3 AND root_kind='artifact_publication' AND root_id=$1::text AND released_at IS NOT NULL LIMIT $2) DELETE FROM source_ack_retention_roots WHERE ctid IN (SELECT ctid FROM doomed)`
		case "artifact_publication_objects":
			query = `WITH doomed AS (SELECT ctid FROM artifact_publication_objects WHERE publication_id=$1 AND released_at IS NOT NULL ORDER BY ordinal LIMIT $2) DELETE FROM artifact_publication_objects WHERE ctid IN (SELECT ctid FROM doomed)`
		}
		args := []any{claim.publicationID, remaining}
		if table == "source_ack_retention_roots" {
			args = append(args, fence.FlowIncarnationID)
		}
		tag, deleteErr := tx.Exec(ctx, query, args...)
		if deleteErr != nil {
			return 0, false, false, deleteErr
		}
		count := int(tag.RowsAffected())
		rows += count
		remaining -= count
	}
	// Remove now-unreferenced terminal object evidence in bounded chunks.
	for remaining > 0 {
		var artifactID string
		err := tx.QueryRow(ctx, `
SELECT value FROM artifact_metadata_prune_claims AS claim,
LATERAL jsonb_array_elements_text(claim.artifact_ids)
WHERE claim.publication_id=$1
  AND EXISTS (SELECT 1 FROM artifact_objects AS object WHERE object.artifact_id=value AND object.state='deleted')
ORDER BY value LIMIT 1`, claim.publicationID).Scan(&artifactID)
		if errors.Is(err, pgx.ErrNoRows) {
			break
		}
		if err != nil {
			return 0, false, false, err
		}
		if _, err := tx.Exec(ctx, `SELECT artifact_id FROM artifact_objects WHERE artifact_id=$1 FOR UPDATE`, artifactID); err != nil {
			return 0, false, false, err
		}
		var shared, blocked bool
		if err := tx.QueryRow(ctx, `SELECT
EXISTS(SELECT 1 FROM artifact_publication_objects WHERE artifact_id=$1),
EXISTS(SELECT 1 FROM artifact_gc_claims WHERE artifact_id=$1)
 OR EXISTS(SELECT 1 FROM artifact_upload_attempts WHERE artifact_id=$1 AND attempt_state='prepared')
 OR EXISTS(SELECT 1 FROM artifact_quota_reservations WHERE artifact_id=$1 AND converted_at IS NULL AND released_at IS NULL)`, artifactID).Scan(&shared, &blocked); err != nil {
			return 0, false, false, err
		}
		if shared || blocked {
			if _, err := tx.Exec(ctx, `UPDATE artifact_metadata_prune_claims SET artifact_ids=artifact_ids-$2,updated_at=clock_timestamp() WHERE publication_id=$1`, claim.publicationID, artifactID); err != nil {
				return 0, false, false, err
			}
			continue
		}
		for _, table := range []string{"artifact_upload_attempts", "artifact_quota_reservations"} {
			if remaining == 0 {
				break
			}
			query := fmt.Sprintf(`WITH doomed AS (SELECT ctid FROM %s WHERE artifact_id=$1 LIMIT $2) DELETE FROM %s WHERE ctid IN (SELECT ctid FROM doomed)`, table, table) // #nosec G201 -- table is a closed constant list.
			tag, deleteErr := tx.Exec(ctx, query, artifactID, remaining)
			if deleteErr != nil {
				return 0, false, false, deleteErr
			}
			count := int(tag.RowsAffected())
			rows += count
			remaining -= count
		}
		if remaining == 0 {
			break
		}
		tag, deleteErr := tx.Exec(ctx, `
DELETE FROM artifact_objects WHERE artifact_id=$1 AND state='deleted'
  AND NOT EXISTS (SELECT 1 FROM artifact_publication_objects WHERE artifact_id=$1)
  AND NOT EXISTS (SELECT 1 FROM artifact_gc_claims WHERE artifact_id=$1)
  AND NOT EXISTS (SELECT 1 FROM artifact_upload_attempts WHERE artifact_id=$1)
  AND NOT EXISTS (SELECT 1 FROM artifact_quota_reservations WHERE artifact_id=$1)`, artifactID)
		if deleteErr != nil {
			return 0, false, false, deleteErr
		}
		if tag.RowsAffected() == 1 {
			rows++
			remaining--
		}
		if _, err := tx.Exec(ctx, `UPDATE artifact_metadata_prune_claims SET artifact_ids=artifact_ids-$2,updated_at=clock_timestamp() WHERE publication_id=$1`, claim.publicationID, artifactID); err != nil {
			return 0, false, false, err
		}
	}

	if remaining > 0 {
		tag, deleteErr := tx.Exec(ctx, `
DELETE FROM artifact_publications AS publication
WHERE publication.publication_id=$1::uuid
  AND NOT EXISTS (SELECT 1 FROM artifact_publication_objects WHERE publication_id=$1::uuid)
  AND NOT EXISTS (SELECT 1 FROM artifact_barriers WHERE publication_id=$1::uuid)
  AND NOT EXISTS (SELECT 1 FROM artifact_deliveries WHERE publication_id=$1::uuid)
  AND NOT EXISTS (SELECT 1 FROM artifact_delivery_attempts WHERE publication_id=$1::uuid)
  AND NOT EXISTS (SELECT 1 FROM artifact_delivery_receipts WHERE publication_id=$1::uuid)
  AND NOT EXISTS (SELECT 1 FROM artifact_consumer_checkpoints WHERE publication_id=$1::uuid)
  AND NOT EXISTS (SELECT 1 FROM authoritative_checkpoints WHERE flow_incarnation_id=$2 AND (lsn=publication.checkpoint_lsn OR metadata->>'artifact_publication_id'=($1::uuid)::text))`, claim.publicationID, fence.FlowIncarnationID)
		if deleteErr != nil {
			return 0, false, false, deleteErr
		}
		if tag.RowsAffected() == 1 {
			rows++
			remaining--
			publicationDeleted = true
		}
	}
	if publicationDeleted && remaining > 0 {
		tag, deleteErr := tx.Exec(ctx, `DELETE FROM artifact_metadata_prune_claims WHERE publication_id=$1 AND claim_epoch=$2`, claim.publicationID, claim.claimEpoch)
		if deleteErr != nil || tag.RowsAffected() != 1 {
			return 0, false, false, fmt.Errorf("delete artifact metadata claim: affected=%d err=%w", tag.RowsAffected(), deleteErr)
		}
		rows++
	}
	if rows > budget {
		return 0, false, false, errors.New("artifact metadata row budget exceeded")
	}
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return 0, false, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, false, false, err
	}
	return rows, publicationDeleted, !publicationDeleted, nil
}
