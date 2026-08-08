package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// FencedStore is the authoritative checkpoint/outbox contract used by managed
// execution. Every mutation validates the exact producer lease in the same
// PostgreSQL transaction.
type FencedStore interface {
	GetFenced(context.Context, authority.RunFence) (connector.Checkpoint, error)
	PutFenced(context.Context, authority.RunFence, connector.Checkpoint) error
	PersistCheckpointAndOutboxFenced(context.Context, authority.RunFence, connector.Checkpoint, []connector.OutboxEntry) error
	ListOutboxFenced(context.Context, authority.RunFence) ([]connector.OutboxEntry, error)
	CompleteOutboxFenced(context.Context, authority.RunFence, string, string) error
}

func (p *PostgresStore) GetFenced(ctx context.Context, fence authority.RunFence) (connector.Checkpoint, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return connector.Checkpoint{}, fmt.Errorf("begin fenced checkpoint read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return connector.Checkpoint{}, err
	}
	checkpoint, err := scanCheckpoint(tx.QueryRow(ctx, `
SELECT lsn, metadata, updated_at
FROM authoritative_checkpoints
WHERE flow_incarnation_id = $1`, fence.FlowIncarnationID))
	if err != nil {
		return connector.Checkpoint{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return connector.Checkpoint{}, fmt.Errorf("commit fenced checkpoint read: %w", err)
	}
	return checkpoint, nil
}

func (p *PostgresStore) PutFenced(ctx context.Context, fence authority.RunFence, checkpoint connector.Checkpoint) error {
	canonical, err := canonicalizeCheckpoint(checkpoint)
	if err != nil {
		return err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin fenced checkpoint: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := putFencedCheckpoint(ctx, tx, fence, canonical); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit fenced checkpoint: %w", err)
	}
	return nil
}

func (p *PostgresStore) PersistCheckpointAndOutboxFenced(ctx context.Context, fence authority.RunFence, checkpoint connector.Checkpoint, entries []connector.OutboxEntry) error {
	canonical, err := canonicalizeCheckpoint(checkpoint)
	if err != nil {
		return err
	}
	encoded, err := encodeOutboxEntries(fence.FlowID, canonical, entries)
	if err != nil {
		return err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin fenced checkpoint outbox: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := putFencedCheckpoint(ctx, tx, fence, canonical); err != nil {
		return err
	}
	for _, item := range encoded {
		var positionHash string
		err := tx.QueryRow(ctx, `
SELECT batch_hash
FROM authoritative_checkpoint_outbox
WHERE flow_incarnation_id=$1 AND position_id=$2
LIMIT 1`, fence.FlowIncarnationID, item.entry.PositionID).Scan(&positionHash)
		switch {
		case err == nil && positionHash != item.batchHash:
			return fmt.Errorf("%w: incarnation=%s position=%s identifies different batches", connector.ErrOutboxConflict, fence.FlowIncarnationID, item.entry.PositionID)
		case err == nil, errors.Is(err, pgx.ErrNoRows):
		default:
			return fmt.Errorf("read fenced outbox identity: %w", err)
		}
		tag, err := tx.Exec(ctx, `
INSERT INTO authoritative_checkpoint_outbox (
  flow_incarnation_id, flow_id, generation, acquisition_id, lease_epoch,
  destination_id, position_id, batch_hash, projection_fingerprint, codec, batch_json, created_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
ON CONFLICT (flow_incarnation_id,destination_id,position_id) DO NOTHING`,
			fence.FlowIncarnationID,
			fence.FlowID,
			fence.Generation,
			fence.AcquisitionID,
			fence.LeaseEpoch,
			item.entry.Destination,
			item.entry.PositionID,
			item.batchHash,
			item.entry.ProjectionFingerprint,
			outboxCodecGobV1,
			item.batchData,
			item.entry.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("insert fenced outbox entry for %s: %w", item.entry.Destination, err)
		}
		if tag.RowsAffected() == 0 {
			var existingHash, existingProjection string
			if err := tx.QueryRow(ctx, `
SELECT batch_hash,projection_fingerprint
FROM authoritative_checkpoint_outbox
WHERE flow_incarnation_id=$1 AND destination_id=$2 AND position_id=$3`, fence.FlowIncarnationID, item.entry.Destination, item.entry.PositionID).Scan(&existingHash, &existingProjection); err != nil {
				return fmt.Errorf("read existing fenced outbox entry for %s: %w", item.entry.Destination, err)
			}
			if existingHash != item.batchHash || existingProjection != item.entry.ProjectionFingerprint {
				return fmt.Errorf("%w: incarnation=%s destination=%s position=%s", connector.ErrOutboxConflict, fence.FlowIncarnationID, item.entry.Destination, item.entry.PositionID)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit fenced checkpoint outbox: %w", err)
	}
	return nil
}

func (p *PostgresStore) ListOutboxFenced(ctx context.Context, fence authority.RunFence) ([]connector.OutboxEntry, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin fenced outbox read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
SELECT destination_id,position_id,batch_hash,projection_fingerprint,replay_order,codec,batch_json,created_at
FROM authoritative_checkpoint_outbox
WHERE flow_incarnation_id=$1 AND delivered_at IS NULL
ORDER BY replay_order`, fence.FlowIncarnationID)
	if err != nil {
		return nil, fmt.Errorf("list fenced checkpoint outbox: %w", err)
	}
	defer rows.Close()
	entries := make([]connector.OutboxEntry, 0)
	for rows.Next() {
		var destination, position, batchHash, projectionFingerprint, codec string
		var replayOrder int64
		var batchData []byte
		var createdAt time.Time
		if err := rows.Scan(&destination, &position, &batchHash, &projectionFingerprint, &replayOrder, &codec, &batchData, &createdAt); err != nil {
			return nil, fmt.Errorf("scan fenced checkpoint outbox: %w", err)
		}
		batch, err := decodeOutboxBatch(codec, batchData)
		if err != nil {
			return nil, err
		}
		entries = append(entries, connector.OutboxEntry{
			FlowID: fence.FlowID, Destination: destination, PositionID: position,
			BatchHash: batchHash, ProjectionFingerprint: projectionFingerprint, ReplayOrder: replayOrder, Batch: batch, CreatedAt: createdAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate fenced checkpoint outbox: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit fenced outbox read: %w", err)
	}
	return entries, nil
}

func (p *PostgresStore) CompleteOutboxFenced(ctx context.Context, fence authority.RunFence, destination, positionID string) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin fenced outbox completion: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
UPDATE authoritative_checkpoint_outbox
SET delivered_at=clock_timestamp(), generation=$4, acquisition_id=$5, lease_epoch=$6
WHERE flow_incarnation_id=$1 AND destination_id=$2 AND position_id=$3 AND delivered_at IS NULL`, fence.FlowIncarnationID, destination, positionID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch)
	if err != nil {
		return fmt.Errorf("complete fenced outbox entry: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("complete fenced outbox entry destination=%s position=%s: pending entry not found", destination, positionID)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit fenced outbox completion: %w", err)
	}
	return nil
}

func putFencedCheckpoint(ctx context.Context, tx pgx.Tx, fence authority.RunFence, checkpoint connector.Checkpoint) error {
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", fence.FlowIncarnationID.String()); err != nil {
		return fmt.Errorf("lock fenced checkpoint incarnation: %w", err)
	}
	if checkpoint.Timestamp.IsZero() {
		checkpoint.Timestamp = time.Now().UTC()
	}
	if checkpoint.Metadata == nil {
		checkpoint.Metadata = map[string]string{}
	}
	var current connector.Checkpoint
	var currentMetadata []byte
	err := tx.QueryRow(ctx, `
SELECT lsn,metadata,updated_at FROM authoritative_checkpoints
WHERE flow_incarnation_id=$1
FOR UPDATE`, fence.FlowIncarnationID).Scan(&current.LSN, &currentMetadata, &current.Timestamp)
	switch {
	case err == nil:
		if err := validateCheckpointAdvance(fence.FlowID, current.LSN, checkpoint.LSN); err != nil {
			return err
		}
		comparison, compareErr := connector.CompareCheckpointLSN(current.LSN, checkpoint.LSN)
		if compareErr != nil {
			return compareErr
		}
		if comparison == 0 {
			if len(currentMetadata) > 0 {
				if err := json.Unmarshal(currentMetadata, &current.Metadata); err != nil {
					return fmt.Errorf("decode current fenced checkpoint metadata: %w", err)
				}
			}
			if current.Metadata == nil {
				current.Metadata = map[string]string{}
			}
			// Equal-position writes may rebind the current fence ownership, but
			// caller metadata and timestamps must never replace authority payload.
			checkpoint = current
		}
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return fmt.Errorf("read current fenced checkpoint: %w", err)
	}
	metadataJSON, err := json.Marshal(checkpoint.Metadata)
	if err != nil {
		return fmt.Errorf("marshal fenced checkpoint metadata: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO authoritative_checkpoints (
  flow_incarnation_id,flow_id,generation,acquisition_id,lease_epoch,lsn,metadata,updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (flow_incarnation_id) DO UPDATE SET
  flow_id=EXCLUDED.flow_id,
  generation=EXCLUDED.generation,
  acquisition_id=EXCLUDED.acquisition_id,
  lease_epoch=EXCLUDED.lease_epoch,
  lsn=EXCLUDED.lsn,
  metadata=EXCLUDED.metadata,
  updated_at=EXCLUDED.updated_at`, fence.FlowIncarnationID, fence.FlowID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, checkpoint.LSN, metadataJSON, checkpoint.Timestamp); err != nil {
		return fmt.Errorf("upsert fenced checkpoint: %w", err)
	}
	return nil
}

var _ FencedStore = (*PostgresStore)(nil)
