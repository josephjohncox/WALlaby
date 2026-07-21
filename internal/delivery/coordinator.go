package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type AckGrant = connector.AckGrant

type deliveryState struct {
	receipt    bool
	attemptID  uuid.UUID
	hasAttempt bool
	externalID string
}

// Coordinator implements the durable prepare -> external side effect ->
// evidence -> receipt/checkpoint protocol for managed destinations.
type Coordinator struct {
	pool *pgxpool.Pool
}

func NewCoordinator(ctx context.Context, pool *pgxpool.Pool) (*Coordinator, error) {
	if pool == nil {
		return nil, errors.New("delivery postgres pool is required")
	}
	if err := runMigrations(ctx, pool); err != nil {
		return nil, err
	}
	return &Coordinator{pool: pool}, nil
}

// RegisterDestinationRevision binds a stable revision ID to one immutable
// destination name and configuration fingerprint before external I/O.
func (c *Coordinator) RegisterDestinationRevision(ctx context.Context, fence authority.RunFence, revisionID, destinationName, configFingerprint string) error {
	if strings.TrimSpace(revisionID) == "" || strings.TrimSpace(destinationName) == "" || strings.TrimSpace(configFingerprint) == "" {
		return errors.New("destination revision id, name, and config fingerprint are required")
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin destination revision registration: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
INSERT INTO destination_revisions (destination_revision_id,destination_name,config_fingerprint)
VALUES ($1,$2,$3)
ON CONFLICT (destination_revision_id) DO NOTHING`, revisionID, destinationName, configFingerprint)
	if err != nil {
		return fmt.Errorf("register destination revision: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var existingName, existingFingerprint string
		if err := tx.QueryRow(ctx, `
SELECT destination_name,config_fingerprint
FROM destination_revisions
WHERE destination_revision_id=$1`, revisionID).Scan(&existingName, &existingFingerprint); err != nil {
			return fmt.Errorf("load destination revision: %w", err)
		}
		if existingName != destinationName || existingFingerprint != configFingerprint {
			return fmt.Errorf("%w: destination revision %q was registered with different configuration", connector.ErrDeliveryConflict, revisionID)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit destination revision registration: %w", err)
	}
	return nil
}

// AuthorizeAck advances a fenced checkpoint and creates its source ACK intent
// atomically for a source transaction that requires no external delivery.
func (c *Coordinator) AuthorizeAck(ctx context.Context, fence authority.RunFence, checkpoint connector.Checkpoint) (AckGrant, error) {
	positionID, err := connector.CheckpointPositionID(checkpoint)
	if err != nil {
		return AckGrant{}, err
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return AckGrant{}, fmt.Errorf("begin source ack authorization: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return AckGrant{}, err
	}
	checkpoint, err = finalizeCheckpointAndAck(ctx, tx, fence, positionID, checkpoint)
	if err != nil {
		return AckGrant{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AckGrant{}, fmt.Errorf("commit checkpoint and source ack intent: %w", err)
	}
	return AckGrant{Checkpoint: checkpoint, PositionID: positionID}, nil
}

// Recover reconciles one unfinished delivery. Applied evidence is adopted by
// the current fence; indeterminate evidence always fails closed.
func (c *Coordinator) Recover(ctx context.Context, fence authority.RunFence, intent connector.DeliveryIntent, checkpoint connector.Checkpoint, driver connector.ManagedDestination) (AckGrant, error) {
	if driver == nil {
		return AckGrant{}, errors.New("managed delivery driver is required")
	}
	state, err := c.inspect(ctx, fence, intent, checkpoint)
	if err != nil {
		return AckGrant{}, err
	}
	if state.receipt {
		return AckGrant{Checkpoint: checkpoint, PositionID: intent.PositionID}, nil
	}
	if !state.hasAttempt {
		return AckGrant{}, errors.New("no unfinished delivery attempt to recover")
	}
	disposition, evidence, err := driver.Reconcile(ctx, intent)
	if err != nil {
		return AckGrant{}, err
	}
	switch disposition {
	case connector.DeliveryApplied:
		if err := c.recordEvidence(ctx, fence, intent, state.attemptID, evidence); err != nil {
			return AckGrant{}, err
		}
		return c.finalize(ctx, fence, intent, state.attemptID, checkpoint)
	case connector.DeliveryNotApplied:
		return AckGrant{}, nil
	default:
		return AckGrant{}, fmt.Errorf("%w: delivery %s cannot be reconciled", connector.ErrDeliveryIndeterminate, intent.PositionID)
	}
}

// Deliver durably prepares an attempt before external I/O, reconciles any
// unfinished attempt first, and adopts evidence under the current fence.
func (c *Coordinator) Deliver(ctx context.Context, fence authority.RunFence, intent connector.DeliveryIntent, batch connector.Batch, driver connector.ManagedDestination) (AckGrant, error) {
	if driver == nil {
		return AckGrant{}, errors.New("managed delivery driver is required")
	}
	if err := validateDeliveryInput(fence, intent, batch); err != nil {
		return AckGrant{}, err
	}
	state, err := c.inspect(ctx, fence, intent, batch.Checkpoint)
	if err != nil {
		return AckGrant{}, err
	}
	if state.receipt {
		telemetry.RecordDeliveryOutcome(ctx, "receipt_reused")
		return AckGrant{Checkpoint: batch.Checkpoint, PositionID: intent.PositionID}, nil
	}
	if state.hasAttempt {
		disposition, evidence, reconcileErr := driver.Reconcile(ctx, intent)
		if reconcileErr != nil {
			return AckGrant{}, reconcileErr
		}
		switch disposition {
		case connector.DeliveryApplied:
			if err := c.recordEvidence(ctx, fence, intent, state.attemptID, evidence); err != nil {
				return AckGrant{}, err
			}
			return c.finalize(ctx, fence, intent, state.attemptID, batch.Checkpoint)
		case connector.DeliveryIndeterminate:
			return AckGrant{}, fmt.Errorf("%w: unfinished delivery %s", connector.ErrDeliveryIndeterminate, intent.PositionID)
		case connector.DeliveryNotApplied:
			// A new append-only attempt may be prepared below.
		}
	}

	attemptID, err := c.prepareAttempt(ctx, fence, intent, batch.Checkpoint)
	if err != nil {
		return AckGrant{}, err
	}
	telemetry.RecordDeliveryOutcome(ctx, "attempt_prepared")
	evidence, err := driver.Apply(ctx, intent, batch)
	if err != nil {
		if errors.Is(err, connector.ErrDeliveryIndeterminate) {
			telemetry.RecordDeliveryOutcome(ctx, "indeterminate")
		} else {
			telemetry.RecordDeliveryOutcome(ctx, "apply_failed")
		}
		return AckGrant{}, err
	}
	if err := c.recordEvidence(ctx, fence, intent, attemptID, evidence); err != nil {
		return AckGrant{}, err
	}
	return c.finalize(ctx, fence, intent, attemptID, batch.Checkpoint)
}

// ValidateAckGrant proves that PostgreSQL contains both the ACK intent and the
// current authoritative checkpoint before any source feedback is sent.
func (c *Coordinator) ValidateAckGrant(ctx context.Context, fence authority.RunFence, grant AckGrant) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin source ack validation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	if err := validateAckGrant(ctx, tx, fence, grant); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit source ack validation: %w", err)
	}
	return nil
}

// RecordAckReceipt records that the source adapter accepted an authorized ACK
// grant. observedFlushLSN is populated only when the adapter can prove an
// externally observed flush position; an empty value means scheduled feedback.
// It never authorizes a position that lacks the corresponding ACK intent.
func (c *Coordinator) RecordAckReceipt(ctx context.Context, fence authority.RunFence, grant AckGrant, observedFlushLSN string) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin source ack receipt: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	if err := validateAckGrant(ctx, tx, fence, grant); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO source_ack_receipts (
  flow_incarnation_id,position_id,checkpoint_lsn,observed_flush_lsn,acquisition_id,lease_epoch
) VALUES ($1,$2,$3,NULLIF($4,''),$5,$6)
ON CONFLICT (flow_incarnation_id,position_id) DO UPDATE SET
  observed_flush_lsn=COALESCE(EXCLUDED.observed_flush_lsn,source_ack_receipts.observed_flush_lsn),
  acquisition_id=EXCLUDED.acquisition_id,
  lease_epoch=EXCLUDED.lease_epoch,
  recorded_at=clock_timestamp()
WHERE source_ack_receipts.checkpoint_lsn=EXCLUDED.checkpoint_lsn`, fence.FlowIncarnationID, grant.PositionID, grant.Checkpoint.LSN, observedFlushLSN, fence.AcquisitionID, fence.LeaseEpoch); err != nil {
		return fmt.Errorf("record source ack receipt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit source ack receipt: %w", err)
	}
	return nil
}

func validateAckGrant(ctx context.Context, tx pgx.Tx, fence authority.RunFence, grant AckGrant) error {
	if grant.PositionID == "" || grant.Checkpoint.LSN == "" {
		return errors.New("source ack grant requires position ID and checkpoint LSN")
	}
	var authorizedLSN, checkpointLSN string
	if err := tx.QueryRow(ctx, `
SELECT intent.checkpoint_lsn,checkpoint.lsn
FROM source_ack_intents AS intent
JOIN authoritative_checkpoints AS checkpoint
  ON checkpoint.flow_incarnation_id=intent.flow_incarnation_id
WHERE intent.flow_incarnation_id=$1 AND intent.position_id=$2`, fence.FlowIncarnationID, grant.PositionID).Scan(&authorizedLSN, &checkpointLSN); err != nil {
		return fmt.Errorf("load source ack authorization: %w", err)
	}
	canonical, err := connector.CanonicalizeCheckpointPosition(grant.Checkpoint.LSN)
	if err != nil {
		return fmt.Errorf("canonicalize source ack grant: %w", err)
	}
	if authorizedLSN != canonical || checkpointLSN != canonical {
		return fmt.Errorf("source ack grant %s does not match intent=%s checkpoint=%s", canonical, authorizedLSN, checkpointLSN)
	}
	return nil
}

func validateDeliveryInput(fence authority.RunFence, intent connector.DeliveryIntent, batch connector.Batch) error {
	if err := intent.Validate(); err != nil {
		return err
	}
	if intent.FlowID != fence.FlowID || intent.FlowIncarnationID != fence.FlowIncarnationID.String() || intent.Generation != fence.Generation || intent.AcquisitionID != fence.AcquisitionID.String() || intent.LeaseEpoch != fence.LeaseEpoch {
		return fmt.Errorf("%w: delivery intent does not match run fence", authority.ErrFenceRejected)
	}
	hash, err := connector.BatchContentHash(batch)
	if err != nil {
		return err
	}
	if hash != intent.ContentHash {
		return fmt.Errorf("%w: delivery content hash mismatch", connector.ErrDeliveryConflict)
	}
	positionID, err := connector.CheckpointPositionID(batch.Checkpoint)
	if err != nil {
		return err
	}
	if positionID != intent.PositionID {
		return fmt.Errorf("%w: intent position %s does not match checkpoint position %s", connector.ErrDeliveryConflict, intent.PositionID, positionID)
	}
	return nil
}

func (c *Coordinator) inspect(ctx context.Context, fence authority.RunFence, intent connector.DeliveryIntent, checkpoint connector.Checkpoint) (deliveryState, error) {
	if err := intent.Validate(); err != nil {
		return deliveryState{}, err
	}
	if intent.FlowIncarnationID != fence.FlowIncarnationID.String() || intent.Generation != fence.Generation || intent.AcquisitionID != fence.AcquisitionID.String() || intent.LeaseEpoch != fence.LeaseEpoch {
		return deliveryState{}, fmt.Errorf("%w: delivery intent does not match run fence", authority.ErrFenceRejected)
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return deliveryState{}, fmt.Errorf("begin delivery inspection: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return deliveryState{}, err
	}
	if err := ensureManifest(ctx, tx, fence, intent, checkpoint); err != nil {
		return deliveryState{}, err
	}
	state := deliveryState{}
	err = tx.QueryRow(ctx, `
SELECT attempt_id,external_id
FROM delivery_receipts
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND source_lineage_id=$3 AND position_id=$4`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, intent.PositionID).Scan(&state.attemptID, &state.externalID)
	switch {
	case err == nil:
		state.receipt = true
	case errors.Is(err, pgx.ErrNoRows):
		err = tx.QueryRow(ctx, `
SELECT attempt.attempt_id
FROM delivery_attempts AS attempt
LEFT JOIN delivery_receipts AS receipt ON receipt.attempt_id=attempt.attempt_id
WHERE attempt.flow_incarnation_id=$1
  AND attempt.destination_revision_id=$2
  AND attempt.source_lineage_id=$3
  AND attempt.position_id=$4
  AND receipt.attempt_id IS NULL
ORDER BY attempt.prepared_at DESC,attempt.attempt_id DESC
LIMIT 1`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, intent.PositionID).Scan(&state.attemptID)
		if err == nil {
			state.hasAttempt = true
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return deliveryState{}, fmt.Errorf("load unfinished delivery attempt: %w", err)
		}
	default:
		return deliveryState{}, fmt.Errorf("load delivery receipt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return deliveryState{}, fmt.Errorf("commit delivery inspection: %w", err)
	}
	return state, nil
}

func ensureManifest(ctx context.Context, tx pgx.Tx, fence authority.RunFence, intent connector.DeliveryIntent, checkpoint connector.Checkpoint) error {
	var registered int
	if err := tx.QueryRow(ctx, `
SELECT 1 FROM destination_revisions WHERE destination_revision_id=$1`, intent.DestinationRevisionID).Scan(&registered); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: destination revision %q is not registered", connector.ErrDeliveryConflict, intent.DestinationRevisionID)
		}
		return fmt.Errorf("validate destination revision: %w", err)
	}
	position, err := connector.CanonicalizeCheckpointPosition(checkpoint.LSN)
	if err != nil {
		return err
	}
	sourceTransactionID := intent.SourceLineageID + ":" + position
	tag, err := tx.Exec(ctx, `
INSERT INTO delivery_manifests (
  flow_incarnation_id,destination_revision_id,source_lineage_id,position_id,source_transaction_id,content_hash,checkpoint_lsn
) VALUES ($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (flow_incarnation_id,destination_revision_id,position_id) DO NOTHING`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, intent.PositionID, sourceTransactionID, intent.ContentHash, position)
	if err != nil {
		return fmt.Errorf("insert delivery manifest: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var existingHash, existingLSN, existingLineage string
		if err := tx.QueryRow(ctx, `
SELECT content_hash,checkpoint_lsn,source_lineage_id FROM delivery_manifests
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND position_id=$3`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.PositionID).Scan(&existingHash, &existingLSN, &existingLineage); err != nil {
			return fmt.Errorf("load delivery manifest: %w", err)
		}
		if existingHash != intent.ContentHash || existingLSN != position || existingLineage != intent.SourceLineageID {
			return fmt.Errorf("%w: immutable delivery manifest differs", connector.ErrDeliveryConflict)
		}
	}
	return nil
}

func (c *Coordinator) prepareAttempt(ctx context.Context, fence authority.RunFence, intent connector.DeliveryIntent, checkpoint connector.Checkpoint) (uuid.UUID, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("begin delivery attempt: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return uuid.Nil, err
	}
	if err := ensureManifest(ctx, tx, fence, intent, checkpoint); err != nil {
		return uuid.Nil, err
	}
	attemptID := uuid.New()
	if _, err := tx.Exec(ctx, `
INSERT INTO delivery_attempts (
  attempt_id,flow_incarnation_id,flow_id,generation,acquisition_id,lease_epoch,
  destination_revision_id,source_lineage_id,position_id,content_hash
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, attemptID, fence.FlowIncarnationID, fence.FlowID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, intent.DestinationRevisionID, intent.SourceLineageID, intent.PositionID, intent.ContentHash); err != nil {
		return uuid.Nil, fmt.Errorf("prepare delivery attempt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, fmt.Errorf("commit delivery attempt: %w", err)
	}
	return attemptID, nil
}

func (c *Coordinator) recordEvidence(ctx context.Context, fence authority.RunFence, intent connector.DeliveryIntent, attemptID uuid.UUID, evidence connector.DeliveryEvidence) error {
	if evidence.ExternalID == "" || evidence.ContentHash != intent.ContentHash {
		return fmt.Errorf("%w: destination evidence is incomplete or mismatched", connector.ErrDeliveryConflict)
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin delivery evidence: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	var attemptHash string
	if err := tx.QueryRow(ctx, `
SELECT content_hash FROM delivery_attempts
WHERE attempt_id=$1 AND flow_incarnation_id=$2 AND destination_revision_id=$3 AND position_id=$4`, attemptID, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.PositionID).Scan(&attemptHash); err != nil {
		return fmt.Errorf("load delivery attempt for evidence: %w", err)
	}
	if attemptHash != intent.ContentHash {
		return fmt.Errorf("%w: delivery attempt content differs", connector.ErrDeliveryConflict)
	}
	tag, err := tx.Exec(ctx, `
INSERT INTO delivery_attempt_evidence (
  attempt_id,external_id,content_hash,recorded_by_acquisition_id,recorded_by_lease_epoch
) VALUES ($1,$2,$3,$4,$5)
ON CONFLICT (attempt_id) DO NOTHING`, attemptID, evidence.ExternalID, evidence.ContentHash, fence.AcquisitionID, fence.LeaseEpoch)
	if err != nil {
		return fmt.Errorf("record delivery evidence: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var existingExternal, existingHash string
		if err := tx.QueryRow(ctx, `SELECT external_id,content_hash FROM delivery_attempt_evidence WHERE attempt_id=$1`, attemptID).Scan(&existingExternal, &existingHash); err != nil {
			return err
		}
		if existingExternal != evidence.ExternalID || existingHash != evidence.ContentHash {
			return fmt.Errorf("%w: attempt evidence differs", connector.ErrDeliveryConflict)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit delivery evidence: %w", err)
	}
	return nil
}

func (c *Coordinator) finalize(ctx context.Context, fence authority.RunFence, intent connector.DeliveryIntent, attemptID uuid.UUID, checkpoint connector.Checkpoint) (AckGrant, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return AckGrant{}, fmt.Errorf("begin delivery finalization: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return AckGrant{}, err
	}
	var externalID, contentHash string
	if err := tx.QueryRow(ctx, `
SELECT evidence.external_id,evidence.content_hash
FROM delivery_attempt_evidence AS evidence
JOIN delivery_attempts AS attempt ON attempt.attempt_id=evidence.attempt_id
WHERE evidence.attempt_id=$1
  AND attempt.flow_incarnation_id=$2
  AND attempt.destination_revision_id=$3
  AND attempt.source_lineage_id=$4
  AND attempt.position_id=$5`, attemptID, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, intent.PositionID).Scan(&externalID, &contentHash); err != nil {
		return AckGrant{}, fmt.Errorf("load delivery evidence for receipt: %w", err)
	}
	if contentHash != intent.ContentHash {
		return AckGrant{}, fmt.Errorf("%w: receipt evidence content differs", connector.ErrDeliveryConflict)
	}
	tag, err := tx.Exec(ctx, `
INSERT INTO delivery_receipts (
  flow_incarnation_id,destination_revision_id,source_lineage_id,position_id,content_hash,attempt_id,external_id,
  adopted_by_acquisition_id,adopted_by_lease_epoch
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT (flow_incarnation_id,destination_revision_id,position_id) DO UPDATE SET
  source_lineage_id=EXCLUDED.source_lineage_id
WHERE delivery_receipts.source_lineage_id=EXCLUDED.source_lineage_id
  AND delivery_receipts.content_hash=EXCLUDED.content_hash
  AND delivery_receipts.external_id=EXCLUDED.external_id`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, intent.PositionID, contentHash, attemptID, externalID, fence.AcquisitionID, fence.LeaseEpoch)
	if err != nil {
		return AckGrant{}, fmt.Errorf("adopt delivery receipt: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return AckGrant{}, fmt.Errorf("%w: immutable delivery receipt differs", connector.ErrDeliveryConflict)
	}

	checkpoint, err = finalizeCheckpointAndAck(ctx, tx, fence, intent.PositionID, checkpoint)
	if err != nil {
		return AckGrant{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return AckGrant{}, fmt.Errorf("commit delivery receipt checkpoint and ack intent: %w", err)
	}
	telemetry.RecordDeliveryOutcome(ctx, "receipt_committed")
	return AckGrant{Checkpoint: checkpoint, PositionID: intent.PositionID}, nil
}

func finalizeCheckpointAndAck(ctx context.Context, tx pgx.Tx, fence authority.RunFence, positionID string, checkpoint connector.Checkpoint) (connector.Checkpoint, error) {
	canonicalLSN, err := connector.CanonicalizeCheckpointPosition(checkpoint.LSN)
	if err != nil {
		return connector.Checkpoint{}, err
	}
	metadata := checkpoint.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return connector.Checkpoint{}, err
	}
	var currentLSN string
	err = tx.QueryRow(ctx, `SELECT lsn FROM authoritative_checkpoints WHERE flow_incarnation_id=$1 FOR UPDATE`, fence.FlowIncarnationID).Scan(&currentLSN)
	if err == nil {
		comparison, compareErr := connector.CompareCheckpointLSN(currentLSN, canonicalLSN)
		if compareErr != nil {
			return connector.Checkpoint{}, compareErr
		}
		if comparison > 0 {
			return connector.Checkpoint{}, fmt.Errorf("checkpoint regression from %s to %s", currentLSN, canonicalLSN)
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return connector.Checkpoint{}, fmt.Errorf("load authoritative checkpoint: %w", err)
	}
	updatedAt := checkpoint.Timestamp
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO authoritative_checkpoints (
  flow_incarnation_id,flow_id,generation,acquisition_id,lease_epoch,lsn,metadata,updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (flow_incarnation_id) DO UPDATE SET
  generation=EXCLUDED.generation,
  acquisition_id=EXCLUDED.acquisition_id,
  lease_epoch=EXCLUDED.lease_epoch,
  lsn=EXCLUDED.lsn,
  metadata=EXCLUDED.metadata,
  updated_at=EXCLUDED.updated_at`, fence.FlowIncarnationID, fence.FlowID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, canonicalLSN, metadataJSON, updatedAt); err != nil {
		return connector.Checkpoint{}, fmt.Errorf("finalize authoritative checkpoint: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO source_ack_intents (
  flow_incarnation_id,position_id,checkpoint_lsn,generation,acquisition_id,lease_epoch
) VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (flow_incarnation_id,position_id) DO NOTHING`, fence.FlowIncarnationID, positionID, canonicalLSN, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch); err != nil {
		return connector.Checkpoint{}, fmt.Errorf("authorize source ack intent: %w", err)
	}
	checkpoint.LSN = canonicalLSN
	checkpoint.Timestamp = updatedAt
	return checkpoint, nil
}
