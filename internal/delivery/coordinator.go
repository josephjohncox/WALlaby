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
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type AckGrant = connector.AckGrant

const (
	maxDeliveryAttempts       = 16
	maxReconciliationAttempts = 16
)

type deliveryState struct {
	receipt                   bool
	authoritativeCheckpoint   connector.Checkpoint
	authoritativeCheckpointID string
	attemptID                 uuid.UUID
	hasAttempt                bool
	attemptState              string
	attemptNumber             int
	reconciliationAttempts    int
	nextAttemptAt             time.Time
	externalID                string
}

// Coordinator implements the durable prepare -> external side effect ->
// evidence -> receipt/checkpoint protocol for managed destinations.
type Coordinator struct {
	pool  *pgxpool.Pool
	hooks CoordinatorHooks
}

// CoordinatorHooks exposes deterministic crash boundaries to integration
// tests without changing production ordering or relying on timing.
type CoordinatorHooks struct {
	AfterTargetApply       func(context.Context, authority.RunFence, connector.DeliveryIntent) error
	AfterSourceFlush       func(context.Context, authority.RunFence, AckGrant, string) error
	AfterRetentionRootLock func(context.Context, authority.RunFence, string) error
}

// CoordinatorOption configures optional coordinator behavior.
type CoordinatorOption func(*Coordinator)

// WithCoordinatorHooks installs deterministic failure-injection hooks.
func WithCoordinatorHooks(hooks CoordinatorHooks) CoordinatorOption {
	return func(coordinator *Coordinator) {
		coordinator.hooks = hooks
	}
}

func NewCoordinator(ctx context.Context, pool *pgxpool.Pool, options ...CoordinatorOption) (*Coordinator, error) {
	if pool == nil {
		return nil, errors.New("delivery postgres pool is required")
	}
	// Delivery finalization writes the authoritative checkpoint and ACK-intent
	// tables directly, so the coordinator owns this migration dependency when
	// constructed outside centralized production startup as well.
	if err := checkpoint.ApplyMigrations(ctx, pool); err != nil {
		return nil, fmt.Errorf("prepare delivery checkpoint authority: %w", err)
	}
	if err := runMigrations(ctx, pool); err != nil {
		return nil, err
	}
	coordinator := &Coordinator{pool: pool}
	for _, option := range options {
		if option != nil {
			option(coordinator)
		}
	}
	return coordinator, nil
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
// the current fence; indeterminate evidence is durably backed off and bounded.
func (c *Coordinator) Recover(ctx context.Context, fence authority.RunFence, intent connector.DeliveryIntent, checkpoint connector.Checkpoint, driver connector.ManagedDestination) (AckGrant, error) {
	if driver == nil {
		return AckGrant{}, errors.New("managed delivery driver is required")
	}
	state, err := c.inspect(ctx, fence, intent, checkpoint, manifestLookupOnly)
	if err != nil {
		return AckGrant{}, err
	}
	if state.receipt {
		return AckGrant{Checkpoint: state.authoritativeCheckpoint, PositionID: state.authoritativeCheckpointID}, nil
	}
	if !state.hasAttempt {
		return AckGrant{}, errors.New("no unfinished delivery attempt to recover")
	}
	disposition, evidence, err := c.reconcileUnfinished(ctx, fence, intent, state, driver)
	if err != nil {
		return AckGrant{}, err
	}
	switch disposition {
	case connector.DeliveryApplied:
		if err := c.recordEvidence(ctx, fence, intent, state.attemptID, evidence); err != nil {
			return AckGrant{}, recoverablePostCommitError("record recovered delivery evidence", err)
		}
		if err := c.markAttemptTerminal(ctx, fence, state.attemptID, "applied", ""); err != nil {
			return AckGrant{}, recoverablePostCommitError("mark recovered delivery applied", err)
		}
		grant, err := c.finalize(ctx, fence, intent, state.attemptID)
		return grant, recoverablePostCommitError("finalize recovered delivery", err)
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
	state, err := c.inspect(ctx, fence, intent, batch.Checkpoint, manifestCreateOrValidate)
	if err != nil {
		return AckGrant{}, err
	}
	if state.receipt {
		telemetry.RecordDeliveryOutcome(ctx, "receipt_reused")
		return AckGrant{Checkpoint: state.authoritativeCheckpoint, PositionID: state.authoritativeCheckpointID}, nil
	}
	if state.hasAttempt {
		disposition, evidence, err := c.reconcileUnfinished(ctx, fence, intent, state, driver)
		if err != nil {
			return AckGrant{}, err
		}
		switch disposition {
		case connector.DeliveryApplied:
			if err := c.recordEvidence(ctx, fence, intent, state.attemptID, evidence); err != nil {
				return AckGrant{}, recoverablePostCommitError("record reconciled delivery evidence", err)
			}
			if err := c.markAttemptTerminal(ctx, fence, state.attemptID, "applied", ""); err != nil {
				return AckGrant{}, recoverablePostCommitError("mark reconciled delivery applied", err)
			}
			grant, err := c.finalize(ctx, fence, intent, state.attemptID)
			return grant, recoverablePostCommitError("finalize reconciled delivery", err)
		case connector.DeliveryNotApplied:
			if err := c.markAttemptTerminal(ctx, fence, state.attemptID, "not_applied", "target marker absent"); err != nil {
				return AckGrant{}, err
			}
			retryState, err := c.inspect(ctx, fence, intent, batch.Checkpoint, manifestCreateOrValidate)
			if err != nil {
				return AckGrant{}, err
			}
			if err := waitForDeliveryRetry(ctx, retryState.nextAttemptAt); err != nil {
				return AckGrant{}, err
			}
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
			_ = c.markAttemptTerminal(context.WithoutCancel(ctx), fence, attemptID, "failed", err.Error())
		}
		return AckGrant{}, err
	}
	if c.hooks.AfterTargetApply != nil {
		if err := c.hooks.AfterTargetApply(ctx, fence, intent); err != nil {
			return AckGrant{}, recoverablePostCommitError("after target apply", err)
		}
	}
	if err := c.recordEvidence(ctx, fence, intent, attemptID, evidence); err != nil {
		return AckGrant{}, recoverablePostCommitError("record delivery evidence", err)
	}
	if err := c.markAttemptTerminal(ctx, fence, attemptID, "applied", ""); err != nil {
		return AckGrant{}, recoverablePostCommitError("mark delivery applied", err)
	}
	grant, err := c.finalize(ctx, fence, intent, attemptID)
	return grant, recoverablePostCommitError("finalize delivery", err)
}

func recoverablePostCommitError(stage string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %s: %w", connector.ErrDeliveryIndeterminate, stage, err)
}

// DeliverTransaction applies one complete committed source transaction. The
// immutable logical batch and attempt are durable before target I/O, while the
// target receipt, checkpoint, and ACK intent are finalized under one fence.
func (c *Coordinator) DeliverTransaction(ctx context.Context, fence authority.RunFence, intent connector.DeliveryIntent, transaction connector.SourceTransaction, driver connector.ManagedTransactionDestination) (AckGrant, error) {
	if driver == nil {
		return AckGrant{}, errors.New("managed transaction delivery driver is required")
	}
	if err := validateTransactionDeliveryInput(fence, intent, transaction); err != nil {
		return AckGrant{}, err
	}
	state, err := c.inspect(ctx, fence, intent, transaction.Checkpoint, manifestCreateOrValidate)
	if err != nil {
		return AckGrant{}, err
	}
	if state.receipt {
		telemetry.RecordDeliveryOutcome(ctx, "receipt_reused")
		return AckGrant{Checkpoint: state.authoritativeCheckpoint, PositionID: state.authoritativeCheckpointID}, nil
	}
	if state.hasAttempt {
		disposition, evidence, err := c.reconcileUnfinished(ctx, fence, intent, state, driver)
		if err != nil {
			return AckGrant{}, err
		}
		switch disposition {
		case connector.DeliveryApplied:
			if err := c.recordEvidence(ctx, fence, intent, state.attemptID, evidence); err != nil {
				return AckGrant{}, recoverablePostCommitError("record reconciled transaction evidence", err)
			}
			if err := c.markAttemptTerminal(ctx, fence, state.attemptID, "applied", ""); err != nil {
				return AckGrant{}, recoverablePostCommitError("mark reconciled transaction applied", err)
			}
			grant, err := c.finalize(ctx, fence, intent, state.attemptID)
			return grant, recoverablePostCommitError("finalize reconciled transaction", err)
		case connector.DeliveryNotApplied:
			if err := c.markAttemptTerminal(ctx, fence, state.attemptID, "not_applied", "target marker absent"); err != nil {
				return AckGrant{}, err
			}
			retryState, err := c.inspect(ctx, fence, intent, transaction.Checkpoint, manifestCreateOrValidate)
			if err != nil {
				return AckGrant{}, err
			}
			if err := waitForDeliveryRetry(ctx, retryState.nextAttemptAt); err != nil {
				return AckGrant{}, err
			}
		}
	}
	var prepared connector.PreparedManagedTransaction
	if preparer, ok := driver.(connector.ManagedTransactionPreparer); ok {
		prepared, err = preparer.PrepareTransaction(ctx, intent, transaction)
	} else {
		err = driver.ValidateTransaction(ctx, transaction)
	}
	if err != nil {
		return AckGrant{}, fmt.Errorf("validate managed target transaction: %w", err)
	}

	attemptID, err := c.prepareAttempt(ctx, fence, intent, transaction.Checkpoint)
	if err != nil {
		return AckGrant{}, err
	}
	telemetry.RecordDeliveryOutcome(ctx, "attempt_prepared")
	var evidence connector.DeliveryEvidence
	if prepared != nil {
		evidence, err = prepared.Apply(ctx)
	} else {
		evidence, err = driver.ApplyTransaction(ctx, intent, transaction)
	}
	if err != nil {
		if errors.Is(err, connector.ErrDeliveryIndeterminate) {
			telemetry.RecordDeliveryOutcome(ctx, "indeterminate")
		} else {
			telemetry.RecordDeliveryOutcome(ctx, "apply_failed")
			_ = c.markAttemptTerminal(context.WithoutCancel(ctx), fence, attemptID, "failed", err.Error())
		}
		return AckGrant{}, err
	}
	if c.hooks.AfterTargetApply != nil {
		if err := c.hooks.AfterTargetApply(ctx, fence, intent); err != nil {
			return AckGrant{}, recoverablePostCommitError("after target transaction apply", err)
		}
	}
	if err := c.recordEvidence(ctx, fence, intent, attemptID, evidence); err != nil {
		return AckGrant{}, recoverablePostCommitError("record transaction evidence", err)
	}
	if err := c.markAttemptTerminal(ctx, fence, attemptID, "applied", ""); err != nil {
		return AckGrant{}, recoverablePostCommitError("mark transaction applied", err)
	}
	grant, err := c.finalize(ctx, fence, intent, attemptID)
	return grant, recoverablePostCommitError("finalize transaction", err)
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
	if err := recordAckReceipt(ctx, tx, fence, grant, observedFlushLSN); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit source ack receipt: %w", err)
	}
	return nil
}

// CommitSourceFeedback validates authority on both sides of external source
// I/O. PostgreSQL slot feedback is monotonic, so a crash or takeover after the
// source flush is repaired by re-sending the authoritative checkpoint; no
// control transaction or advisory lock is held while waiting on the source.
func (c *Coordinator) CommitSourceFeedback(ctx context.Context, fence authority.RunFence, grant AckGrant, source connector.FlushEvidenceSource) error {
	if source == nil {
		return errors.New("managed source flush evidence is required")
	}
	if err := c.ValidateAckGrant(ctx, fence, grant); err != nil {
		return err
	}
	evidence, err := source.AckWithEvidence(ctx, grant.Checkpoint)
	if err != nil {
		return fmt.Errorf("send authorized source feedback: %w", err)
	}
	observed, err := connector.CanonicalizeCheckpointPosition(evidence.ObservedFlushLSN)
	if err != nil {
		return fmt.Errorf("canonicalize observed source flush: %w", err)
	}
	authorized, err := connector.CanonicalizeCheckpointPosition(grant.Checkpoint.LSN)
	if err != nil {
		return fmt.Errorf("canonicalize authorized source flush: %w", err)
	}
	if observed != authorized {
		return fmt.Errorf("observed source flush %s differs from authorized checkpoint %s", observed, authorized)
	}
	if c.hooks.AfterSourceFlush != nil {
		if err := c.hooks.AfterSourceFlush(ctx, fence, grant, observed); err != nil {
			return err
		}
	}
	if err := c.RecordAckReceipt(ctx, fence, grant, observed); err != nil {
		return fmt.Errorf("commit authorized source feedback receipt: %w", err)
	}
	return nil
}

func recordAckReceipt(ctx context.Context, tx pgx.Tx, fence authority.RunFence, grant AckGrant, observedFlushLSN string) error {
	if _, err := tx.Exec(ctx, `
INSERT INTO source_ack_receipts (
  flow_incarnation_id,position_id,checkpoint_lsn,observed_flush_lsn,acquisition_id,lease_epoch,generation
) VALUES ($1,$2,$3,NULLIF($4,''),$5,$6,$7)
ON CONFLICT (flow_incarnation_id,position_id) DO UPDATE SET
  observed_flush_lsn=COALESCE(EXCLUDED.observed_flush_lsn,source_ack_receipts.observed_flush_lsn),
  acquisition_id=EXCLUDED.acquisition_id,
  lease_epoch=EXCLUDED.lease_epoch,
  generation=EXCLUDED.generation,
  recorded_at=clock_timestamp()
WHERE source_ack_receipts.checkpoint_lsn=EXCLUDED.checkpoint_lsn`, fence.FlowIncarnationID, grant.PositionID, grant.Checkpoint.LSN, observedFlushLSN, fence.AcquisitionID, fence.LeaseEpoch, fence.Generation); err != nil {
		return fmt.Errorf("record source ack receipt: %w", err)
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

func (c *Coordinator) reconcileUnfinished(ctx context.Context, fence authority.RunFence, intent connector.DeliveryIntent, state deliveryState, driver connector.ManagedDestination) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	if state.reconciliationAttempts >= maxReconciliationAttempts {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf(
			"%w: logical batch %s exhausted %d reconciliation attempts",
			connector.ErrDeliveryRetryExhausted,
			deliveryLogicalBatchID(intent),
			maxReconciliationAttempts,
		)
	}
	if err := waitForDeliveryRetry(ctx, state.nextAttemptAt); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	disposition, evidence, reconcileErr := driver.Reconcile(ctx, intent)
	if reconcileErr == nil && disposition != connector.DeliveryIndeterminate {
		return disposition, evidence, nil
	}
	detail := "target reconciliation remained indeterminate"
	if reconcileErr != nil {
		detail = reconcileErr.Error()
	}
	attempts, recordErr := c.recordReconciliationFailure(context.WithoutCancel(ctx), fence, state.attemptID, detail)
	if recordErr != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, recordErr
	}
	if attempts >= maxReconciliationAttempts {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf(
			"%w: logical batch %s exhausted %d reconciliation attempts: %s",
			connector.ErrDeliveryRetryExhausted,
			deliveryLogicalBatchID(intent),
			attempts,
			detail,
		)
	}
	return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf(
		"%w: reconcile logical batch %s attempt %d/%d: %s",
		connector.ErrDeliveryIndeterminate,
		deliveryLogicalBatchID(intent),
		attempts,
		maxReconciliationAttempts,
		detail,
	)
}

func (c *Coordinator) recordReconciliationFailure(ctx context.Context, fence authority.RunFence, attemptID uuid.UUID, detail string) (int, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin delivery reconciliation failure: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return 0, err
	}
	var attempts int
	if err := tx.QueryRow(ctx, `
UPDATE delivery_attempts
SET reconciliation_attempts=reconciliation_attempts+1,
    last_reconciled_at=clock_timestamp(),
    last_error=NULLIF($2,''),
    next_attempt_at=clock_timestamp() + LEAST(
      interval '1 minute',
      interval '100 milliseconds' * power(2,GREATEST(reconciliation_attempts,0))
    )
WHERE attempt_id=$1
  AND flow_incarnation_id=$3
  AND attempt_state IN ('pending','applied','failed','not_applied')
RETURNING reconciliation_attempts`, attemptID, detail, fence.FlowIncarnationID).Scan(&attempts); err != nil {
		return 0, fmt.Errorf("record delivery reconciliation failure: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit delivery reconciliation failure: %w", err)
	}
	return attempts, nil
}

func waitForDeliveryRetry(ctx context.Context, retryAt time.Time) error {
	delay := time.Until(retryAt)
	if retryAt.IsZero() || delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func validateTransactionDeliveryInput(fence authority.RunFence, intent connector.DeliveryIntent, transaction connector.SourceTransaction) error {
	if err := intent.Validate(); err != nil {
		return err
	}
	if err := transaction.Validate(); err != nil {
		return err
	}
	if intent.FlowID != fence.FlowID || intent.FlowIncarnationID != fence.FlowIncarnationID.String() || intent.Generation != fence.Generation || intent.AcquisitionID != fence.AcquisitionID.String() || intent.LeaseEpoch != fence.LeaseEpoch {
		return fmt.Errorf("%w: delivery intent does not match run fence", authority.ErrFenceRejected)
	}
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		return err
	}
	if contentHash != intent.ContentHash || logicalBatchID != intent.LogicalBatchID {
		return fmt.Errorf("%w: logical transaction identity mismatch", connector.ErrDeliveryConflict)
	}
	positionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		return err
	}
	if positionID != intent.PositionID {
		return fmt.Errorf("%w: intent position %s does not match transaction position %s", connector.ErrDeliveryConflict, intent.PositionID, positionID)
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

type manifestInspectionMode uint8

const (
	manifestCreateOrValidate manifestInspectionMode = iota
	manifestLookupOnly
)

func (c *Coordinator) inspect(ctx context.Context, fence authority.RunFence, intent connector.DeliveryIntent, checkpoint connector.Checkpoint, manifestMode manifestInspectionMode) (deliveryState, error) {
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
	switch manifestMode {
	case manifestCreateOrValidate:
		err = ensureManifest(ctx, tx, fence, intent, checkpoint)
	case manifestLookupOnly:
		_, err = loadManifestCheckpoint(ctx, tx, fence, intent, "")
	default:
		err = fmt.Errorf("unknown manifest inspection mode %d", manifestMode)
	}
	if err != nil {
		return deliveryState{}, err
	}
	state := deliveryState{}
	var receiptHash string
	var receiptLogicalBatchID pgtype.Text
	err = tx.QueryRow(ctx, `
SELECT attempt_id,external_id,content_hash,logical_batch_id
FROM delivery_receipts
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND source_lineage_id=$3 AND position_id=$4
FOR UPDATE`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, intent.PositionID).Scan(&state.attemptID, &state.externalID, &receiptHash, &receiptLogicalBatchID)
	switch {
	case err == nil:
		expectedLogicalBatchID := deliveryLogicalBatchID(intent)
		legacyLogicalBatch := !receiptLogicalBatchID.Valid || receiptLogicalBatchID.String == "legacy:"+intent.PositionID
		if receiptHash != intent.ContentHash || (!legacyLogicalBatch && receiptLogicalBatchID.String != expectedLogicalBatchID) {
			return deliveryState{}, fmt.Errorf("%w: immutable delivery receipt differs", connector.ErrDeliveryConflict)
		}
		if legacyLogicalBatch {
			if _, err := tx.Exec(ctx, `
WITH adopted AS (
  UPDATE delivery_receipts SET logical_batch_id=$2 WHERE attempt_id=$1 RETURNING attempt_id
)
UPDATE delivery_attempts SET logical_batch_id=$2
WHERE attempt_id IN (SELECT attempt_id FROM adopted)`, state.attemptID, expectedLogicalBatchID); err != nil {
				return deliveryState{}, fmt.Errorf("upgrade legacy delivery receipt identity: %w", err)
			}
		}
		state.authoritativeCheckpoint, state.authoritativeCheckpointID, err = loadAuthoritativeReceiptCheckpoint(ctx, tx, fence)
		if err != nil {
			return deliveryState{}, err
		}
		state.receipt = true
	case errors.Is(err, pgx.ErrNoRows):
		err = tx.QueryRow(ctx, `
SELECT attempt.attempt_id,attempt.attempt_state,attempt.attempt_number,
       attempt.reconciliation_attempts,attempt.next_attempt_at
FROM delivery_attempts AS attempt
LEFT JOIN delivery_receipts AS receipt ON receipt.attempt_id=attempt.attempt_id
WHERE attempt.flow_incarnation_id=$1
  AND attempt.destination_revision_id=$2
  AND attempt.source_lineage_id=$3
  AND attempt.position_id=$4
  AND receipt.attempt_id IS NULL
ORDER BY attempt.prepared_at DESC,attempt.attempt_id DESC
LIMIT 1`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, intent.PositionID).Scan(
			&state.attemptID,
			&state.attemptState,
			&state.attemptNumber,
			&state.reconciliationAttempts,
			&state.nextAttemptAt,
		)
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

// loadAuthoritativeReceiptCheckpoint implements monotonic receipt replay: once
// inspect has proved the requested historical receipt exists and matches its
// immutable identity, PostgreSQL's current checkpoint remains authoritative.
// A retry of receipt A after authority advanced to B therefore returns grant B,
// including B's actual position ID, stored metadata, and stored timestamp.
func loadAuthoritativeReceiptCheckpoint(ctx context.Context, tx pgx.Tx, fence authority.RunFence) (connector.Checkpoint, string, error) {
	var stored connector.Checkpoint
	var metadataJSON []byte
	var positionID string
	if err := tx.QueryRow(ctx, `
SELECT checkpoint.lsn,checkpoint.metadata,checkpoint.updated_at,intent.position_id
FROM authoritative_checkpoints AS checkpoint
JOIN source_ack_intents AS intent
  ON intent.flow_incarnation_id=checkpoint.flow_incarnation_id
 AND intent.checkpoint_lsn=checkpoint.lsn
WHERE checkpoint.flow_incarnation_id=$1
ORDER BY intent.authorized_at DESC,intent.position_id DESC
LIMIT 1
FOR UPDATE OF checkpoint,intent`, fence.FlowIncarnationID).Scan(&stored.LSN, &metadataJSON, &stored.Timestamp, &positionID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return connector.Checkpoint{}, "", fmt.Errorf("%w: current authoritative checkpoint has no matching source ACK intent", connector.ErrDeliveryConflict)
		}
		return connector.Checkpoint{}, "", fmt.Errorf("load authoritative checkpoint for delivery receipt: %w", err)
	}
	canonicalLSN, err := connector.CanonicalizeCheckpointPosition(stored.LSN)
	if err != nil {
		return connector.Checkpoint{}, "", fmt.Errorf("canonicalize authoritative receipt checkpoint: %w", err)
	}
	stored.LSN = canonicalLSN
	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &stored.Metadata); err != nil {
			return connector.Checkpoint{}, "", fmt.Errorf("decode authoritative receipt checkpoint metadata: %w", err)
		}
	}
	if stored.Metadata == nil {
		stored.Metadata = map[string]string{}
	}
	if _, err := tx.Exec(ctx, `
UPDATE authoritative_checkpoints
SET generation=$2,acquisition_id=$3,lease_epoch=$4
WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch); err != nil {
		return connector.Checkpoint{}, "", fmt.Errorf("rebind authoritative receipt checkpoint ownership: %w", err)
	}
	if _, err := tx.Exec(ctx, `
UPDATE source_ack_intents
SET generation=$3,acquisition_id=$4,lease_epoch=$5,authorized_at=clock_timestamp()
WHERE flow_incarnation_id=$1 AND position_id=$2 AND checkpoint_lsn=$6`, fence.FlowIncarnationID, positionID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, stored.LSN); err != nil {
		return connector.Checkpoint{}, "", fmt.Errorf("rebind authoritative receipt ACK ownership: %w", err)
	}
	return stored, positionID, nil
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
	checkpoint.LSN = position
	if checkpoint.Metadata == nil {
		checkpoint.Metadata = map[string]string{}
	}
	if checkpoint.Timestamp.IsZero() {
		checkpoint.Timestamp = time.Now().UTC()
	}
	metadataJSON, err := json.Marshal(checkpoint.Metadata)
	if err != nil {
		return fmt.Errorf("encode delivery manifest checkpoint metadata: %w", err)
	}
	sourceTransactionID := intent.SourceLineageID + ":" + position
	tag, err := tx.Exec(ctx, `
INSERT INTO delivery_manifests (
  flow_incarnation_id,destination_revision_id,source_lineage_id,logical_batch_id,position_id,
  source_transaction_id,content_hash,checkpoint_lsn,checkpoint_metadata,checkpoint_timestamp
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (flow_incarnation_id,destination_revision_id,position_id) DO NOTHING`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, deliveryLogicalBatchID(intent), intent.PositionID, sourceTransactionID, intent.ContentHash, position, metadataJSON, checkpoint.Timestamp)
	if err != nil {
		return fmt.Errorf("insert delivery manifest: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	_, err = loadManifestCheckpoint(ctx, tx, fence, intent, position)
	return err
}

// loadManifestCheckpoint is lookup-only: it validates and returns an existing
// immutable manifest, but never creates one from replay caller data. expectedLSN
// is populated by Deliver/DeliverTransaction and empty for Recover/finalization.
func loadManifestCheckpoint(ctx context.Context, tx pgx.Tx, fence authority.RunFence, intent connector.DeliveryIntent, expectedLSN string) (connector.Checkpoint, error) {
	var existingHash, existingLSN, existingLineage string
	var existingLogicalBatchID pgtype.Text
	var existingMetadata []byte
	var existingTimestamp pgtype.Timestamptz
	if err := tx.QueryRow(ctx, `
SELECT content_hash,checkpoint_lsn,source_lineage_id,logical_batch_id,
       checkpoint_metadata,checkpoint_timestamp
FROM delivery_manifests
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND position_id=$3
FOR UPDATE`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.PositionID).Scan(&existingHash, &existingLSN, &existingLineage, &existingLogicalBatchID, &existingMetadata, &existingTimestamp); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return connector.Checkpoint{}, fmt.Errorf("%w: immutable delivery manifest does not exist", connector.ErrDeliveryConflict)
		}
		return connector.Checkpoint{}, fmt.Errorf("load immutable delivery manifest: %w", err)
	}
	expectedLogicalBatchID := deliveryLogicalBatchID(intent)
	legacyLogicalBatch := !existingLogicalBatchID.Valid || existingLogicalBatchID.String == "legacy:"+intent.PositionID
	hashDiffers := existingHash != intent.ContentHash
	checkpointDiffers := expectedLSN != "" && existingLSN != expectedLSN
	lineageDiffers := existingLineage != intent.SourceLineageID
	logicalBatchDiffers := !legacyLogicalBatch && existingLogicalBatchID.String != expectedLogicalBatchID
	if hashDiffers || checkpointDiffers || lineageDiffers || logicalBatchDiffers {
		return connector.Checkpoint{}, fmt.Errorf("%w: immutable delivery manifest differs (content_hash=%t checkpoint=%t lineage=%t logical_batch=%t)", connector.ErrDeliveryConflict, hashDiffers, checkpointDiffers, lineageDiffers, logicalBatchDiffers)
	}
	if legacyLogicalBatch {
		if _, err := tx.Exec(ctx, `
UPDATE delivery_manifests SET logical_batch_id=$4
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND position_id=$3`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.PositionID, expectedLogicalBatchID); err != nil {
			return connector.Checkpoint{}, fmt.Errorf("upgrade legacy delivery manifest identity: %w", err)
		}
	}
	if len(existingMetadata) == 0 || !existingTimestamp.Valid {
		// Legacy manifests did not retain the immutable metadata/timestamp. Only
		// the exact current PostgreSQL authority row can safely prove that payload;
		// replay caller fields are never used for historical reconstruction.
		if err := tx.QueryRow(ctx, `
SELECT metadata,updated_at FROM authoritative_checkpoints
WHERE flow_incarnation_id=$1 AND lsn=$2
FOR UPDATE`, fence.FlowIncarnationID, existingLSN).Scan(&existingMetadata, &existingTimestamp); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return connector.Checkpoint{}, fmt.Errorf("%w: legacy delivery manifest lacks an authoritative historical checkpoint payload", connector.ErrDeliveryConflict)
			}
			return connector.Checkpoint{}, fmt.Errorf("prove legacy delivery manifest checkpoint payload: %w", err)
		}
		if _, err := tx.Exec(ctx, `
UPDATE delivery_manifests
SET checkpoint_metadata=$4,checkpoint_timestamp=$5
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND position_id=$3`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.PositionID, existingMetadata, existingTimestamp.Time); err != nil {
			return connector.Checkpoint{}, fmt.Errorf("backfill proven legacy delivery manifest checkpoint payload: %w", err)
		}
	}
	stored := connector.Checkpoint{LSN: existingLSN, Timestamp: existingTimestamp.Time}
	if len(existingMetadata) > 0 {
		if err := json.Unmarshal(existingMetadata, &stored.Metadata); err != nil {
			return connector.Checkpoint{}, fmt.Errorf("decode immutable delivery manifest checkpoint metadata: %w", err)
		}
	}
	if stored.Metadata == nil {
		stored.Metadata = map[string]string{}
	}
	return stored, nil
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
	if _, err := tx.Exec(ctx, `
UPDATE delivery_attempts
SET logical_batch_id=$3
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND position_id=$4
  AND source_lineage_id=$5 AND content_hash=$6
  AND (logical_batch_id IS NULL OR logical_batch_id='legacy:' || position_id)`, fence.FlowIncarnationID, intent.DestinationRevisionID, deliveryLogicalBatchID(intent), intent.PositionID, intent.SourceLineageID, intent.ContentHash); err != nil {
		return uuid.Nil, fmt.Errorf("upgrade legacy delivery attempt identity: %w", err)
	}
	var priorAttempts int
	if err := tx.QueryRow(ctx, `
SELECT COALESCE(max(attempt_number),0)
FROM delivery_attempts
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND logical_batch_id=$3`, fence.FlowIncarnationID, intent.DestinationRevisionID, deliveryLogicalBatchID(intent)).Scan(&priorAttempts); err != nil {
		return uuid.Nil, fmt.Errorf("count logical batch delivery attempts: %w", err)
	}
	if priorAttempts >= maxDeliveryAttempts {
		return uuid.Nil, fmt.Errorf("managed delivery exhausted %d attempts for logical batch %s", maxDeliveryAttempts, deliveryLogicalBatchID(intent))
	}
	attemptID := uuid.New()
	if _, err := tx.Exec(ctx, `
INSERT INTO delivery_attempts (
  attempt_id,flow_incarnation_id,flow_id,generation,acquisition_id,lease_epoch,
  destination_revision_id,source_lineage_id,logical_batch_id,position_id,content_hash,attempt_number
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, attemptID, fence.FlowIncarnationID, fence.FlowID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, intent.DestinationRevisionID, intent.SourceLineageID, deliveryLogicalBatchID(intent), intent.PositionID, intent.ContentHash, priorAttempts+1); err != nil {
		return uuid.Nil, fmt.Errorf("prepare delivery attempt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, fmt.Errorf("commit delivery attempt: %w", err)
	}
	return attemptID, nil
}

func (c *Coordinator) markAttemptTerminal(ctx context.Context, fence authority.RunFence, attemptID uuid.UUID, state, detail string) error {
	switch state {
	case "applied", "not_applied", "failed":
	default:
		return fmt.Errorf("invalid delivery attempt terminal state %q", state)
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin delivery attempt terminal transition: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
UPDATE delivery_attempts
SET attempt_state=$2,terminal_at=clock_timestamp(),last_error=NULLIF($3,''),
    next_attempt_at=CASE WHEN $2 IN ('failed','not_applied')
      THEN clock_timestamp() + LEAST(interval '1 minute',interval '100 milliseconds' * power(2,GREATEST(attempt_number-1,0)))
      ELSE clock_timestamp() END
WHERE attempt_id=$1
  AND flow_incarnation_id=$4
  AND (attempt_state IN ('pending','failed','not_applied') OR attempt_state=$2)`, attemptID, state, detail, fence.FlowIncarnationID)
	if err != nil {
		return fmt.Errorf("mark delivery attempt terminal: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("delivery attempt %s is not mutable under the current fence", attemptID)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit delivery attempt terminal transition: %w", err)
	}
	return nil
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

func (c *Coordinator) finalize(ctx context.Context, fence authority.RunFence, intent connector.DeliveryIntent, attemptID uuid.UUID) (AckGrant, error) {
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
  flow_incarnation_id,destination_revision_id,source_lineage_id,logical_batch_id,position_id,content_hash,attempt_id,external_id,
  adopted_by_acquisition_id,adopted_by_lease_epoch
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (flow_incarnation_id,destination_revision_id,position_id) DO UPDATE SET
  source_lineage_id=EXCLUDED.source_lineage_id
WHERE delivery_receipts.source_lineage_id=EXCLUDED.source_lineage_id
  AND delivery_receipts.logical_batch_id=EXCLUDED.logical_batch_id
  AND delivery_receipts.content_hash=EXCLUDED.content_hash
  AND delivery_receipts.external_id=EXCLUDED.external_id`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, deliveryLogicalBatchID(intent), intent.PositionID, contentHash, attemptID, externalID, fence.AcquisitionID, fence.LeaseEpoch)
	if err != nil {
		return AckGrant{}, fmt.Errorf("adopt delivery receipt: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return AckGrant{}, fmt.Errorf("%w: immutable delivery receipt differs", connector.ErrDeliveryConflict)
	}

	checkpoint, err := loadManifestCheckpoint(ctx, tx, fence, intent, "")
	if err != nil {
		return AckGrant{}, err
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

// PruneTerminalDeliveryState bounds retained attempts and receipts while
// preserving the current authoritative checkpoint as an explicit GC root.
// Only batches with observed source flush evidence are eligible.
func (c *Coordinator) PruneTerminalDeliveryState(ctx context.Context, fence authority.RunFence, retention time.Duration, limit int) (int64, error) {
	if retention <= 0 {
		return 0, errors.New("delivery retention must be positive")
	}
	if limit < 1 || limit > 10_000 {
		return 0, errors.New("delivery retention limit must be between 1 and 10000")
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin delivery retention: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return 0, err
	}
	var checkpointLSN string
	if err := tx.QueryRow(ctx, `SELECT lsn FROM authoritative_checkpoints WHERE flow_incarnation_id=$1 FOR UPDATE`, fence.FlowIncarnationID).Scan(&checkpointLSN); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("load delivery retention root: %w", err)
	}
	if c.hooks.AfterRetentionRootLock != nil {
		if err := c.hooks.AfterRetentionRootLock(ctx, fence, checkpointLSN); err != nil {
			return 0, err
		}
	}
	positionID, err := connector.CheckpointPositionID(connector.Checkpoint{LSN: checkpointLSN})
	if err != nil {
		return 0, err
	}
	cutoff := time.Now().UTC().Add(-retention)
	if _, err := tx.Exec(ctx, `
INSERT INTO delivery_retention_roots (flow_incarnation_id,minimum_position_id,retained_after)
VALUES ($1,$2,$3)
ON CONFLICT (flow_incarnation_id) DO UPDATE SET
  minimum_position_id=EXCLUDED.minimum_position_id,
  retained_after=EXCLUDED.retained_after,
  updated_at=clock_timestamp()`, fence.FlowIncarnationID, positionID, cutoff); err != nil {
		return 0, fmt.Errorf("record delivery retention root: %w", err)
	}
	var deleted int64
	if err := tx.QueryRow(ctx, `
WITH candidates AS MATERIALIZED (
  SELECT manifest.destination_revision_id,manifest.logical_batch_id,manifest.position_id
  FROM delivery_manifests AS manifest
  JOIN source_ack_receipts AS ack
    ON ack.flow_incarnation_id=manifest.flow_incarnation_id
   AND ack.position_id=manifest.position_id
  WHERE manifest.flow_incarnation_id=$1
    AND manifest.position_id<>$2
    AND manifest.created_at<$3
    AND ack.observed_flush_lsn IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM delivery_attempts AS pending
      WHERE pending.flow_incarnation_id=manifest.flow_incarnation_id
        AND pending.destination_revision_id=manifest.destination_revision_id
        AND pending.position_id=manifest.position_id
        AND pending.attempt_state='pending'
    )
  ORDER BY manifest.created_at,manifest.destination_revision_id,manifest.logical_batch_id
  LIMIT $4
  FOR UPDATE OF manifest
), deleted_evidence AS (
  DELETE FROM delivery_attempt_evidence AS evidence
  USING delivery_attempts AS attempt,candidates
  WHERE evidence.attempt_id=attempt.attempt_id
    AND attempt.flow_incarnation_id=$1
    AND attempt.destination_revision_id=candidates.destination_revision_id
    AND attempt.position_id=candidates.position_id
), deleted_attempts AS (
  DELETE FROM delivery_attempts AS attempt USING candidates
  WHERE attempt.flow_incarnation_id=$1
    AND attempt.destination_revision_id=candidates.destination_revision_id
    AND attempt.position_id=candidates.position_id
    AND attempt.attempt_state<>'pending'
), deleted_receipts AS (
  DELETE FROM delivery_receipts AS receipt USING candidates
  WHERE receipt.flow_incarnation_id=$1
    AND receipt.destination_revision_id=candidates.destination_revision_id
    AND receipt.position_id=candidates.position_id
), deleted_manifests AS (
  DELETE FROM delivery_manifests AS manifest USING candidates
  WHERE manifest.flow_incarnation_id=$1
    AND manifest.destination_revision_id=candidates.destination_revision_id
    AND manifest.position_id=candidates.position_id
  RETURNING 1
)
SELECT count(*) FROM deleted_manifests`, fence.FlowIncarnationID, positionID, cutoff, limit).Scan(&deleted); err != nil {
		return 0, fmt.Errorf("prune terminal delivery state: %w", err)
	}
	var feedbackDeleted int64
	if err := tx.QueryRow(ctx, `
WITH candidates AS MATERIALIZED (
  SELECT intent.position_id
  FROM source_ack_intents AS intent
  JOIN source_ack_receipts AS receipt
    ON receipt.flow_incarnation_id=intent.flow_incarnation_id
   AND receipt.position_id=intent.position_id
  WHERE intent.flow_incarnation_id=$1
    AND intent.position_id<>$2
    AND intent.authorized_at<$3
    AND receipt.recorded_at<$3
    AND receipt.observed_flush_lsn IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM delivery_manifests AS manifest
      WHERE manifest.flow_incarnation_id=intent.flow_incarnation_id
        AND manifest.position_id=intent.position_id
    )
    AND NOT EXISTS (
      SELECT 1 FROM source_ack_retention_roots AS retention_root
      WHERE retention_root.flow_incarnation_id=intent.flow_incarnation_id
        AND retention_root.position_id=intent.position_id
        AND retention_root.released_at IS NULL
    )
  ORDER BY intent.authorized_at,intent.position_id
  LIMIT $4
  FOR UPDATE OF intent,receipt
), deleted_receipts AS (
  DELETE FROM source_ack_receipts AS receipt USING candidates
  WHERE receipt.flow_incarnation_id=$1
    AND receipt.position_id=candidates.position_id
  RETURNING 1
), deleted_intents AS (
  DELETE FROM source_ack_intents AS intent USING candidates
  WHERE intent.flow_incarnation_id=$1
    AND intent.position_id=candidates.position_id
  RETURNING 1
)
SELECT
  (SELECT count(*) FROM deleted_receipts) +
  (SELECT count(*) FROM deleted_intents)`, fence.FlowIncarnationID, positionID, cutoff, limit).Scan(&feedbackDeleted); err != nil {
		return 0, fmt.Errorf("prune source feedback pairs: %w", err)
	}
	deleted += feedbackDeleted
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit delivery retention: %w", err)
	}
	return deleted, nil
}

func deliveryLogicalBatchID(intent connector.DeliveryIntent) string {
	if value := strings.TrimSpace(intent.LogicalBatchID); value != "" {
		return value
	}
	return "legacy:" + intent.PositionID
}

func finalizeCheckpointAndAck(ctx context.Context, tx pgx.Tx, fence authority.RunFence, positionID string, checkpoint connector.Checkpoint) (connector.Checkpoint, error) {
	canonicalLSN, err := connector.CanonicalizeCheckpointPosition(checkpoint.LSN)
	if err != nil {
		return connector.Checkpoint{}, err
	}
	if checkpoint.Metadata == nil {
		checkpoint.Metadata = map[string]string{}
	}
	checkpoint.LSN = canonicalLSN
	if checkpoint.Timestamp.IsZero() {
		checkpoint.Timestamp = time.Now().UTC()
	}

	var current connector.Checkpoint
	var currentMetadata []byte
	err = tx.QueryRow(ctx, `
SELECT lsn,metadata,updated_at
FROM authoritative_checkpoints
WHERE flow_incarnation_id=$1
FOR UPDATE`, fence.FlowIncarnationID).Scan(&current.LSN, &currentMetadata, &current.Timestamp)
	if err == nil {
		comparison, compareErr := connector.CompareCheckpointLSN(current.LSN, canonicalLSN)
		if compareErr != nil {
			return connector.Checkpoint{}, compareErr
		}
		if comparison > 0 {
			return connector.Checkpoint{}, fmt.Errorf("checkpoint regression from %s to %s", current.LSN, canonicalLSN)
		}
		if comparison == 0 {
			if len(currentMetadata) > 0 {
				if err := json.Unmarshal(currentMetadata, &current.Metadata); err != nil {
					return connector.Checkpoint{}, fmt.Errorf("decode authoritative checkpoint metadata: %w", err)
				}
			}
			if current.Metadata == nil {
				current.Metadata = map[string]string{}
			}
			current.LSN = canonicalLSN
			checkpoint = current
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return connector.Checkpoint{}, fmt.Errorf("load authoritative checkpoint: %w", err)
	}
	metadataJSON, err := json.Marshal(checkpoint.Metadata)
	if err != nil {
		return connector.Checkpoint{}, err
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
  updated_at=EXCLUDED.updated_at`, fence.FlowIncarnationID, fence.FlowID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, checkpoint.LSN, metadataJSON, checkpoint.Timestamp); err != nil {
		return connector.Checkpoint{}, fmt.Errorf("finalize authoritative checkpoint: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO source_ack_intents (
  flow_incarnation_id,position_id,checkpoint_lsn,generation,acquisition_id,lease_epoch
) VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (flow_incarnation_id,position_id) DO UPDATE SET
  checkpoint_lsn=EXCLUDED.checkpoint_lsn,
  generation=EXCLUDED.generation,
  acquisition_id=EXCLUDED.acquisition_id,
  lease_epoch=EXCLUDED.lease_epoch,
  authorized_at=clock_timestamp()
WHERE source_ack_intents.checkpoint_lsn=EXCLUDED.checkpoint_lsn`, fence.FlowIncarnationID, positionID, checkpoint.LSN, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch); err != nil {
		return connector.Checkpoint{}, fmt.Errorf("authorize source ack intent: %w", err)
	}
	return checkpoint, nil
}
