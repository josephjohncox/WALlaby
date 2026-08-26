package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/partauthority"
	"github.com/josephjohncox/wallaby/internal/schemabaseline"
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
	AfterTargetApply           func(context.Context, authority.RunFence, connector.DeliveryIntent) error
	AfterPartReservationCommit func(context.Context, authority.RunFence, connector.DeliveryIntent, string) error
	AfterPartReservationLock   func(context.Context, authority.RunFence, connector.ManagedPartReservationRequest) error
	BeforeFinalizeCommit       func(context.Context, authority.RunFence, connector.DeliveryIntent) error
	BeforeAuthorizeAckCommit   func(context.Context, authority.RunFence, connector.ManagedSchemaBaselinePayload) error
	AfterSourceFlush           func(context.Context, authority.RunFence, AckGrant, string) error
	AfterRetentionRootLock     func(context.Context, authority.RunFence, string) error
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
	if err := bootstrap.ApplyMigrations(ctx, pool); err != nil {
		return nil, fmt.Errorf("prepare delivery schema-baseline authority: %w", err)
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
func (c *Coordinator) AuthorizeAck(ctx context.Context, fence authority.RunFence, checkpoint connector.Checkpoint, baselines connector.ManagedSchemaBaselinePayload) (AckGrant, error) {
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
	if err := schemabaseline.UpsertExactTx(ctx, tx, fence, baselines); err != nil {
		return AckGrant{}, fmt.Errorf("advance schema baselines with source ack authorization: %w", err)
	}
	if c.hooks.BeforeAuthorizeAckCommit != nil {
		if err := c.hooks.BeforeAuthorizeAckCommit(ctx, fence, baselines); err != nil {
			return AckGrant{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return AckGrant{}, fmt.Errorf("commit checkpoint and source ack intent: %w", err)
	}
	return AckGrant{Checkpoint: checkpoint, PositionID: positionID}, nil
}

// Recover reconciles one unfinished delivery. Applied evidence is adopted by
// the current fence; indeterminate evidence is durably backed off and bounded.
func (c *Coordinator) Recover(ctx context.Context, fence authority.RunFence, intent connector.DeliveryIntent, checkpoint connector.Checkpoint, baselines connector.ManagedSchemaBaselinePayload, driver connector.ManagedDestination) (AckGrant, error) {
	if driver == nil {
		return AckGrant{}, errors.New("managed delivery driver is required")
	}
	state, err := c.inspect(ctx, fence, intent, checkpoint, baselines, false)
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

func recoverablePostCommitError(stage string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %s: %w", connector.ErrDeliveryIndeterminate, stage, err)
}

// DeliverTransaction applies one complete committed source transaction. The
// immutable logical batch and attempt are durable before target I/O, while the
// target receipt, checkpoint, and ACK intent are finalized under one fence.
func (c *Coordinator) DeliverTransaction(ctx context.Context, fence authority.RunFence, intent connector.DeliveryIntent, transaction connector.SourceTransaction, baselines connector.ManagedSchemaBaselinePayload, driver connector.ManagedTransactionDestination) (AckGrant, error) {
	if driver == nil {
		return AckGrant{}, errors.New("managed transaction delivery driver is required")
	}
	if err := validateTransactionDeliveryInput(fence, intent, transaction); err != nil {
		return AckGrant{}, err
	}
	state, err := c.inspect(ctx, fence, intent, transaction.Checkpoint, baselines, true)
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
			retryState, err := c.inspect(ctx, fence, intent, transaction.Checkpoint, baselines, true)
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
	var reservationRequest *connector.ManagedPartReservationRequest
	var reservationPrepared partauthority.Prepared
	if candidate, ok := prepared.(partauthority.Prepared); ok {
		request, requestErr := candidate.PartReservationRequest()
		if requestErr != nil {
			return AckGrant{}, fmt.Errorf("validate managed part reservation: %w", requestErr)
		}
		if request.DestinationRevisionID != intent.DestinationRevisionID || request.SourceLineageID != intent.SourceLineageID || request.LogicalBatchID != intent.LogicalBatchID || request.PositionID != intent.PositionID || request.ContentHash != intent.ContentHash {
			return AckGrant{}, fmt.Errorf("%w: managed part reservation differs from delivery intent", connector.ErrDeliveryConflict)
		}
		reservationRequest = &request
		reservationPrepared = candidate
	}

	attemptID, reservation, err := c.prepareAttempt(ctx, fence, intent, transaction.Checkpoint, baselines, reservationRequest, reservationPrepared)
	if err != nil {
		return AckGrant{}, err
	}
	if reservationPrepared != nil {
		if reservation == nil {
			return AckGrant{}, errors.New("managed part reservation was not persisted")
		}
		grant, grantErr := partauthority.NewGrant(reservation.ReservationID(), reservation.GuardPartWrite)
		if grantErr != nil {
			return AckGrant{}, fmt.Errorf("issue managed part authority: %w", grantErr)
		}
		if err := reservationPrepared.BindPartReservation(grant); err != nil {
			return AckGrant{}, fmt.Errorf("bind managed part reservation: %w", err)
		}
		if c.hooks.AfterPartReservationCommit != nil {
			if err := c.hooks.AfterPartReservationCommit(ctx, fence, intent, reservation.ReservationID()); err != nil {
				return AckGrant{}, err
			}
		}
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
// grant. The externally observed flush position must canonicalize to the exact
// authorized checkpoint; scheduled feedback without evidence is not a receipt.
// It never authorizes a position that lacks the corresponding ACK intent.
func (c *Coordinator) RecordAckReceipt(ctx context.Context, fence authority.RunFence, grant AckGrant, observedFlushLSN string) error {
	observed, err := canonicalObservedFlushLSN(grant, observedFlushLSN)
	if err != nil {
		return err
	}
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
	if err := recordAckReceipt(ctx, tx, fence, grant, observed); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit source ack receipt: %w", err)
	}
	return nil
}

func canonicalObservedFlushLSN(grant AckGrant, observedFlushLSN string) (string, error) {
	observed, err := connector.CanonicalizeCheckpointPosition(observedFlushLSN)
	if err != nil {
		return "", fmt.Errorf("canonicalize observed source flush: %w", err)
	}
	authorized, err := connector.CanonicalizeCheckpointPosition(grant.Checkpoint.LSN)
	if err != nil {
		return "", fmt.Errorf("canonicalize authorized source flush: %w", err)
	}
	if observed != authorized {
		return "", fmt.Errorf("observed source flush %s differs from authorized checkpoint %s", observed, authorized)
	}
	return observed, nil
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
	observed, err := canonicalObservedFlushLSN(grant, evidence.ObservedFlushLSN)
	if err != nil {
		return err
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
	result, err := tx.Exec(ctx, `
INSERT INTO source_ack_receipts (
  flow_incarnation_id,position_id,checkpoint_lsn,observed_flush_lsn,acquisition_id,lease_epoch,generation
) VALUES ($1,$2,$3,NULLIF($4,''),$5,$6,$7)
ON CONFLICT (flow_incarnation_id,position_id) DO UPDATE SET
  observed_flush_lsn=COALESCE(EXCLUDED.observed_flush_lsn,source_ack_receipts.observed_flush_lsn),
  acquisition_id=EXCLUDED.acquisition_id,
  lease_epoch=EXCLUDED.lease_epoch,
  generation=EXCLUDED.generation,
  recorded_at=clock_timestamp()
WHERE source_ack_receipts.checkpoint_lsn=EXCLUDED.checkpoint_lsn`, fence.FlowIncarnationID, grant.PositionID, observedFlushLSN, observedFlushLSN, fence.AcquisitionID, fence.LeaseEpoch, fence.Generation)
	if err != nil {
		return fmt.Errorf("record source ack receipt: %w", err)
	}
	if result.RowsAffected() != 1 {
		return errors.New("source ack receipt conflicts with the canonical authorized checkpoint")
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
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf("%w: logical batch %s exhausted %d reconciliation attempts", connector.ErrDeliveryRetryExhausted, intent.LogicalBatchID, maxReconciliationAttempts)
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
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf("%w: logical batch %s exhausted %d reconciliation attempts: %s", connector.ErrDeliveryRetryExhausted, intent.LogicalBatchID, attempts, detail)
	}
	return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf("%w: reconcile logical batch %s attempt %d/%d: %s", connector.ErrDeliveryIndeterminate, intent.LogicalBatchID, attempts, maxReconciliationAttempts, detail)
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

func (c *Coordinator) inspect(ctx context.Context, fence authority.RunFence, intent connector.DeliveryIntent, checkpoint connector.Checkpoint, baselines connector.ManagedSchemaBaselinePayload, createManifest bool) (deliveryState, error) {
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
	state := deliveryState{}
	var receiptHash, receiptLineage, receiptPosition string
	err = tx.QueryRow(ctx, `
SELECT attempt_id,external_id,content_hash,source_lineage_id,position_id
FROM delivery_receipts
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND logical_batch_id=$3
FOR UPDATE`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.LogicalBatchID).Scan(&state.attemptID, &state.externalID, &receiptHash, &receiptLineage, &receiptPosition)
	switch {
	case err == nil:
		if receiptHash != intent.ContentHash || receiptLineage != intent.SourceLineageID || receiptPosition != intent.PositionID {
			return deliveryState{}, fmt.Errorf("%w: immutable delivery receipt differs", connector.ErrDeliveryConflict)
		}
		state.authoritativeCheckpoint, state.authoritativeCheckpointID, err = loadAuthoritativeReceiptCheckpoint(ctx, tx, fence)
		if err != nil {
			return deliveryState{}, err
		}
		state.receipt = true
	case errors.Is(err, pgx.ErrNoRows):
		if createManifest {
			if err := ensureManifest(ctx, tx, fence, intent, checkpoint, baselines); err != nil {
				return deliveryState{}, err
			}
		} else {
			if _, _, err := loadManifestAuthority(ctx, tx, fence, intent); err != nil {
				return deliveryState{}, err
			}
		}
		var attemptHash, attemptLineage, attemptPosition string
		err = tx.QueryRow(ctx, `
SELECT attempt.attempt_id,attempt.attempt_state,attempt.attempt_number,
       attempt.reconciliation_attempts,attempt.next_attempt_at,
       attempt.content_hash,attempt.source_lineage_id,attempt.position_id
FROM delivery_attempts AS attempt
LEFT JOIN delivery_receipts AS receipt ON receipt.attempt_id=attempt.attempt_id
WHERE attempt.flow_incarnation_id=$1
  AND attempt.destination_revision_id=$2
  AND attempt.logical_batch_id=$3
  AND receipt.attempt_id IS NULL
ORDER BY attempt.attempt_number DESC,attempt.attempt_id DESC
LIMIT 1`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.LogicalBatchID).Scan(&state.attemptID, &state.attemptState, &state.attemptNumber, &state.reconciliationAttempts, &state.nextAttemptAt, &attemptHash, &attemptLineage, &attemptPosition)
		if err == nil {
			if attemptHash != intent.ContentHash || attemptLineage != intent.SourceLineageID || attemptPosition != intent.PositionID {
				return deliveryState{}, fmt.Errorf("%w: immutable delivery attempt differs", connector.ErrDeliveryConflict)
			}
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

// loadAuthoritativeReceiptCheckpoint returns PostgreSQL's current checkpoint
// and matching ACK intent after the requested immutable receipt has been proven.
// Historical receipt replay therefore remains monotonic and rebinds authority
// ownership without trusting caller-supplied checkpoint payload.
func loadAuthoritativeReceiptCheckpoint(ctx context.Context, tx pgx.Tx, fence authority.RunFence) (connector.Checkpoint, string, error) {
	var checkpoint connector.Checkpoint
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
FOR UPDATE OF checkpoint,intent`, fence.FlowIncarnationID).Scan(&checkpoint.LSN, &metadataJSON, &checkpoint.Timestamp, &positionID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return connector.Checkpoint{}, "", fmt.Errorf("%w: current authoritative checkpoint has no matching source ACK intent", connector.ErrDeliveryConflict)
		}
		return connector.Checkpoint{}, "", fmt.Errorf("load authoritative checkpoint for delivery receipt: %w", err)
	}
	canonicalLSN, err := connector.CanonicalizeCheckpointPosition(checkpoint.LSN)
	if err != nil {
		return connector.Checkpoint{}, "", fmt.Errorf("canonicalize authoritative receipt checkpoint: %w", err)
	}
	checkpoint.LSN = canonicalLSN
	if err := json.Unmarshal(metadataJSON, &checkpoint.Metadata); err != nil {
		return connector.Checkpoint{}, "", fmt.Errorf("decode authoritative receipt checkpoint metadata: %w", err)
	}
	if checkpoint.Metadata == nil {
		checkpoint.Metadata = map[string]string{}
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
WHERE flow_incarnation_id=$1 AND position_id=$2 AND checkpoint_lsn=$6`, fence.FlowIncarnationID, positionID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, checkpoint.LSN); err != nil {
		return connector.Checkpoint{}, "", fmt.Errorf("rebind authoritative receipt ACK ownership: %w", err)
	}
	return checkpoint, positionID, nil
}

func ensureManifest(ctx context.Context, tx pgx.Tx, fence authority.RunFence, intent connector.DeliveryIntent, checkpoint connector.Checkpoint, baselines connector.ManagedSchemaBaselinePayload) error {
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
	checkpointMetadataJSON, err := json.Marshal(checkpoint.Metadata)
	if err != nil {
		return fmt.Errorf("encode delivery manifest checkpoint metadata: %w", err)
	}
	checkpointTimestamp := checkpoint.Timestamp
	if checkpointTimestamp.IsZero() {
		checkpointTimestamp = time.Now().UTC()
	}
	baselineJSON, baselineFingerprint, err := baselines.Canonical()
	if err != nil {
		return fmt.Errorf("canonicalize delivery schema-baseline manifest: %w", err)
	}
	if baselines.SourceLineageID != intent.SourceLineageID {
		return fmt.Errorf("%w: delivery baseline lineage differs from intent", connector.ErrDeliveryConflict)
	}
	sourceTransactionID := intent.SourceLineageID + ":" + position
	tag, err := tx.Exec(ctx, `
INSERT INTO delivery_manifests (
  flow_incarnation_id,destination_revision_id,source_lineage_id,logical_batch_id,position_id,source_transaction_id,content_hash,checkpoint_lsn,
  checkpoint_metadata,checkpoint_timestamp,schema_baseline_payload,schema_baseline_fingerprint
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11::jsonb,$12)
ON CONFLICT DO NOTHING`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, intent.LogicalBatchID, intent.PositionID, sourceTransactionID, intent.ContentHash, position, checkpointMetadataJSON, checkpointTimestamp, baselineJSON, baselineFingerprint)
	if err != nil {
		return fmt.Errorf("insert delivery manifest: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	existingCheckpoint, existingBaselines, err := loadManifestAuthority(ctx, tx, fence, intent)
	if err != nil {
		return err
	}
	_, existingBaselineFingerprint, err := existingBaselines.Canonical()
	if err != nil {
		return fmt.Errorf("canonicalize existing delivery schema-baseline manifest: %w", err)
	}
	metadataDiffers := !stringMapEqual(existingCheckpoint.Metadata, checkpoint.Metadata)
	// Checkpoint timestamps are observation metadata and can change when the
	// same WAL transaction is decoded again. The first prepared manifest keeps
	// its timestamp for checkpoint reconstruction, but replay identity is bound
	// only to the canonical LSN, metadata, content, and schema baselines.
	checkpointDiffers := existingCheckpoint.LSN != position || metadataDiffers
	baselineDiffers := existingBaselineFingerprint != baselineFingerprint
	if checkpointDiffers || baselineDiffers {
		return fmt.Errorf("%w: immutable delivery manifest differs (checkpoint=%t metadata=%t baselines=%t)", connector.ErrDeliveryConflict, checkpointDiffers, metadataDiffers, baselineDiffers)
	}
	return nil
}

func loadManifestAuthority(ctx context.Context, tx pgx.Tx, fence authority.RunFence, intent connector.DeliveryIntent) (connector.Checkpoint, connector.ManagedSchemaBaselinePayload, error) {
	var existingHash, existingLSN, existingLineage, existingPosition, existingLogicalBatchID, baselineFingerprint string
	var checkpointMetadataJSON, baselineJSON []byte
	var checkpointTimestamp time.Time
	if err := tx.QueryRow(ctx, `
SELECT content_hash,checkpoint_lsn,source_lineage_id,position_id,logical_batch_id,
       checkpoint_metadata,checkpoint_timestamp,schema_baseline_payload,schema_baseline_fingerprint
FROM delivery_manifests
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND logical_batch_id=$3
FOR UPDATE`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.LogicalBatchID).Scan(
		&existingHash, &existingLSN, &existingLineage, &existingPosition, &existingLogicalBatchID,
		&checkpointMetadataJSON, &checkpointTimestamp, &baselineJSON, &baselineFingerprint,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return connector.Checkpoint{}, connector.ManagedSchemaBaselinePayload{}, fmt.Errorf("%w: immutable delivery manifest does not exist", connector.ErrDeliveryConflict)
		}
		return connector.Checkpoint{}, connector.ManagedSchemaBaselinePayload{}, fmt.Errorf("load immutable delivery manifest authority: %w", err)
	}
	if existingHash != intent.ContentHash || existingLineage != intent.SourceLineageID || existingPosition != intent.PositionID || existingLogicalBatchID != intent.LogicalBatchID {
		return connector.Checkpoint{}, connector.ManagedSchemaBaselinePayload{}, fmt.Errorf("%w: immutable delivery manifest identity differs", connector.ErrDeliveryConflict)
	}
	checkpoint := connector.Checkpoint{LSN: existingLSN, Timestamp: checkpointTimestamp}
	if err := json.Unmarshal(checkpointMetadataJSON, &checkpoint.Metadata); err != nil {
		return connector.Checkpoint{}, connector.ManagedSchemaBaselinePayload{}, fmt.Errorf("decode immutable delivery checkpoint metadata: %w", err)
	}
	if checkpoint.Metadata == nil {
		checkpoint.Metadata = map[string]string{}
	}
	var baselines connector.ManagedSchemaBaselinePayload
	if err := json.Unmarshal(baselineJSON, &baselines); err != nil {
		return connector.Checkpoint{}, connector.ManagedSchemaBaselinePayload{}, fmt.Errorf("decode immutable delivery schema baselines: %w", err)
	}
	_, actualBaselineFingerprint, err := baselines.Canonical()
	if err != nil || actualBaselineFingerprint != baselineFingerprint {
		return connector.Checkpoint{}, connector.ManagedSchemaBaselinePayload{}, fmt.Errorf("%w: immutable delivery schema-baseline fingerprint differs", connector.ErrDeliveryConflict)
	}
	return checkpoint, baselines, nil
}

func stringMapEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		other, exists := right[key]
		if !exists || other != value {
			return false
		}
	}
	return true
}

type postgresPartReservation struct {
	pool              *pgxpool.Pool
	fence             authority.RunFence
	reservationID     uuid.UUID
	destinationID     string
	logicalBatchID    string
	contentHash       string
	serverActiveParts int64
	reservedParts     int64
	capacity          int64
	reservationEpoch  int64
}

func (r *postgresPartReservation) ReservationID() string {
	if r == nil {
		return ""
	}
	return r.reservationID.String()
}

func (r *postgresPartReservation) GuardPartWrite(ctx context.Context, part connector.ManagedPartIdentity, write func(context.Context) error) error {
	if r == nil || r.pool == nil || r.reservationID == uuid.Nil || write == nil {
		return errors.New("managed part reservation guard is not initialized")
	}
	if part.Kind != "changelog" && part.Kind != "receipt" || strings.TrimSpace(part.QueryID) == "" || part.Ordinal > math.MaxInt64 {
		return errors.New("managed part identity is invalid")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin managed part write guard: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, r.fence); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(pg_catalog.hashtextextended($1,0))`, "managed-part-budget\x1f"+r.destinationID); err != nil {
		return fmt.Errorf("lock managed part write budget: %w", err)
	}
	var state string
	if err := tx.QueryRow(ctx, `
SELECT part.part_state
FROM managed_part_reservation_parts AS part
JOIN managed_part_reservations AS reservation ON reservation.reservation_id=part.reservation_id
WHERE part.reservation_id=$1 AND part.part_kind=$2 AND part.part_ordinal=$3 AND part.query_id=$4
  AND reservation.flow_incarnation_id=$5 AND reservation.destination_revision_id=$6
  AND reservation.logical_batch_id=$7 AND reservation.content_hash=$8
  AND reservation.reservation_epoch=$9 AND reservation.reservation_state='reserved'
  AND reservation.generation=$10 AND reservation.acquisition_id=$11 AND reservation.lease_epoch=$12
FOR UPDATE OF part,reservation`, r.reservationID, part.Kind, int64(part.Ordinal), part.QueryID, r.fence.FlowIncarnationID, r.destinationID, r.logicalBatchID, r.contentHash, r.reservationEpoch, r.fence.Generation, r.fence.AcquisitionID, r.fence.LeaseEpoch).Scan(&state); err != nil {
		return fmt.Errorf("authorize managed part write: %w", err)
	}
	if state == "durable" {
		return tx.Commit(ctx)
	}
	if state != "reserved" {
		return fmt.Errorf("managed part is not writable in state %q", state)
	}
	if err := write(ctx); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
UPDATE managed_part_reservation_parts
SET part_state='durable',durable_at=clock_timestamp(),updated_at=clock_timestamp()
WHERE reservation_id=$1 AND part_kind=$2 AND part_ordinal=$3 AND query_id=$4 AND part_state='reserved'`, r.reservationID, part.Kind, int64(part.Ordinal), part.QueryID)
	if err != nil {
		return fmt.Errorf("record guarded managed part progress: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("guarded managed part progress was not recorded")
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit managed part write guard: %w", err)
	}
	return nil
}

func (c *Coordinator) reserveManagedPartsTx(ctx context.Context, tx pgx.Tx, fence authority.RunFence, request connector.ManagedPartReservationRequest, observer partauthority.Prepared) (*postgresPartReservation, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if observer == nil || request.Capacity > math.MaxInt64 || len(request.Parts) > math.MaxInt32 {
		return nil, errors.New("managed part reservation observer and bounded capacity are required")
	}
	for _, part := range request.Parts {
		if part.Ordinal > math.MaxInt64 {
			return nil, errors.New("managed part ordinal exceeds PostgreSQL integer bounds")
		}
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(pg_catalog.hashtextextended($1,0))`, "managed-part-budget\x1f"+request.DestinationRevisionID); err != nil {
		return nil, fmt.Errorf("lock managed part budget: %w", err)
	}
	if c.hooks.AfterPartReservationLock != nil {
		if err := c.hooks.AfterPartReservationLock(ctx, fence, request); err != nil {
			return nil, err
		}
	}

	var reservationID uuid.UUID
	var storedFlowIncarnation uuid.UUID
	var sourceLineage, positionID, contentHash, planHash, resource, state string
	var plannedParts int
	var capacity, reservationEpoch int64
	err := tx.QueryRow(ctx, `
SELECT reservation_id,flow_incarnation_id,source_lineage_id,position_id,content_hash,plan_hash,resource,
       planned_parts,capacity,reservation_state,reservation_epoch
FROM managed_part_reservations
WHERE destination_revision_id=$1 AND logical_batch_id=$2
FOR UPDATE`, request.DestinationRevisionID, request.LogicalBatchID).Scan(&reservationID, &storedFlowIncarnation, &sourceLineage, &positionID, &contentHash, &planHash, &resource, &plannedParts, &capacity, &state, &reservationEpoch)
	exists := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("load managed part reservation: %w", err)
	}
	if exists {
		if storedFlowIncarnation != fence.FlowIncarnationID || sourceLineage != request.SourceLineageID || positionID != request.PositionID || contentHash != request.ContentHash || planHash != request.PlanHash || resource != request.Resource || plannedParts != len(request.Parts) || capacity != int64(request.Capacity) {
			return nil, fmt.Errorf("%w: existing managed part reservation immutable identity differs", connector.ErrDeliveryConflict)
		}
		if err := validateReservedPartIdentities(ctx, tx, reservationID, request.Parts); err != nil {
			return nil, err
		}
	}
	requireAbsent := !exists || state == "released"
	observation, err := observer.ObservePartReservation(ctx, requireAbsent)
	if err != nil {
		telemetry.RecordClickHousePartRejection(ctx, "observation")
		return nil, fmt.Errorf("observe managed ClickHouse part budget while locked: %w", err)
	}
	if observation.EndpointCount != 2 || !observation.Quiescent || requireAbsent && !observation.BatchAbsent || observation.ServerActiveParts > math.MaxInt64 {
		telemetry.RecordClickHousePartRejection(ctx, "quiescence")
		return nil, fmt.Errorf("%w: managed ClickHouse part observation is incomplete", connector.ErrDeliveryIndeterminate)
	}
	active := int64(observation.ServerActiveParts)
	limit := int64(request.Capacity)
	if err := observeManagedPartChargesTx(ctx, tx, fence, request.DestinationRevisionID, active); err != nil {
		return nil, err
	}
	if exists {
		if err := tx.QueryRow(ctx, `SELECT reservation_state,reservation_epoch FROM managed_part_reservations WHERE reservation_id=$1 FOR UPDATE`, reservationID).Scan(&state, &reservationEpoch); err != nil {
			return nil, err
		}
	}
	var charged int64
	if err := tx.QueryRow(ctx, `
SELECT count(*) FROM managed_part_reservation_parts AS part
JOIN managed_part_reservations AS reservation ON reservation.reservation_id=part.reservation_id
WHERE reservation.destination_revision_id=$1 AND reservation.resource=$2
  AND reservation.reservation_state IN ('reserved','completed_pending_observation','reclaim_pending')
  AND part.charge_state='charged'`, request.DestinationRevisionID, request.Resource).Scan(&charged); err != nil {
		return nil, fmt.Errorf("sum managed part charges: %w", err)
	}

	if exists && state == "reserved" {
		if active > limit || charged > limit-active {
			telemetry.RecordClickHousePartAdmission(ctx, active, charged, limit, "capacity")
			return nil, fmt.Errorf("managed ClickHouse backpressure: server active parts=%d charged parts=%d capacity=%d", active, charged, limit)
		}
		if _, err := tx.Exec(ctx, `
UPDATE managed_part_reservations
SET flow_id=$2,generation=$3,acquisition_id=$4,lease_epoch=$5,server_active_parts=$6,updated_at=clock_timestamp()
WHERE reservation_id=$1 AND reservation_state='reserved'`, reservationID, fence.FlowID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, active); err != nil {
			return nil, fmt.Errorf("adopt managed part reservation: %w", err)
		}
		if err := recordManagedPartEvent(ctx, tx, reservationID, reservationEpoch, "adopted", fence, active, charged); err != nil {
			return nil, err
		}
		return &postgresPartReservation{fence: fence, reservationID: reservationID, destinationID: request.DestinationRevisionID, logicalBatchID: request.LogicalBatchID, contentHash: request.ContentHash, serverActiveParts: active, reservedParts: charged, capacity: limit, reservationEpoch: reservationEpoch}, nil
	}
	if exists && state == "completed_pending_observation" {
		return nil, fmt.Errorf("%w: completed managed part reservation requires receipt reconciliation", connector.ErrDeliveryIndeterminate)
	}
	if exists && state == "reclaim_pending" {
		return nil, fmt.Errorf("%w: managed part reservation reclaim is pending", connector.ErrDeliveryIndeterminate)
	}

	planned := int64(len(request.Parts))
	if active > limit || charged > limit-active || planned > limit-active-charged {
		telemetry.RecordClickHousePartAdmission(ctx, active, charged, limit, "capacity")
		return nil, fmt.Errorf("managed ClickHouse backpressure: server active parts=%d charged parts=%d planned parts=%d capacity=%d", active, charged, planned, limit)
	}
	if exists {
		reservationEpoch++
		if _, err := tx.Exec(ctx, `
UPDATE managed_part_reservation_parts SET part_state='reserved',charge_state='charged',durable_at=NULL,observed_at=NULL,released_at=NULL,updated_at=clock_timestamp()
WHERE reservation_id=$1`, reservationID); err != nil {
			return nil, fmt.Errorf("reset managed part identities for re-reservation: %w", err)
		}
		if _, err := tx.Exec(ctx, `
UPDATE managed_part_reservations
SET flow_id=$2,generation=$3,acquisition_id=$4,lease_epoch=$5,server_active_parts=$6,
    reservation_epoch=$7,observation_epoch=observation_epoch+1,reservation_state='reserved',
    completed_at=NULL,reclaim_started_at=NULL,released_at=NULL,observed_at=clock_timestamp(),updated_at=clock_timestamp()
WHERE reservation_id=$1 AND reservation_state='released'`, reservationID, fence.FlowID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, active, reservationEpoch); err != nil {
			return nil, fmt.Errorf("re-reserve managed part identity: %w", err)
		}
		if err := recordManagedPartEvent(ctx, tx, reservationID, reservationEpoch, "rereserved", fence, active, charged+planned); err != nil {
			return nil, err
		}
	} else {
		reservationID = uuid.NewSHA1(uuid.NameSpaceOID, []byte(request.DestinationRevisionID+"\x1f"+request.LogicalBatchID))
		reservationEpoch = 1
		if _, err := tx.Exec(ctx, `
INSERT INTO managed_part_reservations (
  reservation_id,flow_incarnation_id,flow_id,generation,acquisition_id,lease_epoch,
  destination_revision_id,source_lineage_id,logical_batch_id,position_id,content_hash,plan_hash,
  resource,server_active_parts,planned_parts,capacity
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`, reservationID, fence.FlowIncarnationID, fence.FlowID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, request.DestinationRevisionID, request.SourceLineageID, request.LogicalBatchID, request.PositionID, request.ContentHash, request.PlanHash, request.Resource, active, planned, limit); err != nil {
			return nil, fmt.Errorf("insert managed part reservation: %w", err)
		}
		for _, part := range request.Parts {
			// #nosec G115 -- every ordinal is bounded to MaxInt64 before the budget lock.
			if _, err := tx.Exec(ctx, `INSERT INTO managed_part_reservation_parts (reservation_id,part_kind,part_ordinal,query_id) VALUES ($1,$2,$3,$4)`, reservationID, part.Kind, int64(part.Ordinal), part.QueryID); err != nil {
				return nil, fmt.Errorf("insert managed part identity: %w", err)
			}
		}
		if err := recordManagedPartEvent(ctx, tx, reservationID, reservationEpoch, "reserved", fence, active, charged+planned); err != nil {
			return nil, err
		}
	}
	return &postgresPartReservation{fence: fence, reservationID: reservationID, destinationID: request.DestinationRevisionID, logicalBatchID: request.LogicalBatchID, contentHash: request.ContentHash, serverActiveParts: active, reservedParts: charged + planned, capacity: limit, reservationEpoch: reservationEpoch}, nil
}

func observeManagedPartChargesTx(ctx context.Context, tx pgx.Tx, fence authority.RunFence, destinationRevisionID string, active int64) error {
	if _, err := tx.Exec(ctx, `
UPDATE managed_part_reservation_parts AS part
SET charge_state='observed',observed_at=clock_timestamp(),updated_at=clock_timestamp()
FROM managed_part_reservations AS reservation
WHERE reservation.reservation_id=part.reservation_id AND reservation.destination_revision_id=$1
  AND reservation.reservation_state IN ('reserved','completed_pending_observation','reclaim_pending')
  AND part.part_state='durable' AND part.charge_state='charged'`, destinationRevisionID); err != nil {
		return fmt.Errorf("observe durable managed part charges: %w", err)
	}
	rows, err := tx.Query(ctx, `
UPDATE managed_part_reservations AS reservation
SET reservation_state='released',released_at=clock_timestamp(),server_active_parts=$2,
    observation_epoch=observation_epoch+1,observed_at=clock_timestamp(),updated_at=clock_timestamp(),
    generation=$3,acquisition_id=$4,lease_epoch=$5
WHERE reservation.destination_revision_id=$1 AND reservation.reservation_state='completed_pending_observation'
  AND NOT EXISTS (SELECT 1 FROM managed_part_reservation_parts AS part WHERE part.reservation_id=reservation.reservation_id AND part.charge_state='charged')
RETURNING reservation_id,reservation_epoch,observation_epoch,reclaim_epoch`, destinationRevisionID, active, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch)
	if err != nil {
		return fmt.Errorf("release observed completed reservations: %w", err)
	}
	defer rows.Close()
	type released struct {
		id                          uuid.UUID
		epoch, observation, reclaim int64
	}
	var releasedRows []released
	for rows.Next() {
		var item released
		if err := rows.Scan(&item.id, &item.epoch, &item.observation, &item.reclaim); err != nil {
			return err
		}
		releasedRows = append(releasedRows, item)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, item := range releasedRows {
		if _, err := tx.Exec(ctx, `UPDATE managed_part_reservation_parts SET part_state='released',charge_state='released',released_at=clock_timestamp(),updated_at=clock_timestamp() WHERE reservation_id=$1`, item.id); err != nil {
			return err
		}
		if err := recordManagedPartEvent(ctx, tx, item.id, item.epoch, "released", fence, active, 0); err != nil {
			return err
		}
	}
	return nil
}

func recordManagedPartEvent(ctx context.Context, tx pgx.Tx, reservationID uuid.UUID, reservationEpoch int64, event string, fence authority.RunFence, active, charged int64) error {
	_, err := tx.Exec(ctx, `
INSERT INTO managed_part_reservation_events (reservation_id,reservation_epoch,observation_epoch,reclaim_epoch,event_kind,generation,acquisition_id,lease_epoch,server_active_parts,charged_parts)
SELECT reservation_id,$2,observation_epoch,reclaim_epoch,$3,$4,$5,$6,$7,$8 FROM managed_part_reservations WHERE reservation_id=$1`, reservationID, reservationEpoch, event, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, active, charged)
	if err != nil {
		return fmt.Errorf("record managed part reservation event: %w", err)
	}
	return nil
}

func validateReservedPartIdentities(ctx context.Context, tx pgx.Tx, reservationID uuid.UUID, expected []connector.ManagedPartIdentity) error {
	rows, err := tx.Query(ctx, `
SELECT part_kind,part_ordinal,query_id
FROM managed_part_reservation_parts
WHERE reservation_id=$1
ORDER BY part_kind,part_ordinal,query_id`, reservationID)
	if err != nil {
		return fmt.Errorf("load managed part identities: %w", err)
	}
	defer rows.Close()
	actual := make(map[string]string, len(expected))
	for rows.Next() {
		var kind, queryID string
		var ordinal int64
		if err := rows.Scan(&kind, &ordinal, &queryID); err != nil {
			return fmt.Errorf("scan managed part identity: %w", err)
		}
		actual[fmt.Sprintf("%s:%d", kind, ordinal)] = queryID
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate managed part identities: %w", err)
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("%w: managed part reservation cardinality differs", connector.ErrDeliveryConflict)
	}
	for _, part := range expected {
		if actual[fmt.Sprintf("%s:%d", part.Kind, part.Ordinal)] != part.QueryID {
			return fmt.Errorf("%w: managed part reservation identity differs", connector.ErrDeliveryConflict)
		}
	}
	return nil
}

func (c *Coordinator) prepareAttempt(ctx context.Context, fence authority.RunFence, intent connector.DeliveryIntent, checkpoint connector.Checkpoint, baselines connector.ManagedSchemaBaselinePayload, reservationRequest *connector.ManagedPartReservationRequest, reservationObserver partauthority.Prepared) (uuid.UUID, *postgresPartReservation, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, nil, fmt.Errorf("begin delivery attempt: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return uuid.Nil, nil, err
	}
	if err := ensureManifest(ctx, tx, fence, intent, checkpoint, baselines); err != nil {
		return uuid.Nil, nil, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(pg_catalog.hashtextextended($1,0))`, strings.Join([]string{fence.FlowIncarnationID.String(), intent.DestinationRevisionID, intent.LogicalBatchID}, "\x1f")); err != nil {
		return uuid.Nil, nil, fmt.Errorf("lock logical batch delivery attempts: %w", err)
	}
	var priorAttempts int
	if err := tx.QueryRow(ctx, `SELECT COALESCE(max(attempt_number),0) FROM delivery_attempts WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND logical_batch_id=$3`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.LogicalBatchID).Scan(&priorAttempts); err != nil {
		return uuid.Nil, nil, fmt.Errorf("count logical batch delivery attempts: %w", err)
	}
	if priorAttempts >= maxDeliveryAttempts {
		return uuid.Nil, nil, fmt.Errorf("managed delivery exhausted %d attempts for logical batch %s", maxDeliveryAttempts, intent.LogicalBatchID)
	}
	attemptID := uuid.New()
	if _, err := tx.Exec(ctx, `
INSERT INTO delivery_attempts (
  attempt_id,flow_incarnation_id,flow_id,generation,acquisition_id,lease_epoch,
  destination_revision_id,source_lineage_id,logical_batch_id,position_id,content_hash,attempt_number
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, attemptID, fence.FlowIncarnationID, fence.FlowID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, intent.DestinationRevisionID, intent.SourceLineageID, intent.LogicalBatchID, intent.PositionID, intent.ContentHash, priorAttempts+1); err != nil {
		return uuid.Nil, nil, fmt.Errorf("prepare delivery attempt: %w", err)
	}
	var reservation *postgresPartReservation
	if reservationRequest != nil {
		reservation, err = c.reserveManagedPartsTx(ctx, tx, fence, *reservationRequest, reservationObserver)
		if err != nil {
			return uuid.Nil, nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, nil, fmt.Errorf("commit delivery attempt: %w", err)
	}
	if reservation != nil {
		reservation.pool = c.pool
		telemetry.RecordClickHousePartAdmission(ctx, reservation.serverActiveParts, reservation.reservedParts, reservation.capacity, "none")
	}
	return attemptID, reservation, nil
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
WHERE attempt_id=$1 AND flow_incarnation_id=$2 AND destination_revision_id=$3 AND logical_batch_id=$4`, attemptID, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.LogicalBatchID).Scan(&attemptHash); err != nil {
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
  AND attempt.logical_batch_id=$5`, attemptID, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, intent.LogicalBatchID).Scan(&externalID, &contentHash); err != nil {
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
ON CONFLICT (flow_incarnation_id,destination_revision_id,logical_batch_id) DO UPDATE SET
  logical_batch_id=EXCLUDED.logical_batch_id
WHERE delivery_receipts.source_lineage_id=EXCLUDED.source_lineage_id
  AND delivery_receipts.position_id=EXCLUDED.position_id
  AND delivery_receipts.content_hash=EXCLUDED.content_hash
  AND delivery_receipts.external_id=EXCLUDED.external_id`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, intent.LogicalBatchID, intent.PositionID, contentHash, attemptID, externalID, fence.AcquisitionID, fence.LeaseEpoch)
	if err != nil {
		return AckGrant{}, fmt.Errorf("adopt delivery receipt: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return AckGrant{}, fmt.Errorf("%w: immutable delivery receipt differs", connector.ErrDeliveryConflict)
	}

	checkpoint, baselines, err := loadManifestAuthority(ctx, tx, fence, intent)
	if err != nil {
		return AckGrant{}, err
	}
	checkpoint, err = finalizeCheckpointAndAck(ctx, tx, fence, intent.PositionID, checkpoint)
	if err != nil {
		return AckGrant{}, err
	}
	if err := schemabaseline.UpsertExactTx(ctx, tx, fence, baselines); err != nil {
		return AckGrant{}, fmt.Errorf("advance delivery schema baselines: %w", err)
	}
	if err := completeManagedPartReservationTx(ctx, tx, fence, intent); err != nil {
		return AckGrant{}, err
	}
	if c.hooks.BeforeFinalizeCommit != nil {
		if err := c.hooks.BeforeFinalizeCommit(ctx, fence, intent); err != nil {
			return AckGrant{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return AckGrant{}, fmt.Errorf("commit delivery receipt checkpoint and ack intent: %w", err)
	}
	telemetry.RecordDeliveryOutcome(ctx, "receipt_committed")
	return AckGrant{Checkpoint: checkpoint, PositionID: intent.PositionID}, nil
}

func completeManagedPartReservationTx(ctx context.Context, tx pgx.Tx, fence authority.RunFence, intent connector.DeliveryIntent) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(pg_catalog.hashtextextended($1,0))`, "managed-part-budget\x1f"+intent.DestinationRevisionID); err != nil {
		return fmt.Errorf("lock managed part completion budget: %w", err)
	}
	var reservationID uuid.UUID
	var epoch, active, charged int64
	err := tx.QueryRow(ctx, `
SELECT reservation_id,reservation_epoch,server_active_parts
FROM managed_part_reservations
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND logical_batch_id=$3
  AND source_lineage_id=$4 AND position_id=$5 AND content_hash=$6
  AND reservation_state IN ('reserved','completed_pending_observation')
FOR UPDATE`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.LogicalBatchID, intent.SourceLineageID, intent.PositionID, intent.ContentHash).Scan(&reservationID, &epoch, &active)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load completed managed part reservation: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM managed_part_reservation_parts WHERE reservation_id=$1 AND charge_state='charged'`, reservationID).Scan(&charged); err != nil {
		return fmt.Errorf("count completed managed part charges: %w", err)
	}
	// The exact destination receipt is written last and binds every planned
	// query ID. Finalization therefore provides reconciliation evidence for an
	// external-success/progress-commit crash and may adopt all planned parts.
	if _, err := tx.Exec(ctx, `UPDATE managed_part_reservation_parts SET part_state='durable',durable_at=COALESCE(durable_at,clock_timestamp()),updated_at=clock_timestamp() WHERE reservation_id=$1 AND part_state='reserved'`, reservationID); err != nil {
		return fmt.Errorf("adopt managed part progress from receipt: %w", err)
	}
	tag, err := tx.Exec(ctx, `
UPDATE managed_part_reservations
SET reservation_state='completed_pending_observation',completed_at=COALESCE(completed_at,clock_timestamp()),
    generation=$2,acquisition_id=$3,lease_epoch=$4,updated_at=clock_timestamp()
WHERE reservation_id=$1 AND reservation_state IN ('reserved','completed_pending_observation')`, reservationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch)
	if err != nil {
		return fmt.Errorf("complete managed part reservation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("managed part reservation was not completed")
	}
	return recordManagedPartEvent(ctx, tx, reservationID, epoch, "completed", fence, active, charged)
}

func releaseReclaimedManagedPartReservationTx(ctx context.Context, tx pgx.Tx, fence authority.RunFence, intent connector.DeliveryIntent, reclaimEpoch int64) error {
	var reservationID uuid.UUID
	var epoch, active int64
	err := tx.QueryRow(ctx, `
SELECT reservation_id,reservation_epoch,server_active_parts
FROM managed_part_reservations
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND logical_batch_id=$3
  AND source_lineage_id=$4 AND position_id=$5 AND content_hash=$6
  AND reservation_state='reclaim_pending' AND reclaim_epoch=$7
  AND generation=$8 AND acquisition_id=$9 AND lease_epoch=$10
FOR UPDATE`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.LogicalBatchID, intent.SourceLineageID, intent.PositionID, intent.ContentHash, reclaimEpoch, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch).Scan(&reservationID, &epoch, &active)
	if err != nil {
		return fmt.Errorf("load managed part reservation for reclaim release: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE managed_part_reservation_parts SET part_state='released',charge_state='released',released_at=clock_timestamp(),updated_at=clock_timestamp() WHERE reservation_id=$1`, reservationID); err != nil {
		return fmt.Errorf("release reclaimed managed part identities: %w", err)
	}
	tag, err := tx.Exec(ctx, `
UPDATE managed_part_reservations
SET reservation_state='released',released_at=clock_timestamp(),updated_at=clock_timestamp()
WHERE reservation_id=$1 AND reservation_state='reclaim_pending' AND reclaim_epoch=$2`, reservationID, reclaimEpoch)
	if err != nil {
		return fmt.Errorf("release reclaimed managed part reservation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("reclaimed managed part reservation was not released")
	}
	return recordManagedPartEvent(ctx, tx, reservationID, epoch, "released", fence, active, 0)
}

// ReclaimManagedPartReservation is a versioned two-phase protocol. Phase one
// commits reclaim_pending under the budget lock and rebinds ownership to the
// demonstrably newer fence. GuardPartWrite takes the same lock and verifies the
// row owner, so no stale authorization can be used after phase one. Phase two
// re-locks the budget, obtains fresh two-endpoint absence/quiescence proof, and
// releases the exact epoch.
func (c *Coordinator) ReclaimManagedPartReservation(ctx context.Context, fence authority.RunFence, intent connector.DeliveryIntent, driver connector.ManagedPartReservationReconciler) error {
	if driver == nil {
		return errors.New("managed part reservation reconciler is required")
	}
	if err := intent.Validate(); err != nil {
		return err
	}
	if intent.FlowIncarnationID != fence.FlowIncarnationID.String() || intent.Generation != fence.Generation || intent.AcquisitionID != fence.AcquisitionID.String() || intent.LeaseEpoch != fence.LeaseEpoch {
		return fmt.Errorf("%w: reservation reclaim intent does not match run fence", authority.ErrFenceRejected)
	}

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin managed part reclaim phase one: %w", err)
	}
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(pg_catalog.hashtextextended($1,0))`, "managed-part-budget\x1f"+intent.DestinationRevisionID); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	var reservationID, oldAcquisition uuid.UUID
	var state string
	var oldGeneration, oldLease, epoch, reclaimEpoch, active int64
	err = tx.QueryRow(ctx, `
SELECT reservation_id,acquisition_id,generation,lease_epoch,reservation_state,reservation_epoch,reclaim_epoch,server_active_parts
FROM managed_part_reservations
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND logical_batch_id=$3
  AND source_lineage_id=$4 AND position_id=$5 AND content_hash=$6
FOR UPDATE`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.LogicalBatchID, intent.SourceLineageID, intent.PositionID, intent.ContentHash).Scan(&reservationID, &oldAcquisition, &oldGeneration, &oldLease, &state, &epoch, &reclaimEpoch, &active)
	if err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("load managed part reclaim owner: %w", err)
	}
	if state == "reclaim_pending" {
		if oldAcquisition != fence.AcquisitionID || oldGeneration != fence.Generation || oldLease != fence.LeaseEpoch {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("%w: reclaim is owned by another fence", authority.ErrFenceRejected)
		}
	} else {
		if state != "reserved" || oldAcquisition == fence.AcquisitionID || oldGeneration > fence.Generation || oldGeneration == fence.Generation && oldLease >= fence.LeaseEpoch {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("%w: managed part reservation owner is not demonstrably superseded", authority.ErrFenceRejected)
		}
		reclaimEpoch++
		tag, updateErr := tx.Exec(ctx, `
UPDATE managed_part_reservations SET reservation_state='reclaim_pending',reclaim_epoch=$2,reclaim_started_at=clock_timestamp(),
  generation=$3,acquisition_id=$4,lease_epoch=$5,updated_at=clock_timestamp()
WHERE reservation_id=$1 AND reservation_state='reserved' AND acquisition_id=$6 AND generation=$7 AND lease_epoch=$8`, reservationID, reclaimEpoch, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, oldAcquisition, oldGeneration, oldLease)
		if updateErr != nil || tag.RowsAffected() != 1 {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("start managed part reclaim: %w", updateErr)
		}
		if err := recordManagedPartEvent(ctx, tx, reservationID, epoch, "reclaim_started", fence, active, 0); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit managed part reclaim phase one: %w", err)
	}

	tx, err = c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin managed part reclaim phase two: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(pg_catalog.hashtextextended($1,0))`, "managed-part-budget\x1f"+intent.DestinationRevisionID); err != nil {
		return err
	}
	observation, err := driver.ObserveManagedPartReservation(ctx, intent, true)
	if err != nil {
		return fmt.Errorf("prove managed part reservation absence: %w", err)
	}
	if observation.EndpointCount != 2 || !observation.Quiescent || !observation.BatchAbsent || observation.ServerActiveParts > math.MaxInt64 {
		return fmt.Errorf("%w: reclaim observation is incomplete", connector.ErrDeliveryIndeterminate)
	}
	if err := observeManagedPartChargesTx(ctx, tx, fence, intent.DestinationRevisionID, int64(observation.ServerActiveParts)); err != nil {
		return err
	}
	var receiptExists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM delivery_receipts WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND logical_batch_id=$3)`, fence.FlowIncarnationID, intent.DestinationRevisionID, intent.LogicalBatchID).Scan(&receiptExists); err != nil {
		return err
	}
	if receiptExists {
		return errors.New("managed part reservation with a delivery receipt cannot be reclaimed as absent")
	}
	if err := releaseReclaimedManagedPartReservationTx(ctx, tx, fence, intent, reclaimEpoch); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit managed part reclaim phase two: %w", err)
	}
	return nil
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
    AND NOT EXISTS (
      SELECT 1 FROM managed_part_reservations AS reservation
      WHERE reservation.flow_incarnation_id=manifest.flow_incarnation_id
        AND reservation.destination_revision_id=manifest.destination_revision_id
        AND reservation.logical_batch_id=manifest.logical_batch_id
        AND reservation.reservation_state='reserved'
    )
  ORDER BY manifest.created_at,manifest.destination_revision_id,manifest.logical_batch_id
  LIMIT $4
  FOR UPDATE OF manifest
), deleted_part_events AS (
  DELETE FROM managed_part_reservation_events AS event
  USING managed_part_reservations AS reservation,candidates
  WHERE event.reservation_id=reservation.reservation_id
    AND reservation.flow_incarnation_id=$1
    AND reservation.destination_revision_id=candidates.destination_revision_id
    AND reservation.logical_batch_id=candidates.logical_batch_id
    AND reservation.reservation_state='released'
  RETURNING event.reservation_id
), deleted_part_identities AS (
  DELETE FROM managed_part_reservation_parts AS part
  USING managed_part_reservations AS reservation,candidates
  WHERE part.reservation_id=reservation.reservation_id
    AND reservation.flow_incarnation_id=$1
    AND reservation.destination_revision_id=candidates.destination_revision_id
    AND reservation.logical_batch_id=candidates.logical_batch_id
    AND reservation.reservation_state='released'
  RETURNING part.reservation_id
), deleted_reservation_children AS MATERIALIZED (
  SELECT
    (SELECT count(*) FROM deleted_part_events) AS deleted_events,
    (SELECT count(*) FROM deleted_part_identities) AS deleted_parts
), deleted_part_reservations AS (
  DELETE FROM managed_part_reservations AS reservation
  USING candidates,deleted_reservation_children
  WHERE reservation.flow_incarnation_id=$1
    AND reservation.destination_revision_id=candidates.destination_revision_id
    AND reservation.logical_batch_id=candidates.logical_batch_id
    AND reservation.reservation_state='released'
    AND deleted_reservation_children.deleted_events>=0
    AND deleted_reservation_children.deleted_parts>=0
  RETURNING reservation.reservation_id
), deleted_reservations_complete AS MATERIALIZED (
  SELECT count(*) AS deleted_reservations FROM deleted_part_reservations
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
  DELETE FROM delivery_manifests AS manifest USING candidates,deleted_reservations_complete
  WHERE manifest.flow_incarnation_id=$1
    AND manifest.destination_revision_id=candidates.destination_revision_id
    AND manifest.position_id=candidates.position_id
    AND deleted_reservations_complete.deleted_reservations>=0
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

func finalizeCheckpointAndAck(ctx context.Context, tx pgx.Tx, fence authority.RunFence, positionID string, checkpoint connector.Checkpoint) (connector.Checkpoint, error) {
	canonicalLSN, err := connector.CanonicalizeCheckpointPosition(checkpoint.LSN)
	if err != nil {
		return connector.Checkpoint{}, err
	}
	checkpoint.LSN = canonicalLSN
	if checkpoint.Metadata == nil {
		checkpoint.Metadata = map[string]string{}
	}
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
			if err := json.Unmarshal(currentMetadata, &current.Metadata); err != nil {
				return connector.Checkpoint{}, fmt.Errorf("decode authoritative checkpoint metadata: %w", err)
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
