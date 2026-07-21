package authority

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

var (
	ErrFenceRejected = errors.New("run fence rejected")
	ErrLeaseHeld     = errors.New("producer lease is held")
	ErrLeaseExpired  = errors.New("producer lease expired")
	ErrClaimHeld     = errors.New("work claim is held")
)

type RunFence = connector.RunFence
type ClaimKind = connector.ClaimKind
type ClaimFence = connector.ClaimFence

const (
	ClaimSnapshot = connector.ClaimSnapshot
	ClaimDelivery = connector.ClaimDelivery
	ClaimConsumer = connector.ClaimConsumer
	ClaimGC       = connector.ClaimGC
)

// Store exposes typed authority operations. It intentionally does not expose
// arbitrary callbacks running inside authority transactions.
type Store interface {
	AcquireProducer(context.Context, string, string, string, int64, time.Duration) (RunFence, error)
	RenewProducer(context.Context, RunFence, time.Duration) error
	FinishProducer(context.Context, RunFence, string) error
	FailFlow(context.Context, RunFence, string) error
	AcquireClaim(context.Context, RunFence, ClaimKind, string, time.Duration) (ClaimFence, error)
	RenewClaim(context.Context, ClaimFence, time.Duration) error
	ReleaseClaim(context.Context, ClaimFence) error
}

// PostgresStore keeps generations, acquisitions, leases, and claims in the
// control PostgreSQL database. The pool is owned by the caller.
type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) (*PostgresStore, error) {
	if pool == nil {
		return nil, errors.New("authority postgres pool is required")
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) AcquireProducer(ctx context.Context, flowID, executionID, backend string, generation int64, lease time.Duration) (RunFence, error) {
	if flowID == "" || executionID == "" || backend == "" {
		return RunFence{}, errors.New("flow, execution, and backend are required")
	}
	if generation <= 0 || lease <= 0 {
		return RunFence{}, errors.New("positive generation and lease are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return RunFence{}, fmt.Errorf("begin producer acquisition: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockFlowAuthority(ctx, tx, flowID); err != nil {
		return RunFence{}, err
	}

	var incarnationID uuid.UUID
	var currentGeneration int64
	var state, target string
	if err := tx.QueryRow(ctx, `
SELECT incarnation_id, lifecycle_generation, state, lifecycle_target
FROM flows
WHERE id = $1
FOR UPDATE`, flowID).Scan(&incarnationID, &currentGeneration, &state, &target); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RunFence{}, fmt.Errorf("%w: flow %q does not exist", ErrFenceRejected, flowID)
		}
		return RunFence{}, fmt.Errorf("load flow authority: %w", err)
	}
	if currentGeneration != generation || state != "running" || target != "running" {
		return RunFence{}, fmt.Errorf(
			"%w: flow %q is state=%s target=%s generation=%d, requested generation=%d",
			ErrFenceRejected,
			flowID,
			state,
			target,
			currentGeneration,
			generation,
		)
	}

	leaseEpoch := int64(1)
	var previousEpoch int64
	var leaseLive bool
	err = tx.QueryRow(ctx, `
SELECT lease_epoch, lease_expires_at > clock_timestamp()
FROM producer_leases
WHERE incarnation_id = $1
FOR UPDATE`, incarnationID).Scan(&previousEpoch, &leaseLive)
	switch {
	case err == nil:
		if leaseLive {
			return RunFence{}, fmt.Errorf("%w: flow %q incarnation %s", ErrLeaseHeld, flowID, incarnationID)
		}
		leaseEpoch = previousEpoch + 1
	case !errors.Is(err, pgx.ErrNoRows):
		return RunFence{}, fmt.Errorf("load producer lease: %w", err)
	}

	acquisitionID := uuid.New()
	if _, err := tx.Exec(ctx, `
INSERT INTO execution_acquisitions (
  acquisition_id, incarnation_id, generation, execution_id, backend, lease_epoch
) VALUES ($1,$2,$3,$4,$5,$6)`, acquisitionID, incarnationID, generation, executionID, backend, leaseEpoch); err != nil {
		return RunFence{}, fmt.Errorf("insert execution acquisition: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO producer_leases (
  incarnation_id, generation, acquisition_id, lease_epoch, lease_expires_at
) VALUES ($1,$2,$3,$4,clock_timestamp() + $5::interval)
ON CONFLICT (incarnation_id) DO UPDATE SET
  generation = EXCLUDED.generation,
  acquisition_id = EXCLUDED.acquisition_id,
  lease_epoch = EXCLUDED.lease_epoch,
  lease_expires_at = EXCLUDED.lease_expires_at,
  updated_at = clock_timestamp()`, incarnationID, generation, acquisitionID, leaseEpoch, lease.String()); err != nil {
		return RunFence{}, fmt.Errorf("store producer lease: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return RunFence{}, fmt.Errorf("commit producer acquisition: %w", err)
	}
	if leaseEpoch > 1 {
		telemetry.RecordLeaseTakeover(ctx, flowID)
	}
	return RunFence{
		FlowIncarnationID: incarnationID,
		FlowID:            flowID,
		Generation:        generation,
		AcquisitionID:     acquisitionID,
		ExecutionID:       executionID,
		LeaseEpoch:        leaseEpoch,
	}, nil
}

func (s *PostgresStore) RenewProducer(ctx context.Context, fence RunFence, lease time.Duration) error {
	if lease <= 0 {
		return errors.New("positive lease is required")
	}
	command, err := s.pool.Exec(ctx, `
UPDATE producer_leases AS producer
SET lease_expires_at = clock_timestamp() + $6::interval,
    updated_at = clock_timestamp()
FROM flows AS flow
WHERE producer.incarnation_id = $1
  AND producer.generation = $2
  AND producer.acquisition_id = $3
  AND producer.lease_epoch = $4
  AND producer.lease_expires_at > clock_timestamp()
  AND flow.id = $5
  AND flow.incarnation_id = producer.incarnation_id
  AND flow.lifecycle_generation = producer.generation
  AND flow.state = 'running'
  AND flow.lifecycle_target = 'running'`, fence.FlowIncarnationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, fence.FlowID, lease.String())
	if err != nil {
		return fmt.Errorf("renew producer lease: %w", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: renew producer", ErrLeaseExpired)
	}
	return nil
}

func (s *PostgresStore) FinishProducer(ctx context.Context, fence RunFence, reason string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin producer finish: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := validateProducerOwnership(ctx, tx, fence); err != nil {
		return err
	}
	command, err := tx.Exec(ctx, `
UPDATE execution_acquisitions
SET finished_at = clock_timestamp(), finish_reason = $6
WHERE acquisition_id = $1
  AND incarnation_id = $2
  AND generation = $3
  AND lease_epoch = $4
  AND execution_id = $5
  AND finished_at IS NULL`, fence.AcquisitionID, fence.FlowIncarnationID, fence.Generation, fence.LeaseEpoch, fence.ExecutionID, reason)
	if err != nil {
		return fmt.Errorf("finish acquisition: %w", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: finish producer", ErrFenceRejected)
	}
	if _, err := tx.Exec(ctx, `
UPDATE producer_leases
SET lease_expires_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE incarnation_id = $1 AND acquisition_id = $2 AND lease_epoch = $3`, fence.FlowIncarnationID, fence.AcquisitionID, fence.LeaseEpoch); err != nil {
		return fmt.Errorf("expire producer lease: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit producer finish: %w", err)
	}
	return nil
}

func (s *PostgresStore) FailFlow(ctx context.Context, fence RunFence, reason string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin fenced flow failure: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	command, err := tx.Exec(ctx, `
UPDATE flows
SET state = 'failed', lifecycle_target = 'failed', dispatch_pending = FALSE, updated_at = clock_timestamp()
WHERE id = $1 AND incarnation_id = $2 AND lifecycle_generation = $3`, fence.FlowID, fence.FlowIncarnationID, fence.Generation)
	if err != nil {
		return fmt.Errorf("fail fenced flow: %w", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: fail flow", ErrFenceRejected)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO flow_state_events (flow_id, incarnation_id, from_state, to_state, reason)
VALUES ($1,$2,'running','failed',$3)`, fence.FlowID, fence.FlowIncarnationID, reason); err != nil {
		return fmt.Errorf("record fenced failure: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit fenced flow failure: %w", err)
	}
	return nil
}

func (s *PostgresStore) AcquireClaim(ctx context.Context, fence RunFence, kind ClaimKind, workID string, lease time.Duration) (ClaimFence, error) {
	if kind == "" || workID == "" || lease <= 0 {
		return ClaimFence{}, errors.New("claim kind, work ID, and positive lease are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ClaimFence{}, fmt.Errorf("begin claim acquisition: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := ValidateRunFence(ctx, tx, fence); err != nil {
		return ClaimFence{}, err
	}
	claimEpoch := int64(1)
	var previousEpoch int64
	var live bool
	err = tx.QueryRow(ctx, `
SELECT claim_epoch, released_at IS NULL AND claim_expires_at > clock_timestamp()
FROM work_claims
WHERE incarnation_id = $1 AND claim_kind = $2 AND work_id = $3
FOR UPDATE`, fence.FlowIncarnationID, string(kind), workID).Scan(&previousEpoch, &live)
	switch {
	case err == nil:
		if live {
			return ClaimFence{}, fmt.Errorf("%w: %s/%s", ErrClaimHeld, kind, workID)
		}
		claimEpoch = previousEpoch + 1
	case !errors.Is(err, pgx.ErrNoRows):
		return ClaimFence{}, fmt.Errorf("load work claim: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO work_claims (
  incarnation_id, claim_kind, work_id, generation, acquisition_id,
  lease_epoch, claim_epoch, claim_expires_at, released_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,clock_timestamp() + $8::interval,NULL)
ON CONFLICT (incarnation_id, claim_kind, work_id) DO UPDATE SET
  generation = EXCLUDED.generation,
  acquisition_id = EXCLUDED.acquisition_id,
  lease_epoch = EXCLUDED.lease_epoch,
  claim_epoch = EXCLUDED.claim_epoch,
  claim_expires_at = EXCLUDED.claim_expires_at,
  released_at = NULL,
  updated_at = clock_timestamp()`, fence.FlowIncarnationID, string(kind), workID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, claimEpoch, lease.String()); err != nil {
		return ClaimFence{}, fmt.Errorf("store work claim: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ClaimFence{}, fmt.Errorf("commit claim acquisition: %w", err)
	}
	return ClaimFence{RunFence: fence, Kind: kind, WorkID: workID, ClaimEpoch: claimEpoch}, nil
}

func (s *PostgresStore) RenewClaim(ctx context.Context, claim ClaimFence, lease time.Duration) error {
	if lease <= 0 {
		return errors.New("positive lease is required")
	}
	command, err := s.pool.Exec(ctx, `
UPDATE work_claims AS claim
SET claim_expires_at = clock_timestamp() + $9::interval,
    updated_at = clock_timestamp()
FROM producer_leases AS producer, flows AS flow
WHERE claim.incarnation_id = $1
  AND claim.claim_kind = $2
  AND claim.work_id = $3
  AND claim.generation = $4
  AND claim.acquisition_id = $5
  AND claim.lease_epoch = $6
  AND claim.claim_epoch = $7
  AND claim.released_at IS NULL
  AND claim.claim_expires_at > clock_timestamp()
  AND producer.incarnation_id = claim.incarnation_id
  AND producer.acquisition_id = claim.acquisition_id
  AND producer.lease_epoch = claim.lease_epoch
  AND producer.lease_expires_at > clock_timestamp()
  AND flow.id = $8
  AND flow.incarnation_id = claim.incarnation_id
  AND flow.lifecycle_generation = claim.generation
  AND flow.state = 'running'
  AND flow.lifecycle_target = 'running'`, claim.FlowIncarnationID, string(claim.Kind), claim.WorkID, claim.Generation, claim.AcquisitionID, claim.LeaseEpoch, claim.ClaimEpoch, claim.FlowID, lease.String())
	if err != nil {
		return fmt.Errorf("renew work claim: %w", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: renew claim", ErrLeaseExpired)
	}
	return nil
}

func (s *PostgresStore) ReleaseClaim(ctx context.Context, claim ClaimFence) error {
	command, err := s.pool.Exec(ctx, `
UPDATE work_claims AS claim
SET released_at = clock_timestamp(), updated_at = clock_timestamp()
FROM producer_leases AS producer
WHERE claim.incarnation_id = $1
  AND claim.claim_kind = $2
  AND claim.work_id = $3
  AND claim.generation = $4
  AND claim.acquisition_id = $5
  AND claim.lease_epoch = $6
  AND claim.claim_epoch = $7
  AND claim.released_at IS NULL
  AND producer.incarnation_id = claim.incarnation_id
  AND producer.acquisition_id = claim.acquisition_id
  AND producer.lease_epoch = claim.lease_epoch
  AND producer.lease_expires_at > clock_timestamp()`, claim.FlowIncarnationID, string(claim.Kind), claim.WorkID, claim.Generation, claim.AcquisitionID, claim.LeaseEpoch, claim.ClaimEpoch)
	if err != nil {
		return fmt.Errorf("release work claim: %w", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: release claim", ErrFenceRejected)
	}
	return nil
}

func validateProducerOwnership(ctx context.Context, tx pgx.Tx, fence RunFence) error {
	if err := lockFlowAuthority(ctx, tx, fence.FlowID); err != nil {
		return err
	}
	var valid bool
	if err := tx.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM flows AS flow
  JOIN producer_leases AS producer ON producer.incarnation_id=flow.incarnation_id
  JOIN execution_acquisitions AS acquisition ON acquisition.acquisition_id=producer.acquisition_id
  WHERE flow.id=$1
    AND flow.incarnation_id=$2
    AND flow.lifecycle_generation=$3
    AND producer.generation=$3
    AND producer.acquisition_id=$4
    AND producer.lease_epoch=$5
    AND producer.lease_expires_at > clock_timestamp()
    AND acquisition.finished_at IS NULL
    AND acquisition.execution_id=$6
)`, fence.FlowID, fence.FlowIncarnationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, fence.ExecutionID).Scan(&valid); err != nil {
		return fmt.Errorf("validate producer ownership: %w", err)
	}
	if !valid {
		telemetry.RecordFenceRejection(ctx, fence.FlowID)
		return fmt.Errorf("%w: flow=%s incarnation=%s generation=%d acquisition=%s epoch=%d", ErrFenceRejected, fence.FlowID, fence.FlowIncarnationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch)
	}
	return nil
}

// ValidateRunFence is the shared repository guard used inside the same
// PostgreSQL transaction as an authoritative mutation. The transaction-scoped
// advisory lock is the same lock used by producer acquisition, so a takeover
// cannot commit between this validation and the guarded mutation.
func ValidateRunFence(ctx context.Context, tx pgx.Tx, fence RunFence) error {
	if err := lockFlowAuthority(ctx, tx, fence.FlowID); err != nil {
		return err
	}
	var valid bool
	if err := tx.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM flows AS flow
  JOIN producer_leases AS producer
    ON producer.incarnation_id = flow.incarnation_id
  JOIN execution_acquisitions AS acquisition
    ON acquisition.acquisition_id = producer.acquisition_id
  WHERE flow.id = $1
    AND flow.incarnation_id = $2
    AND flow.lifecycle_generation = $3
    AND flow.state = 'running'
    AND flow.lifecycle_target = 'running'
    AND producer.generation = $3
    AND producer.acquisition_id = $4
    AND producer.lease_epoch = $5
    AND producer.lease_expires_at > clock_timestamp()
    AND acquisition.finished_at IS NULL
    AND acquisition.execution_id = $6
)`, fence.FlowID, fence.FlowIncarnationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, fence.ExecutionID).Scan(&valid); err != nil {
		return fmt.Errorf("validate run fence: %w", err)
	}
	if !valid {
		telemetry.RecordFenceRejection(ctx, fence.FlowID)
		return fmt.Errorf("%w: flow=%s incarnation=%s generation=%d acquisition=%s epoch=%d", ErrFenceRejected, fence.FlowID, fence.FlowIncarnationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch)
	}
	return nil
}

func lockFlowAuthority(ctx context.Context, tx pgx.Tx, flowID string) error {
	if flowID == "" {
		return fmt.Errorf("%w: empty flow ID", ErrFenceRejected)
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", flowID); err != nil {
		return fmt.Errorf("lock flow authority: %w", err)
	}
	return nil
}

var _ Store = (*PostgresStore)(nil)
