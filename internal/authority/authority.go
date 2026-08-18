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

// CleanupFence is a purpose-built terminal source-resource capability. It is
// acquired only after lifecycle cancellation has quiesced a stopping/stopped
// generation and is rejected by ordinary managed data-plane repositories.
type CleanupFence = connector.CleanupFence

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

// CleanupStore exposes only terminal lifecycle authority operations. Keeping
// it separate from Store prevents data-plane fakes and callers from acquiring
// cleanup authority.
type CleanupStore interface {
	AcquireCleanupFence(context.Context, string, int64, time.Duration) (CleanupFence, error)
	RenewCleanupFence(context.Context, CleanupFence, time.Duration) error
	GuardCleanupFence(context.Context, CleanupFence, time.Duration, connector.CleanupResourceIdentity, func(context.Context) error) error
	FinishCleanup(context.Context, CleanupFence, string) error
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

// AcquireCleanupFence acquires exclusive terminal-cleanup authority for a
// quiescent stopping or stopped flow generation. It deliberately cannot be
// used as ordinary running producer authority.
func (s *PostgresStore) AcquireCleanupFence(ctx context.Context, flowID string, generation int64, lease time.Duration) (CleanupFence, error) {
	if flowID == "" || generation <= 0 || lease <= 0 {
		return CleanupFence{}, errors.New("flow, positive generation, and cleanup lease are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return CleanupFence{}, fmt.Errorf("begin cleanup acquisition: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockFlowAuthority(ctx, tx, flowID); err != nil {
		return CleanupFence{}, err
	}
	var incarnationID uuid.UUID
	var currentGeneration int64
	var state, target string
	if err := tx.QueryRow(ctx, `
SELECT incarnation_id,lifecycle_generation,state,lifecycle_target
FROM flows WHERE id=$1 FOR UPDATE`, flowID).Scan(&incarnationID, &currentGeneration, &state, &target); err != nil {
		return CleanupFence{}, fmt.Errorf("load cleanup flow authority: %w", err)
	}
	if currentGeneration != generation || target != "stopped" || (state != "stopping" && state != "stopped") {
		return CleanupFence{}, fmt.Errorf("%w: cleanup requires stopping/stopped target at generation %d, got state=%s target=%s generation=%d", ErrFenceRejected, generation, state, target, currentGeneration)
	}
	var active int
	if err := tx.QueryRow(ctx, `
SELECT count(*) FROM flow_executions
WHERE incarnation_id=$1 AND generation<=$2 AND status='running'`, incarnationID, generation).Scan(&active); err != nil {
		return CleanupFence{}, fmt.Errorf("inspect active cleanup executions: %w", err)
	}
	if active != 0 {
		return CleanupFence{}, fmt.Errorf("%w: terminal cleanup requires quiescent executions", ErrLeaseHeld)
	}
	leaseEpoch := int64(1)
	var previousEpoch int64
	var leaseLive bool
	err = tx.QueryRow(ctx, `
SELECT lease_epoch,lease_expires_at>clock_timestamp()
FROM producer_leases WHERE incarnation_id=$1 FOR UPDATE`, incarnationID).Scan(&previousEpoch, &leaseLive)
	switch {
	case err == nil:
		if leaseLive {
			return CleanupFence{}, fmt.Errorf("%w: flow %q cleanup", ErrLeaseHeld, flowID)
		}
		leaseEpoch = previousEpoch + 1
	case !errors.Is(err, pgx.ErrNoRows):
		return CleanupFence{}, fmt.Errorf("load cleanup lease: %w", err)
	}
	acquisitionID := uuid.New()
	executionID := "lifecycle-cleanup-" + uuid.NewString()
	if _, err := tx.Exec(ctx, `
INSERT INTO execution_acquisitions(acquisition_id,incarnation_id,generation,execution_id,backend,lease_epoch)
VALUES($1,$2,$3,$4,'lifecycle_cleanup',$5)`, acquisitionID, incarnationID, generation, executionID, leaseEpoch); err != nil {
		return CleanupFence{}, fmt.Errorf("insert cleanup acquisition: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO producer_leases(incarnation_id,generation,acquisition_id,lease_epoch,lease_expires_at)
VALUES($1,$2,$3,$4,clock_timestamp()+$5::interval)
ON CONFLICT(incarnation_id) DO UPDATE SET generation=EXCLUDED.generation,
 acquisition_id=EXCLUDED.acquisition_id,lease_epoch=EXCLUDED.lease_epoch,
 lease_expires_at=EXCLUDED.lease_expires_at,updated_at=clock_timestamp()`, incarnationID, generation, acquisitionID, leaseEpoch, lease.String()); err != nil {
		return CleanupFence{}, fmt.Errorf("store cleanup lease: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return CleanupFence{}, fmt.Errorf("commit cleanup acquisition: %w", err)
	}
	return CleanupFence{RunFence: RunFence{
		FlowIncarnationID: incarnationID,
		FlowID:            flowID,
		Generation:        generation,
		AcquisitionID:     acquisitionID,
		ExecutionID:       executionID,
		LeaseEpoch:        leaseEpoch,
	}}, nil
}

// RenewCleanupFence extends only the exact live terminal-cleanup acquisition.
// The authority lock serializes renewal with lifecycle transitions and producer
// takeovers; renewal fails closed if an execution becomes active again.
func (s *PostgresStore) RenewCleanupFence(ctx context.Context, fence CleanupFence, lease time.Duration) error {
	tx, err := s.beginCleanupGuard(ctx, fence, lease)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit cleanup renewal: %w", err)
	}
	return nil
}

// GuardCleanupFence holds the per-flow authority lock across one irreversible
// external operation. A replacement acquisition cannot complete while the
// callback is running, even if wall-clock lease expiry passes. On callback
// return the exact owner is renewed again before the lock is released.
func (s *PostgresStore) GuardCleanupFence(ctx context.Context, fence CleanupFence, lease time.Duration, identity connector.CleanupResourceIdentity, operation func(context.Context) error) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	if identity.FlowIncarnationID != fence.FlowIncarnationID {
		return fmt.Errorf("%w: cleanup resource incarnation differs from fence", ErrFenceRejected)
	}
	if operation == nil {
		return errors.New("cleanup guarded operation is required")
	}
	tx, err := s.beginCleanupGuard(ctx, fence, lease)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended($1,0))`, identity.AuthorityKey()); err != nil {
		return fmt.Errorf("lock cleanup resource identity: %w", err)
	}
	var conflicting int
	if err := tx.QueryRow(ctx, `
SELECT count(*) FROM source_resources
WHERE source_system_id=$1 AND database_name=$2 AND resource_kind=$3 AND physical_name=$4
  AND state <> 'retired'
  AND NOT (flow_incarnation_id=$5 AND resource_id=$6)`, identity.SourceSystemID, identity.DatabaseName, identity.ResourceKind, identity.PhysicalName, identity.FlowIncarnationID, identity.ResourceID).Scan(&conflicting); err != nil {
		return fmt.Errorf("inspect cleanup resource aliases: %w", err)
	}
	if conflicting != 0 {
		return fmt.Errorf("%w: terminal %s %q is recorded by another flow and cannot be deleted", connector.ErrDeliveryConflict, identity.ResourceKind, identity.PhysicalName)
	}
	operationErr := operation(ctx)
	if err := renewCleanupFenceTx(ctx, tx, fence, lease, false); err != nil {
		return errors.Join(operationErr, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return errors.Join(operationErr, fmt.Errorf("commit guarded cleanup operation: %w", err))
	}
	return operationErr
}

func (s *PostgresStore) beginCleanupGuard(ctx context.Context, fence CleanupFence, lease time.Duration) (pgx.Tx, error) {
	if err := fence.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFenceRejected, err)
	}
	if lease <= 0 {
		return nil, errors.New("positive cleanup lease is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin cleanup renewal: %w", err)
	}
	if err := lockFlowAuthority(ctx, tx, fence.FlowID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	if err := renewCleanupFenceTx(ctx, tx, fence, lease, true); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

func renewCleanupFenceTx(ctx context.Context, tx pgx.Tx, fence CleanupFence, lease time.Duration, requireLive bool) error {
	tag, err := tx.Exec(ctx, `
UPDATE producer_leases AS producer
SET lease_expires_at=clock_timestamp()+$7::interval,
    updated_at=clock_timestamp()
FROM flows AS flow, execution_acquisitions AS acquisition
WHERE flow.id=$1
  AND flow.incarnation_id=$2
  AND flow.lifecycle_generation=$3
  AND flow.lifecycle_target='stopped'
  AND flow.state IN ('stopping','stopped')
  AND producer.incarnation_id=$2
  AND producer.generation=$3
  AND producer.acquisition_id=$4
  AND producer.lease_epoch=$5
  AND (NOT $8::boolean OR producer.lease_expires_at>clock_timestamp())
  AND acquisition.acquisition_id=$4
  AND acquisition.incarnation_id=$2
  AND acquisition.generation=$3
  AND acquisition.lease_epoch=$5
  AND acquisition.execution_id=$6
  AND acquisition.backend='lifecycle_cleanup'
  AND acquisition.finished_at IS NULL
  AND NOT EXISTS (
    SELECT 1 FROM flow_executions AS execution
    WHERE execution.incarnation_id=$2
      AND execution.generation<=$3
      AND execution.status='running'
  )`, fence.FlowID, fence.FlowIncarnationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, fence.ExecutionID, lease.String(), requireLive)
	if err != nil {
		return fmt.Errorf("renew cleanup lease: %w", err)
	}
	if tag.RowsAffected() != 1 {
		telemetry.RecordFenceRejection(ctx, fence.FlowID)
		return fmt.Errorf("%w: renew terminal cleanup fence for flow=%s generation=%d", ErrLeaseExpired, fence.FlowID, fence.Generation)
	}
	return nil
}

// FinishCleanup releases terminal cleanup authority after the lifecycle
// process has either proved cleanup or recorded a recoverable failure.
func (s *PostgresStore) FinishCleanup(ctx context.Context, fence CleanupFence, reason string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin cleanup finish: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := ValidateCleanupFence(ctx, tx, fence); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
UPDATE execution_acquisitions
SET finished_at=clock_timestamp(),finish_reason=$6
WHERE acquisition_id=$1 AND incarnation_id=$2 AND generation=$3
  AND lease_epoch=$4 AND execution_id=$5 AND backend='lifecycle_cleanup'
  AND finished_at IS NULL`, fence.AcquisitionID, fence.FlowIncarnationID, fence.Generation, fence.LeaseEpoch, fence.ExecutionID, reason)
	if err != nil {
		return fmt.Errorf("finish cleanup acquisition: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: finish cleanup", ErrFenceRejected)
	}
	if _, err := tx.Exec(ctx, `
UPDATE producer_leases SET lease_expires_at=clock_timestamp(),updated_at=clock_timestamp()
WHERE incarnation_id=$1 AND acquisition_id=$2 AND lease_epoch=$3`, fence.FlowIncarnationID, fence.AcquisitionID, fence.LeaseEpoch); err != nil {
		return fmt.Errorf("expire cleanup lease: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit cleanup finish: %w", err)
	}
	return nil
}

// ValidateCleanupFence guards terminal cleanup mutations in their PostgreSQL
// transaction without broadening ordinary RunFence validity to stopped flows.
func ValidateCleanupFence(ctx context.Context, tx pgx.Tx, fence CleanupFence) error {
	if err := fence.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrFenceRejected, err)
	}
	if tx == nil {
		return errors.New("cleanup fence validation requires a PostgreSQL transaction")
	}
	if err := lockFlowAuthority(ctx, tx, fence.FlowID); err != nil {
		return err
	}
	var valid bool
	if err := tx.QueryRow(ctx, `
SELECT EXISTS(
 SELECT 1 FROM flows AS flow
 JOIN producer_leases AS producer ON producer.incarnation_id=flow.incarnation_id
 JOIN execution_acquisitions AS acquisition ON acquisition.acquisition_id=producer.acquisition_id
 WHERE flow.id=$1 AND flow.incarnation_id=$2 AND flow.lifecycle_generation=$3
   AND flow.lifecycle_target='stopped' AND flow.state IN ('stopping','stopped')
   AND producer.generation=$3 AND producer.acquisition_id=$4 AND producer.lease_epoch=$5
   AND producer.lease_expires_at>clock_timestamp()
   AND acquisition.incarnation_id=$2 AND acquisition.generation=$3
   AND acquisition.lease_epoch=$5
   AND acquisition.execution_id=$6 AND acquisition.backend='lifecycle_cleanup'
   AND acquisition.finished_at IS NULL
   AND NOT EXISTS (
     SELECT 1 FROM flow_executions AS execution
     WHERE execution.incarnation_id=$2
       AND execution.generation<=$3
       AND execution.status='running'
   )
)`, fence.FlowID, fence.FlowIncarnationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, fence.ExecutionID).Scan(&valid); err != nil {
		return fmt.Errorf("validate cleanup fence: %w", err)
	}
	if !valid {
		telemetry.RecordFenceRejection(ctx, fence.FlowID)
		return fmt.Errorf("%w: invalid terminal cleanup fence for flow=%s generation=%d", ErrFenceRejected, fence.FlowID, fence.Generation)
	}
	return nil
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
	if err := fence.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrFenceRejected, err)
	}
	if tx == nil {
		return errors.New("run fence validation requires a PostgreSQL transaction")
	}
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

// ValidateClaimFence validates both the producer and exact work-claim epochs in
// the transaction that records task progress or a receipt.
func ValidateClaimFence(ctx context.Context, tx pgx.Tx, claim ClaimFence) error {
	if err := claim.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrFenceRejected, err)
	}
	if err := ValidateRunFence(ctx, tx, claim.RunFence); err != nil {
		return err
	}
	var valid bool
	if err := tx.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1 FROM work_claims
  WHERE incarnation_id=$1
    AND claim_kind=$2
    AND work_id=$3
    AND generation=$4
    AND acquisition_id=$5
    AND lease_epoch=$6
    AND claim_epoch=$7
    AND released_at IS NULL
    AND claim_expires_at > clock_timestamp()
)`, claim.FlowIncarnationID, claim.Kind, claim.WorkID, claim.Generation, claim.AcquisitionID, claim.LeaseEpoch, claim.ClaimEpoch).Scan(&valid); err != nil {
		return fmt.Errorf("validate claim fence: %w", err)
	}
	if !valid {
		telemetry.RecordFenceRejection(ctx, claim.FlowID)
		return fmt.Errorf("%w: claim=%s/%s epoch=%d", ErrFenceRejected, claim.Kind, claim.WorkID, claim.ClaimEpoch)
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
var _ CleanupStore = (*PostgresStore)(nil)
