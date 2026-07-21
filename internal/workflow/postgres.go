package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/controlstore"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

const (
	stateCreateReason       = "create"
	stateStartReason        = "start"
	statePauseReason        = "pause"
	stateStopReason         = "stop"
	stateStopCompleteReason = "stop_complete"
	stateResumeReason       = "resume"
	stateFailReason         = "fail"
)

// PostgresEngine stores flow metadata and internal lifecycle fencing in Postgres.
// lockPool may be the same shared control pool; advisory locks are held by one
// acquired connection while callbacks use another pooled connection.
type PostgresEngine struct {
	pool     *pgxpool.Pool
	lockPool *pgxpool.Pool
	ownsPool bool
}

func NewPostgresEngine(ctx context.Context, dsn string) (*PostgresEngine, error) {
	if dsn == "" {
		return nil, errors.New("postgres DSN is required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres DSN: %w", err)
	}
	controlstore.ConfigurePool(cfg)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	engine, err := NewPostgresEngineWithPool(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, err
	}
	engine.ownsPool = true
	return engine, nil
}

// NewPostgresEngineWithPool borrows one shared control PostgreSQL pool.
func NewPostgresEngineWithPool(ctx context.Context, pool *pgxpool.Pool) (*PostgresEngine, error) {
	if pool == nil {
		return nil, errors.New("postgres control pool is required")
	}
	if err := runMigrations(ctx, pool); err != nil {
		return nil, err
	}
	return &PostgresEngine{pool: pool, lockPool: pool}, nil
}

func (p *PostgresEngine) Close() {
	if p.ownsPool && p.pool != nil {
		p.pool.Close()
	}
	p.pool = nil
	p.lockPool = nil
}

func (p *PostgresEngine) WithFlowLock(ctx context.Context, flowID string, try bool, fn func() error) (acquired bool, retErr error) {
	if flowID == "" {
		return false, errors.New("flow id is required")
	}
	if p.lockPool == nil {
		return false, errors.New("lifecycle lock pool is not initialized")
	}
	conn, err := p.lockPool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("acquire lifecycle lock connection: %w", err)
	}
	defer conn.Release()
	lockName := "wallaby-flow-lifecycle:" + flowID
	acquired = true
	if try {
		if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lockName).Scan(&acquired); err != nil {
			return false, fmt.Errorf("try lifecycle lock: %w", err)
		}
		if !acquired {
			return false, nil
		}
	} else if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtextextended($1, 0))", lockName); err != nil {
		return false, fmt.Errorf("acquire lifecycle lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var unlocked bool
		if err := conn.QueryRow(unlockCtx, "SELECT pg_advisory_unlock(hashtextextended($1, 0))", lockName).Scan(&unlocked); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("unlock lifecycle lock: %w", err))
		} else if !unlocked {
			retErr = errors.Join(retErr, errors.New("unlock lifecycle lock: lock was not held by the dedicated connection"))
		}
	}()
	return true, fn()
}

func (p *PostgresEngine) Create(ctx context.Context, f flow.Flow) (flow.Flow, error) {
	if f.ID == "" {
		return flow.Flow{}, errors.New("flow id is required")
	}
	if f.State == "" {
		f.State = flow.StateCreated
	}
	if f.State != flow.StateCreated {
		return flow.Flow{}, fmt.Errorf("%w: flows must be created in state %s", ErrInvalidState, flow.StateCreated)
	}
	if f.Parallelism <= 0 {
		f.Parallelism = 1
	}
	sourceJSON, destJSON, configJSON, err := marshalFlowFields(f)
	if err != nil {
		return flow.Flow{}, err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return flow.Flow{}, fmt.Errorf("begin create flow: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `INSERT INTO flows
		(id, name, source, destinations, state, wire_format, parallelism, config, lifecycle_target, lifecycle_generation, dispatch_pending)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0,FALSE)`, f.ID, f.Name, sourceJSON, destJSON, string(f.State), emptyToNull(string(f.WireFormat)), f.Parallelism, configJSON, string(TargetCreated))
	if err != nil {
		if isUniqueViolation(err) {
			return flow.Flow{}, ErrAlreadyExists
		}
		return flow.Flow{}, fmt.Errorf("insert flow: %w", err)
	}
	if err := recordStateEvent(ctx, tx, f.ID, "", string(f.State), stateCreateReason); err != nil {
		return flow.Flow{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return flow.Flow{}, fmt.Errorf("commit create flow: %w", err)
	}
	return f, nil
}

func (p *PostgresEngine) Update(ctx context.Context, f flow.Flow) (flow.Flow, error) {
	if f.ID == "" {
		return flow.Flow{}, errors.New("flow id is required")
	}
	if f.Parallelism <= 0 {
		f.Parallelism = 1
	}
	sourceJSON, destJSON, configJSON, err := marshalFlowFields(f)
	if err != nil {
		return flow.Flow{}, err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return flow.Flow{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var incarnationID uuid.UUID
	var state string
	var identityChanged bool
	if err := tx.QueryRow(ctx, `
SELECT incarnation_id,state,
       source IS DISTINCT FROM $2::jsonb OR destinations IS DISTINCT FROM $3::jsonb
FROM flows WHERE id=$1 FOR UPDATE`, f.ID, sourceJSON, destJSON).Scan(&incarnationID, &state, &identityChanged); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return flow.Flow{}, ErrNotFound
		}
		return flow.Flow{}, err
	}
	if identityChanged {
		if state == string(flow.StateRunning) || state == string(flow.StateStopping) {
			return flow.Flow{}, fmt.Errorf("%w: source or destination identity cannot change while flow is %s", ErrInvalidState, state)
		}
		newIncarnationID := uuid.New()
		if _, err := tx.Exec(ctx, `INSERT INTO flow_incarnations(incarnation_id,flow_id) VALUES($1,$2)`, newIncarnationID, f.ID); err != nil {
			return flow.Flow{}, fmt.Errorf("create updated flow incarnation: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE flow_incarnations SET retired_at=COALESCE(retired_at,clock_timestamp()) WHERE incarnation_id=$1`, incarnationID); err != nil {
			return flow.Flow{}, fmt.Errorf("retire prior flow incarnation: %w", err)
		}
		incarnationID = newIncarnationID
	}
	updated, err := scanFlow(tx.QueryRow(ctx, `UPDATE flows SET name=$2, source=$3, destinations=$4, wire_format=$5,
		parallelism=$6, config=$7, incarnation_id=$8,
		lifecycle_generation=CASE WHEN incarnation_id IS DISTINCT FROM $8 THEN 0 ELSE lifecycle_generation END,
		updated_at=now() WHERE id=$1
		RETURNING id,name,source,destinations,state,wire_format,parallelism,config`, f.ID, f.Name, sourceJSON, destJSON, emptyToNull(string(f.WireFormat)), f.Parallelism, configJSON, incarnationID))
	if err != nil {
		return flow.Flow{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return flow.Flow{}, err
	}
	return updated, nil
}

func marshalFlowFields(f flow.Flow) ([]byte, []byte, []byte, error) {
	source, err := json.Marshal(f.Source)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal source: %w", err)
	}
	dest, err := json.Marshal(f.Destinations)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal destinations: %w", err)
	}
	config, err := json.Marshal(f.Config)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal config: %w", err)
	}
	if f.Config == (flow.Config{}) {
		config = nil
	}
	return source, dest, config, nil
}

func (p *PostgresEngine) PlanStart(ctx context.Context, flowID string, resume bool) (flow.Flow, LifecycleControl, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return flow.Flow{}, LifecycleControl{}, fmt.Errorf("begin start flow: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	current, control, err := getFlowAndControlForUpdate(ctx, tx, flowID)
	if err != nil {
		return flow.Flow{}, control, err
	}
	if current.State == flow.StateRunning && control.Target == TargetRunning {
		return current, control, nil
	}
	expected := flow.StateCreated
	reason := stateStartReason
	if resume {
		expected, reason = flow.StatePaused, stateResumeReason
	}
	if current.State != expected || LifecycleTarget(current.State) != control.Target {
		return flow.Flow{}, control, fmt.Errorf("%w: cannot start flow in state %s with target %s", ErrInvalidState, current.State, control.Target)
	}
	control.Generation++
	row := tx.QueryRow(ctx, `UPDATE flows SET state='running', lifecycle_target='running', lifecycle_generation=$2,
		dispatch_pending=TRUE, updated_at=now() WHERE id=$1
		RETURNING id,name,source,destinations,state,wire_format,parallelism,config`, flowID, control.Generation)
	updated, err := scanFlow(row)
	if err != nil {
		return flow.Flow{}, control, err
	}
	control.State, control.Target, control.DispatchPending = updated.State, TargetRunning, true
	if err := recordStateEvent(ctx, tx, flowID, string(current.State), string(updated.State), reason); err != nil {
		return flow.Flow{}, control, err
	}
	if err := tx.Commit(ctx); err != nil {
		return flow.Flow{}, control, fmt.Errorf("commit start flow: %w", err)
	}
	return updated, control, nil
}

func (p *PostgresEngine) MarkDispatched(ctx context.Context, flowID string, generation int64) error {
	tag, err := p.pool.Exec(ctx, `UPDATE flows SET dispatch_pending=FALSE, updated_at=now()
		WHERE id=$1 AND lifecycle_generation=$2 AND lifecycle_target='running' AND dispatch_pending`, flowID, generation)
	if err != nil {
		return fmt.Errorf("mark flow dispatched: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var exists bool
		_ = p.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM flows WHERE id=$1)", flowID).Scan(&exists)
		if !exists {
			return ErrNotFound
		}
		control, getErr := p.Control(ctx, flowID)
		if getErr == nil && control.Generation == generation && control.Target == TargetRunning && !control.DispatchPending {
			return nil
		}
		return fmt.Errorf("%w: dispatch generation is fenced", ErrInvalidState)
	}
	return nil
}

func (p *PostgresEngine) RequestPause(ctx context.Context, flowID string) (flow.Flow, LifecycleControl, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return flow.Flow{}, LifecycleControl{}, fmt.Errorf("begin pause request: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	current, control, err := getFlowAndControlForUpdate(ctx, tx, flowID)
	if err != nil {
		return flow.Flow{}, control, err
	}
	if current.State == flow.StatePaused && control.Target == TargetPaused {
		return current, control, nil
	}
	if current.State != flow.StateRunning || (control.Target != TargetRunning && control.Target != TargetPaused) {
		return flow.Flow{}, control, fmt.Errorf("%w: cannot pause flow in state %s with target %s", ErrInvalidState, current.State, control.Target)
	}
	if _, err = tx.Exec(ctx, "UPDATE flows SET lifecycle_target='paused', dispatch_pending=FALSE, updated_at=now() WHERE id=$1", flowID); err != nil {
		return flow.Flow{}, control, err
	}
	control.Target, control.DispatchPending = TargetPaused, false
	if err = tx.Commit(ctx); err != nil {
		return flow.Flow{}, control, fmt.Errorf("commit pause request: %w", err)
	}
	return current, control, nil
}

func (p *PostgresEngine) CompletePause(ctx context.Context, flowID string, generation int64) (flow.Flow, error) {
	return p.completeQuiescent(ctx, flowID, generation, TargetPaused, flow.StateRunning, flow.StatePaused, statePauseReason)
}

func (p *PostgresEngine) RequestStop(ctx context.Context, flowID string) (flow.Flow, LifecycleControl, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return flow.Flow{}, LifecycleControl{}, fmt.Errorf("begin stop request: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	current, control, err := getFlowAndControlForUpdate(ctx, tx, flowID)
	if err != nil {
		return flow.Flow{}, control, err
	}
	if (current.State == flow.StateStopping || current.State == flow.StateStopped) && control.Target == TargetStopped {
		return current, control, nil
	}
	if current.State != flow.StateRunning && current.State != flow.StatePaused {
		return flow.Flow{}, control, fmt.Errorf("%w: cannot stop flow in state %s", ErrInvalidState, current.State)
	}
	row := tx.QueryRow(ctx, `UPDATE flows SET state='stopping', lifecycle_target='stopped', dispatch_pending=FALSE, updated_at=now() WHERE id=$1
		RETURNING id,name,source,destinations,state,wire_format,parallelism,config`, flowID)
	updated, err := scanFlow(row)
	if err != nil {
		return flow.Flow{}, control, err
	}
	control.State, control.Target, control.DispatchPending = updated.State, TargetStopped, false
	if err = recordStateEvent(ctx, tx, flowID, string(current.State), string(updated.State), stateStopReason); err != nil {
		return flow.Flow{}, control, err
	}
	if err = tx.Commit(ctx); err != nil {
		return flow.Flow{}, control, fmt.Errorf("commit stop request: %w", err)
	}
	return updated, control, nil
}

func (p *PostgresEngine) CompleteStopGeneration(ctx context.Context, flowID string, generation int64) (flow.Flow, error) {
	control, err := p.Control(ctx, flowID)
	if err == nil && control.State == flow.StateStopped && control.Target == TargetStopped && control.Generation == generation {
		return p.Get(ctx, flowID)
	}
	return p.completeQuiescent(ctx, flowID, generation, TargetStopped, flow.StateStopping, flow.StateStopped, stateStopCompleteReason)
}

func (p *PostgresEngine) completeQuiescent(ctx context.Context, flowID string, generation int64, target LifecycleTarget, from, to flow.State, reason string) (flow.Flow, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return flow.Flow{}, fmt.Errorf("begin lifecycle completion: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	current, control, err := getFlowAndControlForUpdate(ctx, tx, flowID)
	if err != nil {
		return flow.Flow{}, err
	}
	if current.State == to && control.Target == target && control.Generation == generation {
		return current, nil
	}
	if current.State != from || control.Target != target || control.Generation != generation {
		return flow.Flow{}, fmt.Errorf("%w: lifecycle completion is fenced", ErrInvalidState)
	}
	var active int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM flow_executions WHERE flow_id=$1 AND status='running' AND generation <= $2`, flowID, generation).Scan(&active); err != nil {
		return flow.Flow{}, fmt.Errorf("count active executions: %w", err)
	}
	if active != 0 {
		return flow.Flow{}, fmt.Errorf("%w: %d active executions prevent completion", ErrInvalidState, active)
	}
	row := tx.QueryRow(ctx, `UPDATE flows SET state=$2, updated_at=now() WHERE id=$1 RETURNING id,name,source,destinations,state,wire_format,parallelism,config`, flowID, string(to))
	updated, err := scanFlow(row)
	if err != nil {
		return flow.Flow{}, err
	}
	if err = recordStateEvent(ctx, tx, flowID, string(current.State), string(to), reason); err != nil {
		return flow.Flow{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return flow.Flow{}, fmt.Errorf("commit lifecycle completion: %w", err)
	}
	return updated, nil
}

func (p *PostgresEngine) Start(ctx context.Context, id string) (flow.Flow, error) {
	f, _, e := p.PlanStart(ctx, id, false)
	return f, e
}
func (p *PostgresEngine) Resume(ctx context.Context, id string) (flow.Flow, error) {
	f, _, e := p.PlanStart(ctx, id, true)
	return f, e
}
func (p *PostgresEngine) Pause(ctx context.Context, id string) (flow.Flow, error) {
	_, c, e := p.RequestPause(ctx, id)
	if e != nil {
		return flow.Flow{}, e
	}
	return p.CompletePause(ctx, id, c.Generation)
}
func (p *PostgresEngine) Stop(ctx context.Context, id string) (flow.Flow, error) {
	f, _, e := p.RequestStop(ctx, id)
	return f, e
}
func (p *PostgresEngine) Fail(ctx context.Context, flowID string) (flow.Flow, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return flow.Flow{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	current, _, err := getFlowAndControlForUpdate(ctx, tx, flowID)
	if err != nil {
		return flow.Flow{}, err
	}
	if current.State == flow.StateStopped {
		return flow.Flow{}, fmt.Errorf("%w: stopped flow is terminal", ErrInvalidState)
	}
	row := tx.QueryRow(ctx, `UPDATE flows SET state='failed', lifecycle_target='failed', dispatch_pending=FALSE, updated_at=now() WHERE id=$1 RETURNING id,name,source,destinations,state,wire_format,parallelism,config`, flowID)
	updated, err := scanFlow(row)
	if err != nil {
		return flow.Flow{}, err
	}
	if err = recordStateEvent(ctx, tx, flowID, string(current.State), string(updated.State), stateFailReason); err != nil {
		return flow.Flow{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return flow.Flow{}, err
	}
	return updated, nil
}

func (p *PostgresEngine) Delete(ctx context.Context, flowID string) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	current, control, err := getFlowAndControlForUpdate(ctx, tx, flowID)
	if err != nil {
		return err
	}
	stable := LifecycleTarget(current.State) == control.Target || (current.State == flow.StateStopped && control.Target == TargetStopped)
	var active int
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM flow_executions WHERE flow_id=$1 AND status='running'", flowID).Scan(&active); err != nil {
		return err
	}
	if !stable || control.DispatchPending || active != 0 || current.State == flow.StateRunning || current.State == flow.StateStopping {
		return fmt.Errorf("%w: flow has incomplete lifecycle work", ErrInvalidState)
	}
	if _, err = tx.Exec(ctx, "DELETE FROM flows WHERE id=$1", flowID); err != nil {
		return fmt.Errorf("delete flow: %w", err)
	}
	return tx.Commit(ctx)
}

func (p *PostgresEngine) Get(ctx context.Context, id string) (flow.Flow, error) {
	return scanFlow(p.pool.QueryRow(ctx, "SELECT id,name,source,destinations,state,wire_format,parallelism,config FROM flows WHERE id=$1", id))
}
func (p *PostgresEngine) List(ctx context.Context) ([]flow.Flow, error) {
	rows, err := p.pool.Query(ctx, "SELECT id,name,source,destinations,state,wire_format,parallelism,config FROM flows ORDER BY created_at")
	if err != nil {
		return nil, fmt.Errorf("list flows: %w", err)
	}
	defer rows.Close()
	out := []flow.Flow{}
	for rows.Next() {
		f, e := scanFlow(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, f)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
func (p *PostgresEngine) Control(ctx context.Context, id string) (LifecycleControl, error) {
	var c LifecycleControl
	var state, target string
	err := p.pool.QueryRow(ctx, "SELECT id,state,lifecycle_target,lifecycle_generation,dispatch_pending FROM flows WHERE id=$1", id).Scan(&c.FlowID, &state, &target, &c.Generation, &c.DispatchPending)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, ErrNotFound
	}
	if err != nil {
		return c, fmt.Errorf("get lifecycle control: %w", err)
	}
	c.State, c.Target = flow.State(state), LifecycleTarget(target)
	return c, nil
}
func (p *PostgresEngine) PendingControls(ctx context.Context) ([]LifecycleControl, error) {
	rows, err := p.pool.Query(ctx, `SELECT id,state,lifecycle_target,lifecycle_generation,dispatch_pending FROM flows
		WHERE dispatch_pending OR state='stopping' OR lifecycle_target <> state ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LifecycleControl
	for rows.Next() {
		var c LifecycleControl
		var s, t string
		if err = rows.Scan(&c.FlowID, &s, &t, &c.Generation, &c.DispatchPending); err != nil {
			return nil, err
		}
		c.State, c.Target = flow.State(s), LifecycleTarget(t)
		out = append(out, c)
	}
	return out, rows.Err()
}

func (p *PostgresEngine) RegisterExecutionGeneration(ctx context.Context, flowID, executionID, backend string, generation int64, lease time.Duration) error {
	if executionID == "" {
		return errors.New("execution id is required")
	}
	if lease <= 0 {
		lease = 15 * time.Second
	}
	_, err := p.WithFlowLock(ctx, flowID, false, func() error {
		tx, e := p.pool.Begin(ctx)
		if e != nil {
			return e
		}
		defer func() { _ = tx.Rollback(ctx) }()
		var state, target string
		var currentGen int64
		var incarnationID uuid.UUID
		if e = tx.QueryRow(ctx, "SELECT state,lifecycle_target,lifecycle_generation,incarnation_id FROM flows WHERE id=$1 FOR UPDATE", flowID).Scan(&state, &target, &currentGen, &incarnationID); errors.Is(e, pgx.ErrNoRows) {
			return ErrNotFound
		} else if e != nil {
			return e
		}
		if flow.State(state) != flow.StateRunning || LifecycleTarget(target) != TargetRunning || currentGen != generation {
			return fmt.Errorf("%w: execution generation %d is fenced by generation %d target %s", ErrInvalidState, generation, currentGen, target)
		}
		tag, execErr := tx.Exec(ctx, `INSERT INTO flow_executions(flow_id,execution_id,backend,status,started_at,finished_at,generation,heartbeat_at,lease_expires_at,finish_reason,incarnation_id)
			VALUES($1,$2,$3,'running',now(),NULL,$4,now(),now()+$5::interval,NULL,$6)
			ON CONFLICT(incarnation_id,execution_id) DO UPDATE SET status='running',started_at=now(),finished_at=NULL,heartbeat_at=now(),lease_expires_at=EXCLUDED.lease_expires_at,finish_reason=NULL
			WHERE flow_executions.backend IS NOT DISTINCT FROM EXCLUDED.backend AND flow_executions.generation=EXCLUDED.generation`, flowID, executionID, emptyToNull(backend), generation, lease.String(), incarnationID)
		if execErr != nil {
			return fmt.Errorf("register flow execution: %w", execErr)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("%w: execution identity is already owned by another backend or generation", ErrInvalidState)
		}
		return tx.Commit(ctx)
	})
	return err
}
func (p *PostgresEngine) RenewExecution(ctx context.Context, flowID, executionID string, generation int64, lease time.Duration) error {
	if lease <= 0 {
		lease = 15 * time.Second
	}
	tag, err := p.pool.Exec(ctx, `UPDATE flow_executions e SET heartbeat_at=now(),lease_expires_at=now()+$4::interval
		FROM flows f WHERE e.flow_id=$1 AND e.execution_id=$2 AND e.generation=$3 AND e.status='running' AND f.id=e.flow_id AND f.state='running' AND f.lifecycle_target='running' AND f.lifecycle_generation=$3`, flowID, executionID, generation, lease.String())
	if err != nil {
		return fmt.Errorf("renew flow execution: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: execution lease is fenced", ErrInvalidState)
	}
	return nil
}
func (p *PostgresEngine) FinishExecutionReason(ctx context.Context, flowID, executionID, reason string) error {
	_, err := p.pool.Exec(ctx, "UPDATE flow_executions SET status='finished',finished_at=now(),finish_reason=$3 WHERE flow_id=$1 AND execution_id=$2 AND status='running'", flowID, executionID, emptyToNull(reason))
	if err != nil {
		return fmt.Errorf("finish flow execution: %w", err)
	}
	return nil
}
func (p *PostgresEngine) ActiveExecutionsThrough(ctx context.Context, flowID string, generation int64) (int, error) {
	var n int
	err := p.pool.QueryRow(ctx, `
SELECT count(*)
FROM (
  SELECT execution.execution_id
  FROM flow_executions AS execution
  JOIN flows AS flow ON flow.incarnation_id=execution.incarnation_id
  WHERE flow.id=$1 AND execution.status='running' AND execution.generation <= $2
  UNION ALL
  SELECT acquisition.execution_id
  FROM flows AS flow
  JOIN producer_leases AS producer ON producer.incarnation_id=flow.incarnation_id
  JOIN execution_acquisitions AS acquisition ON acquisition.acquisition_id=producer.acquisition_id
  WHERE flow.id=$1
    AND producer.generation <= $2
    AND producer.lease_expires_at > clock_timestamp()
    AND acquisition.finished_at IS NULL
    AND NOT EXISTS (
      SELECT 1 FROM flow_executions AS execution
      WHERE execution.incarnation_id=flow.incarnation_id
        AND execution.execution_id=acquisition.execution_id
        AND execution.status='running'
    )
) AS active`, flowID, generation).Scan(&n)
	return n, err
}
func (p *PostgresEngine) ReconcileTerminatedExecutions(ctx context.Context, flowID string, generation int64, backend string, executionIDs []string, reason string) error {
	if len(executionIDs) == 0 {
		return nil
	}
	_, err := p.pool.Exec(ctx, `UPDATE flow_executions SET status='finished',finished_at=now(),finish_reason=$5
		WHERE flow_id=$1 AND status='running' AND generation <= $2 AND backend=$3
		AND execution_id = ANY($4) AND lease_expires_at IS NOT NULL AND lease_expires_at <= now()`, flowID, generation, backend, executionIDs, emptyToNull(reason))
	if err != nil {
		return fmt.Errorf("reconcile terminal executions: %w", err)
	}
	return nil
}
func getFlowAndControlForUpdate(ctx context.Context, tx pgx.Tx, id string) (flow.Flow, LifecycleControl, error) {
	row := tx.QueryRow(ctx, `SELECT id,name,source,destinations,state,wire_format,parallelism,config,lifecycle_target,lifecycle_generation,dispatch_pending FROM flows WHERE id=$1 FOR UPDATE`, id)
	var f flow.Flow
	var c LifecycleControl
	var source, dest, config []byte
	var state, target string
	var wire *string
	var par int
	if err := row.Scan(&f.ID, &f.Name, &source, &dest, &state, &wire, &par, &config, &target, &c.Generation, &c.DispatchPending); errors.Is(err, pgx.ErrNoRows) {
		return f, c, ErrNotFound
	} else if err != nil {
		return f, c, fmt.Errorf("scan flow control: %w", err)
	}
	if err := decodeFlow(&f, source, dest, config, state, wire, par); err != nil {
		return f, c, err
	}
	c.FlowID, c.State, c.Target = f.ID, f.State, LifecycleTarget(target)
	return f, c, nil
}
func recordStateEvent(ctx context.Context, tx pgx.Tx, id, from, to, reason string) error {
	_, err := tx.Exec(ctx, `
INSERT INTO flow_state_events(flow_id,incarnation_id,from_state,to_state,reason)
SELECT id,incarnation_id,$2,$3,$4 FROM flows WHERE id=$1`, id, emptyToNull(from), to, emptyToNull(reason))
	if err != nil {
		return fmt.Errorf("record flow state: %w", err)
	}
	return nil
}
func scanFlow(row pgx.Row) (flow.Flow, error) {
	var f flow.Flow
	var source, dest, config []byte
	var state string
	var wire *string
	var par int
	if err := row.Scan(&f.ID, &f.Name, &source, &dest, &state, &wire, &par, &config); errors.Is(err, pgx.ErrNoRows) {
		return f, ErrNotFound
	} else if err != nil {
		return f, fmt.Errorf("scan flow: %w", err)
	}
	if err := decodeFlow(&f, source, dest, config, state, wire, par); err != nil {
		return f, err
	}
	return f, nil
}
func decodeFlow(f *flow.Flow, source, dest, config []byte, state string, wire *string, par int) error {
	if err := json.Unmarshal(source, &f.Source); err != nil {
		return fmt.Errorf("unmarshal source: %w", err)
	}
	if err := json.Unmarshal(dest, &f.Destinations); err != nil {
		return fmt.Errorf("unmarshal destinations: %w", err)
	}
	if len(config) > 0 {
		if err := json.Unmarshal(config, &f.Config); err != nil {
			return fmt.Errorf("unmarshal config: %w", err)
		}
	}
	f.State = flow.State(state)
	if wire != nil {
		f.WireFormat = connector.WireFormat(*wire)
	}
	if par > 0 {
		f.Parallelism = par
	} else {
		f.Parallelism = 1
	}
	return nil
}
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
func emptyToNull(value string) interface{} {
	if value == "" {
		return nil
	}
	return value
}

var _ LifecycleStore = (*PostgresEngine)(nil)
