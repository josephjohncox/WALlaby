package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/ductstream/internal/flow"
	"github.com/josephjohncox/ductstream/pkg/connector"
)

const (
	stateCreateReason = "create"
	stateStartReason  = "start"
	stateStopReason   = "stop"
	stateResumeReason = "resume"
)

// PostgresEngine stores flow metadata in Postgres for durability.
type PostgresEngine struct {
	pool *pgxpool.Pool
}

func NewPostgresEngine(ctx context.Context, dsn string) (*PostgresEngine, error) {
	if dsn == "" {
		return nil, errors.New("postgres DSN is required")
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	if err := runMigrations(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}

	return &PostgresEngine{pool: pool}, nil
}

func (p *PostgresEngine) Close() {
	if p.pool != nil {
		p.pool.Close()
	}
}

func (p *PostgresEngine) Create(ctx context.Context, f flow.Flow) (flow.Flow, error) {
	if f.ID == "" {
		return flow.Flow{}, errors.New("flow id is required")
	}
	if f.State == "" {
		f.State = flow.StateCreated
	}
	if f.Parallelism <= 0 {
		f.Parallelism = 1
	}

	sourceJSON, err := json.Marshal(f.Source)
	if err != nil {
		return flow.Flow{}, fmt.Errorf("marshal source: %w", err)
	}
	destJSON, err := json.Marshal(f.Destinations)
	if err != nil {
		return flow.Flow{}, fmt.Errorf("marshal destinations: %w", err)
	}

	_, err = p.pool.Exec(ctx,
		`INSERT INTO flows (id, name, source, destinations, state, wire_format, parallelism)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		f.ID, f.Name, sourceJSON, destJSON, string(f.State), emptyToNull(string(f.WireFormat)), f.Parallelism,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return flow.Flow{}, ErrAlreadyExists
		}
		return flow.Flow{}, fmt.Errorf("insert flow: %w", err)
	}

	if err := p.recordState(ctx, f.ID, "", string(f.State), stateCreateReason); err != nil {
		return flow.Flow{}, err
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

	sourceJSON, err := json.Marshal(f.Source)
	if err != nil {
		return flow.Flow{}, fmt.Errorf("marshal source: %w", err)
	}
	destJSON, err := json.Marshal(f.Destinations)
	if err != nil {
		return flow.Flow{}, fmt.Errorf("marshal destinations: %w", err)
	}

	row := p.pool.QueryRow(ctx,
		`UPDATE flows
		 SET name = $2, source = $3, destinations = $4, wire_format = $5, parallelism = $6, updated_at = now()
		 WHERE id = $1
		 RETURNING id, name, source, destinations, state, wire_format, parallelism`,
		f.ID, f.Name, sourceJSON, destJSON, emptyToNull(string(f.WireFormat)), f.Parallelism,
	)
	updated, err := scanFlow(row)
	if err != nil {
		return flow.Flow{}, err
	}
	return updated, nil
}

func (p *PostgresEngine) Start(ctx context.Context, flowID string) (flow.Flow, error) {
	return p.transition(ctx, flowID, flow.StateRunning, stateStartReason)
}

func (p *PostgresEngine) Stop(ctx context.Context, flowID string) (flow.Flow, error) {
	return p.transition(ctx, flowID, flow.StatePaused, stateStopReason)
}

func (p *PostgresEngine) Resume(ctx context.Context, flowID string) (flow.Flow, error) {
	return p.transition(ctx, flowID, flow.StateRunning, stateResumeReason)
}

func (p *PostgresEngine) Delete(ctx context.Context, flowID string) error {
	cmd, err := p.pool.Exec(ctx, "DELETE FROM flows WHERE id = $1", flowID)
	if err != nil {
		return fmt.Errorf("delete flow: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (p *PostgresEngine) Get(ctx context.Context, flowID string) (flow.Flow, error) {
	row := p.pool.QueryRow(ctx,
		"SELECT id, name, source, destinations, state, wire_format, parallelism FROM flows WHERE id = $1",
		flowID,
	)
	return scanFlow(row)
}

func (p *PostgresEngine) List(ctx context.Context) ([]flow.Flow, error) {
	rows, err := p.pool.Query(ctx, "SELECT id, name, source, destinations, state, wire_format, parallelism FROM flows ORDER BY created_at")
	if err != nil {
		return nil, fmt.Errorf("list flows: %w", err)
	}
	defer rows.Close()

	flows := make([]flow.Flow, 0)
	for rows.Next() {
		f, err := scanFlow(rows)
		if err != nil {
			return nil, err
		}
		flows = append(flows, f)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate flows: %w", err)
	}

	return flows, nil
}

func (p *PostgresEngine) transition(ctx context.Context, flowID string, target flow.State, reason string) (flow.Flow, error) {
	prev, err := p.getState(ctx, flowID)
	if err != nil {
		return flow.Flow{}, err
	}

	row := p.pool.QueryRow(ctx,
		"UPDATE flows SET state = $2, updated_at = now() WHERE id = $1 RETURNING id, name, source, destinations, state, wire_format, parallelism",
		flowID, string(target),
	)
	updated, err := scanFlow(row)
	if err != nil {
		return flow.Flow{}, err
	}

	if err := p.recordState(ctx, flowID, prev, string(target), reason); err != nil {
		return flow.Flow{}, err
	}

	return updated, nil
}

func (p *PostgresEngine) getState(ctx context.Context, flowID string) (string, error) {
	var state string
	if err := p.pool.QueryRow(ctx, "SELECT state FROM flows WHERE id = $1", flowID).Scan(&state); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("get flow state: %w", err)
	}
	return state, nil
}

func (p *PostgresEngine) recordState(ctx context.Context, flowID, fromState, toState, reason string) error {
	_, err := p.pool.Exec(ctx,
		"INSERT INTO flow_state_events (flow_id, from_state, to_state, reason) VALUES ($1, $2, $3, $4)",
		flowID, emptyToNull(fromState), toState, emptyToNull(reason),
	)
	if err != nil {
		return fmt.Errorf("record flow state: %w", err)
	}
	return nil
}

func scanFlow(row pgx.Row) (flow.Flow, error) {
	var f flow.Flow
	var sourceJSON []byte
	var destJSON []byte
	var state string
	var wireFormat *string
	var parallelism int

	if err := row.Scan(&f.ID, &f.Name, &sourceJSON, &destJSON, &state, &wireFormat, &parallelism); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return flow.Flow{}, ErrNotFound
		}
		return flow.Flow{}, fmt.Errorf("scan flow: %w", err)
	}

	if err := json.Unmarshal(sourceJSON, &f.Source); err != nil {
		return flow.Flow{}, fmt.Errorf("unmarshal source: %w", err)
	}
	if err := json.Unmarshal(destJSON, &f.Destinations); err != nil {
		return flow.Flow{}, fmt.Errorf("unmarshal destinations: %w", err)
	}

	f.State = flow.State(state)
	if wireFormat != nil {
		f.WireFormat = connector.WireFormat(*wireFormat)
	}
	if parallelism > 0 {
		f.Parallelism = parallelism
	} else {
		f.Parallelism = 1
	}
	return f, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}

func emptyToNull(value string) interface{} {
	if value == "" {
		return nil
	}
	return value
}
