package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

const (
	StatusPending  = "pending"
	StatusApproved = "approved"
	StatusRejected = "rejected"
	StatusApplied  = "applied"
)

// Store persists schema and DDL events.
type Store interface {
	RegisterSchema(ctx context.Context, schema connector.Schema) error
	RecordDDL(ctx context.Context, ddl string, plan schema.Plan, lsn string, status string) (int64, error)
	SetDDLStatus(ctx context.Context, id int64, status string) error
	ListPendingDDL(ctx context.Context) ([]DDLEvent, error)
	GetDDL(ctx context.Context, id int64) (DDLEvent, error)
	GetDDLByLSN(ctx context.Context, lsn string) (DDLEvent, error)
	ListDDL(ctx context.Context, status string) ([]DDLEvent, error)
}

// PostgresStore stores registry data in Postgres.
type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
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
	return &PostgresStore{pool: pool}, nil
}

func (p *PostgresStore) Close() {
	if p.pool != nil {
		p.pool.Close()
	}
}

func (p *PostgresStore) RegisterSchema(ctx context.Context, schema connector.Schema) error {
	payload, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("marshal schema: %w", err)
	}

	_, err = p.pool.Exec(ctx,
		`INSERT INTO schema_versions (namespace, name, version, schema_json)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (namespace, name, version) DO NOTHING`,
		schema.Namespace, schema.Name, schema.Version, payload,
	)
	if err != nil {
		return fmt.Errorf("insert schema: %w", err)
	}
	return nil
}

func (p *PostgresStore) RecordDDL(ctx context.Context, ddl string, plan schema.Plan, lsn string, status string) (int64, error) {
	if status == "" {
		status = StatusPending
	}
	planJSON, err := json.Marshal(plan)
	if err != nil {
		return 0, fmt.Errorf("marshal plan: %w", err)
	}

	var id int64
	if err := p.pool.QueryRow(ctx,
		`INSERT INTO ddl_events (ddl, plan_json, lsn, status)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id`,
		ddlOrNull(ddl), planJSON, lsn, status,
	).Scan(&id); err != nil {
		return 0, fmt.Errorf("insert ddl event: %w", err)
	}
	return id, nil
}

func (p *PostgresStore) SetDDLStatus(ctx context.Context, id int64, status string) error {
	_, err := p.pool.Exec(ctx,
		"UPDATE ddl_events SET status = $2, applied_at = $3 WHERE id = $1",
		id, status, appliedAt(status),
	)
	if err != nil {
		return fmt.Errorf("update ddl status: %w", err)
	}
	return nil
}

func (p *PostgresStore) ListPendingDDL(ctx context.Context) ([]DDLEvent, error) {
	rows, err := p.pool.Query(ctx, "SELECT id, ddl, plan_json, lsn, status, created_at, applied_at FROM ddl_events WHERE status = $1 ORDER BY created_at", StatusPending)
	if err != nil {
		return nil, fmt.Errorf("list ddl events: %w", err)
	}
	defer rows.Close()

	items := make([]DDLEvent, 0)
	for rows.Next() {
		event, err := scanDDLEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan ddl event: %w", err)
		}
		items = append(items, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ddl events: %w", err)
	}

	return items, nil
}

func (p *PostgresStore) ListDDL(ctx context.Context, status string) ([]DDLEvent, error) {
	query := "SELECT id, ddl, plan_json, lsn, status, created_at, applied_at FROM ddl_events"
	var rows pgx.Rows
	var err error
	if status != "" && status != "all" {
		query += " WHERE status = $1"
		rows, err = p.pool.Query(ctx, query+" ORDER BY created_at", status)
	} else {
		rows, err = p.pool.Query(ctx, query+" ORDER BY created_at")
	}
	if err != nil {
		return nil, fmt.Errorf("list ddl events: %w", err)
	}
	defer rows.Close()

	items := make([]DDLEvent, 0)
	for rows.Next() {
		event, err := scanDDLEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan ddl event: %w", err)
		}
		items = append(items, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ddl events: %w", err)
	}
	return items, nil
}

func (p *PostgresStore) GetDDL(ctx context.Context, id int64) (DDLEvent, error) {
	row := p.pool.QueryRow(ctx, "SELECT id, ddl, plan_json, lsn, status, created_at, applied_at FROM ddl_events WHERE id = $1", id)
	return scanDDLEvent(row)
}

func (p *PostgresStore) GetDDLByLSN(ctx context.Context, lsn string) (DDLEvent, error) {
	if lsn == "" {
		return DDLEvent{}, ErrNotFound
	}
	row := p.pool.QueryRow(ctx, "SELECT id, ddl, plan_json, lsn, status, created_at, applied_at FROM ddl_events WHERE lsn = $1 ORDER BY id DESC LIMIT 1", lsn)
	return scanDDLEvent(row)
}

// DDLEvent captures a DDL change request.
type DDLEvent struct {
	ID        int64
	DDL       string
	Plan      schema.Plan
	LSN       string
	Status    string
	CreatedAt time.Time
	AppliedAt time.Time
}

// Hook wires replication schema events to the registry.
type Hook struct {
	Store        Store
	AutoApprove  bool
	GateApproval bool
	AutoApply    bool
}

func (h *Hook) OnSchema(ctx context.Context, schema connector.Schema) error {
	if h.Store == nil {
		return nil
	}
	return h.Store.RegisterSchema(ctx, schema)
}

func (h *Hook) OnSchemaChange(ctx context.Context, plan schema.Plan) error {
	if h.Store == nil {
		return nil
	}
	status := StatusPending
	if h.AutoApprove {
		status = StatusApproved
		if h.AutoApply {
			status = StatusApplied
		}
	}
	_, err := h.Store.RecordDDL(ctx, "", plan, "", status)
	if err != nil {
		return err
	}
	if h.GateApproval && status == StatusPending {
		return ErrApprovalRequired
	}
	return nil
}

func (h *Hook) OnDDL(ctx context.Context, ddl string, lsn pglogrepl.LSN) error {
	if h.Store == nil {
		return nil
	}
	lsnStr := lsn.String()
	if lsnStr != "" {
		existing, err := h.Store.GetDDLByLSN(ctx, lsnStr)
		if err == nil {
			switch existing.Status {
			case StatusApproved, StatusApplied:
				return nil
			case StatusRejected:
				if h.GateApproval {
					return ErrApprovalRequired
				}
				return nil
			default:
				if h.GateApproval {
					return ErrApprovalRequired
				}
				return nil
			}
		}
	}
	status := StatusPending
	if h.AutoApprove {
		status = StatusApproved
		if h.AutoApply {
			status = StatusApplied
		}
	}
	_, err := h.Store.RecordDDL(ctx, ddl, schema.Plan{}, lsnStr, status)
	if err != nil {
		return err
	}
	if h.GateApproval && status == StatusPending {
		return ErrApprovalRequired
	}
	return nil
}

func ddlOrNull(ddl string) interface{} {
	if ddl == "" {
		return nil
	}
	return ddl
}

func appliedAt(status string) interface{} {
	if status == StatusApplied {
		return time.Now().UTC()
	}
	return nil
}

var ErrNotFound = errors.New("registry entry not found")

// ErrApprovalRequired indicates DDL gating requires approval before continuing.
var ErrApprovalRequired = errors.New("ddl approval required")

func scanSchemaVersion(row pgx.Row) (connector.Schema, error) {
	var payload []byte
	if err := row.Scan(&payload); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return connector.Schema{}, ErrNotFound
		}
		return connector.Schema{}, err
	}
	var schema connector.Schema
	if err := json.Unmarshal(payload, &schema); err != nil {
		return connector.Schema{}, err
	}
	return schema, nil
}

func scanDDLEvent(row pgx.Row) (DDLEvent, error) {
	var event DDLEvent
	var planJSON []byte
	if err := row.Scan(&event.ID, &event.DDL, &planJSON, &event.LSN, &event.Status, &event.CreatedAt, &event.AppliedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return DDLEvent{}, ErrNotFound
		}
		return DDLEvent{}, err
	}
	if len(planJSON) > 0 {
		_ = json.Unmarshal(planJSON, &event.Plan)
	}
	return event, nil
}
