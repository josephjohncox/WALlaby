package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/flowctx"
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
	RecordDDL(ctx context.Context, flowID string, ddl string, plan schema.Plan, lsn string, status string) (int64, error)
	SetDDLStatus(ctx context.Context, id int64, status string) error
	ListPendingDDL(ctx context.Context, flowID string) ([]DDLEvent, error)
	GetDDL(ctx context.Context, id int64) (DDLEvent, error)
	GetDDLByLSN(ctx context.Context, flowID string, lsn string) (DDLEvent, error)
	ListDDL(ctx context.Context, flowID string, status string) ([]DDLEvent, error)
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

func (p *PostgresStore) RecordDDL(ctx context.Context, flowID string, ddl string, plan schema.Plan, lsn string, status string) (int64, error) {
	if status == "" {
		status = StatusPending
	}
	planJSON, err := json.Marshal(plan)
	if err != nil {
		return 0, fmt.Errorf("marshal plan: %w", err)
	}

	var id int64
	if err := p.pool.QueryRow(ctx,
		`INSERT INTO ddl_events (flow_id, ddl, plan_json, lsn, status)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id`,
		flowIDOrNull(flowID), ddlOrNull(ddl), planJSON, lsn, status,
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

func (p *PostgresStore) ListPendingDDL(ctx context.Context, flowID string) ([]DDLEvent, error) {
	query := "SELECT id, flow_id, ddl, plan_json, lsn, status, created_at, applied_at FROM ddl_events WHERE status = $1"
	args := []any{StatusPending}
	if flowID != "" {
		query += " AND flow_id = $2"
		args = append(args, flowID)
	}
	query += " ORDER BY created_at"
	rows, err := p.pool.Query(ctx, query, args...)
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

func (p *PostgresStore) ListDDL(ctx context.Context, flowID string, status string) ([]DDLEvent, error) {
	query := "SELECT id, flow_id, ddl, plan_json, lsn, status, created_at, applied_at FROM ddl_events"
	args := []any{}
	clauses := []string{}
	if status != "" && status != "all" {
		clauses = append(clauses, "status = $1")
		args = append(args, status)
	}
	if flowID != "" {
		clauses = append(clauses, fmt.Sprintf("flow_id = $%d", len(args)+1))
		args = append(args, flowID)
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY created_at"
	rows, err := p.pool.Query(ctx, query, args...)
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
	row := p.pool.QueryRow(ctx, "SELECT id, flow_id, ddl, plan_json, lsn, status, created_at, applied_at FROM ddl_events WHERE id = $1", id)
	return scanDDLEvent(row)
}

func (p *PostgresStore) GetDDLByLSN(ctx context.Context, flowID string, lsn string) (DDLEvent, error) {
	if strings.TrimSpace(lsn) == "" {
		return DDLEvent{}, ErrNotFound
	}
	query := "SELECT id, flow_id, ddl, plan_json, lsn, status, created_at, applied_at FROM ddl_events WHERE lsn = $1"
	args := []any{lsn}
	if flowID != "" {
		query += " AND flow_id = $2"
		args = append(args, flowID)
	}
	query += " ORDER BY id DESC LIMIT 1"
	row := p.pool.QueryRow(ctx, query, args...)
	return scanDDLEvent(row)
}

// DDLEvent captures a DDL change request.
type DDLEvent struct {
	ID        int64
	FlowID    string
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
	FlowID       string
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
	flowID := h.flowID(ctx)
	status := StatusPending
	if h.AutoApprove {
		status = StatusApproved
	}
	if h.AutoApply {
		status = StatusApproved
	}
	id, err := h.Store.RecordDDL(ctx, flowID, "", plan, "", status)
	if err != nil {
		return err
	}
	if h.GateApproval && status == StatusPending {
		var planJSON string
		if payload, err := json.Marshal(plan); err == nil {
			planJSON = string(payload)
		}
		return &connector.DDLGateError{
			FlowID:   flowID,
			Status:   status,
			EventID:  id,
			PlanJSON: planJSON,
		}
	}
	return nil
}

func (h *Hook) OnDDL(ctx context.Context, ddl string, lsn pglogrepl.LSN) error {
	if h.Store == nil {
		return nil
	}
	flowID := h.flowID(ctx)
	lsnStr := lsn.String()
	if lsnStr != "" {
		existing, err := h.Store.GetDDLByLSN(ctx, flowID, lsnStr)
		if err == nil {
			switch existing.Status {
			case StatusApproved, StatusApplied:
				return nil
			case StatusRejected:
				if h.GateApproval {
					return &connector.DDLGateError{
						FlowID:  flowID,
						LSN:     lsnStr,
						DDL:     ddlOrFallback(ddl, existing.DDL),
						Status:  existing.Status,
						EventID: existing.ID,
					}
				}
				return nil
			default:
				if h.GateApproval {
					return &connector.DDLGateError{
						FlowID:  flowID,
						LSN:     lsnStr,
						DDL:     ddlOrFallback(ddl, existing.DDL),
						Status:  existing.Status,
						EventID: existing.ID,
					}
				}
				return nil
			}
		}
	}
	status := StatusPending
	if h.AutoApprove {
		status = StatusApproved
	}
	if h.AutoApply {
		status = StatusApproved
	}
	id, err := h.Store.RecordDDL(ctx, flowID, ddl, schema.Plan{}, lsnStr, status)
	if err != nil {
		return err
	}
	if h.GateApproval && status == StatusPending {
		return &connector.DDLGateError{
			FlowID:  flowID,
			LSN:     lsnStr,
			DDL:     ddl,
			Status:  status,
			EventID: id,
		}
	}
	return nil
}

func (h *Hook) flowID(ctx context.Context) string {
	if h.FlowID != "" {
		return h.FlowID
	}
	if ctx == nil {
		return ""
	}
	if id, ok := flowctx.FlowIDFromContext(ctx); ok {
		return id
	}
	return ""
}

func ddlOrNull(ddl string) interface{} {
	if ddl == "" {
		return nil
	}
	return ddl
}

func ddlOrFallback(ddl string, fallback string) string {
	if ddl != "" {
		return ddl
	}
	return fallback
}

func flowIDOrNull(flowID string) interface{} {
	if strings.TrimSpace(flowID) == "" {
		return nil
	}
	return flowID
}

// MarkDDLAppliedByLSN updates a DDL event to applied for the given LSN.
func MarkDDLAppliedByLSN(ctx context.Context, store Store, flowID string, lsn string) error {
	if store == nil || strings.TrimSpace(lsn) == "" {
		return nil
	}
	event, err := store.GetDDLByLSN(ctx, flowID, lsn)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	}
	if event.Status == StatusApplied || event.Status == StatusRejected {
		return nil
	}
	if event.Status != StatusApproved {
		return &connector.DDLGateError{
			FlowID:  flowID,
			LSN:     lsn,
			DDL:     event.DDL,
			Status:  event.Status,
			EventID: event.ID,
		}
	}
	return store.SetDDLStatus(ctx, event.ID, StatusApplied)
}

func appliedAt(status string) interface{} {
	if status == StatusApplied {
		return time.Now().UTC()
	}
	return nil
}

var ErrNotFound = errors.New("registry entry not found")

// ErrApprovalRequired indicates DDL gating requires approval before continuing.
var ErrApprovalRequired = connector.ErrDDLApprovalRequired

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
	var flowID *string
	var ddl *string
	var lsn *string
	var appliedAt *time.Time
	if err := row.Scan(&event.ID, &flowID, &ddl, &planJSON, &lsn, &event.Status, &event.CreatedAt, &appliedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return DDLEvent{}, ErrNotFound
		}
		return DDLEvent{}, err
	}
	if flowID != nil {
		event.FlowID = *flowID
	}
	if ddl != nil {
		event.DDL = *ddl
	}
	if lsn != nil {
		event.LSN = *lsn
	}
	if appliedAt != nil {
		event.AppliedAt = *appliedAt
	}
	if len(planJSON) > 0 {
		_ = json.Unmarshal(planJSON, &event.Plan)
	}
	return event, nil
}
