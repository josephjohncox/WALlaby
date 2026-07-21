package registry

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/controlstore"
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

// DDLExecutionStore establishes immutable execution manifests and persists
// replay-safe per-destination execution receipts.
type DDLExecutionStore interface {
	Store
	PrepareDDLExecution(ctx context.Context, flowID, lsn, destination string, expectedDestinations []string) (connector.DDLExecutionState, error)
	RecordDDLExecution(ctx context.Context, flowID, lsn, ddl, destination string, expectedDestinations []string) error
}

// PostgresStore stores registry data in Postgres.
type PostgresStore struct {
	pool     *pgxpool.Pool
	lockPool *pgxpool.Pool
	ownsPool bool
}

func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	if dsn == "" {
		return nil, errors.New("postgres DSN is required")
	}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres registry DSN: %w", err)
	}
	controlstore.ConfigurePool(poolConfig)
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	store, err := NewPostgresStoreWithPool(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, err
	}
	store.ownsPool = true
	return store, nil
}

// NewPostgresStoreWithPool borrows the worker's shared control pool.
func NewPostgresStoreWithPool(ctx context.Context, pool *pgxpool.Pool) (*PostgresStore, error) {
	if pool == nil {
		return nil, errors.New("postgres control pool is required")
	}
	if err := runMigrations(ctx, pool); err != nil {
		return nil, err
	}
	return &PostgresStore{pool: pool, lockPool: pool}, nil
}

func (p *PostgresStore) Close() {
	if p.ownsPool && p.pool != nil {
		p.pool.Close()
	}
	p.pool = nil
	p.lockPool = nil
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
	if status == StatusApplied {
		return 0, ErrExecutionReceiptRequired
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

// RecordCatalogChange serializes changes per table and atomically stores the
// next schema version with its DDL event. An already-current snapshot is a
// no-op and returns event ID zero.
func (p *PostgresStore) RecordCatalogChange(
	ctx context.Context,
	schemaSnapshot connector.Schema,
	plan schema.Plan,
	status string,
) (int64, error) {
	if status == "" {
		status = StatusPending
	}
	if status == StatusApplied {
		return 0, ErrExecutionReceiptRequired
	}
	planJSON, err := json.Marshal(plan)
	if err != nil {
		return 0, fmt.Errorf("marshal catalog plan: %w", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin catalog change: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	lockKey := schemaSnapshot.Namespace + "\x1f" + schemaSnapshot.Name
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", lockKey); err != nil {
		return 0, fmt.Errorf("lock catalog schema version: %w", err)
	}
	var latestVersion int64
	var latestSchema connector.Schema
	err = tx.QueryRow(ctx,
		`SELECT version, schema_json
		 FROM schema_versions
		 WHERE namespace = $1 AND name = $2
		 ORDER BY version DESC
		 LIMIT 1`,
		schemaSnapshot.Namespace, schemaSnapshot.Name,
	).Scan(&latestVersion, &latestSchema)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		schemaSnapshot.Version = 0
	case err != nil:
		return 0, fmt.Errorf("read latest catalog schema version: %w", err)
	case !schema.Diff(latestSchema, schemaSnapshot).HasChanges():
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit duplicate catalog change: %w", err)
		}
		return 0, nil
	default:
		schemaSnapshot.Version = latestVersion + 1
	}
	schemaJSON, err := json.Marshal(schemaSnapshot)
	if err != nil {
		return 0, fmt.Errorf("marshal catalog schema: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO schema_versions (namespace, name, version, schema_json)
		 VALUES ($1, $2, $3, $4)`,
		schemaSnapshot.Namespace, schemaSnapshot.Name, schemaSnapshot.Version, schemaJSON,
	); err != nil {
		return 0, fmt.Errorf("insert catalog schema: %w", err)
	}

	var id int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO ddl_events (flow_id, ddl, plan_json, lsn, status)
		 VALUES (NULL, NULL, $1, '', $2)
		 RETURNING id`,
		planJSON, status,
	).Scan(&id); err != nil {
		return 0, fmt.Errorf("insert catalog DDL event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit catalog change: %w", err)
	}
	return id, nil
}

func (p *PostgresStore) SetDDLStatus(ctx context.Context, id int64, status string) error {
	if status == StatusApplied {
		return ErrExecutionReceiptRequired
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin DDL status update: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	event, err := scanDDLEvent(tx.QueryRow(ctx,
		`SELECT id, flow_id, ddl, plan_json, lsn, status, created_at, applied_at
		 FROM ddl_events WHERE id = $1 FOR UPDATE`,
		id,
	))
	if err != nil {
		return err
	}
	if event.Status == StatusApplied {
		return ErrAppliedStatusImmutable
	}
	var executionStarted bool
	if err := tx.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM ddl_execution_manifests WHERE event_id = $1)",
		id,
	).Scan(&executionStarted); err != nil {
		return fmt.Errorf("check DDL execution start: %w", err)
	}
	if executionStarted {
		return ErrDDLExecutionStarted
	}
	if _, err := tx.Exec(ctx,
		"UPDATE ddl_events SET status = $2, applied_at = NULL WHERE id = $1",
		id, status,
	); err != nil {
		return fmt.Errorf("set DDL status: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit DDL status update: %w", err)
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

// WithDDLExecutionLock holds a session-scoped advisory lock across the
// non-transactional destination boundary. A process crash releases the lock,
// while the already-committed attempt tells the next owner to reconcile.
func (p *PostgresStore) WithDDLExecutionLock(
	ctx context.Context,
	flowID, destination string,
	fn func() error,
) (resultErr error) {
	if fn == nil {
		return errors.New("DDL execution callback is required")
	}
	if p.lockPool == nil {
		return errors.New("DDL execution lock pool is not initialized")
	}
	conn, err := p.lockPool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire DDL execution lock connection: %w", err)
	}
	releaseConnection := true
	defer func() {
		if releaseConnection {
			conn.Release()
		}
	}()

	lockIdentity := fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join([]string{flowID, destination}, "\x00"))))
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtextextended($1, 0))", lockIdentity); err != nil {
		return fmt.Errorf("acquire DDL execution lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var unlocked bool
		unlockErr := conn.QueryRow(unlockCtx,
			"SELECT pg_advisory_unlock(hashtextextended($1, 0))",
			lockIdentity,
		).Scan(&unlocked)
		if unlockErr == nil && unlocked {
			return
		}
		// Never return a session that may still own an advisory lock to the pool.
		releaseConnection = false
		raw := conn.Hijack()
		closeErr := raw.Close(unlockCtx)
		if resultErr == nil {
			switch {
			case unlockErr != nil:
				resultErr = fmt.Errorf("release DDL execution lock: %w", unlockErr)
			case !unlocked:
				resultErr = errors.New("release DDL execution lock: lock was not held")
			case closeErr != nil:
				resultErr = fmt.Errorf("close DDL execution lock connection: %w", closeErr)
			}
		}
	}()

	return fn()
}

// PrepareDDLExecution atomically fixes the destination manifest and records
// the first execution attempt before any downstream side effect. A repeated
// attempt is returned explicitly so the destination can reconcile state.
func (p *PostgresStore) PrepareDDLExecution(
	ctx context.Context,
	flowID, lsn, destination string,
	expectedDestinations []string,
) (connector.DDLExecutionState, error) {
	expected := normalizedDestinations(expectedDestinations)
	if strings.TrimSpace(lsn) == "" || strings.TrimSpace(destination) == "" || len(expected) == 0 {
		return connector.DDLExecutionUnknown, errors.New("DDL execution flow position, destination, and manifest are required")
	}
	if !containsDestination(expected, destination) {
		return connector.DDLExecutionUnknown, fmt.Errorf("DDL destination %q is not in the execution manifest", destination)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return connector.DDLExecutionUnknown, fmt.Errorf("begin DDL execution preparation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	query := `SELECT id, flow_id, ddl, plan_json, lsn, status, created_at, applied_at
		FROM ddl_events WHERE lsn = $1`
	args := []any{lsn}
	if flowID != "" {
		query += " AND flow_id = $2"
		args = append(args, flowID)
	}
	query += " ORDER BY id DESC LIMIT 1 FOR UPDATE"
	event, err := scanDDLEvent(tx.QueryRow(ctx, query, args...))
	if err != nil {
		return connector.DDLExecutionUnknown, err
	}
	if event.Status != StatusApproved && event.Status != StatusApplied {
		return connector.DDLExecutionUnknown, &connector.DDLGateError{
			FlowID: flowID, LSN: lsn, DDL: event.DDL,
			Status: event.Status, EventID: event.ID,
		}
	}

	manifestHash := fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(expected, "\x00"))))
	var storedDestinations []string
	var storedManifestHash string
	if err := tx.QueryRow(ctx,
		`INSERT INTO ddl_execution_manifests (event_id, destinations, manifest_hash)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (event_id) DO UPDATE
		 SET manifest_hash = ddl_execution_manifests.manifest_hash
		 RETURNING destinations, manifest_hash`,
		event.ID, expected, manifestHash,
	).Scan(&storedDestinations, &storedManifestHash); err != nil {
		return connector.DDLExecutionUnknown, fmt.Errorf("prepare DDL execution manifest: %w", err)
	}
	if storedManifestHash != manifestHash || !equalDestinations(storedDestinations, expected) {
		return connector.DDLExecutionUnknown, ErrExecutionManifestChanged
	}

	var receiptExists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM ddl_execution_receipts
		   WHERE event_id = $1 AND destination = $2
		 )`,
		event.ID, destination,
	).Scan(&receiptExists); err != nil {
		return connector.DDLExecutionUnknown, fmt.Errorf("check DDL execution receipt: %w", err)
	}
	if event.Status == StatusApplied && !receiptExists {
		return connector.DDLExecutionUnknown, ErrAppliedReceiptMissing
	}

	state := connector.DDLExecutionComplete
	if !receiptExists {
		result, err := tx.Exec(ctx,
			`INSERT INTO ddl_execution_attempts (event_id, destination, flow_id, lsn)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (event_id, destination) DO NOTHING`,
			event.ID, destination, flowIDOrNull(flowID), lsn,
		)
		if err != nil {
			return connector.DDLExecutionUnknown, fmt.Errorf("persist DDL execution attempt: %w", err)
		}
		if result.RowsAffected() == 1 {
			state = connector.DDLExecutionNew
		} else {
			state = connector.DDLExecutionRetry
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return connector.DDLExecutionUnknown, fmt.Errorf("commit DDL execution preparation: %w", err)
	}
	return state, nil
}

// RecordDDLExecution stores one destination receipt and marks the DDL event
// applied only when every destination in the immutable execution manifest has
// a receipt. Receipt insertion and the applied transition share one transaction.
func (p *PostgresStore) RecordDDLExecution(
	ctx context.Context,
	flowID, lsn, ddl, destination string,
	expectedDestinations []string,
) error {
	expected := normalizedDestinations(expectedDestinations)
	if strings.TrimSpace(destination) == "" || len(expected) == 0 {
		return errors.New("DDL execution destination manifest is required")
	}
	if !containsDestination(expected, destination) {
		return fmt.Errorf("DDL destination %q is not in the execution manifest", destination)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin DDL execution receipt: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	query := `SELECT id, flow_id, ddl, plan_json, lsn, status, created_at, applied_at
		FROM ddl_events WHERE lsn = $1`
	args := []any{lsn}
	if flowID != "" {
		query += " AND flow_id = $2"
		args = append(args, flowID)
	}
	query += " ORDER BY id DESC LIMIT 1 FOR UPDATE"
	event, err := scanDDLEvent(tx.QueryRow(ctx, query, args...))
	if err != nil {
		return err
	}
	if event.Status != StatusApproved && event.Status != StatusApplied {
		return &connector.DDLGateError{
			FlowID: flowID, LSN: lsn, DDL: event.DDL,
			Status: event.Status, EventID: event.ID,
		}
	}

	manifestHash := fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(expected, "\x00"))))
	var storedManifestHash string
	var storedDestinations []string
	if err := tx.QueryRow(ctx,
		`SELECT destinations, manifest_hash
		 FROM ddl_execution_manifests WHERE event_id = $1`,
		event.ID,
	).Scan(&storedDestinations, &storedManifestHash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrDDLExecutionNotPrepared
		}
		return fmt.Errorf("read prepared DDL execution manifest: %w", err)
	}
	if storedManifestHash != manifestHash || !equalDestinations(storedDestinations, expected) {
		return ErrExecutionManifestChanged
	}
	var attemptExists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM ddl_execution_attempts
		   WHERE event_id = $1 AND destination = $2
		 )`,
		event.ID, destination,
	).Scan(&attemptExists); err != nil {
		return fmt.Errorf("check prepared DDL execution attempt: %w", err)
	}
	if !attemptExists {
		return ErrDDLExecutionNotPrepared
	}

	ddlText := ddl
	if ddlText == "" {
		ddlText = event.DDL
	}
	receiptHash := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf(
		"%d\x00%s\x00%s\x00%s\x00%s", event.ID, flowID, lsn, destination, ddlText,
	))))
	var storedReceiptHash string
	if err := tx.QueryRow(ctx,
		`INSERT INTO ddl_execution_receipts (
		   event_id, destination, flow_id, lsn, receipt_hash
		 ) VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (event_id, destination) DO UPDATE
		 SET receipt_hash = ddl_execution_receipts.receipt_hash
		 RETURNING receipt_hash`,
		event.ID, destination, flowIDOrNull(flowID), lsn, receiptHash,
	).Scan(&storedReceiptHash); err != nil {
		return fmt.Errorf("persist DDL execution receipt: %w", err)
	}
	if storedReceiptHash != receiptHash {
		return fmt.Errorf("conflicting DDL execution receipt for destination %q", destination)
	}

	var receiptCount int
	if err := tx.QueryRow(ctx,
		`SELECT COUNT(*) FROM ddl_execution_receipts
		 WHERE event_id = $1 AND destination = ANY($2::text[])`,
		event.ID, expected,
	).Scan(&receiptCount); err != nil {
		return fmt.Errorf("count DDL execution receipts: %w", err)
	}
	if receiptCount == len(expected) {
		if _, err := tx.Exec(ctx,
			"UPDATE ddl_events SET status = $2, applied_at = now() WHERE id = $1",
			event.ID, StatusApplied,
		); err != nil {
			return fmt.Errorf("mark DDL applied from execution receipts: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit DDL execution receipt: %w", err)
	}
	return nil
}

func normalizedDestinations(destinations []string) []string {
	set := make(map[string]struct{}, len(destinations))
	for _, destination := range destinations {
		if destination = strings.TrimSpace(destination); destination != "" {
			set[destination] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for destination := range set {
		result = append(result, destination)
	}
	sort.Strings(result)
	return result
}

func containsDestination(destinations []string, target string) bool {
	index := sort.SearchStrings(destinations, target)
	return index < len(destinations) && destinations[index] == target
}

func equalDestinations(left, right []string) bool {
	left = normalizedDestinations(left)
	right = normalizedDestinations(right)
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
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

// PrepareDDLExecution fixes the manifest and checks replay state through the
// receipt-capable store before downstream execution.
func PrepareDDLExecution(
	ctx context.Context,
	store Store,
	flowID, lsn, destination string,
	expectedDestinations []string,
) (connector.DDLExecutionState, error) {
	receipts, ok := store.(DDLExecutionStore)
	if !ok {
		return connector.DDLExecutionUnknown, ErrExecutionReceiptRequired
	}
	state, err := receipts.PrepareDDLExecution(ctx, flowID, lsn, destination, expectedDestinations)
	if err != nil {
		return connector.DDLExecutionUnknown, fmt.Errorf("prepare DDL execution: %w", err)
	}
	return state, nil
}

// RecordDDLExecution persists one destination receipt and advances the registry
// only after the complete immutable destination manifest has receipts.
func RecordDDLExecution(
	ctx context.Context,
	store Store,
	flowID, lsn, ddl, destination string,
	expectedDestinations []string,
) error {
	receipts, ok := store.(DDLExecutionStore)
	if !ok {
		return ErrExecutionReceiptRequired
	}
	if err := receipts.RecordDDLExecution(ctx, flowID, lsn, ddl, destination, expectedDestinations); err != nil {
		return fmt.Errorf("record DDL execution receipt: %w", err)
	}
	return nil
}

// MarkDDLAppliedByLSN is retained as a fail-closed compatibility shim. Applied
// transitions require destination execution receipts.
func MarkDDLAppliedByLSN(context.Context, Store, string, string) error {
	return ErrExecutionReceiptRequired
}

var (
	ErrNotFound                 = errors.New("registry entry not found")
	ErrExecutionReceiptRequired = errors.New("DDL applied status requires execution receipts")
	ErrAppliedStatusImmutable   = errors.New("receipt-backed DDL applied status is immutable")
	ErrAppliedReceiptMissing    = errors.New("applied DDL event is missing a destination execution receipt")
	ErrExecutionManifestChanged = errors.New("DDL execution destination manifest changed during replay")
	ErrDDLExecutionStarted      = errors.New("DDL execution has started; administrative status is immutable")
	ErrDDLExecutionNotPrepared  = errors.New("DDL execution manifest was not prepared before destination execution")
)

// ErrApprovalRequired indicates DDL gating requires approval before continuing.
var ErrApprovalRequired = connector.ErrDDLApprovalRequired

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
