package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

const (
	snapshotStatusRunning = "running"
	snapshotStatusDone    = "done"
)

type snapshotTaskKey struct {
	Table          string
	PartitionIndex int
	PartitionCount int
}

type snapshotTaskState struct {
	FlowID         string
	Table          string
	PartitionIndex int
	PartitionCount int
	Cursor         string
	Status         string
}

type snapshotStateStore interface {
	Load(ctx context.Context, flowID string) (map[snapshotTaskKey]snapshotTaskState, error)
	Upsert(ctx context.Context, state snapshotTaskState) error
	Close()
}

func newSnapshotStateStore(ctx context.Context, backend string, spec connector.Spec, dsn string) (snapshotStateStore, error) {
	switch strings.ToLower(backend) {
	case "", "postgres":
		stateDSN := dsn
		if spec.Options != nil {
			if value := spec.Options[optSnapshotStateDSN]; value != "" {
				stateDSN = value
			}
		}
		if stateDSN == "" {
			return nil, errors.New("snapshot state dsn is required")
		}
		schema := "wallaby"
		table := "snapshot_state"
		if spec.Options != nil {
			if value := spec.Options[optSnapshotStateSchema]; value != "" {
				schema = value
			}
			if value := spec.Options[optSnapshotStateTable]; value != "" {
				table = value
			}
		}
		return newPostgresSnapshotStateStore(ctx, stateDSN, schema, table)
	case "file":
		path := ""
		if spec.Options != nil {
			path = spec.Options[optSnapshotStatePath]
		}
		return newFileSnapshotStateStore(path)
	case "none":
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported snapshot state backend %q", backend)
	}
}

type postgresSnapshotStateStore struct {
	pool   *pgxpool.Pool
	schema string
	table  string
}

func newPostgresSnapshotStateStore(ctx context.Context, dsn, schema, table string) (*postgresSnapshotStateStore, error) {
	if schema == "" || table == "" {
		return nil, errors.New("snapshot state schema and table are required")
	}
	if strings.Contains(schema, ".") || strings.Contains(table, ".") {
		return nil, errors.New("snapshot state schema/table must not contain '.'")
	}
	pool, err := newPool(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	store := &postgresSnapshotStateStore{
		pool:   pool,
		schema: schema,
		table:  table,
	}
	if err := store.ensure(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return store, nil
}

func (s *postgresSnapshotStateStore) ensure(ctx context.Context) error {
	schemaIdent := pgx.Identifier{s.schema}.Sanitize()
	if _, err := s.pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaIdent)); err != nil {
		return fmt.Errorf("create snapshot state schema: %w", err)
	}
	tableIdent := pgx.Identifier{s.schema, s.table}.Sanitize()
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  flow_id TEXT NOT NULL,
  table_name TEXT NOT NULL,
  partition_index INTEGER NOT NULL DEFAULT 0,
  partition_count INTEGER NOT NULL DEFAULT 1,
  cursor TEXT,
  status TEXT NOT NULL DEFAULT '%s',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (flow_id, table_name, partition_index)
)`, tableIdent, snapshotStatusRunning)
	if _, err := s.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("create snapshot state table: %w", err)
	}
	return nil
}

func (s *postgresSnapshotStateStore) Load(ctx context.Context, flowID string) (map[snapshotTaskKey]snapshotTaskState, error) {
	if flowID == "" {
		return nil, errors.New("flow id is required")
	}

	tableIdent := pgx.Identifier{s.schema, s.table}.Sanitize()
	query := fmt.Sprintf(`SELECT table_name, partition_index, partition_count, cursor, status
FROM %s WHERE flow_id = $1`, tableIdent)
	rows, err := s.pool.Query(ctx, query, flowID)
	if err != nil {
		return nil, fmt.Errorf("load snapshot state: %w", err)
	}
	defer rows.Close()

	out := make(map[snapshotTaskKey]snapshotTaskState)
	for rows.Next() {
		var item snapshotTaskState
		item.FlowID = flowID
		if err := rows.Scan(&item.Table, &item.PartitionIndex, &item.PartitionCount, &item.Cursor, &item.Status); err != nil {
			return nil, fmt.Errorf("scan snapshot state: %w", err)
		}
		out[snapshotTaskKey{Table: item.Table, PartitionIndex: item.PartitionIndex, PartitionCount: item.PartitionCount}] = item
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot state: %w", err)
	}
	return out, nil
}

func (s *postgresSnapshotStateStore) Upsert(ctx context.Context, state snapshotTaskState) error {
	if state.FlowID == "" {
		return errors.New("flow id is required")
	}
	if state.Table == "" {
		return errors.New("table is required")
	}
	if state.PartitionCount <= 0 {
		state.PartitionCount = 1
	}
	if state.Status == "" {
		state.Status = snapshotStatusRunning
	}

	tableIdent := pgx.Identifier{s.schema, s.table}.Sanitize()
	query := fmt.Sprintf(`INSERT INTO %s (
  flow_id, table_name, partition_index, partition_count, cursor, status, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, now())
ON CONFLICT (flow_id, table_name, partition_index)
DO UPDATE SET
  partition_count = EXCLUDED.partition_count,
  cursor = EXCLUDED.cursor,
  status = EXCLUDED.status,
  updated_at = now()`, tableIdent)

	if _, err := s.pool.Exec(ctx, query,
		state.FlowID,
		state.Table,
		state.PartitionIndex,
		state.PartitionCount,
		emptyToNull(state.Cursor),
		state.Status,
	); err != nil {
		return fmt.Errorf("upsert snapshot state: %w", err)
	}
	return nil
}

func (s *postgresSnapshotStateStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

type fileSnapshotStateStore struct {
	path   string
	mu     sync.Mutex
	states map[string]snapshotTaskState
}

func newFileSnapshotStateStore(path string) (*fileSnapshotStateStore, error) {
	if path == "" {
		return nil, errors.New("snapshot state path is required")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create snapshot state directory: %w", err)
	}

	store := &fileSnapshotStateStore{
		path:   path,
		states: map[string]snapshotTaskState{},
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *fileSnapshotStateStore) Load(_ context.Context, flowID string) (map[snapshotTaskKey]snapshotTaskState, error) {
	if flowID == "" {
		return nil, errors.New("flow id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[snapshotTaskKey]snapshotTaskState)
	for _, state := range s.states {
		if state.FlowID != flowID {
			continue
		}
		key := snapshotTaskKey{Table: state.Table, PartitionIndex: state.PartitionIndex, PartitionCount: state.PartitionCount}
		out[key] = state
	}
	return out, nil
}

func (s *fileSnapshotStateStore) Upsert(_ context.Context, state snapshotTaskState) error {
	if state.FlowID == "" {
		return errors.New("flow id is required")
	}
	if state.Table == "" {
		return errors.New("table is required")
	}
	if state.PartitionCount <= 0 {
		state.PartitionCount = 1
	}
	if state.Status == "" {
		state.Status = snapshotStatusRunning
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := snapshotStateKey(state)
	s.states[key] = state
	return s.persist()
}

func (s *fileSnapshotStateStore) Close() {}

func (s *fileSnapshotStateStore) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read snapshot state: %w", err)
	}
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, &s.states); err != nil {
		return fmt.Errorf("decode snapshot state: %w", err)
	}
	return nil
}

func (s *fileSnapshotStateStore) persist() error {
	payload, err := json.MarshalIndent(s.states, "", "  ")
	if err != nil {
		return fmt.Errorf("encode snapshot state: %w", err)
	}
	if err := os.WriteFile(s.path, payload, 0o644); err != nil {
		return fmt.Errorf("write snapshot state: %w", err)
	}
	return nil
}

func snapshotStateKey(state snapshotTaskState) string {
	return fmt.Sprintf("%s|%s|%d|%d", state.FlowID, state.Table, state.PartitionIndex, state.PartitionCount)
}
