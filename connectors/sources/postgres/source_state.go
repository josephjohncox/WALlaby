package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type sourceStateStore struct {
	pool   *pgxpool.Pool
	schema string
	table  string
}

type sourceState struct {
	ID          string
	SourceName  string
	Slot        string
	Publication string
	State       string
	Options     map[string]string
	LastLSN     string
}

func newSourceStateStore(ctx context.Context, dsn, schema, table string) (*sourceStateStore, error) {
	if schema == "" || table == "" {
		return nil, errors.New("state schema and table are required")
	}
	if strings.Contains(schema, ".") || strings.Contains(table, ".") {
		return nil, errors.New("state schema/table must not contain '.'")
	}
	pool, err := newPool(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	store := &sourceStateStore{
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

func (s *sourceStateStore) ensure(ctx context.Context) error {
	schemaIdent := pgx.Identifier{s.schema}.Sanitize()
	if _, err := s.pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaIdent)); err != nil {
		return fmt.Errorf("create source state schema: %w", err)
	}
	tableIdent := pgx.Identifier{s.schema, s.table}.Sanitize()
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  id TEXT PRIMARY KEY,
  source_name TEXT,
  slot_name TEXT NOT NULL,
  publication_name TEXT NOT NULL,
  state TEXT NOT NULL,
  options JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_lsn TEXT,
  last_ack_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`, tableIdent)
	if _, err := s.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("create source state table: %w", err)
	}
	return nil
}

func (s *sourceStateStore) Upsert(ctx context.Context, entry sourceState) error {
	if entry.ID == "" {
		return errors.New("source state id is required")
	}
	options := entry.Options
	if options == nil {
		options = map[string]string{}
	}
	payload, err := json.Marshal(options)
	if err != nil {
		return fmt.Errorf("marshal source state options: %w", err)
	}

	tableIdent := pgx.Identifier{s.schema, s.table}.Sanitize()
	query := fmt.Sprintf(`INSERT INTO %s (
  id, source_name, slot_name, publication_name, state, options, last_lsn, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())
ON CONFLICT (id) DO UPDATE SET
  source_name = EXCLUDED.source_name,
  slot_name = EXCLUDED.slot_name,
  publication_name = EXCLUDED.publication_name,
  state = EXCLUDED.state,
  options = EXCLUDED.options,
  last_lsn = EXCLUDED.last_lsn,
  updated_at = now()`, tableIdent)

	if _, err := s.pool.Exec(ctx, query,
		entry.ID,
		entry.SourceName,
		entry.Slot,
		entry.Publication,
		entry.State,
		payload,
		emptyToNull(entry.LastLSN),
	); err != nil {
		return fmt.Errorf("upsert source state: %w", err)
	}
	return nil
}

func (s *sourceStateStore) RecordAck(ctx context.Context, id, lsn string) error {
	if id == "" || lsn == "" {
		return nil
	}
	tableIdent := pgx.Identifier{s.schema, s.table}.Sanitize()
	query := fmt.Sprintf("UPDATE %s SET last_lsn = $2, last_ack_at = now(), updated_at = now() WHERE id = $1", tableIdent)
	if _, err := s.pool.Exec(ctx, query, id, lsn); err != nil {
		return fmt.Errorf("update source state lsn: %w", err)
	}
	return nil
}

func (s *sourceStateStore) UpdateState(ctx context.Context, id, state string) error {
	if id == "" {
		return nil
	}
	tableIdent := pgx.Identifier{s.schema, s.table}.Sanitize()
	query := fmt.Sprintf("UPDATE %s SET state = $2, updated_at = now() WHERE id = $1", tableIdent)
	if _, err := s.pool.Exec(ctx, query, id, state); err != nil {
		return fmt.Errorf("update source state: %w", err)
	}
	return nil
}

func (s *sourceStateStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func sourceStateID(spec connector.Spec, slot string) string {
	if spec.Options != nil {
		if value := spec.Options[optFlowID]; value != "" {
			return value
		}
	}
	if spec.Name != "" {
		return spec.Name
	}
	if slot != "" {
		return slot
	}
	return "source"
}

func sanitizeOptions(options map[string]string) map[string]string {
	if len(options) == 0 {
		return map[string]string{}
	}
	sanitized := make(map[string]string, len(options))
	for key, value := range options {
		if isSensitiveKey(key) {
			continue
		}
		sanitized[key] = value
	}
	return sanitized
}

func isSensitiveKey(key string) bool {
	lower := strings.ToLower(key)
	if lower == "dsn" {
		return true
	}
	if strings.Contains(lower, "password") || strings.Contains(lower, "secret") || strings.Contains(lower, "token") {
		return true
	}
	if strings.Contains(lower, "key") {
		return true
	}
	return false
}

func emptyToNull(value string) any {
	if value == "" {
		return nil
	}
	return value
}
