package checkpoint

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
	_ "modernc.org/sqlite"
)

const (
	sqliteInitTable = `CREATE TABLE IF NOT EXISTS checkpoints (
  flow_id TEXT PRIMARY KEY,
  lsn TEXT NOT NULL,
  metadata TEXT NOT NULL,
  updated_at TEXT NOT NULL
);`
	sqliteInitIndex = `CREATE INDEX IF NOT EXISTS checkpoints_updated_at_idx ON checkpoints (updated_at);`
)

// SQLiteStore persists checkpoints in a single-file SQLite database.
type SQLiteStore struct {
	db *sql.DB
}

func NewSQLiteStore(ctx context.Context, dsn string) (*SQLiteStore, error) {
	if dsn == "" {
		return nil, errors.New("sqlite dsn is required")
	}
	if err := ensureSQLitePath(dsn); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set wal mode: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqliteInitTable); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create checkpoints table: %w", err)
	}
	if _, err := db.ExecContext(ctx, sqliteInitIndex); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create checkpoints index: %w", err)
	}

	return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *SQLiteStore) Get(ctx context.Context, flowID string) (connector.Checkpoint, error) {
	row := s.db.QueryRowContext(ctx, "SELECT lsn, metadata, updated_at FROM checkpoints WHERE flow_id = ?", flowID)
	var lsn string
	var metadataJSON string
	var updatedAt string
	if err := row.Scan(&lsn, &metadataJSON, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return connector.Checkpoint{}, ErrNotFound
		}
		return connector.Checkpoint{}, fmt.Errorf("get checkpoint: %w", err)
	}

	metadata := map[string]string{}
	if metadataJSON != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			return connector.Checkpoint{}, fmt.Errorf("decode metadata: %w", err)
		}
	}

	checkpoint := connector.Checkpoint{LSN: lsn, Metadata: metadata}
	if updatedAt != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, updatedAt); err == nil {
			checkpoint.Timestamp = parsed
		}
	}

	return checkpoint, nil
}

func (s *SQLiteStore) Put(ctx context.Context, flowID string, checkpoint connector.Checkpoint) error {
	if checkpoint.Timestamp.IsZero() {
		checkpoint.Timestamp = time.Now().UTC()
	}
	if checkpoint.Metadata == nil {
		checkpoint.Metadata = map[string]string{}
	}
	metadataJSON, err := json.Marshal(checkpoint.Metadata)
	if err != nil {
		return fmt.Errorf("encode metadata: %w", err)
	}
	updatedAt := checkpoint.Timestamp.Format(time.RFC3339Nano)

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO checkpoints (flow_id, lsn, metadata, updated_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(flow_id) DO UPDATE SET
		 lsn = excluded.lsn,
		 metadata = excluded.metadata,
		 updated_at = excluded.updated_at`,
		flowID, checkpoint.LSN, string(metadataJSON), updatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert checkpoint: %w", err)
	}
	return nil
}

func (s *SQLiteStore) List(ctx context.Context) ([]connector.FlowCheckpoint, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT flow_id, lsn, metadata, updated_at FROM checkpoints ORDER BY updated_at DESC")
	if err != nil {
		return nil, fmt.Errorf("list checkpoints: %w", err)
	}
	defer rows.Close()

	out := []connector.FlowCheckpoint{}
	for rows.Next() {
		var flowID string
		var lsn string
		var metadataJSON string
		var updatedAt string
		if err := rows.Scan(&flowID, &lsn, &metadataJSON, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan checkpoint: %w", err)
		}
		metadata := map[string]string{}
		if metadataJSON != "" {
			if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
				return nil, fmt.Errorf("decode metadata: %w", err)
			}
		}
		cp := connector.Checkpoint{LSN: lsn, Metadata: metadata}
		if updatedAt != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, updatedAt); err == nil {
				cp.Timestamp = parsed
			}
		}
		out = append(out, connector.FlowCheckpoint{FlowID: flowID, Checkpoint: cp})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate checkpoints: %w", err)
	}

	return out, nil
}

func ensureSQLitePath(dsn string) error {
	path := strings.TrimSpace(dsn)
	if path == "" || path == ":memory:" {
		return nil
	}
	if strings.HasPrefix(path, "file:") {
		path = strings.TrimPrefix(path, "file:")
		path = strings.TrimPrefix(path, "//")
	}
	if idx := strings.IndexAny(path, "?;"); idx >= 0 {
		path = path[:idx]
	}
	if path == "" || path == ":memory:" {
		return nil
	}
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create sqlite dir: %w", err)
	}
	return nil
}
