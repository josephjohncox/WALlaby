package checkpoint

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
	sqliteInitIndex  = `CREATE INDEX IF NOT EXISTS checkpoints_updated_at_idx ON checkpoints (updated_at);`
	sqliteInitOutbox = `CREATE TABLE IF NOT EXISTS checkpoint_outbox (
  replay_order INTEGER PRIMARY KEY AUTOINCREMENT,
  flow_id TEXT NOT NULL,
  destination_id TEXT NOT NULL,
  position_id TEXT NOT NULL,
  batch_hash TEXT NOT NULL,
  projection_fingerprint TEXT NOT NULL,
  codec TEXT NOT NULL,
  batch_json BLOB NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE (flow_id, destination_id, position_id)
);`
	sqliteInitOutboxIndex = `CREATE INDEX IF NOT EXISTS checkpoint_outbox_flow_replay_idx
  ON checkpoint_outbox (flow_id, replay_order);`
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
	// A checkpoint store is deliberately serialized. This also keeps :memory:
	// databases bound to the connection on which their schema was created.
	db.SetMaxOpenConns(1)
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
	if _, err := db.ExecContext(ctx, sqliteInitOutbox); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create checkpoint outbox table: %w", err)
	}
	if err := ensureSQLiteOutboxProjectionSchema(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, sqliteInitOutboxIndex); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create checkpoint outbox index: %w", err)
	}

	return &SQLiteStore{db: db}, nil
}

func ensureSQLiteOutboxProjectionSchema(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info(checkpoint_outbox)")
	if err != nil {
		return fmt.Errorf("inspect checkpoint outbox schema: %w", err)
	}
	foundProjection := false
	foundReplayOrder := false
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notnull, &defaultValue, &pk); err != nil {
			_ = rows.Close()
			return err
		}
		if name == "projection_fingerprint" {
			foundProjection = true
		}
		if name == "replay_order" {
			foundReplayOrder = true
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if foundProjection && foundReplayOrder {
		return nil
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM checkpoint_outbox").Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return errors.New("checkpoint_outbox contains legacy rows without authoritative projection fingerprints and replay order; reconcile or remove them before upgrade")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "DROP TABLE checkpoint_outbox"); err != nil {
		return fmt.Errorf("drop empty legacy outbox: %w", err)
	}
	if _, err := tx.ExecContext(ctx, sqliteInitOutbox); err != nil {
		return fmt.Errorf("recreate ordered checkpoint outbox: %w", err)
	}
	return tx.Commit()
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
	checkpoint, err := canonicalizeCheckpoint(checkpoint)
	if err != nil {
		return connector.Checkpoint{}, fmt.Errorf("canonicalize stored checkpoint: %w", err)
	}
	if updatedAt != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, updatedAt); err == nil {
			checkpoint.Timestamp = parsed
		}
	}

	return checkpoint, nil
}

func (s *SQLiteStore) Put(ctx context.Context, flowID string, checkpoint connector.Checkpoint) error {
	checkpoint, err := prepareCheckpointWrite(checkpoint)
	if err != nil {
		return err
	}
	return s.withImmediateTransaction(ctx, "checkpoint update", func(conn *sql.Conn) error {
		return upsertSQLiteCheckpoint(ctx, conn, flowID, checkpoint)
	})
}

func prepareCheckpointWrite(checkpoint connector.Checkpoint) (connector.Checkpoint, error) {
	checkpoint, err := canonicalizeCheckpoint(checkpoint)
	if err != nil {
		return connector.Checkpoint{}, err
	}
	if checkpoint.Timestamp.IsZero() {
		checkpoint.Timestamp = time.Now().UTC()
	}
	if checkpoint.Metadata == nil {
		checkpoint.Metadata = map[string]string{}
	}
	return checkpoint, nil
}

func (s *SQLiteStore) withImmediateTransaction(ctx context.Context, operation string, fn func(*sql.Conn) error) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire sqlite %s connection: %w", operation, err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("begin %s transaction: %w", operation, err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		}
	}()
	if err := fn(conn); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit %s transaction: %w", operation, err)
	}
	committed = true
	return nil
}

func upsertSQLiteCheckpoint(ctx context.Context, conn *sql.Conn, flowID string, checkpoint connector.Checkpoint) error {
	var currentLSN string
	err := conn.QueryRowContext(ctx, "SELECT lsn FROM checkpoints WHERE flow_id = ?", flowID).Scan(&currentLSN)
	switch {
	case err == nil:
		if err := validateCheckpointAdvance(flowID, currentLSN, checkpoint.LSN); err != nil {
			return err
		}
	case errors.Is(err, sql.ErrNoRows):
		// First checkpoint for this flow.
	default:
		return fmt.Errorf("read current checkpoint: %w", err)
	}
	metadataJSON, err := json.Marshal(checkpoint.Metadata)
	if err != nil {
		return fmt.Errorf("encode metadata: %w", err)
	}
	if _, err := conn.ExecContext(ctx,
		`INSERT INTO checkpoints (flow_id, lsn, metadata, updated_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(flow_id) DO UPDATE SET
		 lsn = excluded.lsn,
		 metadata = excluded.metadata,
		 updated_at = excluded.updated_at`,
		flowID, checkpoint.LSN, string(metadataJSON), checkpoint.Timestamp.Format(time.RFC3339Nano),
	); err != nil {
		return fmt.Errorf("upsert checkpoint: %w", err)
	}
	return nil
}

// PersistCheckpointAndOutbox atomically advances the checkpoint and inserts
// one durable row for each secondary destination.
func (s *SQLiteStore) PersistCheckpointAndOutbox(ctx context.Context, flowID string, checkpoint connector.Checkpoint, entries []connector.OutboxEntry) error {
	checkpoint, err := prepareCheckpointWrite(checkpoint)
	if err != nil {
		return err
	}
	encoded, err := encodeOutboxEntries(flowID, checkpoint, entries)
	if err != nil {
		return err
	}
	return s.withImmediateTransaction(ctx, "checkpoint outbox", func(conn *sql.Conn) error {
		if err := upsertSQLiteCheckpoint(ctx, conn, flowID, checkpoint); err != nil {
			return err
		}
		for _, item := range encoded {
			var positionHash string
			err := conn.QueryRowContext(ctx,
				"SELECT batch_hash FROM checkpoint_outbox WHERE flow_id=? AND position_id=? LIMIT 1",
				flowID, item.entry.PositionID).Scan(&positionHash)
			switch {
			case err == nil && positionHash != item.batchHash:
				return fmt.Errorf("%w: flow=%s position=%s identifies different batches", connector.ErrOutboxConflict, flowID, item.entry.PositionID)
			case err == nil, errors.Is(err, sql.ErrNoRows):
			default:
				return fmt.Errorf("read outbox batch identity: %w", err)
			}
			result, err := conn.ExecContext(ctx,
				`INSERT INTO checkpoint_outbox (flow_id, destination_id, position_id, batch_hash, projection_fingerprint, codec, batch_json, created_at)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(flow_id, destination_id, position_id) DO NOTHING`,
				flowID, item.entry.Destination, item.entry.PositionID, item.batchHash, item.entry.ProjectionFingerprint, outboxCodecGobV1, item.batchData, item.entry.CreatedAt.Format(time.RFC3339Nano))
			if err != nil {
				return fmt.Errorf("insert outbox entry for %s: %w", item.entry.Destination, err)
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("inspect outbox insert for %s: %w", item.entry.Destination, err)
			}
			if rows == 0 {
				var existingHash, existingProjection string
				if err := conn.QueryRowContext(ctx,
					"SELECT batch_hash, projection_fingerprint FROM checkpoint_outbox WHERE flow_id=? AND destination_id=? AND position_id=?",
					flowID, item.entry.Destination, item.entry.PositionID).Scan(&existingHash, &existingProjection); err != nil {
					return fmt.Errorf("read existing outbox entry for %s: %w", item.entry.Destination, err)
				}
				if existingHash != item.batchHash || existingProjection != item.entry.ProjectionFingerprint {
					return fmt.Errorf("%w: flow=%s destination=%s position=%s", connector.ErrOutboxConflict, flowID, item.entry.Destination, item.entry.PositionID)
				}
			}
		}
		return nil
	})
}

func (s *SQLiteStore) ListOutbox(ctx context.Context, flowID string) ([]connector.OutboxEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT destination_id, position_id, batch_hash, projection_fingerprint, replay_order, codec, batch_json, created_at FROM checkpoint_outbox
		 WHERE flow_id=? ORDER BY replay_order`, flowID)
	if err != nil {
		return nil, fmt.Errorf("list checkpoint outbox: %w", err)
	}
	defer func() { _ = rows.Close() }()
	entries := make([]connector.OutboxEntry, 0)
	for rows.Next() {
		var destination, position, batchHash, projectionFingerprint, codec, createdAt string
		var replayOrder int64
		var batchJSON []byte
		if err := rows.Scan(&destination, &position, &batchHash, &projectionFingerprint, &replayOrder, &codec, &batchJSON, &createdAt); err != nil {
			return nil, fmt.Errorf("scan checkpoint outbox: %w", err)
		}
		batch, err := decodeOutboxBatch(codec, batchJSON)
		if err != nil {
			return nil, err
		}
		entry := connector.OutboxEntry{FlowID: flowID, Destination: destination, PositionID: position, BatchHash: batchHash, ProjectionFingerprint: projectionFingerprint, ReplayOrder: replayOrder, Batch: batch}
		entry.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate checkpoint outbox: %w", err)
	}
	return entries, nil
}

func (s *SQLiteStore) DeleteOutbox(ctx context.Context, flowID, destination, positionID string) error {
	result, err := s.db.ExecContext(ctx,
		"DELETE FROM checkpoint_outbox WHERE flow_id=? AND destination_id=? AND position_id=?",
		flowID, destination, positionID)
	if err != nil {
		return fmt.Errorf("delete checkpoint outbox entry: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect checkpoint outbox delete: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("delete checkpoint outbox entry flow=%s destination=%s position=%s: entry not found", flowID, destination, positionID)
	}
	return nil
}

func (s *SQLiteStore) List(ctx context.Context) ([]connector.FlowCheckpoint, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT flow_id, lsn, metadata, updated_at FROM checkpoints ORDER BY updated_at DESC")
	if err != nil {
		return nil, fmt.Errorf("list checkpoints: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("close rows: %v", err)
		}
	}()

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
		cp, err = canonicalizeCheckpoint(cp)
		if err != nil {
			return nil, fmt.Errorf("canonicalize stored checkpoint for %s: %w", flowID, err)
		}
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
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create sqlite dir: %w", err)
	}
	return nil
}
