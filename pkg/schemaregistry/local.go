package schemaregistry

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	_ "modernc.org/sqlite"
)

const localRegistryDatabaseFile = "wallaby-schema-registry.sqlite"

const localRegistrySchema = `CREATE TABLE IF NOT EXISTS wallaby_schema_registry (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  subject TEXT NOT NULL,
  schema_type TEXT NOT NULL,
  schema TEXT NOT NULL,
  schema_references TEXT NOT NULL,
  version INTEGER NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (subject, schema_type, schema, schema_references),
  UNIQUE (subject, version)
);`

type localRegistry struct {
	db *sql.DB
}

func newLocalRegistry(ctx context.Context, directory string) (*localRegistry, error) {
	directory = strings.TrimSpace(directory)
	if directory == "" {
		return nil, errors.New("schema_registry_local_directory is required for local registry")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create local schema registry directory: %w", err)
	}
	info, err := os.Stat(directory)
	if err != nil {
		return nil, fmt.Errorf("stat local schema registry directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("local schema registry path %q is not a directory", directory)
	}

	databasePath, err := filepath.Abs(filepath.Join(directory, localRegistryDatabaseFile))
	if err != nil {
		return nil, fmt.Errorf("resolve local schema registry database path: %w", err)
	}
	dsnURL := &url.URL{Scheme: "file", Path: filepath.ToSlash(databasePath)}
	query := dsnURL.Query()
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "journal_mode(WAL)")
	query.Set("_txlock", "immediate")
	dsnURL.RawQuery = query.Encode()

	db, err := sql.Open("sqlite", dsnURL.String())
	if err != nil {
		return nil, fmt.Errorf("open local schema registry: %w", err)
	}
	// One connection is sufficient for a local registry and ensures every
	// operation uses the connection initialized by the DSN pragmas. SQLite
	// still serializes correctly with other registry processes using this file.
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open local schema registry: %w", err)
	}
	if _, err := db.ExecContext(ctx, localRegistrySchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize local schema registry: %w", err)
	}
	return &localRegistry{db: db}, nil
}

func (r *localRegistry) Register(ctx context.Context, req RegisterRequest) (result RegisterResult, returnErr error) {
	if err := ctx.Err(); err != nil {
		return RegisterResult{}, err
	}
	if strings.TrimSpace(req.Subject) == "" {
		return RegisterResult{}, errors.New("schema registry subject is required")
	}
	if req.Schema == "" {
		return RegisterResult{}, errors.New("schema registry schema is required")
	}
	refsJSON, err := json.Marshal(normalizeReferences(req.References))
	if err != nil {
		return RegisterResult{}, fmt.Errorf("marshal schema references: %w", err)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return RegisterResult{}, fmt.Errorf("begin local schema registration: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			returnErr = errors.Join(returnErr, fmt.Errorf("rollback local schema registration: %w", rollbackErr))
		}
	}()

	// _txlock=immediate acquires the SQLite writer lock at BeginTx. Existing
	// content lookup and version allocation are therefore one serializable unit,
	// including when independent processes use the same database file.
	var id int64
	var version int
	err = tx.QueryRowContext(ctx, `SELECT id, version
		FROM wallaby_schema_registry
		WHERE subject = ? AND schema_type = ? AND schema = ? AND schema_references = ?`,
		req.Subject, string(req.SchemaType), req.Schema, string(refsJSON),
	).Scan(&id, &version)
	switch {
	case err == nil:
		if err := tx.Commit(); err != nil {
			return RegisterResult{}, fmt.Errorf("commit existing local schema registration: %w", err)
		}
		return RegisterResult{ID: strconv.FormatInt(id, 10), Version: version}, nil
	case !errors.Is(err, sql.ErrNoRows):
		return RegisterResult{}, fmt.Errorf("lookup local schema: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) + 1
		FROM wallaby_schema_registry WHERE subject = ?`, req.Subject).Scan(&version); err != nil {
		return RegisterResult{}, fmt.Errorf("allocate local schema version: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `INSERT INTO wallaby_schema_registry
		(subject, schema_type, schema, schema_references, version)
		VALUES (?, ?, ?, ?, ?)
		RETURNING id`,
		req.Subject, string(req.SchemaType), req.Schema, string(refsJSON), version,
	).Scan(&id); err != nil {
		return RegisterResult{}, fmt.Errorf("register local schema: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return RegisterResult{}, fmt.Errorf("commit local schema registration: %w", err)
	}
	return RegisterResult{ID: strconv.FormatInt(id, 10), Version: version}, nil
}

func (r *localRegistry) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}
