package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

func newSnapshotStateStore(ctx context.Context, backend string, spec connector.RuntimeSpec, dsn string) (snapshotStateStore, error) {
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
		return newPostgresSnapshotStateStore(ctx, stateDSN, schema, table, spec.Options)
	case "file":
		path := ""
		if spec.Options != nil {
			path = spec.Options[optSnapshotStatePath]
		}
		return newFileSnapshotStateStore(path)
	case "none":
		return nil, nil //nolint:nilnil // snapshot state explicitly disabled
	default:
		return nil, fmt.Errorf("unsupported snapshot state backend %q", backend)
	}
}

type postgresSnapshotStateStore struct {
	pool   *pgxpool.Pool
	schema string
	table  string
}

func newPostgresSnapshotStateStore(ctx context.Context, dsn, schema, table string, options map[string]string) (*postgresSnapshotStateStore, error) {
	if schema == "" || table == "" {
		return nil, errors.New("snapshot state schema and table are required")
	}
	if strings.Contains(schema, ".") || strings.Contains(table, ".") {
		return nil, errors.New("snapshot state schema/table must not contain '.'")
	}
	// DDL cannot bind identifiers as parameters; they are quoted with
	// pgx.Identifier.Sanitize, which cannot represent NUL, and PostgreSQL
	// silently truncates identifiers longer than 63 bytes.
	for _, ident := range []string{schema, table} {
		if err := validateSnapshotStateIdentifier(ident); err != nil {
			return nil, err
		}
	}
	pool, err := newPool(ctx, dsn, options)
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

const maxPostgresIdentifierBytes = 63

func validateSnapshotStateIdentifier(ident string) error {
	if len(ident) > maxPostgresIdentifierBytes {
		return fmt.Errorf("snapshot state schema/table identifier exceeds %d bytes", maxPostgresIdentifierBytes)
	}
	if strings.ContainsRune(ident, 0) {
		return errors.New("snapshot state schema/table identifier must not contain NUL")
	}
	return nil
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
		item, err := scanSnapshotTaskState(rows, flowID)
		if err != nil {
			return nil, err
		}
		out[snapshotTaskKey{Table: item.Table, PartitionIndex: item.PartitionIndex, PartitionCount: item.PartitionCount}] = item
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot state: %w", err)
	}
	return out, nil
}

// snapshotStateRow is the part of pgx.Rows that scanSnapshotTaskState uses.
type snapshotStateRow interface {
	Scan(dest ...any) error
}

// scanSnapshotTaskState reads one row selected as table_name, partition_index,
// partition_count, cursor, status. The cursor column is nullable because
// Upsert stores an empty cursor as NULL, and the row is validated like any
// other persisted state before it can reach the backfill.
func scanSnapshotTaskState(row snapshotStateRow, flowID string) (snapshotTaskState, error) {
	item := snapshotTaskState{FlowID: flowID}
	var cursor *string
	if err := row.Scan(&item.Table, &item.PartitionIndex, &item.PartitionCount, &cursor, &item.Status); err != nil {
		return snapshotTaskState{}, fmt.Errorf("scan snapshot state: %w", err)
	}
	if cursor != nil {
		item.Cursor = *cursor
	}
	if err := validateSnapshotTaskState(item); err != nil {
		return snapshotTaskState{}, fmt.Errorf("snapshot state row for table %q: %w", item.Table, err)
	}
	return item, nil
}

func (s *postgresSnapshotStateStore) Upsert(ctx context.Context, state snapshotTaskState) error {
	state, err := normalizeSnapshotTaskState(state)
	if err != nil {
		return err
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

// maxSnapshotStateFileBytes bounds the JSON snapshot state file that Load reads
// and Upsert writes. A file the store could not read back is never written.
const maxSnapshotStateFileBytes = 16 << 20

// fileSnapshotStateStore keeps task state in one JSON file.
//
// Trust boundary: snapshot_state_path is an operator-selected file capability.
// The directory part of the path is resolved exactly once, when the store is
// opened, and is then pinned as an *os.Root. The process's authority over that
// directory is unchanged by this type; whoever chooses the path chooses which
// directory the process may write. What the pinned root does confine is the
// file name: every later read, temp file, rename and fsync is relative to the
// pinned directory, so a symlink planted at the file name or a swap of the
// directory path after Open cannot redirect state outside that directory.
type fileSnapshotStateStore struct {
	name   string
	mu     sync.Mutex
	root   *os.Root
	states map[string]snapshotTaskState
	// syncDirectory makes the rename durable. It is a field only so tests can
	// inject the post-rename failure that cannot be provoked portably.
	syncDirectory func(root *os.Root) error
}

func syncRootDirectory(root *os.Root) error {
	dir, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("open snapshot state directory for sync: %w", err)
	}
	defer func() { _ = dir.Close() }()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync snapshot state directory: %w", err)
	}
	return nil
}

// errSnapshotStateCommitUnsynced reports that the new state file is visible
// (renamed into place) but its directory entry may not be durable. The store
// keeps the new state in memory to match the visible file; retrying the same
// Upsert is idempotent.
var errSnapshotStateCommitUnsynced = errors.New("snapshot state committed but directory sync failed")

func newFileSnapshotStateStore(path string) (*fileSnapshotStateStore, error) {
	if path == "" {
		return nil, errors.New("snapshot state path is required")
	}
	path = filepath.Clean(path)
	dir, name := filepath.Split(path)
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, filepath.Separator) {
		return nil, fmt.Errorf("snapshot state path %q must name a file", path)
	}
	if dir == "" {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create snapshot state directory: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open snapshot state directory: %w", err)
	}

	store := &fileSnapshotStateStore{
		name:          name,
		root:          root,
		states:        map[string]snapshotTaskState{},
		syncDirectory: syncRootDirectory,
	}
	if err := store.load(); err != nil {
		_ = root.Close()
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
	state, err := normalizeSnapshotTaskState(state)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.root == nil {
		return errors.New("snapshot state store is closed")
	}

	key := snapshotStateKey(state)
	previous, existed := s.states[key]
	s.states[key] = state
	committed, err := s.persist()
	if err != nil && !committed {
		if existed {
			s.states[key] = previous
		} else {
			delete(s.states, key)
		}
	}
	// After a commit the visible file already holds the new state, so memory
	// keeps it even when the trailing directory sync is reported as an error.
	return err
}

func (s *fileSnapshotStateStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.root != nil {
		_ = s.root.Close()
		s.root = nil
	}
}

func (s *fileSnapshotStateStore) load() error {
	info, err := s.root.Lstat(s.name)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat snapshot state: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("snapshot state %q must be a regular file, not %s", s.name, info.Mode().Type())
	}
	if info.Size() > maxSnapshotStateFileBytes {
		return fmt.Errorf("snapshot state is %d bytes, exceeding the %d byte limit", info.Size(), int64(maxSnapshotStateFileBytes))
	}
	file, err := s.root.Open(s.name)
	if err != nil {
		return fmt.Errorf("read snapshot state: %w", err)
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat snapshot state: %w", err)
	}
	if !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return fmt.Errorf("snapshot state %q changed while it was being opened", s.name)
	}
	// The size was checked above, but the file can grow before it is read.
	data, err := io.ReadAll(io.LimitReader(file, maxSnapshotStateFileBytes+1))
	if err != nil {
		return fmt.Errorf("read snapshot state: %w", err)
	}
	if len(data) > maxSnapshotStateFileBytes {
		return fmt.Errorf("snapshot state exceeds the %d byte limit", int64(maxSnapshotStateFileBytes))
	}
	if len(data) == 0 {
		return nil
	}
	var states map[string]snapshotTaskState
	if err := json.Unmarshal(data, &states); err != nil {
		return fmt.Errorf("decode snapshot state: %w", err)
	}
	for key, state := range states {
		if err := validateSnapshotTaskState(state); err != nil {
			return fmt.Errorf("snapshot state entry %q: %w", key, err)
		}
		if key != snapshotStateKey(state) {
			return fmt.Errorf("snapshot state entry %q does not match its task identity", key)
		}
	}
	if states != nil {
		s.states = states
	}
	return nil
}

// persist replaces the state file atomically: the payload is written and
// synced to an exclusively created temporary file inside the pinned directory,
// renamed over the state file, and the directory is synced. committed reports
// whether the rename happened. A failure before it leaves the previous file
// untouched; a directory-sync failure after it returns committed=true with
// errSnapshotStateCommitUnsynced because the new file is already visible.
func (s *fileSnapshotStateStore) persist() (committed bool, err error) {
	payload, err := json.MarshalIndent(s.states, "", "  ")
	if err != nil {
		return false, fmt.Errorf("encode snapshot state: %w", err)
	}
	if len(payload) > maxSnapshotStateFileBytes {
		return false, fmt.Errorf("snapshot state would be %d bytes, exceeding the %d byte limit", len(payload), int64(maxSnapshotStateFileBytes))
	}
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		return false, fmt.Errorf("name snapshot state temp file: %w", err)
	}
	tempName := "." + s.name + "." + hex.EncodeToString(suffix) + ".tmp"
	temp, err := s.root.OpenFile(tempName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return false, fmt.Errorf("create snapshot state temp file: %w", err)
	}
	renamed := false
	defer func() {
		if !renamed {
			_ = temp.Close()
			_ = s.root.Remove(tempName)
		}
	}()
	if _, err := temp.Write(payload); err != nil {
		return false, fmt.Errorf("write snapshot state: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return false, fmt.Errorf("sync snapshot state: %w", err)
	}
	if err := temp.Close(); err != nil {
		return false, fmt.Errorf("close snapshot state temp file: %w", err)
	}
	if err := s.root.Rename(tempName, s.name); err != nil {
		return false, fmt.Errorf("replace snapshot state: %w", err)
	}
	renamed = true
	if err := s.syncDirectory(s.root); err != nil {
		return true, fmt.Errorf("%w: %w", errSnapshotStateCommitUnsynced, err)
	}
	return true, nil
}

// normalizeSnapshotTaskState applies defaults and rejects identities that no
// backfill task can produce.
func normalizeSnapshotTaskState(state snapshotTaskState) (snapshotTaskState, error) {
	if state.FlowID == "" {
		return state, errors.New("flow id is required")
	}
	if state.Table == "" {
		return state, errors.New("table is required")
	}
	if state.PartitionCount <= 0 {
		state.PartitionCount = 1
	}
	if state.Status == "" {
		state.Status = snapshotStatusRunning
	}
	if err := validateSnapshotTaskState(state); err != nil {
		return state, err
	}
	return state, nil
}

func validateSnapshotTaskState(state snapshotTaskState) error {
	if state.FlowID == "" || state.Table == "" {
		return errors.New("flow id and table are required")
	}
	if state.PartitionCount < 1 || state.PartitionCount > maxBackfillPartitionCount {
		return fmt.Errorf("partition count %d must be between 1 and %d", state.PartitionCount, maxBackfillPartitionCount)
	}
	if state.PartitionIndex < 0 || state.PartitionIndex >= state.PartitionCount {
		return fmt.Errorf("partition index %d is outside partition count %d", state.PartitionIndex, state.PartitionCount)
	}
	if state.Status != snapshotStatusRunning && state.Status != snapshotStatusDone {
		return fmt.Errorf("unsupported snapshot status %q", state.Status)
	}
	return nil
}

func snapshotStateKey(state snapshotTaskState) string {
	return fmt.Sprintf("%s|%s|%d|%d", state.FlowID, state.Table, state.PartitionIndex, state.PartitionCount)
}
