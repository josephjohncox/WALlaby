package checkpoint

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestSQLiteStoreUpgradesEmptyLegacyOutboxSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy-empty.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE checkpoint_outbox (
		flow_id TEXT NOT NULL, destination_id TEXT NOT NULL, position_id TEXT NOT NULL,
		batch_hash TEXT NOT NULL, codec TEXT NOT NULL, batch_json BLOB NOT NULL, created_at TEXT NOT NULL,
		PRIMARY KEY (flow_id,destination_id,position_id))`)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	store, err := NewSQLiteStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	_ = store.Close()
}

func TestSQLiteStoreRejectsLegacyOutboxRowsWithoutProjectionIdentity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE checkpoint_outbox (
		flow_id TEXT NOT NULL, destination_id TEXT NOT NULL, position_id TEXT NOT NULL,
		batch_hash TEXT NOT NULL, codec TEXT NOT NULL, batch_json BLOB NOT NULL, created_at TEXT NOT NULL,
		PRIMARY KEY (flow_id,destination_id,position_id));
		INSERT INTO checkpoint_outbox VALUES ('flow','dest','pos','hash','gob-v1',X'00','now')`)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if _, err := NewSQLiteStore(ctx, path); err == nil || !strings.Contains(err.Error(), "legacy rows") {
		t.Fatalf("NewSQLiteStore() error=%v, want explicit legacy outbox rejection", err)
	}
}

func TestSQLiteStoreRejectsLegacyRowsWithoutReplayOrder(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy-order.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE checkpoint_outbox (
 flow_id TEXT NOT NULL,destination_id TEXT NOT NULL,position_id TEXT NOT NULL,batch_hash TEXT NOT NULL,
 projection_fingerprint TEXT NOT NULL,codec TEXT NOT NULL,batch_json BLOB NOT NULL,created_at TEXT NOT NULL,
 PRIMARY KEY(flow_id,destination_id,position_id));
 INSERT INTO checkpoint_outbox VALUES('flow','dest','pos','hash','projection','gob-v1',X'00','now')`)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if _, err := NewSQLiteStore(ctx, path); err == nil || !strings.Contains(err.Error(), "replay order") {
		t.Fatalf("legacy replay-order error=%v", err)
	}
}

func TestSQLiteOutboxReplayUsesPersistedMonotonicOrderNotTimestamp(t *testing.T) {
	ctx := context.Background()
	store, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "ordered.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	checkpoint := connector.Checkpoint{LSN: "0/20"}
	created := time.Unix(1, 123456789).UTC()
	batch := connector.Batch{Checkpoint: checkpoint, Schema: connector.Schema{Name: "events"}, Records: []connector.Record{{Table: "events", Operation: connector.OpInsert, SourcePosition: "0/20"}}}
	entries := []connector.OutboxEntry{
		{Destination: "dest", PositionID: "0/20/fragment/000000", ProjectionFingerprint: "p", Batch: batch, CreatedAt: created},
		{Destination: "dest", PositionID: "0/20", ProjectionFingerprint: "p", Batch: batch, CreatedAt: created},
	}
	if err := store.PersistCheckpointAndOutbox(ctx, "flow", checkpoint, entries); err != nil {
		t.Fatal(err)
	}
	got, err := store.ListOutbox(ctx, "flow")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].PositionID != entries[0].PositionID || got[1].PositionID != entries[1].PositionID || got[0].ReplayOrder <= 0 || got[1].ReplayOrder <= got[0].ReplayOrder {
		t.Fatalf("ordered outbox=%+v", got)
	}
}

func TestSQLiteStoreRejectsCheckpointRegression(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "checkpoints.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore() error = %v", err)
	}
	defer func() { _ = store.Close() }()

	if _, err := store.Get(ctx, "missing"); !errors.Is(err, connector.ErrCheckpointNotFound) {
		t.Fatalf("Get(missing) error = %v, want ErrCheckpointNotFound", err)
	}

	for _, cp := range []connector.Checkpoint{{LSN: "9"}, {LSN: "10"}, {LSN: "10"}} {
		if err := store.Put(ctx, "ordinal", cp); err != nil {
			t.Fatalf("Put(%s) error = %v", cp.LSN, err)
		}
	}
	if err := store.Put(ctx, "ordinal", connector.Checkpoint{LSN: "8"}); !errors.Is(err, connector.ErrCheckpointRegression) {
		t.Fatalf("Put(regression) error = %v, want ErrCheckpointRegression", err)
	}
	got, err := store.Get(ctx, "ordinal")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.LSN != "10" {
		t.Fatalf("Get().LSN = %q, want 10", got.LSN)
	}
}

func TestSQLiteStoreValidatesFirstCheckpointPosition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "checkpoints.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore() error = %v", err)
	}
	defer func() { _ = store.Close() }()

	invalid := []connector.Checkpoint{
		{LSN: "not-an-lsn"},
		{},
		{Metadata: map[string]string{"control": "true"}},
		{Metadata: map[string]string{"mode": connector.SourceModeBackfill}},
	}
	for index, checkpoint := range invalid {
		flowID := fmt.Sprintf("invalid-%d", index)
		if err := store.Put(ctx, flowID, checkpoint); !errors.Is(err, connector.ErrCheckpointPosition) {
			t.Fatalf("Put(%+v) error = %v, want ErrCheckpointPosition", checkpoint, err)
		}
		if _, err := store.Get(ctx, flowID); !errors.Is(err, connector.ErrCheckpointNotFound) {
			t.Fatalf("Get(%q) error = %v, want ErrCheckpointNotFound", flowID, err)
		}
	}

	backfill := connector.Checkpoint{Metadata: map[string]string{
		"mode":  connector.SourceModeBackfill,
		"table": "public.accounts",
	}}
	if err := store.Put(ctx, "backfill", backfill); err != nil {
		t.Fatalf("Put(backfill) error = %v", err)
	}
	backfill.Metadata["done"] = "true"
	if err := store.Put(ctx, "backfill", backfill); err != nil {
		t.Fatalf("Put(backfill update) error = %v", err)
	}

	control := connector.Checkpoint{Metadata: map[string]string{"control": "true", "position": "schema-ready"}}
	if err := store.Put(ctx, "control", control); err != nil {
		t.Fatalf("Put(control) error = %v", err)
	}
}

func TestSQLiteStoreUsesPostgresLSNSemantics(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "checkpoints.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore() error = %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Put(ctx, "postgres", connector.Checkpoint{LSN: "0/FFFFFFFF"}); err != nil {
		t.Fatalf("Put(initial) error = %v", err)
	}
	if err := store.Put(ctx, "postgres", connector.Checkpoint{LSN: "1/0"}); err != nil {
		t.Fatalf("Put(advance) error = %v", err)
	}
	if err := store.Put(ctx, "postgres", connector.Checkpoint{LSN: "4294967297"}); !errors.Is(err, connector.ErrCheckpointPosition) {
		t.Fatalf("Put(mixed position) error = %v, want ErrCheckpointPosition", err)
	}
}

func TestSQLiteStoreCanonicalizesPositions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "checkpoints.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Put(ctx, "canonical", connector.Checkpoint{LSN: "000a/000ff"}); err != nil {
		t.Fatalf("Put(canonical LSN): %v", err)
	}
	got, err := store.Get(ctx, "canonical")
	if err != nil {
		t.Fatal(err)
	}
	if got.LSN != "A/FF" {
		t.Fatalf("stored LSN = %q, want A/FF", got.LSN)
	}
	for _, invalid := range []string{" 9 ", "+9", "-9"} {
		if err := store.Put(ctx, "invalid-"+invalid, connector.Checkpoint{LSN: invalid}); !errors.Is(err, connector.ErrCheckpointPosition) {
			t.Fatalf("Put(%q) error = %v, want ErrCheckpointPosition", invalid, err)
		}
	}
}

func TestSQLiteStoreAtomicCheckpointOutbox(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "checkpoints.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	checkpoint := connector.Checkpoint{LSN: "0009", Metadata: map[string]string{"seq": "9"}}
	position, err := connector.CheckpointPositionID(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	batch := connector.Batch{
		Checkpoint: checkpoint,
		Schema:     connector.Schema{Name: "events", Namespace: "public"},
		Records: []connector.Record{{
			Table: "events", Operation: connector.OpInsert, Payload: []byte(`{"id":9}`),
			After: map[string]any{"id": int64(9007199254740993)},
		}},
	}
	entries := []connector.OutboxEntry{
		{Destination: "secondary-a", PositionID: position, ProjectionFingerprint: "projection-a", Batch: batch},
		{Destination: "secondary-b", PositionID: position, ProjectionFingerprint: "projection-b", Batch: batch},
	}
	if err := store.PersistCheckpointAndOutbox(ctx, "flow", checkpoint, entries); err != nil {
		t.Fatalf("PersistCheckpointAndOutbox(): %v", err)
	}
	replayedCheckpoint := checkpoint
	replayedCheckpoint.Timestamp = time.Unix(99, 0).UTC()
	replayedEntries := append([]connector.OutboxEntry(nil), entries...)
	for index := range replayedEntries {
		replayedEntries[index].Batch.Checkpoint = replayedCheckpoint
	}
	if err := store.PersistCheckpointAndOutbox(ctx, "flow", replayedCheckpoint, replayedEntries); err != nil {
		t.Fatalf("idempotent replay with a new checkpoint timestamp: %v", err)
	}
	stored, err := store.Get(ctx, "flow")
	if err != nil {
		t.Fatal(err)
	}
	if stored.LSN != "9" {
		t.Fatalf("stored checkpoint = %q, want 9", stored.LSN)
	}
	pending, err := store.ListOutbox(ctx, "flow")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending entries = %d, want 2", len(pending))
	}
	if got, ok := pending[0].Batch.Records[0].After["id"].(int64); !ok || got != 9007199254740993 {
		t.Fatalf("restored int64 = %T(%v), want exact int64", pending[0].Batch.Records[0].After["id"], pending[0].Batch.Records[0].After["id"])
	}
	if err := store.DeleteOutbox(ctx, "flow", "secondary-a", position); err != nil {
		t.Fatal(err)
	}
	pending, err = store.ListOutbox(ctx, "flow")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Destination != "secondary-b" {
		t.Fatalf("pending after one completion = %+v", pending)
	}

	conflicting := batch
	conflicting.Records = []connector.Record{{Table: "events", Operation: connector.OpInsert, Payload: []byte(`{"id":10}`)}}
	err = store.PersistCheckpointAndOutbox(ctx, "flow", checkpoint, []connector.OutboxEntry{{
		Destination: "secondary-b", PositionID: position, ProjectionFingerprint: "projection-b", Batch: conflicting,
	}})
	if !errors.Is(err, connector.ErrOutboxConflict) {
		t.Fatalf("conflicting replay error = %v, want ErrOutboxConflict", err)
	}
}

func TestSQLiteStoreRejectsUnknownOutboxCodec(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "checkpoints.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	checkpoint := connector.Checkpoint{LSN: "9"}
	position, err := connector.CheckpointPositionID(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PersistCheckpointAndOutbox(ctx, "flow", checkpoint, []connector.OutboxEntry{{
		Destination: "secondary", PositionID: position, ProjectionFingerprint: "projection", Batch: connector.Batch{Checkpoint: checkpoint},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, "UPDATE checkpoint_outbox SET codec='future-v2' WHERE flow_id='flow'"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ListOutbox(ctx, "flow"); err == nil || !strings.Contains(err.Error(), "unsupported checkpoint outbox codec") {
		t.Fatalf("ListOutbox() error = %v, want unsupported codec", err)
	}
}

func TestSQLiteStoreBackfillOutboxIdentity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "checkpoints.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	checkpoint := connector.Checkpoint{Metadata: map[string]string{
		"mode": connector.SourceModeBackfill, "table": "public.accounts", "partition": "0/2", "cursor": "100", "done": "true",
	}}
	position, err := connector.CheckpointPositionID(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	batch := connector.Batch{Checkpoint: checkpoint, Records: []connector.Record{{Table: "accounts", Operation: connector.OpLoad}}}
	if err := store.PersistCheckpointAndOutbox(ctx, "backfill", checkpoint, []connector.OutboxEntry{{
		Destination: "secondary", PositionID: position, ProjectionFingerprint: "projection", Batch: batch,
	}}); err != nil {
		t.Fatal(err)
	}
	pending, err := store.ListOutbox(ctx, "backfill")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].PositionID != position {
		t.Fatalf("restored backfill outbox = %+v, want position %s", pending, position)
	}
}
