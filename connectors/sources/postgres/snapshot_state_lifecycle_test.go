package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// After the rename the new file is visible, so a failing directory sync must be
// reported without rolling memory back to a state the file no longer holds.
func TestFileSnapshotStateStoreKeepsCommittedStateWhenDirectorySyncFails(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.json")
	store, err := newFileSnapshotStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Upsert(ctx, testSnapshotState("1")); err != nil {
		t.Fatal(err)
	}

	syncErr := errors.New("injected directory sync failure")
	store.syncDirectory = func(*os.Root) error { return syncErr }
	err = store.Upsert(ctx, testSnapshotState("2"))
	if !errors.Is(err, errSnapshotStateCommitUnsynced) || !errors.Is(err, syncErr) {
		t.Fatalf("err = %v, want committed-but-unsynced wrapping the sync failure", err)
	}

	// Visible file, memory, and a fresh reader must all agree on the new state.
	loaded, err := store.Load(ctx, "flow-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, got := range loaded {
		if got.Cursor != "2" {
			t.Fatalf("memory cursor = %q, want committed %q", got.Cursor, "2")
		}
	}
	reopened, err := newFileSnapshotStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	fromDisk, err := reopened.Load(ctx, "flow-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(fromDisk) != 1 {
		t.Fatalf("disk entries = %v", fromDisk)
	}
	for _, got := range fromDisk {
		if got.Cursor != "2" {
			t.Fatalf("disk cursor = %q, want %q", got.Cursor, "2")
		}
	}

	// Retrying the same upsert once the fault clears is idempotent.
	store.syncDirectory = syncRootDirectory
	if err := store.Upsert(ctx, testSnapshotState("2")); err != nil {
		t.Fatalf("retry: %v", err)
	}
}

func TestFileSnapshotStateStoreSyncsDirectoryOnlyAfterRename(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := newFileSnapshotStateStore(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var calls int
	store.syncDirectory = func(root *os.Root) error {
		calls++
		if _, err := root.Stat("state.json"); err != nil {
			return fmt.Errorf("state file not yet renamed into place: %w", err)
		}
		return syncRootDirectory(root)
	}
	if err := store.Upsert(ctx, testSnapshotState("1")); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("directory syncs = %d, want 1", calls)
	}
	requireNoStateTempFiles(t, dir)
}

// pgxStyleRow mimics pgx v5 Scan: NULL into a plain *string is an error and
// NULL into **string yields nil.
type pgxStyleRow struct {
	table     string
	index     int
	count     int
	cursor    *string
	status    string
	scanError error
}

func (r pgxStyleRow) Scan(dest ...any) error {
	if r.scanError != nil {
		return r.scanError
	}
	if len(dest) != 5 {
		return fmt.Errorf("scan destinations = %d, want 5", len(dest))
	}
	*(dest[0].(*string)) = r.table
	*(dest[1].(*int)) = r.index
	*(dest[2].(*int)) = r.count
	switch cursor := dest[3].(type) {
	case **string:
		*cursor = r.cursor
	case *string:
		if r.cursor == nil {
			return errors.New("cannot scan NULL into *string")
		}
		*cursor = *r.cursor
	default:
		return fmt.Errorf("unexpected cursor destination %T", dest[3])
	}
	*(dest[4].(*string)) = r.status
	return nil
}

func TestScanSnapshotTaskStateHandlesNullCursorWrittenByUpsert(t *testing.T) {
	t.Parallel()
	// emptyToNull is what postgresSnapshotStateStore.Upsert binds for an empty cursor.
	if emptyToNull("") != nil {
		t.Fatal("Upsert no longer stores an empty cursor as NULL")
	}
	got, err := scanSnapshotTaskState(pgxStyleRow{table: "public.widgets", count: 2, index: 1, status: snapshotStatusDone}, "flow-1")
	if err != nil {
		t.Fatalf("NULL cursor row rejected: %v", err)
	}
	want := snapshotTaskState{FlowID: "flow-1", Table: "public.widgets", PartitionIndex: 1, PartitionCount: 2, Status: snapshotStatusDone}
	if got != want {
		t.Fatalf("got %#v, want %#v", got, want)
	}
	cursor := "42"
	got, err = scanSnapshotTaskState(pgxStyleRow{table: "public.widgets", count: 1, cursor: &cursor, status: snapshotStatusRunning}, "flow-1")
	if err != nil || got.Cursor != "42" {
		t.Fatalf("got %#v, %v", got, err)
	}
}

func TestScanSnapshotTaskStateRejectsHostileRows(t *testing.T) {
	t.Parallel()
	valid := pgxStyleRow{table: "public.widgets", count: 1, status: snapshotStatusRunning}
	tests := []struct {
		name   string
		mutate func(*pgxStyleRow)
	}{
		{"negative count", func(r *pgxStyleRow) { r.count = -1 }},
		{"zero count", func(r *pgxStyleRow) { r.count = 0 }},
		{"count above limit", func(r *pgxStyleRow) { r.count = maxBackfillPartitionCount + 1 }},
		{"negative index", func(r *pgxStyleRow) { r.index = -1 }},
		{"index beyond count", func(r *pgxStyleRow) { r.index = 1 }},
		{"unknown status", func(r *pgxStyleRow) { r.status = "bogus" }},
		{"empty table", func(r *pgxStyleRow) { r.table = "" }},
		{"scan failure", func(r *pgxStyleRow) { r.scanError = errors.New("boom") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			row := valid
			tt.mutate(&row)
			if _, err := scanSnapshotTaskState(row, "flow-1"); err == nil {
				t.Fatalf("hostile row accepted: %#v", row)
			}
		})
	}
}

// recordingStateStore wraps a real file store to observe Close.
type recordingStateStore struct {
	*fileSnapshotStateStore
	closes  atomic.Int32
	loadErr error
}

func (r *recordingStateStore) Load(ctx context.Context, flowID string) (map[snapshotTaskKey]snapshotTaskState, error) {
	if r.loadErr != nil {
		return nil, r.loadErr
	}
	return r.fileSnapshotStateStore.Load(ctx, flowID)
}

func (r *recordingStateStore) Close() {
	r.closes.Add(1)
	r.fileSnapshotStateStore.Close()
}

func openFailureSource(t *testing.T, loadErr error) (*BackfillSource, *recordingStateStore, connector.RuntimeSpec) {
	t.Helper()
	dsn := "postgres://127.0.0.1:1/unreachable?connect_timeout=1"
	recorder := &recordingStateStore{loadErr: loadErr}
	source := &BackfillSource{
		// A lazily connecting pool stands in for a reachable server so Open
		// proceeds to the state store and then fails afterwards.
		connect: func(ctx context.Context, dsn string, options map[string]string) (*pgxpool.Pool, error) {
			return newPool(ctx, dsn, options)
		},
		openStateStore: func(_ context.Context, _ string, spec connector.RuntimeSpec, _ string) (snapshotStateStore, error) {
			store, err := newFileSnapshotStateStore(spec.Options[optSnapshotStatePath])
			if err != nil {
				return nil, err
			}
			recorder.fileSnapshotStateStore = store
			return recorder, nil
		},
	}
	spec := connector.RuntimeSpec{Name: "flow-open-failure", Options: map[string]string{
		optDSN:                  dsn,
		optSnapshotStateBackend: "file",
		optSnapshotStatePath:    filepath.Join(t.TempDir(), "state.json"),
	}}
	return source, recorder, spec
}

func TestBackfillSourceOpenClosesStateStoreWhenLaterStepFails(t *testing.T) {
	tests := []struct {
		name    string
		loadErr error
		want    string
	}{
		{"load fails", errors.New("injected load failure"), "injected load failure"},
		{"snapshot connection fails", nil, "snapshot"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source, recorder, spec := openFailureSource(t, tt.loadErr)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err := source.Open(ctx, spec)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Open err = %v, want %q", err, tt.want)
			}
			if got := recorder.closes.Load(); got != 1 {
				t.Fatalf("state store closes = %d, want 1", got)
			}
			if recorder.fileSnapshotStateStore.root != nil {
				t.Fatal("state directory descriptor still open after failed Open")
			}
			if source.stateStore != nil || source.pool != nil {
				t.Fatalf("partial resources retained: store=%v pool=%v", source.stateStore, source.pool)
			}
			// A caller's Close after a failed Open must stay safe and idempotent.
			if err := source.Close(context.Background()); err != nil {
				t.Fatalf("Close after failed Open: %v", err)
			}
			if err := source.Close(context.Background()); err != nil {
				t.Fatalf("second Close: %v", err)
			}
			if got := recorder.closes.Load(); got != 1 {
				t.Fatalf("state store closed %d times, want exactly 1", got)
			}
		})
	}
}

func TestBackfillSourceAckRejectsInvalidPartitionMetadata(t *testing.T) {
	ctx := context.Background()
	store, err := newFileSnapshotStateStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	source := &BackfillSource{stateStore: store, flowID: "flow-1"}
	ack := func(index, count, done string) error {
		meta := map[string]string{"mode": connector.SourceModeBackfill, "table": "public.widgets", "cursor": "7"}
		if index != "" {
			meta["partition_index"] = index
		}
		if count != "" {
			meta["partition_count"] = count
		}
		if done != "" {
			meta["done"] = done
		}
		return source.Ack(ctx, connector.Checkpoint{Metadata: meta})
	}
	for _, tt := range []struct{ name, index, count string }{
		{"index beyond count", "2", "2"},
		{"index without count", "1", ""},
		{"negative index", "-1", "2"},
		{"count above limit", "0", fmt.Sprint(maxBackfillPartitionCount + 1)},
		{"overflowing count falls back to one", "3", "99999999999999999999"},
	} {
		if err := ack(tt.index, tt.count, ""); err == nil {
			t.Fatalf("%s: invalid partition metadata acknowledged", tt.name)
		}
	}
	loaded, err := store.Load(ctx, "flow-1")
	if err != nil || len(loaded) != 0 {
		t.Fatalf("rejected acks were persisted: %v %v", loaded, err)
	}
	if err := ack("1", "2", "true"); err != nil {
		t.Fatalf("valid ack rejected: %v", err)
	}
	loaded, _ = store.Load(ctx, "flow-1")
	got, ok := loaded[snapshotTaskKey{Table: "public.widgets", PartitionIndex: 1, PartitionCount: 2}]
	if !ok || got.Status != snapshotStatusDone || got.Cursor != "7" {
		t.Fatalf("valid ack not stored: %v", loaded)
	}
}
