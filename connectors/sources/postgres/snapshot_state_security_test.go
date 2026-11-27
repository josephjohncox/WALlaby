package postgres

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testSnapshotState(cursor string) snapshotTaskState {
	return snapshotTaskState{FlowID: "flow-1", Table: "public.widgets", PartitionCount: 1, Cursor: cursor, Status: snapshotStatusRunning}
}

func requireNoStateTempFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tmp") {
			t.Fatalf("temporary state file %q was left behind", entry.Name())
		}
	}
}

// A symlink planted at the state file name must neither be read nor written
// through; the old os.ReadFile/os.WriteFile implementation followed it.
func TestFileSnapshotStateStoreRejectsSymlinkAtStatePath(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "victim.json")
	if err := os.WriteFile(outside, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "state.json")
	if err := os.Symlink(outside, path); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := newFileSnapshotStateStore(path); err == nil {
		t.Fatal("expected symlink state path to be rejected")
	}
	if got, _ := os.ReadFile(outside); string(got) != "{}" {
		t.Fatalf("symlink target was modified: %q", got)
	}
}

func TestFileSnapshotStateStoreReplacesSymlinkInsteadOfWritingThroughIt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store, err := newFileSnapshotStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	outside := filepath.Join(t.TempDir(), "victim.json")
	if err := os.WriteFile(outside, []byte("victim"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := store.Upsert(context.Background(), testSnapshotState("1")); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if got, _ := os.ReadFile(outside); string(got) != "victim" {
		t.Fatalf("write followed symlink to %q", got)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("state path is not a regular file after write: %v %v", info, err)
	}
	requireNoStateTempFiles(t, dir)
}

// After Open the directory is pinned: renaming it away and planting a symlink
// at its old path must not redirect later writes to the symlink target.
func TestFileSnapshotStateStoreKeepsPinnedDirectoryAfterPathSwap(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "state")
	store, err := newFileSnapshotStateStore(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	moved := filepath.Join(parent, "moved")
	attacker := filepath.Join(parent, "attacker")
	if err := os.Mkdir(attacker, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(dir, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(attacker, dir); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := store.Upsert(context.Background(), testSnapshotState("1")); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if entries, _ := os.ReadDir(attacker); len(entries) != 0 {
		t.Fatalf("state escaped to the swapped directory: %v", entries)
	}
	if _, err := os.Stat(filepath.Join(moved, "state.json")); err != nil {
		t.Fatalf("state was not written to the pinned directory: %v", err)
	}
}

func TestFileSnapshotStateStoreRejectsNonFileNames(t *testing.T) {
	dir := t.TempDir()
	for _, path := range []string{"", ".", "..", filepath.Join(dir, ".."), string(filepath.Separator)} {
		if _, err := newFileSnapshotStateStore(path); err == nil {
			t.Fatalf("path %q accepted", path)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "isdir"), 0o750); err != nil {
		t.Fatal(err)
	}
	if _, err := newFileSnapshotStateStore(filepath.Join(dir, "isdir")); err == nil {
		t.Fatal("directory accepted as state file")
	}
}

func TestFileSnapshotStateStoreBoundsFileSizeBeforeReading(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	// Sparse file: reading it whole would allocate the full size.
	if err := file.Truncate(maxSnapshotStateFileBytes + 1); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if _, err := newFileSnapshotStateStore(path); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("oversized state file accepted: %v", err)
	}
}

func TestFileSnapshotStateStoreRejectsHostileEntries(t *testing.T) {
	valid := testSnapshotState("")
	tests := []struct {
		name   string
		mutate func(*snapshotTaskState)
		key    string
	}{
		{"negative count", func(s *snapshotTaskState) { s.PartitionCount = -1 }, ""},
		{"zero count", func(s *snapshotTaskState) { s.PartitionCount = 0 }, ""},
		{"huge count", func(s *snapshotTaskState) { s.PartitionCount = int(^uint(0) >> 1) }, ""},
		{"count above limit", func(s *snapshotTaskState) { s.PartitionCount = maxBackfillPartitionCount + 1 }, ""},
		{"negative index", func(s *snapshotTaskState) { s.PartitionIndex = -1 }, ""},
		{"index beyond count", func(s *snapshotTaskState) { s.PartitionIndex = 1 }, ""},
		{"unknown status", func(s *snapshotTaskState) { s.Status = "bogus" }, ""},
		{"empty flow", func(s *snapshotTaskState) { s.FlowID = "" }, ""},
		{"key mismatch", func(*snapshotTaskState) {}, "other|public.widgets|0|1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := valid
			tt.mutate(&state)
			key := tt.key
			if key == "" {
				key = snapshotStateKey(state)
			}
			payload, err := json.Marshal(map[string]snapshotTaskState{key: state})
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "state.json")
			if err := os.WriteFile(path, payload, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := newFileSnapshotStateStore(path); err == nil {
				t.Fatalf("hostile entry accepted: %s", payload)
			}
		})
	}
}

func TestFileSnapshotStateStoreUpsertRejectsInvalidPartition(t *testing.T) {
	store, err := newFileSnapshotStateStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, mutate := range []func(*snapshotTaskState){
		func(s *snapshotTaskState) { s.PartitionIndex = -1 },
		func(s *snapshotTaskState) { s.PartitionIndex = 1 },
		func(s *snapshotTaskState) { s.PartitionCount = maxBackfillPartitionCount + 1 },
		func(s *snapshotTaskState) { s.Status = "bogus" },
	} {
		state := testSnapshotState("")
		mutate(&state)
		if err := store.Upsert(context.Background(), state); err == nil {
			t.Fatalf("invalid state accepted: %#v", state)
		}
	}
	loaded, err := store.Load(context.Background(), "flow-1")
	if err != nil || len(loaded) != 0 {
		t.Fatalf("rejected states were retained: %v %v", loaded, err)
	}
}

// A write that cannot be persisted must leave both memory and the previous file
// unchanged. The old implementation mutated memory before writing and wrote an
// unbounded payload with a truncating os.WriteFile.
func TestFileSnapshotStateStoreFailedWriteKeepsPreviousState(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store, err := newFileSnapshotStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Upsert(ctx, testSnapshotState("1")); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Upsert(ctx, testSnapshotState(strings.Repeat("x", maxSnapshotStateFileBytes))); err == nil {
		t.Fatal("oversized state accepted")
	}
	newTask := testSnapshotState("")
	newTask.Table = "public.other"
	newTask.Cursor = strings.Repeat("x", maxSnapshotStateFileBytes)
	if err := store.Upsert(ctx, newTask); err == nil {
		t.Fatal("oversized new state accepted")
	}

	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(before) {
		t.Fatalf("state file changed by failed write: %v", err)
	}
	loaded, err := store.Load(ctx, "flow-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 {
		t.Fatalf("failed insert leaked into memory: %v", loaded)
	}
	for _, got := range loaded {
		if got.Cursor != "1" {
			t.Fatalf("failed update leaked into memory: %q", got.Cursor)
		}
	}
	requireNoStateTempFiles(t, dir)
}

func TestFileSnapshotStateStoreTempFileFailureRollsBackAndLeavesFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permissions do not bind root")
	}
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store, err := newFileSnapshotStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Upsert(ctx, testSnapshotState("1")); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(dir, 0o750) }()
	if err := store.Upsert(ctx, testSnapshotState("2")); err == nil {
		t.Fatal("expected write failure in read-only directory")
	}
	after, _ := os.ReadFile(path)
	if string(after) != string(before) {
		t.Fatal("previous state file was modified by failed write")
	}
	loaded, _ := store.Load(ctx, "flow-1")
	for _, got := range loaded {
		if got.Cursor != "1" {
			t.Fatalf("failed write leaked into memory: %q", got.Cursor)
		}
	}
}

func TestFileSnapshotStateStoreWritesAtomicallyWithOwnerOnlyMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store, err := newFileSnapshotStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Upsert(context.Background(), testSnapshotState("1")); err != nil {
		t.Fatal(err)
	}
	first, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if first.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v", first.Mode().Perm())
	}
	if err := store.Upsert(context.Background(), testSnapshotState("2")); err != nil {
		t.Fatal(err)
	}
	second, _ := os.Stat(path)
	if os.SameFile(first, second) {
		t.Fatal("state file was rewritten in place instead of replaced by rename")
	}
	requireNoStateTempFiles(t, dir)
}

func TestFileSnapshotStateStoreCloseReleasesDirectoryAndRejectsUpsert(t *testing.T) {
	store, err := newFileSnapshotStateStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	store.Close()
	if err := store.Upsert(context.Background(), testSnapshotState("1")); err == nil {
		t.Fatal("upsert after close succeeded")
	}
}

func TestFileSnapshotStateStoreFailedLoadReleasesDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newFileSnapshotStateStore(path); err == nil {
		t.Fatal("corrupt state accepted")
	}
}

func TestPostgresSnapshotStateIdentifierBounds(t *testing.T) {
	for _, ident := range []string{strings.Repeat("a", maxPostgresIdentifierBytes+1), "a\x00b"} {
		if _, err := newPostgresSnapshotStateStore(context.Background(), "postgres://127.0.0.1:1/db", ident, "t", nil); err == nil || !strings.Contains(err.Error(), "identifier") {
			t.Fatalf("identifier %q accepted before connecting: %v", ident, err)
		}
	}
}
