package tracepath

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const sentinelContent = "sentinel must survive"

func writeSentinel(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(sentinelContent), 0o600); err != nil {
		t.Fatal(err)
	}
}

func requireSentinel(t *testing.T, path string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || string(got) != sentinelContent {
		t.Fatalf("sentinel %s = %q, %v; want unchanged", path, got, err)
	}
}

func TestOpenRejectsUnsafeFlowIDsWithoutTouchingFilesystem(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	root := filepath.Join(base, "traces")
	outside := filepath.Join(base, "outside")
	if err := os.Mkdir(outside, 0o750); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(outside, "owned.jsonl")
	writeSentinel(t, sentinel)

	for _, id := range []string{
		"", ".", "..", "../outside/owned.jsonl", "..\\outside\\owned.jsonl", "../../outside/owned",
		"a/b", "/abs", "a\\b", ".hidden", "-lead", "a\x00b", "a\nb", "a\tb", "id\n", "id ", "caf\u00e9",
		strings.Repeat("a", 129),
	} {
		for name, template := range map[string]string{
			"placeholder": filepath.Join(root, "trace-{flow_id}.jsonl"),
			"directory":   filepath.Join(root, "{flow_id}", "trace.jsonl"),
			"appended":    filepath.Join(root, "trace.jsonl"),
		} {
			if file, err := Open(template, id); err == nil {
				_ = file.Close()
				t.Fatalf("Open(%q, %q) [%s] succeeded", template, id, name)
			}
		}
	}
	requireSentinel(t, sentinel)
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("trace root created before flow ID validation: %v", err)
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 1 {
		t.Fatalf("outside dir entries=%v err=%v, want only sentinel", entries, err)
	}
}

func TestOpenPlacesValidIDsUnderTemplateRoot(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	cases := []struct {
		name     string
		template string
		id       string
		want     string
	}{
		{"placeholder in file name", filepath.Join(base, "a", "trace-{flow_id}.jsonl"), "flow-1", filepath.Join(base, "a", "trace-flow-1.jsonl")},
		{"placeholder directory", filepath.Join(base, "b", "{flow_id}", "main.jsonl"), "Flow_2.v3", filepath.Join(base, "b", "Flow_2.v3", "main.jsonl")},
		{"repeated placeholder", filepath.Join(base, "c", "{flow_id}", "{flow_id}.jsonl"), "x", filepath.Join(base, "c", "x", "x.jsonl")},
		{"no placeholder appends id", filepath.Join(base, "d", "trace.jsonl"), "flow-4", filepath.Join(base, "d", "trace.jsonl.flow-4")},
		{"dots inside id", filepath.Join(base, "e", "{flow_id}"), "a..b", filepath.Join(base, "e", "a..b")},
		{"max length", filepath.Join(base, "f", "{flow_id}"), strings.Repeat("z", 128), filepath.Join(base, "f", strings.Repeat("z", 128))},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			file, err := Open(tt.template, tt.id)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.WriteString("line\n"); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(tt.want)
			if err != nil || string(got) != "line\n" {
				t.Fatalf("trace at %s = %q, %v", tt.want, got, err)
			}
			if runtime.GOOS != "windows" {
				info, err := os.Stat(tt.want)
				if err != nil {
					t.Fatal(err)
				}
				if info.Mode().Perm() != 0o600 {
					t.Fatalf("trace mode=%v, want 0600", info.Mode().Perm())
				}
			}
		})
	}
}

func TestOpenTruncatesExistingTrace(t *testing.T) {
	t.Parallel()
	template := filepath.Join(t.TempDir(), "trace-{flow_id}.jsonl")
	for _, content := range []string{"first run, longer\n", "second\n"} {
		file, err := Open(template, "f")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = file.WriteString(content)
		_ = file.Close()
	}
	got, err := os.ReadFile(strings.ReplaceAll(template, "{flow_id}", "f"))
	if err != nil || string(got) != "second\n" {
		t.Fatalf("trace=%q err=%v, want truncated second run", got, err)
	}
}

func TestOpenTightensExistingTracePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix file permission contract")
	}
	t.Parallel()
	template := filepath.Join(t.TempDir(), "trace-{flow_id}.jsonl")
	path := strings.ReplaceAll(template, "{flow_id}", "flow")
	if err := os.WriteFile(path, []byte("previous trace"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o666); err != nil {
		t.Fatal(err)
	}
	file, err := Open(template, "flow")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("existing trace mode=%v err=%v, want 0600", info, err)
	}
}

func TestOpenDoesNotFollowSymlinksOutOfRoot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation needs privileges on Windows")
	}
	t.Parallel()
	base := t.TempDir()
	root := filepath.Join(base, "traces")
	outside := filepath.Join(base, "outside")
	for _, dir := range []string{root, outside} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	sentinel := filepath.Join(outside, "owned.jsonl")
	writeSentinel(t, sentinel)

	// A flow-named directory that is a symlink out of the root.
	if err := os.Symlink(outside, filepath.Join(root, "dirlink")); err != nil {
		t.Fatal(err)
	}
	if file, err := Open(filepath.Join(root, "{flow_id}", "owned.jsonl"), "dirlink"); err == nil {
		_ = file.Close()
		t.Fatal("trace opened through directory symlink escaping the root")
	}
	// A flow-named file that is a symlink to an outside file.
	if err := os.Symlink(sentinel, filepath.Join(root, "filelink.jsonl")); err != nil {
		t.Fatal(err)
	}
	if file, err := Open(filepath.Join(root, "{flow_id}.jsonl"), "filelink"); err == nil {
		_ = file.Close()
		t.Fatal("trace opened through file symlink escaping the root")
	}
	requireSentinel(t, sentinel)
}

func TestOpenRequiresTemplate(t *testing.T) {
	t.Parallel()
	if file, err := Open("", "flow"); err == nil {
		_ = file.Close()
		t.Fatal("empty template accepted")
	}
}
