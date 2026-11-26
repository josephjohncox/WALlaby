package integrationharness

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func privateTestDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func openTestRoot(t *testing.T) *harnessRoot {
	t.Helper()
	root, err := openHarnessRoot(privateTestDir(t), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.close() })
	return root
}

func writeSentinel(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sentinel")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func requireSentinel(t *testing.T, path, content string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || string(got) != content {
		t.Fatalf("outside sentinel changed: %q, %v", got, err)
	}
}

func TestOpenHarnessRootRejectsUnsafeDirectories(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(base, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for name, mode := range map[string]os.FileMode{"group": 0o770, "other": 0o705, "readable": 0o755, "closed": 0o500} {
		dir := filepath.Join(base, name)
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir, mode); err != nil {
			t.Fatal(err)
		}
		if root, err := openHarnessRoot(dir, 1); err == nil {
			_ = root.close()
			t.Fatalf("accepted %04o state directory", mode)
		}
	}
	for name, path := range map[string]string{
		"symlink":  link,
		"file":     file,
		"relative": "relative/state",
		"missing":  filepath.Join(base, "missing"),
	} {
		if root, err := openHarnessRoot(path, 1); err == nil {
			_ = root.close()
			t.Fatalf("accepted unsafe %s state path", name)
		}
	}
}

func TestOpenHarnessRootWithoutRunnerDirectoryIsSinglePackageAndSelfCleaning(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	if root, err := openHarnessRoot("", 2); err == nil {
		_ = root.close()
		t.Fatal("multi-package sharing without the runner directory was accepted")
	}
	root, err := openHarnessRoot("", 1)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(root.path)
	if err != nil || info.Mode().Perm() != 0o700 || !info.IsDir() {
		t.Fatalf("private directory = %v, %v", info, err)
	}
	if err := root.writeState(&integrationHarnessSharedState{Version: 1}); err != nil {
		t.Fatal(err)
	}
	file, _, err := root.createGeneratedKubeconfig()
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if err := root.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(root.path); !os.IsNotExist(err) {
		t.Fatalf("self-created directory retained: %v", err)
	}
}

func TestRunnerProvidedHarnessRootIsNotRemovedByHarness(t *testing.T) {
	dir := privateTestDir(t)
	root, err := openHarnessRoot(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := root.writeState(&integrationHarnessSharedState{Version: 1}); err != nil {
		t.Fatal(err)
	}
	if err := root.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(dir, harnessStateFileName)); err != nil {
		t.Fatalf("runner-owned directory was modified by close: %v", err)
	}
}

func TestStateRoundTripIsPrivateAtomicAndLeavesNoTemporaryFile(t *testing.T) {
	root := openTestRoot(t)
	want := &integrationHarnessSharedState{Version: 1, Active: true, KindCluster: "c", Participants: 2, ExpectedParticipants: 3}
	if err := root.writeState(want); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(filepath.Join(root.path, harnessStateFileName))
	if err != nil || info.Mode().Perm() != 0o600 || !info.Mode().IsRegular() {
		t.Fatalf("state file = %v, %v", info, err)
	}
	got, err := root.readState()
	if err != nil {
		t.Fatal(err)
	}
	if got.Participants != 2 || got.ExpectedParticipants != 3 || got.KindCluster != "c" || !got.Active {
		t.Fatalf("state round trip lost participant evidence: %+v", got)
	}
	entries, err := os.ReadDir(root.path)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tmp") {
			t.Fatalf("temporary state file retained: %s", entry.Name())
		}
	}
}

func TestStateWriteDoesNotFollowPlantedSymlinks(t *testing.T) {
	root := openTestRoot(t)
	sentinel := writeSentinel(t, "operator data")
	for _, name := range []string{harnessStateFileName + ".tmp", harnessStateFileName} {
		if err := os.Symlink(sentinel, filepath.Join(root.path, name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := root.writeState(&integrationHarnessSharedState{Version: 1}); err != nil {
		t.Fatal(err)
	}
	requireSentinel(t, sentinel, "operator data")
	info, err := os.Lstat(filepath.Join(root.path, harnessStateFileName))
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("symlink was not replaced by regular state: %v, %v", info, err)
	}
}

func TestReadStateRejectsHostileStateFiles(t *testing.T) {
	sentinel := writeSentinel(t, `{"version":1}`)
	for name, prepare := range map[string]func(path string) error{
		"symlink": func(path string) error { return os.Symlink(sentinel, path) },
		"group readable": func(path string) error {
			if err := os.WriteFile(path, []byte(`{"version":1}`), 0o600); err != nil {
				return err
			}
			return os.Chmod(path, 0o640)
		},
		"oversized":      func(path string) error { return os.WriteFile(path, make([]byte, maxHarnessStateBytes+1), 0o600) },
		"unknown field":  func(path string) error { return os.WriteFile(path, []byte(`{"version":1,"kindKubePath":"/x"}`), 0o600) },
		"trailing":       func(path string) error { return os.WriteFile(path, []byte(`{"version":1} {}`), 0o600) },
		"malformed":      func(path string) error { return os.WriteFile(path, []byte(`{`), 0o600) },
		"directory":      func(path string) error { return os.Mkdir(path, 0o700) },
		"wrongly typed":  func(path string) error { return os.WriteFile(path, []byte(`{"participants":"many"}`), 0o600) },
		"forged env key": func(path string) error { return os.WriteFile(path, []byte(`{"env":{"A":5}}`), 0o600) },
	} {
		t.Run(name, func(t *testing.T) {
			root := openTestRoot(t)
			path := filepath.Join(root.path, harnessStateFileName)
			if err := prepare(path); err != nil {
				t.Fatal(err)
			}
			if _, err := root.readState(); err == nil {
				t.Fatal("hostile state was accepted")
			}
			requireSentinel(t, sentinel, `{"version":1}`)
		})
	}
}

func TestOpenLockDoesNotFollowOrCreateThroughSymlink(t *testing.T) {
	root := openTestRoot(t)
	outside := filepath.Join(t.TempDir(), "created-by-lock")
	if err := os.Symlink(outside, filepath.Join(root.path, harnessLockFileName)); err != nil {
		t.Fatal(err)
	}
	if lock, err := root.openLock(); err == nil {
		_ = lock.Close()
		t.Fatal("symlinked lock was accepted")
	}
	if _, err := os.Lstat(outside); !os.IsNotExist(err) {
		t.Fatalf("lock open created an outside file: %v", err)
	}
}

func TestOpenLockRejectsPermissiveLock(t *testing.T) {
	root := openTestRoot(t)
	path := filepath.Join(root.path, harnessLockFileName)
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o666); err != nil {
		t.Fatal(err)
	}
	if lock, err := root.openLock(); err == nil {
		_ = lock.Close()
		t.Fatal("world-writable lock was accepted")
	}
}

func TestGeneratedFilesAreExclusivePrivateAndRemovable(t *testing.T) {
	root := openTestRoot(t)
	kube, kubeName, err := root.createGeneratedKubeconfig()
	if err != nil {
		t.Fatal(err)
	}
	ca, caName, err := root.createGeneratedCA()
	if err != nil {
		t.Fatal(err)
	}
	_ = kube.Close()
	_ = ca.Close()
	if !generatedKubeconfigName.MatchString(kubeName) || !generatedFileName.MatchString(caName) {
		t.Fatalf("unexpected generated names %q %q", kubeName, caName)
	}
	for _, name := range []string{kubeName, caName} {
		info, err := os.Lstat(root.generatedPath(name))
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("generated %s = %v, %v", name, info, err)
		}
		if !root.isGeneratedPath(root.generatedPath(name)) {
			t.Fatalf("generated path %s not recognised", name)
		}
		if err := root.removeGenerated(name); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(root.generatedPath(name)); !os.IsNotExist(err) {
			t.Fatalf("generated file retained: %v", err)
		}
		if err := root.removeGenerated(name); err != nil {
			t.Fatalf("removing an already removed generated file failed: %v", err)
		}
	}
}

func TestRemoveGeneratedRefusesForgedNamesAndSymlinks(t *testing.T) {
	root := openTestRoot(t)
	sentinel := writeSentinel(t, "developer kubeconfig")
	planted := "wallaby-it-kube-" + strings.Repeat("a", 32) + ".kubeconfig"
	if err := os.Symlink(sentinel, root.generatedPath(planted)); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		sentinel,
		"../" + filepath.Base(sentinel),
		"sub/" + planted,
		"..",
		".",
		"",
		harnessStateFileName,
		harnessLockFileName,
		"wallaby-it-kube-short.kubeconfig",
		planted, // matches the generated pattern but is a symlink, not a regular file
	} {
		if err := root.removeGenerated(name); err == nil {
			t.Fatalf("removeGenerated(%q) did not refuse", name)
		}
		requireSentinel(t, sentinel, "developer kubeconfig")
	}
	if _, err := os.Lstat(root.generatedPath(planted)); err != nil {
		t.Fatalf("planted symlink was removed instead of refused: %v", err)
	}
}
