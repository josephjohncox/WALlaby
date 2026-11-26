package integrationharness

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
)

const (
	// stateDirEnv names the private per-invocation directory created by
	// scripts/test-integration.sh and shared by every test package it starts.
	stateDirEnv = "WALLABY_IT_STATE_DIR"

	harnessStateFileName = "harness.state"
	harnessLockFileName  = "harness.lock"
	maxHarnessStateBytes = 1 << 20
	maxRestoredEnvBytes  = 8192

	generatedKubeconfigPrefix = "wallaby-it-kube-"
	generatedKubeconfigSuffix = ".kubeconfig"
	generatedCAPrefix         = "wallaby-it-ca-"
	generatedCASuffix         = ".pem"
)

// generatedFileName admits only names this harness mints itself. Shared state
// can name a file to remove, but never a path, an operator file or a name that
// the harness could not have generated.
var generatedFileName = regexp.MustCompile(
	`^wallaby-it-(kube-[0-9a-f]{32}\.kubeconfig|ca-[0-9a-f]{32}\.pem)$`,
)

var generatedKubeconfigName = regexp.MustCompile(`^wallaby-it-kube-[0-9a-f]{32}\.kubeconfig$`)

// harnessRoot is the only filesystem location the harness uses for shared
// state, its lock and generated files. Every access goes through os.Root, so a
// symlink inside the directory cannot redirect a read, write or removal.
//
// path is display and identity metadata (for example the kind cluster name);
// no cleanup authority is derived from it. Deletion authority is the owned
// capability, which only createOwnedRoot can mint.
type harnessRoot struct {
	path  string
	root  *os.Root
	owned *ownedDirectory
}

// ownedDirectory is the authority to remove one directory this process created
// in a single-package invocation. It pins the parent directory the leaf was
// created in, so renaming or replacing the parent or the leaf afterwards cannot
// redirect removal to another directory, and it records the created
// directory's identity so a replaced leaf is refused instead of removed.
// Runner- or environment-provided roots never carry one.
type ownedDirectory struct {
	parent   *os.Root
	name     string
	identity fs.FileInfo
}

// openHarnessRoot opens the runner-provided private directory. Without one, a
// single-package invocation creates and later removes its own directory;
// sharing infrastructure between packages needs the runner's directory.
func openHarnessRoot(dir string, expectedPeers int) (*harnessRoot, error) {
	if dir == "" {
		if expectedPeers != 1 {
			return nil, fmt.Errorf("%d harness participants require the private state directory from scripts/test-integration.sh (%s)", expectedPeers, stateDirEnv)
		}
		return createOwnedRoot()
	}
	if !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("%s must be an absolute path", stateDirEnv)
	}
	return openPrivateRoot(dir)
}

// createOwnedRoot makes a fresh private directory with a generated name inside
// the temporary directory and returns it together with the only capability to
// remove it. The directory is created, inspected and opened through one pinned
// parent root, so no ambient path is resolved again after creation.
func createOwnedRoot() (*harnessRoot, error) {
	tempDir := os.TempDir()
	parent, err := os.OpenRoot(tempDir)
	if err != nil {
		return nil, fmt.Errorf("open temporary directory for harness state: %w", err)
	}
	name, err := mkdirGenerated(parent)
	if err != nil {
		_ = parent.Close()
		return nil, fmt.Errorf("create private harness state directory: %w", err)
	}
	root, err := openPrivateChild(parent, name, filepath.Join(tempDir, name))
	if err != nil {
		_ = parent.Remove(name)
		_ = parent.Close()
		return nil, err
	}
	identity, err := root.root.Stat(".")
	if err != nil {
		_ = root.root.Close()
		_ = parent.Remove(name)
		_ = parent.Close()
		return nil, fmt.Errorf("inspect created harness state directory: %w", err)
	}
	root.owned = &ownedDirectory{parent: parent, name: name, identity: identity}
	return root, nil
}

// mkdirGenerated creates a new 0700 directory with a random name; Mkdir never
// reuses or follows an existing name.
func mkdirGenerated(parent *os.Root) (string, error) {
	for range 16 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", err
		}
		name := "wallaby-it-" + hex.EncodeToString(random[:])
		err := parent.Mkdir(name, 0o700)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			return "", err
		}
		return name, nil
	}
	return "", errors.New("exhausted random directory name attempts")
}

func openPrivateRoot(dir string) (*harnessRoot, error) {
	dir = filepath.Clean(dir)
	parent, err := os.OpenRoot(filepath.Dir(dir))
	if err != nil {
		return nil, fmt.Errorf("open parent of harness state directory: %w", err)
	}
	defer func() { _ = parent.Close() }()
	return openPrivateChild(parent, filepath.Base(dir), dir)
}

// openPrivateChild opens the named child of parent as the private root.
// displayPath is metadata only.
func openPrivateChild(parent *os.Root, name, displayPath string) (*harnessRoot, error) {
	// Inspect the final component through its parent so a symlink is seen as a
	// symlink, then prove the directory opened is the one inspected.
	before, err := parent.Lstat(name)
	if err != nil {
		return nil, fmt.Errorf("inspect harness state directory: %w", err)
	}
	if err := checkPrivateDirectory(before); err != nil {
		return nil, err
	}
	root, err := parent.OpenRoot(name)
	if err != nil {
		return nil, fmt.Errorf("open harness state directory: %w", err)
	}
	after, err := root.Stat(".")
	if err == nil && !os.SameFile(before, after) {
		err = errors.New("harness state directory changed while opening")
	}
	if err == nil {
		err = checkPrivateDirectory(after)
	}
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	return &harnessRoot{path: displayPath, root: root}, nil
}

func checkPrivateDirectory(info fs.FileInfo) error {
	if info.Mode()&fs.ModeSymlink != 0 {
		return errors.New("harness state directory must not be a symlink")
	}
	if !info.IsDir() {
		return errors.New("harness state path is not a directory")
	}
	if info.Mode().Perm() != 0o700 {
		return fmt.Errorf("harness state directory mode is %04o, want 0700", info.Mode().Perm())
	}
	return checkOwnedByCurrentUser(info, "directory")
}

func checkPrivateFile(info fs.FileInfo, what string) error {
	if !info.Mode().IsRegular() {
		return fmt.Errorf("harness %s is not a regular file", what)
	}
	if info.Mode().Perm() != 0o600 {
		return fmt.Errorf("harness %s mode is %04o, want 0600", what, info.Mode().Perm())
	}
	return checkOwnedByCurrentUser(info, what)
}

func checkOwnedByCurrentUser(info fs.FileInfo, what string) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("cannot determine harness %s owner", what)
	}
	if int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("harness %s is owned by uid %d, want %d", what, stat.Uid, os.Geteuid())
	}
	return nil
}

// openRegular opens an existing or newly created private file without
// following a final symlink and proves the opened inode is the named one.
func (r *harnessRoot) openRegular(name string, flag int, what string) (*os.File, error) {
	file, err := r.root.OpenFile(name, flag|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err == nil {
		err = checkPrivateFile(opened, what)
	}
	if err == nil {
		var named fs.FileInfo
		named, err = r.root.Lstat(name)
		if err == nil && !os.SameFile(opened, named) {
			err = fmt.Errorf("harness %s changed while opening", what)
		}
	}
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func (r *harnessRoot) openLock() (*os.File, error) {
	file, err := r.openRegular(harnessLockFileName, os.O_CREATE|os.O_RDWR, "lock file")
	if err != nil {
		return nil, fmt.Errorf("open harness lock: %w", err)
	}
	return file, nil
}

func (r *harnessRoot) readState() (*integrationHarnessSharedState, error) {
	info, err := r.root.Lstat(harnessStateFileName)
	if errors.Is(err, fs.ErrNotExist) {
		return &integrationHarnessSharedState{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect harness shared state: %w", err)
	}
	if err := checkPrivateFile(info, "shared state"); err != nil {
		return nil, err
	}
	if info.Size() > maxHarnessStateBytes {
		return nil, fmt.Errorf("harness shared state exceeds %d bytes", maxHarnessStateBytes)
	}
	file, err := r.openRegular(harnessStateFileName, os.O_RDONLY, "shared state")
	if err != nil {
		return nil, fmt.Errorf("read harness shared state: %w", err)
	}
	defer func() { _ = file.Close() }()
	raw, err := io.ReadAll(io.LimitReader(file, maxHarnessStateBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read harness shared state: %w", err)
	}
	if len(raw) > maxHarnessStateBytes {
		return nil, fmt.Errorf("harness shared state exceeds %d bytes", maxHarnessStateBytes)
	}
	if len(raw) == 0 {
		return &integrationHarnessSharedState{}, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var state integrationHarnessSharedState
	if err := decoder.Decode(&state); err != nil {
		return nil, fmt.Errorf("decode harness shared state: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode harness shared state: trailing data")
	}
	return &state, nil
}

// writeState replaces the state with a durable random exclusive 0600 file.
func (r *harnessRoot) writeState(state *integrationHarnessSharedState) error {
	encoded, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal harness shared state: %w", err)
	}
	if len(encoded) > maxHarnessStateBytes {
		return fmt.Errorf("harness shared state exceeds %d bytes", maxHarnessStateBytes)
	}
	file, tmp, err := r.createExclusive(harnessStateFileName+".", ".tmp")
	if err != nil {
		return fmt.Errorf("create harness shared state temporary file: %w", err)
	}
	fail := func(err error) error {
		_ = file.Close()
		_ = r.root.Remove(tmp)
		return fmt.Errorf("write harness shared state: %w", err)
	}
	if _, err := file.Write(encoded); err != nil {
		return fail(err)
	}
	if err := file.Sync(); err != nil {
		return fail(err)
	}
	if err := file.Close(); err != nil {
		_ = r.root.Remove(tmp)
		return fmt.Errorf("write harness shared state: %w", err)
	}
	if err := r.root.Rename(tmp, harnessStateFileName); err != nil {
		_ = r.root.Remove(tmp)
		return fmt.Errorf("publish harness shared state: %w", err)
	}
	return r.syncDirectory()
}

func (r *harnessRoot) syncDirectory() error {
	dir, err := r.root.Open(".")
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	return errors.Join(syncErr, closeErr)
}

func (r *harnessRoot) removeState() error {
	if err := r.root.Remove(harnessStateFileName); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// createExclusive creates a new random 0600 file. O_EXCL never reuses or
// follows an existing name, including a pre-planted symlink.
func (r *harnessRoot) createExclusive(prefix, suffix string) (*os.File, string, error) {
	for range 16 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return nil, "", err
		}
		name := prefix + hex.EncodeToString(random[:]) + suffix
		file, err := r.root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			return nil, "", err
		}
		return file, name, nil
	}
	return nil, "", errors.New("exhausted random file name attempts")
}

func (r *harnessRoot) createGeneratedKubeconfig() (*os.File, string, error) {
	return r.createExclusive(generatedKubeconfigPrefix, generatedKubeconfigSuffix)
}

func (r *harnessRoot) createGeneratedCA() (*os.File, string, error) {
	return r.createExclusive(generatedCAPrefix, generatedCASuffix)
}

func (r *harnessRoot) generatedPath(name string) string {
	return filepath.Join(r.path, name)
}

// isGeneratedPath reports whether value names a harness-generated file
// directly inside this root.
func (r *harnessRoot) isGeneratedPath(value string) bool {
	if !filepath.IsAbs(value) {
		return false
	}
	clean := filepath.Clean(value)
	return filepath.Dir(clean) == r.path && generatedFileName.MatchString(filepath.Base(clean))
}

// removeGenerated deletes only a regular file with a harness-minted name from
// the private root. A name from shared state is data, not authority.
func (r *harnessRoot) removeGenerated(name string) error {
	if !generatedFileName.MatchString(name) {
		return fmt.Errorf("refusing to remove %q: not a harness-generated file name", name)
	}
	info, err := r.root.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("refusing to remove %q: not a regular file", name)
	}
	if err := r.root.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// close releases the root. A directory this process created is emptied through
// the pinned root first; its leaf is then removed through the pinned parent
// only if it is still the directory that was created. A replaced or moved leaf
// is refused, never removed. A runner-provided directory is left for the shell
// that created it. Every descriptor is closed on every path.
func (r *harnessRoot) close() error {
	var removeErr error
	if r.owned != nil {
		removeErr = r.removeContents()
	}
	closeErr := r.root.Close()
	if r.owned != nil {
		if removeErr == nil {
			removeErr = r.owned.remove()
		}
		closeErr = errors.Join(closeErr, r.owned.parent.Close())
	}
	return errors.Join(removeErr, closeErr)
}

// remove deletes the now-empty owned leaf through the pinned parent after
// proving the leaf still names the directory that was created.
func (d *ownedDirectory) remove() error {
	current, err := d.parent.Lstat(d.name)
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("refusing to remove harness state directory %q: it was moved or removed", d.name)
	}
	if err != nil {
		return err
	}
	if !current.IsDir() || !os.SameFile(d.identity, current) {
		return fmt.Errorf("refusing to remove harness state directory %q: it no longer names the created directory", d.name)
	}
	return d.parent.Remove(d.name)
}

func (r *harnessRoot) removeContents() error {
	dir, err := r.root.Open(".")
	if err != nil {
		return err
	}
	names, readErr := dir.Readdirnames(-1)
	closeErr := dir.Close()
	err = errors.Join(readErr, closeErr)
	for _, name := range names {
		err = errors.Join(err, r.root.RemoveAll(name))
	}
	return err
}

// derivedKindClusterName names the invocation's kind cluster from its private
// root. Peers sharing the root share the name; separate invocations, whose
// roots are distinct, can neither reuse nor delete each other's cluster.
func (r *harnessRoot) derivedKindClusterName() (string, error) {
	canonical, err := filepath.EvalSymlinks(r.path)
	if err != nil {
		return "", fmt.Errorf("canonicalize harness state directory: %w", err)
	}
	sum := sha256.Sum256([]byte(filepath.Clean(canonical)))
	return "wallaby-it-" + hex.EncodeToString(sum[:8]), nil
}
