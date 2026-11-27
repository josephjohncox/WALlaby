package integrationharness

import (
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// isolateHarnessEnv keeps lifecycle tests away from any developer cluster or
// service: no kubeconfig, no managed endpoints and a preset Postgres DSN.
func isolateHarnessEnv(t *testing.T) {
	t.Helper()
	for _, key := range integrationManagedEnvKeys() {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
	// A runner-provided directory must never leak into a unit test.
	t.Setenv(stateDirEnv, "")
	if err := os.Unsetenv(stateDirEnv); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", "/dev/null")
	t.Setenv("TEST_PG_DSN", "postgres://user:pass@db.example.test:5432/app")
}

func newTestHarness(t *testing.T, root *harnessRoot, peers int) *integrationHarness {
	t.Helper()
	h := &integrationHarness{
		config:    integrationHarnessConfig{kindCluster: "unit-cluster", expectedPeers: peers},
		stateRoot: root,
	}
	h.originalEnv = captureEnv(integrationManagedEnvKeys()...)
	t.Cleanup(h.restoreCapturedEnv)
	return h
}

func TestSharedParticipantsCountThroughRunnerDirectoryAndLastParticipantCleans(t *testing.T) {
	isolateHarnessEnv(t)
	dir := privateTestDir(t)
	t.Setenv(stateDirEnv, dir)

	var harnesses []*integrationHarness
	for i := 1; i <= 3; i++ {
		root, err := openHarnessRoot(os.Getenv(stateDirEnv), 3)
		if err != nil {
			t.Fatal(err)
		}
		h := newTestHarness(t, root, 3)
		if err := h.start(); err != nil {
			t.Fatalf("participant %d: %v", i, err)
		}
		state, err := root.readState()
		if err != nil {
			t.Fatal(err)
		}
		if state.Participants != i || state.JoinedTotal != i || state.ExpectedParticipants != 3 || !state.Active {
			t.Fatalf("participant %d state = %+v", i, state)
		}
		if h.ownsInfra != (i == 1) {
			t.Fatalf("participant %d ownsInfra=%v", i, h.ownsInfra)
		}
		harnesses = append(harnesses, h)
	}
	for i, h := range harnesses {
		if err := h.stop(); err != nil {
			t.Fatal(err)
		}
		reader, err := openHarnessRoot(dir, 3)
		if err != nil {
			t.Fatal(err)
		}
		state, err := reader.readState()
		if err != nil {
			t.Fatal(err)
		}
		last := i == len(harnesses)-1
		if last && (state.Participants != 0 || state.Active) {
			t.Fatalf("last participant left state %+v", state)
		}
		if !last && (state.Participants != len(harnesses)-i-1 || !state.Active) {
			t.Fatalf("after stop %d state = %+v", i+1, state)
		}
		_ = reader.close()
	}
	if _, err := os.Lstat(filepath.Join(dir, harnessStateFileName)); !os.IsNotExist(err) {
		t.Fatalf("shared state retained after last participant: %v", err)
	}
	if _, err := os.Lstat(dir); err != nil {
		t.Fatalf("harness removed the runner-owned directory: %v", err)
	}
}

func TestDirectSinglePackageRunUsesAndRemovesItsOwnPrivateDirectory(t *testing.T) {
	isolateHarnessEnv(t)
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv(stateDirEnv, "")
	h := newTestHarness(t, nil, 1)
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	path := h.stateRoot.path
	if _, err := os.Lstat(filepath.Join(path, harnessStateFileName)); err != nil {
		t.Fatalf("direct run did not persist state in its private directory: %v", err)
	}
	if err := h.stop(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("own private directory retained: %v", err)
	}
}

const forgedStateHead = `{"version":1,"active":true,"kindCluster":"unit-cluster","participants":1,"expectedParticipants":2,"updatedAt":"2999-01-01T00:00:00Z"`

func TestStartRejectsForgedSharedStateWithoutRestoringEnvironment(t *testing.T) {
	for name, state := range map[string]string{
		"outside kubeconfig":   forgedStateHead + `,"env":{"KUBECONFIG":"/etc/passwd"}}`,
		"outside CA":           forgedStateHead + `,"env":{"WALLABY_TEST_CLICKHOUSE_TLS_CA":"/etc/passwd"}}`,
		"unmanaged key":        forgedStateHead + `,"env":{"LD_PRELOAD":"/tmp/x"}}`,
		"oversized value":      forgedStateHead + `,"env":{"TEST_PG_DSN":"` + strings.Repeat("a", maxRestoredEnvBytes+1) + `"}}`,
		"traversing owned":     forgedStateHead + `,"kindKubeFile":"../../etc/passwd"}`,
		"operator path as own": forgedStateHead + `,"kindKubeFile":"/home/dev/.kube/config"}`,
	} {
		t.Run(name, func(t *testing.T) {
			isolateHarnessEnv(t)
			dir := privateTestDir(t)
			t.Setenv(stateDirEnv, dir)
			statePath := filepath.Join(dir, harnessStateFileName)
			if err := os.WriteFile(statePath, []byte(state), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("WALLABY_TEST_S3_BUCKET", "operator-bucket")
			h := newTestHarness(t, nil, 2)
			err := h.start()
			if err == nil {
				t.Fatal("forged state was accepted")
			}
			if !strings.Contains(err.Error(), "harness shared state") {
				t.Fatalf("rejected for the wrong reason (not state validation): %v", err)
			}
			if got := os.Getenv("WALLABY_TEST_S3_BUCKET"); got != "operator-bucket" {
				t.Fatalf("environment changed before validation completed: %q", got)
			}
			if os.Getenv("KUBECONFIG") != "/dev/null" || os.Getenv("LD_PRELOAD") != "" {
				t.Fatal("forged state environment was restored")
			}
			// The rejected peer never joined, so stopping it must leave the live
			// owner's state byte-for-byte alone.
			if err := h.stop(); err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(statePath)
			if err != nil || string(after) != state {
				t.Fatalf("rejected peer modified or removed the owner's state: %v %q", err, after)
			}
		})
	}
}

func TestNonJoinedPeerNeverDecrementsOrCleansLiveOwner(t *testing.T) {
	isolateHarnessEnv(t)
	dir := privateTestDir(t)
	t.Setenv(stateDirEnv, dir)
	owner := newTestHarness(t, nil, 2)
	if err := owner.start(); err != nil {
		t.Fatal(err)
	}
	// A peer that never joined (for example, its start was rejected) must not
	// decrement the count or tear down the live owner's infrastructure.
	peerRoot, err := openHarnessRoot(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	peer := newTestHarness(t, peerRoot, 2)
	peer.config.kindCluster = owner.config.kindCluster
	if err := peer.stop(); err != nil {
		t.Fatal(err)
	}
	reader, err := openHarnessRoot(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.close() }()
	state, err := reader.readState()
	if err != nil {
		t.Fatal(err)
	}
	if state.Participants != 1 || !state.Active {
		t.Fatalf("owner state altered by a non-joined peer: %+v", state)
	}
}

func TestTwoPeersJoinWhenOperatorSetsOnlyOneKubeconfigVariable(t *testing.T) {
	for _, variable := range []string{"KUBECONFIG", "WALLABY_TEST_K8S_KUBECONFIG"} {
		t.Run(variable, func(t *testing.T) {
			isolateHarnessEnv(t)
			dir := privateTestDir(t)
			t.Setenv(stateDirEnv, dir)
			operator := writeSentinel(t, "operator kubeconfig")
			// startKind mirrors an operator kubeconfig into both variables, so the
			// owner publishes both even though the operator exported only one.
			t.Setenv(variable, operator)
			dsn := "postgres://user:pass@db.example.test:5432/app"
			state := integrationHarnessSharedState{
				Version: 1, Active: true, KindCluster: "unit-cluster", Participants: 1, JoinedTotal: 1,
				ExpectedParticipants: 2, UpdatedAt: time.Now(),
				Env: map[string]*string{
					"KUBECONFIG":                  &operator,
					"WALLABY_TEST_K8S_KUBECONFIG": &operator,
					"TEST_PG_DSN":                 &dsn,
				},
			}
			seed, err := openHarnessRoot(dir, 2)
			if err != nil {
				t.Fatal(err)
			}
			if err := seed.writeState(&state); err != nil {
				t.Fatal(err)
			}
			if err := seed.close(); err != nil {
				t.Fatal(err)
			}
			peer := newTestHarness(t, nil, 2)
			peer.config.kindCluster = "unit-cluster"
			if err := peer.start(); err != nil {
				t.Fatalf("legitimate peer rejected: %v", err)
			}
			if !peer.joined {
				t.Fatal("peer did not join")
			}
			if os.Getenv("KUBECONFIG") != operator || os.Getenv("WALLABY_TEST_K8S_KUBECONFIG") != operator {
				t.Fatal("operator kubeconfig was not restored to both variables")
			}
			if err := peer.stop(); err != nil {
				t.Fatal(err)
			}
			reader, err := openHarnessRoot(dir, 2)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reader.close() }()
			after, err := reader.readState()
			if err != nil || after.Participants != 1 || !after.Active {
				t.Fatalf("peer stop did not restore the owner-only count: %+v %v", after, err)
			}
			requireSentinel(t, operator, "operator kubeconfig")
		})
	}
}

func TestRestoreManagedEnvAcceptsOperatorAndGeneratedPathsOnly(t *testing.T) {
	isolateHarnessEnv(t)
	root := openTestRoot(t)
	operator := filepath.Join(t.TempDir(), "operator-kubeconfig")
	t.Setenv("WALLABY_TEST_K8S_KUBECONFIG", operator)
	h := newTestHarness(t, root, 2)
	file, name, err := root.createGeneratedKubeconfig()
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	generated := root.generatedPath(name)
	dsn := "postgres://user:pass@127.0.0.1:5432/app"
	devNull := "/dev/null"
	state := &integrationHarnessSharedState{
		KindKubeFile: name,
		Env: map[string]*string{
			"KUBECONFIG":                  &generated,
			"WALLABY_TEST_K8S_KUBECONFIG": &operator,
			"TEST_PG_DSN":                 &dsn,
			"AWS_CONFIG_FILE":             &devNull,
		},
	}
	if err := h.restoreManagedEnvFromState(state); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("KUBECONFIG") != generated || os.Getenv("WALLABY_TEST_K8S_KUBECONFIG") != operator || os.Getenv("TEST_PG_DSN") != dsn {
		t.Fatal("valid shared environment was not restored")
	}
}

func TestCleanupNeverRemovesOperatorOutOfRootOrSymlinkKubeconfigs(t *testing.T) {
	isolateHarnessEnv(t)
	root := openTestRoot(t)
	operator := writeSentinel(t, "developer kubeconfig")
	linked := "wallaby-it-kube-" + "b" + "0123456789abcdef0123456789abcde" + ".kubeconfig"
	if err := os.Symlink(operator, root.generatedPath(linked)); err != nil {
		t.Fatal(err)
	}
	for name, kubeFile := range map[string]string{
		"absolute operator path": operator,
		"traversal":              "../" + filepath.Base(operator),
		"in-root symlink":        linked,
		"state file":             harnessStateFileName,
	} {
		t.Run(name, func(t *testing.T) {
			h := newTestHarness(t, root, 1)
			h.kindKubePath = operator
			h.cleanupSharedInfrastructure(&integrationHarnessSharedState{KindKubeFile: kubeFile})
			requireSentinel(t, operator, "developer kubeconfig")
		})
	}
	if _, err := os.Lstat(root.generatedPath(linked)); err != nil {
		t.Fatalf("symlink should have been refused, not removed: %v", err)
	}
}

func TestCleanupRemovesOnlyHarnessGeneratedKubeconfigAndKeepsOperatorKubeconfig(t *testing.T) {
	isolateHarnessEnv(t)
	root := openTestRoot(t)
	operator := writeSentinel(t, "developer kubeconfig")
	file, name, err := root.createGeneratedKubeconfig()
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	h := newTestHarness(t, root, 1)
	h.kindKubePath = operator
	h.cleanupSharedInfrastructure(&integrationHarnessSharedState{KindKubeFile: name})
	if _, err := os.Lstat(root.generatedPath(name)); !os.IsNotExist(err) {
		t.Fatalf("generated kubeconfig retained: %v", err)
	}
	requireSentinel(t, operator, "developer kubeconfig")
}

func TestGeneratedClickHouseCAIsRemovedWithItsService(t *testing.T) {
	isolateHarnessEnv(t)
	root := openTestRoot(t)
	file, name, err := root.createGeneratedCA()
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	h := newTestHarness(t, root, 1)
	h.getService("ca-owner").cleanupFiles = []string{name, "/etc/passwd"}
	h.stopManagedService("ca-owner")
	if _, err := os.Lstat(root.generatedPath(name)); !os.IsNotExist(err) {
		t.Fatalf("generated CA retained: %v", err)
	}
}

func TestFailedOwnedStartDoesNotMarkExistingClusterAsCreated(t *testing.T) {
	isolateHarnessEnv(t)
	root := openTestRoot(t)
	h := newTestHarness(t, root, 1)
	if err := h.acquireGlobalLock(); err != nil {
		t.Fatal(err)
	}
	state := &integrationHarnessSharedState{Version: 1, Active: true, KindCluster: "unit-cluster", Participants: 1}
	if err := h.failOwnedStart(state, os.ErrInvalid); err != os.ErrInvalid {
		t.Fatalf("failOwnedStart = %v", err)
	}
	persisted, err := root.readState()
	if err != nil {
		t.Fatal(err)
	}
	if persisted.KindCreated || persisted.KindKubeFile != "" {
		t.Fatalf("pre-existing cluster recorded as owned: %+v", persisted)
	}
}

func TestDefaultKindClusterNameIsInvocationScoped(t *testing.T) {
	isolateHarnessEnv(t)
	startName := func(dir string, explicit string) string {
		t.Helper()
		t.Setenv(stateDirEnv, dir)
		h := newTestHarness(t, nil, 2)
		h.config.kindCluster = explicit
		if err := h.start(); err != nil {
			t.Fatal(err)
		}
		// Release the root; the runner-provided directory itself is left alone.
		t.Cleanup(func() {
			if err := h.stop(); err != nil {
				t.Errorf("stop: %v", err)
			}
		})
		return h.config.kindCluster
	}
	first, second := privateTestDir(t), privateTestDir(t)
	a1 := startName(first, "")
	a2 := startName(first, "") // second peer of the same invocation joins the first
	b := startName(second, "")
	if a1 != a2 {
		t.Fatalf("peers of one invocation derived different names: %q %q", a1, a2)
	}
	if a1 == b {
		t.Fatalf("distinct invocations share cluster %q", a1)
	}
	for _, name := range []string{a1, b} {
		if name == "wallaby-test" || !strings.HasPrefix(name, "wallaby-it-") || name != strings.ToLower(name) || len(name) > 40 {
			t.Fatalf("unsuitable derived name %q", name)
		}
	}
	if got := startName(privateTestDir(t), "developer-cluster"); got != "developer-cluster" {
		t.Fatalf("explicit cluster replaced by %q", got)
	}
}

func TestDerivedKindClusterNameIsStableForSameRootAndDiffersAcrossRoots(t *testing.T) {
	one, two := openTestRoot(t), openTestRoot(t)
	first, err := one.derivedKindClusterName()
	if err != nil {
		t.Fatal(err)
	}
	again, err := one.derivedKindClusterName()
	if err != nil || again != first {
		t.Fatalf("name unstable: %q %q %v", first, again, err)
	}
	other, err := two.derivedKindClusterName()
	if err != nil || other == first {
		t.Fatalf("roots share a name: %q %q %v", first, other, err)
	}
	link := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(one.path, link); err != nil {
		t.Fatal(err)
	}
	aliased := &harnessRoot{path: link}
	if viaAlias, err := aliased.derivedKindClusterName(); err != nil || viaAlias != first {
		t.Fatalf("canonical path not used: %q %v", viaAlias, err)
	}
}

func startTestParticipant(t *testing.T, dir string, peers int) *integrationHarness {
	t.Helper()
	t.Setenv(stateDirEnv, dir)
	h := newTestHarness(t, nil, peers)
	h.config.kindCluster = "unit-cluster"
	h.peerWait = peerWaitTiming{timeout: 20 * time.Second, interval: 5 * time.Millisecond}
	if err := h.start(); err != nil {
		t.Fatal(err)
	}
	return h
}

func requireStillWaiting(t *testing.T, done <-chan error, why string) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("owner stopped waiting %s: %v", why, err)
	case <-time.After(150 * time.Millisecond):
	}
}

// The owner process hosts the port-forwards peers use. A real listener stands
// in for that forward: it must stay reachable until the last peer finishes,
// and must be released only afterwards.
func TestOwnerOutlivesPeersThatJoinLateAndFinishLast(t *testing.T) {
	isolateHarnessEnv(t)
	dir := privateTestDir(t)
	owner := startTestParticipant(t, dir, 2)
	if !owner.ownsInfra {
		t.Fatal("first participant did not claim ownership")
	}
	forward, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = forward.Close() }()
	go func() {
		for {
			conn, err := forward.Accept()
			if err != nil {
				return
			}
			_, _ = conn.Write([]byte("ok"))
			_ = conn.Close()
		}
	}()
	reachable := func() bool {
		conn, err := net.DialTimeout("tcp", forward.Addr().String(), time.Second)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}

	// The owner's own tests are done; the barrier must hold it while the peer
	// has not started at all.
	done := make(chan error, 1)
	go func() {
		err := owner.waitForPeers(context.Background())
		_ = forward.Close() // the owner process exiting ends its port-forwards
		done <- err
	}()
	requireStillWaiting(t, done, "before the peer joined")
	if !reachable() {
		t.Fatal("port-forward stand-in unreachable before the peer joined")
	}

	// Joining needs the shared lock, proving the barrier does not hold it.
	peer := startTestParticipant(t, dir, 2)
	if peer.ownsInfra {
		t.Fatal("second participant claimed ownership")
	}
	requireStillWaiting(t, done, "while the peer is active")
	if !reachable() {
		t.Fatal("port-forward stand-in lost while the peer is active")
	}
	if err := peer.stop(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("barrier failed after the peer finished: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("owner did not leave the barrier after the peer finished")
	}
	if reachable() {
		t.Fatal("port-forward stand-in outlived the owner")
	}
	if err := owner.stop(); err != nil {
		t.Fatal(err)
	}
}

func TestOwnerDoesNotWaitForPeersThatAlreadyJoinedAndFinished(t *testing.T) {
	isolateHarnessEnv(t)
	dir := privateTestDir(t)
	owner := startTestParticipant(t, dir, 2)
	peer := startTestParticipant(t, dir, 2)
	if err := peer.stop(); err != nil {
		t.Fatal(err)
	}
	// Participants is back to 1 but two packages joined in total.
	done := make(chan error, 1)
	go func() { done <- owner.waitForPeers(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("owner waited for a peer that had already joined and finished")
	}
	if err := owner.stop(); err != nil {
		t.Fatal(err)
	}
}

func TestOwnerBarrierTimesOutWhenAPeerNeverJoins(t *testing.T) {
	isolateHarnessEnv(t)
	owner := startTestParticipant(t, privateTestDir(t), 2)
	owner.peerWait = peerWaitTiming{timeout: 100 * time.Millisecond, interval: 5 * time.Millisecond}
	start := time.Now()
	err := owner.waitForPeers(context.Background())
	if err == nil || !strings.Contains(err.Error(), "1 of 2 participants joined") {
		t.Fatalf("barrier error = %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("barrier ignored its timeout")
	}
	if err := owner.stop(); err != nil {
		t.Fatalf("cleanup after a failed barrier: %v", err)
	}
}

func TestOwnerBarrierIsBrokenByCancellationAndDoesNotBlockStop(t *testing.T) {
	isolateHarnessEnv(t)
	dir := privateTestDir(t)
	owner := startTestParticipant(t, dir, 2)
	peer := startTestParticipant(t, dir, 2) // stays active, so the owner would wait
	// A long poll interval means only the cancellation itself can end the wait.
	owner.peerWait = peerWaitTiming{timeout: time.Hour, interval: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- owner.waitForPeers(ctx) }()
	requireStillWaiting(t, done, "while the peer is active")
	cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- owner.stop() }() // the signal path's cleanup
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("barrier error = %v, want cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not break the barrier")
	}
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cleanup deadlocked behind the barrier")
	}
	if err := peer.stop(); err != nil {
		t.Fatal(err)
	}
}

func TestOwnerBarrierRejectsReplacedState(t *testing.T) {
	isolateHarnessEnv(t)
	dir := privateTestDir(t)
	owner := startTestParticipant(t, dir, 2)
	if err := os.Remove(filepath.Join(dir, harnessStateFileName)); err != nil {
		t.Fatal(err)
	}
	if err := owner.waitForPeers(context.Background()); err == nil {
		t.Fatal("barrier accepted missing owner state")
	}
}

func TestPeerAndSingleParticipantNeverWait(t *testing.T) {
	isolateHarnessEnv(t)
	dir := privateTestDir(t)
	owner := startTestParticipant(t, dir, 2)
	peer := startTestParticipant(t, dir, 2)
	if err := peer.waitForPeers(context.Background()); err != nil {
		t.Fatalf("non-owner waited: %v", err)
	}
	if err := peer.stop(); err != nil {
		t.Fatal(err)
	}
	if err := owner.stop(); err != nil {
		t.Fatal(err)
	}
	single := startTestParticipant(t, privateTestDir(t), 1)
	if err := single.waitForPeers(context.Background()); err != nil {
		t.Fatalf("single participant waited: %v", err)
	}
	if err := single.stop(); err != nil {
		t.Fatal(err)
	}
}

func TestPeerJoinsValidStateRegardlessOfTimestampAge(t *testing.T) {
	isolateHarnessEnv(t)
	dir := privateTestDir(t)
	dsn := "postgres://user:pass@db.example.test:5432/app"
	seed, err := openHarnessRoot(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-3 * time.Hour)
	if err := seed.writeState(&integrationHarnessSharedState{
		Version: 1, Active: true, KindCluster: "unit-cluster", Participants: 1, JoinedTotal: 1,
		ExpectedParticipants: 2, UpdatedAt: old, Env: map[string]*string{"TEST_PG_DSN": &dsn},
	}); err != nil {
		t.Fatal(err)
	}
	if err := seed.close(); err != nil {
		t.Fatal(err)
	}
	peer := startTestParticipant(t, dir, 2)
	if peer.ownsInfra || !peer.joined {
		t.Fatalf("late peer did not join the existing owner: owns=%v joined=%v", peer.ownsInfra, peer.joined)
	}
	reader, err := openHarnessRoot(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.close() }()
	state, err := reader.readState()
	if err != nil || state.Participants != 2 || state.JoinedTotal != 2 {
		t.Fatalf("state after late join = %+v, %v", state, err)
	}
	if err := peer.stop(); err != nil {
		t.Fatal(err)
	}
}

func TestParseTestTimeout(t *testing.T) {
	for name, test := range map[string]struct {
		args []string
		want time.Duration
	}{
		"go test default":  {args: []string{"-test.timeout=10m0s"}, want: 10 * time.Minute},
		"explicit":         {args: []string{"-test.timeout=45m"}, want: 45 * time.Minute},
		"disabled":         {args: []string{"-test.timeout=0"}, want: 0},
		"flag not defined": {want: -1},
	} {
		t.Run(name, func(t *testing.T) {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			if test.want >= 0 {
				fs.Duration("test.timeout", 0, "")
			}
			if err := fs.Parse(test.args); err != nil {
				t.Fatal(err)
			}
			want := test.want
			if want < 0 {
				want = 0
			}
			if got := parseTestTimeout(fs); got != want {
				t.Fatalf("parseTestTimeout = %s, want %s", got, want)
			}
		})
	}
}

func TestPeerBarrierBudgetStaysInsideTheTestTimeout(t *testing.T) {
	start := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	for name, test := range map[string]struct {
		timeout time.Duration
		elapsed time.Duration
		want    time.Duration
	}{
		"no timeout uses the finite fallback": {timeout: 0, elapsed: time.Hour, want: defaultOwnerPeerWaitTimeout},
		"default 10m after 3m":                {timeout: 10 * time.Minute, elapsed: 3 * time.Minute, want: 5 * time.Minute},
		"long timeout is not capped":          {timeout: 2 * time.Hour, elapsed: 10 * time.Minute, want: 108 * time.Minute},
		"short timeout scales the reserve":    {timeout: 40 * time.Second, elapsed: 0, want: 30 * time.Second},
		"exhausted":                           {timeout: 10 * time.Minute, elapsed: 9 * time.Minute, want: -time.Minute},
	} {
		t.Run(name, func(t *testing.T) {
			if got := peerBarrierBudget(test.timeout, start, start.Add(test.elapsed)); got != test.want {
				t.Fatalf("budget = %s, want %s", got, test.want)
			}
		})
	}
}

func TestOwnerBarrierFailsWhenTestTimeoutBudgetIsExhaustedWithoutDeletingUnderActivePeer(t *testing.T) {
	isolateHarnessEnv(t)
	dir := privateTestDir(t)
	owner := startTestParticipant(t, dir, 2)
	peer := startTestParticipant(t, dir, 2) // active for the whole test
	owner.peerWait = peerWaitTiming{interval: 5 * time.Millisecond}
	owner.config.testTimeout = time.Millisecond // far past process start: no budget left
	// The barrier must end on its own budget; this context only bounds a
	// regression that ignores the budget, which then fails with a deadline error.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := owner.waitForPeers(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("barrier ignored the exhausted test-timeout budget: %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "gave up") || !strings.Contains(err.Error(), "2 of 2 participants joined and 2 active") {
		t.Fatalf("barrier error = %v, want the exhausted-budget error", err)
	}
	// The failed owner cleans up as it exits; the active peer keeps the shared
	// infrastructure and state.
	if err := owner.stop(); err != nil {
		t.Fatal(err)
	}
	reader, err := openHarnessRoot(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.close() }()
	state, err := reader.readState()
	if err != nil || !state.Active || state.Participants != 1 {
		t.Fatalf("owner failure disturbed the active peer's state: %+v, %v", state, err)
	}
	if err := peer.stop(); err != nil {
		t.Fatal(err)
	}
}

// The owner records that it created the cluster; a peer joining and leaving
// must not erase that, or the last participant would never delete it. The
// seeded state keeps the last stop out of the test so no cluster is touched.
func TestPeerJoinAndLeavePreserveOwnerKindCreated(t *testing.T) {
	isolateHarnessEnv(t)
	dir := privateTestDir(t)
	dsn := "postgres://user:pass@db.example.test:5432/app"
	seed, err := openHarnessRoot(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.writeState(&integrationHarnessSharedState{
		Version: 1, Active: true, KindCluster: "unit-cluster", KindCreated: true,
		Participants: 1, JoinedTotal: 1, ExpectedParticipants: 2, UpdatedAt: time.Now(),
		Env: map[string]*string{"TEST_PG_DSN": &dsn},
	}); err != nil {
		t.Fatal(err)
	}
	if err := seed.close(); err != nil {
		t.Fatal(err)
	}
	readCreated := func(stage string) {
		t.Helper()
		reader, err := openHarnessRoot(dir, 2)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.close() }()
		state, err := reader.readState()
		if err != nil || !state.KindCreated || !state.Active {
			t.Fatalf("owner's KindCreated lost %s: %+v, %v", stage, state, err)
		}
	}
	peer := startTestParticipant(t, dir, 2)
	readCreated("after a peer joined")
	if err := peer.stop(); err != nil {
		t.Fatal(err)
	}
	readCreated("after a peer left")
}
