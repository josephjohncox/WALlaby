package integrationharness

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fakeGoStub stands in for the go tool. It records the private state directory
// the runner exports, proves it is usable while tests run, and lets each test
// choose the exit status or wait for a signal.
const fakeGoStub = `#!/bin/sh
if [ "$1" = "run" ]; then
	exit "${FAKE_VERIFY_STATUS:-0}"
fi
printf '%s\n' "$WALLABY_IT_STATE_DIR" >"$FAKE_RECORD"
printf '%s\n' "$@" >"$FAKE_ARGS"
if [ -n "${FAKE_READ_STDIN:-}" ]; then
	cat >"$FAKE_STDIN"
fi
stat_mode=$(ls -ld "$WALLABY_IT_STATE_DIR" | cut -c1-10)
printf '%s\n' "$stat_mode" >>"$FAKE_RECORD"
printf 'state' >"$WALLABY_IT_STATE_DIR/harness.state"
if [ -n "${FAKE_WAIT_FOR_SIGNAL:-}" ]; then
	trap 'echo got-signal >>"$FAKE_RECORD"; exit 5' TERM INT HUP
	: >"$FAKE_READY"
	while :; do sleep 0.1; done
fi
printf '{"Action":"pass","Test":"TestFake"}\n'
exit "${FAKE_TEST_STATUS:-0}"
`

func integrationScript(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	return filepath.Join(filepath.Dir(source), "..", "..", "scripts", "test-integration.sh")
}

// scriptShells lists the POSIX shells the runner is exercised under; dash and
// bash differ in asynchronous-command stdin and signal handling.
func scriptShells(t *testing.T) []string {
	t.Helper()
	var shells []string
	for _, candidate := range []string{"/bin/sh", "/bin/dash", "/bin/bash", "/usr/bin/bash", "/opt/homebrew/bin/bash"} {
		if _, err := os.Stat(candidate); err != nil {
			continue
		}
		resolved, err := filepath.EvalSymlinks(candidate)
		if err != nil {
			continue
		}
		dup := false
		for _, existing := range shells {
			if existingResolved, _ := filepath.EvalSymlinks(existing); existingResolved == resolved {
				dup = true
			}
		}
		if !dup {
			shells = append(shells, candidate)
		}
	}
	if len(shells) == 0 {
		t.Skip("no POSIX shell found")
	}
	return shells
}

func eachShell(t *testing.T, run func(t *testing.T, shell string)) {
	t.Helper()
	for _, shell := range scriptShells(t) {
		t.Run(filepath.Base(shell)+"-"+strings.ReplaceAll(strings.Trim(filepath.Dir(shell), "/"), "/", "_"), func(t *testing.T) { run(t, shell) })
	}
}

func scriptCommand(t *testing.T, shell string, extraEnv ...string) (*exec.Cmd, string, string) {
	t.Helper()
	dir := t.TempDir()
	stub := filepath.Join(dir, "fake-go")
	if err := os.WriteFile(stub, []byte(fakeGoStub), 0o700); err != nil {
		t.Fatal(err)
	}
	record := filepath.Join(dir, "record")
	tmp := filepath.Join(dir, "tmp")
	if err := os.Mkdir(tmp, 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(shell, integrationScript(t))
	// Drop every inherited integration-task variable so this script test cannot
	// pick up (or overwrite the evidence of) an outer live gate such as
	// IT_REQUIRED_TESTS or IT_RESULTS_FILE.
	cmd.Env = append(scrubbedScriptEnv(os.Environ()),
		"GO="+stub,
		"TMPDIR="+tmp,
		"FAKE_RECORD="+record,
		"FAKE_READY="+filepath.Join(dir, "ready"),
		"FAKE_ARGS="+filepath.Join(dir, "args"),
		"FAKE_STDIN="+filepath.Join(dir, "stdin"),
		"IT_KIND=0",
		"IT_KIND_CLUSTER=",
		"KIND_CLUSTER=",
	)
	cmd.Env = append(cmd.Env, extraEnv...)
	return cmd, record, tmp
}

func scrubbedScriptEnv(environ []string) []string {
	var kept []string
	for _, entry := range environ {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "IT_") || strings.HasPrefix(name, "WALLABY_IT_") ||
			strings.HasPrefix(name, "GO_TEST_") || strings.HasPrefix(name, "KIND_") ||
			name == "INTEGRATION_PACKAGE" || name == "WALLABY_TEST_K8S_KIND" || name == "GO" {
			continue
		}
		kept = append(kept, entry)
	}
	return kept
}

func readRecord(t *testing.T, record string) (stateDir, mode string) {
	t.Helper()
	raw, err := os.ReadFile(record)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) < 2 {
		t.Fatalf("incomplete record %q", raw)
	}
	return lines[0], lines[1]
}

func requireNoLeftovers(t *testing.T, tmp string) {
	t.Helper()
	entries, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("runner left temporary state behind: %v", entries)
	}
}

func TestIntegrationRunnerCreatesPrivateStateDirAndRemovesOnlyThatDirectory(t *testing.T) {
	eachShell(t, func(t *testing.T, shell string) {
		outside := t.TempDir()
		sentinel := filepath.Join(outside, "operator-file")
		if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
		for name, env := range map[string][]string{
			"plain":           nil,
			"json accounting": {"IT_REQUIRED_TESTS=TestFake"},
		} {
			t.Run(name, func(t *testing.T) {
				// A hostile inherited value must be replaced, never trusted or removed.
				cmd, record, tmp := scriptCommand(t, shell, append([]string{stateDirEnv + "=" + outside}, env...)...)
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("runner failed: %v\n%s", err, output)
				}
				stateDir, mode := readRecord(t, record)
				if stateDir == outside || filepath.Dir(stateDir) != mustEval(t, tmp) || !strings.HasPrefix(filepath.Base(stateDir), "wallaby-it.") {
					t.Fatalf("state directory %q is not a fresh directory under %q", stateDir, tmp)
				}
				if mode != "drwx------" {
					t.Fatalf("state directory mode %s, want drwx------", mode)
				}
				if _, err := os.Lstat(stateDir); !os.IsNotExist(err) {
					t.Fatalf("runner left state directory: %v", err)
				}
				requireNoLeftovers(t, tmp)
				if got, err := os.ReadFile(sentinel); err != nil || string(got) != "keep" {
					t.Fatalf("runner touched the inherited directory: %q %v", got, err)
				}
			})
		}
	})
}

func TestIntegrationRunnerUsesDistinctDirectoriesPerInvocation(t *testing.T) {
	eachShell(t, func(t *testing.T, shell string) {
		seen := map[string]bool{}
		for range 3 {
			cmd, record, _ := scriptCommand(t, shell)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("runner failed: %v\n%s", err, output)
			}
			dir, _ := readRecord(t, record)
			if seen[dir] {
				t.Fatalf("state directory %q reused", dir)
			}
			seen[dir] = true
		}
	})
}

func TestIntegrationRunnerRemovesStateDirOnTestAndVerifierFailure(t *testing.T) {
	eachShell(t, func(t *testing.T, shell string) {
		for name, test := range map[string]struct {
			env  []string
			want int
		}{
			"go test failure":    {env: []string{"FAKE_TEST_STATUS=3"}, want: 3},
			"json test failure":  {env: []string{"FAKE_TEST_STATUS=4", "IT_REQUIRED_TESTS=TestFake"}, want: 4},
			"verifier rejection": {env: []string{"FAKE_VERIFY_STATUS=6", "IT_REQUIRED_TESTS=TestFake"}, want: 6},
		} {
			t.Run(name, func(t *testing.T) {
				cmd, record, tmp := scriptCommand(t, shell, test.env...)
				err := cmd.Run()
				exit, ok := err.(*exec.ExitError)
				if !ok || exit.ExitCode() != test.want {
					t.Fatalf("exit = %v, want %d", err, test.want)
				}
				stateDir, _ := readRecord(t, record)
				if _, err := os.Lstat(stateDir); !os.IsNotExist(err) {
					t.Fatalf("state directory retained: %v", err)
				}
				requireNoLeftovers(t, tmp)
			})
		}
	})
}

func TestIntegrationRunnerPassesStdinToGoTest(t *testing.T) {
	eachShell(t, func(t *testing.T, shell string) {
		for name, env := range map[string][]string{"plain": nil, "json accounting": {"IT_REQUIRED_TESTS=TestFake"}} {
			t.Run(name, func(t *testing.T) {
				cmd, _, _ := scriptCommand(t, shell, append([]string{"FAKE_READ_STDIN=1"}, env...)...)
				cmd.Stdin = strings.NewReader("operator input\n")
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("runner failed: %v\n%s", err, output)
				}
				got, err := os.ReadFile(envValue(cmd.Env, "FAKE_STDIN"))
				if err != nil || string(got) != "operator input\n" {
					t.Fatalf("go test did not receive stdin: %q %v", got, err)
				}
			})
		}
	})
}

func TestIntegrationRunnerFallsBackToDevNullWhenStdinIsClosed(t *testing.T) {
	eachShell(t, func(t *testing.T, shell string) {
		for name, env := range map[string][]string{"plain": nil, "json accounting": {"IT_REQUIRED_TESTS=TestFake"}} {
			t.Run(name, func(t *testing.T) {
				cmd, _, tmp := scriptCommand(t, shell, append([]string{"FAKE_READ_STDIN=1"}, env...)...)
				// Start the runner with file descriptor 0 closed, not /dev/null.
				cmd.Args = []string{shell, "-c", `exec 0<&-; exec "$0" "$1"`, shell, integrationScript(t)}
				cmd.Path = shell
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("runner failed with closed stdin: %v\n%s", err, output)
				}
				got, err := os.ReadFile(envValue(cmd.Env, "FAKE_STDIN"))
				if err != nil || len(got) != 0 {
					t.Fatalf("go test stdin was not an empty /dev/null stream: %q %v", got, err)
				}
				requireNoLeftovers(t, tmp)
			})
		}
	})
}

func TestScriptTestsCannotOverwriteAnOuterGateResults(t *testing.T) {
	outer := writeSentinel(t, "outer live-gate JSON evidence")
	t.Setenv("IT_RESULTS_FILE", outer)
	t.Setenv("IT_REQUIRED_TESTS", "TestOuterGate")
	t.Setenv("IT_COUNT", "7")
	t.Setenv("IT_RUN_FILTER", "TestOuterOnly")
	t.Setenv("IT_SERVICES", "kafka")
	t.Setenv("IT_PACKAGE_PARALLELISM", "9")
	t.Setenv("GO_TEST_COVERPROFILE", filepath.Join(t.TempDir(), "outer.cover"))
	t.Setenv("INTEGRATION_PACKAGE", "./outer/...")
	eachShell(t, func(t *testing.T, shell string) {
		cmd, record, tmp := scriptCommand(t, shell)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("runner failed: %v\n%s", err, output)
		}
		requireSentinel(t, outer, "outer live-gate JSON evidence")
		raw, err := os.ReadFile(envValue(cmd.Env, "FAKE_ARGS"))
		if err != nil {
			t.Fatal(err)
		}
		for _, leaked := range []string{"outer", "TestOuterOnly", "-count=7", "kafka", "-coverprofile"} {
			if strings.Contains(string(raw), leaked) {
				t.Fatalf("outer gate configuration leaked into the script test: %s", raw)
			}
		}
		readRecord(t, record)
		requireNoLeftovers(t, tmp)
	})
}

func TestIntegrationRunnerExpectedParticipantsDefaultsAndValidation(t *testing.T) {
	eachShell(t, func(t *testing.T, shell string) {
		for name, test := range map[string]struct {
			selector    string
			parallelism string
			explicit    string
			want        string // expected participants; empty means the runner must refuse
			wantError   string
		}{
			"all tests, -p 2":             {selector: "./tests/...", parallelism: "2", want: "2"},
			"all tests, -p 1":             {selector: "./tests/...", parallelism: "1", want: "1"},
			"all tests, -p 8 is capped":   {selector: "./tests/...", parallelism: "8", want: "2"},
			"root package":                {selector: "./tests", parallelism: "4", want: "1"},
			"root package trailing slash": {selector: "./tests/", parallelism: "4", want: "1"},
			"integration package":         {selector: "./tests/integration", parallelism: "4", want: "1"},
			"integration trailing slash":  {selector: "./tests/integration/", parallelism: "4", want: "1"},
			"integration recursive":       {selector: "./tests/integration/...", parallelism: "4", want: "1"},
			"unknown selector, -p 1":      {selector: "./internal/...", parallelism: "1", want: "1"},
			"unknown selector, -p 4":      {selector: "./internal/...", parallelism: "4", wantError: "set IT_EXPECTED_HARNESS_PARTICIPANTS explicitly"},
			"unknown selector with -run":  {selector: "./tests/other", parallelism: "2", wantError: "cannot infer the harness package count"},
			"explicit within bounds":      {selector: "./tests/...", parallelism: "4", explicit: "2", want: "2"},
			"explicit one":                {selector: "./tests/...", parallelism: "4", explicit: "1", want: "1"},
			"explicit unknown selector":   {selector: "./internal/...", parallelism: "3", explicit: "3", want: "3"},
			"explicit above known count":  {selector: "./tests/...", parallelism: "4", explicit: "3", wantError: "exceeds the 2 harness package"},
			"explicit above single pkg":   {selector: "./tests", parallelism: "4", explicit: "2", wantError: "exceeds the 1 harness package"},
			"explicit above parallelism":  {selector: "./tests/...", parallelism: "1", explicit: "2", wantError: "exceeds IT_PACKAGE_PARALLELISM=1"},
			"explicit zero":               {selector: "./tests/...", parallelism: "2", explicit: "0", wantError: "must be a positive integer"},
			"explicit negative":           {selector: "./tests/...", parallelism: "2", explicit: "-1", wantError: "must be a positive integer"},
			"explicit text":               {selector: "./tests/...", parallelism: "2", explicit: "two", wantError: "must be a positive integer"},
			"parallelism zero":            {selector: "./tests/...", parallelism: "0", wantError: "IT_PACKAGE_PARALLELISM must be a positive integer"},
			"parallelism text":            {selector: "./tests/...", parallelism: "many", wantError: "IT_PACKAGE_PARALLELISM must be a positive integer"},
		} {
			t.Run(name, func(t *testing.T) {
				cmd, _, tmp := scriptCommand(t, shell,
					"INTEGRATION_PACKAGE="+test.selector,
					"IT_PACKAGE_PARALLELISM="+test.parallelism,
					"IT_EXPECTED_HARNESS_PARTICIPANTS="+test.explicit,
				)
				output, err := cmd.CombinedOutput()
				if test.wantError != "" {
					exit, ok := err.(*exec.ExitError)
					if !ok || exit.ExitCode() != 2 || !strings.Contains(string(output), test.wantError) {
						t.Fatalf("runner = %v %q, want exit 2 containing %q", err, output, test.wantError)
					}
					if _, statErr := os.Stat(envValue(cmd.Env, "FAKE_ARGS")); !os.IsNotExist(statErr) {
						t.Fatal("go test ran despite an invalid configuration")
					}
					requireNoLeftovers(t, tmp)
					return
				}
				if err != nil {
					t.Fatalf("runner failed: %v\n%s", err, output)
				}
				raw, err := os.ReadFile(envValue(cmd.Env, "FAKE_ARGS"))
				if err != nil {
					t.Fatal(err)
				}
				want := "\n-it-expected-harness-participants=" + test.want + "\n"
				if !strings.Contains("\n"+string(raw), want) {
					t.Fatalf("expected participants %s not passed: %s", test.want, raw)
				}
			})
		}
	})
}

func TestIntegrationRunnerPassesExplicitGoTestTimeoutOnly(t *testing.T) {
	eachShell(t, func(t *testing.T, shell string) {
		for name, test := range map[string]struct {
			env  []string
			want string
		}{
			"empty keeps the go default": {env: []string{"GO_TEST_TIMEOUT="}},
			"explicit":                   {env: []string{"GO_TEST_TIMEOUT=45m"}, want: "-timeout=45m"},
		} {
			t.Run(name, func(t *testing.T) {
				cmd, _, _ := scriptCommand(t, shell, test.env...)
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("runner failed: %v\n%s", err, output)
				}
				raw, err := os.ReadFile(envValue(cmd.Env, "FAKE_ARGS"))
				if err != nil {
					t.Fatal(err)
				}
				args := "\n" + string(raw)
				if test.want == "" && strings.Contains(args, "-timeout") {
					t.Fatalf("runner invented a timeout: %s", raw)
				}
				if test.want != "" && !strings.Contains(args, "\n"+test.want+"\n") {
					t.Fatalf("explicit timeout not passed: %s", raw)
				}
			})
		}
	})
}

func TestIntegrationRunnerKindClusterFlagOnlyForExplicitNames(t *testing.T) {
	eachShell(t, func(t *testing.T, shell string) {
		for name, test := range map[string]struct {
			env  []string
			want string
		}{
			"default derives per invocation": {},
			"IT_KIND_CLUSTER":                {env: []string{"IT_KIND_CLUSTER=dev-cluster"}, want: "-it-k8s-kind-cluster=dev-cluster"},
			"KIND_CLUSTER alias":             {env: []string{"KIND_CLUSTER=alias-cluster"}, want: "-it-k8s-kind-cluster=alias-cluster"},
		} {
			t.Run(name, func(t *testing.T) {
				cmd, _, _ := scriptCommand(t, shell, test.env...)
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("runner failed: %v\n%s", err, output)
				}
				raw, err := os.ReadFile(envValue(cmd.Env, "FAKE_ARGS"))
				if err != nil {
					t.Fatal(err)
				}
				args := string(raw)
				if strings.Contains(args, "wallaby-test") {
					t.Fatalf("runner substituted the static default cluster: %s", args)
				}
				if test.want == "" && strings.Contains(args, "-it-k8s-kind-cluster") {
					t.Fatalf("runner passed a cluster flag without an explicit name: %s", args)
				}
				if test.want != "" && !strings.Contains(args, "\n"+test.want+"\n") {
					t.Fatalf("explicit cluster not passed: %s", args)
				}
			})
		}
	})
}

func TestIntegrationRunnerForwardsTerminationAndRemovesStateDir(t *testing.T) {
	eachShell(t, func(t *testing.T, shell string) {
		cmd, record, tmp := scriptCommand(t, shell, "FAKE_WAIT_FOR_SIGNAL=1")
		var output bytes.Buffer
		cmd.Stdout, cmd.Stderr = &output, &output
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		ready := filepath.Join(filepath.Dir(record), "ready")
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := os.Stat(ready); err == nil {
				break
			}
			if time.Now().After(deadline) {
				_ = cmd.Process.Kill()
				t.Fatalf("fake go test did not start: %s", output.String())
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case err := <-done:
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != 5 {
				t.Fatalf("exit = %v, want child status 5; output %s", err, output.String())
			}
		case <-time.After(15 * time.Second):
			_ = cmd.Process.Kill()
			t.Fatalf("runner did not terminate after SIGTERM (pid %d)", cmd.Process.Pid)
		}
		raw, _ := os.ReadFile(record)
		if !strings.Contains(string(raw), "got-signal") {
			t.Fatalf("signal was not forwarded to the Go test process: %q", raw)
		}
		stateDir, _ := readRecord(t, record)
		if _, err := os.Lstat(stateDir); !os.IsNotExist(err) {
			t.Fatalf("state directory retained after signal: %v", err)
		}
		requireNoLeftovers(t, tmp)
	})
}

func envValue(env []string, key string) string {
	value := ""
	for _, entry := range env {
		if rest, ok := strings.CutPrefix(entry, key+"="); ok {
			value = rest
		}
	}
	return value
}

func mustEval(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

// harnessMainPackages returns the directories under tests whose TestMain
// calls integrationharness.RunIntegrationHarness.
func harnessMainPackages(t *testing.T, testsDir string) []string {
	t.Helper()
	var dirs []string
	err := filepath.WalkDir(testsDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Name.Name != "TestMain" || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "RunIntegrationHarness" {
					rel, relErr := filepath.Rel(testsDir, filepath.Dir(path))
					if relErr == nil {
						dirs = append(dirs, filepath.ToSlash(rel))
					}
					return false
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return dirs
}

// The runner's expected-participant defaults hard-code how many test packages
// run the harness. Adding or removing a harness package must update both.
func TestRunnerHarnessPackageCountMatchesTestMainCallers(t *testing.T) {
	script, err := os.ReadFile(integrationScript(t))
	if err != nil {
		t.Fatal(err)
	}
	testsDir := filepath.Join(filepath.Dir(integrationScript(t)), "..", "tests")
	dirs := harnessMainPackages(t, testsDir)
	slices.Sort(dirs)
	if want := []string{".", "integration"}; !slices.Equal(dirs, want) {
		t.Fatalf("harness TestMain packages under tests = %v, want %v; update scripts/test-integration.sh harness_packages_for", dirs, want)
	}
	counts := map[string]string{}
	for _, line := range strings.Split(string(script), "\n") {
		line = strings.TrimSpace(line)
		patterns, rest, ok := strings.Cut(line, ") echo ")
		if !ok || !strings.HasSuffix(rest, " ;;") || strings.HasPrefix(patterns, "*") {
			continue
		}
		for _, pattern := range strings.Split(patterns, " | ") {
			counts[pattern] = strings.TrimSuffix(rest, " ;;")
		}
	}
	for selector, want := range map[string]int{
		"tests/...": len(dirs), "tests": 1, "tests/integration": 1, "tests/integration/...": 1,
	} {
		if counts[selector] != fmt.Sprint(want) {
			t.Fatalf("runner count for %s is %q, want %d (runner table %v)", selector, counts[selector], want, counts)
		}
	}
}
