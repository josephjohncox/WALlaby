package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
	"golang.org/x/mod/modfile"
	"gopkg.in/yaml.v3"
)

type miseTask struct {
	Run any `toml:"run"`
}

type misePolicy struct {
	Tools map[string]string   `toml:"tools"`
	Env   map[string]string   `toml:"env"`
	Tasks map[string]miseTask `toml:"tasks"`
}

func readMisePolicy(t *testing.T) misePolicy {
	t.Helper()
	var config misePolicy
	if _, err := toml.DecodeFile("../mise.toml", &config); err != nil {
		t.Fatal(err)
	}
	return config
}

func readPolicyFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestMiseGoPinMatchesApplicationModulesAndContainer(t *testing.T) {
	t.Parallel()
	pin := readMisePolicy(t).Tools["go"]
	if !regexp.MustCompile(`^\d+\.\d+\.\d+$`).MatchString(pin) {
		t.Fatalf("Go toolchain must have an exact patch pin, got %q", pin)
	}
	for _, path := range []string{"../go.mod", "../terraform/provider/go.mod"} {
		file, err := modfile.Parse(path, []byte(readPolicyFile(t, path)), nil)
		if err != nil {
			t.Fatal(err)
		}
		if file.Go == nil || file.Go.Version != pin {
			t.Errorf("%s Go directive must match mise Go %s", path, pin)
		}
	}
	if !strings.Contains(readPolicyFile(t, "../Dockerfile"), "ARG GO_VERSION="+pin+"\n") {
		t.Fatal("Docker build Go version differs from mise.toml")
	}
}

func TestMiseOwnsUVInterpreterSelection(t *testing.T) {
	t.Parallel()
	config := readMisePolicy(t)
	if config.Env["UV_PYTHON"] != config.Tools["python"] || config.Env["UV_PYTHON_DOWNLOADS"] != "never" {
		t.Fatal("uv must use the pinned mise Python, not silently reuse an old .venv or download another interpreter")
	}
}

func taskRunText(t *testing.T, task miseTask) string {
	t.Helper()
	switch value := task.Run.(type) {
	case string:
		return value
	case []any:
		var commands []string
		for _, item := range value {
			command, ok := item.(string)
			if !ok {
				t.Fatalf("unexpected mise run value %T", item)
			}
			commands = append(commands, command)
		}
		return strings.Join(commands, "\n")
	default:
		t.Fatalf("unsupported mise run value %T", task.Run)
		return ""
	}
}

func TestMiseIsTheOnlyTaskRunnerAndReferencesResolve(t *testing.T) {
	t.Parallel()
	config := readMisePolicy(t)
	if _, present := config.Tools["just"]; present {
		t.Fatal("removed task runner remains in the toolchain")
	}
	for _, path := range []string{"../justfile", "../.github/actions/setup-just/action.yml", "../tools/tools.go"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("obsolete task/tool definition remains at %s (err=%v)", path, err)
		}
	}
	for _, name := range []string{"setup", "build", "test", "lint", "format", "format:check", "ci-check-test", "pre-commit", "generate", "security", "deps:check", "test-integration", "test-iggy"} {
		if _, exists := config.Tasks[name]; !exists {
			t.Errorf("missing standard task %s", name)
		}
	}
	calls := regexp.MustCompile(`\bmise run ([a-zA-Z][a-zA-Z0-9_:-]*)`)
	removedRunner := regexp.MustCompile(`\bjust\s`)
	check := func(owner, script string) {
		t.Helper()
		if removedRunner.MatchString(script) {
			t.Errorf("%s still invokes the removed task runner", owner)
		}
		for _, match := range calls.FindAllStringSubmatch(script, -1) {
			if _, exists := config.Tasks[match[1]]; !exists {
				t.Errorf("%s invokes missing mise task %s", owner, match[1])
			}
		}
	}
	for name, task := range config.Tasks {
		check("task "+name, taskRunText(t, task))
	}
	paths, err := filepath.Glob("../.github/workflows/*.yml")
	if err != nil || len(paths) == 0 {
		t.Fatalf("workflow discovery failed: %v", err)
	}
	for _, path := range paths {
		var workflow requiredGateWorkflow
		if err := yaml.Unmarshal([]byte(readPolicyFile(t, path)), &workflow); err != nil {
			t.Fatal(err)
		}
		for name, job := range workflow.Jobs {
			for _, step := range job.Steps {
				check(path+":"+name, step.Run)
				if strings.Contains(step.Uses, "setup-just") || strings.Contains(step.Uses, "actions/setup-go@") {
					t.Errorf("%s:%s bypasses the shared mise toolchain", path, name)
				}
			}
		}
	}
}

func TestIggyServerFixturesMatchTheNativeSDK(t *testing.T) {
	t.Parallel()
	file, err := modfile.Parse("go.mod", []byte(readPolicyFile(t, "../go.mod")), nil)
	if err != nil {
		t.Fatal(err)
	}
	version := ""
	for _, dependency := range file.Require {
		if dependency.Mod.Path == "github.com/apache/iggy/foreign/go" {
			version = strings.TrimPrefix(dependency.Mod.Version, "v")
		}
	}
	if version == "" {
		t.Fatal("native Iggy Go SDK is not a required dependency")
	}
	if !strings.Contains(readPolicyFile(t, "../examples/iggy/docker-compose.yml"), "image: apache/iggy:"+version+"\n") {
		t.Errorf("Iggy fixture server protocol pin differs from SDK %s", version)
	}
	workflow := readPolicyFile(t, "../.github/workflows/ci-iggy.yml")
	for _, required := range []string{"kernel.io_uring_disabled=0", "run: mise run test-live-local", ".cache/live-evidence/"} {
		if !strings.Contains(workflow, required) {
			t.Errorf("native Iggy CI is missing %q", required)
		}
	}
}
