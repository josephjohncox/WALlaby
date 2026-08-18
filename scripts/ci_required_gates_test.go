package main

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type requiredGateWorkflow struct {
	Jobs map[string]struct {
		Env   map[string]any `yaml:"env"`
		Steps []struct {
			Run string `yaml:"run"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
}

func TestRequiredCheckpoint5AndModelOnlyCIGatesStayWired(t *testing.T) {
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	parse := func(path string) requiredGateWorkflow {
		t.Helper()
		var workflow requiredGateWorkflow
		if err := yaml.Unmarshal([]byte(read(path)), &workflow); err != nil {
			t.Fatal(err)
		}
		return workflow
	}
	hasRun := func(job struct {
		Env   map[string]any `yaml:"env"`
		Steps []struct {
			Run string `yaml:"run"`
		} `yaml:"steps"`
	}, command string) bool {
		for _, step := range job.Steps {
			if strings.TrimSpace(step.Run) == command {
				return true
			}
		}
		return false
	}
	integration := parse("../.github/workflows/ci-integration.yml")
	checkpoint, ok := integration.Jobs["checkpoint5-iceberg"]
	if !ok || !hasRun(checkpoint, "just test-checkpoint5-iceberg-integration") {
		t.Fatal("CI Integration lacks the separate checkpoint5-iceberg recipe job")
	}
	evidence := parse("../.github/workflows/ci-evidence.yml")
	model, modelOK := evidence.Jobs["failure-matrix-model"]
	process, processOK := evidence.Jobs["failure-matrix"]
	if !modelOK || !processOK || !hasRun(model, "just test-failure-matrix-model") || hasRun(model, "just test-failure-matrix") || !hasRun(process, "just test-failure-matrix") || hasRun(process, "just test-failure-matrix-model") {
		t.Fatal("CI Evidence does not keep model-only and OS-process recipes in distinct jobs")
	}
	for name, job := range map[string]struct {
		Env   map[string]any `yaml:"env"`
		Steps []struct {
			Run string `yaml:"run"`
		} `yaml:"steps"`
	}{"failure-matrix-model": model, "failure-matrix": process} {
		cycles := fmt.Sprint(job.Env["FAILURE_CYCLES"])
		if !strings.Contains(cycles, "'1000'") || !strings.Contains(cycles, "'100'") || !strings.Contains(cycles, "schedule") {
			t.Fatalf("%s does not preserve explicit 100/1000 cycle bounds: %q", name, cycles)
		}
	}
	justfile := read("../justfile")
	for _, required := range []string{
		"test-checkpoint5-iceberg-integration:",
		"TestIcebergRESTLiveAppendProjection,TestIcebergRESTLiveSchemaEvolutionRename",
		"IT_SERVICES=iceberg IT_REQUIRED_TESTS=\"${required}\"",
		"test-failure-matrix-model:",
		"-model-only -cycles {{ failure_cycles }} -seed {{ failure_seed }} -require-coverage",
	} {
		if !strings.Contains(justfile, required) {
			t.Fatalf("justfile is missing required non-vacuous gate contract %q", required)
		}
	}
	governance := read("../docs/development/ci-governance.md")
	for _, check := range []string{"`checkpoint5-iceberg`", "`failure-matrix-model`", "`failure-matrix`"} {
		if !strings.Contains(governance, check) {
			t.Fatalf("CI governance does not name required check %s", check)
		}
	}
}
