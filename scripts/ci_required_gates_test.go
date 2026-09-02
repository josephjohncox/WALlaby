package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type requiredGateStep struct {
	Run  string `yaml:"run"`
	Uses string `yaml:"uses"`
}

type requiredGateJob struct {
	Env   map[string]any     `yaml:"env"`
	Steps []requiredGateStep `yaml:"steps"`
}

type requiredGateWorkflow struct {
	Jobs map[string]requiredGateJob `yaml:"jobs"`
}

func TestPiLensHelmTemplateExclusionIsNarrowAndRenderValidated(t *testing.T) {
	var cfg struct {
		Ignore []string `json:"ignore"`
		Helm   struct {
			RenderValidation struct {
				Enabled bool `json:"enabled"`
			} `json:"renderValidation"`
		} `json:"helm"`
	}
	raw, err := os.ReadFile("../.pi-lens.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	if len(cfg.Ignore) != 1 || cfg.Ignore[0] != "charts/wallaby/templates/**/*.yaml" || !cfg.Helm.RenderValidation.Enabled {
		t.Fatalf("pi-lens Helm policy=%+v", cfg)
	}
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
	hasRun := func(job requiredGateJob, command string) bool {
		for _, step := range job.Steps {
			if strings.TrimSpace(step.Run) == command {
				return true
			}
		}
		return false
	}
	hasUse := func(job requiredGateJob, action string) bool {
		for _, step := range job.Steps {
			if strings.TrimSpace(step.Uses) == action {
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
	connectorMatrix, connectorMatrixOK := evidence.Jobs["connector-matrix"]
	if !connectorMatrixOK || !hasUse(connectorMatrix, "./.github/actions/setup-just") || !hasRun(connectorMatrix, "just test-snowpipe-streaming-runtime-wiring") {
		t.Fatal("CI Evidence connector-matrix job does not install just before the experimental runtime recipe")
	}
	if !modelOK || !processOK || !hasRun(model, "just test-failure-matrix-model") || hasRun(model, "just test-failure-matrix") || !hasRun(process, "just test-failure-matrix") || hasRun(process, "just test-failure-matrix-model") {
		t.Fatal("CI Evidence does not keep model-only and OS-process recipes in distinct jobs")
	}
	for name, job := range map[string]requiredGateJob{"failure-matrix-model": model, "failure-matrix": process} {
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
	for _, check := range []string{
		"`build`", "`lint`", "`terraform-provider`", "`generated-artifacts`", "`spec`",
		"`integration`", "`checkpoint5-iceberg`", "`postgres-managed-profile-14`",
		"`postgres-managed-profile-15`", "`postgres-managed-profile-16`", "`postgres-managed-profile-17`",
		"`failure-matrix-model`", "`failure-matrix`", "`connector-matrix`", "`benchmark-smoke`",
		"`Analyze (actions)`", "`Analyze (go)`", "`Analyze (python)`", "`CodeQL`",
	} {
		if !strings.Contains(governance, check) {
			t.Fatalf("CI governance does not name required check %s", check)
		}
	}
	for _, policy := range []string{
		"requires a pull request and one approval",
		"reviewer other than the last pusher",
		"permits merge commits only",
		"pull-request-only emergency bypass",
		"enforcement value is `active`",
		"administrator bypass mode is `pull_request`",
	} {
		if !strings.Contains(governance, policy) {
			t.Fatalf("CI governance does not preserve policy text %q", policy)
		}
	}
}
