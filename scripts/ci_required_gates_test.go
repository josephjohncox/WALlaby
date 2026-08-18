package main

import (
	"os"
	"strings"
	"testing"
)

func TestRequiredCheckpoint5AndModelOnlyCIGatesStayWired(t *testing.T) {
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	integration := read("../.github/workflows/ci-integration.yml")
	for _, required := range []string{
		"checkpoint5-iceberg:",
		"name: checkpoint5-iceberg",
		"run: just test-checkpoint5-iceberg-integration",
	} {
		if !strings.Contains(integration, required) {
			t.Fatalf("CI Integration is missing required checkpoint-5 wiring %q", required)
		}
	}
	evidence := read("../.github/workflows/ci-evidence.yml")
	for _, required := range []string{
		"failure-matrix-model:",
		"run: just test-failure-matrix-model",
		"run: just test-failure-matrix",
	} {
		if !strings.Contains(evidence, required) {
			t.Fatalf("CI Evidence is missing required model/OS-process separation %q", required)
		}
	}
	justfile := read("../justfile")
	for _, required := range []string{
		"test-checkpoint5-iceberg-integration:",
		"TestIcebergRESTLiveAppendProjection,TestIcebergRESTLiveSchemaEvolutionRename",
		"IT_REQUIRED_TESTS=\"${required}\"",
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
