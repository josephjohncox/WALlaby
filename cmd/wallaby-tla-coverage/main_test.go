package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseCoverage(t *testing.T) {
	output := `
Coverage report:
  Start: 1
  ReadBatch: 3
Action Deliver was executed 5 times
Action Ack covered 2 times
Action Resume is never enabled
Action Stop is never executed
`
	path := filepath.Join(t.TempDir(), "CDCFlow.txt")
	if err := writeFile(path, output); err != nil {
		t.Fatalf("write temp coverage: %v", err)
	}

	actions, err := parseCoverage(path)
	if err != nil {
		t.Fatalf("parse coverage: %v", err)
	}

	assertCount(t, actions, "Start", 1)
	assertCount(t, actions, "ReadBatch", 3)
	assertCount(t, actions, "Deliver", 5)
	assertCount(t, actions, "Ack", 2)
	assertCount(t, actions, "Resume", 0)
	assertCount(t, actions, "Stop", 0)
}

func writeFile(path, contents string) error {
	return os.WriteFile(path, []byte(contents), 0o644)
}

func assertCount(t *testing.T, actions map[string]int, key string, want int) {
	t.Helper()
	got, ok := actions[key]
	if !ok {
		t.Fatalf("missing action %s", key)
	}
	if got != want {
		t.Fatalf("action %s count=%d want=%d", key, got, want)
	}
}
