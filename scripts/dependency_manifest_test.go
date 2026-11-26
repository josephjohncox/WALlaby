package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestDependencyManifestCoverageRequiresEveryNonemptyGraph(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		omit   string
		empty  string
		wantOK bool
	}{
		{name: "all manifests", wantOK: true},
		{name: "missing provider", omit: "terraform/provider/go.mod"},
		{name: "missing uv", omit: "uv.lock"},
		{name: "missing root", omit: "go.mod"},
		{name: "empty provider", empty: "terraform/provider/go.mod"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			graphs := map[string]any{}
			for _, manifest := range []string{"go.mod", "terraform/provider/go.mod", "uv.lock"} {
				if manifest == test.omit {
					continue
				}
				graph := map[string]any{"package-id": []string{}}
				if manifest == test.empty {
					graph = map[string]any{}
				}
				graphs[filepath.Join(dir, filepath.FromSlash(manifest))] = map[string]any{"graph": graph}
			}
			data, err := json.Marshal(map[string]any{"dependencyGraphs": graphs})
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "detector.json")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			output, err := exec.Command("python3", "verify-dependency-manifest.py", path, "--root", dir).CombinedOutput()
			if (err == nil) != test.wantOK {
				t.Fatalf("coverage check err=%v output=%s", err, output)
			}
			if !test.wantOK && !strings.Contains(string(output), "missing or empty detected dependency graph") {
				t.Fatalf("wrong negative-control failure: %s", output)
			}
		})
	}
}
