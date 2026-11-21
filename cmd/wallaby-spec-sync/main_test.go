package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/spec"
)

func TestEveryRegisteredModelIsSynchronized(t *testing.T) {
	t.Parallel()
	files := registeredSpecFiles("../../specs")
	if len(files) != 8 || len(files) != len(spec.DefaultManifestFiles) {
		t.Fatalf("registered models=%d, want all eight", len(files))
	}
	for _, file := range files {
		t.Run(string(file.Name), func(t *testing.T) {
			if err := validateSpec(file, "../../specs"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEveryModelRejectsActionAndInvariantDrift(t *testing.T) {
	t.Parallel()
	for _, file := range registeredSpecFiles("../../specs") {
		for _, kind := range []string{"action", "invariant"} {
			t.Run(string(file.Name)+"/"+kind, func(t *testing.T) {
				dir := t.TempDir()
				manifest, err := spec.LoadManifest(filepath.Join("../../specs", spec.DefaultManifestFiles[file.Name]))
				if err != nil {
					t.Fatal(err)
				}
				if kind == "action" {
					manifest.Actions = append(manifest.Actions, spec.Action("UnexpectedReviewAction"))
				} else {
					manifest.Invariants = append(manifest.Invariants, spec.Invariant("UnexpectedReviewInvariant"))
				}
				data, err := json.Marshal(manifest)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, spec.DefaultManifestFiles[file.Name]), data, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := validateSpec(file, dir); err == nil || !strings.Contains(err.Error(), "extra manifest") {
					t.Fatalf("%s drift was not rejected: %v", kind, err)
				}
			})
		}
	}
}

func TestExtractNextActionName(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]string{
		"Start": "Start", `\E w \in Workers: Acquire(w)`: "Acquire",
		`\E w \in Workers, p \in Positions: Prepare(w, p)`: "Prepare",
		`\E no_colon`: "",
	} {
		if got := extractActionName(input); got != want {
			t.Errorf("%q: got %q, want %q", input, got, want)
		}
	}
}
