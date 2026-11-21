package main

import (
	"crypto/sha256"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSpecVerifyDetectsDriftWithoutRewritingAnyManifest(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	binaries := t.TempDir()
	for _, name := range []string{"wallaby-spec-manifest", "wallaby-spec-sync"} {
		command := exec.Command("go", "build", "-mod=readonly", "-o", filepath.Join(binaries, name), "./cmd/"+name)
		command.Dir = root
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", name, err, output)
		}
	}
	for _, changed := range []string{"", "coverage.json", "coverage.lifecycle_generation.json"} {
		t.Run("drift="+changed, func(t *testing.T) {
			fixture := t.TempDir()
			for _, dir := range []string{"specs", "scripts"} {
				if err := os.MkdirAll(filepath.Join(fixture, dir), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			files, err := filepath.Glob(filepath.Join(root, "specs", "*"))
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range files {
				ext := filepath.Ext(path)
				if ext != ".json" && ext != ".cfg" && ext != ".tla" {
					continue
				}
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(fixture, "specs", filepath.Base(path)), data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			script, err := os.ReadFile("spec-verify.sh")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(fixture, "scripts", "spec-verify.sh"), script, 0o700); err != nil {
				t.Fatal(err)
			}
			wrapper := filepath.Join(fixture, "go-wrapper")
			if err := os.WriteFile(wrapper, []byte("#!/bin/sh\nset -eu\ncase \"$2\" in\n ./cmd/wallaby-spec-manifest) shift 2; exec \"$SPEC_GENERATOR\" \"$@\" ;;\n ./cmd/wallaby-spec-sync) shift 2; exec \"$SPEC_SYNC\" \"$@\" ;;\n *) exit 99 ;;\nesac\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			if changed != "" {
				path := filepath.Join(fixture, "specs", changed)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var manifest map[string]any
				if err := json.Unmarshal(data, &manifest); err != nil {
					t.Fatal(err)
				}
				actions := manifest["actions"].([]any)
				manifest["actions"] = actions[1:]
				data, err = json.Marshal(manifest)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			before := specFixtureHashes(t, filepath.Join(fixture, "specs"))
			command := exec.Command("sh", filepath.Join(fixture, "scripts", "spec-verify.sh"))
			command.Env = append(os.Environ(), "CI=false", "GO="+wrapper, "SPEC_GENERATOR="+filepath.Join(binaries, "wallaby-spec-manifest"), "SPEC_SYNC="+filepath.Join(binaries, "wallaby-spec-sync"))
			output, err := command.CombinedOutput()
			if (err != nil) != (changed != "") {
				t.Fatalf("drift=%q err=%v\n%s", changed, err, output)
			}
			if after := specFixtureHashes(t, filepath.Join(fixture, "specs")); !reflect.DeepEqual(before, after) {
				t.Fatal("verification changed the files it was supposed to check")
			}
		})
	}
}

func specFixtureHashes(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string][32]byte, len(files))
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		result[filepath.Base(file)] = sha256.Sum256(data)
	}
	return result
}
