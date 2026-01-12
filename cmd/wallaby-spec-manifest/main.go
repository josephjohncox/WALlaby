package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/josephjohncox/wallaby/pkg/spec"
)

func main() {
	output := flag.String("out", "specs/coverage.json", "path to write CDCFlow coverage manifest")
	dir := flag.String("dir", "specs", "directory to write all coverage manifests (empty to disable)")
	flag.Parse()

	manifest := spec.TraceSuiteManifest()
	if *output != "" {
		if err := writeManifest(*output, manifest); err != nil {
			fmt.Fprintf(os.Stderr, "write manifest: %v\n", err)
			os.Exit(1)
		}
	}
	if *dir != "" {
		if err := writeAll(*dir); err != nil {
			fmt.Fprintf(os.Stderr, "write manifests: %v\n", err)
			os.Exit(1)
		}
	}
}

func writeAll(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for _, manifest := range spec.AllManifests() {
		path := spec.ManifestPath(dir, manifest.Spec)
		if err := writeManifest(path, manifest); err != nil {
			return err
		}
	}
	return nil
}

func writeManifest(path string, manifest spec.Manifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}
