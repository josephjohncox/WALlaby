package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/josephjohncox/wallaby/pkg/spec"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	command := newWallabySpecManifestCommand()
	return command.Execute()
}

type specManifestOptions struct {
	out string
	dir string
}

func newWallabySpecManifestCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "wallaby-spec-manifest",
		Short:        "Generate local spec coverage manifests",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runWallabySpecManifest(cmd)
		},
	}
	command.Flags().String("out", "specs/coverage.json", "path to write CDCFlow coverage manifest")
	command.Flags().String("dir", "specs", "directory to write all coverage manifests (empty to disable)")
	command.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		return initWallabySpecManifestConfig(cmd)
	}
	command.InitDefaultCompletionCmd()
	return command
}

func initWallabySpecManifestConfig(_ *cobra.Command) error {
	viper.Reset()
	viper.SetEnvPrefix("WALLABY_SPEC_MANIFEST")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	return nil
}

func runWallabySpecManifest(cmd *cobra.Command) error {
	opts := specManifestOptions{
		out: resolveStringFlag(cmd, "out"),
		dir: resolveStringFlag(cmd, "dir"),
	}
	manifest := spec.TraceSuiteManifest()
	if opts.out != "" {
		if err := writeManifest(opts.out, manifest); err != nil {
			fmt.Fprintf(os.Stderr, "write manifest: %v\n", err)
			return err
		}
	}
	if opts.dir != "" {
		if err := writeAll(opts.dir); err != nil {
			fmt.Fprintf(os.Stderr, "write manifests: %v\n", err)
			return err
		}
	}
	return nil
}

func resolveStringFlag(cmd *cobra.Command, key string) string {
	value, err := cmd.Flags().GetString(key)
	if err != nil {
		return ""
	}
	if f := cmd.Flags().Lookup(key); f == nil || (!f.Changed && viper.IsSet(key)) {
		return viper.GetString(key)
	}
	return value
}

func writeAll(dir string) error {
	// #nosec G301 -- manifest directory intended to be readable by all users.
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
	// #nosec G306 -- manifest is intended to be readable by all users.
	return os.WriteFile(path, data, 0o644)
}
