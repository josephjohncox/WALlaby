package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/josephjohncox/wallaby/internal/cli"
	"github.com/josephjohncox/wallaby/pkg/spec"
	"github.com/spf13/cobra"
)

type specFiles struct {
	Name spec.SpecName
	TLA  string
	CFG  string
}

func main() {
	command := newWallabySpecSyncCommand()
	if err := command.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type specSyncOptions struct {
	specDir     string
	manifestDir string
}

func newWallabySpecSyncCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "wallaby-spec-sync",
		Short:        "Validate local spec declarations against manifests",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runWallabySpecSync(cmd)
		},
	}
	command.Flags().String("spec-dir", "specs", "directory containing TLA+ specs")
	command.Flags().String("manifest-dir", "specs", "directory containing coverage manifests")
	command.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		return initWallabySpecSyncConfig(cmd)
	}
	command.InitDefaultCompletionCmd()
	return command
}

func initWallabySpecSyncConfig(cmd *cobra.Command) error {
	return cli.InitViperFromCommand(cmd, cli.ViperConfig{
		EnvPrefix: "WALLABY_SPEC_SYNC",
	})
}

func runWallabySpecSync(cmd *cobra.Command) error {
	opts := specSyncOptions{
		specDir:     cli.ResolveStringFlag(cmd, "spec-dir"),
		manifestDir: cli.ResolveStringFlag(cmd, "manifest-dir"),
	}

	files := []specFiles{
		{Name: spec.SpecCDCFlow, TLA: filepath.Join(opts.specDir, "CDCFlow.tla"), CFG: filepath.Join(opts.specDir, "CDCFlow.cfg")},
		{Name: spec.SpecFlowState, TLA: filepath.Join(opts.specDir, "FlowStateMachine.tla"), CFG: filepath.Join(opts.specDir, "FlowStateMachine.cfg")},
		{Name: spec.SpecCDCFlowFanout, TLA: filepath.Join(opts.specDir, "CDCFlowFanout.tla"), CFG: filepath.Join(opts.specDir, "CDCFlowFanout.cfg")},
	}

	var failures []string
	for _, file := range files {
		if err := validateSpec(file, opts.manifestDir); err != nil {
			failures = append(failures, err.Error())
		}
	}

	if len(failures) > 0 {
		for _, failure := range failures {
			fmt.Fprintln(os.Stderr, failure)
		}
		return fmt.Errorf("spec sync failed: %d errors", len(failures))
	}

	return nil
}

func validateSpec(file specFiles, manifestDir string) error {
	manifestPath := spec.ManifestPath(manifestDir, file.Name)
	manifest, err := spec.LoadManifest(manifestPath)
	if err != nil {
		return fmt.Errorf("%s: load manifest: %w", file.Name, err)
	}
	if manifest.Spec != file.Name {
		return fmt.Errorf("%s: manifest spec mismatch (%s)", file.Name, manifest.Spec)
	}

	actions, err := parseNextActions(file.TLA)
	if err != nil {
		return fmt.Errorf("%s: parse actions: %w", file.Name, err)
	}
	invariants, err := parseCfgInvariants(file.CFG)
	if err != nil {
		return fmt.Errorf("%s: parse invariants: %w", file.Name, err)
	}

	manifestActions := toStringSetActions(manifest.Actions)
	manifestInvariants := toStringSetInvariants(manifest.Invariants)

	if diff := diffSet(actions, manifestActions); len(diff) > 0 {
		return fmt.Errorf("%s: missing manifest actions: %s", file.Name, strings.Join(diff, ", "))
	}
	if diff := diffSet(manifestActions, actions); len(diff) > 0 {
		return fmt.Errorf("%s: extra manifest actions not in spec: %s", file.Name, strings.Join(diff, ", "))
	}

	if diff := diffSet(invariants, manifestInvariants); len(diff) > 0 {
		return fmt.Errorf("%s: missing manifest invariants: %s", file.Name, strings.Join(diff, ", "))
	}
	if diff := diffSet(manifestInvariants, invariants); len(diff) > 0 {
		return fmt.Errorf("%s: extra manifest invariants not in config: %s", file.Name, strings.Join(diff, ", "))
	}

	return nil
}

func parseNextActions(path string) (map[string]struct{}, error) {
	// #nosec G304 -- spec path is provided via CLI flag.
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close %s: %v\n", path, err)
		}
	}()

	actions := make(map[string]struct{})
	scanner := bufio.NewScanner(file)
	inNext := false
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "Next ==") {
			inNext = true
			continue
		}
		if !inNext {
			continue
		}
		if strings.HasPrefix(trimmed, "Spec ") || strings.HasPrefix(trimmed, "SpecFair") {
			break
		}
		if strings.HasPrefix(trimmed, "\\/") {
			rest := strings.TrimSpace(strings.TrimPrefix(trimmed, "\\/"))
			fields := strings.Fields(rest)
			if len(fields) > 0 {
				actions[fields[0]] = struct{}{}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(actions) == 0 {
		return nil, fmt.Errorf("no actions found in Next == block")
	}
	return actions, nil
}

func parseCfgInvariants(path string) (map[string]struct{}, error) {
	// #nosec G304 -- spec path is provided via CLI flag.
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close %s: %v\n", path, err)
		}
	}()

	invariants := make(map[string]struct{})
	scanner := bufio.NewScanner(file)
	inSection := false
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "INVARIANTS") {
			inSection = true
			continue
		}
		if inSection {
			if strings.HasPrefix(trimmed, "CONSTANTS") || strings.HasPrefix(trimmed, "PROPERTY") || strings.HasPrefix(trimmed, "SPECIFICATION") || strings.HasPrefix(trimmed, "CONSTRAINT") {
				break
			}
			fields := strings.Fields(trimmed)
			if len(fields) > 0 {
				invariants[fields[0]] = struct{}{}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(invariants) == 0 {
		return nil, fmt.Errorf("no invariants found in INVARIANTS section")
	}
	return invariants, nil
}

func diffSet(left, right map[string]struct{}) []string {
	var diff []string
	for item := range left {
		if _, ok := right[item]; !ok {
			diff = append(diff, item)
		}
	}
	sort.Strings(diff)
	return diff
}

func toStringSetActions(items []spec.Action) map[string]struct{} {
	out := make(map[string]struct{}, len(items))
	for _, item := range items {
		out[string(item)] = struct{}{}
	}
	return out
}

func toStringSetInvariants(items []spec.Invariant) map[string]struct{} {
	out := make(map[string]struct{}, len(items))
	for _, item := range items {
		out[string(item)] = struct{}{}
	}
	return out
}
