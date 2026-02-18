package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/josephjohncox/wallaby/internal/cli"
	"github.com/josephjohncox/wallaby/pkg/spec"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"github.com/spf13/cobra"
)

type traceValidateOptions struct {
	input      string
	requireDDL bool
	manifest   string
	noManifest bool
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	command := newWallabyTraceValidateCommand()
	return command.Execute()
}

func newWallabyTraceValidateCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "wallaby-trace-validate",
		Short:        "Validate a wallaby trace against spec manifests",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runWallabyTraceValidate(cmd)
		},
	}
	command.Flags().String("input", "", "path to trace JSONL (defaults to stdin)")
	command.Flags().String("path", "", "alias for --input")
	command.Flags().Bool("require-ddl-approval", false, "require ddl_approved before ddl_applied")
	command.Flags().String("manifest", "", "path to spec coverage manifest (file or directory)")
	command.Flags().Bool("no-manifest", false, "disable spec action validation")
	command.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		return initWallabyTraceValidateConfig(cmd)
	}
	command.InitDefaultCompletionCmd()
	return command
}

func initWallabyTraceValidateConfig(cmd *cobra.Command) error {
	return cli.InitViperFromCommand(cmd, cli.ViperConfig{
		EnvPrefix: "WALLABY_TRACE_VALIDATE",
	})
}

func runWallabyTraceValidate(cmd *cobra.Command) error {
	opts := traceValidateOptions{
		input:      resolveInput(cmd),
		requireDDL: cli.ResolveBoolFlag(cmd, "require-ddl-approval"),
		manifest:   cli.ResolveStringFlag(cmd, "manifest"),
		noManifest: cli.ResolveBoolFlag(cmd, "no-manifest"),
	}

	reader, closer, err := openReader(opts.input)
	if err != nil {
		return err
	}
	if closer != nil {
		defer func() {
			if err := closer(); err != nil {
				fmt.Fprintf(os.Stderr, "close trace input: %v\n", err)
			}
		}()
	}

	events, err := readEvents(reader)
	if err != nil {
		return err
	}

	manifests := make(map[spec.SpecName]spec.Manifest)
	if !opts.noManifest {
		path := opts.manifest
		if path == "" {
			root, err := findModuleRoot()
			if err != nil {
				return fmt.Errorf("resolve manifest: %w (use -manifest or -no-manifest)", err)
			}
			path = filepath.Join(root, "specs")
		}
		loaded, err := spec.LoadManifests(path)
		if err != nil {
			return fmt.Errorf("load manifest: %w (use -manifest or -no-manifest)", err)
		}
		manifests = loaded
	}

	if err := validateBySpec(events, stream.TraceValidationOptions{RequireDDLApproval: opts.requireDDL}, manifests); err != nil {
		return err
	}

	fmt.Printf("Trace OK: %d events\n", len(events))
	return nil
}

func resolveInput(cmd *cobra.Command) string {
	path := cli.ResolveStringFlag(cmd, "input")
	if path == "" {
		path = cli.ResolveStringFlag(cmd, "path")
	}
	return path
}

func validateBySpec(events []stream.TraceEvent, opts stream.TraceValidationOptions, manifests map[spec.SpecName]spec.Manifest) error {
	if len(manifests) == 0 {
		_, err := stream.EvaluateTrace(events, opts, nil)
		return err
	}

	grouped := make(map[spec.SpecName][]stream.TraceEvent)
	for _, evt := range events {
		specName := evt.Spec
		if specName == spec.SpecUnknown {
			specName = spec.SpecCDCFlow
		}
		if _, ok := manifests[specName]; !ok {
			return fmt.Errorf("trace spec %q has no manifest", specName)
		}
		grouped[specName] = append(grouped[specName], evt)
	}

	for specName, subset := range grouped {
		manifest := manifests[specName]
		if _, err := stream.EvaluateTrace(subset, opts, &manifest); err != nil {
			return fmt.Errorf("spec %s: %w", specName, err)
		}
	}
	return nil
}

func openReader(path string) (io.Reader, func() error, error) {
	if path == "" || path == "-" {
		return os.Stdin, nil, nil
	}
	// #nosec G304 -- input path comes from CLI flag.
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open %s: %w", path, err)
	}
	return file, file.Close, nil
}

func readEvents(reader io.Reader) ([]stream.TraceEvent, error) {
	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 1024*1024)
	events := make([]stream.TraceEvent, 0)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var evt stream.TraceEvent
		if err := json.Unmarshal(line, &evt); err != nil {
			return nil, fmt.Errorf("decode trace event: %w", err)
		}
		events = append(events, evt)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan trace: %w", err)
	}
	return events, nil
}

func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		next := filepath.Dir(dir)
		if next == dir {
			break
		}
		dir = next
	}
	return "", fmt.Errorf("go.mod not found for manifest lookup")
}
