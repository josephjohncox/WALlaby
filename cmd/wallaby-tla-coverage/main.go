package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/josephjohncox/wallaby/internal/cli"
	"github.com/spf13/cobra"
)

type coverageFile struct {
	Name    string         `json:"name"`
	Actions map[string]int `json:"actions"`
}

type coverageReport struct {
	Min     int            `json:"min"`
	Ignore  []string       `json:"ignore"`
	Files   []coverageFile `json:"files"`
	Failing []string       `json:"failing"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type tlaCoverageOptions struct {
	dir     string
	min     int
	ignore  string
	jsonOut string
}

func run() error {
	command := newWallabyTLACoverageCommand()
	return command.Execute()
}

func newWallabyTLACoverageCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "wallaby-tla-coverage",
		Short:        "Summarize TLC coverage output and enforce minimum thresholds",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runWallabyTLACoverage(cmd)
		},
	}
	command.Flags().String("dir", "specs/coverage", "directory with TLC coverage outputs")
	command.Flags().Int("min", 1, "minimum coverage count per action")
	command.Flags().String("ignore", "", "comma-separated list of actions to ignore")
	command.Flags().String("json", "", "optional path to write JSON report")
	command.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		return initWallabyTLACoverageConfig(cmd)
	}
	command.InitDefaultCompletionCmd()
	return command
}

func initWallabyTLACoverageConfig(cmd *cobra.Command) error {
	return cli.InitViperFromCommand(cmd, cli.ViperConfig{
		EnvPrefix: "WALLABY_TLA_COVERAGE",
	})
}

func runWallabyTLACoverage(cmd *cobra.Command) error {
	opts := tlaCoverageOptions{
		dir:     cli.ResolveStringFlag(cmd, "dir"),
		min:     cli.ResolveIntFlag(cmd, "min"),
		ignore:  cli.ResolveStringFlag(cmd, "ignore"),
		jsonOut: cli.ResolveStringFlag(cmd, "json"),
	}

	report, err := buildCoverageReport(opts)
	if err != nil {
		return err
	}
	printReport(report)

	if opts.jsonOut != "" {
		if err := writeJSON(opts.jsonOut, report); err != nil {
			return fmt.Errorf("write json report: %w", err)
		}
	}
	if len(report.Failing) > 0 {
		return fmt.Errorf("coverage below threshold (%d): %s", opts.min, strings.Join(report.Failing, ", "))
	}
	return nil
}

func buildCoverageReport(opts tlaCoverageOptions) (coverageReport, error) {
	ignoreSet := make(map[string]struct{})
	for _, item := range strings.Split(opts.ignore, ",") {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			ignoreSet[trimmed] = struct{}{}
		}
	}

	files, err := filepath.Glob(filepath.Join(opts.dir, "*.txt"))
	if err != nil {
		return coverageReport{}, fmt.Errorf("list coverage files: %w", err)
	}
	if len(files) == 0 {
		return coverageReport{}, fmt.Errorf("no coverage files found in %s", opts.dir)
	}

	report := coverageReport{
		Min:    opts.min,
		Ignore: sortedKeys(ignoreSet),
		Files:  make([]coverageFile, 0, len(files)),
	}

	var failures []string
	for _, path := range files {
		actions, err := parseCoverage(path)
		if err != nil {
			return coverageReport{}, fmt.Errorf("parse %s: %w", path, err)
		}
		fileReport := coverageFile{
			Name:    filepath.Base(path),
			Actions: actions,
		}
		report.Files = append(report.Files, fileReport)

		for action, count := range actions {
			if _, skip := ignoreSet[action]; skip {
				continue
			}
			if count < opts.min {
				failures = append(failures, fmt.Sprintf("%s:%s=%d", filepath.Base(path), action, count))
			}
		}
	}
	sort.Strings(failures)
	report.Failing = failures
	return report, nil
}

var (
	actionLine     = regexp.MustCompile(`^\s*([A-Za-z0-9_'-]+)\s*:\s*([0-9]+)\s*$`)
	actionColon    = regexp.MustCompile(`^\s*Action\s+([A-Za-z0-9_'-]+)\s*:\s*([0-9]+)\s*$`)
	actionPlain    = regexp.MustCompile(`^\s*Action\s+([A-Za-z0-9_'-]+)\s+([0-9]+)\s*$`)
	actionBare     = regexp.MustCompile(`^\s*([A-Za-z0-9_'-]+)\s+([0-9]+)\s*$`)
	actionHeader   = regexp.MustCompile(`^\s*<([A-Za-z0-9_'-]+)\s+line[^>]*>:\s*([0-9]+)(?::([0-9]+))?\s*$`)
	neverEnabled   = regexp.MustCompile(`^\s*Action\s+([A-Za-z0-9_'-]+)\s+is\s+never\s+enabled`)
	neverExecuted  = regexp.MustCompile(`^\s*Action\s+([A-Za-z0-9_'-]+)\s+is\s+never\s+executed`)
	actionExecuted = regexp.MustCompile(`^\s*Action\s+([A-Za-z0-9_'-]+)\s+was\s+executed\s+([0-9]+)\s+times`)
	actionCovered  = regexp.MustCompile(`^\s*Action\s+([A-Za-z0-9_'-]+)\s+covered\s+([0-9]+)\s+times`)
	coverageHeader = regexp.MustCompile(`^\s*Coverage\s+report`)
)

func parseCoverage(path string) (map[string]int, error) {
	// #nosec G304 -- coverage path comes from CLI flag.
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(data), "\n")
	actions := make(map[string]int)
	for _, line := range lines {
		if coverageHeader.MatchString(line) {
			continue
		}
		if match := actionLine.FindStringSubmatch(line); len(match) == 3 {
			count, err := strconv.Atoi(match[2])
			if err != nil {
				return nil, fmt.Errorf("parse count %q: %w", match[2], err)
			}
			actions[match[1]] = count
			continue
		}
		if match := actionColon.FindStringSubmatch(line); len(match) == 3 {
			count, err := strconv.Atoi(match[2])
			if err != nil {
				return nil, fmt.Errorf("parse count %q: %w", match[2], err)
			}
			actions[match[1]] = count
			continue
		}
		if match := actionPlain.FindStringSubmatch(line); len(match) == 3 {
			count, err := strconv.Atoi(match[2])
			if err != nil {
				return nil, fmt.Errorf("parse count %q: %w", match[2], err)
			}
			actions[match[1]] = count
			continue
		}
		if match := actionBare.FindStringSubmatch(line); len(match) == 3 {
			count, err := strconv.Atoi(match[2])
			if err != nil {
				return nil, fmt.Errorf("parse count %q: %w", match[2], err)
			}
			actions[match[1]] = count
			continue
		}
		if match := actionHeader.FindStringSubmatch(line); len(match) >= 3 {
			raw := match[2]
			if len(match) > 3 && match[3] != "" {
				raw = match[3]
			}
			count, err := strconv.Atoi(raw)
			if err != nil {
				return nil, fmt.Errorf("parse count %q: %w", raw, err)
			}
			actions[match[1]] = count
			continue
		}
		if match := actionExecuted.FindStringSubmatch(line); len(match) == 3 {
			count, err := strconv.Atoi(match[2])
			if err != nil {
				return nil, fmt.Errorf("parse count %q: %w", match[2], err)
			}
			actions[match[1]] = count
			continue
		}
		if match := actionCovered.FindStringSubmatch(line); len(match) == 3 {
			count, err := strconv.Atoi(match[2])
			if err != nil {
				return nil, fmt.Errorf("parse count %q: %w", match[2], err)
			}
			actions[match[1]] = count
			continue
		}
		if match := neverEnabled.FindStringSubmatch(line); len(match) == 2 {
			actions[match[1]] = 0
			continue
		}
		if match := neverExecuted.FindStringSubmatch(line); len(match) == 2 {
			actions[match[1]] = 0
			continue
		}
	}
	if len(actions) == 0 {
		snippet := make([]string, 0, 5)
		for _, line := range lines {
			if strings.TrimSpace(line) == "" {
				continue
			}
			snippet = append(snippet, line)
			if len(snippet) == cap(snippet) {
				break
			}
		}
		if len(snippet) > 0 {
			return nil, fmt.Errorf("no action coverage lines found (first lines: %s)", strings.Join(snippet, " | "))
		}
		return nil, fmt.Errorf("no action coverage lines found (file empty)")
	}
	return actions, nil
}

func printReport(report coverageReport) {
	fmt.Printf("TLA coverage (min=%d)\n", report.Min)
	for _, file := range report.Files {
		fmt.Printf("  %s\n", file.Name)
		actions := make([]string, 0, len(file.Actions))
		for action := range file.Actions {
			actions = append(actions, action)
		}
		sort.Strings(actions)
		for _, action := range actions {
			fmt.Printf("    %-24s %d\n", action, file.Actions[action])
		}
	}
	if len(report.Failing) == 0 {
		fmt.Println("  coverage OK")
	}
}

func writeJSON(path string, report coverageReport) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	// #nosec G306 -- report is intended to be readable by all users.
	return os.WriteFile(path, data, 0o644)
}

func sortedKeys(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
