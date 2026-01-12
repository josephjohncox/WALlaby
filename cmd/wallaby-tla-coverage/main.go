package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
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
	dir := flag.String("dir", "specs/coverage", "directory with TLC coverage outputs")
	min := flag.Int("min", 1, "minimum coverage count per action")
	ignore := flag.String("ignore", "", "comma-separated list of actions to ignore")
	jsonOut := flag.String("json", "", "optional path to write JSON report")
	flag.Parse()

	ignoreSet := make(map[string]struct{})
	for _, item := range strings.Split(*ignore, ",") {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			ignoreSet[trimmed] = struct{}{}
		}
	}

	files, err := filepath.Glob(filepath.Join(*dir, "*.txt"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "list coverage files: %v\n", err)
		os.Exit(1)
	}
	if len(files) == 0 {
		fmt.Fprintf(os.Stderr, "no coverage files found in %s\n", *dir)
		os.Exit(1)
	}

	report := coverageReport{
		Min:    *min,
		Ignore: sortedKeys(ignoreSet),
		Files:  make([]coverageFile, 0, len(files)),
	}

	var failures []string
	for _, path := range files {
		actions, err := parseCoverage(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse %s: %v\n", path, err)
			os.Exit(1)
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
			if count < *min {
				failures = append(failures, fmt.Sprintf("%s:%s=%d", filepath.Base(path), action, count))
			}
		}
	}
	sort.Strings(failures)
	report.Failing = failures

	printReport(report)
	if *jsonOut != "" {
		if err := writeJSON(*jsonOut, report); err != nil {
			fmt.Fprintf(os.Stderr, "write json report: %v\n", err)
			os.Exit(1)
		}
	}

	if len(failures) > 0 {
		fmt.Fprintf(os.Stderr, "coverage below threshold (%d): %s\n", *min, strings.Join(failures, ", "))
		os.Exit(1)
	}
}

var (
	actionLine     = regexp.MustCompile(`^\s*([A-Za-z0-9_]+)\s*:\s*([0-9]+)\s*$`)
	neverEnabled   = regexp.MustCompile(`^\s*Action\s+([A-Za-z0-9_]+)\s+is\s+never\s+enabled`)
	neverExecuted  = regexp.MustCompile(`^\s*Action\s+([A-Za-z0-9_]+)\s+is\s+never\s+executed`)
	actionExecuted = regexp.MustCompile(`^\s*Action\s+([A-Za-z0-9_]+)\s+was\s+executed\s+([0-9]+)\s+times`)
	actionCovered  = regexp.MustCompile(`^\s*Action\s+([A-Za-z0-9_]+)\s+covered\s+([0-9]+)\s+times`)
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
		return nil, fmt.Errorf("no action coverage lines found")
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
