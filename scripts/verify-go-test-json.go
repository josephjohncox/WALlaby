package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

type event struct {
	Action string `json:"Action"`
	Test   string `json:"Test"`
}

type verificationReport struct {
	Required []string
	Missing  []string
	Skipped  []string
	Failed   []string
}

func main() {
	results := flag.String("results", "", "go test -json output")
	requiredRaw := flag.String("required", "", "comma-separated required test names")
	flag.Parse()
	if *results == "" || strings.TrimSpace(*requiredRaw) == "" {
		fmt.Fprintln(os.Stderr, "results and required tests are required")
		os.Exit(2)
	}
	required := splitRequired(*requiredRaw)
	file, err := os.Open(*results)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	report, verifyErr := verifyRequiredTests(file, required)
	closeErr := file.Close()
	if verifyErr != nil {
		fmt.Fprintln(os.Stderr, verifyErr)
		os.Exit(1)
	}
	if closeErr != nil {
		fmt.Fprintln(os.Stderr, closeErr)
		os.Exit(2)
	}
	fmt.Printf("verified %d required tests: present, passed, and no required suite subtest skipped or failed\n", len(report.Required))
}

func splitRequired(raw string) []string {
	seen := make(map[string]struct{})
	var required []string
	for _, name := range strings.Split(raw, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		required = append(required, name)
	}
	sort.Strings(required)
	return required
}

func verifyRequiredTests(input io.Reader, requiredNames []string) (verificationReport, error) {
	report := verificationReport{Required: append([]string(nil), requiredNames...)}
	sort.Strings(report.Required)
	terminalCount := make(map[string]int)
	runCount := make(map[string]int)
	everSkipped := make(map[string]bool)
	everFailed := make(map[string]bool)
	required := make(map[string]bool, len(report.Required))
	for _, name := range report.Required {
		if strings.TrimSpace(name) == "" {
			return report, errors.New("required test names must not be empty")
		}
		required[name] = true
	}

	scanner := bufio.NewScanner(input)
	buffer := make([]byte, 64*1024)
	scanner.Buffer(buffer, 4*1024*1024)
	for scanner.Scan() {
		var item event
		if json.Unmarshal(scanner.Bytes(), &item) != nil || item.Test == "" {
			continue
		}
		if !belongsToRequiredSuite(item.Test, report.Required) {
			continue
		}
		switch item.Action {
		case "run":
			runCount[item.Test]++
		case "pass":
			terminalCount[item.Test]++
		case "skip":
			terminalCount[item.Test]++
			everSkipped[item.Test] = true
		case "fail":
			terminalCount[item.Test]++
			everFailed[item.Test] = true
		}
	}
	if err := scanner.Err(); err != nil {
		return report, err
	}

	missing := make(map[string]bool)
	skipped := make(map[string]bool)
	failed := make(map[string]bool)
	for name := range required {
		if terminalCount[name] == 0 || runCount[name] > terminalCount[name] {
			missing[name] = true
		}
	}
	// A named top-level required suite is only complete when every nested test
	// that actually ran has a terminal result, and no execution was ever skipped
	// or failed. Evidence files may concatenate repeated go test executions, so
	// a later pass must not erase an earlier non-pass.
	for name, runs := range runCount {
		if belongsToRequiredSuite(name, report.Required) && runs > terminalCount[name] {
			missing[name] = true
		}
	}
	for name := range everSkipped {
		if belongsToRequiredSuite(name, report.Required) {
			skipped[name] = true
		}
	}
	for name := range everFailed {
		if belongsToRequiredSuite(name, report.Required) {
			failed[name] = true
		}
	}
	report.Missing = sortedKeys(missing)
	report.Skipped = sortedKeys(skipped)
	report.Failed = sortedKeys(failed)
	if len(report.Missing) != 0 || len(report.Skipped) != 0 || len(report.Failed) != 0 {
		return report, fmt.Errorf("durability evidence incomplete: missing=%v skipped=%v failed=%v", report.Missing, report.Skipped, report.Failed)
	}
	return report, nil
}

func belongsToRequiredSuite(test string, required []string) bool {
	for _, name := range required {
		if test == name || strings.HasPrefix(test, name+"/") {
			return true
		}
	}
	return false
}

func sortedKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
