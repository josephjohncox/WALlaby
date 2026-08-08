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
	Action  string   `json:"Action"`
	Package string   `json:"Package"`
	Test    string   `json:"Test"`
	Elapsed *float64 `json:"Elapsed"`
	Output  string   `json:"Output"`
}

type verificationReport struct {
	Required []string
	Missing  []string
	Skipped  []string
	Failed   []string
	Invalid  []string
}

type testState struct {
	Name      string
	Package   string
	Parent    string
	Benchmark bool
	Run       int
	Pass      int
	Skip      int
	Fail      int
	Paused    bool
	Terminal  string
}

type packageState struct {
	Name     string
	Start    int
	Pass     int
	Skip     int
	Fail     int
	Terminal string
	Tests    map[string]*testState
}

func main() {
	results := flag.String("results", "", "pure go test -json output")
	requiredRaw := flag.String("required", "", "comma-separated required top-level test names")
	flag.Parse()
	if *results == "" || strings.TrimSpace(*requiredRaw) == "" {
		fmt.Fprintln(os.Stderr, "results and required tests are required")
		os.Exit(2)
	}
	required := splitRequired(*requiredRaw)
	if len(required) == 0 {
		fmt.Fprintln(os.Stderr, "at least one effective required top-level test is required")
		os.Exit(2)
	}
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
	fmt.Printf("verified %d required tests: chronological run/pass, complete nested suites, and passing package terminals\n", len(report.Required))
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
	if len(report.Required) == 0 {
		return report, errors.New("at least one effective required top-level test is required")
	}
	for _, name := range report.Required {
		if strings.TrimSpace(name) == "" {
			return report, errors.New("required test names must not be empty")
		}
		if strings.Contains(name, "/") {
			return report, fmt.Errorf("required evidence %q is not a top-level test", name)
		}
		if isBenchmarkName(name) {
			return report, fmt.Errorf("required benchmark evidence %q is unsupported; require top-level Test evidence", name)
		}
	}

	packages := make(map[string]*packageState)
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		var item event
		line := scanner.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			return report, fmt.Errorf("invalid go test JSON line %d: blank lines are not allowed", lineNumber)
		}
		if err := json.Unmarshal(line, &item); err != nil {
			return report, fmt.Errorf("invalid go test JSON line %d: %w", lineNumber, err)
		}
		if item.Action == "" || item.Package == "" {
			return report, fmt.Errorf("invalid go test JSON line %d: Action and Package are required", lineNumber)
		}
		if !knownGoTestAction(item.Action) {
			return report, fmt.Errorf("invalid go test JSON line %d: unknown action %q", lineNumber, item.Action)
		}
		if item.Elapsed != nil && (item.Action != "pass" && item.Action != "fail" && item.Action != "skip") {
			return report, fmt.Errorf("invalid go test JSON line %d: elapsed is only valid on a terminal action", lineNumber)
		}
		if item.Elapsed != nil && *item.Elapsed < 0 {
			return report, fmt.Errorf("invalid go test JSON line %d: elapsed must not be negative", lineNumber)
		}
		if item.Output != "" && item.Action != "output" {
			return report, fmt.Errorf("invalid go test JSON line %d: Output is only valid on output actions", lineNumber)
		}

		pkg := packages[item.Package]
		if item.Action == "start" {
			if item.Test != "" {
				return report, fmt.Errorf("invalid go test JSON line %d: start is a package-only action", lineNumber)
			}
			if pkg != nil {
				return report, fmt.Errorf("package %s has duplicate or late start on line %d", item.Package, lineNumber)
			}
			packages[item.Package] = &packageState{Name: item.Package, Start: 1, Tests: make(map[string]*testState)}
			continue
		}
		if pkg == nil {
			return report, fmt.Errorf("package %s event %q occurred before package start on line %d", item.Package, item.Action, lineNumber)
		}
		if pkg.Terminal != "" {
			return report, fmt.Errorf("package %s has event %q after terminal %s on line %d", item.Package, item.Action, pkg.Terminal, lineNumber)
		}

		if item.Test == "" {
			if err := applyPackageEvent(pkg, item.Action, lineNumber); err != nil {
				return report, err
			}
			continue
		}
		if err := applyTestEvent(pkg, item, lineNumber); err != nil {
			return report, err
		}
	}
	if err := scanner.Err(); err != nil {
		return report, fmt.Errorf("scan go test JSON: %w", err)
	}

	missing := make(map[string]bool)
	skipped := make(map[string]bool)
	failed := make(map[string]bool)
	invalid := make(map[string]bool)
	for packageName, pkg := range packages {
		if pkg.Terminal == "" {
			invalid[packageName+"(package terminal absent)"] = true
		}
		if pkg.Fail != 0 {
			failed[packageName+"(package)"] = true
		}
		for testName, state := range pkg.Tests {
			if state.Terminal == "" {
				invalid[packageName+":"+testName+"(terminal absent)"] = true
			}
		}
	}
	for _, requiredName := range report.Required {
		var matches []*testState
		for _, pkg := range packages {
			if state := pkg.Tests[requiredName]; state != nil {
				matches = append(matches, state)
			}
		}
		if len(matches) == 0 {
			missing[requiredName] = true
			continue
		}
		if len(matches) != 1 {
			invalid[fmt.Sprintf("%s(package occurrences=%d)", requiredName, len(matches))] = true
			continue
		}
		state := matches[0]
		if state.Run != 1 || state.Pass != 1 || state.Skip != 0 || state.Fail != 0 || state.Terminal != "pass" {
			invalid[fmt.Sprintf("%s(run=%d pass=%d skip=%d fail=%d terminal=%s)", requiredName, state.Run, state.Pass, state.Skip, state.Fail, state.Terminal)] = true
		}
		if state.Skip != 0 {
			skipped[requiredName] = true
		}
		if state.Fail != 0 {
			failed[requiredName] = true
		}
		pkg := packages[state.Package]
		if pkg == nil || pkg.Start != 1 || pkg.Pass != 1 || pkg.Skip != 0 || pkg.Fail != 0 || pkg.Terminal != "pass" {
			invalid[state.Package+"(required package did not start once and pass once)"] = true
		}
		for testName, nested := range pkg.Tests {
			if !strings.HasPrefix(testName, requiredName+"/") {
				continue
			}
			if nested.Skip != 0 {
				skipped[testName] = true
			}
			if nested.Fail != 0 {
				failed[testName] = true
			}
			if nested.Run != 1 || nested.Pass != 1 || nested.Skip != 0 || nested.Fail != 0 || nested.Terminal != "pass" {
				invalid[fmt.Sprintf("%s(run=%d pass=%d skip=%d fail=%d terminal=%s)", testName, nested.Run, nested.Pass, nested.Skip, nested.Fail, nested.Terminal)] = true
			}
		}
	}
	report.Missing = sortedKeys(missing)
	report.Skipped = sortedKeys(skipped)
	report.Failed = sortedKeys(failed)
	report.Invalid = sortedKeys(invalid)
	if len(report.Missing) != 0 || len(report.Skipped) != 0 || len(report.Failed) != 0 || len(report.Invalid) != 0 {
		return report, fmt.Errorf("durability evidence incomplete: missing=%v skipped=%v failed=%v invalid=%v", report.Missing, report.Skipped, report.Failed, report.Invalid)
	}
	return report, nil
}

func applyPackageEvent(pkg *packageState, action string, lineNumber int) error {
	switch action {
	case "output":
		return nil
	case "pass", "fail", "skip":
		for _, state := range pkg.Tests {
			if state.Terminal == "" && state.Benchmark {
				state.Terminal = "benchmark"
				state.Paused = false
				continue
			}
			if state.Terminal == "" {
				return fmt.Errorf("package %s terminal %s occurred with active test %s on line %d", pkg.Name, action, state.Name, lineNumber)
			}
			if action == "pass" && state.Fail != 0 {
				return fmt.Errorf("package %s passed after failed test %s on line %d", pkg.Name, state.Name, lineNumber)
			}
		}
		pkg.Terminal = action
		switch action {
		case "pass":
			pkg.Pass++
		case "fail":
			pkg.Fail++
		case "skip":
			pkg.Skip++
		}
		return nil
	default:
		return fmt.Errorf("package %s has invalid package action %q on line %d", pkg.Name, action, lineNumber)
	}
}

func applyTestEvent(pkg *packageState, item event, lineNumber int) error {
	state := pkg.Tests[item.Test]
	switch item.Action {
	case "run":
		if state != nil {
			return fmt.Errorf("test %s in package %s has duplicate run on line %d", item.Test, pkg.Name, lineNumber)
		}
		parentName := longestActiveParent(pkg, item.Test)
		if strings.Contains(item.Test, "/") && parentName == "" {
			return fmt.Errorf("nested test %s started without an active slash-prefix parent on line %d", item.Test, lineNumber)
		}
		pkg.Tests[item.Test] = &testState{Name: item.Test, Package: pkg.Name, Parent: parentName, Benchmark: isBenchmarkName(item.Test), Run: 1}
		return nil
	case "output", "bench", "pause", "cont", "pass", "fail", "skip":
		if state == nil || state.Run != 1 {
			return fmt.Errorf("test %s action %s occurred before run on line %d", item.Test, item.Action, lineNumber)
		}
		if state.Terminal != "" {
			return fmt.Errorf("test %s has action %s after terminal %s on line %d", item.Test, item.Action, state.Terminal, lineNumber)
		}
	default:
		return fmt.Errorf("test %s has invalid action %s on line %d", item.Test, item.Action, lineNumber)
	}
	switch item.Action {
	case "output", "bench":
		return nil
	case "pause":
		if state.Paused {
			return fmt.Errorf("test %s has duplicate pause on line %d", item.Test, lineNumber)
		}
		state.Paused = true
		return nil
	case "cont":
		if !state.Paused {
			return fmt.Errorf("test %s continued without pause on line %d", item.Test, lineNumber)
		}
		state.Paused = false
		return nil
	case "pass", "fail", "skip":
		if state.Paused {
			return fmt.Errorf("test %s terminated while paused on line %d", item.Test, lineNumber)
		}
		for otherName, other := range pkg.Tests {
			if other.Parent == item.Test && other.Terminal == "" {
				return fmt.Errorf("parent test %s terminal %s occurred before child %s terminal on line %d", item.Test, item.Action, otherName, lineNumber)
			}
			if item.Action == "pass" && other.Parent == item.Test && other.Fail != 0 {
				return fmt.Errorf("parent test %s passed after failed child %s on line %d", item.Test, otherName, lineNumber)
			}
		}
		state.Terminal = item.Action
		switch item.Action {
		case "pass":
			state.Pass++
		case "fail":
			state.Fail++
		case "skip":
			state.Skip++
		}
		return nil
	}
	return nil
}

func longestActiveParent(pkg *packageState, name string) string {
	parent := ""
	for candidateName, candidate := range pkg.Tests {
		if candidate.Run != 1 || candidate.Terminal != "" || candidate.Paused || !strings.HasPrefix(name, candidateName+"/") {
			continue
		}
		if len(candidateName) > len(parent) {
			parent = candidateName
		}
	}
	return parent
}

func isBenchmarkName(name string) bool {
	root := name
	if slash := strings.IndexByte(root, '/'); slash >= 0 {
		root = root[:slash]
	}
	return strings.HasPrefix(root, "Benchmark")
}

func knownGoTestAction(action string) bool {
	switch action {
	case "start", "run", "pause", "cont", "pass", "bench", "fail", "output", "skip":
		return true
	default:
		return false
	}
}

func sortedKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
