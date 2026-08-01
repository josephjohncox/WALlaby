package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

type testEvent struct {
	Action  string `json:"Action"`
	Package string `json:"Package"`
	Test    string `json:"Test"`
	Output  string `json:"Output"`
}

type expectedResults struct {
	Packages []string            `json:"packages"`
	Tests    map[string][]string `json:"tests"`
}

func main() {
	goCommand := flag.String("go", envOr("GO", "go"), "Go command")
	outputPath := flag.String(
		"output",
		envOr("GO_TEST_JSON", ".cache/test-results/go-test.json"),
		"machine-readable Go test output",
	)
	expectedPath := flag.String(
		"expected-output",
		envOr("GO_TEST_EXPECTED_JSON", ".cache/test-results/go-test-expected.json"),
		"enumerated package and test manifest",
	)
	flag.Parse()

	if err := run(*goCommand, *outputPath, *expectedPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(goCommand, outputPath, expectedPath string) error {
	packages, err := listPackages(goCommand)
	if err != nil {
		return err
	}
	expected, err := enumerateTests(goCommand)
	if err != nil {
		return err
	}
	if err := writeExpected(expectedPath, packages, expected); err != nil {
		return err
	}

	returnCode, seenPackages, seenTests, err := runTests(goCommand, outputPath)
	if err != nil {
		return err
	}
	missingPackages := setDifference(packages, seenPackages)
	missing := missingTests(expected, seenTests)
	if len(missingPackages) > 0 {
		fmt.Fprintln(os.Stderr, "missing Go test packages:")
		for _, pkg := range missingPackages {
			fmt.Fprintf(os.Stderr, "  %s\n", pkg)
		}
	}
	if len(missing) > 0 {
		fmt.Fprintln(os.Stderr, "missing enumerated Go tests:")
		for _, test := range missing {
			fmt.Fprintf(os.Stderr, "  %s\n", test)
		}
	}

	expectedCount := testCount(expected)
	seenCount := intersectingTestCount(expected, seenTests)
	fmt.Printf(
		"Go test completeness: packages=%d/%d tests=%d/%d\n",
		len(seenPackages),
		len(packages),
		seenCount,
		expectedCount,
	)
	if returnCode != 0 {
		return fmt.Errorf("go test exited with status %d", returnCode)
	}
	if len(missingPackages) > 0 || len(missing) > 0 {
		return errors.New("go test completeness check failed")
	}
	return nil
}

func listPackages(goCommand string) (map[string]struct{}, error) {
	output, err := exec.Command(goCommand, "list", "./...").Output()
	if err != nil {
		return nil, fmt.Errorf("list Go packages: %w", err)
	}
	packages := make(map[string]struct{})
	for _, line := range strings.Split(string(output), "\n") {
		if pkg := strings.TrimSpace(line); pkg != "" {
			packages[pkg] = struct{}{}
		}
	}
	return packages, nil
}

func enumerateTests(goCommand string) (map[string]map[string]struct{}, error) {
	command := exec.Command(goCommand, "test", "-json", "-list", "^Test", "./...")
	expected := make(map[string]map[string]struct{})
	if err := consumeCommand(command, nil, func(event testEvent) {
		name := strings.TrimSpace(event.Output)
		if event.Package == "" || !strings.HasPrefix(name, "Test") || strings.ContainsAny(name, " \t") {
			return
		}
		addTest(expected, event.Package, name)
	}); err != nil {
		return nil, fmt.Errorf("enumerate Go tests: %w", err)
	}
	return expected, nil
}

func runTests(goCommand, outputPath string) (int, map[string]struct{}, map[string]map[string]struct{}, error) {
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o750); err != nil {
		return 0, nil, nil, fmt.Errorf("create Go test results directory: %w", err)
	}
	// #nosec G304 -- the result path is explicit operator or CI configuration.
	output, err := os.OpenFile(filepath.Clean(outputPath), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("create Go test results: %w", err)
	}
	defer func() { _ = output.Close() }()

	packages := make(map[string]struct{})
	tests := make(map[string]map[string]struct{})
	command := exec.Command(goCommand, "test", "-json", "-count=1", "./...")
	err = consumeCommand(command, output, func(event testEvent) {
		if event.Package != "" {
			packages[event.Package] = struct{}{}
		}
		if event.Package != "" && event.Test != "" {
			addTest(tests, event.Package, strings.SplitN(event.Test, "/", 2)[0])
		}
		if event.Output != "" {
			fmt.Print(event.Output)
		}
	})
	if err == nil {
		return 0, packages, tests, nil
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return exitError.ExitCode(), packages, tests, nil
	}
	return 0, nil, nil, fmt.Errorf("run Go tests: %w", err)
}

func consumeCommand(command *exec.Cmd, copyTo io.Writer, consume func(testEvent)) error {
	stdout, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		return err
	}
	scanner := bufio.NewScanner(stdout)
	buffer := make([]byte, 64*1024)
	scanner.Buffer(buffer, 4*1024*1024)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		if copyTo != nil {
			if _, err := copyTo.Write(append(line, '\n')); err != nil {
				return err
			}
		}
		var event testEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return fmt.Errorf("decode Go test event: %w", err)
		}
		consume(event)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return command.Wait()
}

func writeExpected(path string, packages map[string]struct{}, tests map[string]map[string]struct{}) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create expected results directory: %w", err)
	}
	payload := expectedResults{Packages: sortedSet(packages), Tests: make(map[string][]string, len(tests))}
	for pkg, names := range tests {
		payload.Tests[pkg] = sortedSet(names)
	}
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("encode expected Go tests: %w", err)
	}
	encoded = append(encoded, '\n')
	// #nosec G304 -- the manifest path is explicit operator or CI configuration.
	if err := os.WriteFile(filepath.Clean(path), encoded, 0o600); err != nil {
		return fmt.Errorf("write expected Go tests: %w", err)
	}
	return nil
}

func missingTests(expected, actual map[string]map[string]struct{}) []string {
	var missing []string
	for pkg, names := range expected {
		for name := range names {
			if _, ok := actual[pkg][name]; !ok {
				missing = append(missing, pkg+":"+name)
			}
		}
	}
	sort.Strings(missing)
	return missing
}

func setDifference(expected, actual map[string]struct{}) []string {
	var missing []string
	for value := range expected {
		if _, ok := actual[value]; !ok {
			missing = append(missing, value)
		}
	}
	sort.Strings(missing)
	return missing
}

func sortedSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func addTest(tests map[string]map[string]struct{}, pkg, name string) {
	if tests[pkg] == nil {
		tests[pkg] = make(map[string]struct{})
	}
	tests[pkg][name] = struct{}{}
}

func testCount(tests map[string]map[string]struct{}) int {
	count := 0
	for _, names := range tests {
		count += len(names)
	}
	return count
}

func intersectingTestCount(expected, actual map[string]map[string]struct{}) int {
	count := 0
	for pkg, names := range actual {
		for name := range names {
			if _, ok := expected[pkg][name]; ok {
				count++
			}
		}
	}
	return count
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
