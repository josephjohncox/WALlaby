package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestVerifyRequiredTestsRequiresExactTopLevelLifecycle(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		events  string
		wantErr string
	}{
		{
			name: "successful nested suite",
			events: validPackageEvidence("example/pkg",
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("run", "example/pkg", "TestRequired/one"),
				testJSON("output", "example/pkg", "TestRequired/one"),
				testJSON("pass", "example/pkg", "TestRequired/one"),
				testJSON("pass", "example/pkg", "TestRequired"),
			),
		},
		{
			name: "single subtest name contains slash components",
			events: validPackageEvidence("example/pkg",
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("run", "example/pkg", "TestRequired/Int64/Delta"),
				testJSON("pass", "example/pkg", "TestRequired/Int64/Delta"),
				testJSON("pass", "example/pkg", "TestRequired"),
			),
		},
		{
			name: "longest active actual parent wins",
			events: validPackageEvidence("example/pkg",
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("run", "example/pkg", "TestRequired/Level1"),
				testJSON("run", "example/pkg", "TestRequired/Level1/Level2/Delta"),
				testJSON("pass", "example/pkg", "TestRequired/Level1/Level2/Delta"),
				testJSON("pass", "example/pkg", "TestRequired/Level1"),
				testJSON("pass", "example/pkg", "TestRequired"),
			),
		},
		{
			name: "benchmark slash name does not require synthetic parents",
			events: ndjson(
				packageJSON("start", "example/pkg"),
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("pass", "example/pkg", "TestRequired"),
				testJSON("run", "example/pkg", "BenchmarkDestinationWrite"),
				testJSON("output", "example/pkg", "BenchmarkDestinationWrite"),
				testJSON("run", "example/pkg", "BenchmarkDestinationWrite/json/new"),
				testJSON("output", "example/pkg", "BenchmarkDestinationWrite/json/new"),
				packageJSON("pass", "example/pkg"),
			),
		},
		{
			name: "nested events cannot substitute for top level",
			events: validPackageEvidence("example/pkg",
				testJSON("run", "example/pkg", "TestRequired/one"),
				testJSON("pass", "example/pkg", "TestRequired/one"),
			),
			wantErr: "without an active slash-prefix parent",
		},
		{
			name: "pass only",
			events: validPackageEvidence("example/pkg",
				testJSON("pass", "example/pkg", "TestRequired"),
			),
			wantErr: "action pass occurred before run",
		},
		{
			name: "duplicate run",
			events: validPackageEvidence("example/pkg",
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("pass", "example/pkg", "TestRequired"),
			),
			wantErr: "duplicate run",
		},
		{
			name: "duplicate pass",
			events: validPackageEvidence("example/pkg",
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("pass", "example/pkg", "TestRequired"),
				testJSON("pass", "example/pkg", "TestRequired"),
			),
			wantErr: "after terminal pass",
		},
		{
			name: "skipped nested suite",
			events: validPackageEvidence("example/pkg",
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("run", "example/pkg", "TestRequired/live"),
				testJSON("skip", "example/pkg", "TestRequired/live"),
				testJSON("pass", "example/pkg", "TestRequired"),
			),
			wantErr: "skipped=[TestRequired/live]",
		},
		{
			name: "failed nested suite",
			events: failedPackageEvidence("example/pkg",
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("run", "example/pkg", "TestRequired/fence"),
				testJSON("fail", "example/pkg", "TestRequired/fence"),
				testJSON("fail", "example/pkg", "TestRequired"),
			),
			wantErr: "TestRequired/fence",
		},
		{
			name: "incomplete nested suite",
			events: validPackageEvidence("example/pkg",
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("run", "example/pkg", "TestRequired/live"),
				testJSON("pass", "example/pkg", "TestRequired"),
			),
			wantErr: "before child TestRequired/live terminal",
		},
		{
			name: "similarly prefixed test is not evidence",
			events: validPackageEvidence("example/pkg",
				testJSON("run", "example/pkg", "TestRequiredExtra"),
				testJSON("skip", "example/pkg", "TestRequiredExtra"),
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("pass", "example/pkg", "TestRequired"),
			),
		},
		{
			name: "required test changes package",
			events: ndjson(
				packageJSON("start", "one/pkg"),
				packageJSON("start", "two/pkg"),
				testJSON("run", "one/pkg", "TestRequired"),
				testJSON("pass", "two/pkg", "TestRequired"),
				packageJSON("pass", "one/pkg"),
				packageJSON("pass", "two/pkg"),
			),
			wantErr: "action pass occurred before run",
		},
		{
			name: "truncated package terminal",
			events: ndjson(
				packageJSON("start", "example/pkg"),
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("pass", "example/pkg", "TestRequired"),
			),
			wantErr: "package terminal absent",
		},
		{
			name: "suite package failure",
			events: ndjson(
				packageJSON("start", "example/pkg"),
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("pass", "example/pkg", "TestRequired"),
				packageJSON("fail", "example/pkg"),
			),
			wantErr: "example/pkg(package)",
		},
		{
			name: "unrelated package failure",
			events: ndjson(
				packageJSON("start", "example/pkg"),
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("pass", "example/pkg", "TestRequired"),
				packageJSON("pass", "example/pkg"),
				packageJSON("start", "broken/pkg"),
				packageJSON("fail", "broken/pkg"),
			),
			wantErr: "broken/pkg(package)",
		},
		{
			name: "child starts after parent terminal",
			events: ndjson(
				packageJSON("start", "example/pkg"),
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("pass", "example/pkg", "TestRequired"),
				testJSON("run", "example/pkg", "TestRequired/late"),
			),
			wantErr: "without an active slash-prefix parent",
		},
		{
			name: "package terminal before active test",
			events: ndjson(
				packageJSON("start", "example/pkg"),
				testJSON("run", "example/pkg", "TestRequired"),
				packageJSON("pass", "example/pkg"),
			),
			wantErr: "occurred with active test",
		},
		{
			name: "package pass first",
			events: ndjson(
				packageJSON("pass", "example/pkg"),
				packageJSON("start", "example/pkg"),
			),
			wantErr: "occurred before package start",
		},
		{
			name: "test event after terminal",
			events: ndjson(
				packageJSON("start", "example/pkg"),
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("pass", "example/pkg", "TestRequired"),
				testJSON("output", "example/pkg", "TestRequired"),
			),
			wantErr: "after terminal pass",
		},
		{
			name: "package event after terminal",
			events: ndjson(
				packageJSON("start", "example/pkg"),
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("pass", "example/pkg", "TestRequired"),
				packageJSON("pass", "example/pkg"),
				packageJSON("output", "example/pkg"),
			),
			wantErr: "after terminal pass",
		},
		{
			name: "output before run",
			events: ndjson(
				packageJSON("start", "example/pkg"),
				testJSON("output", "example/pkg", "TestRequired"),
			),
			wantErr: "action output occurred before run",
		},
		{
			name: "continue before pause",
			events: ndjson(
				packageJSON("start", "example/pkg"),
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("cont", "example/pkg", "TestRequired"),
			),
			wantErr: "continued without pause",
		},
		{
			name: "elapsed before terminal",
			events: ndjson(
				packageJSON("start", "example/pkg"),
				`{"Action":"run","Package":"example/pkg","Test":"TestRequired","Elapsed":1}`,
			),
			wantErr: "elapsed is only valid on a terminal action",
		},
		{
			name: "valid pause continue output lifecycle",
			events: validPackageEvidence("example/pkg",
				testJSON("run", "example/pkg", "TestRequired"),
				testJSON("output", "example/pkg", "TestRequired"),
				testJSON("pause", "example/pkg", "TestRequired"),
				testJSON("output", "example/pkg", "TestRequired"),
				testJSON("cont", "example/pkg", "TestRequired"),
				testJSON("output", "example/pkg", "TestRequired"),
				testJSON("pass", "example/pkg", "TestRequired"),
			),
		},
		{
			name: "parallel package streams are independently ordered",
			events: ndjson(
				packageJSON("start", "one/pkg"),
				testJSON("run", "one/pkg", "TestRequired"),
				packageJSON("start", "two/pkg"),
				testJSON("run", "two/pkg", "TestOther"),
				testJSON("pause", "one/pkg", "TestRequired"),
				testJSON("pause", "two/pkg", "TestOther"),
				testJSON("output", "one/pkg", "TestRequired"),
				testJSON("cont", "two/pkg", "TestOther"),
				testJSON("pass", "two/pkg", "TestOther"),
				packageJSON("output", "two/pkg"),
				packageJSON("pass", "two/pkg"),
				testJSON("cont", "one/pkg", "TestRequired"),
				testJSON("pass", "one/pkg", "TestRequired"),
				packageJSON("pass", "one/pkg"),
			),
		},
		{
			name: "required top-level duplicated across packages",
			events: ndjson(
				packageJSON("start", "one/pkg"),
				testJSON("run", "one/pkg", "TestRequired"),
				testJSON("pass", "one/pkg", "TestRequired"),
				packageJSON("pass", "one/pkg"),
				packageJSON("start", "two/pkg"),
				testJSON("run", "two/pkg", "TestRequired"),
				testJSON("pass", "two/pkg", "TestRequired"),
				packageJSON("pass", "two/pkg"),
			),
			wantErr: "package occurrences=2",
		},
		{
			name:    "malformed non JSON line",
			events:  "go test preamble\n",
			wantErr: "invalid go test JSON line 1",
		},
		{
			name:    "truncated JSON",
			events:  `{"Action":"run","Package":"example/pkg","Test":"TestRequired"` + "\n",
			wantErr: "invalid go test JSON line 1",
		},
		{
			name: "unknown required action",
			events: ndjson(
				packageJSON("start", "example/pkg"),
				`{"Action":"mystery","Package":"example/pkg","Test":"TestRequired"}`,
			),
			wantErr: "unknown action",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			report, err := verifyRequiredTests(strings.NewReader(test.events), []string{"TestRequired"})
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("verifyRequiredTests() error=%v report=%+v", err, report)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("verifyRequiredTests() error=%v, want substring %q; report=%+v", err, test.wantErr, report)
			}
		})
	}
}

func TestVerifyRequiredTestsAcceptsPureRealGoTestJSON(t *testing.T) {
	command := exec.Command("go", "test", "-json", "-count=1", "-run", "^TestVerifierRealJSONFixture$", ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("run real go test -json fixture: %v", err)
	}
	if _, err := verifyRequiredTests(bytes.NewReader(output), []string{"TestVerifierRealJSONFixture"}); err != nil {
		t.Fatalf("verify real pure go test JSON: %v\n%s", err, output)
	}
}

func TestVerifyRequiredTestsAcceptsRealBenchmarkSlashStream(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "test", "-json", "-count=1", "-run", "^TestCapabilitiesDoNotOverstateRestartReplaySafety$", "-bench", "^BenchmarkDestinationWrite/json/new$", "-benchtime=1x", "../connectors/destinations/s3")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("run real benchmark go test -json fixture: %v", err)
	}
	if _, err := verifyRequiredTests(bytes.NewReader(output), []string{"TestCapabilitiesDoNotOverstateRestartReplaySafety"}); err != nil {
		t.Fatalf("verify real benchmark slash-name stream: %v\n%s", err, output)
	}
}

func TestVerifyGoTestJSONCLIWithPureRealStream(t *testing.T) {
	t.Parallel()
	testCommand := exec.Command("go", "test", "-json", "-count=1", "-run", "^TestVerifierRealJSONFixture$", ".")
	output, err := testCommand.Output()
	if err != nil {
		t.Fatalf("run real go test -json fixture: %v", err)
	}
	results := t.TempDir() + "/results.json"
	if err := os.WriteFile(results, output, 0o600); err != nil {
		t.Fatal(err)
	}
	verifyCommand := exec.Command("go", "run", "./verify-go-test-json.go", "-results", results, "-required", "TestVerifierRealJSONFixture")
	verified, err := verifyCommand.CombinedOutput()
	if err != nil {
		t.Fatalf("verify-go-test-json CLI dry-run: %v\n%s", err, verified)
	}
	if !strings.Contains(string(verified), "exactly one run/pass") && !strings.Contains(string(verified), "chronological run/pass") {
		t.Fatalf("unexpected verifier output: %s", verified)
	}
}

func TestVerifierRealJSONFixture(t *testing.T) {
	t.Run("serial", func(t *testing.T) { t.Log("serial output") })
	t.Run("Int64/Delta", func(t *testing.T) { t.Log("one slash-bearing subtest name") })
	t.Run("Level1", func(t *testing.T) {
		t.Run("Level2/Delta", func(t *testing.T) { t.Log("actual multi-level parent") })
	})
	t.Run("parallel", func(t *testing.T) {
		t.Parallel()
		t.Log("parallel output")
	})
}

func TestSplitRequiredRequiresAtLeastOneEffectiveName(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"", ",,,", "  , \t,  "} {
		if got := splitRequired(raw); len(got) != 0 {
			t.Fatalf("splitRequired(%q)=%v, want no effective names", raw, got)
		}
	}
	if got := splitRequired("TestOne, TestOne,TestOne"); len(got) != 1 || got[0] != "TestOne" {
		t.Fatalf("duplicate required names were not reduced to one effective test: %v", got)
	}
	if _, err := verifyRequiredTests(strings.NewReader(""), nil); err == nil || !strings.Contains(err.Error(), "at least one effective required") {
		t.Fatalf("empty effective required set error=%v", err)
	}
}

func TestVerifyRequiredTestsRejectsRequiredBenchmark(t *testing.T) {
	t.Parallel()
	_, err := verifyRequiredTests(strings.NewReader(validPackageEvidence("example/pkg")), []string{"BenchmarkDestinationWrite"})
	if err == nil || !strings.Contains(err.Error(), "required benchmark evidence") {
		t.Fatalf("required benchmark error=%v", err)
	}
}

func TestVerifyRequiredTestsRejectsNestedRequiredName(t *testing.T) {
	t.Parallel()
	_, err := verifyRequiredTests(strings.NewReader(validPackageEvidence("example/pkg")), []string{"TestRequired/live"})
	if err == nil || !strings.Contains(err.Error(), "not a top-level test") {
		t.Fatalf("nested required name error=%v", err)
	}
}

func TestVerifyRequiredTestsReportsScannerErrors(t *testing.T) {
	t.Parallel()
	reader := &errorReader{content: []byte(packageJSON("start", "example/pkg") + "\n"), err: errors.New("synthetic read failure")}
	_, err := verifyRequiredTests(reader, []string{"TestRequired"})
	if err == nil || !strings.Contains(err.Error(), "scan go test JSON") || !strings.Contains(err.Error(), "synthetic read failure") {
		t.Fatalf("scanner error=%v", err)
	}
}

type errorReader struct {
	content []byte
	err     error
}

func (r *errorReader) Read(buffer []byte) (int, error) {
	if len(r.content) != 0 {
		n := copy(buffer, r.content)
		r.content = r.content[n:]
		return n, nil
	}
	if r.err != nil {
		err := r.err
		r.err = nil
		return 0, err
	}
	return 0, io.EOF
}

func validPackageEvidence(packageName string, testEvents ...string) string {
	lines := []string{packageJSON("start", packageName)}
	lines = append(lines, testEvents...)
	lines = append(lines, packageJSON("pass", packageName))
	return ndjson(lines...)
}

func failedPackageEvidence(packageName string, testEvents ...string) string {
	lines := []string{packageJSON("start", packageName)}
	lines = append(lines, testEvents...)
	lines = append(lines, packageJSON("fail", packageName))
	return ndjson(lines...)
}

func packageJSON(action, packageName string) string {
	return fmt.Sprintf(`{"Action":%q,"Package":%q}`, action, packageName)
}

func testJSON(action, packageName, testName string) string {
	return fmt.Sprintf(`{"Action":%q,"Package":%q,"Test":%q}`, action, packageName, testName)
}

func ndjson(lines ...string) string { return strings.Join(lines, "\n") + "\n" }
