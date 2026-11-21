package main

import (
	"errors"
	"os/exec"
	"strings"
	"testing"
)

func TestMutantKillRequiresSpecificFailingTestNotToolFailure(t *testing.T) {
	exit := &exec.ExitError{}
	for _, test := range []struct {
		output        string
		err           error
		timeout, want bool
	}{
		{output: "--- FAIL: TestX\nP4: terminal error swallowed", err: exit, want: true},
		{output: "--- FAIL: TestX\nP4", err: exit, timeout: true},
		{output: "P4 compiler error\n[build failed]", err: exit},
		{output: "--- FAIL: TestX\nother property", err: exit},
		{output: "--- FAIL: TestX\nP4", err: errors.New("launcher failed")},
		{output: "--- FAIL: TestX\nP4"},
	} {
		if got := killedByProperty([]byte(test.output), test.err, test.timeout, "P4"); got != test.want {
			t.Fatalf("kill verdict=%v want=%v for %+v", got, test.want, test)
		}
	}
}

func TestMutationAnchorsAreExactAndScoped(t *testing.T) {
	item := mutation{Name: "sample", Function: "func f(", Old: "return 1", New: "return 2"}
	source := "func f() int { return 1 }\nfunc g() int { return 1 }"
	updated, err := patch(source, item)
	if err != nil || !strings.Contains(updated, "func f() int { return 2 }") || !strings.Contains(updated, "func g() int { return 1 }") {
		t.Fatalf("patch=%q err=%v", updated, err)
	}
	for _, bad := range []string{"func x() int { return 1 }", "func f() int { return 1; return 1 }"} {
		if _, err := patch(bad, item); err == nil {
			t.Fatal("source drift was accepted")
		}
	}
}
