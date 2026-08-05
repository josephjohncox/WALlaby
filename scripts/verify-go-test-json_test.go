package main

import (
	"strings"
	"testing"
)

func TestVerifyRequiredTestsAccountsForNestedResults(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		events   string
		required []string
		wantErr  string
	}{
		{
			name: "successful nested suite",
			events: ndjson(
				`{"Action":"run","Test":"TestRequired"}`,
				`{"Action":"run","Test":"TestRequired/one"}`,
				`{"Action":"pass","Test":"TestRequired/one"}`,
				`{"Action":"pass","Test":"TestRequired"}`,
			),
			required: []string{"TestRequired"},
		},
		{
			name: "skipped required subtest fails passed parent",
			events: ndjson(
				`{"Action":"run","Test":"TestRequired"}`,
				`{"Action":"skip","Test":"TestRequired/live"}`,
				`{"Action":"pass","Test":"TestRequired"}`,
			),
			required: []string{"TestRequired"},
			wantErr:  "skipped=[TestRequired/live]",
		},
		{
			name: "failed required subtest fails",
			events: ndjson(
				`{"Action":"run","Test":"TestRequired/fence"}`,
				`{"Action":"fail","Test":"TestRequired/fence"}`,
				`{"Action":"fail","Test":"TestRequired"}`,
			),
			required: []string{"TestRequired"},
			wantErr:  "failed=[TestRequired TestRequired/fence]",
		},
		{
			name: "explicit nested required test missing",
			events: ndjson(
				`{"Action":"pass","Test":"TestRequired"}`,
			),
			required: []string{"TestRequired", "TestRequired/live"},
			wantErr:  "missing=[TestRequired/live]",
		},
		{
			name: "started nested required test without terminal event is missing",
			events: ndjson(
				`{"Action":"run","Test":"TestRequired/live"}`,
				`{"Action":"pass","Test":"TestRequired"}`,
			),
			required: []string{"TestRequired"},
			wantErr:  "missing=[TestRequired/live]",
		},
		{
			name: "similarly prefixed top level test is excluded",
			events: ndjson(
				`{"Action":"skip","Test":"TestRequiredExtra/live"}`,
				`{"Action":"pass","Test":"TestRequired"}`,
			),
			required: []string{"TestRequired"},
		},
		{
			name: "repeated skip then pass remains skipped",
			events: ndjson(
				`{"Action":"run","Test":"TestRequired"}`,
				`{"Action":"skip","Test":"TestRequired"}`,
				`{"Action":"run","Test":"TestRequired"}`,
				`{"Action":"pass","Test":"TestRequired"}`,
			),
			required: []string{"TestRequired"},
			wantErr:  "skipped=[TestRequired]",
		},
		{
			name: "repeated fail then pass remains failed",
			events: ndjson(
				`{"Action":"run","Test":"TestRequired"}`,
				`{"Action":"fail","Test":"TestRequired"}`,
				`{"Action":"run","Test":"TestRequired"}`,
				`{"Action":"pass","Test":"TestRequired"}`,
			),
			required: []string{"TestRequired"},
			wantErr:  "failed=[TestRequired]",
		},
		{
			name: "repeated pass then skip remains skipped",
			events: ndjson(
				`{"Action":"run","Test":"TestRequired"}`,
				`{"Action":"pass","Test":"TestRequired"}`,
				`{"Action":"run","Test":"TestRequired"}`,
				`{"Action":"skip","Test":"TestRequired"}`,
			),
			required: []string{"TestRequired"},
			wantErr:  "skipped=[TestRequired]",
		},
		{
			name: "repeated nested skip then pass remains skipped",
			events: ndjson(
				`{"Action":"run","Test":"TestRequired"}`,
				`{"Action":"run","Test":"TestRequired/live"}`,
				`{"Action":"skip","Test":"TestRequired/live"}`,
				`{"Action":"pass","Test":"TestRequired"}`,
				`{"Action":"run","Test":"TestRequired"}`,
				`{"Action":"run","Test":"TestRequired/live"}`,
				`{"Action":"pass","Test":"TestRequired/live"}`,
				`{"Action":"pass","Test":"TestRequired"}`,
			),
			required: []string{"TestRequired"},
			wantErr:  "skipped=[TestRequired/live]",
		},
		{
			name: "successful repeated suite passes",
			events: ndjson(
				`{"Action":"run","Test":"TestRequired"}`,
				`{"Action":"run","Test":"TestRequired/live"}`,
				`{"Action":"pass","Test":"TestRequired/live"}`,
				`{"Action":"pass","Test":"TestRequired"}`,
				`{"Action":"run","Test":"TestRequired"}`,
				`{"Action":"run","Test":"TestRequired/live"}`,
				`{"Action":"pass","Test":"TestRequired/live"}`,
				`{"Action":"pass","Test":"TestRequired"}`,
			),
			required: []string{"TestRequired"},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			report, err := verifyRequiredTests(strings.NewReader(test.events), test.required)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("verifyRequiredTests() error = %v; report=%+v", err, report)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("verifyRequiredTests() error = %v, want substring %q; report=%+v", err, test.wantErr, report)
			}
		})
	}
}

func ndjson(lines ...string) string { return strings.Join(lines, "\n") + "\n" }
