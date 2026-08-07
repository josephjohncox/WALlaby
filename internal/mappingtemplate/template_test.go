package mappingtemplate

import (
	"strings"
	"sync"
	"testing"
)

func TestParseAndExpandPreservesIdentifierBytes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		raw       string
		component Component
		data      Data
		want      string
	}{
		{name: "schema", raw: "raw_{{ .Schema }}_v1", component: Schema, data: Data{Schema: " Mixed Case.日本語 \"quoted\" "}, want: "raw_ Mixed Case.日本語 \"quoted\" _v1"},
		{name: "table braces", raw: "pre{literal}_{{.Table}}_{suffix}", component: Table, data: Data{Table: "{{/* injected comment */}}{{ .Schema }}"}, want: "pre{literal}_{{/* injected comment */}}{{ .Schema }}_{suffix}"},
		{name: "column dots and quotes", raw: "{{\t.Column\t}}", component: Column, data: Data{Column: ".'quoted'."}, want: ".'quoted'."},
		{name: "leading and trailing injected whitespace", raw: "{{ .Table }}", component: Table, data: Data{Table: "  table name\t"}, want: "  table name\t"},
		{name: "whitespace-only quoted identifier", raw: "{{ .Column }}", component: Column, data: Data{Column: " \t "}, want: " \t "},
		{name: "invalid UTF-8 bytes", raw: "x{{ .Table }}y", component: Table, data: Data{Table: string([]byte{0xff, 0xfe})}, want: string([]byte{'x', 0xff, 0xfe, 'y'})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled, err := Parse(test.raw, test.component)
			if err != nil {
				t.Fatal(err)
			}
			got, err := compiled.Expand(test.data)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("Expand() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestParseRejectsAnythingOutsideRestrictedAST(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		raw       string
		component Component
		want      string
	}{
		{name: "empty", component: Table, want: "required"},
		{name: "leading whitespace", raw: " {{ .Table }}", component: Table, want: "whitespace"},
		{name: "trailing whitespace", raw: "{{ .Table }} ", component: Table, want: "whitespace"},
		{name: "nul", raw: "x\x00{{ .Table }}", component: Table, want: "NUL"},
		{name: "legacy schema", raw: "{schema}", component: Schema, want: "exactly one action"},
		{name: "legacy table", raw: "{table}", component: Table, want: "exactly one action"},
		{name: "legacy column", raw: "{column}", component: Column, want: "exactly one action"},
		{name: "no action", raw: "fixed", component: Table, want: "exactly one action"},
		{name: "duplicate action", raw: "{{ .Table }}_{{ .Table }}", component: Table, want: "exactly one action"},
		{name: "wrong field", raw: "{{ .Schema }}", component: Table, want: ".Table"},
		{name: "wrong case", raw: "{{ .table }}", component: Table, want: ".Table"},
		{name: "nested field", raw: "{{ .Table.Name }}", component: Table, want: ".Table"},
		{name: "dot", raw: "{{ . }}", component: Table, want: ".Table"},
		{name: "variable", raw: "{{ $x }}", component: Table, want: "undefined variable"},
		{name: "declaration", raw: "{{ $x := .Table }}", component: Table, want: "declarations"},
		{name: "assignment", raw: "{{ $x = .Table }}", component: Table, want: "declarations"},
		{name: "identifier builtin", raw: "{{ print }}", component: Table, want: ".Table"},
		{name: "builtin call", raw: "{{ print .Table }}", component: Table, want: "exactly one .Table field"},
		{name: "string literal", raw: "{{ \"table\" }}", component: Table, want: ".Table"},
		{name: "number literal", raw: "{{ 1 }}", component: Table, want: ".Table"},
		{name: "boolean literal", raw: "{{ true }}", component: Table, want: ".Table"},
		{name: "nil literal", raw: "{{ nil }}", component: Table, want: ".Table"},
		{name: "pipeline", raw: "{{ .Table | print }}", component: Table, want: "pipeline"},
		{name: "chain", raw: "{{ (print .Table).Name }}", component: Table, want: ".Table"},
		{name: "if", raw: "{{ if .Table }}x{{ end }}", component: Table, want: "exactly one action"},
		{name: "range", raw: "{{ range .Table }}x{{ end }}", component: Table, want: "exactly one action"},
		{name: "with", raw: "{{ with .Table }}x{{ end }}", component: Table, want: "exactly one action"},
		{name: "template invocation", raw: "{{ template \"x\" . }}", component: Table, want: "unsupported"},
		{name: "define", raw: "{{ define \"x\" }}x{{ end }}{{ .Table }}", component: Table, want: "exactly one action"},
		{name: "block", raw: "{{ block \"x\" . }}x{{ end }}{{ .Table }}", component: Table, want: "exactly one action"},
		{name: "plain comment only", raw: "{{/* comment */}}", component: Table, want: "comments"},
		{name: "trimmed comment only", raw: "{{- /* comment */ -}}", component: Table, want: "comments"},
		{name: "compact trimmed comment only", raw: "{{-/* comment */-}}", component: Table, want: "comments"},
		{name: "plain comment before", raw: "{{/* comment */}}{{ .Table }}", component: Table, want: "exactly one action"},
		{name: "trimmed comment before", raw: "{{- /* comment */ -}}{{ .Table }}", component: Table, want: "exactly one action"},
		{name: "plain comment after", raw: "{{ .Table }}{{/* comment */}}", component: Table, want: "exactly one action"},
		{name: "trimmed comment after", raw: "{{ .Table }}{{- /* comment */ -}}", component: Table, want: "exactly one action"},
		{name: "nested opening delimiter", raw: "{{ .Table {{ }}", component: Table, want: "exactly one action"},
		{name: "additional closing delimiter", raw: "{{ .Table }} }}", component: Table, want: "exactly one action"},
		{name: "delimiter in literal", raw: "{{ \"{{\" }}", component: Table, want: "exactly one action"},
		{name: "unknown function", raw: "{{ env .Table }}", component: Table, want: "function"},
		{name: "malformed action", raw: "{{ .Table", component: Table, want: "exactly one action"},
		{name: "unknown component", raw: "{{ .Unknown }}", component: Component("unknown"), want: "unsupported template component"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(test.raw, test.component)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Parse() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestInverseMatchesOnlyCompiledPrefixSuffixRange(t *testing.T) {
	t.Parallel()
	tests := []struct {
		raw    string
		output string
		want   string
		match  bool
	}{
		{raw: "pre{{ .Table }}post", output: "pre Mixed.Case post", want: " Mixed.Case ", match: true},
		{raw: "{{ .Table }}", output: " .quoted. ", want: " .quoted. ", match: true},
		{raw: "pre {{- .Table -}} post", output: "prevaluepost", want: "value", match: true},
		{raw: "pre{{ .Table }}post", output: "wrongvaluepost"},
		{raw: "pre{{ .Table }}post", output: "prevaluewrong"},
		{raw: "pre{{ .Table }}post", output: "pre"},
	}
	for _, test := range tests {
		compiled, err := Parse(test.raw, Table)
		if err != nil {
			t.Fatalf("Parse(%q): %v", test.raw, err)
		}
		got, match := compiled.Inverse(test.output)
		if match != test.match || got != test.want {
			t.Errorf("Inverse(%q, %q) = %q, %t; want %q, %t", test.raw, test.output, got, match, test.want, test.match)
		}
	}
	if got, match := (Template{}).Inverse("value"); match || got != "" {
		t.Fatalf("zero Template.Inverse() = %q, %t", got, match)
	}
}

func TestExpandRejectsInvalidOutputAndZeroTemplate(t *testing.T) {
	t.Parallel()
	if _, err := (Template{}).Expand(Data{Table: "identifier"}); err == nil || !strings.Contains(err.Error(), "not compiled") {
		t.Fatalf("zero Template.Expand() error = %v", err)
	}
	compiled, err := Parse("{{ .Table }}", Table)
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range []Data{{}, {Table: "contains\x00nul"}} {
		if _, err := compiled.Expand(data); err == nil {
			t.Fatalf("Expand(%q) succeeded", data.Table)
		}
	}
}

func TestCompiledTemplateIsSafeForConcurrentExpansion(t *testing.T) {
	t.Parallel()
	compiled, err := Parse("pre{{ .Table }}post", Table)
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for index := 0; index < 64; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for iteration := 0; iteration < 100; iteration++ {
				got, err := compiled.Expand(Data{Table: "{{/* comment */}}{{ .Column }} 日本語"})
				if err != nil || got != "pre{{/* comment */}}{{ .Column }} 日本語post" {
					t.Errorf("Expand() = %q, %v", got, err)
					return
				}
			}
		}()
	}
	wait.Wait()
}

func TestContainsExecutableAction(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"{{ .Table }}", "prefix{{ print \"x\" }}", "{{ if true }}x{{ end }}", "{{ define \"x\" }}x{{ end }}", "a{{/* comment */}}b", "a{{- /* comment */ -}}b"} {
		if !ContainsExecutableAction(value) {
			t.Errorf("ContainsExecutableAction(%q) = false", value)
		}
	}
	for _, value := range []string{"literal", "a{b}", "{table}", "{{", "{{ not-a-valid-action }}"} {
		if ContainsExecutableAction(value) {
			t.Errorf("ContainsExecutableAction(%q) = true", value)
		}
	}
}

func FuzzExpandIsInjectiveAndPreservesInjectedBytes(f *testing.F) {
	for _, seed := range []string{"identifier", "{{ .Schema }}", "日本語", "a.b", "\"quoted\"", " leading", "trailing ", " \t "} {
		f.Add(seed, seed+"x")
	}
	compiled, err := Parse("prefix{{ .Table }}suffix", Table)
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, left, right string) {
		if strings.IndexByte(left, 0) >= 0 || strings.IndexByte(right, 0) >= 0 {
			t.Skip()
		}
		leftOutput, err := compiled.Expand(Data{Table: left})
		if err != nil {
			t.Fatal(err)
		}
		rightOutput, err := compiled.Expand(Data{Table: right})
		if err != nil {
			t.Fatal(err)
		}
		if leftOutput != "prefix"+left+"suffix" || rightOutput != "prefix"+right+"suffix" {
			t.Fatalf("injected bytes changed: %q => %q; %q => %q", left, leftOutput, right, rightOutput)
		}
		if inverse, match := compiled.Inverse(leftOutput); !match || inverse != left {
			t.Fatalf("inverse changed injected bytes: %q => %q => %q, %t", left, leftOutput, inverse, match)
		}
		if inverse, match := compiled.Inverse(rightOutput); !match || inverse != right {
			t.Fatalf("inverse changed injected bytes: %q => %q => %q, %t", right, rightOutput, inverse, match)
		}
		if left != right && leftOutput == rightOutput {
			t.Fatalf("distinct inputs %q and %q collided at %q", left, right, leftOutput)
		}
	})
}
