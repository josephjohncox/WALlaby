package mappingtemplate

import (
	"strings"
	"testing"
)

func TestParseAndExpandPreservesIdentifierBytes(t *testing.T) {
	template, err := Parse("raw_{table}_v1", Table)
	if err != nil {
		t.Fatal(err)
	}
	identifier := " Mixed Case.日本語 "
	if got, want := template.Expand(identifier), "raw_"+identifier+"_v1"; got != want {
		t.Fatalf("Expand() = %q, want %q", got, want)
	}
}

func TestZeroTemplateDoesNotExpand(t *testing.T) {
	if got := (Template{}).Expand("identifier"); got != "" {
		t.Fatalf("zero Template.Expand() = %q", got)
	}
}

func TestParseRejectsInvalidTemplates(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		component Component
		want      string
	}{
		{name: "empty", component: Table, want: "required"},
		{name: "leading whitespace", raw: " {table}", component: Table, want: "whitespace"},
		{name: "trailing whitespace", raw: "{table} ", component: Table, want: "whitespace"},
		{name: "missing", raw: "fixed", component: Table, want: "exactly one {table}"},
		{name: "duplicate", raw: "{table}_{table}", component: Table, want: "exactly one {table}"},
		{name: "other placeholder", raw: "{schema}_{table}", component: Table, want: "placeholders other than {table}"},
		{name: "unbalanced opening brace", raw: "x{{table}", component: Table, want: "placeholders other than {table}"},
		{name: "unbalanced closing brace", raw: "{table}}x", component: Table, want: "placeholders other than {table}"},
		{name: "unknown component", raw: "{unknown}", component: Component("unknown"), want: "unsupported template component"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse(test.raw, test.component)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Parse() error = %v, want %q", err, test.want)
			}
		})
	}
}
