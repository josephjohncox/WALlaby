package ddl

import (
	"strings"
	"testing"

	"pgregory.net/rapid"
)

func TestNormalizeGeneratedValueRapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		choice := rapid.IntRange(0, 4).Draw(t, "choice")
		var input any
		switch choice {
		case 0:
			input = rapid.StringMatching(`\s*[a-zA-Z0-9_-]{0,8}\s*`).Draw(t, "string")
		case 1:
			input = []byte(rapid.StringMatching(`\s*[a-zA-Z0-9_-]{0,8}\s*`).Draw(t, "bytes"))
		case 2:
			input = byte(rapid.IntRange(0, 120).Draw(t, "byte"))
		case 3:
			input = rune(rapid.IntRange(32, 126).Draw(t, "rune"))
		default:
			input = rapid.IntRange(-100, 100).Draw(t, "int")
		}

		value := normalizeGeneratedValue(input)
		if strings.TrimSpace(value) != value {
			t.Fatalf("expected normalized value to be trimmed")
		}
		if normalizeGeneratedValue(value) != value {
			t.Fatalf("expected normalizeGeneratedValue to be idempotent")
		}
	})
}

func TestFormatTypeNameRapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.SampledFrom([]string{"", "pg_catalog", "pg_toast", "public", "custom"}).Draw(t, "schema")
		formatted := rapid.StringMatching(`[A-Za-z_]{1,8}`).Draw(t, "formatted")
		formatted = strings.TrimSpace(formatted)

		out := formatTypeName(schema, formatted)
		base := strings.ToLower(strings.TrimSpace(formatted))

		switch schema {
		case "", "pg_catalog", "pg_toast":
			if out != base {
				t.Fatalf("expected base type %q, got %q", base, out)
			}
		default:
			if !strings.HasPrefix(out, strings.ToLower(schema)+".") {
				t.Fatalf("expected schema prefix for %q, got %q", schema, out)
			}
		}
	})
}

func TestFormatTypeNameWithDotRapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.SampledFrom([]string{"", "public", "custom"}).Draw(t, "schema")
		formatted := rapid.StringMatching(`[A-Za-z_]{1,4}\.[A-Za-z_]{1,4}`).Draw(t, "formatted")

		out := formatTypeName(schema, formatted)
		if out != strings.ToLower(strings.TrimSpace(formatted)) {
			t.Fatalf("expected formatted type preserved, got %q", out)
		}
	})
}
