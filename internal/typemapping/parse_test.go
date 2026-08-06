package typemapping

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseJSONAndYAMLEquivalence(t *testing.T) {
	jsonMappings, err := Parse(`{"  DOUBLE   PRECISION ": " FLOAT64 ", "EXT:PostGIS.Geometry": " GEOGRAPHY "}`)
	if err != nil {
		t.Fatal(err)
	}
	yamlMappings, err := Parse("DOUBLE PRECISION: FLOAT64\next:postgis.geometry: GEOGRAPHY\n")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(jsonMappings, yamlMappings) {
		t.Fatalf("JSON = %#v, YAML = %#v", jsonMappings, yamlMappings)
	}
}

func TestNormalizeKey(t *testing.T) {
	if got, want := NormalizeKey("  DOUBLE\t precision\n"), "double precision"; got != want {
		t.Fatalf("NormalizeKey() = %q, want %q", got, want)
	}
}

func TestLoadObservesFileReplacementWithPreservedMtime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mappings.yaml")
	if err := os.WriteFile(path, []byte("text: VARCHAR\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := Load(map[string]string{OptTypeMappingsFile: path})
	if err != nil {
		t.Fatal(err)
	}
	if first["text"] != "VARCHAR" {
		t.Fatalf("first Load() = %#v", first)
	}
	if err := os.WriteFile(path, []byte("text: STRING \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, time.Now(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	second, err := Load(map[string]string{OptTypeMappingsFile: filepath.Join(filepath.Dir(path), ".", filepath.Base(path))})
	if err != nil {
		t.Fatal(err)
	}
	if second["text"] != "STRING" {
		t.Fatalf("second Load() = %#v", second)
	}
}

func TestLoadInlinePrecedesFile(t *testing.T) {
	got, err := Load(map[string]string{
		OptTypeMappings:     `{"text":"INLINE"}`,
		OptTypeMappingsFile: filepath.Join(t.TempDir(), "missing.yaml"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got["text"] != "INLINE" {
		t.Fatalf("Load() = %#v", got)
	}
}

func TestLoadFileErrors(t *testing.T) {
	tests := []struct {
		name string
		path func(t *testing.T) string
		want string
	}{
		{name: "missing", path: func(t *testing.T) string { return filepath.Join(t.TempDir(), "missing.yaml") }, want: "read type mappings file:"},
		{name: "unreadable directory", path: func(t *testing.T) string { return t.TempDir() }, want: "read type mappings file:"},
		{name: "malformed", path: func(t *testing.T) string {
			path := filepath.Join(t.TempDir(), "malformed.yaml")
			if err := os.WriteFile(path, []byte("[not: a: mapping"), 0o600); err != nil {
				t.Fatal(err)
			}
			return path
		}, want: "parse type_mappings:"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Load(map[string]string{OptTypeMappingsFile: test.path(t)})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestParseCanonicalCollisionIsDeterministic(t *testing.T) {
	const raw = `{"A  B":"first","a b":"second"}`
	for range 100 {
		mappings, err := Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := mappings["a b"]; got != "second" {
			t.Fatalf("collision result = %q", got)
		}
	}
}
