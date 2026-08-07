package typemapping

import (
	"reflect"
	"testing"
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

func TestLoadNativeInlineMappings(t *testing.T) {
	got, err := Load(map[string]string{OptTypeMappings: `{"text":"INLINE"}`})
	if err != nil {
		t.Fatal(err)
	}
	if got["text"] != "INLINE" {
		t.Fatalf("Load() = %#v", got)
	}
	missing, err := Load(map[string]string{})
	if err != nil || missing != nil {
		t.Fatalf("Load(empty) = %#v, %v", missing, err)
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
