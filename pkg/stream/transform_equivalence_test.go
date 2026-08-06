package stream

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/internal/ddl"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/internal/typemapping"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestTypeMappingFileReplacementAndInlinePrecedenceMatchStreamAndDDLPaths(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mappings.yaml")
	if err := os.WriteFile(path, []byte("text: VARCHAR\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	assertTypeMappingPaths(t, map[string]string{typemapping.OptTypeMappingsFile: path}, "VARCHAR")

	if err := os.WriteFile(path, []byte("text: STRING \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, time.Now(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	assertTypeMappingPaths(t, map[string]string{typemapping.OptTypeMappingsFile: path}, "STRING")

	assertTypeMappingPaths(t, map[string]string{
		typemapping.OptTypeMappings:     `{"text":"INLINE"}`,
		typemapping.OptTypeMappingsFile: filepath.Join(t.TempDir(), "missing.yaml"),
	}, "INLINE")
}

func assertTypeMappingPaths(t *testing.T, options map[string]string, want string) {
	t.Helper()
	batch, _, err := transformBatchForDestination(connector.Batch{Schema: connector.Schema{Name: "events", Columns: []connector.Column{{Name: "payload", Type: "text"}}}}, connector.Spec{Options: options}, nil)
	if err != nil {
		t.Fatalf("stream transform: %v", err)
	}
	if got := batch.Schema.Columns[0].Type; got != want {
		t.Fatalf("stream mapped type = %q, want %q", got, want)
	}
	statements, err := ddl.TranslatePlanDDL(connector.Schema{Name: "events"}, internalschema.Plan{Changes: []internalschema.Change{{Type: internalschema.ChangeAddColumn, Column: "payload", ToType: "text", Nullable: true}}}, ddl.DialectConfigFor(ddl.DialectPostgres), nil, options)
	if err != nil {
		t.Fatalf("DDL transform: %v", err)
	}
	if len(statements) != 1 || !strings.Contains(statements[0], want) {
		t.Fatalf("DDL statements = %#v, want mapped type %q", statements, want)
	}
}

func TestTypeMappingLoadErrorsMatchStreamAndDDLPaths(t *testing.T) {
	malformed := filepath.Join(t.TempDir(), "malformed.yaml")
	if err := os.WriteFile(malformed, []byte("[not: a: mapping"), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		options map[string]string
	}{
		{name: "missing", options: map[string]string{typemapping.OptTypeMappingsFile: filepath.Join(t.TempDir(), "missing.yaml")}},
		{name: "unreadable directory", options: map[string]string{typemapping.OptTypeMappingsFile: t.TempDir()}},
		{name: "malformed", options: map[string]string{typemapping.OptTypeMappingsFile: malformed}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, streamErr := transformBatchForDestination(connector.Batch{}, connector.Spec{Options: test.options}, nil)
			_, ddlErr := ddl.TranslatePlanDDL(connector.Schema{Name: "events"}, internalschema.Plan{}, ddl.DialectConfigFor(ddl.DialectPostgres), nil, test.options)
			if streamErr == nil || ddlErr == nil {
				t.Fatalf("errors stream=%v ddl=%v", streamErr, ddlErr)
			}
			if streamErr.Error() != ddlErr.Error() {
				t.Fatalf("error mismatch:\nstream: %v\nddl:    %v", streamErr, ddlErr)
			}
		})
	}
}

func TestTypeMappingParsingAndMergeMatchesDDLPath(t *testing.T) {
	options := map[string]string{
		typemapping.OptTypeMappings: "  DOUBLE   PRECISION : FLOAT64\next:PostGIS.geometry: GEOGRAPHY\n",
	}
	overrides, err := typemapping.Load(options)
	if err != nil {
		t.Fatal(err)
	}

	base := map[string]string{"  CHARACTER   VARYING ": " VARCHAR ", "double precision": "DOUBLE"}
	streamMerged := mergeTypeMappings(base, overrides)
	ddlMerged := ddl.MergeTypeMappings(base, overrides)
	if !reflect.DeepEqual(streamMerged, ddlMerged) {
		t.Fatalf("stream merge = %#v, DDL merge = %#v", streamMerged, ddlMerged)
	}
}
