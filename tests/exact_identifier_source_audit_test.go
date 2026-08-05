package tests

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestExactPostgresIdentifierProductionPathsDoNotNormalizeOrSplitIdentity(t *testing.T) {
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate audit source")
	}
	root := filepath.Dir(filepath.Dir(current))
	for file, forbidden := range map[string][]string{
		"internal/bootstrap/tasks.go": {
			"strings.TrimSpace(c.Schema.Namespace)", "strings.TrimSpace(c.Schema.Name)",
			"strings.TrimSpace(task.Namespace)", "strings.TrimSpace(task.Table)",
		},
		"internal/tablemap/ddl.go": {"strings.TrimSpace(change.ToColumn)"},
		"connectors/destinations/snowflake/managed_open.go": {
			`strings.TrimSpace(options["managed_source_schema"])`, `strings.TrimSpace(options["managed_source_table"])`,
		},
		"connectors/destinations/snowflake/managed_plan.go": {"strings.TrimSpace(record.Table)"},
		"pkg/stream/runner.go": {
			"strings.TrimSpace(sourceSchema)", "strings.TrimSpace(sourceTable)",
		},
		"internal/runner/managed_admission.go": {
			`strings.TrimSpace(options["managed_source_schema"])`, `strings.TrimSpace(options["managed_source_table"])`,
		},
		"connectors/destinations/postgres/destination.go": {
			`strings.Split(target, ".")`, `strings.Split(trimmed, ".")`, `strings.Split(name, ".")`,
		},
		"connectors/sources/postgres/backfill.go": {
			"strings.TrimSpace(spec.Options[optPartitionColumn])", `strings.Split(value, ".")`, "schemas := parseCSV",
		},
		"connectors/sources/postgres/source.go": {
			"strings.TrimSpace(spec.Options[optDDLTriggerSchema])", "strings.TrimSpace(spec.Options[optDDLTriggerName])", "publicationSchemas := parseCSV",
		},
		"connectors/sources/postgres/managed_bootstrap.go": {"schemas := parseCSV", "relation.Namespace+\".\"+relation.Table"},
	} {
		body, err := os.ReadFile(filepath.Join(root, file))
		if err != nil {
			t.Fatal(err)
		}
		for _, pattern := range forbidden {
			if strings.Contains(string(body), pattern) {
				t.Errorf("%s contains forbidden PostgreSQL identity normalization/reparse %q", file, pattern)
			}
		}
	}
}
