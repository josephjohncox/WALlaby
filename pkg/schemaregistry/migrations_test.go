package schemaregistry

import (
	"strings"
	"testing"
)

func TestUniqueSubjectVersionMigrationFailsWithRepairGuidance(t *testing.T) {
	t.Parallel()

	contents, err := migrationFS.ReadFile("migrations/002_unique_subject_version.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	migration := string(contents)
	for _, required := range []string{
		"HAVING count(*) > 1",
		"ERRCODE = '23505'",
		"cannot enforce unique subject/version",
		"without changing externally published schema versions",
		"CREATE UNIQUE INDEX IF NOT EXISTS",
	} {
		if !strings.Contains(migration, required) {
			t.Fatalf("migration lacks %q", required)
		}
	}
}
