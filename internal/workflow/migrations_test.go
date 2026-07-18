package workflow

import (
	"io/fs"
	"slices"
	"strings"
	"testing"
)

func TestWorkflowMigrationsIncludeLifecycleFencingAfterExecutions(t *testing.T) {
	t.Parallel()

	files, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(files, "migrations/004_lifecycle_executions.sql") {
		t.Fatal("lifecycle execution migration 004 is not embedded")
	}
	if !slices.Contains(files, "migrations/005_lifecycle_fencing.sql") {
		t.Fatal("lifecycle fencing migration 005 is not embedded")
	}
	contents, err := migrationFS.ReadFile("migrations/005_lifecycle_fencing.sql")
	if err != nil {
		t.Fatal(err)
	}
	migration := string(contents)
	for _, required := range []string{
		"lifecycle_target",
		"lifecycle_generation",
		"dispatch_pending",
		"generation BIGINT",
		"heartbeat_at",
		"lease_expires_at",
		"finish_reason",
		"state IN ('running', 'stopping')",
		"requires a quiesced upgrade",
		"WHERE dispatch_pending OR state = 'stopping' OR lifecycle_target <> state",
	} {
		if !strings.Contains(migration, required) {
			t.Fatalf("migration 005 is missing %q", required)
		}
	}
	if strings.Contains(migration, "WHEN 'stopping' THEN 'stopped'") {
		t.Fatal("migration 005 must not hide interrupted stopping rows by translating stopping to stopped")
	}
	guard := strings.Index(migration, "state IN ('running', 'stopping')")
	generationColumn := strings.Index(migration, "ADD COLUMN IF NOT EXISTS lifecycle_generation")
	if guard < 0 || generationColumn < 0 || guard > generationColumn {
		t.Fatal("quiesced-upgrade guard must run before lifecycle generations are introduced")
	}
}
