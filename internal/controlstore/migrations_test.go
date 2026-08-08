package controlstore

import (
	"os"
	"runtime"
	"strings"
	"testing"
)

func TestControlMigrationSourceUsesOnlyAuthoritativeLedger(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate controlstore migration source")
	}
	raw, err := os.ReadFile(strings.TrimSuffix(filename, "_test.go") + ".go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	for _, required := range []string{"wallaby_control_migrations", "incompatible migration ledger(s)", "checksum drift", "c.relname ~ '^wallaby.*_migrations$'"} {
		if !strings.Contains(source, required) {
			t.Fatalf("control migration source missing %q", required)
		}
	}
	for _, forbidden := range []string{"legacyTable", "loadLegacyHistory", "import %s migration", "record legacy", "CREATE TABLE IF NOT EXISTS wallaby_schema_migrations", "CREATE TABLE IF NOT EXISTS wallaby_checkpoint_migrations", "CREATE TABLE IF NOT EXISTS wallaby_registry_migrations"} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("control migration source retains compatibility token %q", forbidden)
		}
	}
}
