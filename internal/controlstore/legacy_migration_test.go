package controlstore

import (
	"context"
	"fmt"
	"os"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestControlStoreLegacyLedgerImportDoesNotReplaySQL exercises the exact
// main->HEAD upgrade path: a populated legacy per-domain ledger is imported into
// the centralized wallaby_control_migrations ledger WITHOUT replaying the already
// applied migration SQL, while genuinely new migrations still run. This is the
// previously untested import branch of ApplyMigrations/loadLegacyHistory.
func TestControlStoreLegacyLedgerImportDoesNotReplaySQL(t *testing.T) {
	ctx, pool := newControlstoreMigrationFixture(t)
	const (
		domain      = "legacy_import_demo"
		legacyTable = "wallaby_legacy_demo_migrations"
	)

	// Seed a populated legacy ledger declaring 001 already applied, and
	// pre-create the table that 001 would create WITHOUT IF NOT EXISTS plus a
	// sentinel row. Correct import skips replay; a wrong replay would either error
	// on the duplicate table or drop the sentinel.
	if _, err := pool.Exec(ctx, `CREATE TABLE `+legacyTable+` (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO `+legacyTable+`(version) VALUES ('001_init.sql')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE legacy_demo_domain_state (id INT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO legacy_demo_domain_state(id) VALUES (42)`); err != nil {
		t.Fatal(err)
	}

	migrations := fstest.MapFS{
		// Replaying this against the pre-created table would fail; it must be
		// imported from the legacy ledger without execution.
		"migrations/001_init.sql": &fstest.MapFile{Data: []byte("CREATE TABLE legacy_demo_domain_state (id INT PRIMARY KEY);")},
		// A genuinely new migration that must be applied.
		"migrations/002_add.sql": &fstest.MapFile{Data: []byte("CREATE TABLE IF NOT EXISTS legacy_demo_domain_extra (id INT PRIMARY KEY);")},
	}

	if err := ApplyMigrations(ctx, pool, domain, migrations, "migrations/*.sql", legacyTable); err != nil {
		t.Fatalf("apply migrations with legacy import: %v", err)
	}

	// 001 was imported without replay: the sentinel row survives.
	var sentinel int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM legacy_demo_domain_state WHERE id=42`).Scan(&sentinel); err != nil {
		t.Fatal(err)
	}
	if sentinel != 1 {
		t.Fatalf("legacy 001 SQL was replayed instead of imported; sentinel rows=%d", sentinel)
	}

	// Both versions are recorded in the centralized ledger with checksums.
	for _, version := range []string{"001_init.sql", "002_add.sql"} {
		var checksum string
		if err := pool.QueryRow(ctx, `SELECT sql_checksum FROM wallaby_control_migrations WHERE domain=$1 AND version=$2`, domain, version).Scan(&checksum); err != nil {
			t.Fatalf("centralized ledger missing %s: %v", version, err)
		}
		if checksum == "" {
			t.Fatalf("centralized ledger recorded empty checksum for %s", version)
		}
	}

	// The new migration 002 actually ran.
	var extraExists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('legacy_demo_domain_extra') IS NOT NULL`).Scan(&extraExists); err != nil {
		t.Fatal(err)
	}
	if !extraExists {
		t.Fatal("new migration 002 was not applied")
	}

	// The legacy ledger is dual-recorded for 002 as well.
	var legacyVersions int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+legacyTable+``).Scan(&legacyVersions); err != nil {
		t.Fatal(err)
	}
	if legacyVersions != 2 {
		t.Fatalf("legacy ledger versions=%d, want 2 (001 seeded + 002 dual-recorded)", legacyVersions)
	}

	// Re-running is idempotent: no replay, no checksum drift, no error.
	if err := ApplyMigrations(ctx, pool, domain, migrations, "migrations/*.sql", legacyTable); err != nil {
		t.Fatalf("idempotent re-run after import: %v", err)
	}
}

func newControlstoreMigrationFixture(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	schema := fmt.Sprintf("wallaby_controlstore_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS "+identifier+" CASCADE")
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return ctx, pool
}
