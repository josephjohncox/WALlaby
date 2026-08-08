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

// TestControlStoreMigrationLedgerDoesNotReplaySQL proves the centralized
// current-schema ledger applies each migration once and does not replay SQL on
// an idempotent startup. Legacy per-domain ledger import is intentionally not
// supported.
func TestControlStoreMigrationLedgerDoesNotReplaySQL(t *testing.T) {
	ctx, pool := newControlstoreMigrationFixture(t)
	domain := fmt.Sprintf("current_ledger_demo_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM public.wallaby_control_migrations WHERE domain=$1`, domain)
	})
	migrations := fstest.MapFS{
		"migrations/001_init.sql": &fstest.MapFile{Data: []byte("CREATE TABLE current_demo_domain_state (id INT PRIMARY KEY);")},
		"migrations/002_add.sql":  &fstest.MapFile{Data: []byte("CREATE TABLE current_demo_domain_extra (id INT PRIMARY KEY);")},
	}
	if err := ApplyMigrations(ctx, pool, domain, migrations, "migrations/*.sql"); err != nil {
		t.Fatalf("apply current migrations: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO current_demo_domain_state(id) VALUES (42)`); err != nil {
		t.Fatal(err)
	}
	if err := ApplyMigrations(ctx, pool, domain, migrations, "migrations/*.sql"); err != nil {
		t.Fatalf("idempotent migration replay: %v", err)
	}
	var sentinel, versions int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM current_demo_domain_state WHERE id=42`).Scan(&sentinel); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.wallaby_control_migrations WHERE domain=$1 AND sql_checksum<>''`, domain).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if sentinel != 1 || versions != 2 {
		t.Fatalf("current ledger sentinel/versions=%d/%d, want 1/2", sentinel, versions)
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
