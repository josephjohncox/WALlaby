package tests

import (
	"context"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/josephjohncox/wallaby/internal/controlstore"
)

var controlstoreMigrationFixture = fstest.MapFS{
	"migrations/001_current.sql": {Data: []byte(`CREATE TABLE current_control_probe(id bigint PRIMARY KEY)`)},
}

func TestControlstoreFreshDatabaseUsesOnlyAuthoritativeLedger(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, cleanup := newDeliveryMigrationDatabase(t, ctx, dsn, "control_current")
	defer cleanup()
	if err := controlstore.ApplyMigrations(ctx, pool, "testdomain", controlstoreMigrationFixture, "migrations/*.sql"); err != nil {
		t.Fatal(err)
	}
	var history, probe int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallaby_control_migrations WHERE domain='testdomain' AND version='001_current.sql' AND sql_checksum<>''`).Scan(&history); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM current_control_probe`).Scan(&probe); err != nil {
		t.Fatal(err)
	}
	if history != 1 || probe != 0 {
		t.Fatalf("authoritative history/probe=%d/%d", history, probe)
	}
	if _, err := pool.Exec(ctx, `UPDATE wallaby_control_migrations SET sql_checksum='conflict' WHERE domain='testdomain' AND version='001_current.sql'`); err != nil {
		t.Fatal(err)
	}
	if err := controlstore.ApplyMigrations(ctx, pool, "testdomain", controlstoreMigrationFixture, "migrations/*.sql"); err == nil || !strings.Contains(err.Error(), "checksum drift") {
		t.Fatalf("checksum drift error=%v", err)
	}
}

func TestControlstoreRejectsNonPrefixAuthoritativeHistory(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, cleanup := newDeliveryMigrationDatabase(t, ctx, dsn, "control_nonprefix")
	defer cleanup()
	if _, err := pool.Exec(ctx, `CREATE TABLE public.wallaby_control_migrations(domain text NOT NULL,version text NOT NULL,sql_checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(domain,version));INSERT INTO public.wallaby_control_migrations(domain,version,sql_checksum)VALUES('testdomain','999_unknown.sql','unknown')`); err != nil {
		t.Fatal(err)
	}
	err := controlstore.ApplyMigrations(ctx, pool, "testdomain", controlstoreMigrationFixture, "migrations/*.sql")
	if err == nil || !strings.Contains(err.Error(), "not an ordered") {
		t.Fatalf("non-prefix history error=%v", err)
	}
	var probe bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('current_control_probe') IS NOT NULL`).Scan(&probe); err != nil {
		t.Fatal(err)
	}
	if probe {
		t.Fatal("non-prefix history applied migration SQL")
	}
}

func TestControlstoreRejectsOldOrExtraMigrationLedgersWithoutImport(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	for _, ledger := range []string{"wallaby_schema_migrations", "wallaby_checkpoint_migrations", "wallaby_registry_migrations", "wallaby_stream_migrations", "wallaby_schema_registry_migrations", "wallaby_extra_migrations"} {
		t.Run(ledger, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			pool, cleanup := newDeliveryMigrationDatabase(t, ctx, dsn, "control_old_"+strings.TrimPrefix(ledger, "wallaby_"))
			defer cleanup()
			if _, err := pool.Exec(ctx, `CREATE TABLE `+ledger+`(version text PRIMARY KEY);INSERT INTO `+ledger+` VALUES('001_current.sql')`); err != nil {
				t.Fatal(err)
			}
			err := controlstore.ApplyMigrations(ctx, pool, "testdomain", controlstoreMigrationFixture, "migrations/*.sql")
			if err == nil || !strings.Contains(err.Error(), "incompatible migration ledger") || !strings.Contains(err.Error(), ledger) || !strings.Contains(err.Error(), "not imported") {
				t.Fatalf("old ledger error=%v", err)
			}
			var authoritative, probe bool
			if err := pool.QueryRow(ctx, `SELECT to_regclass('wallaby_control_migrations') IS NOT NULL,to_regclass('current_control_probe') IS NOT NULL`).Scan(&authoritative, &probe); err != nil {
				t.Fatal(err)
			}
			if authoritative || probe {
				t.Fatalf("old ledger was imported or migration replayed: authoritative/probe=%t/%t", authoritative, probe)
			}
		})
	}
}

func TestControlstoreRejectsConflictingExtraLedgerBesideCurrentHistory(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, cleanup := newDeliveryMigrationDatabase(t, ctx, dsn, "control_conflict")
	defer cleanup()
	if err := controlstore.ApplyMigrations(ctx, pool, "testdomain", controlstoreMigrationFixture, "migrations/*.sql"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE wallaby_conflicting_migrations(version text PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	err := controlstore.ApplyMigrations(ctx, pool, "testdomain", controlstoreMigrationFixture, "migrations/*.sql")
	if err == nil || !strings.Contains(err.Error(), "wallaby_conflicting_migrations") {
		t.Fatalf("conflicting ledger error=%v", err)
	}
	var history int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallaby_control_migrations WHERE domain='testdomain'`).Scan(&history); err != nil {
		t.Fatal(err)
	}
	if history != 1 {
		t.Fatalf("current migration history mutated after conflict: %d", history)
	}
	if _, err := pool.Exec(ctx, `DROP TABLE wallaby_conflicting_migrations;CREATE SCHEMA shadow;CREATE TABLE shadow.wallaby_control_migrations(domain text)`); err != nil {
		t.Fatal(err)
	}
	err = controlstore.ApplyMigrations(ctx, pool, "testdomain", controlstoreMigrationFixture, "migrations/*.sql")
	if err == nil || !strings.Contains(err.Error(), "shadow.wallaby_control_migrations") {
		t.Fatalf("shadow authoritative-ledger conflict error=%v", err)
	}
}
