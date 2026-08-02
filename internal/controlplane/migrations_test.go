package controlplane

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/registry"
)

func TestMigrationDomainsKeepAuthorityBeforeMutations(t *testing.T) {
	want := []string{"workflow", "checkpoint", "registry", "controlplane", "delivery", "bootstrap", "artifactlog"}
	if got := MigrationDomains(); !reflect.DeepEqual(got, want) {
		t.Fatalf("migration order=%v, want %v", got, want)
	}
}

func TestRegistryOnlyLedgerIsRepairedByCentralizedMigration(t *testing.T) {
	ctx, pool := newControlplaneMigrationFixture(t)
	if err := registry.ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"006_run_fencing.sql", "007_authority_protocol_v2.sql"} {
		var applied bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS (
SELECT 1 FROM wallaby_control_migrations WHERE domain='registry' AND version=$1
)`, version).Scan(&applied); err != nil {
			t.Fatal(err)
		}
		if !applied {
			t.Fatalf("registry-only migration ledger omitted %s", version)
		}
	}
	for _, table := range []string{"ddl_execution_run_attempts", "schema_publication_operations"} {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatalf("registry-only history unexpectedly created workflow-dependent table %s", table)
		}
	}

	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("centralized repair: %v", err)
	}
	var repaired bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (
SELECT 1 FROM wallaby_control_migrations WHERE domain='controlplane' AND version='001_registry_authority_repair.sql'
)`).Scan(&repaired); err != nil {
		t.Fatal(err)
	}
	if !repaired {
		t.Fatal("centralized repair was not recorded in its distinct controlplane ledger domain")
	}
	repairSQL, err := migrationFS.ReadFile("migrations/001_registry_authority_repair.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(repairSQL)); err != nil {
		t.Fatalf("idempotent centralized repair replay: %v", err)
	}
	if err := verifyManagedAuthoritySchema(ctx, pool); err != nil {
		t.Fatalf("repaired managed authority schema: %v", err)
	}
}

func TestManagedAuthoritySchemaVerificationFailsClosed(t *testing.T) {
	ctx, pool := newControlplaneMigrationFixture(t)
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}

	assertRejected := func(name, breakSQL, restoreSQL, want string) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, breakSQL); err != nil {
				t.Fatal(err)
			}
			if err := verifyManagedAuthoritySchema(ctx, pool); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("verification error=%v, want %q", err, want)
			}
			if _, err := pool.Exec(ctx, restoreSQL); err != nil {
				t.Fatal(err)
			}
			if err := verifyManagedAuthoritySchema(ctx, pool); err != nil {
				t.Fatalf("verification after restore: %v", err)
			}
		})
	}
	assertRejected(
		"disabled trigger",
		"ALTER TABLE flows DISABLE TRIGGER flows_require_authority_v2",
		"ALTER TABLE flows ENABLE TRIGGER flows_require_authority_v2",
		"trigger coverage",
	)
	assertRejected(
		"missing column",
		"ALTER TABLE ddl_execution_run_attempts RENAME COLUMN flow_id TO broken_flow_id",
		"ALTER TABLE ddl_execution_run_attempts RENAME COLUMN broken_flow_id TO flow_id",
		"missing required columns",
	)
	assertRejected(
		"missing constraint",
		"ALTER TABLE ddl_execution_run_attempts DROP CONSTRAINT ddl_execution_run_attempts_owner_key",
		"ALTER TABLE ddl_execution_run_attempts ADD CONSTRAINT ddl_execution_run_attempts_owner_key UNIQUE(event_id,destination,acquisition_id,lease_epoch)",
		"missing or unvalidated required FK/constraints",
	)
	assertRejected(
		"missing index",
		"ALTER INDEX ddl_execution_run_attempts_event_destination_idx RENAME TO broken_attempt_index",
		"ALTER INDEX broken_attempt_index RENAME TO ddl_execution_run_attempts_event_destination_idx",
		"missing or invalid required indexes",
	)
	const restoreActiveResourceIndex = `DROP INDEX source_resources_active_physical_name_unique;
CREATE UNIQUE INDEX source_resources_active_physical_name_unique
ON source_resources(source_system_id,database_name,resource_kind,physical_name)
WHERE state <> 'retired'`
	assertRejected(
		"physical resource index must be unique",
		`DROP INDEX source_resources_active_physical_name_unique;
CREATE INDEX source_resources_active_physical_name_unique
ON source_resources(source_system_id,database_name,resource_kind,physical_name)
WHERE state <> 'retired'`,
		restoreActiveResourceIndex,
		"requires source_resources_active_physical_name_unique to be a unique index",
	)
	assertRejected(
		"physical resource index columns are exact",
		`DROP INDEX source_resources_active_physical_name_unique;
CREATE UNIQUE INDEX source_resources_active_physical_name_unique
ON source_resources(source_system_id,database_name,physical_name)
WHERE state <> 'retired'`,
		restoreActiveResourceIndex,
		"requires source_resources_active_physical_name_unique to be a unique index",
	)
	assertRejected(
		"physical resource index predicate is exact",
		`DROP INDEX source_resources_active_physical_name_unique;
CREATE UNIQUE INDEX source_resources_active_physical_name_unique
ON source_resources(source_system_id,database_name,resource_kind,physical_name)
WHERE state = 'ready'`,
		restoreActiveResourceIndex,
		"requires source_resources_active_physical_name_unique to be a unique index",
	)
	assertRejected(
		"missing table",
		"ALTER TABLE ddl_execution_run_attempts RENAME TO broken_run_attempts",
		"ALTER TABLE broken_run_attempts RENAME TO ddl_execution_run_attempts",
		"missing required tables",
	)
}

func newControlplaneMigrationFixture(t *testing.T) (context.Context, *pgxpool.Pool) {
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
	schema := fmt.Sprintf("wallaby_controlplane_%d", time.Now().UnixNano())
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
	config.ConnConfig.RuntimeParams["wallaby.authority_protocol"] = "v2"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return ctx, pool
}
