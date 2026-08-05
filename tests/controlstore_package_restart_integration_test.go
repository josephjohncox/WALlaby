package tests

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/controlplane"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/pgstream"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
)

func TestControlstorePackageSchemasSurviveRestartWithOneLedger(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, cleanup := newDeliveryMigrationDatabase(t, ctx, dsn, "package_restart")
	defer cleanup()
	if err := controlplane.ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	preparedDSN := isolatedDatabaseDSN(t, ctx, pool, dsn)

	store, err := pgstream.NewStore(ctx, preparedDSN)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Enqueue(ctx, "restart-stream", []pgstream.Message{{Stream: "restart-stream", Namespace: "public", Table: "restart", LSN: "0/10", WireFormat: connector.WireFormatJSON, Payload: []byte(`{"id":1}`), RegistrySubject: "restart-value", RegistryID: "1", RegistryVersion: 1}}); err != nil {
		store.Close()
		t.Fatal(err)
	}
	store.Close()

	registry, err := schemaregistry.NewRegistry(ctx, schemaregistry.Config{Type: "postgres", DSN: preparedDSN})
	if err != nil {
		t.Fatal(err)
	}
	request := schemaregistry.RegisterRequest{Subject: "restart-value", SchemaType: schemaregistry.SchemaTypeAvro, Schema: `{"type":"record","name":"Restart","fields":[]}`}
	firstRegistration, err := registry.Register(ctx, request)
	if err != nil {
		_ = registry.Close()
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}

	if err := controlplane.ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("restart migration: %v", err)
	}
	store, err = pgstream.NewStore(ctx, preparedDSN)
	if err != nil {
		t.Fatalf("restart pgstream: %v", err)
	}
	claimed, err := store.Claim(ctx, "restart-stream", "restart-group", "restart-consumer", 1, time.Minute)
	store.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || string(claimed[0].Payload) != `{"id":1}` {
		t.Fatalf("restarted pgstream messages=%+v", claimed)
	}
	registry, err = schemaregistry.NewRegistry(ctx, schemaregistry.Config{Type: "postgres", DSN: preparedDSN})
	if err != nil {
		t.Fatalf("restart schema registry: %v", err)
	}
	secondRegistration, err := registry.Register(ctx, request)
	if closeErr := registry.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	if secondRegistration != firstRegistration {
		t.Fatalf("schema registration changed across restart: first=%+v second=%+v", firstRegistration, secondRegistration)
	}

	assertPackageMigrationHistory(t, ctx, pool, "pgstream",
		[]string{"001_init.sql", "002_registry.sql"},
		[]string{"320c0f364fe90e68ce49c9efcec1a752d8d7b456bacead21d5fe3cc7fe2de040", "248103b69cf77651e715e5194e5d4ab1c1f00825957595dcef4209ae0289562a"})
	assertPackageMigrationHistory(t, ctx, pool, "schema_registry",
		[]string{"001_init.sql", "002_unique_subject_version.sql"},
		[]string{"f57e10cda6ff364ac1e46c4254f40db998df64b4573122faca4754317abe0f43", "d5e824a221b7b360af504c943b68dc0644069c6974322427db196e195d752572"})
	var ledgers []string
	if err := pool.QueryRow(ctx, `SELECT COALESCE(array_agg(n.nspname||'.'||c.relname ORDER BY n.nspname,c.relname),'{}'::text[])
FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
WHERE c.relkind IN ('r','p') AND c.relname ~ '^wallaby.*_migrations$'`).Scan(&ledgers); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ledgers, []string{"public.wallaby_control_migrations"}) {
		t.Fatalf("migration ledgers=%v", ledgers)
	}
}

func TestPackageConstructorsRequireExplicitAuthoritativeMigration(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, cleanup := newDeliveryMigrationDatabase(t, ctx, dsn, "package_unprepared")
	preparedDSN := isolatedDatabaseDSN(t, ctx, pool, dsn)
	defer cleanup()
	if store, err := pgstream.NewStore(ctx, preparedDSN); err == nil || !strings.Contains(err.Error(), "run pgstream.ApplyMigrations") {
		if store != nil {
			store.Close()
		}
		t.Fatalf("unprepared pgstream error=%v", err)
	}
	if registry, err := schemaregistry.NewRegistry(ctx, schemaregistry.Config{Type: "postgres", DSN: preparedDSN}); err == nil || !strings.Contains(err.Error(), "run schemaregistry.ApplyMigrations") {
		if registry != nil {
			_ = registry.Close()
		}
		t.Fatalf("unprepared schema registry error=%v", err)
	}
	var created bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.wallaby_control_migrations') IS NOT NULL OR to_regclass('public.stream_events') IS NOT NULL OR to_regclass('public.wallaby_schema_registry') IS NOT NULL`).Scan(&created); err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("package constructors mutated an unprepared database")
	}
}

func TestPackageConstructorsRejectDamagedPreparedSchema(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, cleanup := newDeliveryMigrationDatabase(t, ctx, dsn, "package_damaged")
	defer cleanup()
	if err := controlplane.ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	preparedDSN := isolatedDatabaseDSN(t, ctx, pool, dsn)
	if _, err := pool.Exec(ctx, `ALTER TABLE public.stream_events DROP COLUMN registry_id;ALTER TABLE public.wallaby_schema_registry DROP COLUMN schema_hash CASCADE`); err != nil {
		t.Fatal(err)
	}
	if store, err := pgstream.NewStore(ctx, preparedDSN); err == nil || !strings.Contains(err.Error(), "required public.stream_events shape is absent") {
		if store != nil {
			store.Close()
		}
		t.Fatalf("damaged pgstream error=%v", err)
	}
	if registry, err := schemaregistry.NewRegistry(ctx, schemaregistry.Config{Type: "postgres", DSN: preparedDSN}); err == nil || !strings.Contains(err.Error(), "required public.wallaby_schema_registry shape is absent") {
		if registry != nil {
			_ = registry.Close()
		}
		t.Fatalf("damaged schema registry error=%v", err)
	}
}

func TestControlplaneRestartRejectsTamperedArtifactCatalogAuthority(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN is required")
	}
	type tamperCase struct{ name, sql, want string }
	cases := []tamperCase{
		{name: "dropped_checkpoint_table", sql: `DROP TABLE artifact_consumer_checkpoints`, want: "missing required tables"},
		{name: "dropped_identity_column", sql: `ALTER TABLE artifact_delivery_attempts DROP COLUMN logical_batch_id CASCADE`, want: "missing required columns"},
		{name: "weakened_identity_constraint", sql: `ALTER TABLE artifact_delivery_attempts DROP CONSTRAINT artifact_delivery_attempts_current_identity;ALTER TABLE artifact_delivery_attempts ADD CONSTRAINT artifact_delivery_attempts_current_identity CHECK(manifest_sha256<>'')`, want: "definition differs"},
		{name: "expression_recovery_index", sql: `DROP INDEX artifact_delivery_attempts_commit_idx;CREATE INDEX artifact_delivery_attempts_commit_idx ON artifact_delivery_attempts(lower(commit_id))`, want: "key/predicate/expression contract"},
		{name: "nullable_identity", sql: `ALTER TABLE artifact_delivery_attempts ALTER COLUMN commit_id DROP NOT NULL`, want: "not_null=false"},
		{name: "wrong_identity_type", sql: `ALTER TABLE artifact_consumer_checkpoints ALTER COLUMN snapshot_id TYPE varchar(64)`, want: `type="character varying(64)"`},
		{name: "wrong_unique_key", sql: `ALTER TABLE artifact_delivery_attempts DROP CONSTRAINT artifact_delivery_attempts_publication_unique;ALTER TABLE artifact_delivery_attempts ADD CONSTRAINT artifact_delivery_attempts_publication_unique UNIQUE(flow_incarnation_id,consumer_revision_id,publication_id,commit_id)`, want: "definition differs"},
		{name: "extra_authority_column", sql: `ALTER TABLE artifact_consumer_checkpoints ADD COLUMN compatibility_state text`, want: "requires exact columns"},
		{name: "identity_default_restored", sql: `ALTER TABLE artifact_delivery_attempts ALTER COLUMN logical_batch_id SET DEFAULT ''`, want: "default="},
	}
	constraints := []struct{ table, name string }{
		{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_pkey"},
		{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_flow_incarnation_id_fkey"},
		{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_publication_id_fkey"},
		{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_current_identity"},
		{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_publication_unique"},
		{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_commit_unique"},
		{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_pkey"},
		{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_flow_incarnation_id_fkey"},
		{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_publication_id_fkey"},
		{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_attempt_id_fkey"},
		{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_current_identity"},
		{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_attempt_unique"},
		{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_pkey"},
		{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_flow_incarnation_id_fkey"},
		{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_publication_id_fkey"},
		{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_publication_sequence_check"},
		{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_flow_incarnation_id_consumer__key"},
		{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_flow_incarnation_id_consumer_key1"},
		{table: "canonical_schemas", name: "canonical_schemas_projection_mapping_contract"},
		{table: "artifact_streams", name: "artifact_streams_projection_mapping_contract"},
		{table: "artifact_objects", name: "artifact_objects_projection_mapping_contract"},
		{table: "artifact_publications", name: "artifact_publications_projection_mapping_contract"},
	}
	for _, constraint := range constraints {
		cases = append(cases, tamperCase{name: "drop_constraint_" + constraint.name, sql: fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s CASCADE", constraint.table, constraint.name), want: "verify exact artifact authority constraint"})
	}
	indexes := []struct {
		table, name string
		owned       bool
	}{
		{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_pkey", owned: true},
		{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_lookup_idx"},
		{table: "artifact_deliveries", name: "artifact_deliveries_pending_idx"},
		{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_pkey", owned: true},
		{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_attempt_idx"},
		{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_commit_idx"},
		{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_pkey", owned: true},
		{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_flow_incarnation_id_consumer__key", owned: true},
		{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_flow_incarnation_id_consumer_key1", owned: true},
		{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_publication_unique", owned: true},
		{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_commit_unique", owned: true},
		{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_attempt_unique", owned: true},
	}
	for index, contract := range indexes {
		sql := "DROP INDEX " + contract.name
		want := "verify exact artifact authority index"
		if contract.owned {
			sql = fmt.Sprintf("ALTER INDEX %s RENAME TO broken_artifact_index_%d", contract.name, index)
			want = "verify exact artifact authority constraint"
		}
		cases = append(cases, tamperCase{name: "drop_or_rename_index_" + contract.name, sql: sql, want: want})
	}
	if len(constraints) != 22 || len(indexes) != 12 {
		t.Fatalf("tamper manifest constraints/indexes=%d/%d", len(constraints), len(indexes))
	}
	for _, test := range cases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			pool, cleanup := newDeliveryMigrationDatabase(t, ctx, dsn, "artifactauth")
			defer cleanup()
			if err := controlplane.ApplyMigrations(ctx, pool); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, test.sql); err != nil {
				t.Fatal(err)
			}
			for restart := 1; restart <= 2; restart++ {
				err := controlplane.ApplyMigrations(ctx, pool)
				if err == nil || !strings.Contains(err.Error(), test.want) {
					t.Fatalf("restart %d tamper error=%v want=%q", restart, err, test.want)
				}
			}
		})
	}
}

func isolatedDatabaseDSN(t *testing.T, ctx context.Context, pool *pgxpool.Pool, baseDSN string) string {
	t.Helper()
	var database string
	if err := pool.QueryRow(ctx, `SELECT current_database()`).Scan(&database); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(baseDSN)
	if err != nil || parsed.Scheme == "" {
		t.Fatalf("restart integration requires a PostgreSQL URL DSN: %q", baseDSN)
	}
	parsed.Path = "/" + database
	return parsed.String()
}

func assertPackageMigrationHistory(t *testing.T, ctx context.Context, pool *pgxpool.Pool, domain string, wantVersions, wantChecksums []string) {
	t.Helper()
	var versions, checksums []string
	if err := pool.QueryRow(ctx, `SELECT array_agg(version ORDER BY version),array_agg(sql_checksum ORDER BY version) FROM public.wallaby_control_migrations WHERE domain=$1`, domain).Scan(&versions, &checksums); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(versions, wantVersions) || !reflect.DeepEqual(checksums, wantChecksums) {
		t.Fatalf("%s migration history versions/checksums=%v/%v", domain, versions, checksums)
	}
}
