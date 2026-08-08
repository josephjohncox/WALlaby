package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/controlplane"
	"github.com/josephjohncox/wallaby/internal/controlstore"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestFreshDeliveryCurrentSchemaMigration(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, cleanup := newDeliveryMigrationDatabase(t, ctx, dsn, "fresh")
	defer cleanup()
	if err := controlplane.ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var nullable, currentChecks, exactIndexes, history int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name IN ('delivery_manifests','delivery_attempts','delivery_receipts') AND column_name='logical_batch_id' AND is_nullable='NO'`).Scan(&nullable); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_constraint WHERE conname IN ('delivery_manifests_logical_batch_current','delivery_attempts_logical_batch_current','delivery_receipts_logical_batch_current') AND convalidated AND pg_catalog.pg_get_constraintdef(oid,true) LIKE '%sha256%'`).Scan(&currentChecks); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_indexes WHERE schemaname='public' AND indexname IN ('delivery_manifests_logical_batch_idx','delivery_attempts_logical_batch_idx','delivery_receipts_logical_batch_idx') AND indexdef LIKE 'CREATE UNIQUE INDEX%' AND indexdef NOT LIKE '% WHERE %'`).Scan(&exactIndexes); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallaby_control_migrations WHERE domain='delivery' AND version='008_current_logical_batch_identity.sql'`).Scan(&history); err != nil {
		t.Fatal(err)
	}
	if nullable != 3 || currentChecks != 3 || exactIndexes != 3 || history != 1 {
		t.Fatalf("current delivery schema nullable/checks/indexes/history=%d/%d/%d/%d", nullable, currentChecks, exactIndexes, history)
	}
}

func TestDeliveryCurrentSchemaMigrationRejectsInvalidLogicalRows(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	canonical, err := connector.DeliveryLogicalBatchID("lineage", "position", "hash")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, table string
		logical     any
	}{{"null_manifest", "manifest", nil}, {"empty_attempt", "attempt", ""}, {"legacy_receipt", "receipt", "legacy:position"}, {"malformed_manifest", "manifest", "not-a-logical-batch"}, {"arbitrary_attempt", "attempt", "logical-batch:" + strings.Repeat("a", 64)}, {"case_variant_receipt", "receipt", strings.ToUpper(canonical)}, {"wrong_canonical_manifest", "manifest", mustDeliveryLogicalBatchID(t, "other-lineage", "position", "hash")}}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			pool, cleanup := newDeliveryMigrationDatabase(t, ctx, dsn, "invalid_"+test.name)
			defer cleanup()
			applyDeliveryMigrationsThrough(t, ctx, pool, 6)
			insertInvalidDeliveryIdentityRow(t, ctx, pool, test.table, test.logical)
			_, err := pool.Exec(ctx, deliveryMigrationSQL(t, 8))
			if err == nil || !strings.Contains(err.Error(), "refuses noncanonical logical batch identities") {
				t.Fatalf("migration error=%v", err)
			}
		})
	}
}

func TestDeliveryCurrentSchemaMigrationAcceptsExactCanonicalRows(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, cleanup := newDeliveryMigrationDatabase(t, ctx, dsn, "canonical")
	defer cleanup()
	applyDeliveryMigrationsThrough(t, ctx, pool, 6)
	logical := mustDeliveryLogicalBatchID(t, "lineage", "position", "hash")
	attemptID := uuid.New()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('wallaby.authority_protocol','v2',true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO delivery_manifests(flow_incarnation_id,destination_revision_id,source_lineage_id,logical_batch_id,position_id,source_transaction_id,content_hash,checkpoint_lsn) VALUES($1,'revision','lineage',$2,'position','transaction','hash','0/10')`, deliveryMigrationIncarnation, logical); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO delivery_attempts(attempt_id,flow_incarnation_id,flow_id,generation,acquisition_id,lease_epoch,destination_revision_id,source_lineage_id,logical_batch_id,position_id,content_hash,attempt_number) VALUES($1,$2,'flow',1,$3,1,'revision','lineage',$4,'position','hash',1)`, attemptID, deliveryMigrationIncarnation, uuid.New(), logical); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO delivery_receipts(flow_incarnation_id,destination_revision_id,source_lineage_id,logical_batch_id,position_id,content_hash,attempt_id,external_id,adopted_by_acquisition_id,adopted_by_lease_epoch) VALUES($1,'revision','lineage',$2,'position','hash',$3,'external',$4,1)`, deliveryMigrationIncarnation, logical, attemptID, uuid.New()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, deliveryMigrationSQL(t, 8)); err != nil {
		t.Fatalf("canonical migration: %v", err)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM delivery_manifests)+(SELECT count(*) FROM delivery_attempts)+(SELECT count(*) FROM delivery_receipts)`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 3 {
		t.Fatalf("canonical delivery rows=%d, want 3", rows)
	}
}

func TestDeliveryCurrentSchemaMigrationRejectsAmbiguousRows(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, cleanup := newDeliveryMigrationDatabase(t, ctx, dsn, "ambiguous")
	defer cleanup()
	applyDeliveryMigrationsThrough(t, ctx, pool, 6)
	logical := mustDeliveryLogicalBatchID(t, "lineage", "position", "hash")
	insertLegacyDeliveryManifest(t, ctx, pool, logical, "position")
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('wallaby.authority_protocol','v2',true)`); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := tx.Exec(ctx, `INSERT INTO delivery_attempts(attempt_id,flow_incarnation_id,flow_id,generation,acquisition_id,lease_epoch,destination_revision_id,source_lineage_id,logical_batch_id,position_id,content_hash,attempt_number) VALUES($1,$2,'flow',1,$3,1,'revision','lineage',$4,'position','hash',1)`, uuid.New(), deliveryMigrationIncarnation, uuid.New(), logical); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, deliveryMigrationSQL(t, 8))
	if err == nil || !strings.Contains(err.Error(), "refuses ambiguous logical batch rows") {
		t.Fatalf("migration error=%v", err)
	}
}

func mustDeliveryLogicalBatchID(t *testing.T, lineage, position, content string) string {
	t.Helper()
	logical, err := connector.DeliveryLogicalBatchID(lineage, position, content)
	if err != nil {
		t.Fatal(err)
	}
	return logical
}

var deliveryMigrationIncarnation = uuid.MustParse("11111111-1111-1111-1111-111111111111")

func insertInvalidDeliveryIdentityRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string, logical any) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('wallaby.authority_protocol','v2',true)`); err != nil {
		t.Fatal(err)
	}
	switch table {
	case "manifest":
		_, err = tx.Exec(ctx, `INSERT INTO delivery_manifests(flow_incarnation_id,destination_revision_id,source_lineage_id,logical_batch_id,position_id,source_transaction_id,content_hash,checkpoint_lsn) VALUES($1,'revision','lineage',$2,'position','transaction','hash','0/10')`, deliveryMigrationIncarnation, logical)
	case "attempt":
		_, err = tx.Exec(ctx, `INSERT INTO delivery_attempts(attempt_id,flow_incarnation_id,flow_id,generation,acquisition_id,lease_epoch,destination_revision_id,source_lineage_id,logical_batch_id,position_id,content_hash,attempt_number) VALUES($1,$2,'flow',1,$3,1,'revision','lineage',$4,'position','hash',1)`, uuid.New(), deliveryMigrationIncarnation, uuid.New(), logical)
	case "receipt":
		_, err = tx.Exec(ctx, `INSERT INTO delivery_receipts(flow_incarnation_id,destination_revision_id,source_lineage_id,logical_batch_id,position_id,content_hash,attempt_id,external_id,adopted_by_acquisition_id,adopted_by_lease_epoch) VALUES($1,'revision','lineage',$2,'position','hash',$3,'external',$4,1)`, deliveryMigrationIncarnation, logical, uuid.New(), uuid.New())
	default:
		t.Fatalf("unknown invalid delivery table %q", table)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func insertLegacyDeliveryManifest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, logical any, position string) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('wallaby.authority_protocol','v2',true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO delivery_manifests(flow_incarnation_id,destination_revision_id,source_lineage_id,logical_batch_id,position_id,source_transaction_id,content_hash,checkpoint_lsn) VALUES($1,'revision','lineage',$2,$3,'transaction','hash','0/10')`, deliveryMigrationIncarnation, logical, position); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func applyDeliveryMigrationsThrough(t *testing.T, ctx context.Context, pool *pgxpool.Pool, last int) {
	t.Helper()
	for version := 1; version <= last; version++ {
		if _, err := pool.Exec(ctx, deliveryMigrationSQL(t, version)); err != nil {
			t.Fatalf("apply delivery migration %03d: %v", version, err)
		}
	}
}
func deliveryMigrationSQL(t *testing.T, version int) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate delivery migrations")
	}
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(filepath.Dir(filename)), "internal", "delivery", "migrations", fmt.Sprintf("%03d_*.sql", version)))
	if err != nil || len(matches) != 1 {
		t.Fatalf("delivery migration %03d matches=%v err=%v", version, matches, err)
	}
	contents, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}

func newDeliveryMigrationDatabase(t *testing.T, ctx context.Context, dsn, suffix string) (*pgxpool.Pool, func()) {
	t.Helper()
	admin, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "wallaby_delivery_" + suffix + "_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	identifier := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+identifier+" TEMPLATE template0"); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config.ConnConfig.Database = name
	controlstore.ConfigurePool(config)
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cleanup := func() {
		pool.Close()
		dropCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = admin.Exec(dropCtx, "DROP DATABASE IF EXISTS "+identifier+" WITH (FORCE)")
		admin.Close()
	}
	return pool, cleanup
}
