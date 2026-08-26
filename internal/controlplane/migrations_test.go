package controlplane

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/registry"
)

func TestMigrationDomainsKeepAuthorityBeforeMutations(t *testing.T) {
	want := []string{"workflow", "checkpoint", "registry", "controlplane", "delivery", "bootstrap", "artifactlog", "pgstream", "schema_registry"}
	if got := MigrationDomains(); !reflect.DeepEqual(got, want) {
		t.Fatalf("migration order=%v, want %v", got, want)
	}
}

func TestEveryControlDomainRejectsChecksumDriftAndNonPrefixHistory(t *testing.T) {
	ctx, pool := newControlplaneMigrationFixture(t)
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	for _, domain := range MigrationDomains() {
		t.Run(domain, func(t *testing.T) {
			var version, checksum string
			if err := pool.QueryRow(ctx, `SELECT version,sql_checksum FROM public.wallaby_control_migrations WHERE domain=$1 ORDER BY version LIMIT 1`, domain).Scan(&version, &checksum); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `UPDATE public.wallaby_control_migrations SET sql_checksum=$3 WHERE domain=$1 AND version=$2`, domain, version, strings.Repeat("0", 64)); err != nil {
				t.Fatal(err)
			}
			if err := ApplyMigrations(ctx, pool); err == nil || !strings.Contains(err.Error(), domain) || !strings.Contains(err.Error(), "checksum drift") {
				t.Fatalf("checksum verification error=%v", err)
			}
			if _, err := pool.Exec(ctx, `UPDATE public.wallaby_control_migrations SET sql_checksum=$3 WHERE domain=$1 AND version=$2`, domain, version, checksum); err != nil {
				t.Fatal(err)
			}

			const nonPrefixVersion = "000_task17_nonprefix.sql"
			if _, err := pool.Exec(ctx, `UPDATE public.wallaby_control_migrations SET version=$3 WHERE domain=$1 AND version=$2`, domain, version, nonPrefixVersion); err != nil {
				t.Fatal(err)
			}
			if err := ApplyMigrations(ctx, pool); err == nil || !strings.Contains(err.Error(), domain) || !strings.Contains(err.Error(), "not an ordered prefix") {
				t.Fatalf("ordered-prefix verification error=%v", err)
			}
			if _, err := pool.Exec(ctx, `UPDATE public.wallaby_control_migrations SET version=$3 WHERE domain=$1 AND version=$2`, domain, nonPrefixVersion, version); err != nil {
				t.Fatal(err)
			}
		})
	}
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("restored ordered checksum histories: %v", err)
	}
}

func TestManagedAuthorityVerificationPinsCanonicalSearchPath(t *testing.T) {
	ctx, pool := newControlplaneMigrationFixture(t)
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var role string
	if err := pool.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	quotedRole := pgx.Identifier{role}.Sanitize()
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s; CREATE TABLE %s.flows(id text); CREATE FUNCTION %s.wallaby_require_authority_protocol_v2() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NULL; END$$`, quotedRole, quotedRole, quotedRole)); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, quotedRole))
	}()

	hostileConfig := pool.Config().Copy()
	if hostileConfig.ConnConfig.RuntimeParams == nil {
		hostileConfig.ConnConfig.RuntimeParams = map[string]string{}
	}
	hostileConfig.ConnConfig.RuntimeParams["search_path"] = quotedRole
	hostileConfig.MaxConns = 1
	hostilePool, err := pgxpool.NewWithConfig(ctx, hostileConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer hostilePool.Close()
	if err := hostilePool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	connection, err := hostilePool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Exec(ctx, `CREATE TEMP TABLE flows(id text); CREATE FUNCTION pg_temp.wallaby_require_authority_protocol_v2() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NULL; END$$`); err != nil {
		connection.Release()
		t.Fatal(err)
	}
	connection.Release()
	if err := verifyManagedAuthoritySchema(ctx, hostilePool); err != nil {
		t.Fatalf("canonical authority verification under hostile nonpublic search_path: %v", err)
	}
}

func TestManagedAuthorityVerificationNeverRepairsTamperedSchema(t *testing.T) {
	ctx, pool := newControlplaneMigrationFixture(t)
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE ddl_execution_run_attempts RENAME COLUMN flow_id TO task17_broken_flow_id`); err != nil {
		t.Fatal(err)
	}
	if err := ApplyMigrations(ctx, pool); err == nil || !strings.Contains(err.Error(), "missing required columns") {
		t.Fatalf("tampered authority startup error=%v", err)
	}
	var expectedExists, brokenExists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='ddl_execution_run_attempts'::regclass AND attname='flow_id' AND attnum>0 AND NOT attisdropped), EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='ddl_execution_run_attempts'::regclass AND attname='task17_broken_flow_id' AND attnum>0 AND NOT attisdropped)`).Scan(&expectedExists, &brokenExists); err != nil {
		t.Fatal(err)
	}
	if expectedExists || !brokenExists {
		t.Fatalf("startup repaired authority columns: flow_id=%t broken=%t", expectedExists, brokenExists)
	}
}

func TestDeliveryManifestAuthorityCatalogIsExact(t *testing.T) {
	wantNames := []string{
		"flow_incarnation_id", "destination_revision_id", "source_lineage_id", "position_id", "source_transaction_id",
		"content_hash", "checkpoint_lsn", "created_at", "logical_batch_id", "checkpoint_metadata", "checkpoint_timestamp",
		"schema_baseline_payload", "schema_baseline_fingerprint",
	}
	deliveryColumns := exactAuthorityColumns["delivery_manifests"]
	gotNames := make([]string, len(deliveryColumns))
	for index, column := range deliveryColumns {
		gotNames[index] = column.name
	}
	if !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("delivery manifest authority columns=%v, want exact ordered %v", gotNames, wantNames)
	}
	for _, column := range deliveryColumns {
		if !column.notNull {
			t.Fatalf("delivery manifest authority column %s is unexpectedly nullable", column.name)
		}
	}
}

func TestDeliveryManifestAuthorityTamperCurrentPGMajor(t *testing.T) {
	ctx, pool := newControlplaneMigrationFixture(t)
	var major int
	if err := pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::int / 10000`).Scan(&major); err != nil {
		t.Fatal(err)
	}
	testDeliveryManifestAuthorityTamper(t, major)
}

func TestDeliveryManifestAuthorityTamperPG14(t *testing.T) {
	testDeliveryManifestAuthorityTamper(t, 14)
}

func TestDeliveryManifestAuthorityTamperPG16(t *testing.T) {
	testDeliveryManifestAuthorityTamper(t, 16)
}

func testDeliveryManifestAuthorityTamper(t *testing.T, major int) {
	t.Helper()
	exec := func(statement string) func(context.Context, *pgxpool.Pool) error {
		return func(ctx context.Context, pool *pgxpool.Pool) error {
			_, err := pool.Exec(ctx, statement) // #nosec G202 -- every caller passes a source-constant tamper statement.
			return err
		}
	}
	for _, test := range []struct {
		name   string
		tamper func(context.Context, *pgxpool.Pool) error
		want   string
	}{
		{name: "extra column", tamper: exec(`ALTER TABLE delivery_manifests ADD COLUMN injected text`), want: "requires exact columns for delivery_manifests"},
		{name: "nullable checkpoint", tamper: exec(`ALTER TABLE delivery_manifests ALTER COLUMN checkpoint_metadata DROP NOT NULL`), want: "delivery_manifests.checkpoint_metadata differs"},
		{name: "extra check true", tamper: exec(`ALTER TABLE delivery_manifests ADD CONSTRAINT delivery_manifests_injected_check CHECK (true)`), want: "constraint set for delivery_manifests differs"},
		{name: "extra unique constraint", tamper: exec(`ALTER TABLE delivery_manifests ADD CONSTRAINT delivery_manifests_injected_unique UNIQUE (content_hash)`), want: "constraint set for delivery_manifests differs"},
		{name: "extra nonunique index", tamper: exec(`CREATE INDEX delivery_manifests_injected_idx ON delivery_manifests(content_hash)`), want: "index set for delivery_manifests differs"},
		{name: "extra expression index", tamper: exec(`CREATE INDEX delivery_manifests_injected_expression_idx ON delivery_manifests((lower(content_hash)))`), want: "index set for delivery_manifests differs"},
		{name: "extra predicate index", tamper: exec(`CREATE INDEX delivery_manifests_injected_predicate_idx ON delivery_manifests(content_hash) WHERE content_hash<>''`), want: "index set for delivery_manifests differs"},
		{name: "same-name logical check true", tamper: exec(`ALTER TABLE delivery_manifests DROP CONSTRAINT delivery_manifests_logical_batch_current; ALTER TABLE delivery_manifests ADD CONSTRAINT delivery_manifests_logical_batch_current CHECK (true)`), want: "delivery_manifests.delivery_manifests_logical_batch_current definition differs"},
		{name: "same-name baseline check true", tamper: exec(`ALTER TABLE delivery_manifests DROP CONSTRAINT delivery_manifests_schema_baseline_fingerprint_check; ALTER TABLE delivery_manifests ADD CONSTRAINT delivery_manifests_schema_baseline_fingerprint_check CHECK (true)`), want: "delivery_manifests.delivery_manifests_schema_baseline_fingerprint_check definition differs"},
		{name: "altered primary key columns", tamper: exec(`ALTER TABLE delivery_manifests DROP CONSTRAINT delivery_manifests_pkey; ALTER TABLE delivery_manifests ADD CONSTRAINT delivery_manifests_pkey PRIMARY KEY (flow_incarnation_id,destination_revision_id,logical_batch_id)`), want: "delivery_manifests.delivery_manifests_pkey definition differs"},
		{name: "nonunique logical index", tamper: exec(`DROP INDEX delivery_manifests_logical_batch_idx; CREATE INDEX delivery_manifests_logical_batch_idx ON delivery_manifests(flow_incarnation_id,destination_revision_id,logical_batch_id)`), want: "delivery_manifests.delivery_manifests_logical_batch_idx differs"},
		{name: "wrong logical index columns", tamper: exec(`DROP INDEX delivery_manifests_logical_batch_idx; CREATE UNIQUE INDEX delivery_manifests_logical_batch_idx ON delivery_manifests(flow_incarnation_id,destination_revision_id,position_id)`), want: "delivery_manifests.delivery_manifests_logical_batch_idx differs"},
		{name: "predicated logical index", tamper: exec(`DROP INDEX delivery_manifests_logical_batch_idx; CREATE UNIQUE INDEX delivery_manifests_logical_batch_idx ON delivery_manifests(flow_incarnation_id,destination_revision_id,logical_batch_id) WHERE logical_batch_id<>''`), want: "delivery_manifests.delivery_manifests_logical_batch_idx differs"},
		{name: "expression logical index", tamper: exec(`DROP INDEX delivery_manifests_logical_batch_idx; CREATE UNIQUE INDEX delivery_manifests_logical_batch_idx ON delivery_manifests(flow_incarnation_id,destination_revision_id,(lower(logical_batch_id)))`), want: "delivery_manifests.delivery_manifests_logical_batch_idx differs"},
		{name: "wrong logical index method", tamper: exec(`DROP INDEX delivery_manifests_logical_batch_idx; CREATE INDEX delivery_manifests_logical_batch_idx ON delivery_manifests USING hash(logical_batch_id)`), want: "delivery_manifests.delivery_manifests_logical_batch_idx differs"},
		{name: "invalid logical index", tamper: exec(`UPDATE pg_catalog.pg_index SET indisvalid=false WHERE indexrelid='delivery_manifests_logical_batch_idx'::regclass`), want: "delivery_manifests.delivery_manifests_logical_batch_idx differs"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, pool := newControlplaneMigrationFixture(t)
			var serverVersion int
			if err := pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::int / 10000`).Scan(&serverVersion); err != nil {
				t.Fatal(err)
			}
			if serverVersion != major {
				t.Skipf("PostgreSQL %d tamper cell requires server major %d", major, major)
			}
			if err := ApplyMigrations(ctx, pool); err != nil {
				t.Fatal(err)
			}
			if err := test.tamper(ctx, pool); err != nil {
				t.Fatal(err)
			}
			if err := ApplyMigrations(ctx, pool); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("PostgreSQL %d delivery manifest tamper error=%v, want %q", major, err, test.want)
			}
			if err := ApplyMigrations(ctx, pool); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("PostgreSQL %d second restart repaired tamper: error=%v, want %q", major, err, test.want)
			}
		})
	}
}

func TestManagedSchemaBaselineAuthorityManifestIsCurrent(t *testing.T) {
	if !containsString(authorityMutableTables, "managed_schema_baselines") {
		t.Fatal("managed_schema_baselines is absent from authority tables")
	}
	if got := len(requiredManagedColumns["managed_schema_baselines"]); got != 11 {
		t.Fatalf("managed schema-baseline required columns=%d want=11", got)
	}
	for _, required := range []requiredManagedObject{
		{table: "managed_schema_baselines", name: "managed_schema_baselines_pkey"},
		{table: "managed_schema_baselines", name: "managed_schema_baselines_flow_incarnation_id_fkey"},
		{table: "managed_schema_baselines", name: "managed_schema_baselines_acquisition_id_fkey"},
	} {
		found := false
		for _, actual := range requiredManagedConstraints {
			if actual == required {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("managed schema-baseline constraint absent from manifest: %+v", required)
		}
	}
	if !containsManagedObject(requiredManagedIndexes, requiredManagedObject{table: "managed_schema_baselines", name: "managed_schema_baselines_current_fence_idx"}) {
		t.Fatal("managed schema-baseline current-fence index absent from manifest")
	}
}

func containsManagedObject(values []requiredManagedObject, want requiredManagedObject) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestExactAuthorityCatalogManifestIsCurrent(t *testing.T) {
	if !containsString(authorityMutableTables, "artifact_consumer_checkpoints") {
		t.Fatal("artifact_consumer_checkpoints is absent from authority tables")
	}
	wantColumnCounts := map[string]int{"delivery_manifests": 13, "artifact_delivery_attempts": 11, "artifact_delivery_receipts": 15, "artifact_consumer_checkpoints": 9, "artifact_metadata_prune_claims": 13}
	for table, want := range wantColumnCounts {
		if got := len(exactAuthorityColumns[table]); got != want {
			t.Fatalf("%s exact column count=%d want=%d", table, got, want)
		}
	}
	if len(exactAuthorityConstraints) != 31 || len(selectiveAuthorityConstraints) != 5 {
		t.Fatalf("authority exact/selective constraint manifest count=%d/%d want=31/5", len(exactAuthorityConstraints), len(selectiveAuthorityConstraints))
	}
	if len(exactAuthorityIndexes) != 17 || len(selectiveAuthorityIndexes) != 4 {
		t.Fatalf("authority exact/selective index manifest count=%d/%d want=17/4", len(exactAuthorityIndexes), len(selectiveAuthorityIndexes))
	}
	constraintContracts := append(append([]exactAuthorityConstraint(nil), exactAuthorityConstraints...), selectiveAuthorityConstraints...)
	indexContracts := append(append([]exactAuthorityIndex(nil), exactAuthorityIndexes...), selectiveAuthorityIndexes...)
	seen := make(map[string]struct{}, 43)
	for _, contract := range constraintContracts {
		key := contract.table + "." + contract.name
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("duplicate artifact authority object %s", key)
		}
		seen[key] = struct{}{}
	}
	for _, contract := range indexContracts {
		key := contract.table + "." + contract.name
		if _, duplicate := seen["index:"+key]; duplicate {
			t.Fatalf("duplicate artifact authority index %s", key)
		}
		seen["index:"+key] = struct{}{}
	}
}

func TestExactAuthorityManifestMatchesFreshCatalog(t *testing.T) {
	ctx, pool := newControlplaneMigrationFixture(t)
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	constraintContracts := append(append([]exactAuthorityConstraint(nil), exactAuthorityConstraints...), selectiveAuthorityConstraints...)
	expectedConstraints := make([]string, len(constraintContracts))
	for index, contract := range constraintContracts {
		expectedConstraints[index] = contract.table + "." + contract.name
	}
	sort.Strings(expectedConstraints)
	var actualConstraints []string
	if err := pool.QueryRow(ctx, `SELECT COALESCE(array_agg(table_relation.relname||'.'||constraint_row.conname ORDER BY table_relation.relname,constraint_row.conname),'{}'::text[]) FROM pg_catalog.pg_constraint AS constraint_row JOIN pg_catalog.pg_class AS table_relation ON table_relation.oid=constraint_row.conrelid WHERE table_relation.relname IN ('delivery_manifests','schema_versions','artifact_delivery_attempts','artifact_delivery_receipts','artifact_consumer_checkpoints','artifact_metadata_prune_claims')
   OR (table_relation.relname IN ('artifact_deliveries','canonical_schemas','artifact_streams','artifact_objects','artifact_publications')
       AND constraint_row.conname NOT IN (
         'artifact_deliveries_flow_incarnation_id_consumer_revision_i_key','artifact_deliveries_flow_incarnation_id_fkey','artifact_deliveries_pkey','artifact_deliveries_publication_id_fkey',
         'canonical_schemas_pkey',
         'artifact_streams_backlog_age_high_seconds_check','artifact_streams_backlog_bytes_high_check','artifact_streams_backlog_count_high_check','artifact_streams_flow_incarnation_id_fkey','artifact_streams_hard_retained_bytes_check','artifact_streams_pkey',
         'artifact_objects_bucket_object_key_key','artifact_objects_encoded_length_check','artifact_objects_flow_incarnation_id_fkey','artifact_objects_flow_incarnation_id_source_position_fragme_key','artifact_objects_pkey','artifact_objects_record_count_positive','artifact_objects_schema_id_fkey','artifact_objects_shard_nonnegative','artifact_objects_state_check','artifact_objects_version_evidence',
         'artifact_publications_flow_incarnation_id_fkey','artifact_publications_flow_incarnation_id_source_position_key','artifact_publications_pkey'
       ))`).Scan(&actualConstraints); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actualConstraints, expectedConstraints) {
		t.Fatalf("artifact constraint manifest/catalog mismatch\nmanifest=%v\ncatalog=%v", expectedConstraints, actualConstraints)
	}
	indexContracts := append(append([]exactAuthorityIndex(nil), exactAuthorityIndexes...), selectiveAuthorityIndexes...)
	expectedIndexes := make([]string, len(indexContracts))
	for index, contract := range indexContracts {
		expectedIndexes[index] = contract.table + "." + contract.name
	}
	sort.Strings(expectedIndexes)
	var actualIndexes []string
	if err := pool.QueryRow(ctx, `SELECT COALESCE(array_agg(table_relation.relname||'.'||index_relation.relname ORDER BY table_relation.relname,index_relation.relname),'{}'::text[]) FROM pg_catalog.pg_index AS index_row JOIN pg_catalog.pg_class AS table_relation ON table_relation.oid=index_row.indrelid JOIN pg_catalog.pg_class AS index_relation ON index_relation.oid=index_row.indexrelid WHERE table_relation.relname IN ('delivery_manifests','schema_versions','artifact_delivery_attempts','artifact_delivery_receipts','artifact_consumer_checkpoints','artifact_metadata_prune_claims')
   OR (table_relation.relname IN ('artifact_deliveries','canonical_schemas','artifact_streams','artifact_objects','artifact_publications','artifact_gc_claims')
       AND index_relation.relname NOT IN (
         'artifact_deliveries_flow_incarnation_id_consumer_revision_i_key','artifact_deliveries_pkey','artifact_gc_claims_pkey',
         'canonical_schemas_pkey','artifact_streams_pkey',
         'artifact_objects_bucket_object_key_key','artifact_objects_flow_incarnation_id_source_position_fragme_key','artifact_objects_logical_shard_idx','artifact_objects_pkey',
         'artifact_publications_flow_incarnation_id_source_position_key','artifact_publications_logical_batch_idx','artifact_publications_pkey','artifact_publications_sequence_idx'
       ))`).Scan(&actualIndexes); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actualIndexes, expectedIndexes) {
		t.Fatalf("artifact index manifest/catalog mismatch\nmanifest=%v\ncatalog=%v", expectedIndexes, actualIndexes)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
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
		"metadata claim schema ids remain non-null",
		"ALTER TABLE artifact_metadata_prune_claims ALTER COLUMN schema_ids DROP NOT NULL",
		"ALTER TABLE artifact_metadata_prune_claims ALTER COLUMN schema_ids SET NOT NULL",
		"artifact_metadata_prune_claims.schema_ids differs",
	)
	assertRejected(
		"metadata claim retry default is exact",
		"ALTER TABLE artifact_metadata_prune_claims ALTER COLUMN retry_after DROP DEFAULT",
		"ALTER TABLE artifact_metadata_prune_claims ALTER COLUMN retry_after SET DEFAULT clock_timestamp()",
		"artifact_metadata_prune_claims.retry_after differs",
	)
	assertRejected(
		"metadata claim schema array constraint is exact",
		"ALTER TABLE artifact_metadata_prune_claims DROP CONSTRAINT artifact_metadata_prune_claims_schema_ids_array; ALTER TABLE artifact_metadata_prune_claims ADD CONSTRAINT artifact_metadata_prune_claims_schema_ids_array CHECK (schema_ids IS NOT NULL)",
		"ALTER TABLE artifact_metadata_prune_claims DROP CONSTRAINT artifact_metadata_prune_claims_schema_ids_array; ALTER TABLE artifact_metadata_prune_claims ADD CONSTRAINT artifact_metadata_prune_claims_schema_ids_array CHECK (jsonb_typeof(schema_ids)='array')",
		"artifact_metadata_prune_claims_schema_ids_array definition differs",
	)
	assertRejected(
		"metadata claim catalog tombstone shape is exact",
		"ALTER TABLE artifact_metadata_prune_claims DROP CONSTRAINT artifact_metadata_prune_claims_catalog_evidence_object; ALTER TABLE artifact_metadata_prune_claims ADD CONSTRAINT artifact_metadata_prune_claims_catalog_evidence_object CHECK (jsonb_typeof(catalog_evidence)='object')",
		"ALTER TABLE artifact_metadata_prune_claims DROP CONSTRAINT artifact_metadata_prune_claims_catalog_evidence_object; ALTER TABLE artifact_metadata_prune_claims ADD CONSTRAINT artifact_metadata_prune_claims_catalog_evidence_object CHECK (jsonb_typeof(catalog_evidence)='object' AND jsonb_typeof(catalog_evidence->'publication')='object' AND jsonb_typeof(catalog_evidence->'consumers')='array')",
		"artifact_metadata_prune_claims_catalog_evidence_object definition differs",
	)
	assertRejected(
		"metadata claim scan index order is exact",
		"DROP INDEX artifact_metadata_prune_claims_flow_idx; CREATE INDEX artifact_metadata_prune_claims_flow_idx ON artifact_metadata_prune_claims(flow_incarnation_id,claimed_at,retry_after,publication_id)",
		"DROP INDEX artifact_metadata_prune_claims_flow_idx; CREATE INDEX artifact_metadata_prune_claims_flow_idx ON artifact_metadata_prune_claims(flow_incarnation_id,retry_after,claimed_at,publication_id)",
		"artifact_metadata_prune_claims.artifact_metadata_prune_claims_flow_idx differs",
	)
	const restoreGCClaimPublicationIndex = `DROP INDEX artifact_gc_claims_publication_idx; CREATE INDEX artifact_gc_claims_publication_idx ON artifact_gc_claims(publication_id) WHERE publication_id IS NOT NULL`
	assertRejected(
		"artifact GC publication index predicate is exact",
		"DROP INDEX artifact_gc_claims_publication_idx; CREATE INDEX artifact_gc_claims_publication_idx ON artifact_gc_claims(publication_id) WHERE publication_id IS NULL",
		restoreGCClaimPublicationIndex,
		"artifact_gc_claims.artifact_gc_claims_publication_idx differs",
	)
	assertRejected(
		"artifact GC publication index key is exact",
		"DROP INDEX artifact_gc_claims_publication_idx; CREATE INDEX artifact_gc_claims_publication_idx ON artifact_gc_claims(claim_epoch) WHERE publication_id IS NOT NULL",
		restoreGCClaimPublicationIndex,
		"artifact_gc_claims.artifact_gc_claims_publication_idx differs",
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
	database := fmt.Sprintf("wallaby_controlplane_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{database}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+identifier+" WITH (FORCE)")
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Database = database
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["wallaby.authority_protocol"] = "v2"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return ctx, pool
}
