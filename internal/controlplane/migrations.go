// Package controlplane owns the single ordered control-PostgreSQL migration
// entrypoint used by production server and worker processes.
package controlplane

import (
	"context"
	"embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/controlstore"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/registry"
	"github.com/josephjohncox/wallaby/internal/workflow"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// MigrationDomains returns the monotonic dependency order. Authority tables
// are part of workflow; the separately-ledgered controlplane repair follows
// registry so a registry-only history can be promoted safely.
func MigrationDomains() []string {
	return []string{"workflow", "checkpoint", "registry", "controlplane", "delivery", "bootstrap", "artifactlog"}
}

// ApplyMigrations applies every control schema under the shared coordinator
// lock and checksum history. Domain constructors retain idempotent migration
// calls for library compatibility, but production startup enters here first.
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return fmt.Errorf("control-plane migration pool is required")
	}
	steps := []struct {
		name  string
		apply func(context.Context, *pgxpool.Pool) error
	}{
		{name: "workflow", apply: workflow.ApplyMigrations},
		{name: "checkpoint", apply: checkpoint.ApplyMigrations},
		{name: "registry", apply: registry.ApplyMigrations},
		{name: "controlplane", apply: applyControlplaneMigrations},
		{name: "delivery", apply: delivery.ApplyMigrations},
		{name: "bootstrap", apply: bootstrap.ApplyMigrations},
		{name: "artifactlog", apply: artifactlog.ApplyMigrations},
	}
	for _, step := range steps {
		if err := step.apply(ctx, pool); err != nil {
			return fmt.Errorf("migrate control domain %s: %w", step.name, err)
		}
	}
	if err := verifyManagedAuthoritySchema(ctx, pool); err != nil {
		return err
	}
	return nil
}

func applyControlplaneMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	if err := controlstore.ApplyMigrations(ctx, pool, "controlplane", migrationFS, "migrations/*.sql", ""); err != nil {
		return fmt.Errorf("apply centralized controlplane migrations: %w", err)
	}
	return nil
}

var authorityMutableTables = []string{
	"flows", "flow_incarnations", "flow_state_events", "flow_executions", "execution_acquisitions", "producer_leases", "work_claims",
	"checkpoints", "checkpoint_outbox", "authoritative_checkpoints", "authoritative_checkpoint_outbox",
	"schema_versions", "ddl_events", "ddl_execution_attempts", "ddl_execution_receipts", "ddl_execution_manifests", "ddl_execution_run_attempts", "schema_publication_operations",
	"destination_revisions", "delivery_manifests", "delivery_attempts", "delivery_attempt_evidence", "delivery_receipts", "source_ack_intents", "source_ack_receipts", "delivery_retention_roots", "source_ack_retention_roots",
	"source_bootstraps", "source_bootstrap_tasks", "snapshot_publication_receipts", "source_resources", "source_resource_operations", "snapshot_delivery_attempts", "snapshot_delivery_evidence", "snapshot_delivery_receipts",
	"canonical_schemas", "artifact_streams", "artifact_objects", "artifact_upload_attempts", "artifact_publications", "artifact_publication_objects", "artifact_barriers", "artifact_deliveries", "artifact_quota_accounts", "artifact_quota_reservations", "artifact_gc_claims", "artifact_delivery_attempts", "artifact_delivery_receipts",
}

var requiredManagedColumns = map[string][]string{
	"flow_incarnations":             {"incarnation_id", "flow_id", "created_at", "retired_at"},
	"execution_acquisitions":        {"acquisition_id", "incarnation_id", "generation", "execution_id", "lease_epoch"},
	"producer_leases":               {"incarnation_id", "acquisition_id", "generation", "lease_epoch", "lease_expires_at"},
	"authoritative_checkpoints":     {"flow_incarnation_id", "flow_id", "generation", "acquisition_id", "lease_epoch", "lsn", "metadata"},
	"schema_versions":               {"namespace", "name", "version", "schema_json", "flow_incarnation_id", "generation", "acquisition_id", "lease_epoch", "authority_origin"},
	"ddl_events":                    {"id", "flow_id", "lsn", "flow_incarnation_id", "generation", "acquisition_id", "lease_epoch", "authority_origin"},
	"ddl_execution_run_attempts":    {"attempt_id", "event_id", "destination", "flow_incarnation_id", "flow_id", "lsn", "generation", "acquisition_id", "lease_epoch", "started_at"},
	"schema_publication_operations": {"operation_id", "flow_incarnation_id", "flow_id", "subject", "schema_fingerprint", "registry_revision", "generation", "acquisition_id", "lease_epoch", "status", "external_id", "prepared_at", "completed_at"},
	"source_bootstraps":             {"bootstrap_id", "flow_incarnation_id", "bootstrap_generation", "owner_generation", "owner_acquisition_id", "owner_lease_epoch", "slot_name", "publication_name", "consistent_lsn", "manifest_hash", "phase"},
	"source_resources":              {"flow_incarnation_id", "resource_kind", "resource_id", "generation", "acquisition_id", "lease_epoch", "created_generation", "created_acquisition_id", "created_lease_epoch", "ownership", "revision", "state"},
	"source_resource_operations":    {"operation_id", "flow_incarnation_id", "resource_kind", "resource_id", "operation", "desired_revision", "generation", "acquisition_id", "lease_epoch", "status"},
	"snapshot_delivery_attempts":    {"attempt_id", "bootstrap_id", "relation_id", "task_id", "batch_ordinal", "flow_incarnation_id", "generation", "acquisition_id", "lease_epoch", "claim_epoch"},
	"snapshot_delivery_receipts":    {"bootstrap_id", "relation_id", "task_id", "batch_ordinal", "attempt_id", "durable_cursor", "completed_task"},
	"delivery_manifests":            {"flow_incarnation_id", "destination_revision_id", "source_lineage_id", "logical_batch_id", "position_id", "content_hash", "checkpoint_lsn"},
	"delivery_attempts":             {"attempt_id", "flow_incarnation_id", "generation", "acquisition_id", "lease_epoch", "logical_batch_id", "position_id", "content_hash", "attempt_number", "attempt_state", "next_attempt_at"},
	"delivery_receipts":             {"flow_incarnation_id", "position_id", "destination_revision_id", "logical_batch_id", "attempt_id", "content_hash"},
	"source_ack_intents":            {"flow_incarnation_id", "position_id", "checkpoint_lsn", "generation", "acquisition_id", "lease_epoch"},
	"source_ack_receipts":           {"flow_incarnation_id", "position_id", "checkpoint_lsn", "observed_flush_lsn", "generation", "acquisition_id", "lease_epoch"},
	"delivery_retention_roots":      {"flow_incarnation_id", "minimum_position_id", "retained_after", "updated_at"},
	"source_ack_retention_roots":    {"flow_incarnation_id", "position_id", "root_kind", "root_id", "created_at", "released_at"},
	"artifact_streams":              {"flow_incarnation_id", "flow_id", "projection_id", "consumer_fingerprint", "next_publication_sequence", "gc_epoch", "hard_retained_bytes", "backlog_count_high", "backlog_bytes_high", "backlog_age_high_seconds"},
	"artifact_objects":              {"artifact_id", "flow_incarnation_id", "logical_batch_id", "source_position", "fragment_ordinal", "namespace", "table_name", "schema_id", "partition_value", "shard", "first_record_ordinal", "record_count", "logical_content_hash", "encoded_byte_hash", "encoded_length", "bucket", "object_key", "version_id", "checksum_sha256", "state"},
	"artifact_publications":         {"publication_id", "flow_incarnation_id", "source_lineage_id", "source_transaction_id", "source_xid", "begin_lsn", "commit_lsn", "source_position", "checkpoint_lsn", "position_id", "content_hash", "logical_batch_id", "sequence", "checkpoint_metadata", "generation", "acquisition_id", "lease_epoch", "rooted_bytes", "published_at"},
	"artifact_publication_objects":  {"publication_id", "artifact_id", "ordinal", "release_marked_at", "released_at"},
	"artifact_barriers":             {"publication_id", "ordinal", "fragment_ordinal", "record_ordinal", "kind", "namespace", "table_name", "schema_id", "ddl", "ddl_plan", "content_hash"},
	"artifact_gc_claims":            {"artifact_id", "claim_epoch", "generation", "acquisition_id", "lease_epoch", "claim_kind", "publication_id"},
}

type requiredManagedObject struct {
	table string
	name  string
}

var requiredManagedConstraints = []requiredManagedObject{
	{table: "ddl_execution_run_attempts", name: "ddl_execution_run_attempts_pkey"},
	{table: "ddl_execution_run_attempts", name: "ddl_execution_run_attempts_event_id_fkey"},
	{table: "ddl_execution_run_attempts", name: "ddl_execution_run_attempts_flow_incarnation_id_fkey"},
	{table: "ddl_execution_run_attempts", name: "ddl_execution_run_attempts_acquisition_id_fkey"},
	{table: "ddl_execution_run_attempts", name: "ddl_execution_run_attempts_generation_check"},
	{table: "ddl_execution_run_attempts", name: "ddl_execution_run_attempts_lease_epoch_check"},
	{table: "ddl_execution_run_attempts", name: "ddl_execution_run_attempts_owner_key"},
	{table: "schema_publication_operations", name: "schema_publication_operations_pkey"},
	{table: "schema_publication_operations", name: "schema_publication_operations_flow_incarnation_id_fkey"},
	{table: "schema_publication_operations", name: "schema_publication_operations_acquisition_id_fkey"},
	{table: "schema_publication_operations", name: "schema_publication_operations_generation_check"},
	{table: "schema_publication_operations", name: "schema_publication_operations_lease_epoch_check"},
	{table: "schema_publication_operations", name: "schema_publication_operations_status_check"},
	{table: "schema_publication_operations", name: "schema_publication_operations_identity_key"},
	{table: "source_resources", name: "source_resources_flow_incarnation_id_fkey"},
	{table: "source_resources", name: "source_resources_acquisition_id_fkey"},
	{table: "source_resources", name: "source_resources_bootstrap_id_fkey"},
	{table: "snapshot_delivery_attempts", name: "snapshot_delivery_attempts_bootstrap_id_fkey"},
	{table: "snapshot_delivery_attempts", name: "snapshot_delivery_attempts_flow_incarnation_id_fkey"},
	{table: "snapshot_delivery_attempts", name: "snapshot_delivery_attempts_acquisition_id_fkey"},
	{table: "snapshot_delivery_receipts", name: "snapshot_delivery_receipts_attempt_id_fkey"},
	{table: "delivery_attempts", name: "delivery_attempts_state_valid"},
	{table: "delivery_attempts", name: "delivery_attempts_number_positive"},
	{table: "artifact_objects", name: "artifact_objects_version_evidence"},
	{table: "artifact_objects", name: "artifact_objects_record_count_positive"},
	{table: "artifact_gc_claims", name: "artifact_gc_claims_kind_valid"},
	{table: "artifact_gc_claims", name: "artifact_gc_claims_publication_kind"},
}

var requiredManagedIndexes = []requiredManagedObject{
	{table: "ddl_events", name: "ddl_events_fenced_incarnation_lsn_unique"},
	{table: "ddl_execution_run_attempts", name: "ddl_execution_run_attempts_event_destination_idx"},
	{table: "ddl_execution_run_attempts", name: "ddl_execution_run_attempts_incarnation_idx"},
	{table: "ddl_execution_run_attempts", name: "ddl_execution_run_attempts_acquisition_idx"},
	{table: "schema_publication_operations", name: "schema_publication_operations_incarnation_idx"},
	{table: "schema_publication_operations", name: "schema_publication_operations_acquisition_idx"},
	{table: "source_resources", name: "source_resources_current_kind_idx"},
	{table: "source_resources", name: "source_resources_active_physical_name_unique"},
	{table: "delivery_manifests", name: "delivery_manifests_logical_batch_idx"},
	{table: "delivery_receipts", name: "delivery_receipts_logical_batch_idx"},
	{table: "delivery_attempts", name: "delivery_attempts_retry_idx"},
	{table: "source_ack_retention_roots", name: "source_ack_retention_roots_active_idx"},
	{table: "artifact_objects", name: "artifact_objects_logical_shard_idx"},
	{table: "artifact_publications", name: "artifact_publications_logical_batch_idx"},
	{table: "artifact_publications", name: "artifact_publications_sequence_idx"},
	{table: "artifact_publication_objects", name: "artifact_publication_objects_active_roots_idx"},
}

func verifyManagedAuthoritySchema(ctx context.Context, pool *pgxpool.Pool) error {
	var missingTables []string
	if err := pool.QueryRow(ctx, `
SELECT COALESCE(array_agg(name ORDER BY name) FILTER (WHERE to_regclass(name) IS NULL),'{}')
FROM unnest($1::text[]) AS expected(name)`, authorityMutableTables).Scan(&missingTables); err != nil {
		return fmt.Errorf("verify required managed tables: %w", err)
	}
	if len(missingTables) > 0 {
		return fmt.Errorf("managed authority schema missing required tables %v; stop managed workers and repair the affected domain migration ledger", missingTables)
	}

	var missingColumns []string
	for table, columns := range requiredManagedColumns {
		var missing []string
		if err := pool.QueryRow(ctx, `
SELECT COALESCE(array_agg(column_name ORDER BY column_name) FILTER (WHERE attribute.attname IS NULL),'{}')
FROM unnest($2::text[]) AS expected(column_name)
LEFT JOIN pg_catalog.pg_attribute AS attribute
  ON attribute.attrelid=to_regclass($1) AND attribute.attname=expected.column_name
 AND attribute.attnum>0 AND NOT attribute.attisdropped`, table, columns).Scan(&missing); err != nil {
			return fmt.Errorf("verify managed columns for %s: %w", table, err)
		}
		for _, column := range missing {
			missingColumns = append(missingColumns, table+"."+column)
		}
	}
	if len(missingColumns) > 0 {
		return fmt.Errorf("managed authority schema missing required columns %v; restore the exact migration-defined columns before startup", missingColumns)
	}

	var invalidConstraints []string
	for _, expected := range requiredManagedConstraints {
		var valid bool
		if err := pool.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1 FROM pg_catalog.pg_constraint
  WHERE conrelid=to_regclass($1) AND conname=$2 AND convalidated
)`, expected.table, expected.name).Scan(&valid); err != nil {
			return fmt.Errorf("verify managed constraint %s.%s: %w", expected.table, expected.name, err)
		}
		if !valid {
			invalidConstraints = append(invalidConstraints, expected.table+"."+expected.name)
		}
	}
	if len(invalidConstraints) > 0 {
		return fmt.Errorf("managed authority schema missing or unvalidated required FK/constraints %v; restore them before starting managed workers", invalidConstraints)
	}

	var invalidIndexes []string
	for _, expected := range requiredManagedIndexes {
		var valid bool
		if err := pool.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM pg_catalog.pg_class AS index_relation
  JOIN pg_catalog.pg_index AS index_row ON index_row.indexrelid=index_relation.oid
  WHERE index_relation.oid=to_regclass($2)
    AND index_row.indrelid=to_regclass($1)
    AND index_row.indisvalid AND index_row.indisready
)`, expected.table, expected.name).Scan(&valid); err != nil {
			return fmt.Errorf("verify managed index %s.%s: %w", expected.table, expected.name, err)
		}
		if !valid {
			invalidIndexes = append(invalidIndexes, expected.table+"."+expected.name)
		}
	}
	if len(invalidIndexes) > 0 {
		return fmt.Errorf("managed authority schema missing or invalid required indexes %v; restore them before startup", invalidIndexes)
	}

	var activePhysicalNameIndexExact bool
	if err := pool.QueryRow(ctx, `
SELECT COALESCE((
  SELECT index_row.indisunique
    AND ARRAY(
      SELECT attribute.attname::text
      FROM unnest(index_row.indkey::smallint[]) WITH ORDINALITY AS key(attnum,ordinality)
      JOIN pg_catalog.pg_attribute AS attribute
        ON attribute.attrelid=index_row.indrelid AND attribute.attnum=key.attnum
      WHERE key.ordinality<=index_row.indnkeyatts
      ORDER BY key.ordinality
    )=ARRAY['source_system_id','database_name','resource_kind','physical_name']::text[]
    AND regexp_replace(pg_catalog.pg_get_expr(index_row.indpred,index_row.indrelid),'[[:space:]]','','g')
      ='(state<>''retired''::text)'
  FROM pg_catalog.pg_index AS index_row
  WHERE index_row.indexrelid=to_regclass('source_resources_active_physical_name_unique')
),FALSE)`).Scan(&activePhysicalNameIndexExact); err != nil {
		return fmt.Errorf("verify active physical source-resource uniqueness: %w", err)
	}
	if !activePhysicalNameIndexExact {
		return errors.New("managed authority schema requires source_resources_active_physical_name_unique to be a unique index on source_system_id,database_name,resource_kind,physical_name with predicate state <> 'retired'; restore migration 005 before startup")
	}

	var invalidTriggers []string
	for _, table := range authorityMutableTables {
		var exact bool
		if err := pool.QueryRow(ctx, `
SELECT COUNT(*)=1 AND bool_and(
  trigger.tgname=$2
  AND trigger.tgenabled='O'
  AND trigger.tgtype=31
  AND trigger.tgfoid='wallaby_require_authority_protocol_v2()'::regprocedure
)
FROM pg_catalog.pg_trigger AS trigger
WHERE trigger.tgrelid=to_regclass($1)
  AND NOT trigger.tgisinternal
  AND (trigger.tgname=$2 OR trigger.tgname=$3
       OR trigger.tgfoid='wallaby_require_authority_protocol_v2()'::regprocedure)`, table, table+"_require_authority_v2", table+"_require_authority_v1").Scan(&exact); err != nil {
			return fmt.Errorf("verify authority-v2 trigger for %s: %w", table, err)
		}
		if !exact {
			invalidTriggers = append(invalidTriggers, table)
		}
	}
	if len(invalidTriggers) > 0 {
		return fmt.Errorf("authority-v2 trigger coverage is not exact, enabled, or BEFORE ROW INSERT/UPDATE/DELETE for tables %v; do not start workers until repaired", invalidTriggers)
	}
	return nil
}
