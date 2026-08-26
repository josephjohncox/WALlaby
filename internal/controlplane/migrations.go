// Package controlplane owns the single ordered control-PostgreSQL migration
// entrypoint used by production server and worker processes.
package controlplane

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/controlstore"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/registry"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/pgstream"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// MigrationDomains returns the monotonic dependency order. Authority tables
// are part of workflow; package schemas follow the core authority domains and
// use the same authoritative ledger.
func MigrationDomains() []string {
	return []string{"workflow", "checkpoint", "registry", "controlplane", "delivery", "bootstrap", "artifactlog", "pgstream", "schema_registry"}
}

// ApplyMigrations applies every control schema under the shared coordinator
// lock and checksum history before production startup opens components.
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
		{name: "pgstream", apply: pgstream.ApplyMigrations},
		{name: "schema_registry", apply: schemaregistry.ApplyMigrations},
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
	if err := controlstore.ApplyMigrations(ctx, pool, "controlplane", migrationFS, "migrations/*.sql"); err != nil {
		return fmt.Errorf("apply centralized controlplane migrations: %w", err)
	}
	return nil
}

var authorityMutableTables = []string{
	"flows", "flow_incarnations", "flow_state_events", "flow_executions", "execution_acquisitions", "producer_leases", "work_claims",
	"checkpoints", "checkpoint_outbox", "authoritative_checkpoints", "authoritative_checkpoint_outbox",
	"schema_versions", "ddl_events", "ddl_execution_attempts", "ddl_execution_receipts", "ddl_execution_manifests", "ddl_execution_run_attempts", "schema_publication_operations", "managed_schema_baselines",
	"destination_revisions", "delivery_manifests", "delivery_attempts", "delivery_attempt_evidence", "delivery_receipts", "source_ack_intents", "source_ack_receipts", "delivery_retention_roots", "source_ack_retention_roots",
	"source_bootstraps", "source_bootstrap_tasks", "snapshot_publication_receipts", "source_resources", "source_resource_operations", "snapshot_delivery_attempts", "snapshot_delivery_evidence", "snapshot_delivery_receipts",
	"canonical_schemas", "artifact_streams", "artifact_objects", "artifact_upload_attempts", "artifact_publications", "artifact_publication_objects", "artifact_barriers", "artifact_deliveries", "artifact_quota_accounts", "artifact_quota_reservations", "artifact_gc_claims", "artifact_delivery_attempts", "artifact_delivery_receipts", "artifact_consumer_checkpoints", "artifact_metadata_prune_claims",
}

var requiredManagedColumns = map[string][]string{
	"flow_incarnations":              {"incarnation_id", "flow_id", "created_at", "retired_at"},
	"execution_acquisitions":         {"acquisition_id", "incarnation_id", "generation", "execution_id", "lease_epoch"},
	"producer_leases":                {"incarnation_id", "acquisition_id", "generation", "lease_epoch", "lease_expires_at"},
	"authoritative_checkpoints":      {"flow_incarnation_id", "flow_id", "generation", "acquisition_id", "lease_epoch", "lsn", "metadata"},
	"schema_versions":                {"flow_id", "namespace", "name", "version", "schema_json", "flow_incarnation_id", "generation", "acquisition_id", "lease_epoch", "authority_origin"},
	"ddl_events":                     {"id", "flow_id", "lsn", "flow_incarnation_id", "generation", "acquisition_id", "lease_epoch", "authority_origin"},
	"ddl_execution_run_attempts":     {"attempt_id", "event_id", "destination", "flow_incarnation_id", "flow_id", "lsn", "generation", "acquisition_id", "lease_epoch", "started_at"},
	"schema_publication_operations":  {"operation_id", "flow_incarnation_id", "flow_id", "subject", "schema_fingerprint", "registry_revision", "generation", "acquisition_id", "lease_epoch", "status", "external_id", "prepared_at", "completed_at"},
	"managed_schema_baselines":       {"flow_id", "flow_incarnation_id", "source_lineage_id", "source_namespace", "source_relation", "generation", "acquisition_id", "lease_epoch", "schema_json", "schema_fingerprint", "updated_at"},
	"source_bootstraps":              {"bootstrap_id", "flow_incarnation_id", "bootstrap_generation", "owner_generation", "owner_acquisition_id", "owner_lease_epoch", "slot_name", "publication_name", "consistent_lsn", "manifest_hash", "phase"},
	"source_bootstrap_tasks":         {"bootstrap_id", "relation_id", "task_id", "table_schema", "table_name", "schema_json", "key_columns", "destination_schema_json", "write_policy_json", "projection_fingerprint", "projection_version", "flow_incarnation_id", "generation", "acquisition_id", "lease_epoch", "authority_origin"},
	"source_resources":               {"flow_incarnation_id", "resource_kind", "resource_id", "generation", "acquisition_id", "lease_epoch", "created_generation", "created_acquisition_id", "created_lease_epoch", "ownership", "revision", "state"},
	"source_resource_operations":     {"operation_id", "flow_incarnation_id", "resource_kind", "resource_id", "operation", "desired_revision", "generation", "acquisition_id", "lease_epoch", "status"},
	"snapshot_delivery_attempts":     {"attempt_id", "bootstrap_id", "relation_id", "task_id", "batch_ordinal", "flow_incarnation_id", "generation", "acquisition_id", "lease_epoch", "claim_epoch"},
	"snapshot_delivery_receipts":     {"bootstrap_id", "relation_id", "task_id", "batch_ordinal", "attempt_id", "durable_cursor", "completed_task"},
	"delivery_attempts":              {"attempt_id", "flow_incarnation_id", "generation", "acquisition_id", "lease_epoch", "logical_batch_id", "position_id", "content_hash", "attempt_number", "attempt_state", "next_attempt_at"},
	"delivery_receipts":              {"flow_incarnation_id", "position_id", "destination_revision_id", "logical_batch_id", "attempt_id", "content_hash"},
	"source_ack_intents":             {"flow_incarnation_id", "position_id", "checkpoint_lsn", "generation", "acquisition_id", "lease_epoch"},
	"source_ack_receipts":            {"flow_incarnation_id", "position_id", "checkpoint_lsn", "observed_flush_lsn", "generation", "acquisition_id", "lease_epoch"},
	"delivery_retention_roots":       {"flow_incarnation_id", "minimum_position_id", "retained_after", "updated_at"},
	"source_ack_retention_roots":     {"flow_incarnation_id", "position_id", "root_kind", "root_id", "created_at", "released_at"},
	"artifact_streams":               {"flow_incarnation_id", "flow_id", "projection_id", "consumer_fingerprint", "next_publication_sequence", "gc_epoch", "hard_retained_bytes", "backlog_count_high", "backlog_bytes_high", "backlog_age_high_seconds"},
	"artifact_objects":               {"artifact_id", "flow_incarnation_id", "logical_batch_id", "source_position", "fragment_ordinal", "namespace", "table_name", "schema_id", "partition_value", "shard", "first_record_ordinal", "record_count", "logical_content_hash", "encoded_byte_hash", "encoded_length", "bucket", "object_key", "version_id", "checksum_sha256", "state"},
	"artifact_publications":          {"publication_id", "flow_incarnation_id", "source_lineage_id", "source_transaction_id", "source_xid", "begin_lsn", "commit_lsn", "source_position", "checkpoint_lsn", "position_id", "content_hash", "logical_batch_id", "sequence", "checkpoint_metadata", "generation", "acquisition_id", "lease_epoch", "rooted_bytes", "published_at", "schema_baseline_payload", "schema_baseline_fingerprint"},
	"artifact_publication_objects":   {"publication_id", "artifact_id", "ordinal", "release_marked_at", "released_at"},
	"artifact_barriers":              {"publication_id", "ordinal", "fragment_ordinal", "record_ordinal", "kind", "namespace", "table_name", "schema_id", "ddl", "ddl_plan", "content_hash"},
	"artifact_gc_claims":             {"artifact_id", "claim_epoch", "generation", "acquisition_id", "lease_epoch", "claim_kind", "publication_id"},
	"artifact_delivery_attempts":     {"attempt_id", "flow_incarnation_id", "consumer_revision_id", "publication_id", "generation", "acquisition_id", "lease_epoch", "prepared_at", "commit_id", "manifest_sha256", "logical_batch_id"},
	"artifact_delivery_receipts":     {"flow_incarnation_id", "consumer_revision_id", "publication_id", "attempt_id", "snapshot_id", "content_hash", "acquisition_id", "lease_epoch", "committed_at", "commit_id", "logical_batch_id", "publication_sequence", "position_id", "checkpoint_lsn", "snapshot_ids"},
	"artifact_consumer_checkpoints":  {"flow_incarnation_id", "consumer_revision_id", "publication_sequence", "publication_id", "position_id", "checkpoint_lsn", "commit_id", "snapshot_id", "advanced_at"},
	"artifact_metadata_prune_claims": {"publication_id", "flow_incarnation_id", "generation", "acquisition_id", "lease_epoch", "claim_epoch", "artifact_ids", "eligible_at", "claimed_at", "updated_at"},
}

type exactManagedColumn struct {
	name        string
	dataType    string
	notNull     bool
	defaultExpr string
	identity    string
	generated   string
}

var exactAuthorityColumns = map[string][]exactManagedColumn{
	"delivery_manifests": {
		{name: "flow_incarnation_id", dataType: "uuid", notNull: true},
		{name: "destination_revision_id", dataType: "text", notNull: true},
		{name: "source_lineage_id", dataType: "text", notNull: true},
		{name: "position_id", dataType: "text", notNull: true},
		{name: "source_transaction_id", dataType: "text", notNull: true},
		{name: "content_hash", dataType: "text", notNull: true},
		{name: "checkpoint_lsn", dataType: "text", notNull: true},
		{name: "created_at", dataType: "timestamp with time zone", notNull: true, defaultExpr: "clock_timestamp()"},
		{name: "logical_batch_id", dataType: "text", notNull: true},
		{name: "checkpoint_metadata", dataType: "jsonb", notNull: true},
		{name: "checkpoint_timestamp", dataType: "timestamp with time zone", notNull: true},
		{name: "schema_baseline_payload", dataType: "jsonb", notNull: true},
		{name: "schema_baseline_fingerprint", dataType: "text", notNull: true},
	},
	"artifact_delivery_attempts": {
		{name: "attempt_id", dataType: "uuid", notNull: true},
		{name: "flow_incarnation_id", dataType: "uuid", notNull: true},
		{name: "consumer_revision_id", dataType: "text", notNull: true},
		{name: "publication_id", dataType: "uuid", notNull: true},
		{name: "generation", dataType: "bigint", notNull: true},
		{name: "acquisition_id", dataType: "uuid", notNull: true},
		{name: "lease_epoch", dataType: "bigint", notNull: true},
		{name: "prepared_at", dataType: "timestamp with time zone", notNull: true, defaultExpr: "clock_timestamp()"},
		{name: "commit_id", dataType: "text", notNull: true},
		{name: "manifest_sha256", dataType: "text", notNull: true},
		{name: "logical_batch_id", dataType: "text", notNull: true},
	},
	"artifact_delivery_receipts": {
		{name: "flow_incarnation_id", dataType: "uuid", notNull: true},
		{name: "consumer_revision_id", dataType: "text", notNull: true},
		{name: "publication_id", dataType: "uuid", notNull: true},
		{name: "attempt_id", dataType: "uuid", notNull: true},
		{name: "snapshot_id", dataType: "text", notNull: true},
		{name: "content_hash", dataType: "text", notNull: true},
		{name: "acquisition_id", dataType: "uuid", notNull: true},
		{name: "lease_epoch", dataType: "bigint", notNull: true},
		{name: "committed_at", dataType: "timestamp with time zone", notNull: true, defaultExpr: "clock_timestamp()"},
		{name: "commit_id", dataType: "text", notNull: true},
		{name: "logical_batch_id", dataType: "text", notNull: true},
		{name: "publication_sequence", dataType: "bigint", notNull: true, defaultExpr: "0"},
		{name: "position_id", dataType: "text", notNull: true, defaultExpr: "''::text"},
		{name: "checkpoint_lsn", dataType: "text", notNull: true, defaultExpr: "''::text"},
		{name: "snapshot_ids", dataType: "jsonb", notNull: true, defaultExpr: "'{}'::jsonb"},
	},
	"artifact_consumer_checkpoints": {
		{name: "flow_incarnation_id", dataType: "uuid", notNull: true},
		{name: "consumer_revision_id", dataType: "text", notNull: true},
		{name: "publication_sequence", dataType: "bigint", notNull: true},
		{name: "publication_id", dataType: "uuid", notNull: true},
		{name: "position_id", dataType: "text", notNull: true},
		{name: "checkpoint_lsn", dataType: "text", notNull: true},
		{name: "commit_id", dataType: "text", notNull: true},
		{name: "snapshot_id", dataType: "text", notNull: true},
		{name: "advanced_at", dataType: "timestamp with time zone", notNull: true, defaultExpr: "clock_timestamp()"},
	},
}

type exactAuthorityConstraint struct {
	table            string
	name             string
	kind             string
	columns          []string
	definition       string
	definitionSHA256 string
	noInherit        bool
}

const artifactProjectionMappingConstraintDefinition = "CHECK (projection_id = 'canonical_cdc_parquet_v1'::text AND mapping_fingerprint = ''::text OR projection_id = 'canonical_cdc_parquet_v2'::text AND mapping_fingerprint <> ''::text)"

var exactAuthorityConstraints = []exactAuthorityConstraint{
	{table: "delivery_manifests", name: "delivery_manifests_pkey", kind: "p", noInherit: true, columns: []string{"flow_incarnation_id", "destination_revision_id", "position_id"}, definitionSHA256: "dd9e7d0865213ca03a3aa15aa34573d844c45ef6c00475f643ad3e5c6d0566a8"},
	{table: "delivery_manifests", name: "delivery_manifests_logical_batch_current", kind: "c", columns: []string{"logical_batch_id", "source_lineage_id", "position_id", "content_hash"}, definitionSHA256: "915b56facf7930044439b61996d57c507de416ce97022f2b6f3302bace331647"},
	{table: "delivery_manifests", name: "delivery_manifests_schema_baseline_fingerprint_check", kind: "c", columns: []string{"schema_baseline_fingerprint"}, definitionSHA256: "e5f7f7fde4b3288bfd1370e9358a2ee1e1feb95e2628a47c9f501be33938d75e"},
	{table: "schema_versions", name: "schema_versions_pkey", kind: "p", noInherit: true, columns: []string{"flow_id", "namespace", "name", "version"}, definition: "PRIMARY KEY (flow_id, namespace, name, version)"},
	{table: "schema_versions", name: "schema_versions_authority_complete", kind: "c", definition: "CHECK (authority_origin = 'legacy_unfenced'::text AND flow_incarnation_id IS NULL AND generation IS NULL AND acquisition_id IS NULL AND lease_epoch IS NULL OR authority_origin = 'fenced'::text AND flow_incarnation_id IS NOT NULL AND generation > 0 AND acquisition_id IS NOT NULL AND lease_epoch > 0)"},
	{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_pkey", kind: "p", noInherit: true, columns: []string{"attempt_id"}, definition: "PRIMARY KEY (attempt_id)"},
	{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_flow_incarnation_id_fkey", kind: "f", noInherit: true, columns: []string{"flow_incarnation_id"}, definition: "FOREIGN KEY (flow_incarnation_id) REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT"},
	{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_publication_id_fkey", kind: "f", noInherit: true, columns: []string{"publication_id"}, definition: "FOREIGN KEY (publication_id) REFERENCES artifact_publications(publication_id) ON DELETE RESTRICT"},
	{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_current_identity", kind: "c", definitionSHA256: "b860f1de2b6481c36195c7f05cf1ea15d3d7d65e93c2444b489e091f55b4a10c"},
	{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_publication_unique", kind: "u", noInherit: true, columns: []string{"flow_incarnation_id", "consumer_revision_id", "publication_id"}, definition: "UNIQUE (flow_incarnation_id, consumer_revision_id, publication_id)"},
	{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_commit_unique", kind: "u", noInherit: true, columns: []string{"flow_incarnation_id", "consumer_revision_id", "commit_id"}, definition: "UNIQUE (flow_incarnation_id, consumer_revision_id, commit_id)"},
	{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_pkey", kind: "p", noInherit: true, columns: []string{"flow_incarnation_id", "consumer_revision_id", "publication_id"}, definition: "PRIMARY KEY (flow_incarnation_id, consumer_revision_id, publication_id)"},
	{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_flow_incarnation_id_fkey", kind: "f", noInherit: true, columns: []string{"flow_incarnation_id"}, definition: "FOREIGN KEY (flow_incarnation_id) REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT"},
	{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_publication_id_fkey", kind: "f", noInherit: true, columns: []string{"publication_id"}, definition: "FOREIGN KEY (publication_id) REFERENCES artifact_publications(publication_id) ON DELETE RESTRICT"},
	{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_attempt_id_fkey", kind: "f", noInherit: true, columns: []string{"attempt_id"}, definition: "FOREIGN KEY (attempt_id) REFERENCES artifact_delivery_attempts(attempt_id) ON DELETE RESTRICT"},
	{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_current_identity", kind: "c", definitionSHA256: "99a23e70d602a0ddd9bf1eccf0255fc53de593a9d4877e80e63f83f654594a3c"},
	{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_attempt_unique", kind: "u", noInherit: true, columns: []string{"attempt_id"}, definition: "UNIQUE (attempt_id)"},
	{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_pkey", kind: "p", noInherit: true, columns: []string{"flow_incarnation_id", "consumer_revision_id"}, definition: "PRIMARY KEY (flow_incarnation_id, consumer_revision_id)"},
	{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_flow_incarnation_id_fkey", kind: "f", noInherit: true, columns: []string{"flow_incarnation_id"}, definition: "FOREIGN KEY (flow_incarnation_id) REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT"},
	{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_publication_id_fkey", kind: "f", noInherit: true, columns: []string{"publication_id"}, definition: "FOREIGN KEY (publication_id) REFERENCES artifact_publications(publication_id) ON DELETE RESTRICT"},
	{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_publication_sequence_check", kind: "c", definition: "CHECK (publication_sequence > 0)"},
	{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_flow_incarnation_id_consumer__key", kind: "u", noInherit: true, columns: []string{"flow_incarnation_id", "consumer_revision_id", "publication_sequence"}, definition: "UNIQUE (flow_incarnation_id, consumer_revision_id, publication_sequence)"},
	{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_flow_incarnation_id_consumer_key1", kind: "u", noInherit: true, columns: []string{"flow_incarnation_id", "consumer_revision_id", "publication_id"}, definition: "UNIQUE (flow_incarnation_id, consumer_revision_id, publication_id)"},
}

// selectiveAuthorityConstraints are individually exact but belong to tables
// whose complete constraint sets are owned by broader domain manifests.
var selectiveAuthorityConstraints = []exactAuthorityConstraint{
	{table: "canonical_schemas", name: "canonical_schemas_projection_mapping_contract", kind: "c", definition: artifactProjectionMappingConstraintDefinition},
	{table: "artifact_streams", name: "artifact_streams_projection_mapping_contract", kind: "c", definition: artifactProjectionMappingConstraintDefinition},
	{table: "artifact_objects", name: "artifact_objects_projection_mapping_contract", kind: "c", definition: artifactProjectionMappingConstraintDefinition},
	{table: "artifact_publications", name: "artifact_publications_projection_mapping_contract", kind: "c", definition: artifactProjectionMappingConstraintDefinition},
	{table: "artifact_publications", name: "artifact_publications_schema_baseline_fingerprint_check", kind: "c", definition: "CHECK (schema_baseline_fingerprint ~ '^[0-9a-f]{64}$'::text)"},
}

type exactAuthorityIndex struct {
	table      string
	name       string
	unique     bool
	primary    bool
	columns    []string
	options    []int16
	predicate  string
	expression string
}

var exactAuthorityIndexes = []exactAuthorityIndex{
	{table: "delivery_manifests", name: "delivery_manifests_pkey", unique: true, primary: true, columns: []string{"flow_incarnation_id", "destination_revision_id", "position_id"}, options: []int16{0, 0, 0}},
	{table: "delivery_manifests", name: "delivery_manifests_logical_batch_idx", unique: true, columns: []string{"flow_incarnation_id", "destination_revision_id", "logical_batch_id"}, options: []int16{0, 0, 0}},
	{table: "schema_versions", name: "schema_versions_pkey", unique: true, primary: true, columns: []string{"flow_id", "namespace", "name", "version"}, options: []int16{0, 0, 0, 0}},
	{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_pkey", unique: true, primary: true, columns: []string{"attempt_id"}, options: []int16{0}},
	{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_lookup_idx", columns: []string{"flow_incarnation_id", "consumer_revision_id", "publication_id", "prepared_at"}, options: []int16{0, 0, 0, 3}},
	{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_pkey", unique: true, primary: true, columns: []string{"flow_incarnation_id", "consumer_revision_id", "publication_id"}, options: []int16{0, 0, 0}},
	{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_attempt_idx", columns: []string{"attempt_id"}, options: []int16{0}},
	{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_commit_idx", columns: []string{"flow_incarnation_id", "consumer_revision_id", "commit_id", "prepared_at"}, options: []int16{0, 0, 0, 3}},
	{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_pkey", unique: true, primary: true, columns: []string{"flow_incarnation_id", "consumer_revision_id"}, options: []int16{0, 0}},
	{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_flow_incarnation_id_consumer__key", unique: true, columns: []string{"flow_incarnation_id", "consumer_revision_id", "publication_sequence"}, options: []int16{0, 0, 0}},
	{table: "artifact_consumer_checkpoints", name: "artifact_consumer_checkpoints_flow_incarnation_id_consumer_key1", unique: true, columns: []string{"flow_incarnation_id", "consumer_revision_id", "publication_id"}, options: []int16{0, 0, 0}},
	{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_publication_unique", unique: true, columns: []string{"flow_incarnation_id", "consumer_revision_id", "publication_id"}, options: []int16{0, 0, 0}},
	{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_commit_unique", unique: true, columns: []string{"flow_incarnation_id", "consumer_revision_id", "commit_id"}, options: []int16{0, 0, 0}},
	{table: "artifact_delivery_receipts", name: "artifact_delivery_receipts_attempt_unique", unique: true, columns: []string{"attempt_id"}, options: []int16{0}},
}

// selectiveAuthorityIndexes are individually exact but do not claim ownership
// of every index on their table.
var selectiveAuthorityIndexes = []exactAuthorityIndex{
	{table: "artifact_deliveries", name: "artifact_deliveries_pending_idx", columns: []string{"flow_incarnation_id", "consumer_revision_id", "sequence"}, options: []int16{0, 0, 0}, predicate: "(delivered_at IS NULL)"},
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
	{table: "managed_schema_baselines", name: "managed_schema_baselines_pkey"},
	{table: "managed_schema_baselines", name: "managed_schema_baselines_flow_incarnation_id_fkey"},
	{table: "managed_schema_baselines", name: "managed_schema_baselines_acquisition_id_fkey"},
	{table: "managed_schema_baselines", name: "managed_schema_baselines_generation_check"},
	{table: "managed_schema_baselines", name: "managed_schema_baselines_lease_epoch_check"},
	{table: "managed_schema_baselines", name: "managed_schema_baselines_lineage_check"},
	{table: "managed_schema_baselines", name: "managed_schema_baselines_namespace_check"},
	{table: "managed_schema_baselines", name: "managed_schema_baselines_relation_check"},
	{table: "managed_schema_baselines", name: "managed_schema_baselines_fingerprint_check"},
	{table: "source_resources", name: "source_resources_flow_incarnation_id_fkey"},
	{table: "source_resources", name: "source_resources_acquisition_id_fkey"},
	{table: "source_resources", name: "source_resources_bootstrap_id_fkey"},
	{table: "snapshot_delivery_attempts", name: "snapshot_delivery_attempts_bootstrap_id_fkey"},
	{table: "snapshot_delivery_attempts", name: "snapshot_delivery_attempts_flow_incarnation_id_fkey"},
	{table: "snapshot_delivery_attempts", name: "snapshot_delivery_attempts_acquisition_id_fkey"},
	{table: "snapshot_delivery_receipts", name: "snapshot_delivery_receipts_attempt_id_fkey"},
	{table: "delivery_attempts", name: "delivery_attempts_state_valid"},
	{table: "delivery_attempts", name: "delivery_attempts_number_positive"},
	{table: "artifact_publications", name: "artifact_publications_schema_baseline_fingerprint_check"},
	{table: "artifact_objects", name: "artifact_objects_version_evidence"},
	{table: "artifact_objects", name: "artifact_objects_record_count_positive"},
	{table: "artifact_gc_claims", name: "artifact_gc_claims_kind_valid"},
	{table: "artifact_gc_claims", name: "artifact_gc_claims_publication_kind"},
	{table: "artifact_metadata_prune_claims", name: "artifact_metadata_prune_claims_pkey"},
	{table: "artifact_metadata_prune_claims", name: "artifact_metadata_prune_claims_flow_incarnation_id_fkey"},
	{table: "artifact_metadata_prune_claims", name: "artifact_metadata_prune_claims_artifact_ids_array"},
}

var requiredManagedIndexes = []requiredManagedObject{
	{table: "ddl_events", name: "ddl_events_fenced_incarnation_lsn_unique"},
	{table: "ddl_execution_run_attempts", name: "ddl_execution_run_attempts_event_destination_idx"},
	{table: "ddl_execution_run_attempts", name: "ddl_execution_run_attempts_incarnation_idx"},
	{table: "ddl_execution_run_attempts", name: "ddl_execution_run_attempts_acquisition_idx"},
	{table: "schema_publication_operations", name: "schema_publication_operations_incarnation_idx"},
	{table: "schema_publication_operations", name: "schema_publication_operations_acquisition_idx"},
	{table: "managed_schema_baselines", name: "managed_schema_baselines_current_fence_idx"},
	{table: "source_resources", name: "source_resources_current_kind_idx"},
	{table: "source_resources", name: "source_resources_active_physical_name_unique"},
	{table: "delivery_receipts", name: "delivery_receipts_logical_batch_idx"},
	{table: "delivery_attempts", name: "delivery_attempts_retry_idx"},
	{table: "source_ack_retention_roots", name: "source_ack_retention_roots_active_idx"},
	{table: "artifact_objects", name: "artifact_objects_logical_shard_idx"},
	{table: "artifact_publications", name: "artifact_publications_logical_batch_idx"},
	{table: "artifact_publications", name: "artifact_publications_sequence_idx"},
	{table: "artifact_publication_objects", name: "artifact_publication_objects_active_roots_idx"},
	{table: "artifact_metadata_prune_claims", name: "artifact_metadata_prune_claims_flow_idx"},
	{table: "artifact_publications", name: "artifact_publications_metadata_retention_idx"},
	{table: "artifact_gc_claims", name: "artifact_gc_claims_publication_idx"},
	{table: "artifact_deliveries", name: "artifact_deliveries_publication_idx"},
	{table: "artifact_delivery_attempts", name: "artifact_delivery_attempts_publication_idx"},
}

type authorityCatalogQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func verifyManagedAuthoritySchema(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin managed authority verification: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SET LOCAL search_path = pg_catalog, public`); err != nil {
		return fmt.Errorf("pin managed authority verification search path: %w", err)
	}
	if err := verifyManagedAuthoritySchemaCatalog(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit managed authority verification: %w", err)
	}
	return nil
}

func verifyManagedAuthoritySchemaCatalog(ctx context.Context, pool authorityCatalogQueryer) error {
	var missingTables []string
	if err := pool.QueryRow(ctx, `
SELECT COALESCE(array_agg(name ORDER BY name) FILTER (
  WHERE to_regclass(pg_catalog.format('%I.%I','public',name)) IS NULL
),'{}')
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
 AND attribute.attnum>0 AND NOT attribute.attisdropped`, publicRegclass(table), columns).Scan(&missing); err != nil {
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
)`, publicRegclass(expected.table), expected.name).Scan(&valid); err != nil {
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
)`, publicRegclass(expected.table), publicRegclass(expected.name)).Scan(&valid); err != nil {
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
  WHERE index_row.indexrelid=to_regclass('"public"."source_resources_active_physical_name_unique"')
),FALSE)`).Scan(&activePhysicalNameIndexExact); err != nil {
		return fmt.Errorf("verify active physical source-resource uniqueness: %w", err)
	}
	if !activePhysicalNameIndexExact {
		return errors.New("managed authority schema requires source_resources_active_physical_name_unique to be a unique index on source_system_id,database_name,resource_kind,physical_name with predicate state <> 'retired'; restore migration 005 before startup")
	}
	if err := verifyExactAuthoritySchema(ctx, pool); err != nil {
		return err
	}

	var invalidTriggers []string
	for _, table := range authorityMutableTables {
		var exact bool
		if err := pool.QueryRow(ctx, `
SELECT COUNT(*)=1 AND bool_and(
  trigger.tgname=$2
  AND trigger.tgenabled='O'
  AND trigger.tgtype=31
  AND trigger.tgfoid='public.wallaby_require_authority_protocol_v2()'::regprocedure
)
FROM pg_catalog.pg_trigger AS trigger
WHERE trigger.tgrelid=to_regclass($1)
  AND NOT trigger.tgisinternal
  AND (trigger.tgname=$2 OR trigger.tgname=$3
       OR trigger.tgfoid='public.wallaby_require_authority_protocol_v2()'::regprocedure)`, publicRegclass(table), table+"_require_authority_v2", table+"_require_authority_v1").Scan(&exact); err != nil {
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

func verifyExactAuthoritySchema(ctx context.Context, pool authorityCatalogQueryer) error {
	if err := verifyExactAuthorityObjectSets(ctx, pool); err != nil {
		return err
	}
	for _, table := range []string{"delivery_manifests", "artifact_delivery_attempts", "artifact_delivery_receipts", "artifact_consumer_checkpoints"} {
		expectedColumns := exactAuthorityColumns[table]
		var actualNames []string
		if err := pool.QueryRow(ctx, `
SELECT COALESCE(array_agg(attribute.attname::text ORDER BY attribute.attnum),'{}'::text[])
FROM pg_catalog.pg_attribute AS attribute
WHERE attribute.attrelid=to_regclass($1) AND attribute.attnum>0 AND NOT attribute.attisdropped`, publicRegclass(table)).Scan(&actualNames); err != nil {
			return fmt.Errorf("verify exact managed authority columns for %s: %w", table, err)
		}
		expectedNames := make([]string, len(expectedColumns))
		for index, expected := range expectedColumns {
			expectedNames[index] = expected.name
		}
		if !equalStrings(actualNames, expectedNames) {
			return fmt.Errorf("managed authority schema requires exact columns for %s: database=%v binary=%v; migrations are never repaired at runtime", table, actualNames, expectedNames)
		}
		for _, expected := range expectedColumns {
			var dataType string
			var notNull bool
			var defaultExpr, identity, generated string
			if err := pool.QueryRow(ctx, `
SELECT pg_catalog.format_type(attribute.atttypid,attribute.atttypmod),attribute.attnotnull,
       COALESCE(pg_catalog.pg_get_expr(default_value.adbin,default_value.adrelid),''),
       attribute.attidentity::text,attribute.attgenerated::text
FROM pg_catalog.pg_attribute AS attribute
LEFT JOIN pg_catalog.pg_attrdef AS default_value
  ON default_value.adrelid=attribute.attrelid AND default_value.adnum=attribute.attnum
WHERE attribute.attrelid=to_regclass($1) AND attribute.attname=$2
  AND attribute.attnum>0 AND NOT attribute.attisdropped`, publicRegclass(table), expected.name).Scan(&dataType, &notNull, &defaultExpr, &identity, &generated); err != nil {
				return fmt.Errorf("verify exact managed authority column %s.%s: %w", table, expected.name, err)
			}
			if dataType != expected.dataType || notNull != expected.notNull || defaultExpr != expected.defaultExpr || identity != expected.identity || generated != expected.generated {
				return fmt.Errorf("managed authority schema column %s.%s differs: type=%q not_null=%t default=%q identity=%q generated=%q; expected type=%q not_null=%t default=%q identity=%q generated=%q", table, expected.name, dataType, notNull, defaultExpr, identity, generated, expected.dataType, expected.notNull, expected.defaultExpr, expected.identity, expected.generated)
			}
		}
	}

	constraintContracts := append(append([]exactAuthorityConstraint(nil), exactAuthorityConstraints...), selectiveAuthorityConstraints...)
	for _, expected := range constraintContracts {
		var kind, definition, indexName, method string
		var validated, deferrable, deferred, noInherit bool
		var unique, primary, valid, ready, live, predicateNull, expressionsNull bool
		var keyCount, attributeCount int
		var constraintColumns, indexColumns []string
		if err := pool.QueryRow(ctx, `
SELECT constraint_row.contype::text,constraint_row.convalidated,constraint_row.condeferrable,
       constraint_row.condeferred,constraint_row.connoinherit,
       pg_catalog.pg_get_constraintdef(constraint_row.oid,true),
       COALESCE(ARRAY(
         SELECT attribute.attname::text
         FROM unnest(constraint_row.conkey) WITH ORDINALITY AS constraint_key(attnum,ordinality)
         JOIN pg_catalog.pg_attribute AS attribute
           ON attribute.attrelid=constraint_row.conrelid AND attribute.attnum=constraint_key.attnum
         ORDER BY constraint_key.ordinality
       ),'{}'::text[]),
       COALESCE(index_relation.relname,''),COALESCE(access_method.amname,''),
       COALESCE(index_row.indisunique,false),COALESCE(index_row.indisprimary,false),
       COALESCE(index_row.indisvalid,false),COALESCE(index_row.indisready,false),
       COALESCE(index_row.indislive,false),index_row.indpred IS NULL,index_row.indexprs IS NULL,
       COALESCE(index_row.indnkeyatts,0),COALESCE(index_row.indnatts,0),
       COALESCE(ARRAY(
         SELECT attribute.attname::text
         FROM unnest(index_row.indkey::smallint[]) WITH ORDINALITY AS key(attnum,ordinality)
         JOIN pg_catalog.pg_attribute AS attribute
           ON attribute.attrelid=index_row.indrelid AND attribute.attnum=key.attnum
         WHERE key.ordinality<=index_row.indnkeyatts
         ORDER BY key.ordinality
       ),'{}'::text[])
FROM pg_catalog.pg_constraint AS constraint_row
LEFT JOIN pg_catalog.pg_class AS index_relation ON index_relation.oid=constraint_row.conindid
LEFT JOIN pg_catalog.pg_index AS index_row ON index_row.indexrelid=constraint_row.conindid
LEFT JOIN pg_catalog.pg_am AS access_method ON access_method.oid=index_relation.relam
WHERE constraint_row.conrelid=to_regclass($1) AND constraint_row.conname=$2`, publicRegclass(expected.table), expected.name).Scan(
			&kind, &validated, &deferrable, &deferred, &noInherit, &definition, &constraintColumns,
			&indexName, &method, &unique, &primary, &valid, &ready, &live,
			&predicateNull, &expressionsNull, &keyCount, &attributeCount, &indexColumns,
		); err != nil {
			return fmt.Errorf("verify exact managed authority constraint %s.%s: %w", expected.table, expected.name, err)
		}
		if kind != expected.kind || !validated || deferrable || deferred || noInherit != expected.noInherit {
			return fmt.Errorf("managed authority schema constraint %s.%s has weakened kind/validation/deferrability/inheritance: kind=%q validated=%t deferrable=%t deferred=%t no_inherit=%t", expected.table, expected.name, kind, validated, deferrable, deferred, noInherit)
		}
		if expected.definitionSHA256 != "" {
			digest := sha256.Sum256([]byte(definition))
			if hex.EncodeToString(digest[:]) != expected.definitionSHA256 {
				return fmt.Errorf("managed authority schema constraint %s.%s definition differs", expected.table, expected.name)
			}
		} else if definition != expected.definition {
			return fmt.Errorf("managed authority schema constraint %s.%s definition differs", expected.table, expected.name)
		}
		if len(expected.columns) > 0 && !equalStrings(constraintColumns, expected.columns) {
			return fmt.Errorf("managed authority schema constraint %s.%s columns differ: database=%v binary=%v", expected.table, expected.name, constraintColumns, expected.columns)
		}
		if expected.kind == "c" {
			if indexName != "" || method != "" || unique || primary || valid || ready || live {
				return fmt.Errorf("managed authority schema check constraint %s.%s has an unexpected backing index", expected.table, expected.name)
			}
			continue
		}
		if expected.kind == "f" {
			continue
		}
		expectedPrimary := expected.kind == "p"
		if indexName != expected.name || method != "btree" || !unique || primary != expectedPrimary || !valid || !ready || !live || !predicateNull || !expressionsNull || keyCount != len(expected.columns) || attributeCount != len(expected.columns) || !equalStrings(indexColumns, expected.columns) {
			return fmt.Errorf("managed authority schema key constraint/index %s.%s differs from the exact btree key contract", expected.table, expected.name)
		}
	}

	indexContracts := append(append([]exactAuthorityIndex(nil), exactAuthorityIndexes...), selectiveAuthorityIndexes...)
	for _, expected := range indexContracts {
		var method, predicate, expression string
		var unique, primary, valid, ready, live bool
		var keyCount, attributeCount int
		var columns []string
		var options []int16
		if err := pool.QueryRow(ctx, `
SELECT access_method.amname,index_row.indisunique,index_row.indisprimary,
       index_row.indisvalid,index_row.indisready,index_row.indislive,
       COALESCE(pg_catalog.pg_get_expr(index_row.indpred,index_row.indrelid),''),
       COALESCE(pg_catalog.pg_get_expr(index_row.indexprs,index_row.indrelid),''),
       index_row.indnkeyatts,index_row.indnatts,
       ARRAY(
         SELECT attribute.attname::text
         FROM unnest(index_row.indkey::smallint[]) WITH ORDINALITY AS key(attnum,ordinality)
         JOIN pg_catalog.pg_attribute AS attribute
           ON attribute.attrelid=index_row.indrelid AND attribute.attnum=key.attnum
         WHERE key.ordinality<=index_row.indnkeyatts
         ORDER BY key.ordinality
       ),index_row.indoption::smallint[]
FROM pg_catalog.pg_class AS index_relation
JOIN pg_catalog.pg_index AS index_row ON index_row.indexrelid=index_relation.oid
JOIN pg_catalog.pg_am AS access_method ON access_method.oid=index_relation.relam
WHERE index_relation.oid=to_regclass($2) AND index_row.indrelid=to_regclass($1)
  AND index_relation.relkind='i'`, publicRegclass(expected.table), publicRegclass(expected.name)).Scan(
			&method, &unique, &primary, &valid, &ready, &live, &predicate,
			&expression, &keyCount, &attributeCount, &columns, &options,
		); err != nil {
			return fmt.Errorf("verify exact managed authority index %s.%s: %w", expected.table, expected.name, err)
		}
		if method != "btree" || unique != expected.unique || primary != expected.primary || !valid || !ready || !live || predicate != expected.predicate || expression != expected.expression || keyCount != len(expected.columns) || attributeCount != len(expected.columns) || !equalStrings(columns, expected.columns) || !equalInt16s(options, expected.options) {
			return fmt.Errorf("managed authority schema index %s.%s differs from the exact btree key/predicate/expression contract", expected.table, expected.name)
		}
	}
	return nil
}

func verifyExactAuthorityObjectSets(ctx context.Context, pool authorityCatalogQueryer) error {
	constraintNames := make(map[string][]string)
	constraintSeen := make(map[string]struct{})
	for _, contract := range exactAuthorityConstraints {
		key := contract.table + "\x00" + contract.name
		if _, duplicate := constraintSeen[key]; duplicate {
			return fmt.Errorf("exact authority constraint manifest duplicates %s.%s", contract.table, contract.name)
		}
		constraintSeen[key] = struct{}{}
		constraintNames[contract.table] = append(constraintNames[contract.table], contract.name)
	}
	indexNames := make(map[string][]string)
	indexCounts := make(map[string]int)
	for _, contract := range exactAuthorityIndexes {
		key := contract.table + "\x00" + contract.name
		indexCounts[key]++
		if indexCounts[key] != 1 {
			return fmt.Errorf("exact authority index manifest duplicates %s.%s", contract.table, contract.name)
		}
		indexNames[contract.table] = append(indexNames[contract.table], contract.name)
	}
	for _, contract := range exactAuthorityConstraints {
		if contract.kind != "p" && contract.kind != "u" {
			continue
		}
		if indexCounts[contract.table+"\x00"+contract.name] != 1 {
			return fmt.Errorf("exact authority constraint backing index %s.%s must be represented exactly once", contract.table, contract.name)
		}
	}
	for table, expected := range constraintNames {
		sort.Strings(expected)
		var actual []string
		if err := pool.QueryRow(ctx, `
SELECT COALESCE(array_agg(constraint_row.conname::text ORDER BY constraint_row.conname),'{}'::text[])
FROM pg_catalog.pg_constraint AS constraint_row
WHERE constraint_row.conrelid=to_regclass($1)`, publicRegclass(table)).Scan(&actual); err != nil {
			return fmt.Errorf("enumerate exact authority constraints for %s: %w", table, err)
		}
		if !equalStrings(actual, expected) {
			return fmt.Errorf("managed authority schema constraint set for %s differs: database=%v binary=%v", table, actual, expected)
		}
	}
	for table, expected := range indexNames {
		sort.Strings(expected)
		var actual []string
		if err := pool.QueryRow(ctx, `
SELECT COALESCE(array_agg(index_relation.relname::text ORDER BY index_relation.relname),'{}'::text[])
FROM pg_catalog.pg_index AS index_row
JOIN pg_catalog.pg_class AS index_relation ON index_relation.oid=index_row.indexrelid
WHERE index_row.indrelid=to_regclass($1) AND index_relation.relkind='i'`, publicRegclass(table)).Scan(&actual); err != nil {
			return fmt.Errorf("enumerate exact authority indexes for %s: %w", table, err)
		}
		if !equalStrings(actual, expected) {
			return fmt.Errorf("managed authority schema index set for %s differs: database=%v binary=%v", table, actual, expected)
		}
	}
	return nil
}

func publicRegclass(name string) string {
	return pgx.Identifier{"public", name}.Sanitize()
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalInt16s(left, right []int16) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
