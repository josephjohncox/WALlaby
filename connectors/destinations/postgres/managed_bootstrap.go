package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// PrepareBootstrap creates generation-qualified durable staging tables. The
// destination rows are evidence only; control PostgreSQL remains authoritative
// for whether this generation may be published.
func (d *Destination) PrepareBootstrap(ctx context.Context, intent connector.BootstrapIntent, tables []connector.BootstrapTable) error {
	if err := intent.Validate(); err != nil {
		return err
	}
	if d.pool == nil {
		return errors.New("postgres destination not initialized")
	}
	if len(tables) == 0 {
		return errors.New("bootstrap manifest has no tables")
	}
	seenTargets := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		if err := d.Capabilities().SupportsTablePolicy(table.WritePolicy); err != nil {
			return fmt.Errorf("bootstrap table write policy: %w", err)
		}
		schema := table.Schema
		target, _, _ := d.bootstrapTables(intent, schema)
		if _, exists := seenTargets[target]; exists {
			return fmt.Errorf("bootstrap manifest maps multiple source tables to destination %s", target)
		}
		seenTargets[target] = struct{}{}
		targetSchema, targetTable, _ := d.bootstrapTableCoordinates(intent, schema)
		if err := d.rejectForeignKeyBootstrapTarget(ctx, targetSchema, targetTable); err != nil {
			return err
		}
		if d.spec.Options[optManagedProfile] == connector.ManagedProfilePostgresToPostgresV1 {
			if err := d.validateManagedTargetSchema(ctx, d.pool, schema, table.WritePolicy); err != nil {
				return fmt.Errorf("admit managed bootstrap target %s: %w", target, err)
			}
		}
	}
	tx, err := d.beginWriteTransaction(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := ensureManagedBootstrapTables(ctx, tx); err != nil {
		return err
	}
	for _, table := range tables {
		schema := table.Schema
		target, _, stageName := d.bootstrapTables(intent, schema)
		targetSchema, targetTable, stageTable := d.bootstrapTableCoordinates(intent, schema)
		if _, err := tx.Exec(ctx, `SELECT wallaby.prepare_managed_bootstrap_stage($1,$2,$3,$4)`, targetSchema, targetTable, targetSchema, stageTable); err != nil {
			return fmt.Errorf("create bootstrap stage for %s: %w", target, err)
		}
		if err := verifyManagedBootstrapStage(ctx, tx, targetSchema, targetTable, targetSchema, stageTable); err != nil {
			return fmt.Errorf("bootstrap stage %s contract: %w", stageName, err)
		}
		tag, err := tx.Exec(ctx, `
INSERT INTO wallaby.managed_bootstrap_tables (
  bootstrap_id,manifest_hash,target_table,stage_table
) VALUES ($1,$2,$3,$4)
ON CONFLICT (bootstrap_id,target_table) DO UPDATE SET
  stage_table=EXCLUDED.stage_table
WHERE wallaby.managed_bootstrap_tables.manifest_hash=EXCLUDED.manifest_hash
  AND wallaby.managed_bootstrap_tables.stage_table=EXCLUDED.stage_table`, intent.BootstrapID, intent.ManifestHash, target, stageName)
		if err != nil {
			return fmt.Errorf("record bootstrap stage: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("%w: bootstrap stage identity conflict for %s", connector.ErrDeliveryConflict, target)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit bootstrap stage preparation: %w", err)
	}
	return nil
}

// ApplyBootstrap writes one immutable snapshot batch to its generation stage
// and commits the deterministic destination marker in the same transaction.
func (d *Destination) ApplyBootstrap(ctx context.Context, bootstrap connector.BootstrapIntent, intent connector.DeliveryIntent, batch connector.Batch) (connector.DeliveryEvidence, error) {
	if err := bootstrap.Validate(); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	if err := intent.Validate(); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	if strings.TrimSpace(intent.LogicalBatchID) == "" {
		return connector.DeliveryEvidence{}, errors.New("managed PostgreSQL bootstrap delivery requires logical_batch_id")
	}
	if d.pool == nil {
		return connector.DeliveryEvidence{}, errors.New("postgres destination not initialized")
	}
	contentHash, err := connector.BatchContentHash(batch)
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	if contentHash != intent.ContentHash {
		return connector.DeliveryEvidence{}, fmt.Errorf("%w: bootstrap intent hash mismatch", connector.ErrDeliveryConflict)
	}
	evidence := connector.DeliveryEvidence{ExternalID: postgresDeliveryMarkerID(intent), ContentHash: contentHash}
	tx, err := d.beginWriteTransaction(ctx)
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	existing, err := d.loadManagedReceipt(ctx, tx, intent)
	switch {
	case err == nil:
		if existing != contentHash {
			return connector.DeliveryEvidence{}, fmt.Errorf("%w: bootstrap destination marker hash conflict", connector.ErrDeliveryConflict)
		}
		return evidence, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return connector.DeliveryEvidence{}, err
	}
	_, stage, _ := d.bootstrapTables(bootstrap, batch.Schema)
	for _, record := range batch.Records {
		if record.Operation != connector.OpLoad && record.Operation != connector.OpInsert {
			return connector.DeliveryEvidence{}, fmt.Errorf("managed bootstrap accepts only load/insert records, got %s", record.Operation)
		}
	}
	mode := writeModeTarget
	if batch.WritePolicy.Mode == connector.ResolvedWriteAppend {
		mode = writeModeAppend
	}
	if batch.WritePolicy.Mode != connector.ResolvedWriteAppend && batch.WritePolicy.Mode != connector.ResolvedWriteUpsert {
		return connector.DeliveryEvidence{}, fmt.Errorf("bootstrap batch requires mapped append/upsert policy, got %q", batch.WritePolicy.Mode)
	}
	// Watermark state is seeded from the atomically published target below;
	// writing final-key state while this generation is still abandonable would
	// incorrectly fence live CDC after an abandoned bootstrap.
	policy := batch.WritePolicy
	policy.WatermarkColumn = ""
	if err := d.applyBatch(ctx, tx, stage, batch.Schema, batch.Records, mode, policy); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	if err := d.insertManagedReceipt(ctx, tx, intent, evidence.ExternalID); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return evidence, fmt.Errorf("%w: postgres bootstrap batch commit: %w", connector.ErrDeliveryIndeterminate, err)
	}
	return evidence, nil
}

// ReconcileBootstrap proves an ambiguous stage write through the marker that
// was committed atomically with the staged rows.
func (d *Destination) ReconcileBootstrap(ctx context.Context, _ connector.BootstrapIntent, intent connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	return d.Reconcile(ctx, intent)
}

// PublishBootstrap replaces every target from its matching generation stage in
// one PostgreSQL transaction. Concurrent CDC cannot observe a subset of tables.
func (d *Destination) PublishBootstrap(ctx context.Context, intent connector.BootstrapIntent, tables []connector.BootstrapTable) (connector.DeliveryEvidence, error) {
	if err := intent.Validate(); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	if d.pool == nil {
		return connector.DeliveryEvidence{}, errors.New("postgres destination not initialized")
	}
	externalID := bootstrapPublicationMarker(intent)
	evidence := connector.DeliveryEvidence{ExternalID: externalID, ContentHash: intent.ManifestHash}
	tx, err := d.beginWriteTransaction(ctx)
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := ensureManagedBootstrapTables(ctx, tx); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	var existingHash string
	err = tx.QueryRow(ctx, `SELECT manifest_hash FROM wallaby.managed_bootstrap_publications WHERE bootstrap_id=$1`, intent.BootstrapID).Scan(&existingHash)
	alreadyPublished := err == nil
	switch {
	case alreadyPublished:
		if existingHash != intent.ManifestHash {
			return connector.DeliveryEvidence{}, fmt.Errorf("%w: bootstrap publication hash conflict", connector.ErrDeliveryConflict)
		}
	case !errors.Is(err, pgx.ErrNoRows):
		return connector.DeliveryEvidence{}, err
	}
	sorted := append([]connector.BootstrapTable(nil), tables...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Schema.Namespace+"."+sorted[i].Schema.Name < sorted[j].Schema.Namespace+"."+sorted[j].Schema.Name
	})
	if !alreadyPublished {
		for _, table := range sorted {
			if table.WritePolicy.Mode == connector.ResolvedWriteUpsert && table.WritePolicy.WatermarkColumn != "" {
				if strings.TrimSpace(d.flowID) == "" {
					return connector.DeliveryEvidence{}, errors.New("watermark bootstrap requires flow_id")
				}
				if _, err := postgresWatermarkType(table.Schema, table.WritePolicy.WatermarkColumn); err != nil {
					return connector.DeliveryEvidence{}, err
				}
				if _, err := postgresKeyTypes(table.Schema, table.WritePolicy.KeyColumns); err != nil {
					return connector.DeliveryEvidence{}, err
				}
				if strings.TrimSpace(table.WritePolicy.ProjectionFingerprint) == "" {
					return connector.DeliveryEvidence{}, errors.New("watermark bootstrap requires projection fingerprint")
				}
				position, err := connector.CanonicalizeCheckpointPosition(table.SourcePosition)
				if err != nil || !strings.Contains(position, "/") {
					return connector.DeliveryEvidence{}, errors.New("watermark bootstrap requires canonical PostgreSQL source LSN")
				}
			}
		}
		for _, table := range sorted {
			schema := table.Schema
			target, _, _ := d.bootstrapTables(intent, schema)
			targetSchema, targetTable, stageTable := d.bootstrapTableCoordinates(intent, schema)
			columns := schemaColumns(schema)
			if len(columns) == 0 {
				return connector.DeliveryEvidence{}, fmt.Errorf("bootstrap schema %s.%s has no columns", schema.Namespace, schema.Name)
			}
			if _, err := tx.Exec(ctx, `SELECT wallaby.publish_managed_bootstrap_table($1,$2,$3,$4,$5)`, targetSchema, targetTable, targetSchema, stageTable, columns); err != nil {
				return connector.DeliveryEvidence{}, fmt.Errorf("publish bootstrap target %s: %w", target, err)
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO wallaby.managed_bootstrap_publications(bootstrap_id,manifest_hash,external_id) VALUES($1,$2,$3)`, intent.BootstrapID, intent.ManifestHash, externalID); err != nil {
			return connector.DeliveryEvidence{}, fmt.Errorf("record bootstrap publication marker: %w", err)
		}
	}
	// Reconcile watermark state from the final projected target on every publish
	// retry, including a previously committed publication marker.
	for _, table := range sorted {
		if table.WritePolicy.Mode != connector.ResolvedWriteUpsert || table.WritePolicy.WatermarkColumn == "" {
			continue
		}
		targetSchema, targetTable, _ := d.bootstrapTableCoordinates(intent, table.Schema)
		if err := d.seedBootstrapWatermarkState(ctx, tx, intent, targetSchema, targetTable, table); err != nil {
			return connector.DeliveryEvidence{}, err
		}
	}
	// Staging is generation-scoped mutable state. Remove it in the same
	// transaction as target publication while retaining the immutable marker.
	for _, table := range sorted {
		schema := table.Schema
		targetSchema, _, stageTable := d.bootstrapTableCoordinates(intent, schema)
		if _, err := tx.Exec(ctx, `SELECT wallaby.drop_managed_bootstrap_stage($1,$2)`, targetSchema, stageTable); err != nil {
			return connector.DeliveryEvidence{}, fmt.Errorf("drop published bootstrap stage %s: %w", stageTable, err)
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM wallaby.managed_bootstrap_tables WHERE bootstrap_id=$1 AND manifest_hash=$2`, intent.BootstrapID, intent.ManifestHash); err != nil {
		return connector.DeliveryEvidence{}, fmt.Errorf("remove published bootstrap staging manifest: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return evidence, fmt.Errorf("%w: postgres bootstrap publication commit: %w", connector.ErrDeliveryIndeterminate, err)
	}
	return evidence, nil
}

// ReconcileBootstrapPublication observes only the immutable marker committed
// atomically with target replacement; staging rows are never publication proof.
func (d *Destination) ReconcileBootstrapPublication(ctx context.Context, intent connector.BootstrapIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	if err := intent.Validate(); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if d.pool == nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, errors.New("postgres destination not initialized")
	}
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := verifyManagedBootstrapTables(ctx, tx); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf("verify managed bootstrap recovery contract: %w", err)
	}
	var manifestHash, externalID string
	err = tx.QueryRow(ctx, `SELECT manifest_hash,external_id FROM wallaby.managed_bootstrap_publications WHERE bootstrap_id=$1`, intent.BootstrapID).Scan(&manifestHash, &externalID)
	if errors.Is(err, pgx.ErrNoRows) {
		return connector.DeliveryNotApplied, connector.DeliveryEvidence{}, nil
	}
	if err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if manifestHash != intent.ManifestHash || externalID != bootstrapPublicationMarker(intent) {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf("%w: bootstrap publication marker conflict", connector.ErrDeliveryConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf("commit managed bootstrap reconciliation: %w", err)
	}
	return connector.DeliveryApplied, connector.DeliveryEvidence{ExternalID: externalID, ContentHash: manifestHash}, nil
}

// AbandonBootstrap removes only stage tables whose exact bootstrap identity and
// manifest are present in the destination evidence table.
func (d *Destination) AbandonBootstrap(ctx context.Context, intent connector.BootstrapIntent, tables []connector.BootstrapTable) error {
	if err := intent.Validate(); err != nil {
		return err
	}
	if d.pool == nil {
		return errors.New("postgres destination not initialized")
	}
	tx, err := d.beginWriteTransaction(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := ensureManagedBootstrapTables(ctx, tx); err != nil {
		return err
	}
	for _, table := range tables {
		schema := table.Schema
		target, _, stageName := d.bootstrapTables(intent, schema)
		targetSchema, _, stageTable := d.bootstrapTableCoordinates(intent, schema)
		var storedStage string
		err := tx.QueryRow(ctx, `
SELECT stage_table FROM wallaby.managed_bootstrap_tables
WHERE bootstrap_id=$1 AND manifest_hash=$2 AND target_table=$3`, intent.BootstrapID, intent.ManifestHash, target).Scan(&storedStage)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if storedStage != stageName {
			return fmt.Errorf("%w: bootstrap stage ownership mismatch for %s", connector.ErrDeliveryConflict, target)
		}
		if _, err := tx.Exec(ctx, `SELECT wallaby.drop_managed_bootstrap_stage($1,$2)`, targetSchema, stageTable); err != nil {
			return fmt.Errorf("drop abandoned bootstrap stage %s: %w", stageName, err)
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM wallaby.managed_bootstrap_tables WHERE bootstrap_id=$1 AND manifest_hash=$2`, intent.BootstrapID, intent.ManifestHash); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (d *Destination) seedBootstrapWatermarkState(ctx context.Context, tx pgx.Tx, intent connector.BootstrapIntent, targetSchema, targetTable string, table connector.BootstrapTable) error {
	if strings.TrimSpace(d.flowID) == "" {
		return errors.New("watermark bootstrap requires flow_id")
	}
	watermarkType, err := postgresWatermarkType(table.Schema, table.WritePolicy.WatermarkColumn)
	if err != nil {
		return err
	}
	if len(table.WritePolicy.KeyColumns) == 0 {
		return errors.New("watermark bootstrap requires mapped key columns")
	}
	if strings.TrimSpace(table.WritePolicy.ProjectionFingerprint) == "" {
		return errors.New("watermark bootstrap requires projection fingerprint")
	}
	position, err := connector.CanonicalizeCheckpointPosition(table.SourcePosition)
	if err != nil || !strings.Contains(position, "/") {
		return errors.New("watermark bootstrap requires canonical PostgreSQL source LSN")
	}
	keyTypes, err := postgresKeyTypes(table.Schema, table.WritePolicy.KeyColumns)
	if err != nil {
		return err
	}
	if err := ensureWatermarkStateTable(ctx, tx); err != nil {
		return err
	}
	keyParts := make([]string, len(table.WritePolicy.KeyColumns))
	for index, key := range table.WritePolicy.KeyColumns {
		keyParts[index] = quoteIdent(key, '"') + "::" + keyTypes[index] + "::text"
	}
	qualified := quoteIdent(targetTable, '"')
	if targetSchema != "" {
		qualified = quoteIdent(targetSchema, '"') + "." + qualified
	}
	watermark := quoteIdent(table.WritePolicy.WatermarkColumn, '"')
	scopeArgs := []any{d.flowID, targetSchema, targetTable, table.WritePolicy.ProjectionFingerprint, table.WritePolicy.KeyColumns}
	if _, err := tx.Exec(ctx, `DELETE FROM wallaby.watermark_state
WHERE flow_id=$1 AND target_schema=$2 AND target_table=$3 AND projection_fingerprint=$4 AND key_columns=$5`, scopeArgs...); err != nil {
		return fmt.Errorf("replace bootstrap watermark state scope for %s: %w", qualified, err)
	}
	statement := fmt.Sprintf(`INSERT INTO wallaby.watermark_state(
 flow_id,target_schema,target_table,projection_fingerprint,key_columns,key_values,watermark_type,watermark_value,source_position,content_hash,deleted)
SELECT $1,$2,$3,$4,$5,ARRAY[%s]::text[],$6,%s::%s::text,$7,$8,false FROM %s`, strings.Join(keyParts, ","), watermark, watermarkType, qualified)
	if _, err := tx.Exec(ctx, statement, d.flowID, targetSchema, targetTable, table.WritePolicy.ProjectionFingerprint, table.WritePolicy.KeyColumns, watermarkType, position, "bootstrap:"+intent.ManifestHash); err != nil {
		return fmt.Errorf("seed bootstrap watermark state for %s: %w", qualified, err)
	}
	return nil
}

func (d *Destination) rejectForeignKeyBootstrapTarget(ctx context.Context, schema, table string) error {
	var connected bool
	if err := d.pool.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM pg_catalog.pg_constraint constraint_row
  JOIN pg_catalog.pg_class source_table ON source_table.oid=constraint_row.conrelid
  JOIN pg_catalog.pg_namespace source_schema ON source_schema.oid=source_table.relnamespace
  JOIN pg_catalog.pg_class referenced_table ON referenced_table.oid=constraint_row.confrelid
  JOIN pg_catalog.pg_namespace referenced_schema ON referenced_schema.oid=referenced_table.relnamespace
  WHERE constraint_row.contype='f'
    AND ((source_schema.nspname=$1 AND source_table.relname=$2)
      OR (referenced_schema.nspname=$1 AND referenced_table.relname=$2))
)`, schema, table).Scan(&connected); err != nil {
		return fmt.Errorf("inspect destination foreign keys for %s.%s: %w", schema, table, err)
	}
	if connected {
		return fmt.Errorf("managed bootstrap does not support FK-connected destination target table %s.%s", schema, table)
	}
	return nil
}

func verifyManagedBootstrapStage(ctx context.Context, tx pgx.Tx, targetSchema, targetTable, stageSchema, stageTable string) error {
	var exact bool
	if err := tx.QueryRow(ctx, `WITH relation_signatures AS (
 SELECT n.nspname,c.relname,
  array_agg(a.attname||'|'||pg_catalog.format_type(a.atttypid,a.atttypmod)||'|'||a.attnotnull::text||'|'||a.attgenerated::text||'|'||a.attidentity::text||'|'||COALESCE(pg_catalog.pg_get_expr(d.adbin,d.adrelid),'') ORDER BY a.attnum) columns
 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
 LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
 WHERE (n.nspname=$1 AND c.relname=$2) OR (n.nspname=$3 AND c.relname=$4) GROUP BY n.nspname,c.relname
), index_signatures AS (
 SELECT n.nspname,c.relname,COALESCE(array_agg(
  i.indisunique::text||'|'||i.indnkeyatts::text||'|'||i.indclass::text||'|'||i.indcollation::text||'|'||i.indoption::text||'|'||
  COALESCE(pg_catalog.pg_get_expr(i.indpred,i.indrelid),'')||'|'||COALESCE(pg_catalog.pg_get_expr(i.indexprs,i.indrelid),'')||'|'||COALESCE(keys.names,'')
  ORDER BY i.indexrelid) FILTER (WHERE i.indexrelid IS NOT NULL),'{}'::text[]) indexes
 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_catalog.pg_index i ON i.indrelid=c.oid
 LEFT JOIN LATERAL (SELECT string_agg(a.attname,',' ORDER BY k.ord) names FROM unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,ord)
  LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum WHERE k.ord<=i.indnkeyatts) keys ON true
 WHERE (n.nspname=$1 AND c.relname=$2) OR (n.nspname=$3 AND c.relname=$4) GROUP BY n.nspname,c.relname
)
SELECT target.columns=stage.columns AND target_indexes.indexes=stage_indexes.indexes
FROM relation_signatures target JOIN relation_signatures stage ON true
JOIN index_signatures target_indexes ON target_indexes.nspname=target.nspname AND target_indexes.relname=target.relname
JOIN index_signatures stage_indexes ON stage_indexes.nspname=stage.nspname AND stage_indexes.relname=stage.relname
WHERE target.nspname=$1 AND target.relname=$2 AND stage.nspname=$3 AND stage.relname=$4`, targetSchema, targetTable, stageSchema, stageTable).Scan(&exact); err != nil {
		return fmt.Errorf("compare stage with target catalog: %w", err)
	}
	if !exact {
		return errors.New("stage columns/defaults/indexes differ from target")
	}
	return nil
}

func ensureManagedBootstrapTables(ctx context.Context, tx pgx.Tx) error {
	return validateManagedBootstrapObjects(ctx, tx, true)
}
func verifyManagedBootstrapTables(ctx context.Context, tx pgx.Tx) error {
	return validateManagedBootstrapObjects(ctx, tx, false)
}

func validateManagedBootstrapObjects(ctx context.Context, tx pgx.Tx, createMissing bool) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(pg_catalog.hashtextextended('wallaby.managed_bootstrap_contract',0))`); err != nil {
		return fmt.Errorf("lock managed bootstrap metadata contract: %w", err)
	}
	if createMissing {
		if _, err := tx.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS wallaby`); err != nil {
			return fmt.Errorf("create managed bootstrap schema: %w", err)
		}
	} else {
		var schemaExists bool
		if err := tx.QueryRow(ctx, `SELECT to_regnamespace('wallaby') IS NOT NULL`).Scan(&schemaExists); err != nil {
			return fmt.Errorf("inspect managed bootstrap schema: %w", err)
		}
		if !schemaExists {
			return errors.New("managed bootstrap schema is absent")
		}
	}
	for _, table := range []struct {
		name, create     string
		columns, primary []string
	}{
		{"managed_bootstrap_tables", `CREATE TABLE wallaby.managed_bootstrap_tables (
  bootstrap_id TEXT NOT NULL, manifest_hash TEXT NOT NULL, target_table TEXT NOT NULL, stage_table TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT wallaby_managed_bootstrap_tables_pkey PRIMARY KEY (bootstrap_id,target_table))`,
			[]string{"bootstrap_id|text|true|||", "manifest_hash|text|true|||", "target_table|text|true|||", "stage_table|text|true|||", "created_at|timestamp with time zone|true|||clock_timestamp()"}, []string{"bootstrap_id", "target_table"}},
		{"managed_bootstrap_publications", `CREATE TABLE wallaby.managed_bootstrap_publications (
  bootstrap_id TEXT NOT NULL, manifest_hash TEXT NOT NULL, external_id TEXT NOT NULL,
  published_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT wallaby_managed_bootstrap_publications_pkey PRIMARY KEY (bootstrap_id))`,
			[]string{"bootstrap_id|text|true|||", "manifest_hash|text|true|||", "external_id|text|true|||", "published_at|timestamp with time zone|true|||clock_timestamp()"}, []string{"bootstrap_id"}},
	} {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, "wallaby."+table.name).Scan(&exists); err != nil {
			return fmt.Errorf("inspect managed bootstrap table %s: %w", table.name, err)
		}
		if !exists {
			if !createMissing {
				return fmt.Errorf("managed bootstrap table %s is absent", table.name)
			}
			if _, err := tx.Exec(ctx, table.create); err != nil {
				return fmt.Errorf("create managed bootstrap table %s: %w", table.name, err)
			}
		}
		if err := verifyExactCatalogColumns(ctx, tx, "wallaby", table.name, table.columns); err != nil {
			return fmt.Errorf("managed bootstrap table %s contract: %w", table.name, err)
		}
		indexName := "wallaby_" + table.name + "_pkey"
		definition := "PRIMARY KEY (" + strings.Join(table.primary, ", ") + ")"
		if err := verifyExactConstraintsAndIndexes(ctx, tx, "wallaby", table.name, []string{indexName + "|p|false|false|true|" + definition}, []exactIndexContract{{name: indexName, primary: true, unique: true, columns: table.primary}}); err != nil {
			return fmt.Errorf("managed bootstrap table %s indexes/constraints: %w", table.name, err)
		}
	}
	functions := []struct {
		name, signature, types, create, body string
		args                                 []string
	}{
		{"prepare_managed_bootstrap_stage", "wallaby.prepare_managed_bootstrap_stage(text,text,text,text)", "text, text, text, text", prepareManagedBootstrapStageFunction, prepareManagedBootstrapStageBody, []string{"target_schema", "target_table", "stage_schema", "stage_table"}},
		{"publish_managed_bootstrap_table", "wallaby.publish_managed_bootstrap_table(text,text,text,text,text[])", "text, text, text, text, text[]", publishManagedBootstrapTableFunction, publishManagedBootstrapTableBody, []string{"target_schema", "target_table", "stage_schema", "stage_table", "columns"}},
		{"drop_managed_bootstrap_stage", "wallaby.drop_managed_bootstrap_stage(text,text)", "text, text", dropManagedBootstrapStageFunction, dropManagedBootstrapStageBody, []string{"stage_schema", "stage_table"}},
	}
	for _, function := range functions {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT to_regprocedure($1) IS NOT NULL`, function.signature).Scan(&exists); err != nil {
			return fmt.Errorf("inspect managed bootstrap function %s: %w", function.name, err)
		}
		if !exists {
			if !createMissing {
				return fmt.Errorf("managed bootstrap function %s is absent", function.name)
			}
			if _, err := tx.Exec(ctx, function.create); err != nil {
				return fmt.Errorf("create managed bootstrap function %s: %w", function.name, err)
			}
		}
		var exact bool
		if err := tx.QueryRow(ctx, `SELECT p.prorettype='void'::regtype AND l.lanname='plpgsql' AND p.prokind='f' AND p.provolatile='v'
 AND NOT p.prosecdef AND NOT p.proleakproof AND NOT p.proisstrict AND p.proparallel='u' AND p.proconfig IS NULL
 AND p.proargnames=$2::text[] AND pg_catalog.oidvectortypes(p.proargtypes)=$3 AND p.prosrc=$4
FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace JOIN pg_catalog.pg_language l ON l.oid=p.prolang
WHERE n.nspname='wallaby' AND p.proname=$1`, function.name, function.args, function.types, function.body).Scan(&exact); err != nil {
			return fmt.Errorf("verify managed bootstrap function %s: %w", function.name, err)
		}
		if !exact {
			return fmt.Errorf("managed bootstrap function %s contract mismatch", function.name)
		}
	}
	var functionCount int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
WHERE n.nspname='wallaby' AND p.proname=ANY($1::text[])`, []string{"prepare_managed_bootstrap_stage", "publish_managed_bootstrap_table", "drop_managed_bootstrap_stage"}).Scan(&functionCount); err != nil {
		return fmt.Errorf("count managed bootstrap functions: %w", err)
	}
	if functionCount != 3 {
		return fmt.Errorf("managed bootstrap function overload contract mismatch: found %d functions", functionCount)
	}
	return nil
}

const prepareManagedBootstrapStageBody = `DECLARE target_name TEXT; stage_name TEXT;
BEGIN
  target_name := CASE WHEN target_schema='' THEN format('%I',target_table) ELSE format('%I.%I',target_schema,target_table) END;
  stage_name := CASE WHEN stage_schema='' THEN format('%I',stage_table) ELSE format('%I.%I',stage_schema,stage_table) END;
  EXECUTE format('CREATE TABLE IF NOT EXISTS %s (LIKE %s INCLUDING ALL)',stage_name,target_name);
END`
const prepareManagedBootstrapStageFunction = `CREATE FUNCTION wallaby.prepare_managed_bootstrap_stage(
  target_schema TEXT,target_table TEXT,stage_schema TEXT,stage_table TEXT
) RETURNS void LANGUAGE plpgsql AS $function$` + prepareManagedBootstrapStageBody + `$function$`

const publishManagedBootstrapTableBody = `DECLARE target_name TEXT; stage_name TEXT; column_list TEXT;
BEGIN
  IF cardinality(columns)=0 THEN RAISE EXCEPTION 'managed bootstrap publication requires columns'; END IF;
  target_name := CASE WHEN target_schema='' THEN format('%I',target_table) ELSE format('%I.%I',target_schema,target_table) END;
  stage_name := CASE WHEN stage_schema='' THEN format('%I',stage_table) ELSE format('%I.%I',stage_schema,stage_table) END;
  SELECT string_agg(format('%I',column_name),',' ORDER BY ordinal)
    INTO column_list FROM unnest(columns) WITH ORDINALITY AS item(column_name,ordinal);
  EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE',target_name);
  EXECUTE format('TRUNCATE TABLE %s',target_name);
  EXECUTE format('INSERT INTO %s (%s) SELECT %s FROM %s',target_name,column_list,column_list,stage_name);
END`
const publishManagedBootstrapTableFunction = `CREATE FUNCTION wallaby.publish_managed_bootstrap_table(
  target_schema TEXT,target_table TEXT,stage_schema TEXT,stage_table TEXT,columns TEXT[]
) RETURNS void LANGUAGE plpgsql AS $function$` + publishManagedBootstrapTableBody + `$function$`

const dropManagedBootstrapStageBody = `DECLARE stage_name TEXT;
BEGIN
  stage_name := CASE WHEN stage_schema='' THEN format('%I',stage_table) ELSE format('%I.%I',stage_schema,stage_table) END;
  EXECUTE format('DROP TABLE IF EXISTS %s',stage_name);
END`
const dropManagedBootstrapStageFunction = `CREATE FUNCTION wallaby.drop_managed_bootstrap_stage(
  stage_schema TEXT,stage_table TEXT
) RETURNS void LANGUAGE plpgsql AS $function$` + dropManagedBootstrapStageBody + `$function$`

func (d *Destination) bootstrapTableCoordinates(intent connector.BootstrapIntent, schema connector.Schema) (targetSchema, targetTable, stageTable string) {
	record := connector.Record{Table: schema.Name}
	targetSchema, targetTable = d.targetParts(schema, record.Table)
	digest := sha256.Sum256([]byte(intent.BootstrapID + "\x00" + targetSchema + "\x00" + targetTable))
	suffix := "_wb_" + hex.EncodeToString(digest[:8])
	stageBase := targetTable
	maxBase := 63 - len(suffix)
	if len(stageBase) > maxBase {
		stageBase = stageBase[:maxBase]
	}
	return targetSchema, targetTable, stageBase + suffix
}

func (d *Destination) bootstrapTables(intent connector.BootstrapIntent, schema connector.Schema) (target, stage, stageName string) {
	record := connector.Record{Table: schema.Name}
	target = d.targetTable(schema, record)
	targetSchema, _, stageTable := d.bootstrapTableCoordinates(intent, schema)
	if targetSchema == "" {
		stage = quoteIdent(stageTable, '"')
		stageName = stageTable
	} else {
		stage = quoteIdent(targetSchema, '"') + "." + quoteIdent(stageTable, '"')
		stageName = targetSchema + "." + stageTable
	}
	return target, stage, stageName
}

func bootstrapPublicationMarker(intent connector.BootstrapIntent) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{intent.FlowIncarnationID, intent.BootstrapID, intent.DestinationRevisionID, intent.ManifestHash}, "\x00")))
	return "wallaby-bootstrap-" + hex.EncodeToString(digest[:])
}

var _ connector.ManagedBootstrapDestination = (*Destination)(nil)
