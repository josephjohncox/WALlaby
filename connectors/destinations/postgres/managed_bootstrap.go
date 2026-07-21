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
func (d *Destination) PrepareBootstrap(ctx context.Context, intent connector.BootstrapIntent, schemas []connector.Schema) error {
	if err := intent.Validate(); err != nil {
		return err
	}
	if d.pool == nil {
		return errors.New("postgres destination not initialized")
	}
	if len(schemas) == 0 {
		return errors.New("bootstrap manifest has no tables")
	}
	seenTargets := make(map[string]struct{}, len(schemas))
	for _, schema := range schemas {
		target, _, _ := d.bootstrapTables(intent, schema)
		if _, exists := seenTargets[target]; exists {
			return fmt.Errorf("bootstrap manifest maps multiple source tables to destination %s", target)
		}
		seenTargets[target] = struct{}{}
		targetSchema, targetTable, _ := d.bootstrapTableCoordinates(intent, schema)
		if err := d.rejectForeignKeyBootstrapTarget(ctx, targetSchema, targetTable); err != nil {
			return err
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
	for _, schema := range schemas {
		target, _, stageName := d.bootstrapTables(intent, schema)
		targetSchema, targetTable, stageTable := d.bootstrapTableCoordinates(intent, schema)
		if _, err := tx.Exec(ctx, `SELECT wallaby.prepare_managed_bootstrap_stage($1,$2,$3,$4)`, targetSchema, targetTable, targetSchema, stageTable); err != nil {
			return fmt.Errorf("create bootstrap stage for %s: %w", target, err)
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
	if err := d.applyBatch(ctx, tx, stage, batch.Schema, batch.Records, writeModeTarget); err != nil {
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
func (d *Destination) PublishBootstrap(ctx context.Context, intent connector.BootstrapIntent, schemas []connector.Schema) (connector.DeliveryEvidence, error) {
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
	sorted := append([]connector.Schema(nil), schemas...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Namespace+"."+sorted[i].Name < sorted[j].Namespace+"."+sorted[j].Name
	})
	if !alreadyPublished {
		for _, schema := range sorted {
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
	// Staging is generation-scoped mutable state. Remove it in the same
	// transaction as target publication while retaining the immutable marker.
	for _, schema := range sorted {
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
	var manifestHash, externalID string
	err := d.pool.QueryRow(ctx, `SELECT manifest_hash,external_id FROM wallaby.managed_bootstrap_publications WHERE bootstrap_id=$1`, intent.BootstrapID).Scan(&manifestHash, &externalID)
	if errors.Is(err, pgx.ErrNoRows) {
		return connector.DeliveryNotApplied, connector.DeliveryEvidence{}, nil
	}
	if err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if manifestHash != intent.ManifestHash || externalID != bootstrapPublicationMarker(intent) {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf("%w: bootstrap publication marker conflict", connector.ErrDeliveryConflict)
	}
	return connector.DeliveryApplied, connector.DeliveryEvidence{ExternalID: externalID, ContentHash: manifestHash}, nil
}

// AbandonBootstrap removes only stage tables whose exact bootstrap identity and
// manifest are present in the destination evidence table.
func (d *Destination) AbandonBootstrap(ctx context.Context, intent connector.BootstrapIntent, schemas []connector.Schema) error {
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
	for _, schema := range schemas {
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

func ensureManagedBootstrapTables(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `
CREATE SCHEMA IF NOT EXISTS wallaby;
CREATE TABLE IF NOT EXISTS wallaby.managed_bootstrap_tables (
  bootstrap_id TEXT NOT NULL,
  manifest_hash TEXT NOT NULL,
  target_table TEXT NOT NULL,
  stage_table TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (bootstrap_id,target_table)
);
CREATE TABLE IF NOT EXISTS wallaby.managed_bootstrap_publications (
  bootstrap_id TEXT PRIMARY KEY,
  manifest_hash TEXT NOT NULL,
  external_id TEXT NOT NULL,
  published_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE OR REPLACE FUNCTION wallaby.prepare_managed_bootstrap_stage(
  target_schema TEXT,target_table TEXT,stage_schema TEXT,stage_table TEXT
) RETURNS void LANGUAGE plpgsql AS $function$
DECLARE target_name TEXT; stage_name TEXT;
BEGIN
  target_name := CASE WHEN target_schema='' THEN format('%I',target_table) ELSE format('%I.%I',target_schema,target_table) END;
  stage_name := CASE WHEN stage_schema='' THEN format('%I',stage_table) ELSE format('%I.%I',stage_schema,stage_table) END;
  EXECUTE format('CREATE TABLE IF NOT EXISTS %s (LIKE %s INCLUDING ALL)',stage_name,target_name);
END
$function$;
CREATE OR REPLACE FUNCTION wallaby.publish_managed_bootstrap_table(
  target_schema TEXT,target_table TEXT,stage_schema TEXT,stage_table TEXT,columns TEXT[]
) RETURNS void LANGUAGE plpgsql AS $function$
DECLARE target_name TEXT; stage_name TEXT; column_list TEXT;
BEGIN
  IF cardinality(columns)=0 THEN RAISE EXCEPTION 'managed bootstrap publication requires columns'; END IF;
  target_name := CASE WHEN target_schema='' THEN format('%I',target_table) ELSE format('%I.%I',target_schema,target_table) END;
  stage_name := CASE WHEN stage_schema='' THEN format('%I',stage_table) ELSE format('%I.%I',stage_schema,stage_table) END;
  SELECT string_agg(format('%I',column_name),',' ORDER BY ordinal)
    INTO column_list FROM unnest(columns) WITH ORDINALITY AS item(column_name,ordinal);
  EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE',target_name);
  EXECUTE format('TRUNCATE TABLE %s',target_name);
  EXECUTE format('INSERT INTO %s (%s) SELECT %s FROM %s',target_name,column_list,column_list,stage_name);
END
$function$;
CREATE OR REPLACE FUNCTION wallaby.drop_managed_bootstrap_stage(
  stage_schema TEXT,stage_table TEXT
) RETURNS void LANGUAGE plpgsql AS $function$
DECLARE stage_name TEXT;
BEGIN
  stage_name := CASE WHEN stage_schema='' THEN format('%I',stage_table) ELSE format('%I.%I',stage_schema,stage_table) END;
  EXECUTE format('DROP TABLE IF EXISTS %s',stage_name);
END
$function$`)
	if err != nil {
		return fmt.Errorf("ensure managed bootstrap metadata: %w", err)
	}
	return nil
}

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
