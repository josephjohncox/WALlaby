package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	internalddl "github.com/josephjohncox/wallaby/internal/ddl"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// Apply writes target DML, metadata, and a deterministic destination receipt in
// one PostgreSQL transaction. A commit transport error remains indeterminate
// until Reconcile proves the marker.
func (d *Destination) Apply(ctx context.Context, intent connector.DeliveryIntent, batch connector.Batch) (connector.DeliveryEvidence, error) {
	if err := intent.Validate(); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	if d.pool == nil {
		return connector.DeliveryEvidence{}, errors.New("postgres destination not initialized")
	}
	if err := d.validateManagedProfile(ctx, batch); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	contentHash, err := connector.BatchContentHash(batch)
	if err != nil {
		return connector.DeliveryEvidence{}, fmt.Errorf("hash delivery batch: %w", err)
	}
	if contentHash != intent.ContentHash {
		return connector.DeliveryEvidence{}, fmt.Errorf(
			"%w: intent hash %s does not match batch hash %s",
			connector.ErrDeliveryConflict,
			intent.ContentHash,
			contentHash,
		)
	}

	evidence := connector.DeliveryEvidence{
		ExternalID:  postgresDeliveryMarkerID(intent),
		ContentHash: intent.ContentHash,
	}
	tx, err := d.beginWriteTransaction(ctx)
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	rollback := func() {
		_ = tx.Rollback(ctx)
	}

	existingHash, err := d.loadManagedReceipt(ctx, tx, intent)
	switch {
	case err == nil:
		rollback()
		if existingHash != intent.ContentHash {
			return connector.DeliveryEvidence{}, fmt.Errorf(
				"%w: delivery %s contains hash %s, expected %s",
				connector.ErrDeliveryConflict,
				evidence.ExternalID,
				existingHash,
				intent.ContentHash,
			)
		}
		return evidence, nil
	case !errors.Is(err, pgx.ErrNoRows):
		rollback()
		return connector.DeliveryEvidence{}, fmt.Errorf("load postgres delivery receipt: %w", err)
	}

	if err := d.applyTransaction(ctx, tx, batch); err != nil {
		rollback()
		return connector.DeliveryEvidence{}, err
	}
	if err := d.insertManagedReceipt(ctx, tx, intent, evidence.ExternalID); err != nil {
		rollback()
		return connector.DeliveryEvidence{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return evidence, fmt.Errorf("%w: postgres commit for delivery %s: %w", connector.ErrDeliveryIndeterminate, evidence.ExternalID, err)
	}
	return evidence, nil
}

// ValidateTransaction proves that a full source transaction can be applied by
// the managed PostgreSQL profile before the control-plane attempt is prepared.
// DDL plans are barriers: schemas after a plan are checked inside the target
// transaction after the plan executes.
func (d *Destination) ValidateTransaction(ctx context.Context, transaction connector.SourceTransaction) error {
	if d.pool == nil {
		return errors.New("postgres destination not initialized")
	}
	if err := transaction.Validate(); err != nil {
		return err
	}
	dirty := make(map[string]struct{})
	for _, fragment := range transaction.Fragments {
		batch := fragment.Batch
		batch.Checkpoint = transaction.Checkpoint
		if err := d.validateManagedProfile(ctx, batch); err != nil {
			return err
		}
		key := tableKey(batch.Schema, batch.Schema.Name)
		if batchHasStructuredDDL(batch) {
			dirty[key] = struct{}{}
			continue
		}
		if _, changedInTransaction := dirty[key]; changedInTransaction {
			continue
		}
		if err := d.validateManagedTargetSchema(ctx, d.pool, batch.Schema); err != nil {
			return err
		}
	}
	return nil
}

// ApplyTransaction commits all ordered table/schema fragments, DDL barriers,
// metadata, and the deterministic Wallaby marker in one target transaction.
func (d *Destination) ApplyTransaction(ctx context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (connector.DeliveryEvidence, error) {
	if err := intent.Validate(); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	if strings.TrimSpace(intent.LogicalBatchID) == "" {
		return connector.DeliveryEvidence{}, errors.New("managed PostgreSQL transaction delivery requires logical_batch_id")
	}
	if err := d.ValidateTransaction(ctx, transaction); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		return connector.DeliveryEvidence{}, fmt.Errorf("identify source transaction: %w", err)
	}
	if contentHash != intent.ContentHash || logicalBatchID != intent.LogicalBatchID {
		return connector.DeliveryEvidence{}, fmt.Errorf("%w: managed transaction identity differs from delivery intent", connector.ErrDeliveryConflict)
	}

	evidence := connector.DeliveryEvidence{ExternalID: postgresDeliveryMarkerID(intent), ContentHash: intent.ContentHash}
	tx, err := d.beginWriteTransaction(ctx)
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	existingHash, err := d.loadManagedReceipt(ctx, tx, intent)
	switch {
	case err == nil:
		if existingHash != intent.ContentHash {
			return connector.DeliveryEvidence{}, fmt.Errorf("%w: delivery %s contains hash %s, expected %s", connector.ErrDeliveryConflict, evidence.ExternalID, existingHash, intent.ContentHash)
		}
		return evidence, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return connector.DeliveryEvidence{}, fmt.Errorf("load postgres transaction receipt: %w", err)
	}

	for _, fragment := range transaction.Fragments {
		if err := d.applyManagedFragment(ctx, tx, fragment.Batch, transaction.Checkpoint); err != nil {
			return connector.DeliveryEvidence{}, fmt.Errorf("apply source transaction fragment %d: %w", fragment.Ordinal, err)
		}
	}
	if err := d.insertManagedReceipt(ctx, tx, intent, evidence.ExternalID); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return evidence, fmt.Errorf("%w: postgres commit for logical batch %s: %w", connector.ErrDeliveryIndeterminate, intent.LogicalBatchID, err)
	}
	return evidence, nil
}

func (d *Destination) applyManagedFragment(ctx context.Context, tx pgx.Tx, batch connector.Batch, checkpoint connector.Checkpoint) error {
	var pending []connector.Record
	pendingTarget := ""
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := d.validateManagedTargetSchema(ctx, tx, batch.Schema); err != nil {
			return err
		}
		if err := d.applyBatch(ctx, tx, pendingTarget, batch.Schema, pending, writeModeTarget); err != nil {
			return err
		}
		if d.metaEnabled {
			if err := d.upsertMetadataBatch(ctx, tx, batch.Schema, pending, checkpoint); err != nil {
				return err
			}
		}
		pending = nil
		pendingTarget = ""
		return nil
	}

	for _, record := range batch.Records {
		if record.Operation == connector.OpDDL {
			if err := flush(); err != nil {
				return err
			}
			if strings.TrimSpace(record.DDL) != "" || len(record.DDLPlan) == 0 {
				return errors.New("managed PostgreSQL profile requires structured DDL plans")
			}
			mappedSchema, mappedRecord, err := d.mapManagedDDLTarget(batch.Schema, record)
			if err != nil {
				return err
			}
			statements, err := internalddl.TranslateRecordDDL(mappedSchema, mappedRecord, internalddl.DialectConfigFor(internalddl.DialectPostgres), d.TypeMappings(), d.spec.Options)
			if err != nil {
				return fmt.Errorf("translate managed DDL plan: %w", err)
			}
			for _, statement := range statements {
				if strings.TrimSpace(statement) == "" {
					continue
				}
				if _, err := tx.Exec(ctx, statement); err != nil {
					return fmt.Errorf("apply managed DDL: %w", err)
				}
			}
			continue
		}
		target, isStaging := d.resolveTarget(batch.Schema, record)
		if isStaging {
			return errors.New("managed PostgreSQL profile cannot apply a staging fragment")
		}
		if pendingTarget != "" && pendingTarget != target {
			if err := flush(); err != nil {
				return err
			}
		}
		pendingTarget = target
		pending = append(pending, record)
	}
	return flush()
}

func (d *Destination) mapManagedDDLTarget(schema connector.Schema, record connector.Record) (connector.Schema, connector.Record, error) {
	table := strings.TrimSpace(record.Table)
	if table == "" {
		table = schema.Name
	}
	targetSchema, targetTable := d.targetParts(schema, table)
	if targetSchema == "" {
		targetSchema = "public"
	}
	mappedSchema := schema
	mappedSchema.Namespace = targetSchema
	mappedSchema.Name = targetTable
	mappedRecord := record
	mappedRecord.Table = targetTable
	var plan internalschema.Plan
	if err := json.Unmarshal(record.DDLPlan, &plan); err != nil {
		return connector.Schema{}, connector.Record{}, fmt.Errorf("unmarshal managed DDL plan: %w", err)
	}
	for index := range plan.Changes {
		plan.Changes[index].Namespace = targetSchema
		plan.Changes[index].Table = targetTable
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		return connector.Schema{}, connector.Record{}, fmt.Errorf("marshal mapped managed DDL plan: %w", err)
	}
	mappedRecord.DDLPlan = encoded
	return mappedSchema, mappedRecord, nil
}

func batchHasStructuredDDL(batch connector.Batch) bool {
	for _, record := range batch.Records {
		if record.Operation == connector.OpDDL && strings.TrimSpace(record.DDL) == "" && len(record.DDLPlan) > 0 {
			return true
		}
	}
	return false
}

type managedSchemaQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type managedTargetColumn struct {
	typeName   string
	nullable   bool
	generated  bool
	hasDefault bool
}

func (d *Destination) validateManagedTargetSchema(ctx context.Context, query managedSchemaQuerier, schema connector.Schema) error {
	targetSchema, targetTable := d.targetParts(schema, schema.Name)
	if targetSchema == "" {
		targetSchema = "public"
	}
	rows, err := query.Query(ctx, `
SELECT attribute.attname,
       pg_catalog.format_type(attribute.atttypid,attribute.atttypmod),
       NOT attribute.attnotnull,
       attribute.attgenerated <> '',
       default_value.adbin IS NOT NULL
FROM pg_catalog.pg_class AS relation
JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace
JOIN pg_catalog.pg_attribute AS attribute ON attribute.attrelid=relation.oid
LEFT JOIN pg_catalog.pg_attrdef AS default_value
  ON default_value.adrelid=attribute.attrelid AND default_value.adnum=attribute.attnum
WHERE namespace.nspname=$1 AND relation.relname=$2
  AND attribute.attnum>0 AND NOT attribute.attisdropped
ORDER BY attribute.attnum`, targetSchema, targetTable)
	if err != nil {
		return fmt.Errorf("inspect managed target %s.%s: %w", targetSchema, targetTable, err)
	}
	defer rows.Close()
	targetColumns := make(map[string]managedTargetColumn)
	for rows.Next() {
		var name string
		var column managedTargetColumn
		if err := rows.Scan(&name, &column.typeName, &column.nullable, &column.generated, &column.hasDefault); err != nil {
			return fmt.Errorf("scan managed target column: %w", err)
		}
		targetColumns[name] = column
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("inspect managed target columns: %w", err)
	}
	if len(targetColumns) == 0 {
		return fmt.Errorf("managed target table %s.%s does not exist", targetSchema, targetTable)
	}

	sourceColumns := make(map[string]struct{}, len(schema.Columns))
	keyColumns := make([]string, 0)
	for _, source := range schema.Columns {
		sourceColumns[source.Name] = struct{}{}
		target, ok := targetColumns[source.Name]
		if !ok {
			return fmt.Errorf("managed target %s.%s is missing source column %q", targetSchema, targetTable, source.Name)
		}
		if normalizePostgresDDLType(source.Type) != normalizePostgresDDLType(target.typeName) {
			return fmt.Errorf("managed target %s.%s column %q type %q is incompatible with source %q", targetSchema, targetTable, source.Name, target.typeName, source.Type)
		}
		if source.TypeMetadata["nullability_known"] == "true" && source.Nullable && !target.nullable {
			return fmt.Errorf("managed target %s.%s column %q rejects source NULL values", targetSchema, targetTable, source.Name)
		}
		if source.TypeMetadata["generated_known"] == "true" && source.Generated != target.generated {
			return fmt.Errorf("managed target %s.%s column %q generated status differs from source", targetSchema, targetTable, source.Name)
		}
		if source.TypeMetadata["primary_key"] == "true" || source.TypeMetadata["replica_identity"] == "true" {
			keyColumns = append(keyColumns, source.Name)
		}
	}
	for name, target := range targetColumns {
		if _, ok := sourceColumns[name]; ok {
			continue
		}
		if !target.nullable && !target.generated && !target.hasDefault {
			return fmt.Errorf("managed target %s.%s has required unmapped column %q", targetSchema, targetTable, name)
		}
	}
	if len(keyColumns) == 0 {
		return fmt.Errorf("managed source schema %s.%s has no primary/replica identity columns", schema.Namespace, schema.Name)
	}
	var unique bool
	if err := query.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM pg_catalog.pg_index AS index_row
  JOIN pg_catalog.pg_class AS relation ON relation.oid=index_row.indrelid
  JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace
  WHERE namespace.nspname=$1 AND relation.relname=$2
    AND index_row.indisunique AND index_row.indisvalid AND index_row.indimmediate AND index_row.indpred IS NULL
    AND ARRAY(
      SELECT attribute.attname::text
      FROM unnest(index_row.indkey::smallint[]) WITH ORDINALITY AS key(attnum,ordinality)
      JOIN pg_catalog.pg_attribute AS attribute
        ON attribute.attrelid=index_row.indrelid AND attribute.attnum=key.attnum
      WHERE key.ordinality<=index_row.indnkeyatts
      ORDER BY key.ordinality
    )=$3::text[]
)`, targetSchema, targetTable, keyColumns).Scan(&unique); err != nil {
		return fmt.Errorf("inspect managed target uniqueness: %w", err)
	}
	if !unique {
		return fmt.Errorf("managed target %s.%s requires a valid non-partial unique constraint on source identity columns %v", targetSchema, targetTable, keyColumns)
	}
	return nil
}

// Reconcile proves an ambiguous managed delivery from the deterministic marker
// committed atomically with target DML.
func (d *Destination) Reconcile(ctx context.Context, intent connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	if err := intent.Validate(); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if d.pool == nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, errors.New("postgres destination not initialized")
	}
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf("begin postgres delivery reconciliation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	contentHash, err := d.loadManagedReceipt(ctx, tx, intent)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return connector.DeliveryNotApplied, connector.DeliveryEvidence{}, nil
		}
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf("reconcile postgres delivery: %w", err)
	}
	if contentHash != intent.ContentHash {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf(
			"%w: reconciled hash %s, expected %s",
			connector.ErrDeliveryConflict,
			contentHash,
			intent.ContentHash,
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf("commit postgres delivery reconciliation: %w", err)
	}
	return connector.DeliveryApplied, connector.DeliveryEvidence{
		ExternalID:  postgresDeliveryMarkerID(intent),
		ContentHash: contentHash,
	}, nil
}

func (d *Destination) validateManagedProfile(ctx context.Context, batch connector.Batch) error {
	if d.writeMode != "" && d.writeMode != writeModeTarget {
		return errors.New("managed postgres delivery requires target write mode")
	}
	if d.batchMode != "" && d.batchMode != batchModeTarget {
		return errors.New("managed postgres delivery does not admit staging mode")
	}
	if batchContainsRawDDL(batch) {
		return errors.New("managed postgres delivery requires separately reconciled structured DDL")
	}

	syncCommit := d.syncCommit
	if syncCommit == "" {
		if err := d.pool.QueryRow(ctx, "SHOW synchronous_commit").Scan(&syncCommit); err != nil {
			return fmt.Errorf("read synchronous_commit: %w", err)
		}
		syncCommit = normalizeSyncCommit(syncCommit)
	}
	switch syncCommit {
	case "on", "remote_apply":
		return nil
	default:
		return fmt.Errorf("managed postgres delivery requires durable synchronous_commit=on or remote_apply; got %q", syncCommit)
	}
}

func batchContainsRawDDL(batch connector.Batch) bool {
	for _, record := range batch.Records {
		if record.Operation == connector.OpDDL && strings.TrimSpace(record.DDL) != "" {
			return true
		}
	}
	return false
}

func (d *Destination) ensureManagedReceiptTable(ctx context.Context) error {
	if _, err := d.pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS wallaby_meta"); err != nil {
		return fmt.Errorf("create managed receipt schema: %w", err)
	}
	const query = `CREATE TABLE IF NOT EXISTS wallaby_meta.__delivery_receipts (
  marker_id TEXT NOT NULL,
  flow_id TEXT NOT NULL,
  flow_incarnation_id TEXT NOT NULL,
  generation BIGINT NOT NULL,
  acquisition_id TEXT NOT NULL,
  lease_epoch BIGINT NOT NULL,
  destination_revision_id TEXT NOT NULL,
  source_lineage_id TEXT NOT NULL,
  logical_batch_id TEXT,
  position_id TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (flow_incarnation_id, destination_revision_id, position_id),
  UNIQUE (marker_id)
)`
	if _, err := d.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("create managed receipt table: %w", err)
	}
	if _, err := d.pool.Exec(ctx, `ALTER TABLE wallaby_meta.__delivery_receipts ADD COLUMN IF NOT EXISTS source_lineage_id TEXT NOT NULL DEFAULT 'legacy-unqualified'`); err != nil {
		return fmt.Errorf("upgrade managed receipt lineage: %w", err)
	}
	if _, err := d.pool.Exec(ctx, `
ALTER TABLE wallaby_meta.__delivery_receipts ADD COLUMN IF NOT EXISTS logical_batch_id TEXT;
ALTER TABLE wallaby_meta.__delivery_receipts ALTER COLUMN logical_batch_id DROP NOT NULL;
DROP INDEX IF EXISTS wallaby_meta.wallaby_delivery_receipts_logical_batch_idx;
CREATE UNIQUE INDEX wallaby_delivery_receipts_logical_batch_idx
  ON wallaby_meta.__delivery_receipts (flow_incarnation_id,destination_revision_id,logical_batch_id)
  WHERE logical_batch_id IS NOT NULL`); err != nil {
		return fmt.Errorf("upgrade managed receipt logical batch identity: %w", err)
	}
	return nil
}

func (d *Destination) loadManagedReceipt(ctx context.Context, tx pgx.Tx, intent connector.DeliveryIntent) (string, error) {
	row := tx.QueryRow(ctx, `
SELECT content_hash,logical_batch_id
FROM wallaby_meta.__delivery_receipts
WHERE flow_incarnation_id = $1
  AND destination_revision_id = $2
  AND source_lineage_id = $3
  AND position_id = $4
FOR UPDATE`, intent.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, intent.PositionID)
	var contentHash string
	var logicalBatchID pgtype.Text
	if err := row.Scan(&contentHash, &logicalBatchID); err != nil {
		return "", err
	}
	expected := deliveryLogicalBatchID(intent)
	if !logicalBatchID.Valid || logicalBatchID.String == "legacy:"+intent.PositionID {
		if contentHash != intent.ContentHash {
			return "", fmt.Errorf("%w: legacy target receipt content differs", connector.ErrDeliveryConflict)
		}
		if _, err := tx.Exec(ctx, `
UPDATE wallaby_meta.__delivery_receipts
SET logical_batch_id=$5
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND source_lineage_id=$3 AND position_id=$4`, intent.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, intent.PositionID, expected); err != nil {
			return "", fmt.Errorf("upgrade legacy target logical batch: %w", err)
		}
		return contentHash, nil
	}
	if logicalBatchID.String != expected {
		return "", fmt.Errorf("%w: target logical batch %s differs from %s", connector.ErrDeliveryConflict, logicalBatchID.String, expected)
	}
	return contentHash, nil
}

func (d *Destination) insertManagedReceipt(ctx context.Context, tx pgx.Tx, intent connector.DeliveryIntent, markerID string) error {
	_, err := tx.Exec(ctx, `
INSERT INTO wallaby_meta.__delivery_receipts (
  marker_id, flow_id, flow_incarnation_id, generation, acquisition_id,
  lease_epoch, destination_revision_id, source_lineage_id, logical_batch_id, position_id, content_hash
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		markerID,
		intent.FlowID,
		intent.FlowIncarnationID,
		intent.Generation,
		intent.AcquisitionID,
		intent.LeaseEpoch,
		intent.DestinationRevisionID,
		intent.SourceLineageID,
		deliveryLogicalBatchID(intent),
		intent.PositionID,
		intent.ContentHash,
	)
	if err != nil {
		var postgresErr *pgconn.PgError
		if errors.As(err, &postgresErr) && postgresErr.Code == "23505" {
			return fmt.Errorf("%w: concurrent postgres delivery receipt requires reconciliation: %w", connector.ErrDeliveryIndeterminate, err)
		}
		return fmt.Errorf("insert postgres delivery receipt: %w", err)
	}
	if _, err := tx.Exec(ctx, `
DELETE FROM wallaby_meta.__delivery_receipts
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2 AND marker_id<>$3`, intent.FlowIncarnationID, intent.DestinationRevisionID, markerID); err != nil {
		return fmt.Errorf("prune superseded postgres delivery receipts: %w", err)
	}
	return nil
}

func postgresDeliveryMarkerID(intent connector.DeliveryIntent) string {
	hash := sha256.Sum256([]byte(strings.Join([]string{
		intent.FlowIncarnationID,
		intent.SourceLineageID,
		intent.DestinationRevisionID,
		deliveryLogicalBatchID(intent),
		intent.PositionID,
		intent.ContentHash,
	}, "\x00")))
	return "wallaby-pg-" + hex.EncodeToString(hash[:])
}

func deliveryLogicalBatchID(intent connector.DeliveryIntent) string {
	if value := strings.TrimSpace(intent.LogicalBatchID); value != "" {
		return value
	}
	return "legacy:" + intent.PositionID
}

var _ connector.ManagedDestination = (*Destination)(nil)
