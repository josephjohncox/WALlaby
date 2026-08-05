package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	internalddl "github.com/josephjohncox/wallaby/internal/ddl"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

var _ connector.ManagedTransactionDestination = (*Destination)(nil)

// InitializeManagedDelivery establishes and exactly verifies the immutable
// destination receipt authority before any managed bootstrap or CDC I/O.
func (d *Destination) InitializeManagedDelivery(ctx context.Context) error {
	if d.pool == nil {
		return errors.New("postgres destination not initialized")
	}
	if d.batchMode != batchModeTarget {
		return errors.New("managed PostgreSQL delivery requires batch_mode=target")
	}
	return d.ensureManagedReceiptTable(ctx)
}

// Apply writes target DML, metadata, and a deterministic destination receipt in
// one PostgreSQL transaction. A commit transport error remains indeterminate
// until Reconcile proves the marker.
func (d *Destination) Apply(ctx context.Context, intent connector.DeliveryIntent, batch connector.Batch) (connector.DeliveryEvidence, error) {
	if err := intent.Validate(); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	if strings.TrimSpace(intent.LogicalBatchID) == "" {
		return connector.DeliveryEvidence{}, errors.New("managed PostgreSQL delivery requires logical_batch_id")
	}
	if d.pool == nil {
		return connector.DeliveryEvidence{}, errors.New("postgres destination not initialized")
	}
	if err := d.validateManagedProfile(ctx, batch); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	if !batchHasStructuredDDL(batch) {
		if err := d.validateManagedTargetSchema(ctx, d.pool, batch.Schema, batch.WritePolicy); err != nil {
			return connector.DeliveryEvidence{}, err
		}
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
			keys, err := structuredDDLTableKeys(batch)
			if err != nil {
				return err
			}
			for _, changedKey := range keys {
				dirty[changedKey] = struct{}{}
			}
			if len(keys) == 0 {
				dirty[key] = struct{}{}
			}
			continue
		}
		if _, changedInTransaction := dirty[key]; changedInTransaction {
			continue
		}
		if err := d.validateManagedTargetSchema(ctx, d.pool, batch.Schema, batch.WritePolicy); err != nil {
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
	var pendingTarget postgresTarget
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := d.validateManagedTargetSchema(ctx, tx, batch.Schema, batch.WritePolicy); err != nil {
			return err
		}
		mode := writeModeTarget
		if batch.WritePolicy.Mode == connector.ResolvedWriteAppend {
			mode = writeModeAppend
		}
		if batch.WritePolicy.Mode != connector.ResolvedWriteAppend && batch.WritePolicy.Mode != connector.ResolvedWriteUpsert {
			return fmt.Errorf("managed PostgreSQL fragment requires mapped append/upsert policy, got %q", batch.WritePolicy.Mode)
		}
		if err := d.applyBatch(ctx, tx, pendingTarget, batch.Schema, pending, mode, batch.WritePolicy); err != nil {
			return err
		}
		if d.metaEnabled {
			if err := d.upsertMetadataBatch(ctx, tx, batch.Schema, pending, checkpoint); err != nil {
				return err
			}
		}
		pending = nil
		pendingTarget = postgresTarget{}
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
			statements, err := internalddl.TranslateRecordDDL(batch.Schema, record, internalddl.DialectConfigFor(internalddl.DialectPostgres), d.TypeMappings(), d.spec.Options)
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
		target, isStaging, err := d.resolveTarget(batch.Schema, record)
		if err != nil {
			return err
		}
		if isStaging {
			return errors.New("managed PostgreSQL profile cannot apply a staging fragment")
		}
		if len(pendingTarget.identifier) != 0 && strings.Join(pendingTarget.identifier, "\x00") != strings.Join(target.identifier, "\x00") {
			if err := flush(); err != nil {
				return err
			}
		}
		pendingTarget = target
		pending = append(pending, record)
	}
	return flush()
}

func structuredDDLTableKeys(batch connector.Batch) ([]string, error) {
	seen := make(map[string]struct{})
	for _, record := range batch.Records {
		if record.Operation != connector.OpDDL || len(record.DDLPlan) == 0 {
			continue
		}
		var plan internalschema.Plan
		if err := json.Unmarshal(record.DDLPlan, &plan); err != nil {
			return nil, fmt.Errorf("decode managed DDL plan for target validation: %w", err)
		}
		for _, change := range plan.Changes {
			if change.Table == "" {
				continue
			}
			seen[tableKey(connector.Schema{Namespace: change.Namespace, Name: change.Table}, change.Table)] = struct{}{}
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
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

func (d *Destination) validateManagedTargetSchema(ctx context.Context, query managedSchemaQuerier, schema connector.Schema, policy connector.TableWritePolicy) error {
	if err := validateProjectedOldImagePolicy(schema, policy); err != nil {
		return err
	}
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
	}
	for name, target := range targetColumns {
		if _, ok := sourceColumns[name]; ok {
			continue
		}
		if !target.nullable && !target.generated && !target.hasDefault {
			return fmt.Errorf("managed target %s.%s has required unmapped column %q", targetSchema, targetTable, name)
		}
	}
	if policy.Mode == connector.ResolvedWriteAppend {
		var anyUnique bool
		if err := query.QueryRow(ctx, `SELECT EXISTS (
 SELECT 1 FROM pg_catalog.pg_index i
 JOIN pg_catalog.pg_class c ON c.oid=i.indrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname=$1 AND c.relname=$2 AND i.indisunique)`, targetSchema, targetTable).Scan(&anyUnique); err != nil {
			return fmt.Errorf("inspect managed append target uniqueness: %w", err)
		}
		if anyUnique {
			return fmt.Errorf("managed append target %s.%s cannot contain any unique or primary-key index", targetSchema, targetTable)
		}
		return nil
	}
	if policy.Mode != connector.ResolvedWriteUpsert {
		return fmt.Errorf("managed target %s.%s requires mapped append/upsert policy, got %q", targetSchema, targetTable, policy.Mode)
	}
	if len(policy.KeyColumns) == 0 {
		return fmt.Errorf("managed upsert target %s.%s requires projected policy key columns", targetSchema, targetTable)
	}
	seenKeys := make(map[string]struct{}, len(policy.KeyColumns))
	for _, key := range policy.KeyColumns {
		if _, duplicate := seenKeys[key]; duplicate {
			return fmt.Errorf("managed upsert target policy repeats key column %q", key)
		}
		seenKeys[key] = struct{}{}
		if _, exists := sourceColumns[key]; !exists {
			return fmt.Errorf("managed upsert target policy key column %q is absent from projected schema", key)
		}
	}
	var unique bool
	if err := query.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM pg_catalog.pg_constraint AS constraint_row
  JOIN pg_catalog.pg_index AS index_row ON index_row.indexrelid=constraint_row.conindid
  JOIN pg_catalog.pg_class AS relation ON relation.oid=constraint_row.conrelid
  JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace
  WHERE namespace.nspname=$1 AND relation.relname=$2 AND constraint_row.contype IN ('p','u')
    AND index_row.indisunique AND index_row.indisvalid AND index_row.indimmediate AND index_row.indpred IS NULL
    AND ARRAY(
      SELECT attribute.attname::text
      FROM unnest(index_row.indkey::smallint[]) WITH ORDINALITY AS key(attnum,ordinality)
      JOIN pg_catalog.pg_attribute AS attribute
        ON attribute.attrelid=index_row.indrelid AND attribute.attnum=key.attnum
      WHERE key.ordinality<=index_row.indnkeyatts
      ORDER BY key.ordinality
    )=$3::text[]
)`, targetSchema, targetTable, policy.KeyColumns).Scan(&unique); err != nil {
		return fmt.Errorf("inspect managed target uniqueness: %w", err)
	}
	if !unique {
		return fmt.Errorf("managed target %s.%s requires a valid immediate unique/primary-key constraint on projected policy key columns in order %v", targetSchema, targetTable, policy.KeyColumns)
	}
	return nil
}

func validateProjectedOldImagePolicy(schema connector.Schema, policy connector.TableWritePolicy) error {
	if policy.Mode != connector.ResolvedWriteUpsert {
		return nil
	}
	byName := make(map[string]connector.Column, len(schema.Columns))
	for _, column := range schema.Columns {
		byName[column.Name] = column
	}
	for _, key := range policy.KeyColumns {
		column, ok := byName[key]
		if !ok {
			return fmt.Errorf("projected upsert key column %q is absent from schema", key)
		}
		if column.TypeMetadata["replica_identity"] != "true" {
			return fmt.Errorf("projected upsert key column %q lacks PostgreSQL replica-identity/full old-image availability", key)
		}
	}
	if policy.WatermarkColumn != "" {
		column, ok := byName[policy.WatermarkColumn]
		if !ok {
			return fmt.Errorf("projected watermark column %q is absent from schema", policy.WatermarkColumn)
		}
		if column.Nullable {
			return fmt.Errorf("projected watermark column %q must be non-nullable", policy.WatermarkColumn)
		}
		if column.TypeMetadata["replica_identity"] != "true" {
			return fmt.Errorf("projected watermark column %q lacks PostgreSQL replica-identity/full old-image availability", policy.WatermarkColumn)
		}
	}
	return nil
}

// Reconcile proves an ambiguous managed delivery from the deterministic marker
// committed atomically with target DML.
func (d *Destination) Reconcile(ctx context.Context, intent connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	if err := intent.Validate(); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if strings.TrimSpace(intent.LogicalBatchID) == "" {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, errors.New("managed PostgreSQL delivery requires logical_batch_id")
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
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin managed receipt contract transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(pg_catalog.hashtextextended('wallaby_meta.__delivery_receipts',0))`); err != nil {
		return fmt.Errorf("lock managed receipt contract: %w", err)
	}
	if _, err := tx.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS wallaby_meta"); err != nil {
		return fmt.Errorf("create managed receipt schema: %w", err)
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass('"wallaby_meta"."__delivery_receipts"') IS NOT NULL`).Scan(&exists); err != nil {
		return fmt.Errorf("inspect managed receipt table existence: %w", err)
	}
	if !exists {
		if _, err := tx.Exec(ctx, `CREATE TABLE wallaby_meta.__delivery_receipts (
  marker_id TEXT NOT NULL,
  flow_id TEXT NOT NULL,
  flow_incarnation_id TEXT NOT NULL,
  generation BIGINT NOT NULL,
  acquisition_id TEXT NOT NULL,
  lease_epoch BIGINT NOT NULL,
  destination_revision_id TEXT NOT NULL,
  source_lineage_id TEXT NOT NULL,
  logical_batch_id TEXT NOT NULL,
  position_id TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT wallaby_delivery_receipts_pkey PRIMARY KEY (flow_incarnation_id,destination_revision_id,position_id),
  CONSTRAINT wallaby_delivery_receipts_marker_unique UNIQUE (marker_id),
  CONSTRAINT wallaby_delivery_receipts_logical_batch_unique UNIQUE (flow_incarnation_id,destination_revision_id,logical_batch_id),
  CONSTRAINT wallaby_delivery_receipts_logical_batch_current CHECK (
    logical_batch_id='logical-batch:'||pg_catalog.encode(
      pg_catalog.sha256(pg_catalog.convert_to(source_lineage_id,'UTF8')||pg_catalog.decode('00','hex')||pg_catalog.convert_to(position_id,'UTF8')||pg_catalog.decode('00','hex')||pg_catalog.convert_to(content_hash,'UTF8')),
      'hex'
    )
  )
)`); err != nil {
			return fmt.Errorf("create managed receipt table: %w", err)
		}
	}
	if err := verifyExactCatalogColumns(ctx, tx, "wallaby_meta", "__delivery_receipts", []string{
		"marker_id|text|true|||", "flow_id|text|true|||", "flow_incarnation_id|text|true|||", "generation|bigint|true|||",
		"acquisition_id|text|true|||", "lease_epoch|bigint|true|||", "destination_revision_id|text|true|||", "source_lineage_id|text|true|||",
		"logical_batch_id|text|true|||", "position_id|text|true|||", "content_hash|text|true|||", "committed_at|timestamp with time zone|true|||clock_timestamp()",
	}); err != nil {
		return fmt.Errorf("managed receipt table contract: %w", err)
	}
	if err := verifyExactManagedReceiptRowVisibility(ctx, tx); err != nil {
		return fmt.Errorf("managed receipt table row visibility: %w", err)
	}
	if err := verifyExactConstraintsAndIndexes(ctx, tx, "wallaby_meta", "__delivery_receipts", []string{
		managedReceiptCanonicalConstraint,
		"wallaby_delivery_receipts_logical_batch_unique|u|false|false|true|UNIQUE (flow_incarnation_id, destination_revision_id, logical_batch_id)",
		"wallaby_delivery_receipts_marker_unique|u|false|false|true|UNIQUE (marker_id)",
		"wallaby_delivery_receipts_pkey|p|false|false|true|PRIMARY KEY (flow_incarnation_id, destination_revision_id, position_id)",
	}, []exactIndexContract{
		{name: "wallaby_delivery_receipts_logical_batch_unique", unique: true, columns: []string{"flow_incarnation_id", "destination_revision_id", "logical_batch_id"}},
		{name: "wallaby_delivery_receipts_marker_unique", unique: true, columns: []string{"marker_id"}},
		{name: "wallaby_delivery_receipts_pkey", primary: true, unique: true, columns: []string{"flow_incarnation_id", "destination_revision_id", "position_id"}},
	}); err != nil {
		return fmt.Errorf("managed receipt table indexes/constraints: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit managed receipt contract: %w", err)
	}
	return nil
}

func verifyExactCatalogColumns(ctx context.Context, query managedSchemaQuerier, namespace, table string, expected []string) error {
	var exact bool
	if err := query.QueryRow(ctx, `SELECT COALESCE(r.relkind='r' AND r.relpersistence='p' AND
 array_agg(a.attname||'|'||pg_catalog.format_type(a.atttypid,a.atttypmod)||'|'||a.attnotnull::text||'|'||a.attgenerated::text||'|'||a.attidentity::text||'|'||COALESCE(pg_catalog.pg_get_expr(d.adbin,d.adrelid),'') ORDER BY a.attnum)=$3::text[],false)
FROM pg_catalog.pg_class r JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
JOIN pg_catalog.pg_attribute a ON a.attrelid=r.oid AND a.attnum>0 AND NOT a.attisdropped
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
WHERE n.nspname=$1 AND r.relname=$2 GROUP BY r.relkind,r.relpersistence`, namespace, table, expected).Scan(&exact); err != nil {
		return fmt.Errorf("inspect exact columns for %s.%s: %w", namespace, table, err)
	}
	if !exact {
		return fmt.Errorf("exact columns/NOT NULL contract mismatch for %s.%s", namespace, table)
	}
	return nil
}

func verifyExactManagedReceiptRowVisibility(ctx context.Context, query managedSchemaQuerier) error {
	var exact bool
	if err := query.QueryRow(ctx, `
SELECT COALESCE(
  relation.relkind='r'
  AND NOT relation.relispartition
  AND NOT relation.relhassubclass
  AND NOT relation.relrowsecurity
  AND NOT relation.relforcerowsecurity
  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid=relation.oid OR inhparent=relation.oid)
  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid=relation.oid AND NOT tgisinternal)
  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_rewrite WHERE ev_class=relation.oid)
  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_policy WHERE polrelid=relation.oid),
  false)
FROM pg_catalog.pg_class AS relation
JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace
WHERE namespace.nspname='wallaby_meta' AND relation.relname='__delivery_receipts'`).Scan(&exact); err != nil {
		return fmt.Errorf("inspect exact row-visibility contract: %w", err)
	}
	if !exact {
		return errors.New("delivery receipt relation admits inheritance, partitions, triggers, rules, or row-level visibility mutation")
	}
	return nil
}

const managedReceiptCanonicalConstraint = "wallaby_delivery_receipts_logical_batch_current|c|false|false|true|CHECK (logical_batch_id = ('logical-batch:'::text || encode(sha256((((convert_to(source_lineage_id, 'UTF8'::name) || decode('00'::text, 'hex'::text)) || convert_to(position_id, 'UTF8'::name)) || decode('00'::text, 'hex'::text)) || convert_to(content_hash, 'UTF8'::name)), 'hex'::text)))"

type exactIndexContract struct {
	name            string
	primary, unique bool
	columns         []string
}

func verifyExactConstraintsAndIndexes(ctx context.Context, query managedSchemaQuerier, namespace, table string, constraints []string, indexes []exactIndexContract) error {
	expectedConstraints := append([]string(nil), constraints...)
	sort.Strings(expectedConstraints)
	var constraintsExact bool
	if err := query.QueryRow(ctx, `SELECT COALESCE(array_agg(c.conname||'|'||c.contype::text||'|'||c.condeferrable::text||'|'||c.condeferred::text||'|'||c.convalidated::text||'|'||pg_catalog.pg_get_constraintdef(c.oid,true) ORDER BY c.conname),'{}'::text[])=$3::text[]
FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_class r ON r.oid=c.conrelid JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname=$1 AND r.relname=$2`, namespace, table, expectedConstraints).Scan(&constraintsExact); err != nil {
		return fmt.Errorf("inspect exact constraints for %s.%s: %w", namespace, table, err)
	}
	if !constraintsExact {
		return fmt.Errorf("exact constraint contract mismatch for %s.%s", namespace, table)
	}
	var indexCount int
	if err := query.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class r ON r.oid=i.indrelid JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname=$1 AND r.relname=$2`, namespace, table).Scan(&indexCount); err != nil {
		return err
	}
	if indexCount != len(indexes) {
		return fmt.Errorf("exact index count mismatch for %s.%s: got %d want %d", namespace, table, indexCount, len(indexes))
	}
	for _, expected := range indexes {
		var exact bool
		if err := query.QueryRow(ctx, `SELECT am.amname='btree' AND i.indisunique=$4 AND i.indisprimary=$5 AND i.indisvalid AND i.indisready AND i.indislive AND i.indimmediate
 AND NOT i.indisclustered AND NOT i.indisreplident AND NOT i.indisexclusion AND NOT i.indcheckxmin
 AND NOT COALESCE((to_jsonb(i)->>'indnullsnotdistinct')::boolean,false)
 AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnkeyatts=cardinality($6::text[]) AND i.indnatts=i.indnkeyatts
 AND keys.names=$6::text[] AND NOT EXISTS (
  SELECT 1 FROM generate_subscripts(i.indkey::smallint[],1) s(ord)
  JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=(i.indkey::smallint[])[s.ord]
  JOIN pg_catalog.pg_opclass opc ON opc.oid=(i.indclass::oid[])[s.ord]
  WHERE NOT opc.opcdefault OR opc.opcmethod<>index_relation.relam OR (i.indcollation::oid[])[s.ord]<>a.attcollation OR (i.indoption::smallint[])[s.ord]<>0)
FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class r ON r.oid=i.indrelid JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
JOIN pg_catalog.pg_class index_relation ON index_relation.oid=i.indexrelid JOIN pg_catalog.pg_am am ON am.oid=index_relation.relam
CROSS JOIN LATERAL (SELECT array_agg(a.attname::text ORDER BY k.ord) names FROM unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,ord) JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum WHERE k.ord<=i.indnkeyatts) keys
WHERE n.nspname=$1 AND r.relname=$2 AND index_relation.relname=$3`, namespace, table, expected.name, expected.unique, expected.primary, expected.columns).Scan(&exact); err != nil {
			return fmt.Errorf("inspect exact index %s: %w", expected.name, err)
		}
		if !exact {
			return fmt.Errorf("exact index contract mismatch for %s.%s index %s", namespace, table, expected.name)
		}
	}
	return nil
}

func (d *Destination) loadManagedReceipt(ctx context.Context, tx pgx.Tx, intent connector.DeliveryIntent) (string, error) {
	if strings.TrimSpace(intent.LogicalBatchID) == "" {
		return "", errors.New("managed PostgreSQL delivery requires logical_batch_id")
	}
	rows, err := tx.Query(ctx, `
SELECT marker_id,flow_id,source_lineage_id,logical_batch_id,position_id,content_hash
FROM ONLY wallaby_meta.__delivery_receipts
WHERE flow_incarnation_id=$1
  AND destination_revision_id=$2
  AND (logical_batch_id=$3 OR position_id=$4)
FOR UPDATE`, intent.FlowIncarnationID, intent.DestinationRevisionID, intent.LogicalBatchID, intent.PositionID)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var markerID, flowID, sourceLineageID, logicalBatchID, positionID, contentHash string
		if err := rows.Scan(&markerID, &flowID, &sourceLineageID, &logicalBatchID, &positionID, &contentHash); err != nil {
			return "", err
		}
		if found || markerID != postgresDeliveryMarkerID(intent) || flowID != intent.FlowID || sourceLineageID != intent.SourceLineageID || logicalBatchID != intent.LogicalBatchID || positionID != intent.PositionID || contentHash != intent.ContentHash {
			return "", fmt.Errorf("%w: target receipt immutable identity differs", connector.ErrDeliveryConflict)
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if !found {
		return "", pgx.ErrNoRows
	}
	return intent.ContentHash, nil
}

func (d *Destination) insertManagedReceipt(ctx context.Context, tx pgx.Tx, intent connector.DeliveryIntent, markerID string) error {
	if strings.TrimSpace(intent.LogicalBatchID) == "" {
		return errors.New("managed PostgreSQL delivery requires logical_batch_id")
	}
	if _, err := tx.Exec(ctx, `SAVEPOINT wallaby_managed_receipt_insert`); err != nil {
		return fmt.Errorf("create postgres delivery receipt savepoint: %w", err)
	}
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
		intent.LogicalBatchID,
		intent.PositionID,
		intent.ContentHash,
	)
	if err == nil {
		if _, err := tx.Exec(ctx, `RELEASE SAVEPOINT wallaby_managed_receipt_insert`); err != nil {
			return fmt.Errorf("release postgres delivery receipt savepoint: %w", err)
		}
		return nil
	}
	var postgresErr *pgconn.PgError
	if !errors.As(err, &postgresErr) || postgresErr.Code != "23505" {
		return fmt.Errorf("insert postgres delivery receipt: %w", err)
	}
	if _, rollbackErr := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT wallaby_managed_receipt_insert`); rollbackErr != nil {
		return fmt.Errorf("%w: recover concurrent postgres receipt insert: %w (insert: %w)", connector.ErrDeliveryIndeterminate, rollbackErr, err)
	}
	_, reconcileErr := d.loadManagedReceipt(ctx, tx, intent)
	switch {
	case reconcileErr == nil:
		return fmt.Errorf("%w: an exact concurrent postgres receipt committed; rollback target DML and reconcile", connector.ErrDeliveryIndeterminate)
	case errors.Is(reconcileErr, connector.ErrDeliveryConflict):
		return reconcileErr
	case errors.Is(reconcileErr, pgx.ErrNoRows):
		return fmt.Errorf("%w: postgres receipt uniqueness conflict is not yet reconcilable", connector.ErrDeliveryIndeterminate)
	default:
		return fmt.Errorf("%w: reconcile concurrent postgres receipt insert: %w", connector.ErrDeliveryIndeterminate, reconcileErr)
	}
}

func postgresDeliveryMarkerID(intent connector.DeliveryIntent) string {
	hash := sha256.Sum256([]byte(strings.Join([]string{
		intent.FlowIncarnationID,
		intent.SourceLineageID,
		intent.DestinationRevisionID,
		intent.LogicalBatchID,
		intent.PositionID,
		intent.ContentHash,
	}, "\x00")))
	return "wallaby-pg-" + hex.EncodeToString(hash[:])
}

var _ connector.ManagedDestination = (*Destination)(nil)
