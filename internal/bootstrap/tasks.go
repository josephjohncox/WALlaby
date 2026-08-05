package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

const (
	maxBootstrapTasks              = 256
	SnapshotDeliveryContractV1 int = 1
)

// SnapshotDeliveryContract is the frozen mapped destination shape for one
// source snapshot task. It is distinct from the source query schema and keys.
type SnapshotDeliveryContract struct {
	Version               int                        `json:"version"`
	Schema                connector.Schema           `json:"schema"`
	WritePolicy           connector.TableWritePolicy `json:"write_policy"`
	ProjectionFingerprint string                     `json:"projection_fingerprint"`
}

// SnapshotTask is one bounded, immutable relation task in the frozen manifest.
// Namespace, Table, Schema, and KeyColumns remain source-shaped for snapshot
// queries and cursors; Delivery is the only contract accepted by destination I/O.
type SnapshotTask struct {
	RelationID uint32                   `json:"relation_id"`
	TaskID     string                   `json:"task_id"`
	Namespace  string                   `json:"source_namespace"`
	Table      string                   `json:"source_table"`
	Schema     connector.Schema         `json:"source_schema"`
	KeyColumns []string                 `json:"source_key_columns"`
	Delivery   SnapshotDeliveryContract `json:"destination"`
}

// WorkID is the exact work-claim identity for this task.
func (t SnapshotTask) WorkID(bootstrapID uuid.UUID) string {
	return fmt.Sprintf("%s/%d/%s", bootstrapID, t.RelationID, t.TaskID)
}

// Validate rejects missing, ambiguous, or internally inconsistent destination
// contracts before they can become bootstrap identity.
func (c SnapshotDeliveryContract) Validate() error {
	if c.Version != SnapshotDeliveryContractV1 {
		return fmt.Errorf("destination contract version must be %d", SnapshotDeliveryContractV1)
	}
	if strings.TrimSpace(c.ProjectionFingerprint) == "" || c.ProjectionFingerprint != strings.TrimSpace(c.ProjectionFingerprint) {
		return errors.New("projection fingerprint is required and must be canonical")
	}
	if strings.TrimSpace(c.Schema.Namespace) == "" || strings.TrimSpace(c.Schema.Name) == "" || len(c.Schema.Columns) == 0 {
		return errors.New("projected destination schema, namespace, table, and columns are required")
	}
	if c.WritePolicy.ProjectionFingerprint != c.ProjectionFingerprint {
		return errors.New("write policy projection fingerprint differs from the destination contract")
	}
	switch c.WritePolicy.Mode {
	case connector.ResolvedWriteAppend:
	case connector.ResolvedWriteUpsert:
		if len(c.WritePolicy.KeyColumns) == 0 {
			return errors.New("projected upsert destination contract requires key columns")
		}
	default:
		return fmt.Errorf("projected destination write mode %q is invalid", c.WritePolicy.Mode)
	}
	return nil
}

// SnapshotManifestHash returns the canonical identity of the complete source
// query and mapped destination contracts for a frozen snapshot generation.
func SnapshotManifestHash(tasks []SnapshotTask) (string, error) {
	manifest := append([]SnapshotTask(nil), tasks...)
	sort.Slice(manifest, func(i, j int) bool {
		if manifest[i].RelationID != manifest[j].RelationID {
			return manifest[i].RelationID < manifest[j].RelationID
		}
		return manifest[i].TaskID < manifest[j].TaskID
	})
	for _, task := range manifest {
		if err := task.Delivery.Validate(); err != nil {
			return "", fmt.Errorf("validate snapshot destination contract %d/%s: %w", task.RelationID, task.TaskID, err)
		}
	}
	encoded, err := json.Marshal(struct {
		Version int            `json:"version"`
		Tasks   []SnapshotTask `json:"tasks"`
	}{Version: SnapshotDeliveryContractV1, Tasks: manifest})
	if err != nil {
		return "", fmt.Errorf("marshal snapshot manifest: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// FreezeManifest installs the exact relation/schema plan observed through an
// imported exported snapshot. No task can be added after the first receipt.
func (b *Bootstrapper) FreezeManifest(ctx context.Context, fence authority.RunFence, snapshot ExportedSnapshot, sourceLineageID, manifestHash, publicationRevision string, tasks []SnapshotTask) (ExportedSnapshot, error) {
	if strings.TrimSpace(sourceLineageID) == "" || strings.TrimSpace(manifestHash) == "" || strings.TrimSpace(publicationRevision) == "" {
		return ExportedSnapshot{}, errors.New("source lineage, manifest, and publication revision are required")
	}
	if len(tasks) == 0 || len(tasks) > maxBootstrapTasks {
		return ExportedSnapshot{}, fmt.Errorf("bootstrap task count must be between 1 and %d", maxBootstrapTasks)
	}
	sorted := append([]SnapshotTask(nil), tasks...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].RelationID != sorted[j].RelationID {
			return sorted[i].RelationID < sorted[j].RelationID
		}
		return sorted[i].TaskID < sorted[j].TaskID
	})
	expectedManifestHash, err := SnapshotManifestHash(sorted)
	if err != nil {
		return ExportedSnapshot{}, err
	}
	if manifestHash != expectedManifestHash {
		return ExportedSnapshot{}, fmt.Errorf("%w: supplied bootstrap manifest hash does not bind the frozen source and destination contracts", connector.ErrDeliveryConflict)
	}
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return ExportedSnapshot{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return ExportedSnapshot{}, err
	}
	persisted, phase, err := loadSnapshotForUpdate(ctx, tx, fence, snapshot.BootstrapID)
	if err != nil {
		return ExportedSnapshot{}, err
	}
	if phase != "snapshotting" {
		return ExportedSnapshot{}, fmt.Errorf("freeze bootstrap manifest in phase %s", phase)
	}
	if err := compareSnapshot(persisted, snapshot); err != nil {
		return ExportedSnapshot{}, err
	}
	var receiptCount int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM snapshot_delivery_receipts WHERE bootstrap_id=$1`, snapshot.BootstrapID).Scan(&receiptCount); err != nil {
		return ExportedSnapshot{}, err
	}
	if receiptCount != 0 {
		return ExportedSnapshot{}, errors.New("bootstrap manifest cannot change after a task receipt")
	}
	for _, task := range sorted {
		if task.RelationID == 0 || strings.TrimSpace(task.TaskID) == "" || strings.TrimSpace(task.Namespace) == "" || strings.TrimSpace(task.Table) == "" || len(task.Schema.Columns) == 0 || len(task.KeyColumns) == 0 {
			return ExportedSnapshot{}, fmt.Errorf("bootstrap task %d/%q is incomplete or lacks a primary key", task.RelationID, task.TaskID)
		}
		if err := task.Delivery.Validate(); err != nil {
			return ExportedSnapshot{}, fmt.Errorf("bootstrap task %s destination contract: %w", task.WorkID(snapshot.BootstrapID), err)
		}
		schemaJSON, err := json.Marshal(task.Schema)
		if err != nil {
			return ExportedSnapshot{}, fmt.Errorf("marshal bootstrap source schema: %w", err)
		}
		keysJSON, err := json.Marshal(task.KeyColumns)
		if err != nil {
			return ExportedSnapshot{}, fmt.Errorf("marshal bootstrap keys: %w", err)
		}
		destinationSchemaJSON, err := json.Marshal(task.Delivery.Schema)
		if err != nil {
			return ExportedSnapshot{}, fmt.Errorf("marshal bootstrap destination schema: %w", err)
		}
		writePolicyJSON, err := json.Marshal(task.Delivery.WritePolicy)
		if err != nil {
			return ExportedSnapshot{}, fmt.Errorf("marshal bootstrap destination write policy: %w", err)
		}
		tag, err := tx.Exec(ctx, `
INSERT INTO source_bootstrap_tasks (
  bootstrap_id,relation_id,task_id,flow_incarnation_id,generation,acquisition_id,lease_epoch,
  table_schema,table_name,schema_json,key_columns,destination_schema_json,write_policy_json,
  projection_fingerprint,projection_version,status,authority_origin
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'pending','fenced')
ON CONFLICT (bootstrap_id,relation_id,task_id) DO UPDATE SET task_id=source_bootstrap_tasks.task_id
WHERE source_bootstrap_tasks.flow_incarnation_id=EXCLUDED.flow_incarnation_id
  AND source_bootstrap_tasks.generation=EXCLUDED.generation
  AND source_bootstrap_tasks.acquisition_id=EXCLUDED.acquisition_id
  AND source_bootstrap_tasks.lease_epoch=EXCLUDED.lease_epoch
  AND source_bootstrap_tasks.table_schema=EXCLUDED.table_schema
  AND source_bootstrap_tasks.table_name=EXCLUDED.table_name
  AND source_bootstrap_tasks.schema_json=EXCLUDED.schema_json
  AND source_bootstrap_tasks.key_columns=EXCLUDED.key_columns
  AND source_bootstrap_tasks.destination_schema_json=EXCLUDED.destination_schema_json
  AND source_bootstrap_tasks.write_policy_json=EXCLUDED.write_policy_json
  AND source_bootstrap_tasks.projection_fingerprint=EXCLUDED.projection_fingerprint
  AND source_bootstrap_tasks.projection_version=EXCLUDED.projection_version
  AND source_bootstrap_tasks.batch_ordinal=0
  AND source_bootstrap_tasks.receipt_count=0`, snapshot.BootstrapID, task.RelationID, task.TaskID, fence.FlowIncarnationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, task.Namespace, task.Table, schemaJSON, keysJSON, destinationSchemaJSON, writePolicyJSON, task.Delivery.ProjectionFingerprint, task.Delivery.Version)
		if err != nil {
			return ExportedSnapshot{}, fmt.Errorf("freeze bootstrap task %s: %w", task.WorkID(snapshot.BootstrapID), err)
		}
		if tag.RowsAffected() != 1 {
			return ExportedSnapshot{}, fmt.Errorf("%w: bootstrap task identity conflict", connector.ErrDeliveryConflict)
		}
	}
	tag, err := tx.Exec(ctx, `
UPDATE source_bootstraps
SET source_lineage_id=$4,manifest_hash=$5,publication_revision=$6,updated_at=clock_timestamp()
WHERE bootstrap_id=$1 AND flow_incarnation_id=$2 AND owner_generation=$3 AND phase='snapshotting'`, snapshot.BootstrapID, fence.FlowIncarnationID, fence.Generation, sourceLineageID, manifestHash, publicationRevision)
	if err != nil {
		return ExportedSnapshot{}, err
	}
	if tag.RowsAffected() != 1 {
		return ExportedSnapshot{}, errors.New("bootstrap manifest phase changed concurrently")
	}
	if err := tx.Commit(ctx); err != nil {
		return ExportedSnapshot{}, err
	}
	snapshot.SourceLineageID = sourceLineageID
	snapshot.PublicationRevision = publicationRevision
	snapshot.ManifestHash = manifestHash
	return snapshot, nil
}

// DeliverTaskBatch executes prepare -> reconcile/apply -> fenced receipt. The
// durable cursor and completion state advance in the receipt transaction.
func (b *Bootstrapper) DeliverTaskBatch(ctx context.Context, claim authority.ClaimFence, snapshot ExportedSnapshot, task SnapshotTask, ordinal int64, cursor json.RawMessage, complete bool, destinationRevisionID string, batch connector.Batch, driver connector.ManagedBootstrapDestination) error {
	if strings.TrimSpace(snapshot.SourceLineageID) == "" || strings.TrimSpace(snapshot.PublicationRevision) == "" || strings.TrimSpace(snapshot.ManifestHash) == "" {
		return errors.New("bootstrap delivery requires a frozen manifest identity")
	}
	if task.RelationID == 0 || strings.TrimSpace(task.TaskID) == "" || len(task.Schema.Columns) == 0 || len(task.KeyColumns) == 0 {
		return errors.New("bootstrap delivery requires a complete source task identity")
	}
	if err := task.Delivery.Validate(); err != nil {
		return fmt.Errorf("bootstrap delivery requires an explicit frozen destination contract: %w", err)
	}
	if !reflect.DeepEqual(batch.Schema, task.Delivery.Schema) || !reflect.DeepEqual(batch.WritePolicy, task.Delivery.WritePolicy) {
		return errors.New("bootstrap delivery batch differs from the frozen destination schema or write policy")
	}
	if driver == nil {
		return errors.New("managed bootstrap destination is required")
	}
	if ordinal <= 0 || strings.TrimSpace(destinationRevisionID) == "" {
		return errors.New("positive bootstrap batch ordinal and destination revision are required")
	}
	if claim.Kind != authority.ClaimSnapshot || claim.WorkID != task.WorkID(snapshot.BootstrapID) {
		return errors.New("snapshot batch claim does not match its task")
	}
	contentHash, err := connector.BatchContentHash(batch)
	if err != nil {
		return err
	}
	positionID := fmt.Sprintf("bootstrap/%s/%d/%s/%d", snapshot.BootstrapID, task.RelationID, task.TaskID, ordinal)
	logicalBatchID, err := connector.DeliveryLogicalBatchID(snapshot.SourceLineageID, positionID, contentHash)
	if err != nil {
		return err
	}
	intent := connector.DeliveryIntent{
		FlowID: claim.FlowID, FlowIncarnationID: claim.FlowIncarnationID.String(),
		SourceLineageID: snapshot.SourceLineageID,
		Generation:      claim.Generation, AcquisitionID: claim.AcquisitionID.String(), LeaseEpoch: claim.LeaseEpoch,
		DestinationRevisionID: destinationRevisionID, LogicalBatchID: logicalBatchID, PositionID: positionID, ContentHash: contentHash,
	}
	bootstrapIntent := connector.BootstrapIntent{
		FlowID: claim.FlowID, FlowIncarnationID: claim.FlowIncarnationID.String(),
		SourceLineageID: intent.SourceLineageID, BootstrapID: snapshot.BootstrapID.String(),
		BootstrapGeneration: snapshot.BootstrapGeneration, Generation: claim.Generation,
		AcquisitionID: claim.AcquisitionID.String(), LeaseEpoch: claim.LeaseEpoch,
		DestinationRevisionID: destinationRevisionID, ManifestHash: snapshot.ManifestHash,
	}
	attemptID, alreadyComplete, err := b.prepareTaskAttempt(ctx, claim, snapshot, task, ordinal, destinationRevisionID, positionID, logicalBatchID, contentHash)
	if err != nil || alreadyComplete {
		return err
	}
	disposition, evidence, err := driver.ReconcileBootstrap(ctx, bootstrapIntent, intent)
	if err != nil {
		return fmt.Errorf("reconcile bootstrap batch: %w", err)
	}
	switch disposition {
	case connector.DeliveryApplied:
	case connector.DeliveryNotApplied:
		evidence, err = driver.ApplyBootstrap(ctx, bootstrapIntent, intent, batch)
		if err != nil {
			return err
		}
	case connector.DeliveryIndeterminate:
		return fmt.Errorf("%w: bootstrap batch %s has no conclusive destination evidence", connector.ErrDeliveryIndeterminate, positionID)
	default:
		return fmt.Errorf("unknown bootstrap reconciliation disposition %d", disposition)
	}
	if evidence.ContentHash != contentHash || strings.TrimSpace(evidence.ExternalID) == "" {
		return fmt.Errorf("%w: bootstrap destination evidence mismatch", connector.ErrDeliveryConflict)
	}
	if b.hooks.AfterSnapshotBatchApply != nil {
		if err := b.hooks.AfterSnapshotBatchApply(ctx, snapshot, task, ordinal); err != nil {
			return err
		}
	}
	return b.finalizeTaskAttempt(ctx, claim, snapshot, task, ordinal, cursor, complete, attemptID, positionID, logicalBatchID, contentHash, evidence)
}

func (b *Bootstrapper) prepareTaskAttempt(ctx context.Context, claim authority.ClaimFence, snapshot ExportedSnapshot, task SnapshotTask, ordinal int64, destinationRevisionID, positionID, logicalBatchID, contentHash string) (uuid.UUID, bool, error) {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return uuid.Nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateClaimFence(ctx, tx, claim); err != nil {
		return uuid.Nil, false, err
	}
	persisted, phase, err := loadSnapshotForUpdate(ctx, tx, claim.RunFence, snapshot.BootstrapID)
	if err != nil {
		return uuid.Nil, false, err
	}
	if phase != "snapshotting" || compareSnapshot(persisted, snapshot) != nil {
		return uuid.Nil, false, errors.New("bootstrap task is not attached to the current snapshotting generation")
	}
	var currentOrdinal int64
	var status, namespace, table, projectionFingerprint string
	var projectionVersion int
	var schemaJSON, keysJSON, destinationSchemaJSON, writePolicyJSON []byte
	if err := tx.QueryRow(ctx, `
SELECT batch_ordinal,status,table_schema,table_name,schema_json,key_columns,
       destination_schema_json,write_policy_json,projection_fingerprint,projection_version
FROM source_bootstrap_tasks
WHERE bootstrap_id=$1 AND relation_id=$2 AND task_id=$3
  AND flow_incarnation_id=$4 AND generation=$5
FOR UPDATE`, snapshot.BootstrapID, task.RelationID, task.TaskID, claim.FlowIncarnationID, claim.Generation).Scan(&currentOrdinal, &status, &namespace, &table, &schemaJSON, &keysJSON, &destinationSchemaJSON, &writePolicyJSON, &projectionFingerprint, &projectionVersion); err != nil {
		return uuid.Nil, false, err
	}
	var frozenSchema, frozenDestinationSchema connector.Schema
	var frozenKeys []string
	var frozenWritePolicy connector.TableWritePolicy
	if err := json.Unmarshal(schemaJSON, &frozenSchema); err != nil {
		return uuid.Nil, false, fmt.Errorf("decode frozen bootstrap source schema: %w", err)
	}
	if err := json.Unmarshal(keysJSON, &frozenKeys); err != nil {
		return uuid.Nil, false, fmt.Errorf("decode frozen bootstrap keys: %w", err)
	}
	if err := json.Unmarshal(destinationSchemaJSON, &frozenDestinationSchema); err != nil {
		return uuid.Nil, false, fmt.Errorf("decode frozen bootstrap destination schema: %w", err)
	}
	if err := json.Unmarshal(writePolicyJSON, &frozenWritePolicy); err != nil {
		return uuid.Nil, false, fmt.Errorf("decode frozen bootstrap write policy: %w", err)
	}
	frozenDelivery := SnapshotDeliveryContract{Version: projectionVersion, Schema: frozenDestinationSchema, WritePolicy: frozenWritePolicy, ProjectionFingerprint: projectionFingerprint}
	if namespace != task.Namespace || table != task.Table || !reflect.DeepEqual(frozenSchema, task.Schema) || !reflect.DeepEqual(frozenKeys, task.KeyColumns) || !reflect.DeepEqual(frozenDelivery, task.Delivery) {
		return uuid.Nil, false, fmt.Errorf("%w: bootstrap task differs from frozen source or destination manifest", connector.ErrDeliveryConflict)
	}
	if ordinal <= currentOrdinal {
		var existingHash, existingLogicalBatchID string
		err := tx.QueryRow(ctx, `
SELECT content_hash,logical_batch_id FROM snapshot_delivery_receipts
WHERE bootstrap_id=$1 AND relation_id=$2 AND task_id=$3 AND batch_ordinal=$4`, snapshot.BootstrapID, task.RelationID, task.TaskID, ordinal).Scan(&existingHash, &existingLogicalBatchID)
		if err != nil {
			return uuid.Nil, false, err
		}
		if existingHash != contentHash || existingLogicalBatchID != logicalBatchID {
			return uuid.Nil, false, fmt.Errorf("%w: replayed snapshot batch changed content", connector.ErrDeliveryConflict)
		}
		return uuid.Nil, true, tx.Commit(ctx)
	}
	if status == "complete" || ordinal != currentOrdinal+1 {
		return uuid.Nil, false, fmt.Errorf("snapshot task batch ordinal=%d current=%d status=%s", ordinal, currentOrdinal, status)
	}
	attemptID := uuid.New()
	tag, err := tx.Exec(ctx, `
INSERT INTO snapshot_delivery_attempts (
  attempt_id,bootstrap_id,relation_id,task_id,batch_ordinal,flow_incarnation_id,
  generation,acquisition_id,lease_epoch,claim_epoch,destination_revision_id,position_id,logical_batch_id,content_hash
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
ON CONFLICT (bootstrap_id,relation_id,task_id,batch_ordinal) DO NOTHING`, attemptID, snapshot.BootstrapID, task.RelationID, task.TaskID, ordinal, claim.FlowIncarnationID, claim.Generation, claim.AcquisitionID, claim.LeaseEpoch, claim.ClaimEpoch, destinationRevisionID, positionID, logicalBatchID, contentHash)
	if err != nil {
		return uuid.Nil, false, err
	}
	if tag.RowsAffected() == 0 {
		if err := tx.QueryRow(ctx, `
SELECT attempt_id FROM snapshot_delivery_attempts
WHERE bootstrap_id=$1 AND relation_id=$2 AND task_id=$3 AND batch_ordinal=$4
  AND destination_revision_id=$5 AND position_id=$6 AND logical_batch_id=$7 AND content_hash=$8`, snapshot.BootstrapID, task.RelationID, task.TaskID, ordinal, destinationRevisionID, positionID, logicalBatchID, contentHash).Scan(&attemptID); err != nil {
			return uuid.Nil, false, fmt.Errorf("%w: bootstrap attempt identity conflict", connector.ErrDeliveryConflict)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, false, err
	}
	return attemptID, false, nil
}

func (b *Bootstrapper) finalizeTaskAttempt(ctx context.Context, claim authority.ClaimFence, snapshot ExportedSnapshot, task SnapshotTask, ordinal int64, cursor json.RawMessage, complete bool, attemptID uuid.UUID, positionID, logicalBatchID, contentHash string, evidence connector.DeliveryEvidence) error {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateClaimFence(ctx, tx, claim); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO snapshot_delivery_evidence(attempt_id,external_id,logical_batch_id,content_hash)
VALUES($1,$2,$3,$4)
ON CONFLICT(attempt_id) DO UPDATE SET external_id=EXCLUDED.external_id
WHERE snapshot_delivery_evidence.external_id=EXCLUDED.external_id
  AND snapshot_delivery_evidence.logical_batch_id=EXCLUDED.logical_batch_id
  AND snapshot_delivery_evidence.content_hash=EXCLUDED.content_hash`, attemptID, evidence.ExternalID, logicalBatchID, contentHash); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
INSERT INTO snapshot_delivery_receipts (
  bootstrap_id,relation_id,task_id,batch_ordinal,attempt_id,position_id,logical_batch_id,content_hash,external_id,durable_cursor,completed_task
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT (bootstrap_id,relation_id,task_id,batch_ordinal) DO UPDATE SET
  external_id=EXCLUDED.external_id
WHERE snapshot_delivery_receipts.attempt_id=EXCLUDED.attempt_id
  AND snapshot_delivery_receipts.position_id=EXCLUDED.position_id
  AND snapshot_delivery_receipts.logical_batch_id=EXCLUDED.logical_batch_id
  AND snapshot_delivery_receipts.content_hash=EXCLUDED.content_hash
  AND snapshot_delivery_receipts.external_id=EXCLUDED.external_id`, snapshot.BootstrapID, task.RelationID, task.TaskID, ordinal, attemptID, positionID, logicalBatchID, contentHash, evidence.ExternalID, cursor, complete)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: bootstrap receipt conflicts", connector.ErrDeliveryConflict)
	}
	status := "running"
	if complete {
		status = "complete"
	}
	receiptHash := contentHash
	tag, err = tx.Exec(ctx, `
UPDATE source_bootstrap_tasks
SET batch_ordinal=$4,durable_cursor=$5,receipt_hash=$6,status=$7,
    receipt_count=receipt_count+1,claim_epoch=$8,updated_at=clock_timestamp()
WHERE bootstrap_id=$1 AND relation_id=$2 AND task_id=$3
  AND flow_incarnation_id=$9 AND generation=$10
  AND batch_ordinal=$4-1 AND status <> 'complete'`, snapshot.BootstrapID, task.RelationID, task.TaskID, ordinal, cursor, receiptHash, status, claim.ClaimEpoch, claim.FlowIncarnationID, claim.Generation)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: stale snapshot task cursor", authority.ErrFenceRejected)
	}
	return tx.Commit(ctx)
}
