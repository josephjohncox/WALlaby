package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// LoadLatest adopts the newest bootstrap row for the current flow incarnation
// under a fresh producer acquisition. Public flow-ID reuse cannot match it.
func (b *Bootstrapper) LoadLatest(ctx context.Context, fence authority.RunFence) (ExportedSnapshot, string, error) {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return ExportedSnapshot{}, "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return ExportedSnapshot{}, "", err
	}
	var snapshot ExportedSnapshot
	var lsnText, phase string
	err = tx.QueryRow(ctx, `
SELECT bootstrap_id,bootstrap_generation,slot_name,publication_name,plugin,
       consistent_lsn,snapshot_name,source_system_id,database_name,COALESCE(source_lineage_id,''),
       COALESCE(publication_revision,''),manifest_hash,phase
FROM source_bootstraps
WHERE flow_incarnation_id=$1
ORDER BY bootstrap_generation DESC
LIMIT 1
FOR UPDATE`, fence.FlowIncarnationID).Scan(
		&snapshot.BootstrapID, &snapshot.BootstrapGeneration, &snapshot.SlotName,
		&snapshot.Publication, &snapshot.Plugin, &lsnText, &snapshot.SnapshotName,
		&snapshot.SourceSystem, &snapshot.DatabaseName, &snapshot.SourceLineageID,
		&snapshot.PublicationRevision, &snapshot.ManifestHash, &phase,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ExportedSnapshot{}, "", pgx.ErrNoRows
	}
	if err != nil {
		return ExportedSnapshot{}, "", err
	}
	snapshot.ConsistentLSN, err = pglogrepl.ParseLSN(lsnText)
	if err != nil {
		return ExportedSnapshot{}, "", fmt.Errorf("parse persisted bootstrap LSN: %w", err)
	}
	tag, err := tx.Exec(ctx, `
UPDATE source_bootstraps
SET owner_generation=$2,owner_acquisition_id=$3,owner_lease_epoch=$4,updated_at=clock_timestamp()
WHERE bootstrap_id=$1 AND flow_incarnation_id=$5`, snapshot.BootstrapID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, fence.FlowIncarnationID)
	if err != nil {
		return ExportedSnapshot{}, "", err
	}
	if tag.RowsAffected() != 1 {
		return ExportedSnapshot{}, "", fmt.Errorf("%w: bootstrap generation is not current", authority.ErrFenceRejected)
	}
	if _, err := tx.Exec(ctx, `
UPDATE source_resources
SET generation=$2,acquisition_id=$3,lease_epoch=$4,updated_at=clock_timestamp()
WHERE flow_incarnation_id=$1 AND bootstrap_id=$5 AND state IN ('ready','cleanup_pending')`, fence.FlowIncarnationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, snapshot.BootstrapID); err != nil {
		return ExportedSnapshot{}, "", fmt.Errorf("rebind active bootstrap resources: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ExportedSnapshot{}, "", err
	}
	return snapshot, phase, nil
}

// LoadSchemas reconstructs the frozen destination manifest for cleanup or
// publication reconciliation after exporter loss. Schema rows are immutable
// once any delivery receipt exists.
func (b *Bootstrapper) LoadSchemas(ctx context.Context, fence authority.RunFence, snapshot ExportedSnapshot) ([]connector.Schema, error) {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return nil, err
	}
	persisted, _, err := loadSnapshotForUpdate(ctx, tx, fence, snapshot.BootstrapID)
	if err != nil {
		return nil, err
	}
	if err := compareSnapshot(persisted, snapshot); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
SELECT schema_json FROM source_bootstrap_tasks
WHERE bootstrap_id=$1
ORDER BY relation_id,task_id`, snapshot.BootstrapID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var schemas []connector.Schema
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		var schema connector.Schema
		if err := json.Unmarshal(encoded, &schema); err != nil {
			return nil, fmt.Errorf("decode persisted bootstrap schema: %w", err)
		}
		schemas = append(schemas, schema)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(schemas) == 0 {
		return nil, errors.New("persisted bootstrap manifest has no schemas")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return schemas, nil
}

// TaskProgress returns the last receipt-backed cursor and ordinal. It never
// reads task state by public flow ID.
func (b *Bootstrapper) TaskProgress(ctx context.Context, fence authority.RunFence, snapshot ExportedSnapshot, task SnapshotTask) (int64, []byte, bool, error) {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return 0, nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return 0, nil, false, err
	}
	var ordinal int64
	var cursor []byte
	var status string
	if err := tx.QueryRow(ctx, `
SELECT batch_ordinal,durable_cursor,status
FROM source_bootstrap_tasks
WHERE bootstrap_id=$1 AND relation_id=$2 AND task_id=$3
  AND flow_incarnation_id=$4 AND generation=$5`, snapshot.BootstrapID, task.RelationID, task.TaskID, fence.FlowIncarnationID, fence.Generation).Scan(&ordinal, &cursor, &status); err != nil {
		return 0, nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, nil, false, err
	}
	return ordinal, cursor, status == "complete", nil
}
