package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
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

// Reconcile proves an ambiguous managed delivery from the deterministic marker
// committed atomically with target DML.
func (d *Destination) Reconcile(ctx context.Context, intent connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	if err := intent.Validate(); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if d.pool == nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, errors.New("postgres destination not initialized")
	}
	row := d.pool.QueryRow(ctx, `
SELECT content_hash
FROM wallaby_meta.__delivery_receipts
WHERE flow_incarnation_id = $1
  AND destination_revision_id = $2
  AND source_lineage_id = $3
  AND position_id = $4`, intent.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, intent.PositionID)
	var contentHash string
	if err := row.Scan(&contentHash); err != nil {
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
	return nil
}

func (d *Destination) loadManagedReceipt(ctx context.Context, tx pgx.Tx, intent connector.DeliveryIntent) (string, error) {
	row := tx.QueryRow(ctx, `
SELECT content_hash
FROM wallaby_meta.__delivery_receipts
WHERE flow_incarnation_id = $1
  AND destination_revision_id = $2
  AND source_lineage_id = $3
  AND position_id = $4
FOR UPDATE`, intent.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, intent.PositionID)
	var contentHash string
	return contentHash, row.Scan(&contentHash)
}

func (d *Destination) insertManagedReceipt(ctx context.Context, tx pgx.Tx, intent connector.DeliveryIntent, markerID string) error {
	_, err := tx.Exec(ctx, `
INSERT INTO wallaby_meta.__delivery_receipts (
  marker_id, flow_id, flow_incarnation_id, generation, acquisition_id,
  lease_epoch, destination_revision_id, source_lineage_id, position_id, content_hash
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		markerID,
		intent.FlowID,
		intent.FlowIncarnationID,
		intent.Generation,
		intent.AcquisitionID,
		intent.LeaseEpoch,
		intent.DestinationRevisionID,
		intent.SourceLineageID,
		intent.PositionID,
		intent.ContentHash,
	)
	if err != nil {
		return fmt.Errorf("insert postgres delivery receipt: %w", err)
	}
	return nil
}

func postgresDeliveryMarkerID(intent connector.DeliveryIntent) string {
	hash := sha256.Sum256([]byte(strings.Join([]string{
		intent.FlowIncarnationID,
		intent.SourceLineageID,
		intent.DestinationRevisionID,
		intent.PositionID,
		intent.ContentHash,
	}, "\x00")))
	return "wallaby-pg-" + hex.EncodeToString(hash[:])
}

var _ connector.ManagedDestination = (*Destination)(nil)
