package artifactlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// RootedArtifact is immutable, exact-version input to one asynchronous catalog
// delivery. Record ordinals let a consumer merge artifacts with ordered
// barriers without depending on object-list order.
type RootedArtifact struct {
	Evidence           ObjectEvidence
	ArtifactID         string
	LogicalBatchID     string
	Namespace          string
	Table              string
	SchemaID           string
	EncodedByteHash    string
	FragmentOrdinal    uint64
	FirstRecordOrdinal uint64
	RecordCount        uint64
}

// Consumer delivers rooted publications through an append-only Iceberg catalog
// seam. It does not implement equality deletes, upserts, or compaction.
type Consumer struct {
	pool    *pgxpool.Pool
	catalog Catalog
}

type claimedPublication struct {
	publicationID uuid.UUID
	objects       []RootedArtifact
	barriers      []Barrier
	contentHash   string
	attemptID     uuid.UUID
	hasAttempt    bool
	claim         authority.ClaimFence
}

func NewConsumer(pool *pgxpool.Pool, catalog Catalog) (*Consumer, error) {
	if pool == nil || catalog == nil {
		return nil, errors.New("artifact consumer PostgreSQL pool and catalog are required")
	}
	return &Consumer{pool: pool, catalog: catalog}, nil
}

// ConsumeNext processes one publication in PostgreSQL sequence order. Attempts
// are persisted before catalog I/O; ambiguous commits reconcile by exact
// publication/content identity or fail closed.
func (c *Consumer) ConsumeNext(ctx context.Context, fence authority.RunFence, consumerRevisionID, table string) (bool, error) {
	publication, err := c.loadNext(ctx, fence, consumerRevisionID)
	if err != nil {
		return false, err
	}
	if publication.publicationID == uuid.Nil {
		telemetry.RecordArtifactConsumerOutcome(ctx, "idle_or_claimed")
		return false, nil
	}
	defer func() {
		store, storeErr := authority.NewPostgresStore(c.pool)
		if storeErr == nil {
			_ = store.ReleaseClaim(context.WithoutCancel(ctx), publication.claim)
		}
	}()
	if publication.hasAttempt {
		disposition, commit, err := c.catalog.Reconcile(ctx, table, publication.publicationID)
		if err != nil {
			return false, err
		}
		switch disposition {
		case CatalogApplied:
			if commit.ContentHash != publication.contentHash {
				return false, fmt.Errorf("%w: catalog content hash %s, expected %s", connector.ErrDeliveryConflict, commit.ContentHash, publication.contentHash)
			}
			return true, c.finalize(ctx, fence, consumerRevisionID, publication.publicationID, publication.attemptID, commit)
		case CatalogIndeterminate:
			return false, fmt.Errorf("%w: Iceberg publication %s", connector.ErrDeliveryIndeterminate, publication.publicationID)
		case CatalogNotApplied:
			// Prepare a new append-only attempt below.
		}
	}
	attemptID, err := c.prepare(ctx, fence, consumerRevisionID, publication.publicationID)
	if err != nil {
		return false, err
	}
	commit, err := c.catalog.Append(ctx, table, publication.publicationID, publication.objects, publication.barriers)
	if err != nil {
		return false, err
	}
	if commit.ContentHash != publication.contentHash {
		return false, fmt.Errorf("%w: catalog commit hash %s, expected %s", connector.ErrDeliveryConflict, commit.ContentHash, publication.contentHash)
	}
	if err := c.finalize(ctx, fence, consumerRevisionID, publication.publicationID, attemptID, commit); err != nil {
		telemetry.RecordArtifactConsumerOutcome(ctx, "finalize_failed")
		return false, err
	}
	telemetry.RecordArtifactConsumerOutcome(ctx, "committed")
	return true, nil
}

func (c *Consumer) loadNext(ctx context.Context, fence authority.RunFence, consumerRevisionID string) (claimedPublication, error) {
	var result claimedPublication
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return result, err
	}
	err = tx.QueryRow(ctx, `
SELECT publication_id
FROM artifact_deliveries
WHERE flow_incarnation_id=$1 AND consumer_revision_id=$2 AND delivered_at IS NULL
ORDER BY sequence
LIMIT 1
FOR UPDATE`, fence.FlowIncarnationID, consumerRevisionID).Scan(&result.publicationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return claimedPublication{}, nil
	}
	if err != nil {
		return result, err
	}
	claimKind := authority.ClaimKind("artifact_delivery")
	workID := consumerRevisionID + ":" + result.publicationID.String()
	claimEpoch := int64(1)
	var previousEpoch int64
	var live bool
	err = tx.QueryRow(ctx, `
SELECT claim.claim_epoch,
       claim.released_at IS NULL
       AND claim.claim_expires_at > clock_timestamp()
       AND producer.acquisition_id=claim.acquisition_id
       AND producer.lease_epoch=claim.lease_epoch
       AND producer.lease_expires_at > clock_timestamp()
FROM work_claims AS claim
LEFT JOIN producer_leases AS producer ON producer.incarnation_id=claim.incarnation_id
WHERE claim.incarnation_id=$1 AND claim.claim_kind=$2 AND claim.work_id=$3
FOR UPDATE OF claim`, fence.FlowIncarnationID, string(claimKind), workID).Scan(&previousEpoch, &live)
	switch {
	case err == nil && live:
		return claimedPublication{}, nil
	case err == nil:
		claimEpoch = previousEpoch + 1
	case !errors.Is(err, pgx.ErrNoRows):
		return result, err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO work_claims (
  incarnation_id,claim_kind,work_id,generation,acquisition_id,lease_epoch,
  claim_epoch,claim_expires_at,released_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,clock_timestamp()+interval '5 minutes',NULL)
ON CONFLICT (incarnation_id,claim_kind,work_id) DO UPDATE SET
  generation=EXCLUDED.generation,acquisition_id=EXCLUDED.acquisition_id,
  lease_epoch=EXCLUDED.lease_epoch,claim_epoch=EXCLUDED.claim_epoch,
  claim_expires_at=EXCLUDED.claim_expires_at,released_at=NULL,updated_at=clock_timestamp()`, fence.FlowIncarnationID, string(claimKind), workID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, claimEpoch); err != nil {
		return result, err
	}
	result.claim = authority.ClaimFence{RunFence: fence, Kind: claimKind, WorkID: workID, ClaimEpoch: claimEpoch}
	rows, err := tx.Query(ctx, `
SELECT object.bucket,object.object_key,object.version_id,object.checksum_sha256,
       object.encoded_length,object.encryption_mode,object.object_lock_evidence,
       object.artifact_id,object.encoded_byte_hash,object.logical_batch_id,
       object.namespace,object.table_name,object.schema_id,object.fragment_ordinal,
       object.first_record_ordinal,object.record_count
FROM artifact_publication_objects AS item
JOIN artifact_objects AS object ON object.artifact_id=item.artifact_id
WHERE item.publication_id=$1 AND item.release_marked_at IS NULL
  AND item.released_at IS NULL AND object.state='rooted'
ORDER BY item.ordinal`, result.publicationID)
	if err != nil {
		return result, err
	}
	hash := sha256.New()
	for rows.Next() {
		var object RootedArtifact
		var fragmentOrdinal, firstRecordOrdinal, recordCount int64
		if err := rows.Scan(
			&object.Evidence.Bucket, &object.Evidence.Key, &object.Evidence.VersionID,
			&object.Evidence.ChecksumSHA256, &object.Evidence.Length, &object.Evidence.EncryptionMode,
			&object.Evidence.ObjectLock, &object.ArtifactID, &object.EncodedByteHash,
			&object.LogicalBatchID, &object.Namespace, &object.Table, &object.SchemaID,
			&fragmentOrdinal, &firstRecordOrdinal, &recordCount,
		); err != nil {
			rows.Close()
			return result, err
		}
		if fragmentOrdinal < 0 || firstRecordOrdinal < 0 || recordCount <= 0 {
			rows.Close()
			return result, fmt.Errorf("rooted artifact %s has invalid ordinal metadata", object.ArtifactID)
		}
		object.FragmentOrdinal = uint64(fragmentOrdinal)       // #nosec G115 -- nonnegative BIGINT checked above.
		object.FirstRecordOrdinal = uint64(firstRecordOrdinal) // #nosec G115 -- nonnegative BIGINT checked above.
		object.RecordCount = uint64(recordCount)               // #nosec G115 -- positive BIGINT checked above.
		_, _ = hash.Write([]byte(object.ArtifactID))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(object.EncodedByteHash))
		_, _ = hash.Write([]byte{0})
		result.objects = append(result.objects, object)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return result, err
	}
	rows.Close()

	barrierRows, err := tx.Query(ctx, `
SELECT fragment_ordinal,record_ordinal,kind,namespace,table_name,
       COALESCE(schema_id,''),ddl,ddl_plan,content_hash
FROM artifact_barriers
WHERE publication_id=$1
ORDER BY record_ordinal`, result.publicationID)
	if err != nil {
		return result, err
	}
	for barrierRows.Next() {
		var barrier Barrier
		var fragmentOrdinal, recordOrdinal int64
		if err := barrierRows.Scan(
			&fragmentOrdinal, &recordOrdinal, &barrier.Kind, &barrier.Namespace,
			&barrier.Table, &barrier.SchemaID, &barrier.DDL, &barrier.DDLPlan, &barrier.ContentHash,
		); err != nil {
			barrierRows.Close()
			return result, err
		}
		if fragmentOrdinal < 0 || recordOrdinal < 0 {
			barrierRows.Close()
			return result, errors.New("artifact barrier has invalid ordinal metadata")
		}
		barrier.FragmentOrdinal = uint64(fragmentOrdinal) // #nosec G115 -- nonnegative BIGINT checked above.
		barrier.RecordOrdinal = uint64(recordOrdinal)     // #nosec G115 -- nonnegative BIGINT checked above.
		_, _ = hash.Write([]byte("barrier"))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(barrier.ContentHash))
		_, _ = hash.Write([]byte{0})
		result.barriers = append(result.barriers, barrier)
	}
	if err := barrierRows.Err(); err != nil {
		barrierRows.Close()
		return result, err
	}
	barrierRows.Close()
	if len(result.objects) == 0 && len(result.barriers) == 0 {
		return result, errors.New("artifact publication has no rooted objects or barriers")
	}
	result.contentHash = hex.EncodeToString(hash.Sum(nil))
	err = tx.QueryRow(ctx, `
SELECT attempt.attempt_id
FROM artifact_delivery_attempts AS attempt
LEFT JOIN artifact_delivery_receipts AS receipt ON receipt.attempt_id=attempt.attempt_id
WHERE attempt.flow_incarnation_id=$1 AND attempt.consumer_revision_id=$2
  AND attempt.publication_id=$3 AND receipt.attempt_id IS NULL
ORDER BY attempt.prepared_at DESC,attempt.attempt_id DESC
LIMIT 1`, fence.FlowIncarnationID, consumerRevisionID, result.publicationID).Scan(&result.attemptID)
	result.hasAttempt = err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return result, err
	}
	if err := tx.Commit(ctx); err != nil {
		return result, err
	}
	return result, nil
}

func (c *Consumer) prepare(ctx context.Context, fence authority.RunFence, consumerRevisionID string, publicationID uuid.UUID) (uuid.UUID, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return uuid.Nil, err
	}
	attemptID := uuid.New()
	if _, err := tx.Exec(ctx, `
INSERT INTO artifact_delivery_attempts (
  attempt_id,flow_incarnation_id,consumer_revision_id,publication_id,
  generation,acquisition_id,lease_epoch
) VALUES ($1,$2,$3,$4,$5,$6,$7)`, attemptID, fence.FlowIncarnationID, consumerRevisionID, publicationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch); err != nil {
		return uuid.Nil, fmt.Errorf("prepare artifact consumer attempt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, err
	}
	return attemptID, nil
}

func (c *Consumer) finalize(ctx context.Context, fence authority.RunFence, consumerRevisionID string, publicationID, attemptID uuid.UUID, commit CatalogCommit) error {
	if commit.SnapshotID == "" || commit.ContentHash == "" {
		return errors.New("catalog commit evidence is incomplete")
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	receiptTag, err := tx.Exec(ctx, `
INSERT INTO artifact_delivery_receipts (
  flow_incarnation_id,consumer_revision_id,publication_id,attempt_id,
  snapshot_id,content_hash,acquisition_id,lease_epoch
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (flow_incarnation_id,consumer_revision_id,publication_id) DO UPDATE SET
  snapshot_id=EXCLUDED.snapshot_id
WHERE artifact_delivery_receipts.snapshot_id=EXCLUDED.snapshot_id
  AND artifact_delivery_receipts.content_hash=EXCLUDED.content_hash`, fence.FlowIncarnationID, consumerRevisionID, publicationID, attemptID, commit.SnapshotID, commit.ContentHash, fence.AcquisitionID, fence.LeaseEpoch)
	if err != nil {
		return fmt.Errorf("record artifact consumer receipt: %w", err)
	}
	if receiptTag.RowsAffected() != 1 {
		return fmt.Errorf("%w: artifact consumer receipt differs", connector.ErrDeliveryConflict)
	}
	tag, err := tx.Exec(ctx, `
UPDATE artifact_deliveries
SET delivered_at=clock_timestamp()
WHERE flow_incarnation_id=$1 AND consumer_revision_id=$2 AND publication_id=$3 AND delivered_at IS NULL`, fence.FlowIncarnationID, consumerRevisionID, publicationID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("artifact delivery is no longer pending")
	}
	return tx.Commit(ctx)
}
