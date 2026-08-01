package artifactlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

var (
	ErrQuotaExceeded = errors.New("artifact retained-byte quota exceeded")
	ErrBackpressure  = errors.New("artifact consumer backlog high-water mark exceeded")
)

// StreamConfig is durable admission for one canonical artifact stream.
type StreamConfig struct {
	HardRetainedBytes int64
	BacklogCountHigh  int64
	BacklogBytesHigh  int64
	BacklogAgeHigh    time.Duration
	Consumers         []string
}

// Publication is the rooted canonical transaction and source ACK grant.
type Publication struct {
	ID        uuid.UUID
	Artifacts []Artifact
	AckGrant  connector.AckGrant
}

// Publisher owns PostgreSQL reservation/publication and immutable object I/O.
type Publisher struct {
	pool    *pgxpool.Pool
	objects ObjectStore
	encoder *Encoder
	config  StreamConfig
}

func NewPublisher(ctx context.Context, pool *pgxpool.Pool, objects ObjectStore, config StreamConfig) (*Publisher, error) {
	if pool == nil || objects == nil {
		return nil, errors.New("artifact PostgreSQL pool and object store are required")
	}
	if config.HardRetainedBytes <= 0 || config.BacklogCountHigh <= 0 || config.BacklogBytesHigh <= 0 {
		return nil, errors.New("positive artifact retained and backlog limits are required")
	}
	if config.BacklogAgeHigh <= 0 {
		config.BacklogAgeHigh = 24 * time.Hour
	}
	if err := runMigrations(ctx, pool); err != nil {
		return nil, err
	}
	return &Publisher{pool: pool, objects: objects, encoder: NewEncoder(), config: config}, nil
}

// Publish encodes locally, reserves exact bytes before PUT, reconciles exact S3
// versions, then roots publication/checkpoint/ACK intent in one transaction.
func (p *Publisher) Publish(ctx context.Context, fence authority.RunFence, transaction connector.SourceTransaction) (Publication, error) {
	transaction, err := canonicalSourceTransaction(transaction)
	if err != nil {
		return Publication{}, err
	}
	artifacts, err := p.encoder.EncodeTransaction(ctx, fence.FlowIncarnationID, transaction)
	if err != nil {
		return Publication{}, err
	}
	if existing, ok, err := p.loadPublication(ctx, fence, transaction, artifacts); err != nil {
		return Publication{}, err
	} else if ok {
		return existing, nil
	}
	if err := p.reserve(ctx, fence, artifacts); err != nil {
		return Publication{}, err
	}
	for _, artifact := range artifacts {
		if err := p.uploadAndVerify(ctx, fence, artifact); err != nil {
			return Publication{}, err
		}
	}
	return p.root(ctx, fence, transaction, artifacts)
}

// RecomputeQuota restores durable accounting from PostgreSQL roots and active
// reservations after a crash. S3 listings are not consulted.
func (p *Publisher) RecomputeQuota(ctx context.Context, fence authority.RunFence) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	if err := p.ensureStreamRows(ctx, tx, fence); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM artifact_quota_accounts WHERE flow_incarnation_id=$1 FOR UPDATE`, fence.FlowIncarnationID); err != nil {
		return fmt.Errorf("lock artifact quota account: %w", err)
	}
	var reserved, rooted int64
	if err := tx.QueryRow(ctx, `
SELECT COALESCE(sum(bytes) FILTER (WHERE converted_at IS NULL AND released_at IS NULL),0)
FROM artifact_quota_reservations WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&reserved); err != nil {
		return fmt.Errorf("recompute artifact reservations: %w", err)
	}
	if err := tx.QueryRow(ctx, `
SELECT COALESCE(sum(encoded_length) FILTER (WHERE state='rooted'),0)
FROM artifact_objects WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&rooted); err != nil {
		return fmt.Errorf("recompute rooted artifact bytes: %w", err)
	}
	if _, err := tx.Exec(ctx, `
UPDATE artifact_quota_accounts
SET reserved_bytes=$2,rooted_bytes=$3,updated_at=clock_timestamp()
WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID, reserved, rooted); err != nil {
		return fmt.Errorf("store recomputed artifact quota: %w", err)
	}
	return tx.Commit(ctx)
}

func (p *Publisher) reserve(ctx context.Context, fence authority.RunFence, artifacts []Artifact) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin artifact reservation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	if err := p.ensureStreamRows(ctx, tx, fence); err != nil {
		return err
	}
	var reserved, rooted, hardLimit int64
	if err := tx.QueryRow(ctx, `
SELECT reserved_bytes,rooted_bytes,hard_limit_bytes
FROM artifact_quota_accounts WHERE flow_incarnation_id=$1 FOR UPDATE`, fence.FlowIncarnationID).Scan(&reserved, &rooted, &hardLimit); err != nil {
		return fmt.Errorf("lock artifact quota: %w", err)
	}
	var backlogCount, backlogBytes int64
	var backlogAgeSeconds float64
	if err := tx.QueryRow(ctx, `
SELECT count(*),COALESCE(sum(bytes),0),
       COALESCE(EXTRACT(EPOCH FROM clock_timestamp()-min(created_at)),0)
FROM artifact_deliveries
WHERE flow_incarnation_id=$1 AND delivered_at IS NULL`, fence.FlowIncarnationID).Scan(&backlogCount, &backlogBytes, &backlogAgeSeconds); err != nil {
		return fmt.Errorf("read artifact backlog: %w", err)
	}
	publicationBytes := int64(0)
	for _, artifact := range artifacts {
		publicationBytes += int64(len(artifact.Encoded))
	}
	projectedCount := backlogCount + int64(len(p.config.Consumers))
	projectedBytes := backlogBytes + publicationBytes*int64(len(p.config.Consumers))
	if projectedCount > p.config.BacklogCountHigh || projectedBytes > p.config.BacklogBytesHigh || time.Duration(backlogAgeSeconds*float64(time.Second)) >= p.config.BacklogAgeHigh {
		return fmt.Errorf("%w: projected count=%d bytes=%d age=%s", ErrBackpressure, projectedCount, projectedBytes, time.Duration(backlogAgeSeconds*float64(time.Second)))
	}

	newBytes := int64(0)
	for _, artifact := range artifacts {
		if _, err := tx.Exec(ctx, `
INSERT INTO canonical_schemas (schema_id,projection_id,schema_json)
VALUES ($1,$2,$3)
ON CONFLICT (schema_id) DO NOTHING`, artifact.SchemaID, ProjectionID, artifact.SchemaJSON); err != nil {
			return fmt.Errorf("store canonical schema: %w", err)
		}
		tag, err := tx.Exec(ctx, `
INSERT INTO artifact_objects (
  artifact_id,flow_incarnation_id,source_position,fragment_ordinal,schema_id,
  logical_content_hash,encoded_byte_hash,encoded_length,bucket,object_key,
  checksum_sha256,encoding,state
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'parquet','reserved')
ON CONFLICT (artifact_id) DO NOTHING`, artifact.ID, fence.FlowIncarnationID, artifact.SourcePosition, artifact.FragmentOrdinal, artifact.SchemaID, artifact.LogicalContentHash, artifact.EncodedByteHash, len(artifact.Encoded), p.objects.Bucket(), artifact.ObjectKey, artifact.ChecksumSHA256)
		if err != nil {
			return fmt.Errorf("reserve artifact object: %w", err)
		}
		if tag.RowsAffected() == 0 {
			var logicalHash, encodedHash, bucket, key string
			var length int64
			if err := tx.QueryRow(ctx, `
SELECT logical_content_hash,encoded_byte_hash,encoded_length,bucket,object_key
FROM artifact_objects WHERE artifact_id=$1 FOR UPDATE`, artifact.ID).Scan(&logicalHash, &encodedHash, &length, &bucket, &key); err != nil {
				return err
			}
			if logicalHash != artifact.LogicalContentHash || encodedHash != artifact.EncodedByteHash || length != int64(len(artifact.Encoded)) || bucket != p.objects.Bucket() || key != artifact.ObjectKey {
				return fmt.Errorf("%w: artifact identity %s was reused with different content", connector.ErrDeliveryConflict, artifact.ID)
			}
			continue
		}
		newBytes += int64(len(artifact.Encoded))
		if _, err := tx.Exec(ctx, `
INSERT INTO artifact_quota_reservations (artifact_id,flow_incarnation_id,bytes)
VALUES ($1,$2,$3)`, artifact.ID, fence.FlowIncarnationID, len(artifact.Encoded)); err != nil {
			return fmt.Errorf("reserve artifact quota: %w", err)
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO artifact_upload_attempts (attempt_id,artifact_id,generation,acquisition_id,lease_epoch)
VALUES ($1,$2,$3,$4,$5)`, uuid.New(), artifact.ID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch); err != nil {
			return fmt.Errorf("prepare artifact upload attempt: %w", err)
		}
	}
	if reserved+rooted+newBytes > hardLimit {
		return fmt.Errorf("%w: retained=%d reservation=%d hard_limit=%d", ErrQuotaExceeded, rooted, reserved+newBytes, hardLimit)
	}
	if _, err := tx.Exec(ctx, `
UPDATE artifact_quota_accounts
SET reserved_bytes=reserved_bytes+$2,updated_at=clock_timestamp()
WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID, newBytes); err != nil {
		return fmt.Errorf("charge artifact reservation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit artifact reservation: %w", err)
	}
	telemetry.RecordArtifactTransition(ctx, "reserved", newBytes)
	return nil
}

func (p *Publisher) uploadAndVerify(ctx context.Context, fence authority.RunFence, artifact Artifact) error {
	state, evidence, err := p.loadObject(ctx, fence, artifact.ID)
	if err != nil {
		return err
	}
	if state == "rooted" {
		return p.objects.HeadVersion(ctx, evidence)
	}
	if state == "reserved" {
		evidence, err = p.objects.PutImmutable(ctx, artifact.ObjectKey, artifact.Encoded, artifact.EncodedByteHash)
		if err != nil {
			evidence, err = p.objects.ReconcileVersion(ctx, artifact.ObjectKey, artifact.EncodedByteHash, int64(len(artifact.Encoded)))
			if err != nil {
				return err
			}
		}
		if err := p.recordUploaded(ctx, fence, artifact, evidence); err != nil {
			return err
		}
	}
	if err := p.objects.HeadVersion(ctx, evidence); err != nil {
		return err
	}
	if err := p.markVerified(ctx, fence, artifact.ID, evidence); err != nil {
		return err
	}
	telemetry.RecordArtifactTransition(ctx, "verified", evidence.Length)
	return nil
}

func (p *Publisher) loadObject(ctx context.Context, fence authority.RunFence, artifactID string) (string, ObjectEvidence, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return "", ObjectEvidence{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return "", ObjectEvidence{}, err
	}
	var state string
	var evidence ObjectEvidence
	var versionID *string
	if err := tx.QueryRow(ctx, `
SELECT state,bucket,object_key,version_id,checksum_sha256,encoded_length,encryption_mode,object_lock_evidence
FROM artifact_objects WHERE artifact_id=$1 AND flow_incarnation_id=$2`, artifactID, fence.FlowIncarnationID).Scan(&state, &evidence.Bucket, &evidence.Key, &versionID, &evidence.ChecksumSHA256, &evidence.Length, &evidence.EncryptionMode, &evidence.ObjectLock); err != nil {
		return "", ObjectEvidence{}, err
	}
	if versionID != nil {
		evidence.VersionID = *versionID
	}
	if err := tx.Commit(ctx); err != nil {
		return "", ObjectEvidence{}, err
	}
	return state, evidence, nil
}

func (p *Publisher) recordUploaded(ctx context.Context, fence authority.RunFence, artifact Artifact, evidence ObjectEvidence) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
UPDATE artifact_objects
SET version_id=$3,encryption_mode=$4,object_lock_evidence=$5,state='uploaded',updated_at=clock_timestamp()
WHERE artifact_id=$1 AND flow_incarnation_id=$2 AND state='reserved'
  AND bucket=$6 AND object_key=$7 AND encoded_byte_hash=$8 AND encoded_length=$9`, artifact.ID, fence.FlowIncarnationID, evidence.VersionID, evidence.EncryptionMode, evidence.ObjectLock, evidence.Bucket, evidence.Key, artifact.EncodedByteHash, len(artifact.Encoded))
	if err != nil {
		return fmt.Errorf("record uploaded artifact: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: stale or conflicting artifact upload %s", authority.ErrFenceRejected, artifact.ID)
	}
	return tx.Commit(ctx)
}

func (p *Publisher) markVerified(ctx context.Context, fence authority.RunFence, artifactID string, evidence ObjectEvidence) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
UPDATE artifact_objects
SET state='verified',updated_at=clock_timestamp()
WHERE artifact_id=$1 AND flow_incarnation_id=$2
  AND state IN ('uploaded','verified')
  AND version_id=$3 AND checksum_sha256=$4 AND encoded_length=$5`, artifactID, fence.FlowIncarnationID, evidence.VersionID, evidence.ChecksumSHA256, evidence.Length)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: artifact verification evidence differs", connector.ErrDeliveryConflict)
	}
	return tx.Commit(ctx)
}

func (p *Publisher) root(ctx context.Context, fence authority.RunFence, transaction connector.SourceTransaction, artifacts []Artifact) (Publication, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return Publication{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return Publication{}, err
	}
	if err := p.ensureStreamRows(ctx, tx, fence); err != nil {
		return Publication{}, err
	}
	publicationID := uuid.New()
	canonicalLSN, err := connector.CanonicalizeCheckpointPosition(transaction.Checkpoint.LSN)
	if err != nil {
		return Publication{}, err
	}
	positionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		return Publication{}, err
	}
	contentHash := publicationContentHash(artifacts)
	sourceTransactionID := fmt.Sprintf("%s:%d:%s:%s:%s", transaction.SourceLineageID, transaction.TransactionID, transaction.BeginLSN, transaction.CommitLSN, transaction.EndLSN)
	rootedBytes := int64(0)
	for _, artifact := range artifacts {
		var state string
		var length int64
		if err := tx.QueryRow(ctx, `SELECT state,encoded_length FROM artifact_objects WHERE artifact_id=$1 AND flow_incarnation_id=$2 FOR UPDATE`, artifact.ID, fence.FlowIncarnationID).Scan(&state, &length); err != nil {
			return Publication{}, err
		}
		if state != "verified" {
			return Publication{}, fmt.Errorf("artifact %s is %s, want verified", artifact.ID, state)
		}
		rootedBytes += length
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO artifact_publications (
  publication_id,flow_incarnation_id,source_lineage_id,source_transaction_id,source_xid,
  begin_lsn,commit_lsn,source_position,checkpoint_lsn,position_id,content_hash,
  generation,acquisition_id,lease_epoch,rooted_bytes
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, publicationID, fence.FlowIncarnationID, transaction.SourceLineageID, sourceTransactionID, transaction.TransactionID, transaction.BeginLSN, transaction.CommitLSN, transaction.EndLSN, canonicalLSN, positionID, contentHash, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, rootedBytes); err != nil {
		return Publication{}, fmt.Errorf("insert artifact publication: %w", err)
	}
	for index, artifact := range artifacts {
		if _, err := tx.Exec(ctx, `INSERT INTO artifact_publication_objects(publication_id,artifact_id,ordinal) VALUES($1,$2,$3)`, publicationID, artifact.ID, index); err != nil {
			return Publication{}, err
		}
		tag, err := tx.Exec(ctx, `UPDATE artifact_objects SET state='rooted',updated_at=clock_timestamp() WHERE artifact_id=$1 AND state='verified'`, artifact.ID)
		if err != nil || tag.RowsAffected() != 1 {
			return Publication{}, fmt.Errorf("root artifact %s: affected=%d err=%w", artifact.ID, tag.RowsAffected(), err)
		}
		tag, err = tx.Exec(ctx, `UPDATE artifact_quota_reservations SET converted_at=clock_timestamp() WHERE artifact_id=$1 AND converted_at IS NULL AND released_at IS NULL`, artifact.ID)
		if err != nil || tag.RowsAffected() != 1 {
			return Publication{}, fmt.Errorf("convert artifact reservation %s: affected=%d err=%w", artifact.ID, tag.RowsAffected(), err)
		}
	}
	for _, consumer := range p.config.Consumers {
		if _, err := tx.Exec(ctx, `
INSERT INTO artifact_deliveries(flow_incarnation_id,consumer_revision_id,publication_id,bytes)
VALUES($1,$2,$3,$4)`, fence.FlowIncarnationID, consumer, publicationID, rootedBytes); err != nil {
			return Publication{}, fmt.Errorf("queue artifact consumer %s: %w", consumer, err)
		}
	}
	tag, err := tx.Exec(ctx, `
UPDATE artifact_quota_accounts
SET reserved_bytes=reserved_bytes-$2,rooted_bytes=rooted_bytes+$2,updated_at=clock_timestamp()
WHERE flow_incarnation_id=$1 AND reserved_bytes >= $2`, fence.FlowIncarnationID, rootedBytes)
	if err != nil {
		return Publication{}, fmt.Errorf("convert artifact quota: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return Publication{}, errors.New("artifact quota conversion did not charge PostgreSQL account")
	}
	checkpoint := transaction.Checkpoint
	checkpoint.LSN = canonicalLSN
	metadata := make(map[string]string, len(checkpoint.Metadata)+1)
	for key, value := range checkpoint.Metadata {
		metadata[key] = value
	}
	metadata["artifact_publication_id"] = publicationID.String()
	checkpoint.Metadata = metadata
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return Publication{}, err
	}
	var currentLSN string
	err = tx.QueryRow(ctx, `SELECT lsn FROM authoritative_checkpoints WHERE flow_incarnation_id=$1 FOR UPDATE`, fence.FlowIncarnationID).Scan(&currentLSN)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if _, err := tx.Exec(ctx, `
INSERT INTO authoritative_checkpoints (
  flow_incarnation_id,flow_id,generation,acquisition_id,lease_epoch,lsn,metadata
) VALUES ($1,$2,$3,$4,$5,$6,$7)`, fence.FlowIncarnationID, fence.FlowID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, canonicalLSN, metadataJSON); err != nil {
			return Publication{}, fmt.Errorf("checkpoint artifact publication: %w", err)
		}
	case err != nil:
		return Publication{}, fmt.Errorf("load artifact checkpoint: %w", err)
	default:
		comparison, compareErr := connector.CompareCheckpointLSN(canonicalLSN, currentLSN)
		if compareErr != nil {
			return Publication{}, compareErr
		}
		if comparison < 0 {
			return Publication{}, fmt.Errorf("%w: current=%s artifact=%s", connector.ErrCheckpointRegression, currentLSN, canonicalLSN)
		}
		if _, err := tx.Exec(ctx, `
UPDATE authoritative_checkpoints
SET generation=$2,acquisition_id=$3,lease_epoch=$4,lsn=$5,metadata=$6,updated_at=clock_timestamp()
WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, canonicalLSN, metadataJSON); err != nil {
			return Publication{}, fmt.Errorf("advance artifact checkpoint: %w", err)
		}
	}
	tag, err = tx.Exec(ctx, `
INSERT INTO source_ack_intents (
  flow_incarnation_id,position_id,checkpoint_lsn,generation,acquisition_id,lease_epoch
) VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (flow_incarnation_id,position_id) DO UPDATE SET checkpoint_lsn=EXCLUDED.checkpoint_lsn
WHERE source_ack_intents.checkpoint_lsn=EXCLUDED.checkpoint_lsn`, fence.FlowIncarnationID, positionID, canonicalLSN, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch)
	if err != nil {
		return Publication{}, fmt.Errorf("authorize artifact source ack: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return Publication{}, fmt.Errorf("%w: artifact ACK intent conflicts", connector.ErrDeliveryConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return Publication{}, fmt.Errorf("commit artifact publication: %w", err)
	}
	telemetry.RecordArtifactTransition(ctx, "rooted", rootedBytes)
	return Publication{ID: publicationID, Artifacts: publicationArtifacts(artifacts), AckGrant: connector.AckGrant{Checkpoint: checkpoint, PositionID: positionID}}, nil
}

func publicationContentHash(artifacts []Artifact) string {
	hash := sha256.New()
	for _, artifact := range artifacts {
		_, _ = hash.Write([]byte(artifact.ID))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(artifact.LogicalContentHash))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(artifact.EncodedByteHash))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func publicationArtifacts(artifacts []Artifact) []Artifact {
	result := make([]Artifact, len(artifacts))
	copy(result, artifacts)
	for index := range result {
		result[index].Encoded = nil
	}
	return result
}

func (p *Publisher) ensureStreamRows(ctx context.Context, tx pgx.Tx, fence authority.RunFence) error {
	if _, err := tx.Exec(ctx, `
INSERT INTO artifact_streams (
  flow_incarnation_id,flow_id,hard_retained_bytes,backlog_count_high,backlog_bytes_high,backlog_age_high_seconds
) VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (flow_incarnation_id) DO NOTHING`, fence.FlowIncarnationID, fence.FlowID, p.config.HardRetainedBytes, p.config.BacklogCountHigh, p.config.BacklogBytesHigh, int64(p.config.BacklogAgeHigh/time.Second)); err != nil {
		return fmt.Errorf("ensure artifact stream: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO artifact_quota_accounts(flow_incarnation_id,hard_limit_bytes)
VALUES($1,$2)
ON CONFLICT (flow_incarnation_id) DO NOTHING`, fence.FlowIncarnationID, p.config.HardRetainedBytes); err != nil {
		return fmt.Errorf("ensure artifact quota: %w", err)
	}
	var flowID string
	var retained, countHigh, bytesHigh, ageHigh, quotaLimit int64
	if err := tx.QueryRow(ctx, `
SELECT stream.flow_id,stream.hard_retained_bytes,stream.backlog_count_high,
       stream.backlog_bytes_high,stream.backlog_age_high_seconds,quota.hard_limit_bytes
FROM artifact_streams AS stream
JOIN artifact_quota_accounts AS quota USING (flow_incarnation_id)
WHERE stream.flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&flowID, &retained, &countHigh, &bytesHigh, &ageHigh, &quotaLimit); err != nil {
		return fmt.Errorf("load artifact stream admission: %w", err)
	}
	if flowID != fence.FlowID || retained != p.config.HardRetainedBytes || quotaLimit != p.config.HardRetainedBytes || countHigh != p.config.BacklogCountHigh || bytesHigh != p.config.BacklogBytesHigh || ageHigh != int64(p.config.BacklogAgeHigh/time.Second) {
		return fmt.Errorf("%w: artifact stream configuration differs from PostgreSQL authority", connector.ErrDeliveryConflict)
	}
	return nil
}

func (p *Publisher) loadPublication(ctx context.Context, fence authority.RunFence, transaction connector.SourceTransaction, artifacts []Artifact) (Publication, bool, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return Publication{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return Publication{}, false, err
	}
	canonicalLSN, err := connector.CanonicalizeCheckpointPosition(transaction.Checkpoint.LSN)
	if err != nil {
		return Publication{}, false, err
	}
	expectedPositionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		return Publication{}, false, err
	}
	expectedTransactionID := fmt.Sprintf("%s:%d:%s:%s:%s", transaction.SourceLineageID, transaction.TransactionID, transaction.BeginLSN, transaction.CommitLSN, transaction.EndLSN)
	expectedContentHash := publicationContentHash(artifacts)
	var publicationID uuid.UUID
	var sourceLineage, sourceTransactionID, checkpointLSN, positionID, contentHash, authorizedLSN string
	var sourceXID int64
	var metadataJSON []byte
	err = tx.QueryRow(ctx, `
SELECT publication.publication_id,publication.source_lineage_id,publication.source_transaction_id,
       publication.source_xid,publication.checkpoint_lsn,publication.position_id,publication.content_hash,
       checkpoint.metadata,intent.checkpoint_lsn
FROM artifact_publications AS publication
JOIN authoritative_checkpoints AS checkpoint
  ON checkpoint.flow_incarnation_id=publication.flow_incarnation_id
 AND checkpoint.lsn=publication.checkpoint_lsn
JOIN source_ack_intents AS intent
  ON intent.flow_incarnation_id=publication.flow_incarnation_id
 AND intent.position_id=publication.position_id
WHERE publication.flow_incarnation_id=$1 AND publication.source_position=$2
FOR UPDATE`, fence.FlowIncarnationID, canonicalLSN).Scan(&publicationID, &sourceLineage, &sourceTransactionID, &sourceXID, &checkpointLSN, &positionID, &contentHash, &metadataJSON, &authorizedLSN)
	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		if queryErr := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM artifact_publications WHERE flow_incarnation_id=$1 AND source_position=$2)`, fence.FlowIncarnationID, canonicalLSN).Scan(&exists); queryErr != nil {
			return Publication{}, false, queryErr
		}
		if exists {
			return Publication{}, false, fmt.Errorf("%w: existing artifact publication is not the current authorized checkpoint", connector.ErrCheckpointRegression)
		}
		return Publication{}, false, nil
	}
	if err != nil {
		return Publication{}, false, err
	}
	if sourceLineage != transaction.SourceLineageID || sourceTransactionID != expectedTransactionID || sourceXID != int64(transaction.TransactionID) || checkpointLSN != canonicalLSN || positionID != expectedPositionID || contentHash != expectedContentHash || authorizedLSN != canonicalLSN {
		return Publication{}, false, fmt.Errorf("%w: existing artifact publication identity or ACK authorization differs", connector.ErrDeliveryConflict)
	}
	rows, err := tx.Query(ctx, `
SELECT object.artifact_id,object.logical_content_hash,object.encoded_byte_hash,
       object.bucket,object.object_key,object.version_id,object.checksum_sha256,
       object.encoded_length,object.encryption_mode,object.object_lock_evidence
FROM artifact_publication_objects AS item
JOIN artifact_objects AS object ON object.artifact_id=item.artifact_id
WHERE item.publication_id=$1 AND object.state='rooted'
ORDER BY item.ordinal`, publicationID)
	if err != nil {
		return Publication{}, false, err
	}
	var evidence []ObjectEvidence
	index := 0
	for rows.Next() {
		if index >= len(artifacts) {
			rows.Close()
			return Publication{}, false, fmt.Errorf("%w: publication has extra objects", connector.ErrDeliveryConflict)
		}
		var artifactID, logicalHash, encodedHash string
		var object ObjectEvidence
		if err := rows.Scan(&artifactID, &logicalHash, &encodedHash, &object.Bucket, &object.Key, &object.VersionID, &object.ChecksumSHA256, &object.Length, &object.EncryptionMode, &object.ObjectLock); err != nil {
			rows.Close()
			return Publication{}, false, err
		}
		expected := artifacts[index]
		if artifactID != expected.ID || logicalHash != expected.LogicalContentHash || encodedHash != expected.EncodedByteHash || object.ChecksumSHA256 != expected.ChecksumSHA256 || object.Length != int64(len(expected.Encoded)) {
			rows.Close()
			return Publication{}, false, fmt.Errorf("%w: rooted artifact %d differs", connector.ErrDeliveryConflict, index)
		}
		evidence = append(evidence, object)
		index++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return Publication{}, false, err
	}
	rows.Close()
	if index != len(artifacts) {
		return Publication{}, false, fmt.Errorf("%w: publication object count=%d expected=%d", connector.ErrDeliveryConflict, index, len(artifacts))
	}
	if err := tx.Commit(ctx); err != nil {
		return Publication{}, false, err
	}
	for _, object := range evidence {
		if err := p.objects.HeadVersion(ctx, object); err != nil {
			return Publication{}, false, fmt.Errorf("revalidate rooted artifact: %w", err)
		}
	}
	checkpoint := connector.Checkpoint{LSN: checkpointLSN}
	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &checkpoint.Metadata); err != nil {
			return Publication{}, false, fmt.Errorf("decode authoritative artifact checkpoint: %w", err)
		}
	}
	return Publication{ID: publicationID, Artifacts: publicationArtifacts(artifacts), AckGrant: connector.AckGrant{Checkpoint: checkpoint, PositionID: positionID}}, true, nil
}

// Catalog is the sole seam through which Iceberg and S3 Tables consume rooted
// canonical artifacts. S3 Tables implementations belong behind this Iceberg
// catalog abstraction; managed table files are never Wallaby GC roots.
type Catalog interface {
	Append(context.Context, string, uuid.UUID, []ObjectEvidence) (CatalogCommit, error)
	Reconcile(context.Context, string, uuid.UUID) (CatalogDisposition, CatalogCommit, error)
}

type CatalogCommit struct {
	SnapshotID  string
	ContentHash string
}

type CatalogDisposition uint8

const (
	CatalogIndeterminate CatalogDisposition = iota
	CatalogNotApplied
	CatalogApplied
)
