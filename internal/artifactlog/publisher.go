package artifactlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func dbBigintEqualsUint64(databaseValue int64, expected uint64) bool {
	return databaseValue >= 0 && strconv.FormatInt(databaseValue, 10) == strconv.FormatUint(expected, 10)
}

func publicationIdentityV2(incarnationID uuid.UUID, mappingFingerprint string, transaction connector.SourceTransaction, plan Plan) (uuid.UUID, error) {
	positionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		return uuid.Nil, err
	}
	digest := sha256.Sum256([]byte(strings.Join([]string{"wallaby.artifact-publication.v2", ProjectionIDV2, incarnationID.String(), mappingFingerprint, transaction.SourceLineageID, transaction.BeginLSN, transaction.CommitLSN, transaction.EndLSN, positionID, plan.ContentHash, plan.LogicalBatchID}, "\x00")))
	var id uuid.UUID
	copy(id[:], digest[:16])
	id[6] = (id[6] & 0x0f) | 0x50
	id[8] = (id[8] & 0x3f) | 0x80
	return id, nil
}

func consumerRevisionFingerprint(consumers []string) string {
	digest := sha256.Sum256([]byte(strings.Join(consumers, "\x00")))
	return hex.EncodeToString(digest[:])
}

var (
	ErrQuotaExceeded     = errors.New("artifact retained-byte quota exceeded")
	ErrBackpressure      = errors.New("artifact consumer backlog high-water mark exceeded")
	errPublicationExists = errors.New("artifact publication already exists")
)

// StreamConfig is durable admission for one canonical artifact stream.
type StreamConfig struct {
	ProjectionID             string
	MappingFingerprint       string
	HardRetainedBytes        int64
	BacklogCountHigh         int64
	BacklogBytesHigh         int64
	BacklogAgeHigh           time.Duration
	BackpressurePollInterval time.Duration
	Consumers                []string
}

// PublisherHooks exposes named deterministic crash boundaries. Production
// callers leave it unset; live-service tests can stop exactly between durable
// protocol steps without sleeps.
type PublisherHooks struct {
	Boundary func(context.Context, string) error
}

// PublisherOption configures optional publisher behavior.
type PublisherOption func(*Publisher)

// WithPublisherHooks installs deterministic failure injection hooks.
func WithPublisherHooks(hooks PublisherHooks) PublisherOption {
	return func(publisher *Publisher) { publisher.hooks = hooks }
}

// Publication is the rooted canonical transaction and source ACK grant.
type Publication struct {
	ID             uuid.UUID
	LogicalBatchID string
	Sequence       int64
	Artifacts      []Artifact
	AckGrant       connector.AckGrant
}

// Publisher owns PostgreSQL reservation/publication and immutable object I/O.
type Publisher struct {
	pool                *pgxpool.Pool
	objects             ObjectStore
	encoder             *Encoder
	config              StreamConfig
	consumerFingerprint string
	hooks               PublisherHooks
}

func NewPublisher(ctx context.Context, pool *pgxpool.Pool, objects ObjectStore, config StreamConfig, options ...PublisherOption) (*Publisher, error) {
	if pool == nil || objects == nil {
		return nil, errors.New("artifact PostgreSQL pool and object store are required")
	}
	if config.ProjectionID == "" {
		config.ProjectionID = ProjectionID
	}
	config.MappingFingerprint = strings.TrimSpace(config.MappingFingerprint)
	if config.ProjectionID != ProjectionID && config.ProjectionID != ProjectionIDV2 {
		return nil, fmt.Errorf("unsupported artifact projection %q", config.ProjectionID)
	}
	if (config.ProjectionID == ProjectionIDV2) != (config.MappingFingerprint != "") {
		return nil, errors.New("canonical v2 requires a mapping fingerprint and canonical v1 forbids one")
	}
	if config.ProjectionID == ProjectionIDV2 {
		if len(config.MappingFingerprint) != 64 || config.MappingFingerprint != strings.ToLower(config.MappingFingerprint) {
			return nil, errors.New("canonical v2 mapping fingerprint must be lowercase 64-hex")
		}
		if _, err := hex.DecodeString(config.MappingFingerprint); err != nil {
			return nil, errors.New("canonical v2 mapping fingerprint must be lowercase 64-hex")
		}
	}
	if config.HardRetainedBytes <= 0 || config.BacklogCountHigh <= 0 || config.BacklogBytesHigh <= 0 {
		return nil, errors.New("positive artifact retained and backlog limits are required")
	}
	if config.BacklogAgeHigh <= 0 {
		config.BacklogAgeHigh = 24 * time.Hour
	}
	if config.BackpressurePollInterval <= 0 {
		config.BackpressurePollInterval = time.Second
	}
	configuredConsumers := append([]string(nil), config.Consumers...)
	consumerSet := make(map[string]struct{}, len(configuredConsumers))
	config.Consumers = config.Consumers[:0]
	for _, consumer := range configuredConsumers {
		consumer = strings.TrimSpace(consumer)
		if consumer == "" {
			return nil, errors.New("artifact consumer revision cannot be empty")
		}
		if _, exists := consumerSet[consumer]; exists {
			continue
		}
		consumerSet[consumer] = struct{}{}
		config.Consumers = append(config.Consumers, consumer)
	}
	sort.Strings(config.Consumers)
	consumerFingerprint := consumerRevisionFingerprint(config.Consumers)
	if err := runMigrations(ctx, pool); err != nil {
		return nil, err
	}
	publisher := &Publisher{
		pool: pool, objects: objects, encoder: NewEncoder(), config: config,
		consumerFingerprint: consumerFingerprint,
	}
	for _, option := range options {
		option(publisher)
	}
	return publisher, nil
}

// Publish encodes locally, reserves exact bytes before PUT, reconciles exact S3
// versions, then roots publication/checkpoint/ACK intent in one transaction.
func (p *Publisher) Publish(ctx context.Context, fence authority.RunFence, transaction connector.SourceTransaction) (Publication, error) {
	transaction, err := canonicalSourceTransaction(transaction)
	if err != nil {
		return Publication{}, err
	}
	var plan Plan
	if p.config.ProjectionID == ProjectionIDV2 {
		plan, err = p.encoder.PlanMappedTransaction(ctx, fence.FlowIncarnationID, p.config.MappingFingerprint, transaction)
	} else {
		plan, err = p.encoder.PlanTransaction(ctx, fence.FlowIncarnationID, transaction)
	}
	if err != nil {
		return Publication{}, err
	}
	if existing, ok, err := p.loadPublication(ctx, fence, transaction, plan); err != nil {
		return Publication{}, err
	} else if ok {
		return existing, nil
	}
	if err := p.reserve(ctx, fence, plan.Artifacts, len(plan.Artifacts)+len(plan.Barriers) > 0); err != nil {
		return Publication{}, err
	}
	if err := p.reach(ctx, "after_upload_intent_commit"); err != nil {
		return Publication{}, err
	}
	for _, artifact := range plan.Artifacts {
		if err := p.uploadAndVerify(ctx, fence, artifact); err != nil {
			return Publication{}, err
		}
	}
	if err := p.reach(ctx, "before_publication_transaction"); err != nil {
		return Publication{}, err
	}
	publication, err := p.root(ctx, fence, transaction, plan)
	if errors.Is(err, errPublicationExists) {
		if existing, ok, loadErr := p.loadPublication(ctx, fence, transaction, plan); loadErr != nil {
			return Publication{}, loadErr
		} else if ok {
			return existing, nil
		}
	}
	return publication, err
}

// Append is the small runner seam for ACK_POLICY_MATERIALIZED.
func (p *Publisher) Append(ctx context.Context, fence connector.RunFence, transaction connector.SourceTransaction) (connector.AckGrant, error) {
	publication, err := p.Publish(ctx, fence, transaction)
	if err != nil {
		return connector.AckGrant{}, err
	}
	return publication.AckGrant, nil
}

func (p *Publisher) reach(ctx context.Context, boundary string) error {
	if p.hooks.Boundary == nil {
		return nil
	}
	if err := p.hooks.Boundary(ctx, boundary); err != nil {
		return fmt.Errorf("artifact boundary %s: %w", boundary, err)
	}
	return nil
}

// RecomputeQuota restores durable accounting from PostgreSQL roots and active
// reservations after a crash. S3 listings are not consulted.
// Recover restores PostgreSQL-derived quota state before a worker opens the
// source. S3 is never consulted for progress or backlog authority.
func (p *Publisher) Recover(ctx context.Context, fence connector.RunFence) error {
	return p.RecomputeQuota(ctx, fence)
}

// RestoreCheckpoint proves that the authoritative checkpoint still names a
// current publication, ACK intent, active PostgreSQL roots, and intact exact
// object versions before a restarted worker opens or acknowledges its source.
func (p *Publisher) RestoreCheckpoint(ctx context.Context, fence connector.RunFence, checkpoint connector.Checkpoint) (connector.AckGrant, error) {
	publicationID, err := uuid.Parse(strings.TrimSpace(checkpoint.Metadata["artifact_publication_id"]))
	if err != nil {
		return connector.AckGrant{}, fmt.Errorf("restore artifact publication id: %w", err)
	}
	canonicalLSN, err := connector.CanonicalizeCheckpointPosition(checkpoint.LSN)
	if err != nil {
		return connector.AckGrant{}, err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return connector.AckGrant{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return connector.AckGrant{}, err
	}
	var publicationLSN, positionID, authoritativeLSN, authorizedLSN, publicationProjection, mappingFingerprint string
	var publicationMetadataJSON, authoritativeMetadataJSON []byte
	if err := tx.QueryRow(ctx, `
SELECT publication.checkpoint_lsn,publication.position_id,publication.checkpoint_metadata,
       checkpoint.lsn,checkpoint.metadata,intent.checkpoint_lsn,
       publication.projection_id,publication.mapping_fingerprint
FROM artifact_publications AS publication
JOIN authoritative_checkpoints AS checkpoint
  ON checkpoint.flow_incarnation_id=publication.flow_incarnation_id
JOIN source_ack_intents AS intent
  ON intent.flow_incarnation_id=publication.flow_incarnation_id
 AND intent.position_id=publication.position_id
WHERE publication.flow_incarnation_id=$1 AND publication.publication_id=$2
FOR UPDATE OF publication,checkpoint,intent`, fence.FlowIncarnationID, publicationID).Scan(
		&publicationLSN, &positionID, &publicationMetadataJSON,
		&authoritativeLSN, &authoritativeMetadataJSON, &authorizedLSN, &publicationProjection, &mappingFingerprint,
	); err != nil {
		return connector.AckGrant{}, fmt.Errorf("load restored artifact publication: %w", err)
	}
	var publicationMetadata, authoritativeMetadata map[string]string
	if err := json.Unmarshal(publicationMetadataJSON, &publicationMetadata); err != nil {
		return connector.AckGrant{}, fmt.Errorf("decode artifact publication checkpoint: %w", err)
	}
	if err := json.Unmarshal(authoritativeMetadataJSON, &authoritativeMetadata); err != nil {
		return connector.AckGrant{}, fmt.Errorf("decode authoritative artifact checkpoint: %w", err)
	}
	if publicationLSN != canonicalLSN || authoritativeLSN != canonicalLSN || authorizedLSN != canonicalLSN || publicationProjection != p.config.ProjectionID || mappingFingerprint != p.config.MappingFingerprint ||
		publicationMetadata["artifact_publication_id"] != publicationID.String() ||
		!maps.Equal(publicationMetadata, authoritativeMetadata) || !maps.Equal(authoritativeMetadata, checkpoint.Metadata) {
		return connector.AckGrant{}, fmt.Errorf("%w: restored artifact publication, checkpoint, or ACK intent differs", connector.ErrDeliveryConflict)
	}
	expectedPositionID, err := connector.CheckpointPositionID(connector.Checkpoint{LSN: canonicalLSN})
	if err != nil {
		return connector.AckGrant{}, err
	}
	if positionID != expectedPositionID {
		return connector.AckGrant{}, fmt.Errorf("%w: restored artifact position %s differs from %s", connector.ErrDeliveryConflict, positionID, expectedPositionID)
	}
	rows, err := tx.Query(ctx, `
SELECT root.release_marked_at IS NULL,root.released_at IS NULL,object.state,
       object.bucket,object.object_key,object.version_id,object.checksum_sha256,
       object.encoded_length,object.encryption_mode,object.object_lock_evidence,
       object.projection_id,object.mapping_fingerprint
FROM artifact_publication_objects AS root
JOIN artifact_objects AS object ON object.artifact_id=root.artifact_id
WHERE root.publication_id=$1
ORDER BY root.ordinal`, publicationID)
	if err != nil {
		return connector.AckGrant{}, err
	}
	var evidence []ObjectEvidence
	for rows.Next() {
		var activeMark, unreleased bool
		var state string
		var object ObjectEvidence
		if err := rows.Scan(
			&activeMark, &unreleased, &state, &object.Bucket, &object.Key,
			&object.VersionID, &object.ChecksumSHA256, &object.Length,
			&object.EncryptionMode, &object.ObjectLock, &object.ProjectionID, &object.MappingFingerprint,
		); err != nil {
			rows.Close()
			return connector.AckGrant{}, err
		}
		if !activeMark || !unreleased || state != "rooted" || object.VersionID == "" || object.VersionID == "null" || object.ProjectionID != p.config.ProjectionID || object.MappingFingerprint != p.config.MappingFingerprint {
			rows.Close()
			return connector.AckGrant{}, fmt.Errorf("%w: restored artifact publication contains an inactive or non-rooted object", connector.ErrDeliveryConflict)
		}
		evidence = append(evidence, object)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return connector.AckGrant{}, err
	}
	rows.Close()
	if len(evidence) > 0 {
		var activeAckRoots int
		if err := tx.QueryRow(ctx, `
SELECT count(*) FROM source_ack_retention_roots
WHERE flow_incarnation_id=$1 AND position_id=$2
  AND root_kind='artifact_publication' AND root_id=$3 AND released_at IS NULL`,
			fence.FlowIncarnationID, positionID, publicationID.String(),
		).Scan(&activeAckRoots); err != nil {
			return connector.AckGrant{}, err
		}
		if activeAckRoots != 1 {
			return connector.AckGrant{}, fmt.Errorf("%w: restored artifact publication has %d active ACK roots", connector.ErrDeliveryConflict, activeAckRoots)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return connector.AckGrant{}, err
	}
	for _, object := range evidence {
		if _, err := p.objects.HeadVersion(ctx, object); err != nil {
			return connector.AckGrant{}, fmt.Errorf("revalidate restored artifact version: %w", err)
		}
	}
	return connector.AckGrant{
		Checkpoint: connector.Checkpoint{LSN: canonicalLSN, Metadata: authoritativeMetadata},
		PositionID: positionID,
	}, nil
}

// WaitForReadAdmission enforces restored byte, batch-count, and age high-water
// marks before every source read. The producer heartbeat continues while this
// call waits, and cancellation remains immediate.
func (p *Publisher) WaitForReadAdmission(ctx context.Context, fence connector.RunFence) error {
	for {
		err := p.checkReadAdmission(ctx, fence)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrBackpressure) {
			return err
		}
		telemetry.RecordArtifactTransition(ctx, "backpressure", 0)
		timer := time.NewTimer(p.config.BackpressurePollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (p *Publisher) checkReadAdmission(ctx context.Context, fence authority.RunFence) error {
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
	var backlogCount, backlogBytes int64
	var backlogAgeSeconds float64
	if err := tx.QueryRow(ctx, `
SELECT count(*),COALESCE(sum(bytes),0),
       COALESCE(EXTRACT(EPOCH FROM clock_timestamp()-min(created_at)),0)
FROM artifact_deliveries
WHERE flow_incarnation_id=$1 AND delivered_at IS NULL`, fence.FlowIncarnationID).Scan(&backlogCount, &backlogBytes, &backlogAgeSeconds); err != nil {
		return fmt.Errorf("read restored artifact backlog: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	age := time.Duration(backlogAgeSeconds * float64(time.Second))
	if backlogCount >= p.config.BacklogCountHigh || backlogBytes >= p.config.BacklogBytesHigh || (backlogCount > 0 && age >= p.config.BacklogAgeHigh) {
		return fmt.Errorf("%w: count=%d bytes=%d age=%s", ErrBackpressure, backlogCount, backlogBytes, age)
	}
	return nil
}

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
SELECT COALESCE(sum(object.encoded_length),0)
FROM artifact_objects AS object
JOIN artifact_publication_objects AS root ON root.artifact_id=object.artifact_id
WHERE object.flow_incarnation_id=$1
  AND root.released_at IS NULL`, fence.FlowIncarnationID).Scan(&rooted); err != nil {
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

func (p *Publisher) reserve(ctx context.Context, fence authority.RunFence, artifacts []Artifact, queuesDelivery bool) error {
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
	projectedCount := backlogCount
	projectedBytes := backlogBytes
	if queuesDelivery {
		projectedCount += int64(len(p.config.Consumers))
		projectedBytes += publicationBytes * int64(len(p.config.Consumers))
	}
	if projectedCount > p.config.BacklogCountHigh || projectedBytes > p.config.BacklogBytesHigh || time.Duration(backlogAgeSeconds*float64(time.Second)) >= p.config.BacklogAgeHigh {
		return fmt.Errorf("%w: projected count=%d bytes=%d age=%s", ErrBackpressure, projectedCount, projectedBytes, time.Duration(backlogAgeSeconds*float64(time.Second)))
	}

	newBytes := int64(0)
	for _, artifact := range artifacts {
		if _, err := tx.Exec(ctx, `
INSERT INTO canonical_schemas (schema_id,projection_id,mapping_fingerprint,schema_json)
VALUES ($1,$2,$3,$4)
ON CONFLICT (schema_id) DO NOTHING`, artifact.SchemaID, p.config.ProjectionID, p.config.MappingFingerprint, artifact.SchemaJSON); err != nil {
			return fmt.Errorf("store canonical schema: %w", err)
		}
		tag, err := tx.Exec(ctx, `
INSERT INTO artifact_objects (
  artifact_id,flow_incarnation_id,source_position,fragment_ordinal,logical_batch_id,
  namespace,table_name,partition_value,shard,first_record_ordinal,record_count,schema_id,
  logical_content_hash,encoded_byte_hash,encoded_length,bucket,object_key,
  checksum_sha256,encoding,state,projection_id,mapping_fingerprint
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,'parquet','reserved',$19,$20)
ON CONFLICT (artifact_id) DO NOTHING`,
			artifact.ID, fence.FlowIncarnationID, artifact.SourcePosition, artifact.FragmentOrdinal,
			artifact.LogicalBatchID, artifact.Namespace, artifact.Table, artifact.Partition, artifact.Shard,
			artifact.FirstRecordOrdinal, artifact.RecordCount, artifact.SchemaID, artifact.LogicalContentHash,
			artifact.EncodedByteHash, len(artifact.Encoded), p.objects.Bucket(), artifact.ObjectKey, artifact.ChecksumSHA256, p.config.ProjectionID, p.config.MappingFingerprint,
		)
		if err != nil {
			return fmt.Errorf("reserve artifact object: %w", err)
		}
		if tag.RowsAffected() == 0 {
			var state, logicalBatchID, namespace, table, partition, schemaID, logicalHash, encodedHash, bucket, key, projectionID, mappingFingerprint string
			var shard, firstOrdinal, recordCount, length int64
			if err := tx.QueryRow(ctx, `
SELECT state,logical_batch_id,namespace,table_name,partition_value,shard,first_record_ordinal,
       record_count,schema_id,logical_content_hash,encoded_byte_hash,encoded_length,bucket,object_key,
       projection_id,mapping_fingerprint
FROM artifact_objects WHERE artifact_id=$1 AND flow_incarnation_id=$2 FOR UPDATE`, artifact.ID, fence.FlowIncarnationID).Scan(
				&state, &logicalBatchID, &namespace, &table, &partition, &shard, &firstOrdinal, &recordCount,
				&schemaID, &logicalHash, &encodedHash, &length, &bucket, &key, &projectionID, &mappingFingerprint,
			); err != nil {
				return err
			}
			if logicalBatchID != artifact.LogicalBatchID || namespace != artifact.Namespace || table != artifact.Table ||
				partition != artifact.Partition || shard != int64(artifact.Shard) || !dbBigintEqualsUint64(firstOrdinal, artifact.FirstRecordOrdinal) ||
				!dbBigintEqualsUint64(recordCount, artifact.RecordCount) || schemaID != artifact.SchemaID || logicalHash != artifact.LogicalContentHash ||
				encodedHash != artifact.EncodedByteHash || length != int64(len(artifact.Encoded)) || bucket != p.objects.Bucket() || key != artifact.ObjectKey || projectionID != p.config.ProjectionID || mappingFingerprint != p.config.MappingFingerprint {
				return fmt.Errorf("%w: artifact identity %s was reused with different content", connector.ErrDeliveryConflict, artifact.ID)
			}
			if state == "deleted" {
				tag, err := tx.Exec(ctx, `
UPDATE artifact_objects
SET state='reserved',version_id=NULL,encryption_mode='',object_lock_evidence='',updated_at=clock_timestamp()
WHERE artifact_id=$1 AND flow_incarnation_id=$2 AND state='deleted'
  AND NOT EXISTS (SELECT 1 FROM artifact_gc_claims WHERE artifact_id=$1)
  AND NOT EXISTS (
    SELECT 1 FROM artifact_publication_objects
    WHERE artifact_id=$1 AND release_marked_at IS NULL AND released_at IS NULL
  )`, artifact.ID, fence.FlowIncarnationID)
				if err != nil || tag.RowsAffected() != 1 {
					return fmt.Errorf("reopen swept artifact %s: affected=%d err=%w", artifact.ID, tag.RowsAffected(), err)
				}
				var reopenedBytes int64
				if err := tx.QueryRow(ctx, `
UPDATE artifact_quota_reservations
SET released_at=NULL
WHERE artifact_id=$1 AND flow_incarnation_id=$2
  AND converted_at IS NULL AND released_at IS NOT NULL
RETURNING bytes`, artifact.ID, fence.FlowIncarnationID).Scan(&reopenedBytes); err != nil {
					return fmt.Errorf("restore swept artifact reservation %s: %w", artifact.ID, err)
				}
				if reopenedBytes != length {
					return fmt.Errorf("%w: swept artifact reservation bytes=%d object=%d", connector.ErrDeliveryConflict, reopenedBytes, length)
				}
				newBytes += reopenedBytes
				state = "reserved"
			}
			if state != "rooted" {
				if _, err := tx.Exec(ctx, `
INSERT INTO artifact_upload_attempts (attempt_id,artifact_id,generation,acquisition_id,lease_epoch)
VALUES ($1,$2,$3,$4,$5)`, uuid.New(), artifact.ID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch); err != nil {
					return fmt.Errorf("prepare resumed artifact upload attempt: %w", err)
				}
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
		_, err := p.objects.HeadVersion(ctx, evidence)
		return err
	}
	if state == "reserved" {
		evidence, err = p.putAndReconcile(ctx, artifact)
		if err != nil {
			return err
		}
		if err := p.reach(ctx, "after_object_put"); err != nil {
			return err
		}
		if err := p.recordUploaded(ctx, fence, artifact, evidence); err != nil {
			return err
		}
		if err := p.reach(ctx, "after_upload_evidence"); err != nil {
			return err
		}
	}
	evidence, err = p.objects.HeadVersion(ctx, evidence)
	if err != nil {
		return err
	}
	if err := p.markVerified(ctx, fence, artifact.ID, evidence); err != nil {
		return err
	}
	if err := p.reach(ctx, "after_object_verified"); err != nil {
		return err
	}
	telemetry.RecordArtifactTransition(ctx, "verified", evidence.Length)
	return nil
}

func (p *Publisher) putAndReconcile(ctx context.Context, artifact Artifact) (ObjectEvidence, error) {
	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		evidence, putErr := p.objects.PutImmutable(ctx, artifact.ObjectKey, artifact.Encoded, artifact.EncodedByteHash, p.config.ProjectionID, p.config.MappingFingerprint)
		if putErr == nil {
			return evidence, nil
		}
		evidence, reconcileErr := p.objects.ReconcileVersion(ctx, artifact.ObjectKey, artifact.EncodedByteHash, int64(len(artifact.Encoded)), p.config.ProjectionID, p.config.MappingFingerprint)
		if reconcileErr == nil {
			return evidence, nil
		}
		if !errors.Is(reconcileErr, ErrObjectNotFound) || attempt == maxAttempts {
			return ObjectEvidence{}, fmt.Errorf("immutable artifact PUT and reconciliation failed: %w", errors.Join(putErr, reconcileErr))
		}
		timer := time.NewTimer(time.Duration(attempt) * 25 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ObjectEvidence{}, ctx.Err()
		case <-timer.C:
		}
	}
	return ObjectEvidence{}, ErrObjectIndeterminate
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
SELECT state,bucket,object_key,version_id,checksum_sha256,encoded_length,encryption_mode,object_lock_evidence,projection_id,mapping_fingerprint
FROM artifact_objects WHERE artifact_id=$1 AND flow_incarnation_id=$2`, artifactID, fence.FlowIncarnationID).Scan(&state, &evidence.Bucket, &evidence.Key, &versionID, &evidence.ChecksumSHA256, &evidence.Length, &evidence.EncryptionMode, &evidence.ObjectLock, &evidence.ProjectionID, &evidence.MappingFingerprint); err != nil {
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
	if _, err := tx.Exec(ctx, `
UPDATE artifact_upload_attempts
SET attempt_state='failed',completed_at=clock_timestamp(),last_error='superseded by fenced exact-version reconciliation'
WHERE artifact_id=$1 AND attempt_state='prepared'
  AND (generation<>$2 OR acquisition_id<>$3 OR lease_epoch<>$4)`, artifact.ID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch); err != nil {
		return fmt.Errorf("supersede stale artifact upload attempts: %w", err)
	}
	if _, err := tx.Exec(ctx, `
UPDATE artifact_upload_attempts SET attempt_state='uploaded'
WHERE artifact_id=$1 AND generation=$2 AND acquisition_id=$3 AND lease_epoch=$4
  AND attempt_state='prepared'`, artifact.ID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch); err != nil {
		return fmt.Errorf("mark artifact upload attempt: %w", err)
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
SET state='verified',encryption_mode=$6,object_lock_evidence=$7,updated_at=clock_timestamp()
WHERE artifact_id=$1 AND flow_incarnation_id=$2
  AND state IN ('uploaded','verified')
  AND version_id=$3 AND checksum_sha256=$4 AND encoded_length=$5`, artifactID, fence.FlowIncarnationID, evidence.VersionID, evidence.ChecksumSHA256, evidence.Length, evidence.EncryptionMode, evidence.ObjectLock)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: artifact verification evidence differs", connector.ErrDeliveryConflict)
	}
	if _, err := tx.Exec(ctx, `
UPDATE artifact_upload_attempts
SET attempt_state='verified',completed_at=clock_timestamp()
WHERE artifact_id=$1 AND generation=$2 AND acquisition_id=$3 AND lease_epoch=$4
  AND attempt_state IN ('prepared','uploaded')`, artifactID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch); err != nil {
		return fmt.Errorf("complete artifact upload attempt: %w", err)
	}
	return tx.Commit(ctx)
}

func (p *Publisher) root(ctx context.Context, fence authority.RunFence, transaction connector.SourceTransaction, plan Plan) (Publication, error) {
	artifacts := plan.Artifacts
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
	var sequence int64
	if err := tx.QueryRow(ctx, `
SELECT next_publication_sequence FROM artifact_streams
WHERE flow_incarnation_id=$1 FOR UPDATE`, fence.FlowIncarnationID).Scan(&sequence); err != nil {
		return Publication{}, fmt.Errorf("lock artifact publication stream: %w", err)
	}
	var publicationExists bool
	if err := tx.QueryRow(ctx, `
SELECT EXISTS(SELECT 1 FROM artifact_publications WHERE flow_incarnation_id=$1 AND source_position=$2)`,
		fence.FlowIncarnationID, transaction.EndLSN,
	).Scan(&publicationExists); err != nil {
		return Publication{}, err
	}
	if publicationExists {
		return Publication{}, errPublicationExists
	}

	publicationID := uuid.New()
	if p.config.ProjectionID == ProjectionIDV2 {
		publicationID, err = publicationIdentityV2(fence.FlowIncarnationID, p.config.MappingFingerprint, transaction, plan)
		if err != nil {
			return Publication{}, err
		}
	}
	canonicalLSN, err := connector.CanonicalizeCheckpointPosition(transaction.Checkpoint.LSN)
	if err != nil {
		return Publication{}, err
	}
	positionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		return Publication{}, err
	}
	sourceTransactionID := fmt.Sprintf("%s:%d:%s:%s:%s", transaction.SourceLineageID, transaction.TransactionID, transaction.BeginLSN, transaction.CommitLSN, transaction.EndLSN)
	checkpoint := transaction.Checkpoint
	checkpoint.LSN = canonicalLSN
	metadata := make(map[string]string, len(checkpoint.Metadata)+3)
	for key, value := range checkpoint.Metadata {
		metadata[key] = value
	}
	metadata["artifact_publication_id"] = publicationID.String()
	metadata["artifact_logical_batch_id"] = plan.LogicalBatchID
	metadata["artifact_publication_sequence"] = fmt.Sprintf("%d", sequence)
	checkpoint.Metadata = metadata
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return Publication{}, err
	}

	rootedBytes := int64(0)
	for _, artifact := range artifacts {
		var state string
		var length int64
		var claimed bool
		if err := tx.QueryRow(ctx, `
SELECT object.state,object.encoded_length,
       EXISTS(SELECT 1 FROM artifact_gc_claims AS claim WHERE claim.artifact_id=object.artifact_id)
FROM artifact_objects AS object
WHERE object.artifact_id=$1 AND object.flow_incarnation_id=$2
FOR UPDATE`, artifact.ID, fence.FlowIncarnationID).Scan(&state, &length, &claimed); err != nil {
			return Publication{}, err
		}
		if state != "verified" || claimed {
			return Publication{}, fmt.Errorf("artifact %s cannot be rooted from state=%s claimed=%t", artifact.ID, state, claimed)
		}
		rootedBytes += length
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO artifact_publications (
  publication_id,flow_incarnation_id,source_lineage_id,source_transaction_id,source_xid,
  begin_lsn,commit_lsn,source_position,checkpoint_lsn,position_id,content_hash,
  generation,acquisition_id,lease_epoch,rooted_bytes,logical_batch_id,sequence,checkpoint_metadata,
  projection_id,mapping_fingerprint
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`,
		publicationID, fence.FlowIncarnationID, transaction.SourceLineageID, sourceTransactionID,
		transaction.TransactionID, transaction.BeginLSN, transaction.CommitLSN, transaction.EndLSN,
		canonicalLSN, positionID, plan.ContentHash, fence.Generation, fence.AcquisitionID,
		fence.LeaseEpoch, rootedBytes, plan.LogicalBatchID, sequence, metadataJSON, p.config.ProjectionID, p.config.MappingFingerprint,
	); err != nil {
		return Publication{}, fmt.Errorf("insert artifact publication: %w", err)
	}
	if len(artifacts) > 0 {
		if _, err := tx.Exec(ctx, `
INSERT INTO source_ack_retention_roots(flow_incarnation_id,position_id,root_kind,root_id)
VALUES($1,$2,'artifact_publication',$3)`, fence.FlowIncarnationID, positionID, publicationID.String()); err != nil {
			return Publication{}, fmt.Errorf("retain artifact source ACK root: %w", err)
		}
	}
	for index, artifact := range artifacts {
		if _, err := tx.Exec(ctx, `
INSERT INTO artifact_publication_objects(publication_id,artifact_id,ordinal)
VALUES($1,$2,$3)`, publicationID, artifact.ID, index); err != nil {
			return Publication{}, err
		}
		tag, err := tx.Exec(ctx, `
UPDATE artifact_objects SET state='rooted',updated_at=clock_timestamp()
WHERE artifact_id=$1 AND flow_incarnation_id=$2 AND state='verified'
  AND NOT EXISTS (SELECT 1 FROM artifact_gc_claims WHERE artifact_id=$1)`, artifact.ID, fence.FlowIncarnationID)
		if err != nil || tag.RowsAffected() != 1 {
			return Publication{}, fmt.Errorf("root artifact %s: affected=%d err=%w", artifact.ID, tag.RowsAffected(), err)
		}
		tag, err = tx.Exec(ctx, `
UPDATE artifact_quota_reservations SET converted_at=clock_timestamp()
WHERE artifact_id=$1 AND converted_at IS NULL AND released_at IS NULL`, artifact.ID)
		if err != nil || tag.RowsAffected() != 1 {
			return Publication{}, fmt.Errorf("convert artifact reservation %s: affected=%d err=%w", artifact.ID, tag.RowsAffected(), err)
		}
	}
	for index, barrier := range plan.Barriers {
		if _, err := tx.Exec(ctx, `
INSERT INTO artifact_barriers (
  publication_id,ordinal,fragment_ordinal,record_ordinal,kind,namespace,table_name,
  schema_id,ddl,ddl_plan,content_hash
) VALUES ($1,$2,$3,$4,$5,$6,$7,NULLIF($8,''),$9,COALESCE($10,''::BYTEA),$11)`,
			publicationID, index, barrier.FragmentOrdinal, barrier.RecordOrdinal, barrier.Kind,
			barrier.Namespace, barrier.Table, barrier.SchemaID, barrier.DDL, barrier.DDLPlan, barrier.ContentHash,
		); err != nil {
			return Publication{}, fmt.Errorf("root artifact barrier %d: %w", index, err)
		}
	}
	if len(artifacts)+len(plan.Barriers) > 0 {
		for _, consumer := range p.config.Consumers {
			if _, err := tx.Exec(ctx, `
INSERT INTO artifact_deliveries(flow_incarnation_id,consumer_revision_id,publication_id,bytes)
VALUES($1,$2,$3,$4)`, fence.FlowIncarnationID, consumer, publicationID, rootedBytes); err != nil {
				return Publication{}, fmt.Errorf("queue artifact consumer %s: %w", consumer, err)
			}
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

	var currentLSN string
	err = tx.QueryRow(ctx, `
SELECT lsn FROM authoritative_checkpoints WHERE flow_incarnation_id=$1 FOR UPDATE`, fence.FlowIncarnationID).Scan(&currentLSN)
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
ON CONFLICT (flow_incarnation_id,position_id) DO UPDATE SET
  checkpoint_lsn=EXCLUDED.checkpoint_lsn,generation=EXCLUDED.generation,
  acquisition_id=EXCLUDED.acquisition_id,lease_epoch=EXCLUDED.lease_epoch
WHERE source_ack_intents.checkpoint_lsn=EXCLUDED.checkpoint_lsn`,
		fence.FlowIncarnationID, positionID, canonicalLSN, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch,
	)
	if err != nil {
		return Publication{}, fmt.Errorf("authorize artifact source ack: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return Publication{}, fmt.Errorf("%w: artifact ACK intent conflicts", connector.ErrDeliveryConflict)
	}
	tag, err = tx.Exec(ctx, `
UPDATE artifact_streams SET next_publication_sequence=$2
WHERE flow_incarnation_id=$1 AND next_publication_sequence=$3`, fence.FlowIncarnationID, sequence+1, sequence)
	if err != nil {
		return Publication{}, fmt.Errorf("advance artifact publication sequence: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return Publication{}, errors.New("artifact publication sequence changed while locked")
	}
	if err := p.reach(ctx, "before_publication_commit"); err != nil {
		return Publication{}, err
	}
	// The hook may block long enough for takeover. Revalidate in the same
	// transaction immediately before commit so stale work changes no root,
	// delivery, quota, checkpoint, or ACK row.
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return Publication{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Publication{}, fmt.Errorf("commit artifact publication: %w", err)
	}
	if err := p.reach(ctx, "after_publication_commit"); err != nil {
		return Publication{}, err
	}
	telemetry.RecordArtifactTransition(ctx, "rooted", rootedBytes)
	return Publication{
		ID: publicationID, LogicalBatchID: plan.LogicalBatchID, Sequence: sequence,
		Artifacts: publicationArtifacts(artifacts),
		AckGrant:  connector.AckGrant{Checkpoint: checkpoint, PositionID: positionID},
	}, nil
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
  flow_incarnation_id,flow_id,hard_retained_bytes,backlog_count_high,backlog_bytes_high,
  backlog_age_high_seconds,projection_id,mapping_fingerprint,consumer_fingerprint
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT (flow_incarnation_id) DO NOTHING`, fence.FlowIncarnationID, fence.FlowID, p.config.HardRetainedBytes, p.config.BacklogCountHigh, p.config.BacklogBytesHigh, int64(p.config.BacklogAgeHigh/time.Second), p.config.ProjectionID, p.config.MappingFingerprint, p.consumerFingerprint); err != nil {
		return fmt.Errorf("ensure artifact stream: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO artifact_quota_accounts(flow_incarnation_id,hard_limit_bytes)
VALUES($1,$2)
ON CONFLICT (flow_incarnation_id) DO NOTHING`, fence.FlowIncarnationID, p.config.HardRetainedBytes); err != nil {
		return fmt.Errorf("ensure artifact quota: %w", err)
	}
	var flowID, projectionID, mappingFingerprint, consumerFingerprint string
	var retained, countHigh, bytesHigh, ageHigh, quotaLimit int64
	if err := tx.QueryRow(ctx, `
SELECT stream.flow_id,stream.projection_id,stream.mapping_fingerprint,stream.consumer_fingerprint,
       stream.hard_retained_bytes,stream.backlog_count_high,
       stream.backlog_bytes_high,stream.backlog_age_high_seconds,quota.hard_limit_bytes
FROM artifact_streams AS stream
JOIN artifact_quota_accounts AS quota USING (flow_incarnation_id)
WHERE stream.flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&flowID, &projectionID, &mappingFingerprint, &consumerFingerprint, &retained, &countHigh, &bytesHigh, &ageHigh, &quotaLimit); err != nil {
		return fmt.Errorf("load artifact stream admission: %w", err)
	}
	if flowID != fence.FlowID || projectionID != p.config.ProjectionID || mappingFingerprint != p.config.MappingFingerprint || consumerFingerprint != p.consumerFingerprint || retained != p.config.HardRetainedBytes || quotaLimit != p.config.HardRetainedBytes || countHigh != p.config.BacklogCountHigh || bytesHigh != p.config.BacklogBytesHigh || ageHigh != int64(p.config.BacklogAgeHigh/time.Second) {
		return fmt.Errorf("%w: artifact stream configuration differs from PostgreSQL authority", connector.ErrDeliveryConflict)
	}
	return nil
}

func (p *Publisher) loadPublication(ctx context.Context, fence authority.RunFence, transaction connector.SourceTransaction, plan Plan) (Publication, bool, error) {
	artifacts := plan.Artifacts
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
	expectedContentHash := plan.ContentHash
	var publicationID uuid.UUID
	var sourceLineage, sourceTransactionID, checkpointLSN, positionID, contentHash, authorizedLSN, logicalBatchID, projectionID, mappingFingerprint string
	var sourceXID, sequence int64
	var metadataJSON []byte
	err = tx.QueryRow(ctx, `
SELECT publication.publication_id,publication.source_lineage_id,publication.source_transaction_id,
       publication.source_xid,publication.checkpoint_lsn,publication.position_id,publication.content_hash,
       publication.logical_batch_id,publication.sequence,publication.checkpoint_metadata,intent.checkpoint_lsn,
       publication.projection_id,publication.mapping_fingerprint
FROM artifact_publications AS publication
JOIN authoritative_checkpoints AS checkpoint
  ON checkpoint.flow_incarnation_id=publication.flow_incarnation_id
 AND checkpoint.lsn=publication.checkpoint_lsn
JOIN source_ack_intents AS intent
  ON intent.flow_incarnation_id=publication.flow_incarnation_id
 AND intent.position_id=publication.position_id
WHERE publication.flow_incarnation_id=$1 AND publication.source_position=$2
FOR UPDATE`, fence.FlowIncarnationID, canonicalLSN).Scan(
		&publicationID, &sourceLineage, &sourceTransactionID, &sourceXID, &checkpointLSN,
		&positionID, &contentHash, &logicalBatchID, &sequence, &metadataJSON, &authorizedLSN, &projectionID, &mappingFingerprint,
	)
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
	if sourceLineage != transaction.SourceLineageID || sourceTransactionID != expectedTransactionID || sourceXID != int64(transaction.TransactionID) || checkpointLSN != canonicalLSN || positionID != expectedPositionID || contentHash != expectedContentHash || logicalBatchID != plan.LogicalBatchID || authorizedLSN != canonicalLSN || projectionID != p.config.ProjectionID || mappingFingerprint != p.config.MappingFingerprint {
		return Publication{}, false, fmt.Errorf("%w: existing artifact publication identity or ACK authorization differs", connector.ErrDeliveryConflict)
	}
	rows, err := tx.Query(ctx, `
SELECT object.artifact_id,object.logical_content_hash,object.encoded_byte_hash,
       object.bucket,object.object_key,object.version_id,object.checksum_sha256,
       object.encoded_length,object.encryption_mode,object.object_lock_evidence,
       object.projection_id,object.mapping_fingerprint
FROM artifact_publication_objects AS item
JOIN artifact_objects AS object ON object.artifact_id=item.artifact_id
WHERE item.publication_id=$1 AND item.release_marked_at IS NULL
  AND item.released_at IS NULL AND object.state='rooted'
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
		if err := rows.Scan(&artifactID, &logicalHash, &encodedHash, &object.Bucket, &object.Key, &object.VersionID, &object.ChecksumSHA256, &object.Length, &object.EncryptionMode, &object.ObjectLock, &object.ProjectionID, &object.MappingFingerprint); err != nil {
			rows.Close()
			return Publication{}, false, err
		}
		expected := artifacts[index]
		if artifactID != expected.ID || logicalHash != expected.LogicalContentHash || encodedHash != expected.EncodedByteHash || object.ChecksumSHA256 != expected.ChecksumSHA256 || object.Length != int64(len(expected.Encoded)) || object.ProjectionID != p.config.ProjectionID || object.MappingFingerprint != p.config.MappingFingerprint {
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
	barrierRows, err := tx.Query(ctx, `
SELECT fragment_ordinal,record_ordinal,kind,namespace,table_name,COALESCE(schema_id,''),ddl,ddl_plan,content_hash
FROM artifact_barriers WHERE publication_id=$1 ORDER BY ordinal`, publicationID)
	if err != nil {
		return Publication{}, false, err
	}
	barrierIndex := 0
	for barrierRows.Next() {
		if barrierIndex >= len(plan.Barriers) {
			barrierRows.Close()
			return Publication{}, false, fmt.Errorf("%w: publication has extra barriers", connector.ErrDeliveryConflict)
		}
		var stored Barrier
		if err := barrierRows.Scan(
			&stored.FragmentOrdinal, &stored.RecordOrdinal, &stored.Kind, &stored.Namespace,
			&stored.Table, &stored.SchemaID, &stored.DDL, &stored.DDLPlan, &stored.ContentHash,
		); err != nil {
			barrierRows.Close()
			return Publication{}, false, err
		}
		expected := plan.Barriers[barrierIndex]
		if stored.FragmentOrdinal != expected.FragmentOrdinal || stored.RecordOrdinal != expected.RecordOrdinal ||
			stored.Kind != expected.Kind || stored.Namespace != expected.Namespace || stored.Table != expected.Table ||
			stored.SchemaID != expected.SchemaID || stored.DDL != expected.DDL || !bytes.Equal(stored.DDLPlan, expected.DDLPlan) ||
			stored.ContentHash != expected.ContentHash {
			barrierRows.Close()
			return Publication{}, false, fmt.Errorf("%w: rooted barrier %d differs", connector.ErrDeliveryConflict, barrierIndex)
		}
		barrierIndex++
	}
	if err := barrierRows.Err(); err != nil {
		barrierRows.Close()
		return Publication{}, false, err
	}
	barrierRows.Close()
	if barrierIndex != len(plan.Barriers) {
		return Publication{}, false, fmt.Errorf("%w: publication barrier count=%d expected=%d", connector.ErrDeliveryConflict, barrierIndex, len(plan.Barriers))
	}
	if err := tx.Commit(ctx); err != nil {
		return Publication{}, false, err
	}
	for _, object := range evidence {
		if _, err := p.objects.HeadVersion(ctx, object); err != nil {
			return Publication{}, false, fmt.Errorf("revalidate rooted artifact: %w", err)
		}
	}
	checkpoint := connector.Checkpoint{LSN: checkpointLSN}
	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &checkpoint.Metadata); err != nil {
			return Publication{}, false, fmt.Errorf("decode authoritative artifact checkpoint: %w", err)
		}
	}
	return Publication{
		ID: publicationID, LogicalBatchID: logicalBatchID, Sequence: sequence,
		Artifacts: publicationArtifacts(artifacts),
		AckGrant:  connector.AckGrant{Checkpoint: checkpoint, PositionID: positionID},
	}, true, nil
}
