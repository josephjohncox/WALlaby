package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

var snapshotNamePattern = regexp.MustCompile(`^[0-9A-Fa-f]+-[0-9A-Fa-f]+-[0-9]+$`)

// Hooks exposes deterministic crash boundaries to process/integration tests.
type Hooks struct {
	AfterSlotCreated        func(context.Context, ExportedSnapshot) error
	AfterPersisted          func(context.Context, ExportedSnapshot) error
	AfterPublicationCreated func(context.Context, string) error
	AfterSnapshotBatchApply func(context.Context, ExportedSnapshot, SnapshotTask, int64) error
	AfterPublication        func(context.Context, ExportedSnapshot) error
	AfterPublicationReceipt func(context.Context, ExportedSnapshot) error
	BeforeHandoff           func(context.Context, ExportedSnapshot) error
	AfterHandoff            func(context.Context, ExportedSnapshot) error
	// DropSlot injects deterministic source drop failures in crash-window
	// tests. Production leaves it nil and uses pg_drop_replication_slot.
	DropSlot func(context.Context, string) error
}

// ExportedSnapshot is the durable slot cut plus the diagnostic snapshot name.
type ExportedSnapshot struct {
	BootstrapID         uuid.UUID
	BootstrapGeneration int64
	SlotName            string
	Publication         string
	Plugin              string
	ConsistentLSN       pglogrepl.LSN
	SnapshotName        string
	SourceSystem        string
	DatabaseName        string
	SourceLineageID     string
	PublicationRevision string
	ManifestHash        string
}

// Session owns the replication exporter connection. The exported snapshot is
// importable only while this connection remains open and idle.
type Session struct {
	Snapshot ExportedSnapshot

	mu        sync.Mutex
	exporter  *pgconn.PgConn
	createdAt time.Time
	closed    bool
}

func (s *Session) Alive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.closed && s.exporter != nil && !s.exporter.IsClosed()
}

func (s *Session) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	conn := s.exporter
	createdAt := s.createdAt
	s.exporter = nil
	s.mu.Unlock()
	if conn != nil {
		outcome := "closed"
		if conn.IsClosed() {
			outcome = "lost"
		}
		if !createdAt.IsZero() {
			telemetry.RecordBootstrapExporterAge(ctx, time.Since(createdAt), outcome)
		}
		return conn.Close(ctx)
	}
	return nil
}

// Bootstrapper coordinates a slot exporter with PostgreSQL authority. control
// and source may be the same PostgreSQL cluster but have distinct roles.
type Bootstrapper struct {
	control *pgxpool.Pool
	source  *pgxpool.Pool
	dsn     string
	hooks   Hooks
}

func NewBootstrapper(ctx context.Context, control *pgxpool.Pool, sourceDSN string, source *pgxpool.Pool, hooks Hooks) (*Bootstrapper, error) {
	if control == nil || source == nil || strings.TrimSpace(sourceDSN) == "" {
		return nil, errors.New("control pool, source pool, and source DSN are required")
	}
	if err := ApplyMigrations(ctx, control); err != nil {
		return nil, err
	}
	return &Bootstrapper{control: control, source: source, dsn: sourceDSN, hooks: hooks}, nil
}

// GenerationSlotName returns a private physical slot name that cannot alias a
// newer flow incarnation or generation.
func GenerationSlotName(flowID string, incarnationID uuid.UUID, generation int64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d", flowID, incarnationID, generation)))
	return fmt.Sprintf("wallaby_%d_%s", generation, hex.EncodeToString(digest[:8]))
}

// Start creates a logical slot with EXPORT_SNAPSHOT, persists its exact cut
// under the producer fence, and leaves the exporter connection idle.
func (b *Bootstrapper) Start(ctx context.Context, fence authority.RunFence, publication, manifestHash string) (*Session, error) {
	if strings.TrimSpace(publication) == "" || strings.TrimSpace(manifestHash) == "" {
		return nil, errors.New("publication and frozen manifest hash are required")
	}
	cfg, err := pgconn.ParseConfig(b.dsn)
	if err != nil {
		return nil, fmt.Errorf("parse source DSN: %w", err)
	}
	cfg.RuntimeParams["replication"] = "database"
	exporter, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect snapshot exporter: %w", err)
	}
	cleanup := true
	resourcePersisted := false
	var slotName string
	var slotResource preparedResource
	defer func() {
		if !cleanup {
			return
		}
		cleanupCtx := context.WithoutCancel(ctx)
		_ = exporter.Close(cleanupCtx)
		if !resourcePersisted {
			_ = b.cleanupUnpersistedSlot(cleanupCtx, fence, slotResource, slotName)
		}
	}()

	system, err := pglogrepl.IdentifySystem(ctx, exporter)
	if err != nil {
		return nil, fmt.Errorf("identify snapshot source: %w", err)
	}
	if err := b.reconcilePreparedSlotCreates(ctx, fence, system.SystemID, system.DBName); err != nil {
		return nil, err
	}
	bootstrapGeneration, err := b.allocateGeneration(ctx, fence)
	if err != nil {
		return nil, err
	}
	bootstrapID := uuid.New()
	slotName = GenerationSlotName(fence.FlowID, fence.FlowIncarnationID, bootstrapGeneration)
	slotResource, slotRevision, err := b.prepareOwnedSlot(ctx, fence, bootstrapID, system.SystemID, system.DBName, slotName)
	if err != nil {
		return nil, err
	}
	created, err := pglogrepl.CreateReplicationSlot(ctx, exporter, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{SnapshotAction: "EXPORT_SNAPSHOT"})
	if err != nil {
		return nil, fmt.Errorf("create exported-snapshot slot %s: %w", slotName, err)
	}
	consistentLSN, err := pglogrepl.ParseLSN(created.ConsistentPoint)
	if err != nil {
		return nil, fmt.Errorf("parse slot consistent point %q: %w", created.ConsistentPoint, err)
	}
	snapshot := ExportedSnapshot{
		BootstrapID:         bootstrapID,
		BootstrapGeneration: bootstrapGeneration,
		SlotName:            slotName,
		Publication:         publication,
		Plugin:              "pgoutput",
		ConsistentLSN:       consistentLSN,
		SnapshotName:        created.SnapshotName,
		SourceSystem:        system.SystemID,
		DatabaseName:        system.DBName,
		ManifestHash:        manifestHash,
	}
	if !snapshotNamePattern.MatchString(snapshot.SnapshotName) {
		return nil, fmt.Errorf("server returned invalid exported snapshot name %q", snapshot.SnapshotName)
	}
	if b.hooks.AfterSlotCreated != nil {
		if err := b.hooks.AfterSlotCreated(ctx, snapshot); err != nil {
			return nil, err
		}
	}
	if err := b.persistSnapshot(ctx, fence, snapshot, slotResource, slotRevision); err != nil {
		return nil, err
	}
	resourcePersisted = true
	if b.hooks.AfterPersisted != nil {
		if err := b.hooks.AfterPersisted(ctx, snapshot); err != nil {
			return nil, err
		}
	}
	cleanup = false
	telemetry.RecordBootstrapEvent(ctx, "snapshot_exported")
	return &Session{Snapshot: snapshot, exporter: exporter, createdAt: time.Now()}, nil
}

func (b *Bootstrapper) allocateGeneration(ctx context.Context, fence authority.RunFence) (int64, error) {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin bootstrap generation allocation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return 0, err
	}
	var generation int64
	if err := tx.QueryRow(ctx, `SELECT nextval('wallaby_bootstrap_generation_seq')`).Scan(&generation); err != nil {
		return 0, fmt.Errorf("allocate bootstrap generation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit bootstrap generation: %w", err)
	}
	return generation, nil
}

func (b *Bootstrapper) persistSnapshot(ctx context.Context, fence authority.RunFence, snapshot ExportedSnapshot, slotResource preparedResource, slotRevision string) error {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin bootstrap persistence: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO source_bootstraps (
  bootstrap_id,flow_incarnation_id,flow_id,generation,bootstrap_generation,acquisition_id,lease_epoch,
  owner_generation,owner_acquisition_id,owner_lease_epoch,
  source_system_id,database_name,slot_name,publication_name,plugin,
  consistent_lsn,snapshot_name,manifest_hash,selection_hash,exporter_execution_id,phase
) VALUES ($1,$2,$3,$4,$5,$6,$7,$4,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$15,$16,'snapshotting')`,
		snapshot.BootstrapID,
		fence.FlowIncarnationID,
		fence.FlowID,
		fence.Generation,
		snapshot.BootstrapGeneration,
		fence.AcquisitionID,
		fence.LeaseEpoch,
		snapshot.SourceSystem,
		snapshot.DatabaseName,
		snapshot.SlotName,
		snapshot.Publication,
		snapshot.Plugin,
		snapshot.ConsistentLSN.String(),
		snapshot.SnapshotName,
		snapshot.ManifestHash,
		fence.ExecutionID,
	); err != nil {
		return fmt.Errorf("persist source bootstrap: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO source_resources (
  flow_incarnation_id,resource_kind,resource_id,flow_id,generation,acquisition_id,lease_epoch,
  created_generation,created_acquisition_id,created_lease_epoch,
  source_system_id,database_name,physical_name,ownership,revision,state,bootstrap_id
) VALUES($1,'slot',$2,$3,$4,$5,$6,$4,$5,$6,$7,$8,$9,'owned',$10,'ready',$11)`, fence.FlowIncarnationID, slotResource.resourceID, fence.FlowID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, snapshot.SourceSystem, snapshot.DatabaseName, snapshot.SlotName, slotRevision, snapshot.BootstrapID); err != nil {
		return fmt.Errorf("persist owned slot resource: %w", err)
	}
	tag, err := tx.Exec(ctx, `
UPDATE source_resource_operations
SET status='applied',external_evidence=jsonb_build_object('consistent_lsn',$2::text),completed_at=clock_timestamp()
WHERE operation_id=$1 AND flow_incarnation_id=$3 AND status='prepared'`, slotResource.operationID, snapshot.ConsistentLSN.String(), fence.FlowIncarnationID)
	if err != nil {
		return fmt.Errorf("complete slot resource operation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: slot operation changed before persistence", authority.ErrFenceRejected)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit source bootstrap: %w", err)
	}
	return nil
}

// ImportSnapshot starts a read-only repeatable-read transaction and imports the
// slot snapshot before any caller catalog or row query can run.
func (b *Bootstrapper) ImportSnapshot(ctx context.Context, fence authority.RunFence, session *Session) (pgx.Tx, error) {
	if session == nil || !session.Alive() {
		return nil, errors.New("exported snapshot connection is not alive; bootstrap generation must be abandoned")
	}
	if err := b.validateSession(ctx, fence, session.Snapshot); err != nil {
		return nil, fmt.Errorf("validate snapshot exporter session: %w", err)
	}
	tx, err := b.source.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin snapshot importer: %w", err)
	}
	command, valid := importSnapshotCommand(session.Snapshot.SnapshotName)
	if !valid {
		_ = tx.Rollback(ctx)
		return nil, fmt.Errorf("invalid exported snapshot name %q", session.Snapshot.SnapshotName)
	}
	// SnapshotName is server-generated and allowlisted by
	// importSnapshotCommand before it reaches this grammar position.
	if _, err := tx.Exec(ctx, command); err != nil {
		_ = tx.Rollback(ctx)
		return nil, fmt.Errorf("import exported snapshot: %w", err)
	}
	return tx, nil
}

func importSnapshotCommand(snapshotName string) (string, bool) {
	if !snapshotNamePattern.MatchString(snapshotName) {
		return "", false
	}
	return fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName), true
}

func (b *Bootstrapper) validateSession(ctx context.Context, fence authority.RunFence, snapshot ExportedSnapshot) error {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	var count int
	if err := tx.QueryRow(ctx, `
SELECT count(*) FROM source_bootstraps
WHERE bootstrap_id=$1 AND flow_incarnation_id=$2 AND owner_generation=$3
  AND slot_name=$4 AND consistent_lsn=$5 AND snapshot_name=$6
  AND phase='snapshotting'`, snapshot.BootstrapID, fence.FlowIncarnationID, fence.Generation, snapshot.SlotName, snapshot.ConsistentLSN.String(), snapshot.SnapshotName).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return errors.New("exported snapshot does not match the durable bootstrap generation")
	}
	return tx.Commit(ctx)
}

func loadSnapshotForUpdate(ctx context.Context, tx pgx.Tx, fence authority.RunFence, bootstrapID uuid.UUID) (ExportedSnapshot, string, error) {
	var snapshot ExportedSnapshot
	var phase, consistentLSN string
	err := tx.QueryRow(ctx, `
SELECT bootstrap_id,bootstrap_generation,slot_name,publication_name,plugin,
       consistent_lsn,snapshot_name,source_system_id,database_name,
       COALESCE(source_lineage_id,''),COALESCE(publication_revision,''),manifest_hash,phase
FROM source_bootstraps
WHERE bootstrap_id=$1 AND flow_incarnation_id=$2 AND owner_generation=$3
FOR UPDATE`, bootstrapID, fence.FlowIncarnationID, fence.Generation).Scan(
		&snapshot.BootstrapID,
		&snapshot.BootstrapGeneration,
		&snapshot.SlotName,
		&snapshot.Publication,
		&snapshot.Plugin,
		&consistentLSN,
		&snapshot.SnapshotName,
		&snapshot.SourceSystem,
		&snapshot.DatabaseName,
		&snapshot.SourceLineageID,
		&snapshot.PublicationRevision,
		&snapshot.ManifestHash,
		&phase,
	)
	if err != nil {
		return ExportedSnapshot{}, "", fmt.Errorf("load persisted bootstrap: %w", err)
	}
	parsed, err := pglogrepl.ParseLSN(consistentLSN)
	if err != nil {
		return ExportedSnapshot{}, "", fmt.Errorf("parse persisted bootstrap LSN %q: %w", consistentLSN, err)
	}
	snapshot.ConsistentLSN = parsed
	return snapshot, phase, nil
}

func compareSnapshot(persisted, supplied ExportedSnapshot) error {
	if persisted.BootstrapID != supplied.BootstrapID ||
		persisted.BootstrapGeneration != supplied.BootstrapGeneration ||
		persisted.SlotName != supplied.SlotName ||
		persisted.Publication != supplied.Publication ||
		persisted.Plugin != supplied.Plugin ||
		persisted.ConsistentLSN != supplied.ConsistentLSN ||
		persisted.SnapshotName != supplied.SnapshotName ||
		persisted.SourceSystem != supplied.SourceSystem ||
		persisted.DatabaseName != supplied.DatabaseName ||
		persisted.SourceLineageID != supplied.SourceLineageID ||
		persisted.PublicationRevision != supplied.PublicationRevision ||
		persisted.ManifestHash != supplied.ManifestHash {
		return fmt.Errorf("%w: supplied bootstrap does not match PostgreSQL authority", connector.ErrDeliveryConflict)
	}
	return nil
}

// RecordPublication records durable destination snapshot publication. Handoff
// is forbidden until this receipt exists.
func (b *Bootstrapper) RecordPublication(ctx context.Context, fence authority.RunFence, snapshot ExportedSnapshot, destinationRevisionID, contentHash string, attemptID uuid.UUID) (retErr error) {
	ctx, endSpan := telemetry.StartBootstrapSpan(ctx, "publication", fence.FlowID, snapshot.BootstrapID.String(), "", snapshot.BootstrapGeneration)
	defer func() { endSpan(retErr) }()
	if strings.TrimSpace(destinationRevisionID) == "" || strings.TrimSpace(contentHash) == "" || attemptID == uuid.Nil {
		return errors.New("destination revision, content hash, and attempt ID are required")
	}
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	persisted, phase, err := loadSnapshotForUpdate(ctx, tx, fence, snapshot.BootstrapID)
	if err != nil {
		return err
	}
	if phase != "snapshotting" && phase != "published" {
		return fmt.Errorf("bootstrap publication requires snapshotting phase, got %s", phase)
	}
	if err := compareSnapshot(persisted, snapshot); err != nil {
		return err
	}
	var taskCount, incomplete int
	if err := tx.QueryRow(ctx, `
SELECT count(*),count(*) FILTER (WHERE status <> 'complete' OR receipt_hash IS NULL)
FROM source_bootstrap_tasks WHERE bootstrap_id=$1`, snapshot.BootstrapID).Scan(&taskCount, &incomplete); err != nil {
		return fmt.Errorf("check bootstrap task completion: %w", err)
	}
	if taskCount == 0 || incomplete != 0 {
		return fmt.Errorf("snapshot publication requires completed durable task receipts: tasks=%d incomplete=%d", taskCount, incomplete)
	}
	tag, err := tx.Exec(ctx, `
INSERT INTO snapshot_publication_receipts (
  bootstrap_id,content_hash,destination_revision_id,attempt_id,
  flow_incarnation_id,generation,acquisition_id,lease_epoch,authority_origin
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'fenced')
ON CONFLICT (bootstrap_id) DO UPDATE SET
  content_hash=EXCLUDED.content_hash
WHERE snapshot_publication_receipts.content_hash=EXCLUDED.content_hash
  AND snapshot_publication_receipts.destination_revision_id=EXCLUDED.destination_revision_id
  AND snapshot_publication_receipts.attempt_id=EXCLUDED.attempt_id
  AND snapshot_publication_receipts.flow_incarnation_id=EXCLUDED.flow_incarnation_id`, snapshot.BootstrapID, contentHash, destinationRevisionID, attemptID, fence.FlowIncarnationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch)
	if err != nil {
		return fmt.Errorf("record snapshot publication receipt: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: bootstrap publication receipt conflicts", connector.ErrDeliveryConflict)
	}
	tag, err = tx.Exec(ctx, `
UPDATE source_bootstraps
SET phase='published',updated_at=clock_timestamp()
WHERE bootstrap_id=$1 AND flow_incarnation_id=$2 AND owner_generation=$3 AND phase IN ('snapshotting','published')`, snapshot.BootstrapID, fence.FlowIncarnationID, fence.Generation)
	if err != nil {
		return fmt.Errorf("mark snapshot published: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("bootstrap publication phase changed concurrently")
	}
	return tx.Commit(ctx)
}

// Handoff atomically roots snapshot publication, records the exact slot
// consistent point as the managed checkpoint, and authorizes source feedback.
func (b *Bootstrapper) Handoff(ctx context.Context, fence authority.RunFence, snapshot ExportedSnapshot) (checkpoint connector.Checkpoint, retErr error) {
	ctx, endSpan := telemetry.StartBootstrapSpan(ctx, "handoff", fence.FlowID, snapshot.BootstrapID.String(), "", snapshot.BootstrapGeneration)
	defer func() { endSpan(retErr) }()
	if b.hooks.BeforeHandoff != nil {
		if err := b.hooks.BeforeHandoff(ctx, snapshot); err != nil {
			return connector.Checkpoint{}, err
		}
	}
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return connector.Checkpoint{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return connector.Checkpoint{}, err
	}
	persisted, phase, err := loadSnapshotForUpdate(ctx, tx, fence, snapshot.BootstrapID)
	if err != nil {
		return connector.Checkpoint{}, err
	}
	if phase != "published" {
		return connector.Checkpoint{}, fmt.Errorf("bootstrap handoff requires published phase, got %s", phase)
	}
	if err := compareSnapshot(persisted, snapshot); err != nil {
		return connector.Checkpoint{}, err
	}
	var receiptCount, taskCount, incomplete int
	if err := tx.QueryRow(ctx, `
SELECT count(*) FROM snapshot_publication_receipts
WHERE bootstrap_id=$1 AND flow_incarnation_id=$2 AND authority_origin='fenced'`, persisted.BootstrapID, fence.FlowIncarnationID).Scan(&receiptCount); err != nil {
		return connector.Checkpoint{}, err
	}
	if err := tx.QueryRow(ctx, `
SELECT count(*),count(*) FILTER (WHERE status <> 'complete' OR receipt_hash IS NULL)
FROM source_bootstrap_tasks WHERE bootstrap_id=$1`, persisted.BootstrapID).Scan(&taskCount, &incomplete); err != nil {
		return connector.Checkpoint{}, err
	}
	if receiptCount != 1 || taskCount == 0 || incomplete != 0 {
		return connector.Checkpoint{}, errors.New("complete snapshot task receipts and one publication receipt are required before CDC handoff")
	}
	rows, err := tx.Query(ctx, `
SELECT destination_schema_json FROM source_bootstrap_tasks
WHERE bootstrap_id=$1
ORDER BY relation_id,task_id`, persisted.BootstrapID)
	if err != nil {
		return connector.Checkpoint{}, fmt.Errorf("load bootstrap handoff schemas: %w", err)
	}
	baselineTransaction := connector.SourceTransaction{}
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			rows.Close()
			return connector.Checkpoint{}, fmt.Errorf("scan bootstrap handoff schema: %w", err)
		}
		if len(encoded) == 0 || string(encoded) == "{}" || string(encoded) == "null" {
			rows.Close()
			return connector.Checkpoint{}, errors.New("bootstrap handoff requires a frozen schema manifest for every task")
		}
		var schema connector.Schema
		if err := json.Unmarshal(encoded, &schema); err != nil {
			rows.Close()
			return connector.Checkpoint{}, fmt.Errorf("decode bootstrap handoff destination schema: %w", err)
		}
		baselineTransaction.Fragments = append(baselineTransaction.Fragments, connector.TransactionFragment{Batch: connector.Batch{Schema: schema}})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return connector.Checkpoint{}, fmt.Errorf("iterate bootstrap handoff schemas: %w", err)
	}
	rows.Close()
	metadata := map[string]string{"bootstrap_id": persisted.BootstrapID.String()}
	if len(baselineTransaction.Fragments) > 0 {
		metadata, err = connector.MergeManagedSchemaBaselines(metadata, baselineTransaction)
		if err != nil {
			return connector.Checkpoint{}, err
		}
	}
	checkpoint = connector.Checkpoint{LSN: persisted.ConsistentLSN.String(), Metadata: metadata}
	positionID, err := connector.CheckpointPositionID(checkpoint)
	if err != nil {
		return connector.Checkpoint{}, err
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return connector.Checkpoint{}, fmt.Errorf("encode bootstrap checkpoint metadata: %w", err)
	}
	var currentLSN string
	err = tx.QueryRow(ctx, `SELECT lsn FROM authoritative_checkpoints WHERE flow_incarnation_id=$1 FOR UPDATE`, fence.FlowIncarnationID).Scan(&currentLSN)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if _, err := tx.Exec(ctx, `
INSERT INTO authoritative_checkpoints (
  flow_incarnation_id,flow_id,generation,acquisition_id,lease_epoch,lsn,metadata
) VALUES ($1,$2,$3,$4,$5,$6,$7)`, fence.FlowIncarnationID, fence.FlowID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, checkpoint.LSN, metadataJSON); err != nil {
			return connector.Checkpoint{}, fmt.Errorf("commit bootstrap checkpoint: %w", err)
		}
	case err != nil:
		return connector.Checkpoint{}, fmt.Errorf("load bootstrap checkpoint: %w", err)
	case currentLSN != checkpoint.LSN:
		return connector.Checkpoint{}, fmt.Errorf("%w: existing checkpoint %s differs from bootstrap cut %s", connector.ErrDeliveryConflict, currentLSN, checkpoint.LSN)
	default:
		if _, err := tx.Exec(ctx, `
UPDATE authoritative_checkpoints
SET generation=$2,acquisition_id=$3,lease_epoch=$4,metadata=$5,updated_at=clock_timestamp()
WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, metadataJSON); err != nil {
			return connector.Checkpoint{}, fmt.Errorf("adopt bootstrap checkpoint: %w", err)
		}
	}
	tag, err := tx.Exec(ctx, `
INSERT INTO source_ack_intents (
  flow_incarnation_id,position_id,checkpoint_lsn,generation,acquisition_id,lease_epoch
) VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (flow_incarnation_id,position_id) DO UPDATE SET checkpoint_lsn=EXCLUDED.checkpoint_lsn
WHERE source_ack_intents.checkpoint_lsn=EXCLUDED.checkpoint_lsn`, fence.FlowIncarnationID, positionID, checkpoint.LSN, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch)
	if err != nil {
		return connector.Checkpoint{}, fmt.Errorf("authorize bootstrap source ack: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return connector.Checkpoint{}, fmt.Errorf("%w: bootstrap ACK intent conflicts", connector.ErrDeliveryConflict)
	}
	tag, err = tx.Exec(ctx, `
UPDATE source_bootstraps
SET phase='streaming',updated_at=clock_timestamp()
WHERE bootstrap_id=$1 AND flow_incarnation_id=$2 AND owner_generation=$3 AND phase='published'`, snapshot.BootstrapID, fence.FlowIncarnationID, fence.Generation)
	if err != nil {
		return connector.Checkpoint{}, fmt.Errorf("complete bootstrap handoff: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return connector.Checkpoint{}, errors.New("bootstrap is not in published phase")
	}
	if err := tx.Commit(ctx); err != nil {
		return connector.Checkpoint{}, fmt.Errorf("commit bootstrap handoff: %w", err)
	}
	telemetry.RecordBootstrapEvent(ctx, "handoff_committed")
	return checkpoint, nil
}

// Abandon durably journals cleanup before dropping the exact private slot.
// A failed drop leaves phase=abandoning so the current owner can retry.
func (b *Bootstrapper) Abandon(ctx context.Context, fence authority.RunFence, snapshot ExportedSnapshot, reason string) error {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	persisted, phase, err := loadSnapshotForUpdate(ctx, tx, fence, snapshot.BootstrapID)
	if err != nil {
		return err
	}
	if err := compareSnapshot(persisted, snapshot); err != nil {
		return err
	}
	if phase == "abandoned" {
		return tx.Commit(ctx)
	}
	if phase != "exporting" && phase != "snapshotting" && phase != "abandoning" {
		return errors.New("bootstrap is not an unpublished generation owned by this fence")
	}
	var slotResourceID uuid.UUID
	var slotRevision, slotOwnership, slotState string
	if err := tx.QueryRow(ctx, `
SELECT resource_id,revision,ownership,state
FROM source_resources
WHERE flow_incarnation_id=$1 AND resource_kind='slot' AND bootstrap_id=$2
  AND physical_name=$3
FOR UPDATE`, fence.FlowIncarnationID, snapshot.BootstrapID, persisted.SlotName).Scan(&slotResourceID, &slotRevision, &slotOwnership, &slotState); err != nil {
		return fmt.Errorf("load owned bootstrap slot resource: %w", err)
	}
	if slotOwnership != "owned" || (slotState != "ready" && slotState != "cleanup_pending") {
		return fmt.Errorf("bootstrap cleanup refuses slot ownership=%s state=%s", slotOwnership, slotState)
	}
	dropOperationID := uuid.New()
	if _, err := tx.Exec(ctx, `
INSERT INTO source_resource_operations (
  operation_id,flow_incarnation_id,resource_kind,resource_id,operation,desired_revision,
  generation,acquisition_id,lease_epoch,status,bootstrap_id,source_system_id,database_name,physical_name
) VALUES($1,$2,'slot',$3,'drop',$4,$5,$6,$7,'prepared',$8,$9,$10,$11)
ON CONFLICT (flow_incarnation_id,resource_kind,resource_id,operation,desired_revision,acquisition_id,lease_epoch) DO NOTHING`, dropOperationID, fence.FlowIncarnationID, slotResourceID, slotRevision, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, snapshot.BootstrapID, snapshot.SourceSystem, snapshot.DatabaseName, persisted.SlotName); err != nil {
		return fmt.Errorf("prepare slot cleanup: %w", err)
	}
	if err := tx.QueryRow(ctx, `
SELECT operation_id FROM source_resource_operations
WHERE flow_incarnation_id=$1 AND resource_kind='slot' AND resource_id=$2
  AND operation='drop' AND desired_revision=$3 AND acquisition_id=$4 AND lease_epoch=$5`, fence.FlowIncarnationID, slotResourceID, slotRevision, fence.AcquisitionID, fence.LeaseEpoch).Scan(&dropOperationID); err != nil {
		return fmt.Errorf("load slot cleanup operation: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE source_resources SET state='cleanup_pending',generation=$3,acquisition_id=$4,lease_epoch=$5,updated_at=clock_timestamp() WHERE flow_incarnation_id=$1 AND resource_id=$2`, fence.FlowIncarnationID, slotResourceID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch); err != nil {
		return fmt.Errorf("mark slot cleanup pending: %w", err)
	}
	tag, err := tx.Exec(ctx, `
UPDATE source_bootstraps
SET phase='abandoning',abandoned_reason=$4,updated_at=clock_timestamp()
WHERE bootstrap_id=$1 AND flow_incarnation_id=$2 AND owner_generation=$3
  AND slot_name=$5 AND phase IN ('exporting','snapshotting','abandoning')`, snapshot.BootstrapID, fence.FlowIncarnationID, fence.Generation, reason, persisted.SlotName)
	if err != nil {
		return fmt.Errorf("journal bootstrap abandonment: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("journal bootstrap abandonment: affected=%d", tag.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if _, err := b.source.Exec(ctx, "SELECT pg_catalog.pg_drop_replication_slot($1)", persisted.SlotName); err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42704" {
			return fmt.Errorf("drop abandoned bootstrap slot %s: %w", persisted.SlotName, err)
		}
	}
	finalize, err := b.control.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = finalize.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, finalize, fence); err != nil {
		return err
	}
	tag, err = finalize.Exec(ctx, `
UPDATE source_bootstraps
SET phase='abandoned',updated_at=clock_timestamp()
WHERE bootstrap_id=$1 AND flow_incarnation_id=$2 AND owner_generation=$3
  AND slot_name=$4 AND phase='abandoning'`, persisted.BootstrapID, fence.FlowIncarnationID, fence.Generation, persisted.SlotName)
	if err != nil {
		return fmt.Errorf("finalize bootstrap abandonment: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("finalize bootstrap abandonment: affected=%d", tag.RowsAffected())
	}
	tag, err = finalize.Exec(ctx, `
UPDATE source_resources SET state='retired',updated_at=clock_timestamp()
WHERE flow_incarnation_id=$1 AND resource_id=$2 AND resource_kind='slot'
  AND ownership='owned' AND physical_name=$3 AND state='cleanup_pending'`, fence.FlowIncarnationID, slotResourceID, persisted.SlotName)
	if err != nil {
		return fmt.Errorf("retire owned slot resource: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("retire owned slot resource: affected=%d", tag.RowsAffected())
	}
	tag, err = finalize.Exec(ctx, `
UPDATE source_resource_operations
SET status='applied',external_evidence=jsonb_build_object('slot_absent',true),completed_at=clock_timestamp()
WHERE operation_id=$1 AND flow_incarnation_id=$2 AND status IN ('prepared','indeterminate')`, dropOperationID, fence.FlowIncarnationID)
	if err != nil {
		return fmt.Errorf("complete slot cleanup operation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("complete slot cleanup operation: affected=%d", tag.RowsAffected())
	}
	if err := finalize.Commit(ctx); err != nil {
		return err
	}
	telemetry.RecordBootstrapEvent(ctx, "generation_abandoned")
	telemetry.RecordBootstrapEvent(ctx, "cleanup")
	return nil
}
