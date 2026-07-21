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
	AfterSlotCreated func(context.Context, ExportedSnapshot) error
	AfterPersisted   func(context.Context, ExportedSnapshot) error
	BeforeHandoff    func(context.Context, ExportedSnapshot) error
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
	ManifestHash        string
}

// Session owns the replication exporter connection. The exported snapshot is
// importable only while this connection remains open and idle.
type Session struct {
	Snapshot ExportedSnapshot

	mu       sync.Mutex
	exporter *pgconn.PgConn
	closed   bool
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
	s.exporter = nil
	s.mu.Unlock()
	if conn != nil {
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
	if err := runMigrations(ctx, control); err != nil {
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
	bootstrapGeneration, err := b.allocateGeneration(ctx, fence)
	if err != nil {
		return nil, err
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
	var slotName string
	defer func() {
		if cleanup {
			_ = exporter.Close(context.WithoutCancel(ctx))
			if slotName != "" {
				_, _ = b.source.Exec(context.WithoutCancel(ctx), "SELECT pg_catalog.pg_drop_replication_slot($1)", slotName)
			}
		}
	}()

	system, err := pglogrepl.IdentifySystem(ctx, exporter)
	if err != nil {
		return nil, fmt.Errorf("identify snapshot source: %w", err)
	}
	slotName = GenerationSlotName(fence.FlowID, fence.FlowIncarnationID, bootstrapGeneration)
	created, err := pglogrepl.CreateReplicationSlot(ctx, exporter, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{SnapshotAction: "EXPORT_SNAPSHOT"})
	if err != nil {
		return nil, fmt.Errorf("create exported-snapshot slot %s: %w", slotName, err)
	}
	consistentLSN, err := pglogrepl.ParseLSN(created.ConsistentPoint)
	if err != nil {
		return nil, fmt.Errorf("parse slot consistent point %q: %w", created.ConsistentPoint, err)
	}
	snapshot := ExportedSnapshot{
		BootstrapID:         uuid.New(),
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
	if err := b.persistSnapshot(ctx, fence, snapshot); err != nil {
		return nil, err
	}
	if b.hooks.AfterPersisted != nil {
		if err := b.hooks.AfterPersisted(ctx, snapshot); err != nil {
			return nil, err
		}
	}
	cleanup = false
	telemetry.RecordBootstrapEvent(ctx, "snapshot_exported")
	return &Session{Snapshot: snapshot, exporter: exporter}, nil
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

func (b *Bootstrapper) persistSnapshot(ctx context.Context, fence authority.RunFence, snapshot ExportedSnapshot) error {
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
  source_system_id,database_name,slot_name,publication_name,plugin,
  consistent_lsn,snapshot_name,manifest_hash,phase
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'snapshotting')`,
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
	); err != nil {
		return fmt.Errorf("persist source bootstrap: %w", err)
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
		return nil, err
	}
	tx, err := b.source.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin snapshot importer: %w", err)
	}
	command, err := importSnapshotCommand(session.Snapshot.SnapshotName)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	// SnapshotName is server-generated and allowlisted by
	// importSnapshotCommand before it reaches this grammar position.
	if _, err := tx.Exec(ctx, command); err != nil {
		_ = tx.Rollback(ctx)
		return nil, fmt.Errorf("import exported snapshot: %w", err)
	}
	return tx, nil
}

func importSnapshotCommand(snapshotName string) (string, error) {
	if !snapshotNamePattern.MatchString(snapshotName) {
		return "", fmt.Errorf("invalid exported snapshot name %q", snapshotName)
	}
	return fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName), nil
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
WHERE bootstrap_id=$1 AND flow_incarnation_id=$2 AND generation=$3
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
       consistent_lsn,snapshot_name,source_system_id,database_name,manifest_hash,phase
FROM source_bootstraps
WHERE bootstrap_id=$1 AND flow_incarnation_id=$2 AND generation=$3
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
		persisted.ManifestHash != supplied.ManifestHash {
		return fmt.Errorf("%w: supplied bootstrap does not match PostgreSQL authority", connector.ErrDeliveryConflict)
	}
	return nil
}

// RecordTaskReceipt atomically records the durable cursor and final receipt for
// one snapshot task. Completed task receipts are immutable.
func (b *Bootstrapper) RecordTaskReceipt(ctx context.Context, fence authority.RunFence, snapshot ExportedSnapshot, relationID uint32, taskID string, cursor json.RawMessage, receiptHash string) error {
	if relationID == 0 || strings.TrimSpace(taskID) == "" || strings.TrimSpace(receiptHash) == "" {
		return errors.New("relation, task ID, and receipt hash are required")
	}
	if len(cursor) > 0 && !json.Valid(cursor) {
		return errors.New("snapshot task cursor must be valid JSON")
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
	if phase != "snapshotting" {
		return fmt.Errorf("bootstrap task receipt requires snapshotting phase, got %s", phase)
	}
	if err := compareSnapshot(persisted, snapshot); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
INSERT INTO source_bootstrap_tasks (
  bootstrap_id,relation_id,task_id,durable_cursor,receipt_hash,status
) VALUES ($1,$2,$3,NULLIF($4,'')::jsonb,$5,'complete')
ON CONFLICT (bootstrap_id,relation_id,task_id) DO UPDATE SET
  durable_cursor=EXCLUDED.durable_cursor,
  receipt_hash=EXCLUDED.receipt_hash,
  status='complete',
  updated_at=clock_timestamp()
WHERE source_bootstrap_tasks.status <> 'complete'
   OR (source_bootstrap_tasks.receipt_hash=EXCLUDED.receipt_hash
       AND source_bootstrap_tasks.durable_cursor IS NOT DISTINCT FROM EXCLUDED.durable_cursor)`, snapshot.BootstrapID, relationID, taskID, string(cursor), receiptHash)
	if err != nil {
		return fmt.Errorf("record bootstrap task receipt: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: completed bootstrap task receipt conflicts", connector.ErrDeliveryConflict)
	}
	return tx.Commit(ctx)
}

// RecordPublication records durable destination snapshot publication. Handoff
// is forbidden until this receipt exists.
func (b *Bootstrapper) RecordPublication(ctx context.Context, fence authority.RunFence, snapshot ExportedSnapshot, destinationRevisionID, contentHash string, attemptID uuid.UUID) error {
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
INSERT INTO snapshot_publication_receipts (bootstrap_id,content_hash,destination_revision_id,attempt_id)
VALUES ($1,$2,$3,$4)
ON CONFLICT (bootstrap_id) DO UPDATE SET
  content_hash=EXCLUDED.content_hash
WHERE snapshot_publication_receipts.content_hash=EXCLUDED.content_hash
  AND snapshot_publication_receipts.destination_revision_id=EXCLUDED.destination_revision_id
  AND snapshot_publication_receipts.attempt_id=EXCLUDED.attempt_id`, snapshot.BootstrapID, contentHash, destinationRevisionID, attemptID)
	if err != nil {
		return fmt.Errorf("record snapshot publication receipt: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: bootstrap publication receipt conflicts", connector.ErrDeliveryConflict)
	}
	tag, err = tx.Exec(ctx, `
UPDATE source_bootstraps
SET phase='published',updated_at=clock_timestamp()
WHERE bootstrap_id=$1 AND flow_incarnation_id=$2 AND generation=$3 AND phase IN ('snapshotting','published')`, snapshot.BootstrapID, fence.FlowIncarnationID, fence.Generation)
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
func (b *Bootstrapper) Handoff(ctx context.Context, fence authority.RunFence, snapshot ExportedSnapshot) (connector.Checkpoint, error) {
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
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM snapshot_publication_receipts WHERE bootstrap_id=$1`, persisted.BootstrapID).Scan(&receiptCount); err != nil {
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
	checkpoint := connector.Checkpoint{LSN: persisted.ConsistentLSN.String()}
	positionID, err := connector.CheckpointPositionID(checkpoint)
	if err != nil {
		return connector.Checkpoint{}, err
	}
	metadataJSON := []byte(fmt.Sprintf(`{"bootstrap_id":%q}`, persisted.BootstrapID.String()))
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
WHERE bootstrap_id=$1 AND flow_incarnation_id=$2 AND generation=$3 AND phase='published'`, snapshot.BootstrapID, fence.FlowIncarnationID, fence.Generation)
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
	tag, err := tx.Exec(ctx, `
UPDATE source_bootstraps
SET phase='abandoning',abandoned_reason=$4,updated_at=clock_timestamp()
WHERE bootstrap_id=$1 AND flow_incarnation_id=$2 AND generation=$3
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
WHERE bootstrap_id=$1 AND flow_incarnation_id=$2 AND generation=$3
  AND slot_name=$4 AND phase='abandoning'`, persisted.BootstrapID, fence.FlowIncarnationID, fence.Generation, persisted.SlotName)
	if err != nil {
		return fmt.Errorf("finalize bootstrap abandonment: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("finalize bootstrap abandonment: affected=%d", tag.RowsAffected())
	}
	if err := finalize.Commit(ctx); err != nil {
		return err
	}
	telemetry.RecordBootstrapEvent(ctx, "generation_abandoned")
	return nil
}
