package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/controlstore"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// PostgresStore persists checkpoints in Postgres.
type PostgresStore struct {
	pool     *pgxpool.Pool
	ownsPool bool
}

func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	if dsn == "" {
		return nil, errors.New("postgres DSN is required")
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres DSN: %w", err)
	}
	controlstore.ConfigurePool(cfg)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	store, err := NewPostgresStoreWithPool(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, err
	}
	store.ownsPool = true
	return store, nil
}

// NewPostgresStoreWithPool borrows the worker's shared control pool.
func NewPostgresStoreWithPool(ctx context.Context, pool *pgxpool.Pool) (*PostgresStore, error) {
	if pool == nil {
		return nil, errors.New("postgres control pool is required")
	}
	if err := runMigrations(ctx, pool); err != nil {
		return nil, err
	}
	return &PostgresStore{pool: pool}, nil
}

func (p *PostgresStore) Close() {
	if p.ownsPool && p.pool != nil {
		p.pool.Close()
	}
	p.pool = nil
}

func (p *PostgresStore) Get(ctx context.Context, flowID string) (connector.Checkpoint, error) {
	row := p.pool.QueryRow(ctx, "SELECT lsn, metadata, updated_at FROM checkpoints WHERE flow_id = $1", flowID)
	return scanCheckpoint(row)
}

// CheckExternalOverrideAllowed is retained for read/check compatibility. New
// administrative writers must call PutExternal so authority cannot be acquired
// between this check and the checkpoint write.
func (p *PostgresStore) CheckExternalOverrideAllowed(ctx context.Context, flowID string) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin checkpoint override guard: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := checkExternalOverrideAllowed(ctx, tx, flowID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// PutExternal atomically takes the flow authority lock, rejects every live
// dispatch/execution/producer owner, validates monotonicity, and writes.
func (p *PostgresStore) PutExternal(ctx context.Context, flowID string, cp connector.Checkpoint) error {
	canonical, err := canonicalizeCheckpoint(cp)
	if err != nil {
		return err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin external checkpoint transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := checkExternalOverrideAllowed(ctx, tx, flowID); err != nil {
		return err
	}
	if err := putPostgresCheckpoint(ctx, tx, flowID, canonical); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit external checkpoint transaction: %w", err)
	}
	return nil
}

func checkExternalOverrideAllowed(ctx context.Context, tx pgx.Tx, flowID string) error {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", flowID); err != nil {
		return fmt.Errorf("lock checkpoint override guard: %w", err)
	}
	var active bool
	if err := tx.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1
  FROM flows AS flow
  WHERE flow.id=$1
    AND (
      flow.dispatch_pending
      OR EXISTS (
        SELECT 1 FROM flow_executions AS execution
        WHERE execution.incarnation_id=flow.incarnation_id
          AND execution.status='running'
          AND (execution.lease_expires_at IS NULL OR execution.lease_expires_at > clock_timestamp())
      )
      OR EXISTS (
        SELECT 1
        FROM producer_leases AS producer
        JOIN execution_acquisitions AS acquisition ON acquisition.acquisition_id=producer.acquisition_id
        WHERE producer.incarnation_id=flow.incarnation_id
          AND producer.lease_expires_at > clock_timestamp()
          AND acquisition.finished_at IS NULL
      )
    )
)`, flowID).Scan(&active); err != nil {
		return fmt.Errorf("check active checkpoint authority: %w", err)
	}
	if active {
		return ErrManagedProducerActive
	}
	return nil
}

func (p *PostgresStore) Put(ctx context.Context, flowID string, checkpoint connector.Checkpoint) error {
	var err error
	checkpoint, err = canonicalizeCheckpoint(checkpoint)
	if err != nil {
		return err
	}
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin checkpoint transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := putPostgresCheckpoint(ctx, tx, flowID, checkpoint); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit checkpoint transaction: %w", err)
	}
	return nil
}

// PersistCheckpointAndOutbox atomically advances a checkpoint and records all
// secondary-destination deliveries in the same PostgreSQL transaction.
func (p *PostgresStore) PersistCheckpointAndOutbox(ctx context.Context, flowID string, checkpoint connector.Checkpoint, entries []connector.OutboxEntry) error {
	var err error
	checkpoint, err = canonicalizeCheckpoint(checkpoint)
	if err != nil {
		return err
	}
	encoded, err := encodeOutboxEntries(flowID, checkpoint, entries)
	if err != nil {
		return err
	}
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin checkpoint outbox transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := putPostgresCheckpoint(ctx, tx, flowID, checkpoint); err != nil {
		return err
	}
	for _, item := range encoded {
		var positionHash string
		err := tx.QueryRow(ctx,
			"SELECT batch_hash FROM checkpoint_outbox WHERE flow_id=$1 AND position_id=$2 LIMIT 1",
			flowID, item.entry.PositionID).Scan(&positionHash)
		switch {
		case err == nil && positionHash != item.batchHash:
			return fmt.Errorf("%w: flow=%s position=%s identifies different batches", connector.ErrOutboxConflict, flowID, item.entry.PositionID)
		case err == nil, errors.Is(err, pgx.ErrNoRows):
		default:
			return fmt.Errorf("read outbox batch identity: %w", err)
		}
		tag, err := tx.Exec(ctx,
			`INSERT INTO checkpoint_outbox (flow_id, destination_id, position_id, batch_hash, codec, batch_json, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)
			 ON CONFLICT (flow_id, destination_id, position_id) DO NOTHING`,
			flowID, item.entry.Destination, item.entry.PositionID, item.batchHash, outboxCodecGobV1, item.batchData, item.entry.CreatedAt)
		if err != nil {
			return fmt.Errorf("insert outbox entry for %s: %w", item.entry.Destination, err)
		}
		if tag.RowsAffected() == 0 {
			var existingHash string
			if err := tx.QueryRow(ctx,
				`SELECT batch_hash FROM checkpoint_outbox
				 WHERE flow_id=$1 AND destination_id=$2 AND position_id=$3`,
				flowID, item.entry.Destination, item.entry.PositionID).Scan(&existingHash); err != nil {
				return fmt.Errorf("read existing outbox entry for %s: %w", item.entry.Destination, err)
			}
			if existingHash != item.batchHash {
				return fmt.Errorf("%w: flow=%s destination=%s position=%s", connector.ErrOutboxConflict, flowID, item.entry.Destination, item.entry.PositionID)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit checkpoint outbox transaction: %w", err)
	}
	return nil
}

func putPostgresCheckpoint(ctx context.Context, tx pgx.Tx, flowID string, checkpoint connector.Checkpoint) error {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", flowID); err != nil {
		return fmt.Errorf("lock checkpoint flow: %w", err)
	}
	if checkpoint.Timestamp.IsZero() {
		checkpoint.Timestamp = time.Now().UTC()
	}
	if checkpoint.Metadata == nil {
		checkpoint.Metadata = map[string]string{}
	}
	metadataJSON, err := json.Marshal(checkpoint.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}
	var currentLSN string
	err = tx.QueryRow(ctx, "SELECT lsn FROM checkpoints WHERE flow_id=$1 FOR UPDATE", flowID).Scan(&currentLSN)
	switch {
	case err == nil:
		if err := validateCheckpointAdvance(flowID, currentLSN, checkpoint.LSN); err != nil {
			return err
		}
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return fmt.Errorf("read current checkpoint: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO checkpoints (flow_id, lsn, metadata, updated_at) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (flow_id) DO UPDATE SET lsn=EXCLUDED.lsn, metadata=EXCLUDED.metadata, updated_at=EXCLUDED.updated_at`,
		flowID, checkpoint.LSN, metadataJSON, checkpoint.Timestamp); err != nil {
		return fmt.Errorf("upsert checkpoint: %w", err)
	}
	return nil
}

func (p *PostgresStore) ListOutbox(ctx context.Context, flowID string) ([]connector.OutboxEntry, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT destination_id, position_id, batch_hash, codec, batch_json, created_at FROM checkpoint_outbox
		 WHERE flow_id=$1 ORDER BY created_at, destination_id`, flowID)
	if err != nil {
		return nil, fmt.Errorf("list checkpoint outbox: %w", err)
	}
	defer rows.Close()
	entries := make([]connector.OutboxEntry, 0)
	for rows.Next() {
		var destination, position, batchHash, codec string
		var batchJSON []byte
		var createdAt time.Time
		if err := rows.Scan(&destination, &position, &batchHash, &codec, &batchJSON, &createdAt); err != nil {
			return nil, fmt.Errorf("scan checkpoint outbox: %w", err)
		}
		batch, err := decodeOutboxBatch(codec, batchJSON)
		if err != nil {
			return nil, err
		}
		entries = append(entries, connector.OutboxEntry{
			FlowID: flowID, Destination: destination, PositionID: position, BatchHash: batchHash, Batch: batch, CreatedAt: createdAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate checkpoint outbox: %w", err)
	}
	return entries, nil
}

func (p *PostgresStore) DeleteOutbox(ctx context.Context, flowID, destination, positionID string) error {
	tag, err := p.pool.Exec(ctx,
		"DELETE FROM checkpoint_outbox WHERE flow_id=$1 AND destination_id=$2 AND position_id=$3",
		flowID, destination, positionID)
	if err != nil {
		return fmt.Errorf("delete checkpoint outbox entry: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("delete checkpoint outbox entry flow=%s destination=%s position=%s: entry not found", flowID, destination, positionID)
	}
	return nil
}

func (p *PostgresStore) List(ctx context.Context) ([]connector.FlowCheckpoint, error) {
	rows, err := p.pool.Query(ctx, "SELECT flow_id, lsn, metadata, updated_at FROM checkpoints ORDER BY updated_at DESC")
	if err != nil {
		return nil, fmt.Errorf("list checkpoints: %w", err)
	}
	defer rows.Close()

	items := make([]connector.FlowCheckpoint, 0)
	for rows.Next() {
		cp, err := scanFlowCheckpoint(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, cp)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate checkpoints: %w", err)
	}
	return items, nil
}

func scanCheckpoint(row pgx.Row) (connector.Checkpoint, error) {
	var cp connector.Checkpoint
	var metadataJSON []byte
	var updated time.Time

	if err := row.Scan(&cp.LSN, &metadataJSON, &updated); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return connector.Checkpoint{}, ErrNotFound
		}
		return connector.Checkpoint{}, fmt.Errorf("scan checkpoint: %w", err)
	}

	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &cp.Metadata); err != nil {
			return connector.Checkpoint{}, fmt.Errorf("decode metadata: %w", err)
		}
	}
	cp.Timestamp = updated
	canonical, err := canonicalizeCheckpoint(cp)
	if err != nil {
		return connector.Checkpoint{}, fmt.Errorf("canonicalize stored checkpoint: %w", err)
	}
	return canonical, nil
}

func scanFlowCheckpoint(row pgx.Row) (connector.FlowCheckpoint, error) {
	var flowID string
	var metadataJSON []byte
	var updated time.Time
	var lsn string

	if err := row.Scan(&flowID, &lsn, &metadataJSON, &updated); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return connector.FlowCheckpoint{}, ErrNotFound
		}
		return connector.FlowCheckpoint{}, fmt.Errorf("scan flow checkpoint: %w", err)
	}

	cp := connector.Checkpoint{
		LSN:       lsn,
		Timestamp: updated,
	}
	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &cp.Metadata); err != nil {
			return connector.FlowCheckpoint{}, fmt.Errorf("decode metadata: %w", err)
		}
	}
	canonical, err := canonicalizeCheckpoint(cp)
	if err != nil {
		return connector.FlowCheckpoint{}, fmt.Errorf("canonicalize stored checkpoint for %s: %w", flowID, err)
	}
	return connector.FlowCheckpoint{FlowID: flowID, Checkpoint: canonical}, nil
}
