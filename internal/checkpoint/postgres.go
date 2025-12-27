package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// PostgresStore persists checkpoints in Postgres.
type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	if dsn == "" {
		return nil, errors.New("postgres DSN is required")
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	if err := runMigrations(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}

	return &PostgresStore{pool: pool}, nil
}

func (p *PostgresStore) Close() {
	if p.pool != nil {
		p.pool.Close()
	}
}

func (p *PostgresStore) Get(ctx context.Context, flowID string) (connector.Checkpoint, error) {
	row := p.pool.QueryRow(ctx, "SELECT lsn, metadata, updated_at FROM checkpoints WHERE flow_id = $1", flowID)
	return scanCheckpoint(row)
}

func (p *PostgresStore) Put(ctx context.Context, flowID string, checkpoint connector.Checkpoint) error {
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

	_, err = p.pool.Exec(ctx,
		`INSERT INTO checkpoints (flow_id, lsn, metadata, updated_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (flow_id)
		 DO UPDATE SET lsn = EXCLUDED.lsn, metadata = EXCLUDED.metadata, updated_at = EXCLUDED.updated_at`,
		flowID, checkpoint.LSN, metadataJSON, checkpoint.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("upsert checkpoint: %w", err)
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

	return cp, nil
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

	return connector.FlowCheckpoint{FlowID: flowID, Checkpoint: cp}, nil
}
