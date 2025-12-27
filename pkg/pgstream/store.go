package pgstream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

const (
	DeliveryStatusAvailable = "available"
	DeliveryStatusClaimed   = "claimed"
	DeliveryStatusAcked     = "acked"
)

// Message represents a stream event stored in Postgres.
type Message struct {
	ID         int64
	Stream     string
	Namespace  string
	Table      string
	LSN        string
	WireFormat connector.WireFormat
	Payload    []byte
	CreatedAt  time.Time
}

// Store persists stream events and delivery state.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(ctx context.Context, dsn string) (*Store, error) {
	if dsn == "" {
		return nil, errors.New("postgres dsn is required")
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

	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) Enqueue(ctx context.Context, stream string, messages []Message) error {
	if s.pool == nil {
		return errors.New("stream store not initialized")
	}
	if stream == "" {
		return errors.New("stream name is required")
	}
	if len(messages) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, msg := range messages {
		batch.Queue(
			`INSERT INTO stream_events (stream, namespace, table_name, lsn, wire_format, payload)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			stream,
			nullIfEmpty(msg.Namespace),
			nullIfEmpty(msg.Table),
			nullIfEmpty(msg.LSN),
			nullIfEmpty(string(msg.WireFormat)),
			msg.Payload,
		)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < len(messages); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("insert stream event: %w", err)
		}
	}

	return nil
}

func (s *Store) Claim(ctx context.Context, stream, consumerGroup, consumerID string, maxMessages int, visibilityTimeout time.Duration) ([]Message, error) {
	if s.pool == nil {
		return nil, errors.New("stream store not initialized")
	}
	if stream == "" {
		return nil, errors.New("stream name is required")
	}
	if consumerGroup == "" {
		return nil, errors.New("consumer group is required")
	}
	if maxMessages <= 0 {
		maxMessages = 100
	}
	if visibilityTimeout <= 0 {
		visibilityTimeout = 30 * time.Second
	}

	visSeconds := int64(visibilityTimeout.Seconds())
	if visSeconds <= 0 {
		visSeconds = 30
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin claim: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	rows, err := tx.Query(ctx, `
WITH candidate AS (
  SELECT e.id
  FROM stream_events e
  LEFT JOIN stream_deliveries d
    ON e.id = d.event_id AND d.consumer_group = $2
  WHERE e.stream = $1
    AND (d.event_id IS NULL OR (d.status IN ('available', 'claimed') AND d.visible_at <= now()))
  ORDER BY e.id
  LIMIT $3
  FOR UPDATE SKIP LOCKED
),
upsert AS (
  INSERT INTO stream_deliveries (event_id, consumer_group, status, visible_at, attempts, consumer_id)
  SELECT id, $2, 'claimed', now() + ($4::bigint * interval '1 second'), 1, $5
  FROM candidate
  ON CONFLICT (event_id, consumer_group) DO UPDATE
    SET status = 'claimed',
        visible_at = now() + ($4::bigint * interval '1 second'),
        attempts = stream_deliveries.attempts + 1,
        consumer_id = $5,
        updated_at = now()
  RETURNING event_id
)
SELECT e.id, e.stream, e.namespace, e.table_name, e.lsn, e.wire_format, e.payload, e.created_at
FROM stream_events e
JOIN upsert u ON e.id = u.event_id
ORDER BY e.id`, stream, consumerGroup, maxMessages, visSeconds, consumerID)
	if err != nil {
		return nil, fmt.Errorf("claim stream events: %w", err)
	}
	defer rows.Close()

	messages := make([]Message, 0)
	for rows.Next() {
		var msg Message
		var wireFormat *string
		if err := rows.Scan(&msg.ID, &msg.Stream, &msg.Namespace, &msg.Table, &msg.LSN, &wireFormat, &msg.Payload, &msg.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan stream event: %w", err)
		}
		if wireFormat != nil {
			msg.WireFormat = connector.WireFormat(*wireFormat)
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stream events: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}

	return messages, nil
}

func (s *Store) Ack(ctx context.Context, stream, consumerGroup string, ids []int64) (int64, error) {
	if s.pool == nil {
		return 0, errors.New("stream store not initialized")
	}
	if stream == "" {
		return 0, errors.New("stream name is required")
	}
	if consumerGroup == "" {
		return 0, errors.New("consumer group is required")
	}
	if len(ids) == 0 {
		return 0, nil
	}

	cmd, err := s.pool.Exec(ctx,
		`UPDATE stream_deliveries d
		 SET status = 'acked', visible_at = now(), updated_at = now()
		 FROM stream_events e
		 WHERE d.event_id = e.id
		   AND e.stream = $1
		   AND d.consumer_group = $2
		   AND d.event_id = ANY($3::bigint[])`,
		stream, consumerGroup, ids,
	)
	if err != nil {
		return 0, fmt.Errorf("ack stream events: %w", err)
	}

	return cmd.RowsAffected(), nil
}

type ReplayOptions struct {
	FromLSN string
	Since   time.Time
}

func (s *Store) Replay(ctx context.Context, stream, consumerGroup string, opts ReplayOptions) (int64, error) {
	if s.pool == nil {
		return 0, errors.New("stream store not initialized")
	}
	if stream == "" {
		return 0, errors.New("stream name is required")
	}
	if consumerGroup == "" {
		return 0, errors.New("consumer group is required")
	}

	query := `UPDATE stream_deliveries d
	SET status = 'available', visible_at = now(), updated_at = now()
	FROM stream_events e
	WHERE d.event_id = e.id
	  AND e.stream = $1
	  AND d.consumer_group = $2`
	args := []any{stream, consumerGroup}

	if opts.FromLSN != "" {
		query += " AND e.lsn >= $3"
		args = append(args, opts.FromLSN)
		if !opts.Since.IsZero() {
			query += " AND e.created_at >= $4"
			args = append(args, opts.Since)
		}
	} else if !opts.Since.IsZero() {
		query += " AND e.created_at >= $3"
		args = append(args, opts.Since)
	}

	cmd, err := s.pool.Exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("replay stream events: %w", err)
	}

	return cmd.RowsAffected(), nil
}

func nullIfEmpty(value string) interface{} {
	if value == "" {
		return nil
	}
	return value
}
