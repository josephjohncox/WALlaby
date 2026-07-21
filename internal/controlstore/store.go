package controlstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultWorkerMaxConns int32 = 12
	// AuthorityProtocol is the canonical mutable-control-store protocol spoken
	// by this binary. PostgreSQL triggers reject every other value, including
	// the explicit v1 value used by pre-cutover workers.
	AuthorityProtocol = "v2"
)

// ConfigurePool marks connections as understanding the canonical authority
// mutation protocol. Database triggers use this session parameter to reject
// stale binaries after the managed-durability cutover is installed.
func ConfigurePool(cfg *pgxpool.Config) {
	if cfg == nil {
		return
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	cfg.ConnConfig.RuntimeParams["wallaby.authority_protocol"] = AuthorityProtocol
}

// Store owns the worker's shared PostgreSQL control pool. Domain repositories
// borrow this pool; PostgreSQL remains the only authority for control state.
type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Store, error) {
	if dsn == "" {
		return nil, errors.New("control PostgreSQL DSN is required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse control PostgreSQL DSN: %w", err)
	}
	ConfigurePool(cfg)
	if cfg.MaxConns > defaultWorkerMaxConns {
		cfg.MaxConns = defaultWorkerMaxConns
	}
	if cfg.MaxConns < 4 {
		cfg.MaxConns = 4
	}
	cfg.MinConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect control PostgreSQL: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping control PostgreSQL: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Pool() *pgxpool.Pool {
	if s == nil {
		return nil
	}
	return s.pool
}

func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
		s.pool = nil
	}
}
