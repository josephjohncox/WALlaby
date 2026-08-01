package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	postgrescodec "github.com/josephjohncox/wallaby/internal/postgres"
)

func newPool(ctx context.Context, dsn string, options map[string]string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}

	iamProvider, err := postgrescodec.NewRDSIAMTokenProvider(ctx, dsn, options)
	if err != nil {
		return nil, err
	}
	if err := iamProvider.ApplyToPoolConfig(ctx, cfg); err != nil {
		return nil, err
	}
	maxConns := parseInt(options["pool_max_conns"], 4)
	if maxConns < 1 || maxConns > 64 {
		return nil, fmt.Errorf("postgres pool_max_conns must be between 1 and 64, got %d", maxConns)
	}
	cfg.MaxConns = int32(maxConns) // #nosec G115 -- range checked above.
	cfg.MinConns = 0

	afterConnect := cfg.AfterConnect
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		postgrescodec.RegisterRawJSONCodecs(conn.TypeMap())
		if afterConnect != nil {
			return afterConnect(ctx, conn)
		}
		return nil
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

// OpenPool exposes the standard Postgres pool configuration (IAM + codecs).
func OpenPool(ctx context.Context, dsn string, options map[string]string) (*pgxpool.Pool, error) {
	return newPool(ctx, dsn, options)
}
