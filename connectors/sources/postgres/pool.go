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

	iamProvider, err := newRDSIAMTokenProvider(ctx, dsn, options)
	if err != nil {
		return nil, err
	}
	if err := iamProvider.ApplyToPoolConfig(ctx, cfg); err != nil {
		return nil, err
	}

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
