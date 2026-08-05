package pgstream

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/controlstore"
)

const migrationDomain = "pgstream"

//go:embed migrations/*.sql
var migrationFS embed.FS

// ApplyMigrations explicitly prepares the pgstream schema through the shared,
// checksummed public.wallaby_control_migrations ledger.
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	if err := controlstore.ApplyMigrations(ctx, pool, migrationDomain, migrationFS, "migrations/*.sql"); err != nil {
		return fmt.Errorf("apply pgstream migrations: %w", err)
	}
	return nil
}

func verifyPreparedSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if err := controlstore.VerifyMigrations(ctx, pool, migrationDomain, migrationFS, "migrations/*.sql"); err != nil {
		return fmt.Errorf("pgstream schema is not prepared; run pgstream.ApplyMigrations before NewStore: %w", err)
	}
	if _, err := pool.Exec(ctx, `SELECT id,stream,namespace,table_name,lsn,wire_format,payload,registry_subject,registry_id,registry_version,created_at FROM public.stream_events LIMIT 0`); err != nil {
		return fmt.Errorf("pgstream schema is not prepared; required public.stream_events shape is absent: %w", err)
	}
	if _, err := pool.Exec(ctx, `SELECT event_id,consumer_group,status,visible_at,attempts,consumer_id,updated_at FROM public.stream_deliveries LIMIT 0`); err != nil {
		return fmt.Errorf("pgstream schema is not prepared; required public.stream_deliveries shape is absent: %w", err)
	}
	return nil
}
