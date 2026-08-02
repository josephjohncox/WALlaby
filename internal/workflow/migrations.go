package workflow

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/controlstore"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

func runMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	if err := ApplyMigrations(ctx, pool); err != nil {
		return fmt.Errorf("run workflow migrations: %w", err)
	}
	return nil
}

// ApplyMigrations participates in the centralized control-plane migration order.
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	if err := controlstore.ApplyMigrations(ctx, pool, "workflow", migrationFS, "migrations/*.sql", "wallaby_schema_migrations"); err != nil {
		return fmt.Errorf("apply workflow migrations: %w", err)
	}
	return nil
}
