package checkpoint

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
	if err := controlstore.ApplyMigrations(ctx, pool, "checkpoint", migrationFS, "migrations/*.sql", "wallaby_checkpoint_migrations"); err != nil {
		return fmt.Errorf("apply checkpoint migrations: %w", err)
	}
	return nil
}
