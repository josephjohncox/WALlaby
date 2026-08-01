package bootstrap

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
	if err := controlstore.ApplyMigrations(ctx, pool, "bootstrap", migrationFS, "migrations/*.sql", ""); err != nil {
		return fmt.Errorf("apply bootstrap migrations: %w", err)
	}
	return nil
}
