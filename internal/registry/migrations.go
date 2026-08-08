package registry

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/controlstore"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// ApplyMigrations participates in the centralized control-plane migration order.
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	if err := controlstore.ApplyMigrations(ctx, pool, "registry", migrationFS, "migrations/*.sql"); err != nil {
		return fmt.Errorf("apply registry migrations: %w", err)
	}
	return nil
}
