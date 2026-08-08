package schemaregistry

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/controlstore"
)

const migrationDomain = "schema_registry"

//go:embed migrations/*.sql
var migrationFS embed.FS

// ApplyMigrations explicitly prepares the PostgreSQL schema-registry schema
// through the shared, checksummed public.wallaby_control_migrations ledger.
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	if err := controlstore.ApplyMigrations(ctx, pool, migrationDomain, migrationFS, "migrations/*.sql"); err != nil {
		return fmt.Errorf("apply PostgreSQL schema-registry migrations: %w", err)
	}
	return nil
}

func verifyPreparedSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if err := controlstore.VerifyMigrations(ctx, pool, migrationDomain, migrationFS, "migrations/*.sql"); err != nil {
		return fmt.Errorf("PostgreSQL schema-registry schema is not prepared; run schemaregistry.ApplyMigrations before NewRegistry: %w", err)
	}
	if _, err := pool.Exec(ctx, `SELECT id,subject,schema_type,schema,schema_hash,schema_references,references_hash,version,created_at FROM public.wallaby_schema_registry LIMIT 0`); err != nil {
		return fmt.Errorf("PostgreSQL schema-registry schema is not prepared; required public.wallaby_schema_registry shape is absent: %w", err)
	}
	return nil
}
