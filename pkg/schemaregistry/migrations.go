package schemaregistry

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

const migrationsTableSQL = `CREATE TABLE IF NOT EXISTS wallaby_schema_registry_migrations (
	version TEXT PRIMARY KEY,
	applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);`

//go:embed migrations/*.sql
var migrationFS embed.FS

func runMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, migrationsTableSQL); err != nil {
		return fmt.Errorf("ensure registry migrations table: %w", err)
	}

	applied, err := loadAppliedMigrations(ctx, pool)
	if err != nil {
		return err
	}

	files, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		return fmt.Errorf("list registry migrations: %w", err)
	}
	sort.Strings(files)

	for _, file := range files {
		version := strings.TrimPrefix(file, "migrations/")
		if applied[version] {
			continue
		}
		contents, err := migrationFS.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read registry migration %s: %w", version, err)
		}
		if _, err := pool.Exec(ctx, string(contents)); err != nil {
			return fmt.Errorf("apply registry migration %s: %w", version, err)
		}
		if _, err := pool.Exec(ctx, "INSERT INTO wallaby_schema_registry_migrations (version) VALUES ($1)", version); err != nil {
			return fmt.Errorf("record registry migration %s: %w", version, err)
		}
	}
	return nil
}

func loadAppliedMigrations(ctx context.Context, pool *pgxpool.Pool) (map[string]bool, error) {
	rows, err := pool.Query(ctx, "SELECT version FROM wallaby_schema_registry_migrations")
	if err != nil {
		return nil, fmt.Errorf("read registry migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("scan registry migrations: %w", err)
		}
		applied[version] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate registry migrations: %w", err)
	}
	return applied, nil
}
