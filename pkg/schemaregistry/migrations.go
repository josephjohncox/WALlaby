package schemaregistry

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const migrationsTableSQL = `CREATE TABLE IF NOT EXISTS wallaby_schema_registry_migrations (
	version TEXT PRIMARY KEY,
	applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);`

//go:embed migrations/*.sql
var migrationFS embed.FS

func runMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin registry migrations: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('wallaby_schema_registry_migrations', 0))`); err != nil {
		return fmt.Errorf("lock registry migrations: %w", err)
	}
	if _, err := tx.Exec(ctx, migrationsTableSQL); err != nil {
		return fmt.Errorf("ensure registry migrations table: %w", err)
	}

	applied, err := loadAppliedMigrations(ctx, tx)
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
		if _, err := tx.Exec(ctx, string(contents)); err != nil {
			return fmt.Errorf("apply registry migration %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO wallaby_schema_registry_migrations (version) VALUES ($1)", version); err != nil {
			return fmt.Errorf("record registry migration %s: %w", version, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit registry migrations: %w", err)
	}
	return nil
}

func loadAppliedMigrations(ctx context.Context, tx pgx.Tx) (map[string]bool, error) {
	rows, err := tx.Query(ctx, "SELECT version FROM wallaby_schema_registry_migrations")
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
