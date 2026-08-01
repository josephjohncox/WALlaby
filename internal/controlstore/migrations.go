package controlstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var migrationIdentifier = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// ApplyMigrations serializes all control-schema domains under one PostgreSQL
// advisory lock, records SQL checksums, and applies each migration and its
// history row in one transaction. legacyTable imports and dual-records the
// pre-coordinator history used by older Wallaby releases.
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool, domain string, migrationFS fs.FS, pattern, legacyTable string) error {
	if pool == nil || !migrationIdentifier.MatchString(domain) {
		return errors.New("migration pool and safe domain are required")
	}
	if legacyTable != "" && !migrationIdentifier.MatchString(legacyTable) {
		return fmt.Errorf("unsafe legacy migration table %q", legacyTable)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin %s migrations: %w", domain, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext('wallaby_control_migrations'))"); err != nil {
		return fmt.Errorf("lock %s migrations: %w", domain, err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS wallaby_control_migrations (
  domain TEXT NOT NULL,
  version TEXT NOT NULL,
  sql_checksum TEXT NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (domain,version)
)`); err != nil {
		return fmt.Errorf("ensure control migration history: %w", err)
	}
	files, err := fs.Glob(migrationFS, pattern)
	if err != nil {
		return fmt.Errorf("list %s migrations: %w", domain, err)
	}
	sort.Strings(files)
	legacy, err := loadLegacyHistory(ctx, tx, legacyTable)
	if err != nil {
		return err
	}
	for _, file := range files {
		version := strings.TrimPrefix(file, strings.TrimSuffix(pattern, "*.sql"))
		contents, err := fs.ReadFile(migrationFS, file)
		if err != nil {
			return fmt.Errorf("read %s migration %s: %w", domain, version, err)
		}
		digest := sha256.Sum256(contents)
		checksum := hex.EncodeToString(digest[:])
		var existing string
		err = tx.QueryRow(ctx, `SELECT sql_checksum FROM wallaby_control_migrations WHERE domain=$1 AND version=$2`, domain, version).Scan(&existing)
		switch {
		case err == nil:
			if existing != checksum {
				return fmt.Errorf("%s migration %s checksum drift: database=%s binary=%s", domain, version, existing, checksum)
			}
			continue
		case errors.Is(err, pgx.ErrNoRows):
			// Import a legacy applied version without replaying its SQL.
			if legacy[version] {
				if _, err := tx.Exec(ctx, `INSERT INTO wallaby_control_migrations(domain,version,sql_checksum) VALUES($1,$2,$3)`, domain, version, checksum); err != nil {
					return fmt.Errorf("import %s migration %s: %w", domain, version, err)
				}
				continue
			}
		case err != nil:
			return fmt.Errorf("read %s migration %s history: %w", domain, version, err)
		}
		if _, err := tx.Exec(ctx, string(contents)); err != nil {
			return fmt.Errorf("apply %s migration %s: %w", domain, version, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO wallaby_control_migrations(domain,version,sql_checksum) VALUES($1,$2,$3)`, domain, version, checksum); err != nil {
			return fmt.Errorf("record %s migration %s: %w", domain, version, err)
		}
		if legacyTable != "" {
			query := fmt.Sprintf("INSERT INTO %s(version) VALUES($1) ON CONFLICT(version) DO NOTHING", legacyTable) // #nosec G201 -- identifier is allowlisted above.
			if _, err := tx.Exec(ctx, query, version); err != nil {
				return fmt.Errorf("record legacy %s migration %s: %w", domain, version, err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s migrations: %w", domain, err)
	}
	return nil
}

func loadLegacyHistory(ctx context.Context, tx pgx.Tx, table string) (map[string]bool, error) {
	result := make(map[string]bool)
	if table == "" {
		return result, nil
	}
	create := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  version TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`, table) // #nosec G201 -- identifier is allowlisted before this helper.
	if _, err := tx.Exec(ctx, create); err != nil {
		return nil, fmt.Errorf("ensure legacy migration history %s: %w", table, err)
	}
	query := fmt.Sprintf("SELECT version FROM %s", table) // #nosec G201 -- identifier is allowlisted before this helper.
	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("read legacy migration history %s: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("scan legacy migration history %s: %w", table, err)
		}
		result[version] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate legacy migration history %s: %w", table, err)
	}
	return result, nil
}
