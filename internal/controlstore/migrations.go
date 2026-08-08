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

const incompatibleLedgerQuery = `SELECT COALESCE(array_agg(pg_catalog.quote_ident(n.nspname)||'.'||pg_catalog.quote_ident(c.relname) ORDER BY n.nspname,c.relname),'{}'::text[])
FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
WHERE c.relkind IN ('r','p','v','m','f') AND n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
  AND ((c.relname='wallaby_control_migrations' AND n.nspname<>'public') OR (c.relname<>'wallaby_control_migrations' AND c.relname ~ '^wallaby.*_migrations$'))`

type migrationFile struct {
	version  string
	contents []byte
	checksum string
}

// ApplyMigrations serializes all control-schema domains under one PostgreSQL
// advisory lock, records SQL checksums, and applies each migration and its
// authoritative history row in one transaction.
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool, domain string, migrationFS fs.FS, pattern string) error {
	if pool == nil || !migrationIdentifier.MatchString(domain) {
		return errors.New("migration pool and safe domain are required")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin %s migrations: %w", domain, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext('wallaby_control_migrations'))"); err != nil {
		return fmt.Errorf("lock %s migrations: %w", domain, err)
	}
	if err := rejectIncompatibleLedgers(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS public.wallaby_control_migrations (
  domain TEXT NOT NULL,
  version TEXT NOT NULL,
  sql_checksum TEXT NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (domain,version)
)`); err != nil {
		return fmt.Errorf("ensure control migration history: %w", err)
	}
	migrations, err := readMigrations(domain, migrationFS, pattern)
	if err != nil {
		return err
	}
	historyRows, err := tx.Query(ctx, `SELECT version,sql_checksum FROM public.wallaby_control_migrations WHERE domain=$1 ORDER BY version`, domain)
	if err != nil {
		return fmt.Errorf("read ordered %s migration history: %w", domain, err)
	}
	history := make([]migrationFile, 0, len(migrations))
	for historyRows.Next() {
		var applied migrationFile
		if err := historyRows.Scan(&applied.version, &applied.checksum); err != nil {
			historyRows.Close()
			return fmt.Errorf("scan ordered %s migration history: %w", domain, err)
		}
		history = append(history, applied)
	}
	if err := historyRows.Err(); err != nil {
		historyRows.Close()
		return fmt.Errorf("iterate ordered %s migration history: %w", domain, err)
	}
	historyRows.Close()
	if len(history) > len(migrations) {
		return fmt.Errorf("%s migration history is not an ordered checksum prefix: database count=%d binary=%d", domain, len(history), len(migrations))
	}
	for index := range history {
		if history[index].version != migrations[index].version {
			return fmt.Errorf("%s migration history is not an ordered prefix at %d: database=%s binary=%s", domain, index, history[index].version, migrations[index].version)
		}
		if history[index].checksum != migrations[index].checksum {
			return fmt.Errorf("%s migration %s checksum drift: database=%s binary=%s", domain, history[index].version, history[index].checksum, migrations[index].checksum)
		}
	}
	for _, migration := range migrations {
		var existing string
		err = tx.QueryRow(ctx, `SELECT sql_checksum FROM public.wallaby_control_migrations WHERE domain=$1 AND version=$2`, domain, migration.version).Scan(&existing)
		switch {
		case err == nil:
			if existing != migration.checksum {
				return fmt.Errorf("%s migration %s checksum drift: database=%s binary=%s", domain, migration.version, existing, migration.checksum)
			}
			continue
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			return fmt.Errorf("read %s migration %s history: %w", domain, migration.version, err)
		}
		// A v2 binary may need to finish an already-shipped v1 migration before
		// its monotonic v2 cutover file can replace the triggers. Scope that
		// compatibility only to this serialized migration transaction; ordinary
		// pools always advertise AuthorityProtocol. Once a v2 file is reached,
		// any later v1-only SQL fails closed against the v2 triggers.
		migrationProtocol := "v1"
		if strings.Contains(string(migration.contents), "wallaby_require_authority_protocol_v2") {
			migrationProtocol = AuthorityProtocol
		}
		if _, err := tx.Exec(ctx, "SELECT set_config('wallaby.authority_protocol',$1,true)", migrationProtocol); err != nil {
			return fmt.Errorf("select %s migration protocol for %s: %w", domain, migration.version, err)
		}
		if _, err := tx.Exec(ctx, string(migration.contents)); err != nil {
			return fmt.Errorf("apply %s migration %s: %w", domain, migration.version, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO public.wallaby_control_migrations(domain,version,sql_checksum) VALUES($1,$2,$3)`, domain, migration.version, migration.checksum); err != nil {
			return fmt.Errorf("record %s migration %s: %w", domain, migration.version, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s migrations: %w", domain, err)
	}
	return nil
}

// VerifyMigrations verifies that a domain was prepared through the sole
// authoritative ledger. It never creates tables or records migration history.
func VerifyMigrations(ctx context.Context, pool *pgxpool.Pool, domain string, migrationFS fs.FS, pattern string) error {
	if pool == nil || !migrationIdentifier.MatchString(domain) {
		return errors.New("migration pool and safe domain are required")
	}
	if err := rejectIncompatibleLedgers(ctx, pool); err != nil {
		return err
	}
	var ledgerExists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('"public"."wallaby_control_migrations"') IS NOT NULL`).Scan(&ledgerExists); err != nil {
		return fmt.Errorf("verify authoritative migration ledger: %w", err)
	}
	if !ledgerExists {
		return fmt.Errorf("%s schema is not prepared: public.wallaby_control_migrations is absent; run the explicit %s migration API before opening the component", domain, domain)
	}
	expected, err := readMigrations(domain, migrationFS, pattern)
	if err != nil {
		return err
	}
	rows, err := pool.Query(ctx, `SELECT version,sql_checksum FROM public.wallaby_control_migrations WHERE domain=$1 ORDER BY version`, domain)
	if err != nil {
		return fmt.Errorf("read authoritative %s migration history: %w", domain, err)
	}
	defer rows.Close()
	actual := make([]migrationFile, 0, len(expected))
	for rows.Next() {
		var migration migrationFile
		if err := rows.Scan(&migration.version, &migration.checksum); err != nil {
			return fmt.Errorf("scan authoritative %s migration history: %w", domain, err)
		}
		actual = append(actual, migration)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate authoritative %s migration history: %w", domain, err)
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("%s schema is not current: authoritative migration count=%d binary=%d; run the explicit %s migration API before opening the component", domain, len(actual), len(expected), domain)
	}
	for index := range expected {
		if actual[index].version != expected[index].version || actual[index].checksum != expected[index].checksum {
			return fmt.Errorf("%s schema is not current at migration %d: database=%s/%s binary=%s/%s", domain, index, actual[index].version, actual[index].checksum, expected[index].version, expected[index].checksum)
		}
	}
	return nil
}

type migrationQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func rejectIncompatibleLedgers(ctx context.Context, queryer migrationQueryer) error {
	var conflictingLedgers []string
	if err := queryer.QueryRow(ctx, incompatibleLedgerQuery).Scan(&conflictingLedgers); err != nil {
		return fmt.Errorf("inspect incompatible migration ledgers: %w", err)
	}
	if len(conflictingLedgers) > 0 {
		return fmt.Errorf("incompatible migration ledger(s) %s; public.wallaby_control_migrations is the only supported history and old ledgers are not imported", strings.Join(conflictingLedgers, ","))
	}
	return nil
}

func readMigrations(domain string, migrationFS fs.FS, pattern string) ([]migrationFile, error) {
	files, err := fs.Glob(migrationFS, pattern)
	if err != nil {
		return nil, fmt.Errorf("list %s migrations: %w", domain, err)
	}
	sort.Strings(files)
	migrations := make([]migrationFile, 0, len(files))
	for _, file := range files {
		version := strings.TrimPrefix(file, strings.TrimSuffix(pattern, "*.sql"))
		contents, err := fs.ReadFile(migrationFS, file)
		if err != nil {
			return nil, fmt.Errorf("read %s migration %s: %w", domain, version, err)
		}
		digest := sha256.Sum256(contents)
		migrations = append(migrations, migrationFile{version: version, contents: contents, checksum: hex.EncodeToString(digest[:])})
	}
	return migrations, nil
}
