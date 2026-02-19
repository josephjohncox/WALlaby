package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	publicationModeAdd  = "add"
	publicationModeSync = "sync"
)

// NormalizeSyncPublicationMode normalizes and validates a publication sync mode.
//
// It accepts case-insensitive "add" or "sync"; empty values default to "add".
func NormalizeSyncPublicationMode(raw string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(raw))
	if mode == "" {
		return publicationModeAdd, nil
	}
	if mode != publicationModeAdd && mode != publicationModeSync {
		return "", fmt.Errorf("invalid sync publication mode %q", mode)
	}
	return mode, nil
}

func ensureReplication(ctx context.Context, dsn string, options map[string]string, publication string, tables []string, ensurePublication, validateSettings, captureDDL bool, ddlSchema, ddlTrigger, ddlPrefix string) error {
	pool, err := newPool(ctx, dsn, options)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	if validateSettings {
		if err := validateLogicalReplication(ctx, pool); err != nil {
			return err
		}
	}

	if ensurePublication {
		if publication == "" {
			return errors.New("publication is required")
		}
		exists, err := publicationExists(ctx, pool, publication)
		if err != nil {
			return err
		}
		if !exists {
			if err := createPublication(ctx, pool, publication, tables); err != nil {
				return err
			}
		}
	}

	if captureDDL {
		if err := ensureDDLCapture(ctx, pool, ddlSchema, ddlTrigger, ddlPrefix); err != nil {
			return err
		}
	}

	return nil
}

// ListPublicationTables returns schema-qualified tables in a publication.
func ListPublicationTables(ctx context.Context, dsn, publication string, options map[string]string) ([]string, error) {
	if dsn == "" {
		return nil, errors.New("postgres dsn is required")
	}
	if publication == "" {
		return nil, errors.New("publication is required")
	}
	pool, err := newPool(ctx, dsn, options)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return listPublicationTables(ctx, pool, publication)
}

// AddPublicationTables adds tables to a publication.
func AddPublicationTables(ctx context.Context, dsn, publication string, tables []string, options map[string]string) error {
	if len(tables) == 0 {
		return nil
	}
	pool, err := newPool(ctx, dsn, options)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	return alterPublicationTables(ctx, pool, publication, tables, "ADD")
}

// DropPublicationTables removes tables from a publication.
func DropPublicationTables(ctx context.Context, dsn, publication string, tables []string, options map[string]string) error {
	if len(tables) == 0 {
		return nil
	}
	pool, err := newPool(ctx, dsn, options)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	return alterPublicationTables(ctx, pool, publication, tables, "DROP")
}

// SyncPublicationTables adds missing tables and optionally drops extras.
func SyncPublicationTables(ctx context.Context, dsn, publication string, tables []string, mode string, options map[string]string) ([]string, []string, error) {
	if dsn == "" {
		return nil, nil, errors.New("postgres dsn is required")
	}
	if publication == "" {
		return nil, nil, errors.New("publication is required")
	}
	mode, err := NormalizeSyncPublicationMode(mode)
	if err != nil {
		return nil, nil, err
	}
	pool, err := newPool(ctx, dsn, options)
	if err != nil {
		return nil, nil, fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return nil, nil, fmt.Errorf("ping postgres: %w", err)
	}

	current, err := listPublicationTables(ctx, pool, publication)
	if err != nil {
		return nil, nil, err
	}
	currentSet := make(map[string]struct{}, len(current))
	for _, table := range current {
		currentSet[strings.ToLower(table)] = struct{}{}
	}

	desiredSet := make(map[string]string, len(tables))
	for _, table := range tables {
		desiredSet[strings.ToLower(table)] = table
	}

	var toAdd []string
	for _, table := range tables {
		if _, ok := currentSet[strings.ToLower(table)]; !ok {
			toAdd = append(toAdd, table)
		}
	}

	var toDrop []string
	if strings.ToLower(mode) == "sync" {
		for _, table := range current {
			if _, ok := desiredSet[strings.ToLower(table)]; !ok {
				toDrop = append(toDrop, table)
			}
		}
	}

	if len(toAdd) > 0 {
		if err := alterPublicationTables(ctx, pool, publication, toAdd, "ADD"); err != nil {
			return nil, nil, err
		}
	}
	if len(toDrop) > 0 {
		if err := alterPublicationTables(ctx, pool, publication, toDrop, "DROP"); err != nil {
			return nil, nil, err
		}
	}

	return toAdd, toDrop, nil
}

// ScrapeTables discovers tables in schemas.
func ScrapeTables(ctx context.Context, dsn string, schemas []string, options map[string]string) ([]string, error) {
	if dsn == "" {
		return nil, errors.New("postgres dsn is required")
	}
	pool, err := newPool(ctx, dsn, options)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return listTables(ctx, pool, schemas)
}

func validateLogicalReplication(ctx context.Context, pool *pgxpool.Pool) error {
	var walLevel string
	if err := pool.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		return fmt.Errorf("read wal_level: %w", err)
	}
	if strings.ToLower(walLevel) != "logical" {
		return fmt.Errorf("wal_level must be logical (current: %s)", walLevel)
	}

	var maxSlots int
	if err := pool.QueryRow(ctx, "SELECT current_setting('max_replication_slots')::int").Scan(&maxSlots); err != nil {
		return fmt.Errorf("read max_replication_slots: %w", err)
	}
	if maxSlots < 1 {
		return errors.New("max_replication_slots must be >= 1")
	}

	var maxSenders int
	if err := pool.QueryRow(ctx, "SELECT current_setting('max_wal_senders')::int").Scan(&maxSenders); err != nil {
		return fmt.Errorf("read max_wal_senders: %w", err)
	}
	if maxSenders < 1 {
		return errors.New("max_wal_senders must be >= 1")
	}

	return nil
}

func ensureDDLCapture(ctx context.Context, pool *pgxpool.Pool, schema, triggerName, prefix string) error {
	if schema == "" {
		schema = "wallaby"
	}
	if triggerName == "" {
		triggerName = "wallaby_ddl_capture"
	}
	if prefix == "" {
		prefix = "wallaby_ddl"
	}

	schemaIdent := pgx.Identifier{schema}.Sanitize()
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaIdent)); err != nil {
		return fmt.Errorf("create ddl schema: %w", err)
	}

	funcIdent := fmt.Sprintf("%s.%s", schemaIdent, pgx.Identifier{"emit_ddl"}.Sanitize())
	funcSQL := fmt.Sprintf(
		`CREATE OR REPLACE FUNCTION %s()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
  ddl TEXT;
BEGIN
  ddl := current_query();
  IF ddl IS NULL OR ddl = '' THEN
    RETURN;
  END IF;
  PERFORM pg_logical_emit_message(true, %s, ddl);
END;
$$;`,
		funcIdent,
		quoteLiteral(prefix),
	)
	if _, err := pool.Exec(ctx, funcSQL); err != nil {
		return fmt.Errorf("create ddl capture function: %w", err)
	}

	exists, err := ddlTriggerExists(ctx, pool, triggerName)
	if err != nil {
		return err
	}
	if !exists {
		triggerIdent := pgx.Identifier{triggerName}.Sanitize()
		ddlTriggerSQL := fmt.Sprintf(
			"CREATE EVENT TRIGGER %s ON ddl_command_end EXECUTE FUNCTION %s()",
			triggerIdent,
			funcIdent,
		)
		if _, err := pool.Exec(ctx, ddlTriggerSQL); err != nil {
			return fmt.Errorf("create ddl event trigger: %w", err)
		}
	}

	return nil
}

func ddlTriggerExists(ctx context.Context, pool *pgxpool.Pool, name string) (bool, error) {
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = $1)", name).Scan(&exists); err != nil {
		return false, fmt.Errorf("check ddl trigger: %w", err)
	}
	return exists, nil
}

func quoteLiteral(value string) string {
	escaped := strings.ReplaceAll(value, "'", "''")
	return "'" + escaped + "'"
}

func publicationExists(ctx context.Context, pool *pgxpool.Pool, name string) (bool, error) {
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", name).Scan(&exists); err != nil {
		return false, fmt.Errorf("check publication: %w", err)
	}
	return exists, nil
}

func createPublication(ctx context.Context, pool *pgxpool.Pool, name string, tables []string) error {
	if len(tables) == 0 {
		_, err := pool.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", pgx.Identifier{name}.Sanitize()))
		if err != nil {
			return fmt.Errorf("create publication: %w", err)
		}
		return nil
	}

	qualified := make([]string, 0, len(tables))
	for _, table := range tables {
		qualifiedTable, err := qualifyTable(table)
		if err != nil {
			return err
		}
		qualified = append(qualified, qualifiedTable)
	}

	query := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pgx.Identifier{name}.Sanitize(), strings.Join(qualified, ", "))
	if _, err := pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("create publication: %w", err)
	}
	return nil
}

func listTables(ctx context.Context, pool *pgxpool.Pool, schemas []string) ([]string, error) {
	if len(schemas) == 0 {
		return nil, nil
	}
	rows, err := pool.Query(ctx,
		`SELECT table_schema, table_name
		 FROM information_schema.tables
		 WHERE table_schema = ANY($1::text[])
		   AND table_type = 'BASE TABLE'
		 ORDER BY table_schema, table_name`, schemas)
	if err != nil {
		return nil, fmt.Errorf("load tables: %w", err)
	}
	defer rows.Close()

	out := make([]string, 0)
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return nil, fmt.Errorf("scan table: %w", err)
		}
		out = append(out, fmt.Sprintf("%s.%s", schema, table))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tables: %w", err)
	}
	return out, nil
}

func listPublicationTables(ctx context.Context, pool *pgxpool.Pool, name string) ([]string, error) {
	if name == "" {
		return nil, errors.New("publication is required")
	}
	var allTables bool
	if err := pool.QueryRow(ctx, "SELECT puballtables FROM pg_publication WHERE pubname = $1", name).Scan(&allTables); err != nil {
		return nil, fmt.Errorf("read publication: %w", err)
	}
	if allTables {
		return nil, fmt.Errorf("publication %s uses FOR ALL TABLES", name)
	}

	rows, err := pool.Query(ctx,
		`SELECT schemaname, tablename
		 FROM pg_publication_tables
		 WHERE pubname = $1
		 ORDER BY schemaname, tablename`, name)
	if err != nil {
		return nil, fmt.Errorf("list publication tables: %w", err)
	}
	defer rows.Close()

	out := make([]string, 0)
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return nil, fmt.Errorf("scan publication table: %w", err)
		}
		out = append(out, fmt.Sprintf("%s.%s", schema, table))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate publication tables: %w", err)
	}
	return out, nil
}

func alterPublicationTables(ctx context.Context, pool *pgxpool.Pool, name string, tables []string, action string) error {
	if name == "" {
		return errors.New("publication is required")
	}
	if len(tables) == 0 {
		return nil
	}

	var allTables bool
	if err := pool.QueryRow(ctx, "SELECT puballtables FROM pg_publication WHERE pubname = $1", name).Scan(&allTables); err != nil {
		return fmt.Errorf("read publication: %w", err)
	}
	if allTables {
		return fmt.Errorf("publication %s uses FOR ALL TABLES", name)
	}

	qualified := make([]string, 0, len(tables))
	for _, table := range tables {
		qualifiedTable, err := qualifyTable(table)
		if err != nil {
			return err
		}
		qualified = append(qualified, qualifiedTable)
	}

	switch strings.ToUpper(action) {
	case "ADD":
		query := fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", pgx.Identifier{name}.Sanitize(), strings.Join(qualified, ", "))
		if _, err := pool.Exec(ctx, query); err != nil {
			return fmt.Errorf("alter publication add: %w", err)
		}
	case "DROP":
		query := fmt.Sprintf("ALTER PUBLICATION %s DROP TABLE %s", pgx.Identifier{name}.Sanitize(), strings.Join(qualified, ", "))
		if _, err := pool.Exec(ctx, query); err != nil {
			return fmt.Errorf("alter publication drop: %w", err)
		}
	default:
		return fmt.Errorf("unsupported publication action %q", action)
	}
	return nil
}

func qualifyTable(name string) (string, error) {
	parts := strings.Split(name, ".")
	switch len(parts) {
	case 1:
		return pgx.Identifier{"public", parts[0]}.Sanitize(), nil
	case 2:
		return pgx.Identifier{parts[0], parts[1]}.Sanitize(), nil
	default:
		return "", fmt.Errorf("invalid table name %q", name)
	}
}
