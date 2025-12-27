package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/josephjohncox/ductstream/pkg/connector"
)

const (
	optDSN          = "dsn"
	optDatabase     = "database"
	optSchema       = "schema"
	optTable        = "table"
	optWriteMode    = "write_mode"
	optMetaTable    = "meta_table"
	optMetaSchema   = "meta_schema"
	optMetaDatabase = "meta_database"
	optMetaEnabled  = "meta_table_enabled"
	optMetaPKPrefix = "meta_pk_prefix"
	optMetaEngine   = "meta_engine"
	optMetaOrderBy  = "meta_order_by"
	optFlowID       = "flow_id"

	writeModeTarget   = "target"
	writeModeAppend   = "append"
	defaultMetaSchema = "ductstream_meta"
	defaultMetaTable  = "__metadata"
	defaultMetaPKPref = "pk_"
	defaultMetaEngine = "MergeTree"
)

// Destination writes change events into ClickHouse tables.
type Destination struct {
	spec         connector.Spec
	db           *sql.DB
	writeMode    string
	metaEnabled  bool
	metaSchema   string
	metaTable    string
	metaPKPrefix string
	flowID       string
	metaColumns  map[string]struct{}
	metaEngine   string
	metaOrderBy  string
}

func (d *Destination) Open(ctx context.Context, spec connector.Spec) error {
	d.spec = spec
	dsn := spec.Options[optDSN]
	if dsn == "" {
		return errors.New("clickhouse dsn is required")
	}

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("open clickhouse: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("ping clickhouse: %w", err)
	}
	d.db = db

	d.writeMode = strings.ToLower(spec.Options[optWriteMode])
	if d.writeMode == "" {
		d.writeMode = writeModeTarget
	}

	d.metaEnabled = parseBool(spec.Options[optMetaEnabled], true)
	d.metaSchema = strings.TrimSpace(spec.Options[optMetaSchema])
	if d.metaSchema == "" {
		d.metaSchema = strings.TrimSpace(spec.Options[optMetaDatabase])
	}
	if d.metaSchema == "" {
		d.metaSchema = defaultMetaSchema
	}
	d.metaTable = strings.TrimSpace(spec.Options[optMetaTable])
	if d.metaTable == "" {
		d.metaTable = defaultMetaTable
	}
	d.metaPKPrefix = spec.Options[optMetaPKPrefix]
	if d.metaPKPrefix == "" {
		d.metaPKPrefix = defaultMetaPKPref
	}
	d.flowID = spec.Options[optFlowID]
	d.metaColumns = map[string]struct{}{}
	d.metaEngine = strings.TrimSpace(spec.Options[optMetaEngine])
	if d.metaEngine == "" {
		d.metaEngine = defaultMetaEngine
	}
	d.metaOrderBy = strings.TrimSpace(spec.Options[optMetaOrderBy])
	if d.metaOrderBy == "" {
		d.metaOrderBy = "flow_id, source_schema, source_table, key_json"
	}

	if d.metaEnabled {
		if err := d.ensureMetaTable(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) error {
	if d.db == nil {
		return errors.New("clickhouse destination not initialized")
	}
	if len(batch.Records) == 0 {
		return nil
	}

	mode := d.writeMode
	if mode == "" {
		mode = writeModeTarget
	}

	for _, record := range batch.Records {
		if err := d.applyRecord(ctx, batch.Schema, record, mode); err != nil {
			return err
		}
		if d.metaEnabled {
			if err := d.upsertMetadata(ctx, batch.Schema, record, batch.Checkpoint); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Destination) Close(_ context.Context) error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

func (d *Destination) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		SupportsDDL:           true,
		SupportsSchemaChanges: true,
		SupportsStreaming:     true,
		SupportsBulkLoad:      true,
		SupportedWireFormats: []connector.WireFormat{
			connector.WireFormatArrow,
			connector.WireFormatParquet,
			connector.WireFormatAvro,
			connector.WireFormatProto,
			connector.WireFormatJSON,
		},
	}
}

func (d *Destination) applyRecord(ctx context.Context, schema connector.Schema, record connector.Record, mode string) error {
	target := d.targetTable(schema, record)
	switch record.Operation {
	case connector.OpDelete:
		if mode == writeModeAppend {
			return d.insertRow(ctx, target, schema, record)
		}
		return d.deleteRow(ctx, target, record)
	case connector.OpUpdate:
		if mode == writeModeAppend {
			return d.insertRow(ctx, target, schema, record)
		}
		return d.updateRow(ctx, target, schema, record)
	case connector.OpInsert, connector.OpLoad:
		return d.insertRow(ctx, target, schema, record)
	default:
		return nil
	}
}

func (d *Destination) targetTable(schema connector.Schema, record connector.Record) string {
	table := strings.TrimSpace(d.spec.Options[optTable])
	targetSchema := strings.TrimSpace(d.spec.Options[optDatabase])
	if targetSchema == "" {
		targetSchema = strings.TrimSpace(d.spec.Options[optSchema])
	}
	if table == "" {
		table = record.Table
	}
	if strings.Contains(table, ".") {
		return quoteQualified(table, '`')
	}
	if targetSchema == "" {
		targetSchema = schema.Namespace
	}
	if targetSchema == "" {
		return quoteIdent(table, '`')
	}
	return quoteIdent(targetSchema, '`') + "." + quoteIdent(table, '`')
}

func (d *Destination) insertRow(ctx context.Context, target string, schema connector.Schema, record connector.Record) error {
	cols, vals := recordColumns(schema, record)
	if len(cols) == 0 {
		return nil
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", target, quoteColumns(cols, '`'), placeholders(len(cols)))
	if _, err := d.db.ExecContext(ctx, stmt, vals...); err != nil {
		return fmt.Errorf("insert row: %w", err)
	}
	return nil
}

func (d *Destination) updateRow(ctx context.Context, target string, schema connector.Schema, record connector.Record) error {
	key, err := decodeKey(record.Key)
	if err != nil {
		return err
	}
	if len(key) == 0 {
		return errors.New("update requires record key")
	}

	cols, vals := recordColumns(schema, record)
	if len(cols) == 0 {
		return nil
	}

	setClause := make([]string, 0, len(cols))
	for _, col := range cols {
		setClause = append(setClause, fmt.Sprintf("%s = ?", quoteIdent(col, '`')))
	}

	whereClause, whereArgs := whereFromKey(key, '`', "")
	args := append(vals, whereArgs...)

	stmt := fmt.Sprintf("ALTER TABLE %s UPDATE %s WHERE %s", target, strings.Join(setClause, ", "), whereClause)
	if _, err := d.db.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("update row: %w", err)
	}
	return nil
}

func (d *Destination) deleteRow(ctx context.Context, target string, record connector.Record) error {
	key, err := decodeKey(record.Key)
	if err != nil {
		return err
	}
	if len(key) == 0 {
		return errors.New("delete requires record key")
	}

	whereClause, args := whereFromKey(key, '`', "")
	stmt := fmt.Sprintf("ALTER TABLE %s DELETE WHERE %s", target, whereClause)
	if _, err := d.db.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("delete row: %w", err)
	}
	return nil
}

func (d *Destination) ensureMetaTable(ctx context.Context) error {
	if d.metaSchema == "" || d.metaTable == "" {
		return errors.New("meta schema and table are required")
	}
	schemaIdent := quoteIdent(d.metaSchema, '`')
	if _, err := d.db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", schemaIdent)); err != nil {
		return fmt.Errorf("create meta database: %w", err)
	}
	tableIdent := schemaIdent + "." + quoteIdent(d.metaTable, '`')
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  flow_id String,
  source_schema String,
  source_table String,
  synced_at DateTime64(9),
  is_deleted UInt8,
  lsn String,
  operation String,
  key_json String
) ENGINE = %s ORDER BY (%s)`, tableIdent, d.metaEngine, d.metaOrderBy)
	if _, err := d.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("create meta table: %w", err)
	}
	return d.refreshMetaColumns(ctx)
}

func (d *Destination) refreshMetaColumns(ctx context.Context) error {
	rows, err := d.db.QueryContext(ctx,
		`SELECT name FROM system.columns WHERE database = ? AND table = ?`,
		d.metaSchema, d.metaTable)
	if err != nil {
		return fmt.Errorf("load meta columns: %w", err)
	}
	defer rows.Close()

	d.metaColumns = map[string]struct{}{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("scan meta column: %w", err)
		}
		d.metaColumns[strings.ToLower(name)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate meta columns: %w", err)
	}
	return nil
}

func (d *Destination) upsertMetadata(ctx context.Context, schema connector.Schema, record connector.Record, checkpoint connector.Checkpoint) error {
	key, err := decodeKey(record.Key)
	if err != nil {
		return err
	}
	if len(key) == 0 {
		return nil
	}

	pkCols := make([]string, 0, len(key))
	pkVals := make([]any, 0, len(key))
	for col, val := range key {
		columnName := d.metaPKPrefix + col
		pkCols = append(pkCols, columnName)
		pkVals = append(pkVals, val)
		if err := d.ensureMetaColumn(ctx, columnName); err != nil {
			return err
		}
	}

	target := quoteIdent(d.metaSchema, '`') + "." + quoteIdent(d.metaTable, '`')
	whereClause, whereArgs := whereFromKey(key, '`', d.metaPKPrefix)
	whereClause = "flow_id = ? AND source_schema = ? AND source_table = ? AND " + whereClause
	whereArgs = append([]any{d.flowID, schema.Namespace, record.Table}, whereArgs...)

	deleteStmt := fmt.Sprintf("ALTER TABLE %s DELETE WHERE %s", target, whereClause)
	if _, err := d.db.ExecContext(ctx, deleteStmt, whereArgs...); err != nil {
		return fmt.Errorf("delete meta row: %w", err)
	}

	syncedAt := record.Timestamp
	if syncedAt.IsZero() {
		syncedAt = time.Now().UTC()
	}
	keyJSON := string(record.Key)
	if keyJSON == "" {
		raw, _ := json.Marshal(key)
		keyJSON = string(raw)
	}

	columns := []string{"flow_id", "source_schema", "source_table", "synced_at", "is_deleted", "lsn", "operation", "key_json"}
	values := []any{d.flowID, schema.Namespace, record.Table, syncedAt, record.Operation == connector.OpDelete, checkpoint.LSN, string(record.Operation), keyJSON}
	columns = append(columns, pkCols...)
	values = append(values, pkVals...)

	insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", target, quoteColumns(columns, '`'), placeholders(len(columns)))
	if _, err := d.db.ExecContext(ctx, insertStmt, values...); err != nil {
		return fmt.Errorf("insert meta row: %w", err)
	}
	return nil
}

func (d *Destination) ensureMetaColumn(ctx context.Context, column string) error {
	if column == "" {
		return nil
	}
	key := strings.ToLower(column)
	if _, ok := d.metaColumns[key]; ok {
		return nil
	}
	target := quoteIdent(d.metaSchema, '`') + "." + quoteIdent(d.metaTable, '`')
	stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s String", target, quoteIdent(column, '`'))
	if _, err := d.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("add meta column: %w", err)
	}
	d.metaColumns[key] = struct{}{}
	return nil
}

func recordColumns(schema connector.Schema, record connector.Record) ([]string, []any) {
	if record.After == nil {
		return nil, nil
	}
	cols := make([]string, 0, len(schema.Columns))
	vals := make([]any, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		if val, ok := record.After[col.Name]; ok {
			cols = append(cols, col.Name)
			vals = append(vals, val)
		}
	}
	return cols, vals
}

func decodeKey(raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode record key: %w", err)
	}
	return out, nil
}

func whereFromKey(key map[string]any, quote rune, prefix string) (string, []any) {
	parts := make([]string, 0, len(key))
	args := make([]any, 0, len(key))
	for col, val := range key {
		name := prefix + col
		parts = append(parts, fmt.Sprintf("%s = ?", quoteIdent(name, quote)))
		args = append(args, val)
	}
	return strings.Join(parts, " AND "), args
}

func quoteColumns(cols []string, quote rune) string {
	quoted := make([]string, 0, len(cols))
	for _, col := range cols {
		quoted = append(quoted, quoteIdent(col, quote))
	}
	return strings.Join(quoted, ", ")
}

func placeholders(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.TrimRight(strings.Repeat("?,", count), ",")
}

func quoteIdent(value string, quote rune) string {
	if value == "" {
		return value
	}
	escaped := strings.ReplaceAll(value, string(quote), string(quote)+string(quote))
	return string(quote) + escaped + string(quote)
}

func quoteQualified(name string, quote rune) string {
	parts := strings.Split(name, ".")
	if len(parts) == 1 {
		return quoteIdent(parts[0], quote)
	}
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted = append(quoted, quoteIdent(part, quote))
	}
	return strings.Join(quoted, ".")
}

func parseBool(value string, fallback bool) bool {
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
