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
	"github.com/josephjohncox/wallaby/internal/ddl"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

const (
	optDSN             = "dsn"
	optDatabase        = "database"
	optSchema          = "schema"
	optTable           = "table"
	optWriteMode       = "write_mode"
	optBatchMode       = "batch_mode"
	optBatchResolution = "batch_resolution"
	optStagingSchema   = "staging_schema"
	optStagingTable    = "staging_table"
	optStagingSuffix   = "staging_suffix"
	optMetaTable       = "meta_table"
	optMetaSchema      = "meta_schema"
	optMetaDatabase    = "meta_database"
	optMetaEnabled     = "meta_table_enabled"
	optMetaPKPrefix    = "meta_pk_prefix"
	optMetaEngine      = "meta_engine"
	optMetaOrderBy     = "meta_order_by"
	optFlowID          = "flow_id"

	writeModeTarget      = "target"
	writeModeAppend      = "append"
	batchModeTarget      = "target"
	batchModeStaging     = "staging"
	batchResolveNone     = "none"
	batchResolveAppend   = "append"
	batchResolveReplace  = "replace"
	defaultMetaSchema    = "wallaby_meta"
	defaultMetaTable     = "__metadata"
	defaultMetaPKPref    = "pk_"
	defaultMetaEngine    = "MergeTree"
	defaultStagingSuffix = "_staging"
)

// Destination writes change events into ClickHouse tables.
type Destination struct {
	spec             connector.Spec
	db               *sql.DB
	writeMode        string
	batchMode        string
	batchResolve     string
	stagingSchema    string
	stagingTableName string
	stagingSuffix    string
	metaEnabled      bool
	metaSchema       string
	metaTable        string
	metaPKPrefix     string
	flowID           string
	metaColumns      map[string]struct{}
	metaEngine       string
	metaOrderBy      string
	stagingTables    map[string]tableInfo
	stagingResolved  bool
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
	d.batchMode = strings.ToLower(spec.Options[optBatchMode])
	if d.batchMode == "" {
		d.batchMode = batchModeTarget
	}
	d.batchResolve = strings.ToLower(spec.Options[optBatchResolution])
	if d.batchResolve == "" {
		d.batchResolve = batchResolveNone
	}
	d.stagingSchema = strings.TrimSpace(spec.Options[optStagingSchema])
	d.stagingTableName = strings.TrimSpace(spec.Options[optStagingTable])
	d.stagingSuffix = strings.TrimSpace(spec.Options[optStagingSuffix])
	if d.stagingSuffix == "" {
		d.stagingSuffix = defaultStagingSuffix
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
	d.stagingTables = map[string]tableInfo{}

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
		target, isStaging := d.resolveTarget(batch.Schema, record)
		if isStaging {
			d.trackStaging(batch.Schema, record)
		}
		if err := d.applyRecord(ctx, target, batch.Schema, record, mode); err != nil {
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

func (d *Destination) Close(ctx context.Context) error {
	if d.db != nil {
		if err := d.finalizeStaging(ctx); err != nil {
			_ = d.db.Close()
			return err
		}
		return d.db.Close()
	}
	return nil
}

// ResolveStaging applies staged backfill data into target tables.
func (d *Destination) ResolveStaging(ctx context.Context) error {
	if d.db == nil {
		return nil
	}
	return d.finalizeStaging(ctx)
}

// ResolveStagingFor applies staged data for specific schemas/tables.
func (d *Destination) ResolveStagingFor(ctx context.Context, schemas []connector.Schema) error {
	if d.db == nil {
		return errors.New("clickhouse destination not initialized")
	}
	for _, schema := range schemas {
		if schema.Name == "" {
			continue
		}
		key := tableKey(schema, schema.Name)
		d.stagingTables[key] = tableInfo{schema: schema, table: schema.Name}
	}
	return d.finalizeStaging(ctx)
}

func (d *Destination) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		SupportsDDL:           true,
		SupportsSchemaChanges: true,
		SupportsStreaming:     true,
		SupportsBulkLoad:      true,
		SupportsTypeMapping:   true,
		SupportedWireFormats: []connector.WireFormat{
			connector.WireFormatArrow,
			connector.WireFormatParquet,
			connector.WireFormatAvro,
			connector.WireFormatProto,
			connector.WireFormatJSON,
		},
	}
}

func (d *Destination) ApplyDDL(ctx context.Context, schema connector.Schema, record connector.Record) error {
	if d.db == nil {
		return errors.New("clickhouse destination not initialized")
	}
	statements, err := ddl.TranslateRecordDDL(schema, record, ddl.DialectConfigFor(ddl.DialectClickHouse), d.TypeMappings(), d.spec.Options)
	if err != nil {
		return err
	}
	for _, stmt := range statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := d.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("apply ddl: %w", err)
		}
	}
	return nil
}

func (d *Destination) TypeMappings() map[string]string {
	return defaultClickHouseTypeMappings()
}

func (d *Destination) applyRecord(ctx context.Context, target string, schema connector.Schema, record connector.Record, mode string) error {
	if target == "" {
		target = d.targetTable(schema, record)
	}
	switch record.Operation {
	case connector.OpDelete:
		if mode == writeModeAppend {
			return d.insertRow(ctx, target, schema, record)
		}
		return d.deleteRow(ctx, target, schema, record)
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

func defaultClickHouseTypeMappings() map[string]string {
	return map[string]string{
		"bool":                        "UInt8",
		"boolean":                     "UInt8",
		"int2":                        "Int16",
		"smallint":                    "Int16",
		"int4":                        "Int32",
		"integer":                     "Int32",
		"int":                         "Int32",
		"int8":                        "Int64",
		"bigint":                      "Int64",
		"serial":                      "Int32",
		"bigserial":                   "Int64",
		"float4":                      "Float32",
		"real":                        "Float32",
		"float8":                      "Float64",
		"double precision":            "Float64",
		"numeric":                     "Decimal",
		"decimal":                     "Decimal",
		"money":                       "Decimal",
		"uuid":                        "String",
		"text":                        "String",
		"varchar":                     "String",
		"character varying":           "String",
		"character":                   "String",
		"bpchar":                      "String",
		"json":                        "String",
		"jsonb":                       "String",
		"bytea":                       "String",
		"date":                        "Date",
		"time":                        "String",
		"timetz":                      "String",
		"timestamp":                   "DateTime64(6)",
		"timestamp without time zone": "DateTime64(6)",
		"timestamp with time zone":    "DateTime64(6)",
		"timestamptz":                 "DateTime64(6)",
		"inet":                        "String",
		"cidr":                        "String",
		"citext":                      "String",
		"ltree":                       "String",
		"hstore":                      "String",
		"vector":                      "Array(Float32)",
		"geometry":                    "String",
		"geography":                   "String",
		"postgis.geometry":            "String",
		"postgis.geography":           "String",
		"ext:postgis.geometry":        "String",
		"ext:postgis.geography":       "String",
		"ext:hstore.hstore":           "String",
		"ext:hstore":                  "String",
		"ext:citext.citext":           "String",
		"ext:citext":                  "String",
		"ext:ltree.ltree":             "String",
		"ext:ltree":                   "String",
		"ext:vector.vector":           "Array(Float32)",
		"ext:vector":                  "Array(Float32)",
	}
}

func (d *Destination) resolveTarget(schema connector.Schema, record connector.Record) (string, bool) {
	if record.Operation == connector.OpLoad && d.batchMode == batchModeStaging {
		return d.stagingTable(schema, record), true
	}
	return d.targetTable(schema, record), false
}

func (d *Destination) trackStaging(schema connector.Schema, record connector.Record) {
	if record.Table == "" {
		return
	}
	key := tableKey(schema, record.Table)
	if _, ok := d.stagingTables[key]; ok {
		return
	}
	d.stagingTables[key] = tableInfo{schema: schema, table: record.Table}
}

func (d *Destination) targetTable(schema connector.Schema, record connector.Record) string {
	targetSchema, table := d.targetParts(schema, record.Table)
	if strings.Contains(table, ".") {
		return quoteQualified(table, '`')
	}
	if targetSchema == "" {
		return quoteIdent(table, '`')
	}
	return quoteIdent(targetSchema, '`') + "." + quoteIdent(table, '`')
}

func (d *Destination) stagingTable(schema connector.Schema, record connector.Record) string {
	stagingTable := strings.TrimSpace(d.stagingTableName)
	stagingSchema := strings.TrimSpace(d.stagingSchema)

	targetSchema, table := d.targetParts(schema, record.Table)
	if stagingTable == "" {
		stagingTable = table + d.stagingSuffix
	}
	if strings.Contains(stagingTable, ".") {
		return quoteQualified(stagingTable, '`')
	}
	if stagingSchema == "" {
		stagingSchema = targetSchema
	}
	if stagingSchema == "" {
		return quoteIdent(stagingTable, '`')
	}
	return quoteIdent(stagingSchema, '`') + "." + quoteIdent(stagingTable, '`')
}

func (d *Destination) insertRow(ctx context.Context, target string, schema connector.Schema, record connector.Record) error {
	cols, vals, err := recordColumns(schema, record)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", target, quoteColumns(cols, '`'), placeholders(len(cols)))
	if _, err := d.db.ExecContext(ctx, stmt, vals...); err != nil {
		return fmt.Errorf("insert row: %w", err)
	}
	return nil
}

func (d *Destination) finalizeStaging(ctx context.Context) error {
	if d.batchMode != batchModeStaging {
		return nil
	}
	if d.batchResolve == "" || d.batchResolve == batchResolveNone {
		return nil
	}
	if d.stagingResolved {
		return nil
	}
	if len(d.stagingTables) == 0 {
		return nil
	}

	for _, info := range d.stagingTables {
		if err := d.resolveStagingTable(ctx, info); err != nil {
			return err
		}
	}

	d.stagingResolved = true
	return nil
}

func (d *Destination) resolveStagingTable(ctx context.Context, info tableInfo) error {
	target := d.targetTable(info.schema, connector.Record{Table: info.table})
	staging := d.stagingTable(info.schema, connector.Record{Table: info.table})
	cols := schemaColumns(info.schema)
	if len(cols) == 0 {
		loaded, err := d.loadColumns(ctx, info.schema, info.table)
		if err != nil {
			return err
		}
		cols = loaded
	}
	if len(cols) == 0 {
		return nil
	}
	colList := quoteColumns(cols, '`')
	if d.batchResolve == batchResolveReplace {
		if _, err := d.db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", target)); err != nil {
			return fmt.Errorf("truncate target: %w", err)
		}
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", target, colList, colList, staging)
	if _, err := d.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("resolve staging: %w", err)
	}
	return nil
}

func (d *Destination) targetParts(schema connector.Schema, table string) (string, string) {
	targetSchema := strings.TrimSpace(d.spec.Options[optDatabase])
	if targetSchema == "" {
		targetSchema = strings.TrimSpace(d.spec.Options[optSchema])
	}
	targetTable := strings.TrimSpace(d.spec.Options[optTable])
	if targetTable == "" {
		targetTable = table
	}
	if strings.Contains(targetTable, ".") {
		parts := strings.SplitN(targetTable, ".", 2)
		if len(parts) == 2 {
			targetSchema = parts[0]
			targetTable = parts[1]
		}
	}
	if targetSchema == "" {
		targetSchema = schema.Namespace
	}
	return targetSchema, targetTable
}

func (d *Destination) loadColumns(ctx context.Context, schema connector.Schema, table string) ([]string, error) {
	targetSchema, targetTable := d.targetParts(schema, table)
	if targetTable == "" {
		return nil, nil
	}
	if targetSchema == "" {
		return nil, nil
	}
	rows, err := d.db.QueryContext(ctx,
		`SELECT name FROM system.columns WHERE database = ? AND table = ? ORDER BY position`,
		targetSchema, targetTable,
	)
	if err != nil {
		return nil, fmt.Errorf("load columns: %w", err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan column: %w", err)
		}
		cols = append(cols, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate columns: %w", err)
	}
	return cols, nil
}

func (d *Destination) updateRow(ctx context.Context, target string, schema connector.Schema, record connector.Record) error {
	key, err := decodeKeyForSchema(schema, record.Key)
	if err != nil {
		return err
	}
	if len(key) == 0 {
		return errors.New("update requires record key")
	}

	cols, vals, err := recordColumns(schema, record)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}

	setClause := make([]string, 0, len(cols))
	setArgs := make([]any, 0, len(cols))
	for i, col := range cols {
		if _, isKey := key[col]; isKey {
			continue
		}
		setClause = append(setClause, fmt.Sprintf("%s = ?", quoteIdent(col, '`')))
		setArgs = append(setArgs, vals[i])
	}
	if len(setClause) == 0 {
		return nil
	}

	whereClause, whereArgs := whereFromKey(key, '`', "")
	args := append(setArgs, whereArgs...)

	stmt := fmt.Sprintf("ALTER TABLE %s UPDATE %s WHERE %s", target, strings.Join(setClause, ", "), whereClause)
	if _, err := d.db.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("update row: %w", err)
	}
	return nil
}

func (d *Destination) deleteRow(ctx context.Context, target string, schema connector.Schema, record connector.Record) error {
	key, err := decodeKeyForSchema(schema, record.Key)
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
	key, err := decodeKeyForSchema(schema, record.Key)
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

func recordColumns(schema connector.Schema, record connector.Record) ([]string, []any, error) {
	if record.After == nil {
		return nil, nil, nil
	}
	cols := make([]string, 0, len(schema.Columns))
	vals := make([]any, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		if val, ok := record.After[col.Name]; ok {
			normalized, err := normalizeClickHouseValue(col.Type, val)
			if err != nil {
				return nil, nil, err
			}
			cols = append(cols, col.Name)
			vals = append(vals, normalized)
		}
	}
	return cols, vals, nil
}

func normalizeClickHouseValue(colType string, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if isClickHouseArrayType(colType) {
		return value, nil
	}
	switch v := value.(type) {
	case json.RawMessage:
		return string(v), nil
	case []byte:
		if isClickHouseJSONType(colType) {
			return string(v), nil
		}
		return value, nil
	case map[string]any, []any:
		payload, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("marshal json value: %w", err)
		}
		return string(payload), nil
	default:
		if isClickHouseJSONType(colType) {
			payload, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("marshal json value: %w", err)
			}
			return string(payload), nil
		}
		return value, nil
	}
}

func isClickHouseArrayType(colType string) bool {
	normalized := strings.ToLower(strings.TrimSpace(colType))
	return strings.HasPrefix(normalized, "array(") || strings.HasSuffix(normalized, "[]")
}

func isClickHouseJSONType(colType string) bool {
	normalized := strings.ToLower(strings.TrimSpace(colType))
	return strings.HasPrefix(normalized, "json")
}

func schemaColumns(schema connector.Schema) []string {
	cols := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		cols = append(cols, col.Name)
	}
	return cols
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

func decodeKeyForSchema(schema connector.Schema, raw []byte) (map[string]any, error) {
	key, err := decodeKey(raw)
	if err != nil {
		return nil, err
	}
	return connector.NormalizeKeyForSchema(schema, key)
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

type tableInfo struct {
	schema connector.Schema
	table  string
}

func tableKey(schema connector.Schema, table string) string {
	if schema.Namespace == "" {
		return table
	}
	if table == "" {
		return schema.Namespace
	}
	return schema.Namespace + "." + table
}
