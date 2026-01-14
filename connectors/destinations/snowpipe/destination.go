package snowpipe

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/ddl"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/wire"
	_ "github.com/snowflakedb/gosnowflake"
)

const (
	optDSN          = "dsn"
	optStage        = "stage"
	optStagePath    = "stage_path"
	optSchema       = "schema"
	optTable        = "table"
	optFormat       = "format"
	optFileFormat   = "file_format"
	optCopyOnWrite  = "copy_on_write"
	optCopyPattern  = "copy_pattern"
	optCopyOnError  = "copy_on_error"
	optCopyPurge    = "copy_purge"
	optCopyMatch    = "copy_match_by_column_name"
	optAutoIngest   = "auto_ingest"
	optCompatMode   = "compat_mode"
	optWriteMode    = "write_mode"
	optMetaTable    = "meta_table"
	optMetaSchema   = "meta_schema"
	optMetaEnabled  = "meta_table_enabled"
	optMetaPKPrefix = "meta_pk_prefix"
	optFlowID       = "flow_id"

	writeModeAppend    = "append"
	compatModeFakesnow = "fakesnow"
	defaultMetaSchema  = "WALLABY_META"
	defaultMetaTable   = "__METADATA"
	defaultMetaPKPref  = "pk_"
)

// Destination writes batches to Snowflake stages and optionally issues COPY INTO.
type Destination struct {
	spec         connector.Spec
	db           *sql.DB
	codec        wire.Codec
	stage        string
	stagePath    string
	copyOnWrite  bool
	copyPattern  string
	copyOnError  string
	copyPurge    *bool
	copyMatch    string
	fileFormat   string
	writeMode    string
	compatMode   string
	compatNoTx   bool
	metaEnabled  bool
	metaSchema   string
	metaTable    string
	metaPKPrefix string
	flowID       string
	metaColumns  map[string]struct{}
}

type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (d *Destination) Open(ctx context.Context, spec connector.Spec) error {
	d.spec = spec
	dsn := spec.Options[optDSN]
	if dsn == "" {
		return errors.New("snowpipe dsn is required")
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return fmt.Errorf("open snowflake: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("ping snowflake: %w", err)
	}
	d.db = db

	format := spec.Options[optFormat]
	if format == "" {
		format = string(connector.WireFormatParquet)
	}
	codec, err := wire.NewCodec(format)
	if err != nil {
		return err
	}
	switch codec.Name() {
	case connector.WireFormatParquet, connector.WireFormatAvro, connector.WireFormatJSON:
		// supported
	default:
		return fmt.Errorf("snowpipe format %s not supported", codec.Name())
	}
	d.codec = codec

	d.stage = strings.TrimSpace(spec.Options[optStage])
	d.stagePath = strings.Trim(strings.TrimSpace(spec.Options[optStagePath]), "/")
	d.copyOnWrite = parseBool(spec.Options[optCopyOnWrite], true)
	d.copyPattern = strings.TrimSpace(spec.Options[optCopyPattern])
	d.copyOnError = strings.TrimSpace(spec.Options[optCopyOnError])
	d.copyMatch = strings.TrimSpace(spec.Options[optCopyMatch])
	if raw := strings.TrimSpace(spec.Options[optCopyPurge]); raw != "" {
		val := parseBool(raw, false)
		d.copyPurge = &val
	}
	if parseBool(spec.Options[optAutoIngest], false) {
		d.copyOnWrite = false
	}
	d.fileFormat = strings.TrimSpace(spec.Options[optFileFormat])
	d.writeMode = strings.ToLower(strings.TrimSpace(spec.Options[optWriteMode]))
	if d.writeMode == "" {
		d.writeMode = writeModeAppend
	}
	if d.writeMode != writeModeAppend {
		return fmt.Errorf("snowpipe only supports append write_mode")
	}

	compatMode := strings.ToLower(strings.TrimSpace(spec.Options[optCompatMode]))
	switch compatMode {
	case "", "none":
		d.compatMode = ""
	case compatModeFakesnow:
		d.compatMode = compatModeFakesnow
		d.compatNoTx = true
	default:
		return fmt.Errorf("snowpipe compat_mode %s not supported", compatMode)
	}

	d.metaEnabled = parseBool(spec.Options[optMetaEnabled], true)
	d.metaSchema = strings.TrimSpace(spec.Options[optMetaSchema])
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

	if d.metaEnabled {
		if err := d.ensureMetaTable(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) error {
	if d.db == nil {
		return errors.New("snowpipe destination not initialized")
	}
	if len(batch.Records) == 0 {
		return nil
	}

	payload, err := d.codec.Encode(batch)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}

	filePath, fileName, err := d.writeTempFile(payload, batch.Schema)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(filePath) }()

	stage := d.resolveStage(batch)
	if stage == "" {
		return errors.New("snowpipe stage is required")
	}
	stageLocation := joinStage(stage, d.stagePath)

	putStmt := fmt.Sprintf("PUT file://%s %s AUTO_COMPRESS=FALSE", filePath, stageLocation)
	if _, err := d.db.ExecContext(ctx, putStmt); err != nil {
		return d.fallbackIfCompat(ctx, batch, err, "put to stage")
	}

	if d.copyOnWrite {
		// #nosec G201 -- identifiers are quoted and derived from schema/config.
		copyStmt := fmt.Sprintf("COPY INTO %s FROM %s FILES = ('%s') %s", d.targetTable(batch.Schema, batch.Records[0]), stageLocation, fileName, d.copyOptionsClause())
		if _, err := d.db.ExecContext(ctx, copyStmt); err != nil {
			return d.fallbackIfCompat(ctx, batch, err, "copy into")
		}
	}

	if d.metaEnabled {
		tx, err := d.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin metadata transaction: %w", err)
		}
		for _, record := range batch.Records {
			if err := d.upsertMetadata(ctx, tx, batch.Schema, record, batch.Checkpoint); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit metadata transaction: %w", err)
		}
	}

	return nil
}

func (d *Destination) fallbackIfCompat(ctx context.Context, batch connector.Batch, err error, action string) error {
	if d.compatMode != compatModeFakesnow {
		return fmt.Errorf("%s: %w", action, err)
	}
	log.Printf("snowpipe compat fallback (%s): %v", action, err)
	return d.writeCompat(ctx, batch)
}

func (d *Destination) writeCompat(ctx context.Context, batch connector.Batch) error {
	if d.compatNoTx {
		exec := execer(d.db)
		for _, record := range batch.Records {
			cols, vals := recordColumns(batch.Schema, record)
			if len(cols) == 0 {
				continue
			}
			target := d.targetTable(batch.Schema, record)
			// #nosec G201 -- identifiers are quoted and derived from schema/config.
			insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", target, quoteColumns(cols, '"'), placeholders(len(cols)))
			if _, err := exec.ExecContext(ctx, insertStmt, vals...); err != nil {
				return fmt.Errorf("compat insert: %w", err)
			}
			if d.metaEnabled {
				if err := d.upsertMetadata(ctx, exec, batch.Schema, record, batch.Checkpoint); err != nil {
					return err
				}
			}
		}
		return nil
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin compat transaction: %w", err)
	}
	for _, record := range batch.Records {
		cols, vals := recordColumns(batch.Schema, record)
		if len(cols) == 0 {
			continue
		}
		target := d.targetTable(batch.Schema, record)
		// #nosec G201 -- identifiers are quoted and derived from schema/config.
		insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", target, quoteColumns(cols, '"'), placeholders(len(cols)))
		if _, err := tx.ExecContext(ctx, insertStmt, vals...); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("compat insert: %w", err)
		}
		if d.metaEnabled {
			if err := d.upsertMetadata(ctx, tx, batch.Schema, record, batch.Checkpoint); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit compat transaction: %w", err)
	}
	return nil
}

func (d *Destination) ApplyDDL(ctx context.Context, schema connector.Schema, record connector.Record) error {
	if d.db == nil {
		return errors.New("snowpipe destination not initialized")
	}
	statements, err := ddl.TranslateRecordDDL(schema, record, ddl.DialectConfigFor(ddl.DialectSnowflake), d.TypeMappings(), d.spec.Options)
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
	return defaultSnowpipeTypeMappings()
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
		SupportsStreaming:     false,
		SupportsBulkLoad:      true,
		SupportsTypeMapping:   true,
		SupportedWireFormats: []connector.WireFormat{
			connector.WireFormatParquet,
			connector.WireFormatAvro,
			connector.WireFormatJSON,
		},
	}
}

func (d *Destination) resolveStage(batch connector.Batch) string {
	if d.stage != "" {
		return ensureStagePrefix(d.stage)
	}

	table := strings.TrimSpace(d.spec.Options[optTable])
	schema := strings.TrimSpace(d.spec.Options[optSchema])
	if table == "" && len(batch.Records) > 0 {
		table = batch.Records[0].Table
	}
	if table == "" {
		return ""
	}
	if schema == "" {
		schema = batch.Schema.Namespace
	}
	if schema != "" && !strings.Contains(table, ".") {
		return ensureStagePrefix("@%" + schema + "." + table)
	}
	if strings.Contains(table, ".") {
		return ensureStagePrefix("@%" + table)
	}
	return ensureStagePrefix("@%" + table)
}

func (d *Destination) targetTable(schema connector.Schema, record connector.Record) string {
	table := strings.TrimSpace(d.spec.Options[optTable])
	targetSchema := strings.TrimSpace(d.spec.Options[optSchema])
	if table == "" {
		table = record.Table
	}
	if strings.Contains(table, ".") {
		return quoteQualified(table, '"')
	}
	if targetSchema == "" {
		targetSchema = schema.Namespace
	}
	if targetSchema == "" {
		return quoteIdent(table, '"')
	}
	return quoteIdent(targetSchema, '"') + "." + quoteIdent(table, '"')
}

func (d *Destination) writeTempFile(payload []byte, schema connector.Schema) (string, string, error) {
	ext := extensionForFormat(d.codec.Name())
	stamp := time.Now().UTC().Format("20060102T150405Z")
	name := fmt.Sprintf("%s_%s_%s", schema.Name, stamp, uuid.NewString())

	file, err := os.CreateTemp("", "wallaby-snowpipe-*")
	if err != nil {
		return "", "", fmt.Errorf("create temp file: %w", err)
	}
	filePath := file.Name()
	if ext != "" {
		newPath := filepath.Join(filepath.Dir(filePath), name+"."+ext)
		if err := os.Rename(filePath, newPath); err == nil {
			filePath = newPath
		}
	}

	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		return "", "", fmt.Errorf("write temp file: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", "", fmt.Errorf("close temp file: %w", err)
	}
	return filePath, filepath.Base(filePath), nil
}

func (d *Destination) fileFormatClause() string {
	if d.fileFormat != "" {
		return fmt.Sprintf("FILE_FORMAT = (FORMAT_NAME = '%s')", strings.ReplaceAll(d.fileFormat, "'", "''"))
	}
	switch d.codec.Name() {
	case connector.WireFormatParquet:
		return "FILE_FORMAT = (TYPE = PARQUET)"
	case connector.WireFormatAvro:
		return "FILE_FORMAT = (TYPE = AVRO)"
	case connector.WireFormatJSON:
		return "FILE_FORMAT = (TYPE = JSON)"
	default:
		return ""
	}
}

func (d *Destination) copyOptionsClause() string {
	parts := make([]string, 0, 4)
	if clause := d.fileFormatClause(); clause != "" {
		parts = append(parts, clause)
	}
	if d.copyPattern != "" {
		parts = append(parts, fmt.Sprintf("PATTERN = '%s'", escapeCopyString(d.copyPattern)))
	}
	if d.copyOnError != "" {
		parts = append(parts, fmt.Sprintf("ON_ERROR = '%s'", escapeCopyString(d.copyOnError)))
	}
	if d.copyMatch != "" {
		parts = append(parts, fmt.Sprintf("MATCH_BY_COLUMN_NAME = '%s'", escapeCopyString(d.copyMatch)))
	}
	if d.copyPurge != nil {
		if *d.copyPurge {
			parts = append(parts, "PURGE = TRUE")
		} else {
			parts = append(parts, "PURGE = FALSE")
		}
	}
	return strings.Join(parts, " ")
}

func escapeCopyString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func extensionForFormat(format connector.WireFormat) string {
	switch format {
	case connector.WireFormatParquet:
		return "parquet"
	case connector.WireFormatAvro:
		return "avro"
	case connector.WireFormatJSON:
		return "json"
	default:
		return "bin"
	}
}

func defaultSnowpipeTypeMappings() map[string]string {
	return map[string]string{
		"bool":                        "BOOLEAN",
		"boolean":                     "BOOLEAN",
		"int2":                        "SMALLINT",
		"smallint":                    "SMALLINT",
		"int4":                        "INTEGER",
		"integer":                     "INTEGER",
		"int":                         "INTEGER",
		"int8":                        "BIGINT",
		"bigint":                      "BIGINT",
		"serial":                      "INTEGER",
		"bigserial":                   "BIGINT",
		"float4":                      "FLOAT",
		"real":                        "FLOAT",
		"float8":                      "DOUBLE",
		"double precision":            "DOUBLE",
		"numeric":                     "NUMBER",
		"decimal":                     "NUMBER",
		"money":                       "NUMBER",
		"uuid":                        "STRING",
		"text":                        "STRING",
		"varchar":                     "STRING",
		"character varying":           "STRING",
		"character":                   "STRING",
		"bpchar":                      "STRING",
		"json":                        "VARIANT",
		"jsonb":                       "VARIANT",
		"bytea":                       "BINARY",
		"date":                        "DATE",
		"time":                        "TIME",
		"timetz":                      "TIME",
		"timestamp":                   "TIMESTAMP_NTZ",
		"timestamp without time zone": "TIMESTAMP_NTZ",
		"timestamp with time zone":    "TIMESTAMP_TZ",
		"timestamptz":                 "TIMESTAMP_TZ",
		"inet":                        "STRING",
		"cidr":                        "STRING",
		"citext":                      "STRING",
		"ltree":                       "STRING",
		"hstore":                      "OBJECT",
		"vector":                      "ARRAY",
		"geometry":                    "STRING",
		"geography":                   "STRING",
		"postgis.geometry":            "STRING",
		"postgis.geography":           "STRING",
		"ext:postgis.geometry":        "STRING",
		"ext:postgis.geography":       "STRING",
		"ext:hstore.hstore":           "OBJECT",
		"ext:hstore":                  "OBJECT",
		"ext:citext.citext":           "STRING",
		"ext:citext":                  "STRING",
		"ext:ltree.ltree":             "STRING",
		"ext:ltree":                   "STRING",
		"ext:vector.vector":           "ARRAY",
		"ext:vector":                  "ARRAY",
	}
}

func ensureStagePrefix(stage string) string {
	trim := strings.TrimSpace(stage)
	if trim == "" {
		return ""
	}
	if strings.HasPrefix(trim, "@") {
		return trim
	}
	return "@" + trim
}

func joinStage(stage, prefix string) string {
	base := ensureStagePrefix(stage)
	if prefix == "" {
		return base
	}
	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(prefix, "/")
}

func (d *Destination) ensureMetaTable(ctx context.Context) error {
	if d.metaSchema == "" || d.metaTable == "" {
		return errors.New("meta schema and table are required")
	}
	schemaIdent := quoteIdent(d.metaSchema, '"')
	if _, err := d.db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaIdent)); err != nil {
		return fmt.Errorf("create meta schema: %w", err)
	}
	tableIdent := schemaIdent + "." + quoteIdent(d.metaTable, '"')
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  FLOW_ID STRING,
  SOURCE_SCHEMA STRING,
  SOURCE_TABLE STRING,
  SYNCED_AT TIMESTAMP_TZ,
  IS_DELETED BOOLEAN,
  LSN STRING,
  OPERATION STRING,
  KEY_JSON STRING
)`, tableIdent)
	if _, err := d.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("create meta table: %w", err)
	}
	return d.refreshMetaColumns(ctx)
}

func (d *Destination) refreshMetaColumns(ctx context.Context) error {
	table := strings.ToUpper(d.metaTable)
	schema := strings.ToUpper(d.metaSchema)
	rows, err := d.db.QueryContext(ctx,
		`SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
		schema, table)
	if err != nil {
		return fmt.Errorf("load meta columns: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("close rows: %v", err)
		}
	}()

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

func (d *Destination) upsertMetadata(ctx context.Context, exec execer, schema connector.Schema, record connector.Record, checkpoint connector.Checkpoint) error {
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

	target := quoteIdent(d.metaSchema, '"') + "." + quoteIdent(d.metaTable, '"')
	whereClause, whereArgs := whereFromKey(key, '"', d.metaPKPrefix)
	whereClause = "FLOW_ID = ? AND SOURCE_SCHEMA = ? AND SOURCE_TABLE = ? AND " + whereClause
	whereArgs = append([]any{d.flowID, schema.Namespace, record.Table}, whereArgs...)

	// #nosec G201 -- identifiers are quoted and derived from schema/config.
	deleteStmt := fmt.Sprintf("DELETE FROM %s WHERE %s", target, whereClause)
	if _, err := exec.ExecContext(ctx, deleteStmt, whereArgs...); err != nil {
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

	columns := make([]string, 0, 8+len(pkCols))
	columns = append(columns, "FLOW_ID", "SOURCE_SCHEMA", "SOURCE_TABLE", "SYNCED_AT", "IS_DELETED", "LSN", "OPERATION", "KEY_JSON")
	values := make([]any, 0, 8+len(pkVals))
	values = append(values, d.flowID, schema.Namespace, record.Table, syncedAt, record.Operation == connector.OpDelete, checkpoint.LSN, string(record.Operation), keyJSON)
	columns = append(columns, pkCols...)
	values = append(values, pkVals...)

	// #nosec G201 -- identifiers are quoted and derived from schema/config.
	insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", target, quoteColumns(columns, '"'), placeholders(len(columns)))
	if _, err := exec.ExecContext(ctx, insertStmt, values...); err != nil {
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
	target := quoteIdent(d.metaSchema, '"') + "." + quoteIdent(d.metaTable, '"')
	stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s STRING", target, quoteIdent(column, '"'))
	if _, err := d.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("add meta column: %w", err)
	}
	d.metaColumns[key] = struct{}{}
	return nil
}

func recordColumns(schema connector.Schema, record connector.Record) ([]string, []any) {
	if record.After == nil {
		return []string{}, []any{}
	}
	cols := make([]string, 0, len(schema.Columns))
	vals := make([]any, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		val, ok := record.After[col.Name]
		if !ok {
			continue
		}
		cols = append(cols, col.Name)
		vals = append(vals, val)
	}
	return cols, vals
}

func decodeKey(raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
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
