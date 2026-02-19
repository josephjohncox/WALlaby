package snowflake

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/internal/ddl"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
	"github.com/josephjohncox/wallaby/pkg/wire"
	_ "github.com/snowflakedb/gosnowflake"
)

func safeMetaCapacity(base, extra int) int {
	maxInt := int(^uint(0) >> 1)
	if extra > maxInt-base {
		return maxInt
	}
	return base + extra
}

const (
	optDSN               = "dsn"
	optSchema            = "schema"
	optTable             = "table"
	optDisableTx         = "disable_transactions"
	optWarehouse         = "warehouse"
	optWarehouseSize     = "warehouse_size"
	optWarehouseSuspend  = "warehouse_auto_suspend"
	optWarehouseResume   = "warehouse_auto_resume"
	optSessionKeepAlive  = "session_keep_alive"
	optWriteMode         = "write_mode"
	optBatchMode         = "batch_mode"
	optBatchResolution   = "batch_resolution"
	optStagingSchema     = "staging_schema"
	optStagingTable      = "staging_table"
	optStagingSuffix     = "staging_suffix"
	optMetaTable         = "meta_table"
	optMetaSchema        = "meta_schema"
	optMetaEnabled       = "meta_table_enabled"
	optMetaPKPrefix      = "meta_pk_prefix"
	optFlowID            = "flow_id"
	writeModeTarget      = "target"
	writeModeAppend      = "append"
	batchModeTarget      = "target"
	batchModeStaging     = "staging"
	batchResolveNone     = "none"
	batchResolveAppend   = "append"
	batchResolveReplace  = "replace"
	defaultMetaSchema    = "WALLABY_META"
	defaultMetaTable     = "__METADATA"
	defaultMetaPKPrefx   = "pk_"
	defaultStagingSuffix = "_staging"
	defaultAutoSuspend   = 60
)

// Destination writes change events into Snowflake tables.
type Destination struct {
	spec             connector.Spec
	db               *sql.DB
	disableTx        bool
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
	registry         schemaregistry.Registry
	registrySubject  string
	stagingTables    map[string]tableInfo
	stagingResolved  bool
	warehouse        string
	warehouseSize    string
	warehouseSuspend *int
	warehouseResume  *bool
	sessionKeepAlive *bool
}

func (d *Destination) Open(ctx context.Context, spec connector.Spec) error {
	d.spec = spec
	dsn := spec.Options[optDSN]
	if dsn == "" {
		return errors.New("snowflake dsn is required")
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
	d.disableTx = parseBool(spec.Options[optDisableTx], false)

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
		d.metaSchema = defaultMetaSchema
	}
	d.metaTable = strings.TrimSpace(spec.Options[optMetaTable])
	if d.metaTable == "" {
		d.metaTable = defaultMetaTable
	}
	d.metaPKPrefix = spec.Options[optMetaPKPrefix]
	if d.metaPKPrefix == "" {
		d.metaPKPrefix = defaultMetaPKPrefx
	}
	d.flowID = spec.Options[optFlowID]
	d.metaColumns = map[string]struct{}{}
	d.stagingTables = map[string]tableInfo{}
	d.registrySubject = strings.TrimSpace(spec.Options[schemaregistry.OptRegistrySubject])

	registryCfg := schemaregistry.ConfigFromOptions(spec.Options)
	registry, err := schemaregistry.NewRegistry(ctx, registryCfg)
	if err != nil && !errors.Is(err, schemaregistry.ErrRegistryDisabled) {
		_ = d.db.Close()
		return err
	}
	if errors.Is(err, schemaregistry.ErrRegistryDisabled) {
		registry = nil
	}
	d.registry = registry

	d.warehouse = strings.TrimSpace(spec.Options[optWarehouse])
	if raw := strings.TrimSpace(spec.Options[optWarehouseSize]); raw != "" {
		d.warehouseSize = normalizeWarehouseSize(raw)
	}
	if raw := strings.TrimSpace(spec.Options[optWarehouseSuspend]); raw != "" {
		val, err := parseInt(raw)
		if err != nil {
			return fmt.Errorf("parse %s: %w", optWarehouseSuspend, err)
		}
		d.warehouseSuspend = &val
	}
	if raw := strings.TrimSpace(spec.Options[optWarehouseResume]); raw != "" {
		val := parseBool(raw, true)
		d.warehouseResume = &val
	}
	if raw := strings.TrimSpace(spec.Options[optSessionKeepAlive]); raw != "" {
		val := parseBool(raw, false)
		d.sessionKeepAlive = &val
	} else {
		val := false
		d.sessionKeepAlive = &val
	}

	if err := d.configureSession(ctx); err != nil {
		_ = d.db.Close()
		return err
	}

	if d.metaEnabled {
		if err := d.ensureMetaTable(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (d *Destination) configureSession(ctx context.Context) error {
	if d.db == nil {
		return nil
	}
	if d.sessionKeepAlive != nil {
		value := "FALSE"
		if *d.sessionKeepAlive {
			value = "TRUE"
		}
		if _, err := d.db.ExecContext(ctx, fmt.Sprintf("ALTER SESSION SET CLIENT_SESSION_KEEP_ALIVE = %s", value)); err != nil {
			if isUnsupportedSessionSetting(err) {
				log.Printf("snowflake session keep-alive not supported by driver: %v", err)
			} else {
				return fmt.Errorf("set session keep alive: %w", err)
			}
		}
	}
	if d.warehouse == "" {
		return nil
	}
	if _, err := d.db.ExecContext(ctx, fmt.Sprintf("USE WAREHOUSE %s", quoteIdent(d.warehouse, '"'))); err != nil {
		return fmt.Errorf("use warehouse: %w", err)
	}

	settings := make([]string, 0, 3)
	if d.warehouseSize != "" {
		settings = append(settings, fmt.Sprintf("WAREHOUSE_SIZE = %s", d.warehouseSize))
	}
	if d.warehouseSuspend == nil {
		val := defaultAutoSuspend
		d.warehouseSuspend = &val
	}
	if d.warehouseSuspend != nil {
		settings = append(settings, fmt.Sprintf("AUTO_SUSPEND = %d", *d.warehouseSuspend))
	}
	if d.warehouseResume == nil {
		val := true
		d.warehouseResume = &val
	}
	if d.warehouseResume != nil {
		val := "FALSE"
		if *d.warehouseResume {
			val = "TRUE"
		}
		settings = append(settings, fmt.Sprintf("AUTO_RESUME = %s", val))
	}
	if len(settings) == 0 {
		return nil
	}
	stmt := fmt.Sprintf("ALTER WAREHOUSE %s SET %s", quoteIdent(d.warehouse, '"'), strings.Join(settings, " "))
	if _, err := d.db.ExecContext(ctx, stmt); err != nil {
		if isUnsupportedSessionSetting(err) {
			log.Printf("snowflake warehouse settings not supported by driver: %v", err)
		} else {
			return fmt.Errorf("alter warehouse: %w", err)
		}
	}
	return nil
}

func isUnsupportedSessionSetting(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not implemented") || strings.Contains(msg, "fakesnow")
}

type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) error {
	if d.db == nil {
		return errors.New("snowflake destination not initialized")
	}
	if len(batch.Records) == 0 {
		return nil
	}

	mode := d.writeMode
	if mode == "" {
		mode = writeModeTarget
	}
	meta, err := d.ensureSchema(ctx, batch.Schema)
	if err != nil {
		if errors.Is(err, schemaregistry.ErrRegistryDisabled) {
			meta = nil
		} else {
			return err
		}
	}

	if d.disableTx {
		exec := execer(d.db)
		for _, record := range batch.Records {
			target, isStaging := d.resolveTarget(batch.Schema, record)
			if isStaging {
				d.trackStaging(batch.Schema, record)
			}
			if err := d.applyRecord(ctx, exec, target, batch.Schema, record, mode); err != nil {
				return err
			}
			if d.metaEnabled {
				if err := d.upsertMetadata(ctx, exec, batch.Schema, record, batch.Checkpoint, meta); err != nil {
					return err
				}
			}
		}
		return nil
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	for _, record := range batch.Records {
		target, isStaging := d.resolveTarget(batch.Schema, record)
		if isStaging {
			d.trackStaging(batch.Schema, record)
		}
		if err := d.applyRecord(ctx, tx, target, batch.Schema, record, mode); err != nil {
			_ = tx.Rollback()
			return err
		}
		if d.metaEnabled {
			if err := d.upsertMetadata(ctx, tx, batch.Schema, record, batch.Checkpoint, meta); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func (d *Destination) Close(ctx context.Context) error {
	if d.db != nil {
		if err := d.finalizeStaging(ctx); err != nil {
			_ = d.db.Close()
			return err
		}
		if err := d.db.Close(); err != nil {
			return err
		}
	}
	if d.registry != nil {
		if err := d.registry.Close(); err != nil {
			return err
		}
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
		return errors.New("snowflake destination not initialized")
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
		return errors.New("snowflake destination not initialized")
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
	return defaultSnowflakeTypeMappings()
}

type schemaMeta struct {
	Subject string
	ID      string
	Version int
}

func (d *Destination) ensureSchema(ctx context.Context, schema connector.Schema) (*schemaMeta, error) {
	if d.registry == nil {
		return nil, schemaregistry.ErrRegistryDisabled
	}
	subject := d.registrySubjectFor(schema)
	return d.registerAvroSchema(ctx, subject, schema)
}

func (d *Destination) registerAvroSchema(ctx context.Context, subject string, schema connector.Schema) (*schemaMeta, error) {
	req := schemaregistry.RegisterRequest{
		Subject:    subject,
		Schema:     wire.AvroSchema(schema),
		SchemaType: schemaregistry.SchemaTypeAvro,
	}
	result, err := d.registry.Register(ctx, req)
	if err != nil {
		return nil, err
	}
	return &schemaMeta{Subject: subject, ID: result.ID, Version: result.Version}, nil
}

func (d *Destination) registrySubjectFor(schema connector.Schema) string {
	if d.registrySubject != "" {
		return d.registrySubject
	}
	if schema.Namespace != "" {
		return fmt.Sprintf("%s.%s", schema.Namespace, schema.Name)
	}
	return schema.Name
}

func (d *Destination) applyRecord(ctx context.Context, exec execer, target string, schema connector.Schema, record connector.Record, mode string) error {
	if target == "" {
		target = d.targetTable(schema, record)
	}
	switch record.Operation {
	case connector.OpDelete:
		if mode == writeModeAppend {
			return d.insertRow(ctx, exec, target, schema, record)
		}
		return d.deleteRow(ctx, exec, target, schema, record)
	case connector.OpUpdate:
		if mode == writeModeAppend {
			return d.insertRow(ctx, exec, target, schema, record)
		}
		return d.updateRow(ctx, exec, target, schema, record)
	case connector.OpInsert, connector.OpLoad:
		return d.insertRow(ctx, exec, target, schema, record)
	default:
		return nil
	}
}

func defaultSnowflakeTypeMappings() map[string]string {
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
		return quoteQualified(table, '"')
	}
	if targetSchema == "" {
		return quoteIdent(table, '"')
	}
	return quoteIdent(targetSchema, '"') + "." + quoteIdent(table, '"')
}

func (d *Destination) stagingTable(schema connector.Schema, record connector.Record) string {
	stagingTable := strings.TrimSpace(d.stagingTableName)
	stagingSchema := strings.TrimSpace(d.stagingSchema)

	targetSchema, table := d.targetParts(schema, record.Table)
	if stagingTable == "" {
		stagingTable = table + d.stagingSuffix
	}
	if strings.Contains(stagingTable, ".") {
		return quoteQualified(stagingTable, '"')
	}
	if stagingSchema == "" {
		stagingSchema = targetSchema
	}
	if stagingSchema == "" {
		return quoteIdent(stagingTable, '"')
	}
	return quoteIdent(stagingSchema, '"') + "." + quoteIdent(stagingTable, '"')
}

func (d *Destination) insertRow(ctx context.Context, exec execer, target string, schema connector.Schema, record connector.Record) error {
	cols, vals, exprs, err := recordColumns(schema, record)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", target, quoteColumns(cols, '"'), strings.Join(exprs, ", "))
	if _, err := exec.ExecContext(ctx, stmt, vals...); err != nil {
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
	colList := quoteColumns(cols, '"')
	if d.batchResolve == batchResolveReplace {
		if _, err := d.db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", target)); err != nil {
			return fmt.Errorf("truncate target: %w", err)
		}
	}
	// #nosec G201 -- identifiers are quoted and derived from schema/config.
	stmt := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", target, colList, colList, staging)
	if _, err := d.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("resolve staging: %w", err)
	}
	return nil
}

func (d *Destination) targetParts(schema connector.Schema, table string) (string, string) {
	targetSchema := strings.TrimSpace(d.spec.Options[optSchema])
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
		return []string{}, nil
	}
	if targetSchema == "" {
		targetSchema = schema.Namespace
	}
	if targetSchema == "" {
		return []string{}, nil
	}
	rows, err := d.db.QueryContext(ctx,
		`SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`,
		strings.ToUpper(targetSchema), strings.ToUpper(targetTable),
	)
	if err != nil {
		return nil, fmt.Errorf("load columns: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("close rows: %v", err)
		}
	}()

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

func (d *Destination) updateRow(ctx context.Context, exec execer, target string, schema connector.Schema, record connector.Record) error {
	key, err := decodeKeyForSchema(schema, record.Key)
	if err != nil {
		return err
	}
	if len(key) == 0 {
		return errors.New("update requires record key")
	}

	cols, vals, exprs, err := recordColumns(schema, record)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}

	setClause := make([]string, 0, len(cols))
	for idx, col := range cols {
		setClause = append(setClause, fmt.Sprintf("%s = %s", quoteIdent(col, '"'), exprs[idx]))
	}

	whereClause, whereArgs := whereFromKey(key, '"', "")
	vals = append(vals, whereArgs...)
	args := vals

	stmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s", target, strings.Join(setClause, ", "), whereClause)
	if _, err := exec.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("update row: %w", err)
	}
	return nil
}

func (d *Destination) deleteRow(ctx context.Context, exec execer, target string, schema connector.Schema, record connector.Record) error {
	key, err := decodeKeyForSchema(schema, record.Key)
	if err != nil {
		return err
	}
	if len(key) == 0 {
		return errors.New("delete requires record key")
	}

	whereClause, args := whereFromKey(key, '"', "")
	stmt := fmt.Sprintf("DELETE FROM %s WHERE %s", target, whereClause)
	if _, err := exec.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("delete row: %w", err)
	}
	return nil
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

func (d *Destination) upsertMetadata(ctx context.Context, exec execer, schema connector.Schema, record connector.Record, checkpoint connector.Checkpoint, meta *schemaMeta) error {
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

	columns := make([]string, 0, safeMetaCapacity(11, len(pkCols)))
	columns = append(columns, "FLOW_ID", "SOURCE_SCHEMA", "SOURCE_TABLE", "SYNCED_AT", "IS_DELETED", "LSN", "OPERATION", "KEY_JSON")
	values := make([]any, 0, safeMetaCapacity(11, len(pkVals)))
	values = append(values, d.flowID, schema.Namespace, record.Table, syncedAt, record.Operation == connector.OpDelete, checkpoint.LSN, string(record.Operation), keyJSON)
	regSubject := ""
	regID := ""
	regVersion := ""
	if meta != nil {
		regSubject = meta.Subject
		regID = meta.ID
		if meta.Version > 0 {
			regVersion = strconv.Itoa(meta.Version)
		}
	}
	const (
		registrySubjectCol = "REGISTRY_SUBJECT"
		registryIDCol      = "REGISTRY_ID"
		registryVersionCol = "REGISTRY_VERSION"
	)
	if err := d.ensureMetaColumn(ctx, registrySubjectCol); err != nil {
		return err
	}
	if err := d.ensureMetaColumn(ctx, registryIDCol); err != nil {
		return err
	}
	if err := d.ensureMetaColumn(ctx, registryVersionCol); err != nil {
		return err
	}
	columns = append(columns, registrySubjectCol, registryIDCol, registryVersionCol)
	values = append(values, regSubject, regID, regVersion)
	columns = append(columns, pkCols...)
	values = append(values, pkVals...)

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

func recordColumns(schema connector.Schema, record connector.Record) ([]string, []any, []string, error) {
	if record.After == nil {
		return []string{}, []any{}, []string{}, nil
	}
	cols := make([]string, 0, len(schema.Columns))
	vals := make([]any, 0, len(schema.Columns))
	exprs := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		if val, ok := record.After[col.Name]; ok {
			expr := "?"
			normalized, err := normalizeSnowflakeValue(col.Type, val)
			if err != nil {
				return nil, nil, nil, err
			}
			if isSnowflakeJSONType(col.Type) {
				expr = "PARSE_JSON(?)"
			}
			cols = append(cols, col.Name)
			vals = append(vals, normalized)
			exprs = append(exprs, expr)
		}
	}
	return cols, vals, exprs, nil
}

func normalizeSnowflakeValue(colType string, value any) (any, error) {
	if value == nil {
		return nil, nil //nolint:nilnil // nil maps to NULL
	}
	if isSnowflakeJSONType(colType) {
		switch v := value.(type) {
		case json.RawMessage:
			return string(v), nil
		case []byte:
			return string(v), nil
		default:
			payload, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("marshal json value: %w", err)
			}
			return string(payload), nil
		}
	}

	switch v := value.(type) {
	case json.RawMessage:
		return string(v), nil
	case map[string]any, []any:
		payload, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("marshal json value: %w", err)
		}
		return string(payload), nil
	default:
		return value, nil
	}
}

func isSnowflakeJSONType(colType string) bool {
	switch strings.ToLower(strings.TrimSpace(colType)) {
	case "variant", "object", "array":
		return true
	default:
		return false
	}
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

func parseInt(value string) (int, error) {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

func normalizeWarehouseSize(value string) string {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, "_", "")
	switch normalized {
	case "XSMALL", "X-SMALL", "X_SMALL":
		return "XSMALL"
	case "XXSMALL", "XX-SMALL", "XX_SMALL":
		return "XXSMALL"
	default:
		return normalized
	}
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
