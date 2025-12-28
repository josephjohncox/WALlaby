package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

const (
	optDSN             = "dsn"
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
	optMetaEnabled     = "meta_table_enabled"
	optMetaPKPrefix    = "meta_pk_prefix"
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
	defaultStagingSuffix = "_staging"
)

// Destination writes change events into Postgres tables.
type Destination struct {
	spec             connector.Spec
	pool             *pgxpool.Pool
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
	stagingTables    map[string]tableInfo
	stagingResolved  bool
}

func (d *Destination) Open(ctx context.Context, spec connector.Spec) error {
	d.spec = spec
	dsn := spec.Options[optDSN]
	if dsn == "" {
		return errors.New("postgres dsn is required")
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("ping postgres: %w", err)
	}
	d.pool = pool

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
		d.metaPKPrefix = defaultMetaPKPref
	}
	d.flowID = spec.Options[optFlowID]
	d.metaColumns = map[string]struct{}{}
	d.stagingTables = map[string]tableInfo{}

	if d.metaEnabled {
		if err := d.ensureMetaTable(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) error {
	if d.pool == nil {
		return errors.New("postgres destination not initialized")
	}
	if len(batch.Records) == 0 {
		return nil
	}

	mode := d.writeMode
	if mode == "" {
		mode = writeModeTarget
	}

	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	targetRecords := map[string][]connector.Record{}
	for _, record := range batch.Records {
		if record.Operation == connector.OpDDL {
			continue
		}
		target, isStaging := d.resolveTarget(batch.Schema, record)
		if isStaging {
			d.trackStaging(batch.Schema, record)
		}
		targetRecords[target] = append(targetRecords[target], record)
	}
	for target, records := range targetRecords {
		if err := d.applyBatch(ctx, tx, target, batch.Schema, records, mode); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
	}
	if d.metaEnabled {
		for _, record := range batch.Records {
			if record.Operation == connector.OpDDL {
				continue
			}
			if err := d.upsertMetadata(ctx, tx, batch.Schema, record, batch.Checkpoint); err != nil {
				_ = tx.Rollback(ctx)
				return err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func (d *Destination) Close(ctx context.Context) error {
	if d.pool != nil {
		if err := d.finalizeStaging(ctx); err != nil {
			d.pool.Close()
			return err
		}
		d.pool.Close()
	}
	return nil
}

// ResolveStaging applies staged backfill data into target tables.
func (d *Destination) ResolveStaging(ctx context.Context) error {
	if d.pool == nil {
		return nil
	}
	return d.finalizeStaging(ctx)
}

// ResolveStagingFor applies staged data for specific schemas/tables.
func (d *Destination) ResolveStagingFor(ctx context.Context, schemas []connector.Schema) error {
	if d.pool == nil {
		return errors.New("postgres destination not initialized")
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

func (d *Destination) ApplyDDL(ctx context.Context, _ connector.Schema, record connector.Record) error {
	if d.pool == nil {
		return errors.New("postgres destination not initialized")
	}
	if strings.TrimSpace(record.DDL) == "" {
		return nil
	}
	if _, err := d.pool.Exec(ctx, record.DDL); err != nil {
		return fmt.Errorf("apply ddl: %w", err)
	}
	return nil
}

func (d *Destination) TypeMappings() map[string]string { return nil }

func (d *Destination) applyBatch(ctx context.Context, tx pgx.Tx, target string, schema connector.Schema, records []connector.Record, mode string) error {
	if len(records) == 0 {
		return nil
	}
	switch mode {
	case writeModeAppend:
		return d.applyAppendBatch(ctx, tx, target, schema, records)
	default:
		return d.applyTargetBatch(ctx, tx, target, schema, records)
	}
}

func (d *Destination) applyAppendBatch(ctx context.Context, tx pgx.Tx, target string, schema connector.Schema, records []connector.Record) error {
	inserts := make([]connector.Record, 0, len(records))
	for _, record := range records {
		if record.Operation == connector.OpDelete {
			continue
		}
		inserts = append(inserts, record)
	}
	return d.insertRows(ctx, tx, target, schema, inserts)
}

func (d *Destination) applyTargetBatch(ctx context.Context, tx pgx.Tx, target string, schema connector.Schema, records []connector.Record) error {
	inserts := make([]connector.Record, 0, len(records))
	updates := make([]connector.Record, 0, len(records))
	deletes := make([]connector.Record, 0, len(records))
	for _, record := range records {
		switch record.Operation {
		case connector.OpInsert, connector.OpLoad:
			inserts = append(inserts, record)
		case connector.OpUpdate:
			updates = append(updates, record)
		case connector.OpDelete:
			deletes = append(deletes, record)
		}
	}

	updatesSameKey := make([]connector.Record, 0, len(updates))
	updatesKeyChange := make([]connector.Record, 0, len(updates))
	for _, record := range updates {
		changed, err := keyChanged(schema, record)
		if err != nil {
			return err
		}
		if changed {
			updatesKeyChange = append(updatesKeyChange, record)
			continue
		}
		updatesSameKey = append(updatesSameKey, record)
	}

	upserts := append(inserts, updatesSameKey...)
	if err := d.upsertRows(ctx, tx, target, schema, upserts); err != nil {
		return err
	}
	if err := d.updateRows(ctx, tx, target, schema, updatesKeyChange); err != nil {
		return err
	}
	if err := d.deleteRows(ctx, tx, target, schema, deletes); err != nil {
		return err
	}
	return nil
}

func (d *Destination) applyRecord(ctx context.Context, tx pgx.Tx, target string, schema connector.Schema, record connector.Record, mode string) error {
	if target == "" {
		target = d.targetTable(schema, record)
	}
	switch record.Operation {
	case connector.OpDelete:
		if mode == writeModeAppend {
			return d.insertRow(ctx, tx, target, schema, record)
		}
		return d.deleteRow(ctx, tx, target, record)
	case connector.OpUpdate:
		if mode == writeModeAppend {
			return d.insertRow(ctx, tx, target, schema, record)
		}
		return d.updateRow(ctx, tx, target, schema, record)
	case connector.OpInsert, connector.OpLoad:
		return d.insertRow(ctx, tx, target, schema, record)
	default:
		return nil
	}
}

type rowGroup struct {
	cols []string
	rows [][]any
}

type updateGroup struct {
	keyCols []string
	cols    []string
	rows    [][]any
}

type deleteGroup struct {
	keyCols []string
	rows    [][]any
}

func (d *Destination) insertRows(ctx context.Context, tx pgx.Tx, target string, schema connector.Schema, records []connector.Record) error {
	if len(records) == 0 {
		return nil
	}
	colTypes := columnTypeMap(schema)
	groups := map[string]*rowGroup{}
	for _, record := range records {
		cols, vals, _, err := recordColumns(schema, record)
		if err != nil {
			return err
		}
		if len(cols) == 0 {
			continue
		}
		key := columnsKey(cols)
		group := groups[key]
		if group == nil {
			group = &rowGroup{cols: cols}
			groups[key] = group
		}
		group.rows = append(group.rows, vals)
	}
	for _, group := range groups {
		if len(group.rows) == 0 {
			continue
		}
		if err := d.copyRows(ctx, tx, target, group.cols, group.rows); err != nil {
			valuesClause, args, err := buildValuesClause(group.cols, colTypes, group.rows)
			if err != nil {
				return err
			}
			stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", target, quoteColumns(group.cols, '"'), valuesClause)
			if _, err := tx.Exec(ctx, stmt, args...); err != nil {
				return fmt.Errorf("insert rows: %w", err)
			}
		}
	}
	return nil
}

type upsertGroup struct {
	keyCols []string
	cols    []string
	rows    [][]any
}

func (d *Destination) upsertRows(ctx context.Context, tx pgx.Tx, target string, schema connector.Schema, records []connector.Record) error {
	if len(records) == 0 {
		return nil
	}
	if len(records) > 1 {
		deduped, err := dedupeUpsertRecords(records, schema)
		if err != nil {
			return err
		}
		records = deduped
		if len(records) == 0 {
			return nil
		}
	}
	colTypes := columnTypeMap(schema)
	groups := map[string]*upsertGroup{}
	for _, record := range records {
		cols, vals, _, err := recordColumns(schema, record)
		if err != nil {
			return err
		}
		if len(cols) == 0 {
			continue
		}
		keyCols, keyVals, err := keyColumnsAndValues(schema, record)
		if err != nil {
			return err
		}
		if len(keyCols) == 0 {
			return errors.New("upsert requires record key")
		}
		colIndex := make(map[string]int, len(cols))
		for idx, col := range cols {
			colIndex[col] = idx
		}
		for idx, keyCol := range keyCols {
			if _, ok := colIndex[keyCol]; ok {
				continue
			}
			cols = append(cols, keyCol)
			vals = append(vals, keyVals[idx])
		}
		groupKey := columnsKey(keyCols) + "||" + columnsKey(cols)
		group := groups[groupKey]
		if group == nil {
			group = &upsertGroup{keyCols: keyCols, cols: cols}
			groups[groupKey] = group
		}
		group.rows = append(group.rows, vals)
	}

	if len(groups) == 0 {
		return nil
	}
	tempName := fmt.Sprintf("tmp_wallaby_%d", time.Now().UnixNano())
	tempIdent := quoteIdent(tempName, '"')
	create := fmt.Sprintf("CREATE TEMP TABLE %s (LIKE %s INCLUDING DEFAULTS INCLUDING IDENTITY EXCLUDING CONSTRAINTS) ON COMMIT DROP", tempIdent, target)
	if _, err := tx.Exec(ctx, create); err != nil {
		return fmt.Errorf("create temp table: %w", err)
	}

	for _, group := range groups {
		if len(group.rows) == 0 {
			continue
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", tempIdent)); err != nil {
			return fmt.Errorf("truncate temp table: %w", err)
		}
		if err := d.copyRowsInto(ctx, tx, pgx.Identifier{tempName}, group.cols, group.rows); err != nil {
			valuesClause, args, err := buildValuesClause(group.cols, colTypes, group.rows)
			if err != nil {
				return err
			}
			stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tempIdent, quoteColumns(group.cols, '"'), valuesClause)
			if _, err := tx.Exec(ctx, stmt, args...); err != nil {
				return fmt.Errorf("insert temp rows: %w", err)
			}
		}

		insertCols := quoteColumns(group.cols, '"')
		quotedKeyCols := make([]string, 0, len(group.keyCols))
		for _, col := range group.keyCols {
			quotedKeyCols = append(quotedKeyCols, quoteIdent(col, '"'))
		}
		keyCols := strings.Join(quotedKeyCols, ", ")
		updateCols := make([]string, 0, len(group.cols))
		keySet := map[string]struct{}{}
		for _, key := range group.keyCols {
			keySet[key] = struct{}{}
		}
		for _, col := range group.cols {
			if _, ok := keySet[col]; ok {
				continue
			}
			updateCols = append(updateCols, col)
		}
		conflictAction := "DO NOTHING"
		if len(updateCols) > 0 {
			setClause := make([]string, 0, len(updateCols))
			for _, col := range updateCols {
				setClause = append(setClause, fmt.Sprintf("%s = EXCLUDED.%s", quoteIdent(col, '"'), quoteIdent(col, '"')))
			}
			conflictAction = "DO UPDATE SET " + strings.Join(setClause, ", ")
		}
		upsert := fmt.Sprintf(
			"INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) %s",
			target,
			insertCols,
			insertCols,
			tempIdent,
			keyCols,
			conflictAction,
		)
		if _, err := tx.Exec(ctx, upsert); err != nil {
			return fmt.Errorf("upsert rows: %w", err)
		}
	}
	return nil
}

func (d *Destination) updateRows(ctx context.Context, tx pgx.Tx, target string, schema connector.Schema, records []connector.Record) error {
	if len(records) == 0 {
		return nil
	}
	colTypes := columnTypeMap(schema)
	groups := map[string]*updateGroup{}
	for _, record := range records {
		cols, vals, _, err := recordColumns(schema, record)
		if err != nil {
			return err
		}
		if len(cols) == 0 {
			continue
		}
		keyCols, keyVals, err := keyColumnsAndValues(schema, record)
		if err != nil {
			return err
		}
		if len(keyCols) == 0 {
			return errors.New("update requires record key")
		}
		key := columnsKey(keyCols) + "||" + columnsKey(cols)
		group := groups[key]
		if group == nil {
			group = &updateGroup{keyCols: keyCols, cols: cols}
			groups[key] = group
		}
		row := append(append([]any{}, keyVals...), vals...)
		group.rows = append(group.rows, row)
	}
	for _, group := range groups {
		if len(group.rows) == 0 {
			continue
		}
		localTypes := normalizeKeyColumnTypes(colTypes, group.keyCols, group.rows)
		keyAliases := make([]string, 0, len(group.keyCols))
		aliasTypes := make(map[string]string, len(localTypes)+len(group.keyCols))
		for name, typ := range localTypes {
			aliasTypes[name] = typ
		}
		for _, col := range group.keyCols {
			alias := "__key_" + col
			keyAliases = append(keyAliases, alias)
			if typ := columnType(localTypes, col); typ != "" {
				aliasTypes[alias] = typ
			}
		}
		allCols := append(append([]string{}, keyAliases...), group.cols...)
		valuesClause, args, err := buildValuesClause(allCols, aliasTypes, group.rows)
		if err != nil {
			return err
		}
		setClause := make([]string, 0, len(group.cols))
		for _, col := range group.cols {
			setClause = append(setClause, fmt.Sprintf("%s = v.%s", quoteIdent(col, '"'), quoteIdent(col, '"')))
		}
		whereClause := make([]string, 0, len(group.keyCols))
		for idx, col := range group.keyCols {
			alias := keyAliases[idx]
			if colType := columnType(localTypes, col); colType != "" {
				if postgresJSONType(colType) != "" {
					whereClause = append(whereClause, fmt.Sprintf("t.%s::text = v.%s::text", quoteIdent(col, '"'), quoteIdent(alias, '"')))
				} else {
					whereClause = append(whereClause, fmt.Sprintf("t.%s = v.%s", quoteIdent(col, '"'), quoteIdent(alias, '"')))
				}
			} else {
				whereClause = append(whereClause, fmt.Sprintf("t.%s::text = v.%s::text", quoteIdent(col, '"'), quoteIdent(alias, '"')))
			}
		}
		stmt := fmt.Sprintf(
			"UPDATE %s AS t SET %s FROM (VALUES %s) AS v(%s) WHERE %s",
			target,
			strings.Join(setClause, ", "),
			valuesClause,
			quoteColumns(allCols, '"'),
			strings.Join(whereClause, " AND "),
		)
		if _, err := tx.Exec(ctx, stmt, args...); err != nil {
			return fmt.Errorf("update rows: %w", err)
		}
	}
	return nil
}

func (d *Destination) deleteRows(ctx context.Context, tx pgx.Tx, target string, schema connector.Schema, records []connector.Record) error {
	if len(records) == 0 {
		return nil
	}
	colTypes := columnTypeMap(schema)
	groups := map[string]*deleteGroup{}
	for _, record := range records {
		keyCols, keyVals, err := keyColumnsAndValues(schema, record)
		if err != nil {
			return err
		}
		if len(keyCols) == 0 {
			return errors.New("delete requires record key")
		}
		key := columnsKey(keyCols)
		group := groups[key]
		if group == nil {
			group = &deleteGroup{keyCols: keyCols}
			groups[key] = group
		}
		group.rows = append(group.rows, keyVals)
	}
	for _, group := range groups {
		if len(group.rows) == 0 {
			continue
		}
		localTypes := normalizeKeyColumnTypes(colTypes, group.keyCols, group.rows)
		valuesClause, args, err := buildValuesClause(group.keyCols, localTypes, group.rows)
		if err != nil {
			return err
		}
		whereClause := make([]string, 0, len(group.keyCols))
		for _, col := range group.keyCols {
			if colType := columnType(localTypes, col); colType != "" {
				if postgresJSONType(colType) != "" {
					whereClause = append(whereClause, fmt.Sprintf("t.%s::text = v.%s::text", quoteIdent(col, '"'), quoteIdent(col, '"')))
				} else {
					whereClause = append(whereClause, fmt.Sprintf("t.%s = v.%s", quoteIdent(col, '"'), quoteIdent(col, '"')))
				}
			} else {
				whereClause = append(whereClause, fmt.Sprintf("t.%s::text = v.%s::text", quoteIdent(col, '"'), quoteIdent(col, '"')))
			}
		}
		stmt := fmt.Sprintf(
			"DELETE FROM %s AS t USING (VALUES %s) AS v(%s) WHERE %s",
			target,
			valuesClause,
			quoteColumns(group.keyCols, '"'),
			strings.Join(whereClause, " AND "),
		)
		if _, err := tx.Exec(ctx, stmt, args...); err != nil {
			return fmt.Errorf("delete rows: %w", err)
		}
	}
	return nil
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

func (d *Destination) insertRow(ctx context.Context, tx pgx.Tx, target string, schema connector.Schema, record connector.Record) error {
	cols, vals, exprs, err := recordColumns(schema, record)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", target, quoteColumns(cols, '"'), strings.Join(exprs, ", "))
	if _, err := tx.Exec(ctx, stmt, vals...); err != nil {
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
		if _, err := d.pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", target)); err != nil {
			return fmt.Errorf("truncate target: %w", err)
		}
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", target, colList, colList, staging)
	if _, err := d.pool.Exec(ctx, stmt); err != nil {
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
		return nil, nil
	}
	if targetSchema == "" {
		targetSchema = "public"
	}
	rows, err := d.pool.Query(ctx,
		`SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`,
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

func (d *Destination) updateRow(ctx context.Context, tx pgx.Tx, target string, schema connector.Schema, record connector.Record) error {
	key, err := decodeKey(record.Key)
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

	whereClause, whereArgs := whereFromKey(key, '"', "", len(vals))
	args := append(vals, whereArgs...)

	stmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s", target, strings.Join(setClause, ", "), whereClause)
	if _, err := tx.Exec(ctx, stmt, args...); err != nil {
		return fmt.Errorf("update row: %w", err)
	}
	return nil
}

func (d *Destination) deleteRow(ctx context.Context, tx pgx.Tx, target string, record connector.Record) error {
	key, err := decodeKey(record.Key)
	if err != nil {
		return err
	}
	if len(key) == 0 {
		return errors.New("delete requires record key")
	}

	whereClause, args := whereFromKey(key, '"', "", 0)
	stmt := fmt.Sprintf("DELETE FROM %s WHERE %s", target, whereClause)
	if _, err := tx.Exec(ctx, stmt, args...); err != nil {
		return fmt.Errorf("delete row: %w", err)
	}
	return nil
}

func (d *Destination) ensureMetaTable(ctx context.Context) error {
	if d.metaSchema == "" || d.metaTable == "" {
		return errors.New("meta schema and table are required")
	}
	schemaIdent := quoteIdent(d.metaSchema, '"')
	if _, err := d.pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaIdent)); err != nil {
		return fmt.Errorf("create meta schema: %w", err)
	}
	tableIdent := schemaIdent + "." + quoteIdent(d.metaTable, '"')
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  flow_id TEXT,
  source_schema TEXT,
  source_table TEXT,
  synced_at TIMESTAMPTZ,
  is_deleted BOOLEAN,
  lsn TEXT,
  operation TEXT,
  key_json TEXT
)`, tableIdent)
	if _, err := d.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("create meta table: %w", err)
	}
	return nil
}

func (d *Destination) upsertMetadata(ctx context.Context, tx pgx.Tx, schema connector.Schema, record connector.Record, checkpoint connector.Checkpoint) error {
	key, err := decodeKey(record.Key)
	if err != nil {
		return err
	}

	pkCols := make([]string, 0, len(key))
	pkVals := make([]any, 0, len(key))
	for col, val := range key {
		metaCol := d.metaPKPrefix + col
		if err := d.ensureMetaColumn(ctx, metaCol); err != nil {
			return err
		}
		pkCols = append(pkCols, metaCol)
		pkVals = append(pkVals, val)
	}

	target := quoteIdent(d.metaSchema, '"') + "." + quoteIdent(d.metaTable, '"')
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

	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", target, quoteColumns(columns, '"'), placeholders(1, len(columns)))
	if _, err := tx.Exec(ctx, stmt, values...); err != nil {
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
	stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s TEXT", target, quoteIdent(column, '"'))
	if _, err := d.pool.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("add meta column: %w", err)
	}
	d.metaColumns[key] = struct{}{}
	return nil
}

func recordColumns(schema connector.Schema, record connector.Record) ([]string, []any, []string, error) {
	if record.After == nil {
		return nil, nil, nil, nil
	}
	cols := make([]string, 0, len(schema.Columns))
	vals := make([]any, 0, len(schema.Columns))
	exprs := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		val, ok := record.After[col.Name]
		if !ok {
			continue
		}
		normalized, err := normalizePostgresValue(col.Type, val)
		if err != nil {
			return nil, nil, nil, err
		}
		placeholder := fmt.Sprintf("$%d", len(vals)+1)
		expr := placeholder
		if jsonType := postgresJSONType(col.Type); jsonType != "" {
			expr = fmt.Sprintf("CAST(%s AS %s)", placeholder, jsonType)
		}
		cols = append(cols, col.Name)
		vals = append(vals, normalized)
		exprs = append(exprs, expr)
	}
	return cols, vals, exprs, nil
}

func normalizePostgresValue(colType string, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if isPostgresArrayType(colType) {
		return value, nil
	}
	if jsonType := postgresJSONType(colType); jsonType != "" {
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

func postgresJSONType(colType string) string {
	normalized := normalizeTypeKey(colType)
	switch normalized {
	case "jsonb":
		return "jsonb"
	case "json":
		return "json"
	default:
		return ""
	}
}

func columnTypeMap(schema connector.Schema) map[string]string {
	if len(schema.Columns) == 0 {
		return nil
	}
	out := make(map[string]string, len(schema.Columns))
	for _, col := range schema.Columns {
		out[col.Name] = col.Type
		if lower := strings.ToLower(col.Name); lower != col.Name {
			out[lower] = col.Type
		}
	}
	return out
}

func columnsKey(cols []string) string {
	if len(cols) == 0 {
		return ""
	}
	return strings.Join(cols, "|")
}

func keyColumnsAndValues(schema connector.Schema, record connector.Record) ([]string, []any, error) {
	keyMap, err := decodeKey(record.Key)
	if err != nil {
		return nil, nil, err
	}
	if len(keyMap) == 0 {
		return nil, nil, nil
	}
	keys := make([]string, 0, len(keyMap))
	for key := range keyMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	colTypes := columnTypeMap(schema)
	values := make([]any, 0, len(keys))
	for _, key := range keys {
		val := keyMap[key]
		if colType := columnType(colTypes, key); colType != "" {
			normalized, err := normalizePostgresValue(colType, val)
			if err != nil {
				return nil, nil, err
			}
			val = normalized
			if coerced, ok, err := coerceKeyValue(colType, val); err != nil {
				return nil, nil, err
			} else if ok {
				val = coerced
			}
		}
		values = append(values, val)
	}
	return keys, values, nil
}

func buildValuesClause(cols []string, colTypes map[string]string, rows [][]any) (string, []any, error) {
	if len(cols) == 0 || len(rows) == 0 {
		return "", nil, nil
	}
	args := make([]any, 0, len(rows)*len(cols))
	valueRows := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) != len(cols) {
			return "", nil, fmt.Errorf("row has %d values for %d columns", len(row), len(cols))
		}
		exprs := make([]string, 0, len(cols))
		for idx, col := range cols {
			args = append(args, row[idx])
			placeholder := fmt.Sprintf("$%d", len(args))
			expr := placeholder
			if colType := columnType(colTypes, col); colType != "" {
				castType := postgresJSONType(colType)
				if castType == "" {
					castType = colType
				}
				expr = fmt.Sprintf("CAST(%s AS %s)", placeholder, castType)
			}
			exprs = append(exprs, expr)
		}
		valueRows = append(valueRows, "("+strings.Join(exprs, ", ")+")")
	}
	return strings.Join(valueRows, ", "), args, nil
}

func coerceKeyValue(colType string, value any) (any, bool, error) {
	switch normalizeTypeKey(colType) {
	case "int2", "smallint", "int4", "integer", "int", "int8", "bigint", "serial", "bigserial":
		coerced, ok, err := coerceInt64(value)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		switch normalizeTypeKey(colType) {
		case "int2", "smallint":
			return int16(coerced), true, nil
		case "int4", "integer", "int", "serial":
			return int32(coerced), true, nil
		default:
			return coerced, true, nil
		}
	case "bool", "boolean":
		coerced, ok, err := coerceBool(value)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return coerced, true, nil
		}
	}
	return nil, false, nil
}

func coerceInt64(value any) (int64, bool, error) {
	switch v := value.(type) {
	case int64:
		return v, true, nil
	case int32:
		return int64(v), true, nil
	case int:
		return int64(v), true, nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, false, fmt.Errorf("value %d overflows int64", v)
		}
		return int64(v), true, nil
	case float64:
		if math.Trunc(v) != v {
			return 0, false, fmt.Errorf("value %v is not an integer", v)
		}
		return int64(v), true, nil
	case json.Number:
		i, err := v.Int64()
		if err != nil {
			return 0, false, err
		}
		return i, true, nil
	case string:
		if v == "" {
			return 0, false, nil
		}
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, false, err
		}
		return i, true, nil
	default:
		return 0, false, nil
	}
}

func coerceBool(value any) (bool, bool, error) {
	switch v := value.(type) {
	case bool:
		return v, true, nil
	case string:
		if v == "" {
			return false, false, nil
		}
		b, err := strconv.ParseBool(v)
		if err != nil {
			return false, false, err
		}
		return b, true, nil
	default:
		return false, false, nil
	}
}

func keyChanged(schema connector.Schema, record connector.Record) (bool, error) {
	if record.After == nil {
		return false, nil
	}
	keyMap, err := decodeKey(record.Key)
	if err != nil {
		return false, err
	}
	if len(keyMap) == 0 {
		return false, nil
	}
	colTypes := columnTypeMap(schema)
	for key, oldVal := range keyMap {
		afterVal, ok := record.After[key]
		if !ok {
			continue
		}
		oldNorm, err := normalizeComparable(columnType(colTypes, key), oldVal)
		if err != nil {
			return false, err
		}
		newNorm, err := normalizeComparable(columnType(colTypes, key), afterVal)
		if err != nil {
			return false, err
		}
		if !reflect.DeepEqual(oldNorm, newNorm) {
			return true, nil
		}
	}
	return false, nil
}

func dedupeUpsertRecords(records []connector.Record, schema connector.Schema) ([]connector.Record, error) {
	colTypes := columnTypeMap(schema)
	indexByKey := make(map[string]int, len(records))
	out := make([]connector.Record, 0, len(records))
	for _, record := range records {
		key, err := upsertDedupKey(record, colTypes)
		if err != nil {
			return nil, err
		}
		if key == "" {
			return records, nil
		}
		if idx, ok := indexByKey[key]; ok {
			out[idx] = record
			continue
		}
		indexByKey[key] = len(out)
		out = append(out, record)
	}
	if len(out) == 0 {
		return records, nil
	}
	return out, nil
}

func upsertDedupKey(record connector.Record, colTypes map[string]string) (string, error) {
	keyMap, err := decodeKey(record.Key)
	if err != nil {
		return "", err
	}
	if len(keyMap) == 0 {
		return "", nil
	}
	keys := make([]string, 0, len(keyMap))
	for key := range keyMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var buf strings.Builder
	for _, key := range keys {
		if buf.Len() > 0 {
			buf.WriteString("|")
		}
		buf.WriteString(key)
		buf.WriteString("=")
		val := keyMap[key]
		if colType := columnType(colTypes, key); colType != "" {
			normalized, err := normalizeComparable(colType, val)
			if err != nil {
				return "", err
			}
			val = normalized
		}
		payload, err := json.Marshal(val)
		if err != nil {
			return "", err
		}
		buf.Write(payload)
	}
	return buf.String(), nil
}

func normalizeComparable(colType string, value any) (any, error) {
	if colType != "" {
		normalized, err := normalizePostgresValue(colType, value)
		if err != nil {
			return nil, err
		}
		if coerced, ok, err := coerceKeyValue(colType, normalized); err != nil {
			return nil, err
		} else if ok {
			return coerced, nil
		}
		return normalized, nil
	}
	return value, nil
}

func (d *Destination) copyRows(ctx context.Context, tx pgx.Tx, target string, cols []string, rows [][]any) error {
	ident, err := parseTargetIdent(target)
	if err != nil {
		return err
	}
	return d.copyRowsInto(ctx, tx, ident, cols, rows)
}

func (d *Destination) copyRowsInto(ctx context.Context, tx pgx.Tx, ident pgx.Identifier, cols []string, rows [][]any) error {
	if len(cols) == 0 || len(rows) == 0 {
		return nil
	}
	_, err := tx.CopyFrom(ctx, ident, cols, pgx.CopyFromRows(rows))
	if err != nil {
		return fmt.Errorf("copy from: %w", err)
	}
	return nil
}

func parseTargetIdent(target string) (pgx.Identifier, error) {
	trimmed := strings.TrimSpace(target)
	if trimmed == "" {
		return nil, errors.New("target table is required")
	}
	parts := strings.Split(trimmed, ".")
	ident := make(pgx.Identifier, 0, len(parts))
	for _, part := range parts {
		ident = append(ident, unquoteIdent(part))
	}
	return ident, nil
}

func unquoteIdent(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 2 {
		quote := value[0]
		if (quote == '"' || quote == '`') && value[len(value)-1] == quote {
			value = value[1 : len(value)-1]
			if quote == '"' {
				value = strings.ReplaceAll(value, `""`, `"`)
			} else {
				value = strings.ReplaceAll(value, "``", "`")
			}
		}
	}
	return value
}

func columnType(colTypes map[string]string, col string) string {
	if len(colTypes) == 0 {
		return ""
	}
	if typ, ok := colTypes[col]; ok {
		return typ
	}
	if typ, ok := colTypes[strings.ToLower(col)]; ok {
		return typ
	}
	return ""
}

func comparisonCastType(colType string) string {
	castType := postgresJSONType(colType)
	if castType != "" {
		return "text"
	}
	return colType
}

func normalizeTypeKey(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.TrimSuffix(value, "[]")
	value = strings.TrimPrefix(value, "_")
	if idx := strings.Index(value, "("); idx > 0 {
		value = value[:idx]
	}
	if idx := strings.LastIndex(value, "."); idx > 0 {
		value = value[idx+1:]
	}
	return strings.TrimSpace(value)
}

func normalizeKeyColumnTypes(colTypes map[string]string, keyCols []string, rows [][]any) map[string]string {
	if len(keyCols) == 0 || len(rows) == 0 {
		return colTypes
	}
	row := rows[0]
	if len(row) == 0 {
		return colTypes
	}
	var out map[string]string
	for idx, col := range keyCols {
		if idx >= len(row) {
			break
		}
		colType := columnType(colTypes, col)
		if postgresJSONType(colType) != "" {
			continue
		}
		if !looksLikeJSONValue(row[idx]) {
			continue
		}
		if out == nil {
			out = make(map[string]string, len(colTypes)+2)
			for key, val := range colTypes {
				out[key] = val
			}
		}
		out[col] = "jsonb"
		if lower := strings.ToLower(col); lower != col {
			out[lower] = "jsonb"
		}
	}
	if out == nil {
		return colTypes
	}
	return out
}

func looksLikeJSONValue(value any) bool {
	switch value.(type) {
	case map[string]any, []any, json.RawMessage:
		return true
	default:
		return false
	}
}

func isPostgresArrayType(colType string) bool {
	normalized := strings.TrimSpace(strings.ToLower(colType))
	return strings.HasSuffix(normalized, "[]") || strings.HasPrefix(normalized, "_")
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

func whereFromKey(key map[string]any, quote rune, prefix string, offset int) (string, []any) {
	parts := make([]string, 0, len(key))
	args := make([]any, 0, len(key))
	idx := offset
	for col, val := range key {
		idx++
		name := prefix + col
		parts = append(parts, fmt.Sprintf("%s = $%d", quoteIdent(name, quote), idx))
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

func placeholders(start, count int) string {
	if count <= 0 {
		return ""
	}
	parts := make([]string, 0, count)
	for i := 0; i < count; i++ {
		parts = append(parts, fmt.Sprintf("$%d", start+i))
	}
	return strings.Join(parts, ",")
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
