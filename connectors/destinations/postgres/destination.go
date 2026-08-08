package postgres

import (
	"bytes"
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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/ddl"
	postgrescodec "github.com/josephjohncox/wallaby/internal/postgres"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

const (
	optDSN             = "dsn"
	optSchema          = "schema"
	optTable           = "table"
	optBatchMode       = "batch_mode"
	optBatchResolution = "batch_resolution"
	optStagingSchema   = "staging_schema"
	optStagingTable    = "staging_table"
	optStagingSuffix   = "staging_suffix"
	optMetaTable       = "meta_table"
	optMetaSchema      = "meta_schema"
	optMetaEnabled     = "meta_table_enabled"
	optManagedProfile  = "managed_profile"
	optMetaPKPrefix    = "meta_pk_prefix"
	optFlowID          = "flow_id"
	optSyncCommit      = "synchronous_commit"

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

type ddlExecer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

type txBeginner func(context.Context) (pgx.Tx, error)

type postgresTarget struct {
	identifier pgx.Identifier
	sql        string
}

type postgresTargetRecords struct {
	target  postgresTarget
	records []connector.Record
}

// Destination writes change events into Postgres tables.
type Destination struct {
	spec                 connector.RuntimeSpec
	pool                 *pgxpool.Pool
	beginTx              txBeginner
	ddlExecutor          ddlExecer
	batchMode            string
	batchResolve         string
	stagingSchema        string
	stagingTableName     string
	stagingSuffix        string
	metaEnabled          bool
	metaSchema           string
	metaTable            string
	metaPKPrefix         string
	flowID               string
	syncCommit           string
	managedPostgresMajor int
	metaColumns          map[string]struct{}
	stagingTables        map[string]tableInfo
	stagingResolved      bool
}

func (d *Destination) Open(ctx context.Context, spec connector.RuntimeSpec) error {
	d.spec = spec
	opened := false
	defer func() {
		if !opened && d.pool != nil {
			d.pool.Close()
			d.pool = nil
		}
	}()
	dsn := spec.Options[optDSN]
	if dsn == "" {
		return errors.New("postgres dsn is required")
	}

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse postgres dsn: %w", err)
	}
	iamProvider, err := postgrescodec.NewRDSIAMTokenProvider(ctx, dsn, spec.Options)
	if err != nil {
		return err
	}
	if err := iamProvider.ApplyToPoolConfig(ctx, poolCfg); err != nil {
		return err
	}
	maxConns := parseInt(spec.Options["pool_max_conns"], 4)
	if maxConns < 1 || maxConns > 64 {
		return fmt.Errorf("postgres pool_max_conns must be between 1 and 64, got %d", maxConns)
	}
	poolCfg.MaxConns = int32(maxConns) // #nosec G115 -- range checked above.
	poolCfg.MinConns = 0

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("ping postgres: %w", err)
	}
	d.pool = pool
	d.managedPostgresMajor, err = validateManagedPostgresServerVersion(ctx, pool, spec.Options[optManagedProfile])
	if err != nil {
		return err
	}

	d.batchMode = strings.ToLower(spec.Options[optBatchMode])
	if d.batchMode == "" {
		d.batchMode = batchModeTarget
	}
	d.batchResolve = strings.ToLower(spec.Options[optBatchResolution])
	if d.batchResolve == "" {
		d.batchResolve = batchResolveNone
	}
	d.stagingSchema = spec.Options[optStagingSchema]
	d.stagingTableName = spec.Options[optStagingTable]
	d.stagingSuffix = spec.Options[optStagingSuffix]
	for option, identifier := range map[string]string{optStagingSchema: d.stagingSchema, optStagingTable: d.stagingTableName} {
		if strings.ContainsRune(identifier, '\x00') {
			return fmt.Errorf("postgres %s contains NUL", option)
		}
	}
	if d.stagingSuffix == "" {
		d.stagingSuffix = defaultStagingSuffix
	}

	d.metaEnabled = parseBool(spec.Options[optMetaEnabled], true)
	d.metaSchema = spec.Options[optMetaSchema]
	if d.metaSchema == "" {
		d.metaSchema = defaultMetaSchema
	}
	d.metaTable = spec.Options[optMetaTable]
	if d.metaTable == "" {
		d.metaTable = defaultMetaTable
	}
	if strings.ContainsRune(d.metaSchema, '\x00') || strings.ContainsRune(d.metaTable, '\x00') {
		return errors.New("postgres metadata identifiers must not contain NUL")
	}
	d.metaPKPrefix = spec.Options[optMetaPKPrefix]
	if d.metaPKPrefix == "" {
		d.metaPKPrefix = defaultMetaPKPref
	}
	d.flowID = spec.Options[optFlowID]
	d.syncCommit = normalizeSyncCommit(spec.Options[optSyncCommit])
	d.metaColumns = map[string]struct{}{}
	d.stagingTables = map[string]tableInfo{}

	if d.metaEnabled {
		if err := d.ensureMetaTable(ctx); err != nil {
			return err
		}
	}
	if strings.TrimSpace(spec.Options[optManagedProfile]) != "" && d.batchMode == batchModeTarget {
		if err := d.ensureManagedReceiptTable(ctx); err != nil {
			return err
		}
	}

	opened = true
	return nil
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) error {
	if len(batch.Records) == 0 {
		return nil
	}
	if err := validateTableScopedBatch(batch); err != nil {
		return err
	}
	if d.pool == nil && d.beginTx == nil {
		return errors.New("postgres destination not initialized")
	}

	tx, err := d.beginWriteTransaction(ctx)
	if err != nil {
		return err
	}
	if err := d.applyTransaction(ctx, tx, batch); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func validateTableScopedBatch(batch connector.Batch) error {
	for _, record := range batch.Records {
		if record.Operation == connector.OpDDL {
			continue
		}
		if batch.Schema.Name == "" {
			return errors.New("generic PostgreSQL data batches require one table-scoped schema")
		}
		if record.Table != batch.Schema.Name {
			return fmt.Errorf("generic PostgreSQL data batch for table %q contains record for table %q; full-transaction delivery is a separate managed contract", batch.Schema.Name, record.Table)
		}
	}
	return nil
}

func (d *Destination) beginWriteTransaction(ctx context.Context) (pgx.Tx, error) {
	var tx pgx.Tx
	var err error
	if d.beginTx != nil {
		tx, err = d.beginTx(ctx)
	} else {
		tx, err = d.pool.Begin(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	if d.syncCommit != "" {
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL synchronous_commit = %s", d.syncCommit)); err != nil {
			_ = tx.Rollback(ctx)
			return nil, fmt.Errorf("set synchronous_commit: %w", err)
		}
	}
	return tx, nil
}

func (d *Destination) applyTransaction(ctx context.Context, tx pgx.Tx, batch connector.Batch) error {
	targetRecords := map[string]*postgresTargetRecords{}
	for _, record := range batch.Records {
		if record.Operation == connector.OpDDL {
			continue
		}
		target, isStaging, err := d.resolveTarget(batch.Schema, record)
		if err != nil {
			return err
		}
		if isStaging {
			d.trackStaging(batch.Schema, record)
		}
		key := strings.Join(target.identifier, "\x00")
		group := targetRecords[key]
		if group == nil {
			group = &postgresTargetRecords{target: target}
			targetRecords[key] = group
		}
		group.records = append(group.records, record)
	}
	if len(targetRecords) == 0 {
		return nil
	}
	mode := writeModeTarget
	switch batch.WritePolicy.Mode {
	case connector.ResolvedWriteAppend:
		mode = writeModeAppend
	case connector.ResolvedWriteUpsert:
	default:
		return fmt.Errorf("postgres destination requires a resolved table write policy, got %q", batch.WritePolicy.Mode)
	}
	for _, group := range targetRecords {
		if err := d.applyBatch(ctx, tx, group.target, batch.Schema, group.records, mode, batch.WritePolicy); err != nil {
			return err
		}
	}
	if d.metaEnabled {
		if err := d.upsertMetadataBatch(ctx, tx, batch.Schema, batch.Records, batch.Checkpoint); err != nil {
			return err
		}
	}
	return nil
}

func (d *Destination) Close(ctx context.Context) error {
	if d.pool == nil {
		return nil
	}
	defer func() {
		d.pool.Close()
		d.pool = nil
	}()
	return d.finalizeStaging(ctx)
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
		Support:     connector.SupportExperimental,
		TableWrites: connector.TableWriteSemantics{Append: true, Upsert: true, ExplicitKey: true, WatermarkGuard: true},
		Delivery: connector.DeliverySemantics{
			TransactionalBatch: true,
			ExecutesDDL:        true,
		},
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
	executor := d.ddlExecutor
	if executor == nil {
		executor = d.pool
	}
	if executor == nil {
		return errors.New("postgres destination not initialized")
	}

	var statements []string
	if strings.TrimSpace(record.DDL) == "" && len(record.DDLPlan) > 0 {
		planStatements, err := ddl.TranslateRecordDDL(schema, record, ddl.DialectConfigFor(ddl.DialectPostgres), d.TypeMappings(), d.spec.Options)
		if err != nil {
			return fmt.Errorf("translate ddl plan: %w", err)
		}
		statements = planStatements
	} else {
		statements = []string{record.DDL}
	}

	if len(statements) == 0 {
		return nil
	}

	for _, statement := range statements {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := executor.Exec(ctx, statement); err != nil {
			return fmt.Errorf("apply ddl: %w", err)
		}
	}
	return nil
}

func (d *Destination) TypeMappings() map[string]string { return nil }

func (d *Destination) applyBatch(ctx context.Context, tx pgx.Tx, target postgresTarget, schema connector.Schema, records []connector.Record, mode string, policy connector.TableWritePolicy) error {
	if len(records) == 0 {
		return nil
	}
	switch mode {
	case writeModeAppend:
		return d.applyAppendBatch(ctx, tx, target, schema, records)
	default:
		if policy.WatermarkColumn != "" {
			return d.applyWatermarkBatch(ctx, tx, target.sql, schema, records, policy)
		}
		return d.applyTargetBatch(ctx, tx, target, schema, records, policy)
	}
}

func (d *Destination) applyAppendBatch(ctx context.Context, tx pgx.Tx, target postgresTarget, schema connector.Schema, records []connector.Record) error {
	inserts := make([]connector.Record, 0, len(records))
	for _, record := range records {
		if record.Operation == connector.OpDelete {
			continue
		}
		inserts = append(inserts, record)
	}
	return d.insertRows(ctx, tx, target, schema, inserts)
}

func (d *Destination) applyTargetBatch(ctx context.Context, tx pgx.Tx, target postgresTarget, schema connector.Schema, records []connector.Record, policy connector.TableWritePolicy) error {
	groups, err := planTargetOperations(schema, records)
	if err != nil {
		return err
	}
	for _, group := range groups {
		switch group.kind {
		case targetOperationUpsert:
			err = d.upsertRows(ctx, tx, target.sql, schema, group.records, policy.KeyColumns)
		case targetOperationKeyChange:
			err = d.updateRows(ctx, tx, target.sql, schema, group.records)
		case targetOperationDelete:
			err = d.deleteRows(ctx, tx, target.sql, schema, group.records)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

type targetOperationKind uint8

const (
	targetOperationUpsert targetOperationKind = iota + 1
	targetOperationKeyChange
	targetOperationDelete
)

type targetOperationGroup struct {
	kind    targetOperationKind
	records []connector.Record
}

func planTargetOperations(schema connector.Schema, records []connector.Record) ([]targetOperationGroup, error) {
	groups := make([]targetOperationGroup, 0, len(records))
	for _, record := range records {
		var kind targetOperationKind
		switch record.Operation {
		case connector.OpInsert, connector.OpLoad:
			kind = targetOperationUpsert
		case connector.OpUpdate:
			changed, err := keyChanged(schema, record)
			if err != nil {
				return nil, err
			}
			if changed {
				kind = targetOperationKeyChange
			} else {
				kind = targetOperationUpsert
			}
		case connector.OpDelete:
			kind = targetOperationDelete
		default:
			continue
		}

		// Key-changing updates must execute one statement at a time. Combining
		// 1→2 and 2→3 in one UPDATE ... FROM statement evaluates both matches
		// against one PostgreSQL statement snapshot and leaves the row at 2.
		if len(groups) == 0 || groups[len(groups)-1].kind != kind || kind == targetOperationKeyChange {
			groups = append(groups, targetOperationGroup{kind: kind})
		}
		groups[len(groups)-1].records = append(groups[len(groups)-1].records, record)
	}
	return groups, nil
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

func (d *Destination) insertRows(ctx context.Context, tx pgx.Tx, target postgresTarget, schema connector.Schema, records []connector.Record) error {
	if len(records) == 0 {
		return nil
	}
	colTypes := columnTypeMap(schema)
	groups := map[string]*rowGroup{}
	for _, record := range records {
		cols, vals, err := recordColumns(schema, record)
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
		if err := d.copyRows(ctx, tx, target.identifier, group.cols, group.rows); err != nil {
			valuesClause, args, err := buildValuesClause(group.cols, colTypes, group.rows)
			if err != nil {
				return err
			}
			stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", target.sql, quoteColumns(group.cols), valuesClause)
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

func (d *Destination) upsertRows(ctx context.Context, tx pgx.Tx, target string, schema connector.Schema, records []connector.Record, policyKeys []string) error {
	if len(records) == 0 {
		return nil
	}
	if len(records) > 1 {
		batches, err := partitionUpsertRecords(records, schema)
		if err != nil {
			return err
		}
		if len(batches) > 1 {
			for index, batch := range batches {
				if err := d.upsertRows(ctx, tx, target, schema, batch, policyKeys); err != nil {
					return fmt.Errorf("apply ordered upsert batch %d: %w", index, err)
				}
			}
			return nil
		}
		records = batches[0]
	}
	colTypes := columnTypeMap(schema)
	groups := map[string]*upsertGroup{}
	for _, record := range records {
		cols, vals, err := recordColumns(schema, record)
		if err != nil {
			return err
		}
		if len(cols) == 0 {
			continue
		}
		keyCols, keyVals, err := orderedPolicyKeyValues(schema, record, policyKeys)
		if err != nil {
			return err
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
	tempIdent := quoteIdent(tempName)
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
			stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tempIdent, quoteColumns(group.cols), valuesClause)
			if _, err := tx.Exec(ctx, stmt, args...); err != nil {
				return fmt.Errorf("insert temp rows: %w", err)
			}
		}

		insertCols := quoteColumns(group.cols)
		quotedKeyCols := make([]string, 0, len(group.keyCols))
		for _, col := range group.keyCols {
			quotedKeyCols = append(quotedKeyCols, quoteIdent(col))
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
				setClause = append(setClause, fmt.Sprintf("%s = EXCLUDED.%s", quoteIdent(col), quoteIdent(col)))
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

func orderedPolicyKeyValues(schema connector.Schema, record connector.Record, policyKeys []string) ([]string, []any, error) {
	if len(policyKeys) == 0 {
		return nil, nil, errors.New("upsert requires projected policy key columns")
	}
	columns, values, err := keyColumnsAndValues(schema, record)
	if err != nil {
		return nil, nil, err
	}
	if len(columns) != len(policyKeys) {
		return nil, nil, fmt.Errorf("record key columns %v do not match projected policy key columns %v", columns, policyKeys)
	}
	byColumn := make(map[string]any, len(columns))
	for index, column := range columns {
		byColumn[column] = values[index]
	}
	ordered := make([]any, len(policyKeys))
	seen := make(map[string]struct{}, len(policyKeys))
	for index, column := range policyKeys {
		if _, duplicate := seen[column]; duplicate {
			return nil, nil, fmt.Errorf("projected policy repeats key column %q", column)
		}
		seen[column] = struct{}{}
		value, ok := byColumn[column]
		if !ok {
			return nil, nil, fmt.Errorf("record key is missing projected policy key column %q", column)
		}
		ordered[index] = value
	}
	return append([]string(nil), policyKeys...), ordered, nil
}

type preparedWatermarkMutation struct {
	record             connector.Record
	keyValues          []any
	canonicalKeys      []string
	watermarkValue     any
	canonicalWatermark string
	sourcePosition     string
	contentHash        string
	deleted            bool
}

func (d *Destination) applyWatermarkBatch(ctx context.Context, tx pgx.Tx, target string, schema connector.Schema, records []connector.Record, policy connector.TableWritePolicy) error {
	flowID := strings.TrimSpace(d.flowID)
	if flowID == "" {
		return errors.New("watermark-guarded writes require a flow_id")
	}
	if strings.TrimSpace(policy.ProjectionFingerprint) == "" {
		return errors.New("watermark-guarded writes require a projection fingerprint")
	}
	watermarkType, err := postgresWatermarkType(schema, policy.WatermarkColumn)
	if err != nil {
		return err
	}
	keyTypes, err := postgresKeyTypes(schema, policy.KeyColumns)
	if err != nil {
		return err
	}
	prepared := make([]preparedWatermarkMutation, 0, len(records))
	for _, record := range records {
		mutation, err := prepareWatermarkMutation(schema, record, policy)
		if err != nil {
			return err
		}
		mutation.canonicalKeys = make([]string, len(mutation.keyValues))
		for index, value := range mutation.keyValues {
			if value == nil {
				return fmt.Errorf("watermark key column %q is null", policy.KeyColumns[index])
			}
			if err := tx.QueryRow(ctx, "SELECT $1::"+keyTypes[index]+"::text", value).Scan(&mutation.canonicalKeys[index]); err != nil {
				return fmt.Errorf("canonicalize watermark key column %q as %s: %w", policy.KeyColumns[index], keyTypes[index], err)
			}
		}
		if err := tx.QueryRow(ctx, "SELECT $1::"+watermarkType+"::text", mutation.watermarkValue).Scan(&mutation.canonicalWatermark); err != nil {
			return fmt.Errorf("canonicalize watermark column %q as %s: %w", policy.WatermarkColumn, watermarkType, err)
		}
		prepared = append(prepared, mutation)
	}
	if err := ensureWatermarkStateTable(ctx, tx); err != nil {
		return err
	}
	targetSchema, targetTable := d.targetParts(schema, schema.Name)
	for _, mutation := range prepared {
		accept, err := advanceWatermarkState(ctx, tx, watermarkStateIdentity{
			flowID: flowID, targetSchema: targetSchema, targetTable: targetTable,
			projectionFingerprint: policy.ProjectionFingerprint, keyColumns: policy.KeyColumns, keyValues: mutation.canonicalKeys,
		}, watermarkType, mutation.canonicalWatermark, mutation.sourcePosition, mutation.contentHash, mutation.deleted)
		if err != nil {
			return err
		}
		if !accept {
			continue
		}
		if mutation.deleted {
			if err := d.deleteRows(ctx, tx, target, schema, []connector.Record{mutation.record}); err != nil {
				return err
			}
		} else if err := d.upsertRows(ctx, tx, target, schema, []connector.Record{mutation.record}, policy.KeyColumns); err != nil {
			return err
		}
	}
	return nil
}

func postgresWatermarkType(schema connector.Schema, columnName string) (string, error) {
	for _, column := range schema.Columns {
		if column.Name != columnName {
			continue
		}
		if column.Nullable {
			return "", fmt.Errorf("watermark column %q must be non-nullable", columnName)
		}
		if column.TypeMetadata["replica_identity"] != "true" {
			return "", fmt.Errorf("watermark column %q is not present in PostgreSQL replica identity/full old images", columnName)
		}
		cast, ok := postgresCanonicalType(column.Type, true)
		if !ok {
			return "", fmt.Errorf("watermark column %q type %q is not an admitted orderable PostgreSQL type", columnName, column.Type)
		}
		return cast, nil
	}
	return "", fmt.Errorf("watermark column %q is absent from projected schema", columnName)
}

func postgresKeyTypes(schema connector.Schema, keyColumns []string) ([]string, error) {
	byName := make(map[string]connector.Column, len(schema.Columns))
	for _, column := range schema.Columns {
		byName[column.Name] = column
	}
	types := make([]string, len(keyColumns))
	for index, name := range keyColumns {
		column, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("watermark key column %q is absent from projected schema", name)
		}
		cast, ok := postgresCanonicalType(column.Type, false)
		if !ok {
			return nil, fmt.Errorf("watermark key column %q type %q is not supported for canonical state identity", name, column.Type)
		}
		types[index] = cast
	}
	return types, nil
}

func postgresCanonicalType(raw string, orderable bool) (string, bool) {
	typeName := strings.ToLower(strings.TrimSpace(raw))
	if index := strings.Index(typeName, "("); index >= 0 {
		typeName = strings.TrimSpace(typeName[:index])
	}
	aliases := map[string]string{
		"int2": "smallint", "smallint": "smallint", "int4": "integer", "integer": "integer", "int": "integer",
		"int8": "bigint", "bigint": "bigint", "numeric": "numeric", "decimal": "numeric",
		"float4": "real", "real": "real", "float8": "double precision", "double precision": "double precision",
		"date": "date", "timestamp": "timestamp without time zone", "timestamp without time zone": "timestamp without time zone",
		"timestamptz": "timestamp with time zone", "timestamp with time zone": "timestamp with time zone",
		"time": "time without time zone", "time without time zone": "time without time zone", "timetz": "time with time zone", "time with time zone": "time with time zone",
		"text": "text", "varchar": "text", "character varying": "text", "char": "text", "character": "text", "uuid": "uuid",
		"boolean": "boolean", "bool": "boolean", "bytea": "bytea",
	}
	cast, ok := aliases[typeName]
	if !ok || (orderable && (cast == "boolean" || cast == "bytea")) {
		return "", false
	}
	return cast, true
}

func prepareWatermarkMutation(schema connector.Schema, record connector.Record, policy connector.TableWritePolicy) (preparedWatermarkMutation, error) {
	keyColumns, keyValues, err := keyColumnsAndValues(schema, record)
	if err != nil {
		return preparedWatermarkMutation{}, err
	}
	policyKeys := append([]string(nil), policy.KeyColumns...)
	sortedPolicyKeys := append([]string(nil), policyKeys...)
	sort.Strings(sortedPolicyKeys)
	if !reflect.DeepEqual(keyColumns, sortedPolicyKeys) {
		return preparedWatermarkMutation{}, fmt.Errorf("projected record key columns %v do not match write policy %v", keyColumns, policy.KeyColumns)
	}
	byColumn := make(map[string]any, len(keyColumns))
	for index, column := range keyColumns {
		byColumn[column] = keyValues[index]
	}
	orderedKeys := make([]any, len(policyKeys))
	for index, column := range policyKeys {
		orderedKeys[index] = byColumn[column]
	}
	image := record.After
	deleted := record.Operation == connector.OpDelete
	if deleted {
		image = record.Before
		if image == nil {
			image = record.After
		}
	}
	if record.Operation != connector.OpInsert && record.Operation != connector.OpUpdate && record.Operation != connector.OpLoad && !deleted {
		return preparedWatermarkMutation{}, fmt.Errorf("unsupported watermark mutation operation %q", record.Operation)
	}
	value, ok := image[policy.WatermarkColumn]
	if !ok || value == nil {
		return preparedWatermarkMutation{}, fmt.Errorf("watermark column %q is missing or null", policy.WatermarkColumn)
	}
	position, err := connector.CanonicalizeCheckpointPosition(strings.TrimSpace(record.SourcePosition))
	if err != nil || !strings.Contains(position, "/") {
		return preparedWatermarkMutation{}, errors.New("watermark mutation requires canonical PostgreSQL source LSN")
	}
	contentHash, err := connector.BatchContentHash(connector.Batch{Schema: schema, Records: []connector.Record{record}, WritePolicy: policy})
	if err != nil {
		return preparedWatermarkMutation{}, fmt.Errorf("hash watermark mutation: %w", err)
	}
	return preparedWatermarkMutation{record: record, keyValues: orderedKeys, watermarkValue: value, sourcePosition: position, contentHash: contentHash, deleted: deleted}, nil
}

func ensureWatermarkStateTable(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS wallaby;
CREATE TABLE IF NOT EXISTS wallaby.watermark_state (
  flow_id TEXT NOT NULL,
  target_schema TEXT NOT NULL,
  target_table TEXT NOT NULL,
  projection_fingerprint TEXT NOT NULL,
  key_columns TEXT[] NOT NULL,
  key_values TEXT[] NOT NULL,
  watermark_type TEXT NOT NULL,
  watermark_value TEXT NOT NULL,
  source_position TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  deleted BOOLEAN NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (flow_id,target_schema,target_table,projection_fingerprint,key_columns,key_values)
)`)
	if err != nil {
		return fmt.Errorf("ensure durable watermark state: %w", err)
	}
	if err := verifyExactCatalogColumns(ctx, tx, "wallaby", "watermark_state", []string{
		"flow_id|text|true|||", "target_schema|text|true|||", "target_table|text|true|||", "projection_fingerprint|text|true|||", "key_columns|text[]|true|||", "key_values|text[]|true|||", "watermark_type|text|true|||", "watermark_value|text|true|||", "source_position|text|true|||", "content_hash|text|true|||", "deleted|boolean|true|||", "updated_at|timestamp with time zone|true|||clock_timestamp()",
	}); err != nil {
		return fmt.Errorf("verify durable watermark state columns: %w", err)
	}
	if err := verifyExactConstraintsAndIndexes(ctx, tx, "wallaby", "watermark_state", []string{"watermark_state_pkey|p|false|false|true|PRIMARY KEY (flow_id, target_schema, target_table, projection_fingerprint, key_columns, key_values)"}, []exactIndexContract{{name: "watermark_state_pkey", primary: true, unique: true, columns: []string{"flow_id", "target_schema", "target_table", "projection_fingerprint", "key_columns", "key_values"}}}); err != nil {
		return fmt.Errorf("verify durable watermark state indexes/constraints: %w", err)
	}
	return nil
}

type watermarkStateIdentity struct {
	flowID, targetSchema, targetTable, projectionFingerprint string
	keyColumns, keyValues                                    []string
}

func advanceWatermarkState(ctx context.Context, tx pgx.Tx, identity watermarkStateIdentity, watermarkType, incoming, sourcePosition, contentHash string, deleted bool) (bool, error) {
	args := []any{identity.flowID, identity.targetSchema, identity.targetTable, identity.projectionFingerprint, identity.keyColumns, identity.keyValues}
	var inserted int
	err := tx.QueryRow(ctx, `INSERT INTO wallaby.watermark_state(
 flow_id,target_schema,target_table,projection_fingerprint,key_columns,key_values,watermark_type,watermark_value,source_position,content_hash,deleted)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT DO NOTHING RETURNING 1`, append(args, watermarkType, incoming, sourcePosition, contentHash, deleted)...).Scan(&inserted)
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("insert durable watermark state: %w", err)
	}
	var storedType, storedWatermark, storedPosition, storedContent string
	if err := tx.QueryRow(ctx, `SELECT watermark_type,watermark_value,source_position,content_hash FROM wallaby.watermark_state
WHERE flow_id=$1 AND target_schema=$2 AND target_table=$3 AND projection_fingerprint=$4 AND key_columns=$5 AND key_values=$6 FOR UPDATE`, args...).Scan(&storedType, &storedWatermark, &storedPosition, &storedContent); err != nil {
		return false, fmt.Errorf("lock durable watermark state: %w", err)
	}
	if storedType != watermarkType {
		return false, fmt.Errorf("durable watermark type changed from %q to %q", storedType, watermarkType)
	}
	var watermarkCmp, positionCmp int
	compareSQL := "SELECT CASE WHEN $1::" + watermarkType + " > $2::" + watermarkType + " THEN 1 WHEN $1::" + watermarkType + " = $2::" + watermarkType + " THEN 0 ELSE -1 END, CASE WHEN $3::pg_lsn > $4::pg_lsn THEN 1 WHEN $3::pg_lsn = $4::pg_lsn THEN 0 ELSE -1 END"
	if err := tx.QueryRow(ctx, compareSQL, incoming, storedWatermark, sourcePosition, storedPosition).Scan(&watermarkCmp, &positionCmp); err != nil {
		return false, fmt.Errorf("compare durable watermark state: %w", err)
	}
	if watermarkCmp < 0 || (watermarkCmp == 0 && positionCmp < 0) {
		return false, nil
	}
	if watermarkCmp == 0 && positionCmp == 0 {
		if contentHash == storedContent {
			return false, nil
		}
		return false, fmt.Errorf("%w: equal watermark/source position has different mutation content", connector.ErrDeliveryConflict)
	}
	if _, err := tx.Exec(ctx, `UPDATE wallaby.watermark_state SET watermark_value=$7,source_position=$8,content_hash=$9,deleted=$10,updated_at=clock_timestamp()
WHERE flow_id=$1 AND target_schema=$2 AND target_table=$3 AND projection_fingerprint=$4 AND key_columns=$5 AND key_values=$6`, append(args, incoming, sourcePosition, contentHash, deleted)...); err != nil {
		return false, fmt.Errorf("advance durable watermark state: %w", err)
	}
	return true, nil
}

func (d *Destination) updateRows(ctx context.Context, tx pgx.Tx, target string, schema connector.Schema, records []connector.Record) error {
	if len(records) == 0 {
		return nil
	}
	colTypes := columnTypeMap(schema)
	groups := map[string]*updateGroup{}
	for _, record := range records {
		cols, vals, err := recordColumns(schema, record)
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
			setClause = append(setClause, fmt.Sprintf("%s = v.%s", quoteIdent(col), quoteIdent(col)))
		}
		whereClause := make([]string, 0, len(group.keyCols))
		for idx, col := range group.keyCols {
			alias := keyAliases[idx]
			if colType := columnType(localTypes, col); colType != "" {
				if postgresJSONType(colType) != "" {
					whereClause = append(whereClause, fmt.Sprintf("t.%s::text = v.%s::text", quoteIdent(col), quoteIdent(alias)))
				} else {
					whereClause = append(whereClause, fmt.Sprintf("t.%s = v.%s", quoteIdent(col), quoteIdent(alias)))
				}
			} else {
				whereClause = append(whereClause, fmt.Sprintf("t.%s::text = v.%s::text", quoteIdent(col), quoteIdent(alias)))
			}
		}
		stmt := fmt.Sprintf(
			"UPDATE %s AS t SET %s FROM (VALUES %s) AS v(%s) WHERE %s",
			target,
			strings.Join(setClause, ", "),
			valuesClause,
			quoteColumns(allCols),
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
					whereClause = append(whereClause, fmt.Sprintf("t.%s::text = v.%s::text", quoteIdent(col), quoteIdent(col)))
				} else {
					whereClause = append(whereClause, fmt.Sprintf("t.%s = v.%s", quoteIdent(col), quoteIdent(col)))
				}
			} else {
				whereClause = append(whereClause, fmt.Sprintf("t.%s::text = v.%s::text", quoteIdent(col), quoteIdent(col)))
			}
		}
		stmt := fmt.Sprintf(
			"DELETE FROM %s AS t USING (VALUES %s) AS v(%s) WHERE %s",
			target,
			valuesClause,
			quoteColumns(group.keyCols),
			strings.Join(whereClause, " AND "),
		)
		if _, err := tx.Exec(ctx, stmt, args...); err != nil {
			return fmt.Errorf("delete rows: %w", err)
		}
	}
	return nil
}

func (d *Destination) resolveTarget(schema connector.Schema, record connector.Record) (postgresTarget, bool, error) {
	if record.Operation == connector.OpLoad && d.batchMode == batchModeStaging {
		target, err := d.stagingTarget(schema, record)
		return target, true, err
	}
	target, err := d.targetRelation(schema, record)
	return target, false, err
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
	target, err := d.targetRelation(schema, record)
	if err != nil {
		return ""
	}
	return target.sql
}

func (d *Destination) targetRelation(schema connector.Schema, record connector.Record) (postgresTarget, error) {
	targetSchema, table := d.targetParts(schema, record.Table)
	return newPostgresTarget(targetSchema, table)
}

func (d *Destination) stagingTable(schema connector.Schema, record connector.Record) string {
	target, err := d.stagingTarget(schema, record)
	if err != nil {
		return ""
	}
	return target.sql
}

func (d *Destination) stagingTarget(schema connector.Schema, record connector.Record) (postgresTarget, error) {
	stagingTable := d.stagingTableName
	stagingSchema := d.stagingSchema
	targetSchema, table := d.targetParts(schema, record.Table)
	if stagingTable == "" {
		stagingTable = table + d.stagingSuffix
	}
	if stagingSchema == "" {
		stagingSchema = targetSchema
	}
	return newPostgresTarget(stagingSchema, stagingTable)
}

func newPostgresTarget(schema, table string) (postgresTarget, error) {
	if table == "" || strings.ContainsRune(table, '\x00') || strings.ContainsRune(schema, '\x00') {
		return postgresTarget{}, errors.New("PostgreSQL target requires an exact nonempty table identifier and NUL-free schema")
	}
	identifier := pgx.Identifier{table}
	if schema != "" {
		identifier = pgx.Identifier{schema, table}
	}
	return postgresTarget{identifier: identifier, sql: identifier.Sanitize()}, nil
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
	colList := quoteColumns(cols)
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
	targetSchema := schema.Namespace
	targetTable := table
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

func (d *Destination) ensureMetaTable(ctx context.Context) error {
	if d.metaSchema == "" || d.metaTable == "" {
		return errors.New("meta schema and table are required")
	}
	schemaIdent := quoteIdent(d.metaSchema)
	if _, err := d.pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaIdent)); err != nil {
		return fmt.Errorf("create meta schema: %w", err)
	}
	tableIdent := schemaIdent + "." + quoteIdent(d.metaTable)
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

func (d *Destination) upsertMetadataBatch(ctx context.Context, tx pgx.Tx, schema connector.Schema, records []connector.Record, checkpoint connector.Checkpoint) error {
	type metadataRow struct {
		record connector.Record
		key    map[string]any
	}
	rows := make([]metadataRow, 0, len(records))
	keyColumns := map[string]struct{}{}
	for _, record := range records {
		if record.Operation == connector.OpDDL {
			continue
		}
		key, err := decodeKey(record.Key)
		if err != nil {
			return err
		}
		for column := range key {
			keyColumns[column] = struct{}{}
		}
		rows = append(rows, metadataRow{record: record, key: key})
	}
	if len(rows) == 0 {
		return nil
	}
	keys := make([]string, 0, len(keyColumns))
	for column := range keyColumns {
		keys = append(keys, column)
	}
	sort.Strings(keys)
	pkColumns := make([]string, len(keys))
	for index, column := range keys {
		pkColumns[index] = d.metaPKPrefix + column
		if err := d.ensureMetaColumn(ctx, tx, pkColumns[index]); err != nil {
			return err
		}
	}
	columns := make([]string, 0, 8+len(pkColumns))
	columns = append(columns, "flow_id", "source_schema", "source_table", "synced_at", "is_deleted", "lsn", "operation", "key_json")
	columns = append(columns, pkColumns...)
	const maxMetadataRowsPerInsert = 500
	target := quoteIdent(d.metaSchema) + "." + quoteIdent(d.metaTable)
	fallbackTimestamp := time.Now().UTC()
	for start := 0; start < len(rows); start += maxMetadataRowsPerInsert {
		end := min(start+maxMetadataRowsPerInsert, len(rows))
		values := make([]any, 0, (end-start)*len(columns))
		var statement strings.Builder
		_, _ = fmt.Fprintf(&statement, "INSERT INTO %s (%s) VALUES ", target, quoteColumns(columns))
		for rowIndex, row := range rows[start:end] {
			if rowIndex > 0 {
				statement.WriteByte(',')
			}
			statement.WriteByte('(')
			statement.WriteString(placeholders(len(values)+1, len(columns)))
			statement.WriteByte(')')
			syncedAt := row.record.Timestamp
			if syncedAt.IsZero() {
				syncedAt = fallbackTimestamp
			}
			keyJSON := string(row.record.Key)
			if keyJSON == "" {
				raw, _ := json.Marshal(row.key)
				keyJSON = string(raw)
			}
			values = append(values, d.flowID, schema.Namespace, row.record.Table, syncedAt, row.record.Operation == connector.OpDelete, checkpoint.LSN, string(row.record.Operation), keyJSON)
			for _, key := range keys {
				values = append(values, row.key[key])
			}
		}
		if _, err := tx.Exec(ctx, statement.String(), values...); err != nil {
			return fmt.Errorf("insert %d metadata rows: %w", end-start, err)
		}
	}
	return nil
}

func (d *Destination) ensureMetaColumn(ctx context.Context, tx pgx.Tx, column string) error {
	if column == "" {
		return nil
	}
	key := column
	if _, ok := d.metaColumns[key]; ok {
		return nil
	}
	target := quoteIdent(d.metaSchema) + "." + quoteIdent(d.metaTable)
	stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s TEXT", target, quoteIdent(column))
	if _, err := tx.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("add meta column: %w", err)
	}
	d.metaColumns[key] = struct{}{}
	return nil
}

func recordColumns(schema connector.Schema, record connector.Record) ([]string, []any, error) {
	if record.After == nil {
		return []string{}, []any{}, nil
	}
	cols := make([]string, 0, len(schema.Columns))
	vals := make([]any, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		if col.Generated {
			continue
		}
		val, ok := record.After[col.Name]
		if !ok {
			continue
		}
		normalized, err := normalizePostgresValue(col.Type, val)
		if err != nil {
			return nil, nil, err
		}
		cols = append(cols, col.Name)
		vals = append(vals, normalized)
	}
	return cols, vals, nil
}

func normalizePostgresValue(colType string, value any) (any, error) {
	if value == nil {
		return nil, nil //nolint:nilnil // nil maps to NULL
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
		return []string{}, []any{}, nil
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
		return "", []any{}, nil
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
			if coerced < math.MinInt16 || coerced > math.MaxInt16 {
				return nil, false, fmt.Errorf("int16 overflow: %d", coerced)
			}
			// #nosec G115 -- bounds checked above.
			return int16(coerced), true, nil
		case "int4", "integer", "int", "serial":
			if coerced < math.MinInt32 || coerced > math.MaxInt32 {
				return nil, false, fmt.Errorf("int32 overflow: %d", coerced)
			}
			// #nosec G115 -- bounds checked above.
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

// partitionUpsertRecords preserves every source image while preventing a single
// INSERT ... ON CONFLICT statement from affecting the same target row twice.
// Repeated keys start a later statement instead of replacing an earlier partial
// image: pgoutput may omit unchanged external TOAST columns from the later image.
func partitionUpsertRecords(records []connector.Record, schema connector.Schema) ([][]connector.Record, error) {
	if len(records) == 0 {
		return nil, nil
	}
	colTypes := columnTypeMap(schema)
	batches := make([][]connector.Record, 0, 1)
	current := make([]connector.Record, 0, len(records))
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		key, err := upsertDedupKey(record, colTypes)
		if err != nil {
			return nil, err
		}
		if key == "" {
			return [][]connector.Record{records}, nil
		}
		if _, duplicate := seen[key]; duplicate {
			batches = append(batches, current)
			current = make([]connector.Record, 0, len(records))
			clear(seen)
		}
		seen[key] = struct{}{}
		current = append(current, record)
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches, nil
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

func (d *Destination) copyRows(ctx context.Context, tx pgx.Tx, target pgx.Identifier, cols []string, rows [][]any) error {
	return d.copyRowsInto(ctx, tx, target, cols, rows)
}

func (d *Destination) copyRowsInto(ctx context.Context, tx pgx.Tx, ident pgx.Identifier, cols []string, rows [][]any) error {
	if len(cols) == 0 || len(rows) == 0 {
		return nil
	}
	const savepoint = "wallaby_copy_fallback"
	if _, err := tx.Exec(ctx, "SAVEPOINT "+savepoint); err != nil {
		return fmt.Errorf("create copy fallback savepoint: %w", err)
	}
	// Mark only the COPY substatement. Besides making server-side diagnostics
	// unambiguous, rolling back this savepoint necessarily clears the local GUC
	// before the ordinary INSERT fallback runs.
	if _, err := tx.Exec(ctx, "SET LOCAL wallaby.copy_from_active = 'on'"); err != nil {
		_, _ = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+savepoint)
		_, _ = tx.Exec(ctx, "RELEASE SAVEPOINT "+savepoint)
		return fmt.Errorf("mark copy substatement: %w", err)
	}
	_, copyErr := tx.CopyFrom(ctx, ident, cols, pgx.CopyFromRows(rows))
	if copyErr == nil {
		if _, err := tx.Exec(ctx, "SET LOCAL wallaby.copy_from_active = 'off'"); err != nil {
			_, _ = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+savepoint)
			_, _ = tx.Exec(ctx, "RELEASE SAVEPOINT "+savepoint)
			return fmt.Errorf("clear copy substatement marker: %w", err)
		}
		if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT "+savepoint); err != nil {
			return fmt.Errorf("release copy fallback savepoint: %w", err)
		}
		return nil
	}
	if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); err != nil {
		return fmt.Errorf("copy from and rollback to fallback savepoint failed: %w", errors.Join(copyErr, err))
	}
	if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT "+savepoint); err != nil {
		return fmt.Errorf("copy from and release fallback savepoint failed: %w", errors.Join(copyErr, err))
	}
	return fmt.Errorf("copy from: %w", copyErr)
}

func columnType(colTypes map[string]string, col string) string {
	if len(colTypes) == 0 {
		return ""
	}
	return colTypes[col]
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
		if col.Generated {
			continue
		}
		cols = append(cols, col.Name)
	}
	return cols
}

func decodeKey(raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var out map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("decode record key: %w", err)
	}
	return out, nil
}

func quoteColumns(cols []string) string {
	quoted := make([]string, 0, len(cols))
	for _, col := range cols {
		quoted = append(quoted, quoteIdent(col))
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

func quoteIdent(value string) string {
	if value == "" {
		return value
	}
	const quote = `"`
	escaped := strings.ReplaceAll(value, quote, quote+quote)
	return quote + escaped + quote
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

func parseInt(value string, fallback int) int {
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

// ManagedPostgresMajor reports the live destination major admitted during Open.
func (d *Destination) ManagedPostgresMajor() int {
	return d.managedPostgresMajor
}

func validateManagedPostgresServerVersion(ctx context.Context, pool *pgxpool.Pool, profileName string) (int, error) {
	profileName = strings.TrimSpace(profileName)
	if profileName == "" {
		return 0, nil
	}
	if profileName != connector.ManagedProfilePostgresToPostgresV1 {
		return 0, fmt.Errorf("unsupported PostgreSQL managed profile %q", profileName)
	}
	var raw string
	if err := pool.QueryRow(ctx, "SHOW server_version_num").Scan(&raw); err != nil {
		return 0, fmt.Errorf("read PostgreSQL server version for managed profile: %w", err)
	}
	versionNumber, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parse PostgreSQL server_version_num %q: %w", raw, err)
	}
	major := versionNumber / 10000
	profile := connector.PostgresToPostgresV1Profile()
	if !profile.SupportsPostgresVersion(major) {
		return 0, fmt.Errorf("managed profile %s does not admit PostgreSQL %d", profileName, major)
	}
	return major, nil
}

func normalizeSyncCommit(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	switch value {
	case "", "on", "off", "local", "remote_write", "remote_apply":
		return value
	default:
		return ""
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
	// PostgreSQL identifiers cannot contain NUL. Preserve exact identifier
	// bytes and avoid collisions between quoted identifiers containing dots.
	return schema.Namespace + "\x00" + table
}
