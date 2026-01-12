package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

const (
	optTables               = "tables"
	optSchemas              = "schemas"
	optSnapshotWorkers      = "snapshot_workers"
	optParallelTables       = "parallel_tables"
	optPartitionColumn      = "partition_column"
	optPartitionCount       = "partition_count"
	optSnapshotConsistent   = "snapshot_consistent"
	optSnapshotStateBackend = "snapshot_state_backend"
	optSnapshotStateSchema  = "snapshot_state_schema"
	optSnapshotStateTable   = "snapshot_state_table"
	optSnapshotStatePath    = "snapshot_state_path"
	optSnapshotStateDSN     = "snapshot_state_dsn"
)

// BackfillSource reads existing table data in bulk.
type BackfillSource struct {
	spec           connector.Spec
	pool           *pgxpool.Pool
	tables         []string
	batchSize      int
	wireFormat     connector.WireFormat
	workers        int
	partitionCol   string
	partitionCount int
	results        chan batchResult
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	snapshotName   string
	snapshotConn   *pgxpool.Conn
	snapshotTx     pgx.Tx
	stateStore     snapshotStateStore
	flowID         string
	taskStates     map[snapshotTaskKey]snapshotTaskState
}

func (b *BackfillSource) Open(ctx context.Context, spec connector.Spec) error {
	b.spec = spec

	dsn := spec.Options[optDSN]
	if dsn == "" {
		return errors.New("postgres dsn is required")
	}

	pool, err := newPool(ctx, dsn, spec.Options)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("ping postgres: %w", err)
	}
	b.pool = pool

	b.batchSize = parseInt(spec.Options[optBatchSize], 1000)
	b.wireFormat = connector.WireFormat(spec.Options[optFormat])
	if b.wireFormat == "" {
		b.wireFormat = connector.WireFormatArrow
	}
	b.workers = parseInt(spec.Options[optSnapshotWorkers], 1)
	if b.workers <= 0 {
		b.workers = 1
	}
	if b.workers == 1 {
		if alt := parseInt(spec.Options[optParallelTables], 0); alt > 0 {
			b.workers = alt
		}
	}
	b.partitionCol = strings.TrimSpace(spec.Options[optPartitionColumn])
	if strings.Contains(b.partitionCol, ".") {
		return errors.New("partition_column must be a bare column name")
	}
	b.partitionCount = parseInt(spec.Options[optPartitionCount], 1)
	if b.partitionCount < 1 {
		b.partitionCount = 1
	}

	b.flowID = snapshotFlowID(spec)
	if backend := strings.ToLower(spec.Options[optSnapshotStateBackend]); backend != "none" {
		store, err := newSnapshotStateStore(ctx, backend, spec, dsn)
		if err != nil {
			return err
		}
		b.stateStore = store
		if b.stateStore != nil {
			states, err := b.stateStore.Load(ctx, b.flowID)
			if err != nil {
				return err
			}
			b.taskStates = states
		}
	}

	if parseBool(spec.Options[optSnapshotConsistent], true) {
		if err := b.beginSnapshot(ctx); err != nil {
			return err
		}
	}

	tables := parseCSV(spec.Options[optTables])
	if len(tables) == 0 {
		schemas := parseCSV(spec.Options[optSchemas])
		if len(schemas) == 0 {
			schemas = []string{"public"}
		}
		found, err := b.loadTables(ctx, schemas)
		if err != nil {
			return err
		}
		tables = found
	}
	if len(tables) == 0 {
		return errors.New("no tables configured for backfill")
	}
	b.tables = tables
	b.startWorkers(ctx)
	return nil
}

func (b *BackfillSource) Read(ctx context.Context) (connector.Batch, error) {
	if b.results == nil {
		return connector.Batch{}, errors.New("backfill source not initialized")
	}

	select {
	case <-ctx.Done():
		return connector.Batch{}, ctx.Err()
	case result, ok := <-b.results:
		if !ok {
			return connector.Batch{}, io.EOF
		}
		if result.err != nil {
			return connector.Batch{}, result.err
		}
		return result.batch, nil
	}
}

func (b *BackfillSource) Ack(ctx context.Context, checkpoint connector.Checkpoint) error {
	if b.stateStore == nil || b.flowID == "" {
		return nil
	}
	meta := checkpoint.Metadata
	if meta == nil || meta["mode"] != "backfill" {
		return nil
	}
	table := meta["table"]
	if table == "" {
		return nil
	}
	partitionIndex := parseInt(meta["partition_index"], 0)
	partitionCount := parseInt(meta["partition_count"], 1)
	cursor := meta["cursor"]
	status := snapshotStatusRunning
	if parseBool(meta["done"], false) {
		status = snapshotStatusDone
	}
	return b.stateStore.Upsert(ctx, snapshotTaskState{
		FlowID:         b.flowID,
		Table:          table,
		PartitionIndex: partitionIndex,
		PartitionCount: maxPartitionCount(partitionCount),
		Cursor:         cursor,
		Status:         status,
	})
}

func (b *BackfillSource) Close(_ context.Context) error {
	if b.cancel != nil {
		b.cancel()
		b.wg.Wait()
	}
	b.endSnapshot(context.Background())
	if b.pool != nil {
		b.pool.Close()
		b.pool = nil
	}
	if b.stateStore != nil {
		b.stateStore.Close()
		b.stateStore = nil
	}
	return nil
}

func (b *BackfillSource) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		SupportsDDL:           false,
		SupportsSchemaChanges: false,
		SupportsStreaming:     false,
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

func (b *BackfillSource) loadTables(ctx context.Context, schemas []string) ([]string, error) {
	rows, err := b.pool.Query(ctx,
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

func (b *BackfillSource) loadSchema(ctx context.Context, schema, table string) (connector.Schema, error) {
	rows, err := b.pool.Query(ctx,
		`SELECT a.attname,
		        NOT a.attnotnull AS is_nullable,
		        format_type(a.atttypid, a.atttypmod) AS data_type,
		        a.attgenerated::text AS generated,
		        pg_get_expr(ad.adbin, ad.adrelid) AS generation_expression,
		        tns.nspname AS type_schema,
		        ext.extname AS extension
		 FROM pg_class c
		 JOIN pg_namespace ns ON ns.oid = c.relnamespace
		 JOIN pg_attribute a ON a.attrelid = c.oid
		 JOIN pg_type t ON t.oid = a.atttypid
		 JOIN pg_namespace tns ON tns.oid = t.typnamespace
		 LEFT JOIN pg_attrdef ad ON ad.adrelid = c.oid AND ad.adnum = a.attnum
		 LEFT JOIN pg_depend dep ON dep.classid = 'pg_type'::regclass
		   AND dep.objid = t.oid AND dep.deptype = 'e'
		 LEFT JOIN pg_extension ext ON ext.oid = dep.refobjid
		 WHERE ns.nspname = $1
		   AND c.relname = $2
		   AND a.attnum > 0
		   AND NOT a.attisdropped
		 ORDER BY a.attnum`, schema, table)
	if err != nil {
		return connector.Schema{}, fmt.Errorf("load schema: %w", err)
	}
	defer rows.Close()

	columns := make([]connector.Column, 0)
	for rows.Next() {
		var column, dataType string
		var generated any
		var nullable bool
		var expression *string
		var typeSchema string
		var extension *string
		if err := rows.Scan(&column, &nullable, &dataType, &generated, &expression, &typeSchema, &extension); err != nil {
			return connector.Schema{}, fmt.Errorf("scan schema row: %w", err)
		}

		generatedText := normalizeGeneratedValue(generated)
		col := connector.Column{
			Name:      column,
			Type:      formatTypeName(typeSchema, dataType),
			Nullable:  nullable,
			Generated: generatedText != "",
		}
		if extension != nil && *extension != "" {
			col.TypeMetadata = map[string]string{
				"extension": strings.ToLower(*extension),
			}
		}
		if expression != nil {
			col.Expression = *expression
		}
		columns = append(columns, col)
	}
	if err := rows.Err(); err != nil {
		return connector.Schema{}, fmt.Errorf("iterate schema: %w", err)
	}

	return connector.Schema{
		Name:      table,
		Namespace: schema,
		Version:   1,
		Columns:   columns,
	}, nil
}

func normalizeGeneratedValue(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(v)
	case []byte:
		return strings.TrimSpace(string(v))
	case byte:
		return strings.TrimSpace(string([]byte{v}))
	case rune:
		return strings.TrimSpace(string(v))
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func splitTable(value string) (string, string, error) {
	parts := strings.Split(value, ".")
	if len(parts) == 1 {
		return "public", parts[0], nil
	}
	if len(parts) == 2 {
		return parts[0], parts[1], nil
	}
	return "", "", fmt.Errorf("invalid table name %q", value)
}

type backfillTask struct {
	table          string
	partitionIndex int
	partitionCount int
	cursor         string
}

type batchResult struct {
	batch connector.Batch
	err   error
}

func (b *BackfillSource) startWorkers(ctx context.Context) {
	tasks := make([]backfillTask, 0)
	for _, table := range b.tables {
		if b.partitionCol != "" && b.partitionCount > 1 {
			for idx := 0; idx < b.partitionCount; idx++ {
				task := backfillTask{
					table:          table,
					partitionIndex: idx,
					partitionCount: b.partitionCount,
				}
				if state := b.stateForTask(task); state.Status == snapshotStatusDone {
					continue
				} else if state.Cursor != "" {
					task.cursor = state.Cursor
				}
				tasks = append(tasks, task)
			}
		} else {
			task := backfillTask{table: table}
			if state := b.stateForTask(task); state.Status == snapshotStatusDone {
				continue
			} else if state.Cursor != "" {
				task.cursor = state.Cursor
			}
			tasks = append(tasks, task)
		}
	}

	if len(tasks) == 0 {
		return
	}

	if b.workers > len(tasks) {
		b.workers = len(tasks)
	}

	taskCh := make(chan backfillTask, len(tasks))
	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)

	b.results = make(chan batchResult, b.workers*2)
	workerCtx, cancel := context.WithCancel(ctx)
	b.cancel = cancel

	b.wg.Add(b.workers)
	for i := 0; i < b.workers; i++ {
		go func() {
			defer b.wg.Done()
			for task := range taskCh {
				if workerCtx.Err() != nil {
					return
				}
				if err := b.runTask(workerCtx, task); err != nil {
					select {
					case b.results <- batchResult{err: err}:
					default:
					}
					b.cancel()
					return
				}
			}
		}()
	}

	go func() {
		b.wg.Wait()
		close(b.results)
	}()
}

func (b *BackfillSource) runTask(ctx context.Context, task backfillTask) error {
	namespace, table, err := splitTable(task.table)
	if err != nil {
		return err
	}

	schema, err := b.loadSchema(ctx, namespace, table)
	if err != nil {
		return err
	}
	if b.partitionCol != "" && b.partitionCount > 1 {
		if !schemaHasColumn(schema, b.partitionCol) {
			return fmt.Errorf("partition column %s not found on %s", b.partitionCol, task.table)
		}
	}
	if task.cursor != "" && b.partitionCol == "" {
		return fmt.Errorf("snapshot cursor requires partition_column for %s", task.table)
	}

	if b.stateStore != nil {
		_ = b.stateStore.Upsert(ctx, snapshotTaskState{
			FlowID:         b.flowID,
			Table:          task.table,
			PartitionIndex: task.partitionIndex,
			PartitionCount: maxPartitionCount(task.partitionCount),
			Cursor:         task.cursor,
			Status:         snapshotStatusRunning,
		})
	}

	rows, cleanup, err := b.queryTablePartition(ctx, namespace, table, task)
	if err != nil {
		return err
	}
	defer cleanup()
	defer rows.Close()

	records := make([]connector.Record, 0, b.batchSize)
	lastCursor := task.cursor
	produced := false
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("read row values: %w", err)
		}

		fields := rows.FieldDescriptions()
		row := make(map[string]any, len(fields))
		for idx, field := range fields {
			row[field.Name] = values[idx]
		}
		if err := connector.NormalizePostgresRecord(schema, row); err != nil {
			return err
		}
		if b.partitionCol != "" {
			if value, ok := row[b.partitionCol]; ok && value != nil {
				lastCursor = fmt.Sprint(value)
			}
		}

		records = append(records, connector.Record{
			Table:         schema.Name,
			Operation:     connector.OpLoad,
			SchemaVersion: schema.Version,
			After:         row,
			Timestamp:     time.Now().UTC(),
		})

		if len(records) >= b.batchSize {
			if err := b.emitBatch(ctx, schema, task, records, lastCursor, false); err != nil {
				return err
			}
			records = make([]connector.Record, 0, b.batchSize)
			produced = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate rows: %w", err)
	}
	if len(records) > 0 {
		if err := b.emitBatch(ctx, schema, task, records, lastCursor, true); err != nil {
			return err
		}
		produced = true
	}
	if err := b.emitControlDone(ctx, schema, task, lastCursor, produced); err != nil {
		return err
	}

	return nil
}

func (b *BackfillSource) emitBatch(ctx context.Context, schema connector.Schema, task backfillTask, records []connector.Record, cursor string, done bool) error {
	checkpoint := snapshotCheckpoint(task, cursor, done)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.results <- batchResult{batch: connector.Batch{
		Records:    records,
		Schema:     schema,
		Checkpoint: checkpoint,
		WireFormat: b.wireFormat,
	}}:
		return nil
	}
}

func (b *BackfillSource) emitControlDone(ctx context.Context, schema connector.Schema, task backfillTask, cursor string, produced bool) error {
	checkpoint := snapshotCheckpoint(task, cursor, true)
	if produced {
		checkpoint.Metadata["control"] = "true"
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.results <- batchResult{batch: connector.Batch{
		Records:    nil,
		Schema:     schema,
		Checkpoint: checkpoint,
		WireFormat: b.wireFormat,
	}}:
		return nil
	}
}

func (b *BackfillSource) queryTablePartition(ctx context.Context, schema, table string, task backfillTask) (pgx.Rows, func(), error) {
	identifier := pgx.Identifier{schema, table}.Sanitize()
	query := fmt.Sprintf("SELECT * FROM %s", identifier)
	var args []any
	clauses := make([]string, 0, 2)
	if b.partitionCol != "" && task.partitionCount > 1 {
		columnIdent := pgx.Identifier{b.partitionCol}.Sanitize()
		clauses = append(clauses, fmt.Sprintf("mod(hashtext(%s::text), $1) = $2", columnIdent))
		args = append(args, task.partitionCount, task.partitionIndex)
	}
	if b.partitionCol != "" && task.cursor != "" {
		columnIdent := pgx.Identifier{b.partitionCol}.Sanitize()
		clauses = append(clauses, fmt.Sprintf("%s > $%d", columnIdent, len(args)+1))
		args = append(args, task.cursor)
	}
	if len(clauses) > 0 {
		query = fmt.Sprintf("%s WHERE %s", query, strings.Join(clauses, " AND "))
	}
	if b.partitionCol != "" {
		columnIdent := pgx.Identifier{b.partitionCol}.Sanitize()
		query = fmt.Sprintf("%s ORDER BY %s", query, columnIdent)
	}

	if b.snapshotName == "" {
		rows, err := b.pool.Query(ctx, query, args...)
		if err != nil {
			return nil, nil, fmt.Errorf("query table %s: %w", identifier, err)
		}
		return rows, func() {}, nil
	}

	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("acquire snapshot connection: %w", err)
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		conn.Release()
		return nil, nil, fmt.Errorf("begin snapshot transaction: %w", err)
	}
	if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT $1", b.snapshotName); err != nil {
		_ = tx.Rollback(ctx)
		conn.Release()
		return nil, nil, fmt.Errorf("set snapshot: %w", err)
	}
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		_ = tx.Rollback(ctx)
		conn.Release()
		return nil, nil, fmt.Errorf("query table %s: %w", identifier, err)
	}
	cleanup := func() {
		_ = tx.Commit(ctx)
		conn.Release()
	}
	return rows, cleanup, nil
}

func schemaHasColumn(schema connector.Schema, name string) bool {
	for _, col := range schema.Columns {
		if col.Name == name {
			return true
		}
	}
	return false
}

func (b *BackfillSource) beginSnapshot(ctx context.Context) error {
	if b.pool == nil {
		return errors.New("snapshot pool not initialized")
	}
	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire snapshot connection: %w", err)
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		conn.Release()
		return fmt.Errorf("begin snapshot transaction: %w", err)
	}
	var snapshot string
	if err := tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshot); err != nil {
		_ = tx.Rollback(ctx)
		conn.Release()
		return fmt.Errorf("export snapshot: %w", err)
	}
	b.snapshotConn = conn
	b.snapshotTx = tx
	b.snapshotName = snapshot
	return nil
}

func (b *BackfillSource) endSnapshot(ctx context.Context) {
	if b.snapshotTx != nil {
		_ = b.snapshotTx.Commit(ctx)
		b.snapshotTx = nil
	}
	if b.snapshotConn != nil {
		b.snapshotConn.Release()
		b.snapshotConn = nil
	}
	b.snapshotName = ""
}

func snapshotFlowID(spec connector.Spec) string {
	if spec.Options != nil {
		if value := spec.Options[optFlowID]; value != "" {
			return value
		}
	}
	if spec.Name != "" {
		return spec.Name
	}
	return "snapshot"
}

func snapshotCheckpoint(task backfillTask, cursor string, done bool) connector.Checkpoint {
	meta := map[string]string{
		"mode":  "backfill",
		"table": task.table,
	}
	if task.partitionCount > 1 {
		meta["partition_index"] = fmt.Sprintf("%d", task.partitionIndex)
		meta["partition_count"] = fmt.Sprintf("%d", task.partitionCount)
		meta["partition"] = fmt.Sprintf("%d/%d", task.partitionIndex, task.partitionCount)
	}
	if cursor != "" {
		meta["cursor"] = cursor
	}
	if done {
		meta["done"] = "true"
	}
	return connector.Checkpoint{
		Timestamp: time.Now().UTC(),
		Metadata:  meta,
	}
}

func (b *BackfillSource) stateForTask(task backfillTask) snapshotTaskState {
	if b.taskStates == nil {
		return snapshotTaskState{}
	}
	key := snapshotTaskKey{
		Table:          task.table,
		PartitionIndex: task.partitionIndex,
		PartitionCount: maxPartitionCount(task.partitionCount),
	}
	if state, ok := b.taskStates[key]; ok {
		return state
	}
	return snapshotTaskState{}
}

func maxPartitionCount(value int) int {
	if value <= 0 {
		return 1
	}
	return value
}
