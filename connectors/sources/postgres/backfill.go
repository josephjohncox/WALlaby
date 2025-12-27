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
	"github.com/josephjohncox/ductstream/pkg/connector"
)

const (
	optTables          = "tables"
	optSchemas         = "schemas"
	optSnapshotWorkers = "snapshot_workers"
	optParallelTables  = "parallel_tables"
	optPartitionColumn = "partition_column"
	optPartitionCount  = "partition_count"
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
}

func (b *BackfillSource) Open(ctx context.Context, spec connector.Spec) error {
	b.spec = spec

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

func (b *BackfillSource) Ack(_ context.Context, _ connector.Checkpoint) error {
	return nil
}

func (b *BackfillSource) Close(_ context.Context) error {
	if b.cancel != nil {
		b.cancel()
		b.wg.Wait()
	}
	if b.pool != nil {
		b.pool.Close()
		b.pool = nil
	}
	return nil
}

func (b *BackfillSource) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		SupportsDDL:           false,
		SupportsSchemaChanges: false,
		SupportsStreaming:     false,
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
		`SELECT column_name, is_nullable, data_type, is_generated, generation_expression
		 FROM information_schema.columns
		 WHERE table_schema = $1 AND table_name = $2
		 ORDER BY ordinal_position`, schema, table)
	if err != nil {
		return connector.Schema{}, fmt.Errorf("load schema: %w", err)
	}
	defer rows.Close()

	columns := make([]connector.Column, 0)
	for rows.Next() {
		var column, nullable, dataType, isGenerated string
		var expression *string
		if err := rows.Scan(&column, &nullable, &dataType, &isGenerated, &expression); err != nil {
			return connector.Schema{}, fmt.Errorf("scan schema row: %w", err)
		}

		col := connector.Column{
			Name:      column,
			Type:      strings.ToLower(dataType),
			Nullable:  nullable == "YES",
			Generated: isGenerated == "ALWAYS",
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
				tasks = append(tasks, backfillTask{
					table:          table,
					partitionIndex: idx,
					partitionCount: b.partitionCount,
				})
			}
		} else {
			tasks = append(tasks, backfillTask{table: table})
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

	rows, err := b.queryTablePartition(ctx, namespace, table, task)
	if err != nil {
		return err
	}
	defer rows.Close()

	records := make([]connector.Record, 0, b.batchSize)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("read row values: %w", err)
		}

		fields := rows.FieldDescriptions()
		row := make(map[string]any, len(fields))
		for idx, field := range fields {
			row[string(field.Name)] = values[idx]
		}

		records = append(records, connector.Record{
			Table:         schema.Name,
			Operation:     connector.OpLoad,
			SchemaVersion: schema.Version,
			After:         row,
			Timestamp:     time.Now().UTC(),
		})

		if len(records) >= b.batchSize {
			if err := b.emitBatch(ctx, schema, task, records); err != nil {
				return err
			}
			records = make([]connector.Record, 0, b.batchSize)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate rows: %w", err)
	}
	if len(records) > 0 {
		if err := b.emitBatch(ctx, schema, task, records); err != nil {
			return err
		}
	}

	return nil
}

func (b *BackfillSource) emitBatch(ctx context.Context, schema connector.Schema, task backfillTask, records []connector.Record) error {
	checkpoint := connector.Checkpoint{
		Timestamp: time.Now().UTC(),
		Metadata: map[string]string{
			"mode":  "backfill",
			"table": task.table,
		},
	}
	if task.partitionCount > 1 {
		checkpoint.Metadata["partition"] = fmt.Sprintf("%d/%d", task.partitionIndex, task.partitionCount)
	}

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

func (b *BackfillSource) queryTablePartition(ctx context.Context, schema, table string, task backfillTask) (pgx.Rows, error) {
	identifier := pgx.Identifier{schema, table}.Sanitize()
	query := fmt.Sprintf("SELECT * FROM %s", identifier)
	var args []any
	if b.partitionCol != "" && task.partitionCount > 1 {
		columnIdent := pgx.Identifier{b.partitionCol}.Sanitize()
		query = fmt.Sprintf("%s WHERE mod(hashtext(%s::text), $1) = $2", query, columnIdent)
		args = []any{task.partitionCount, task.partitionIndex}
	}
	rows, err := b.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query table %s: %w", identifier, err)
	}
	return rows, nil
}

func schemaHasColumn(schema connector.Schema, name string) bool {
	for _, col := range schema.Columns {
		if col.Name == name {
			return true
		}
	}
	return false
}
