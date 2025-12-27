package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/ductstream/pkg/connector"
)

const (
	optTables  = "tables"
	optSchemas = "schemas"
)

// BackfillSource reads existing table data in bulk.
type BackfillSource struct {
	spec          connector.Spec
	pool          *pgxpool.Pool
	tables        []string
	batchSize     int
	wireFormat    connector.WireFormat
	tableIndex    int
	rows          pgx.Rows
	currentTable  string
	currentSchema connector.Schema
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

	return nil
}

func (b *BackfillSource) Read(ctx context.Context) (connector.Batch, error) {
	if b.pool == nil {
		return connector.Batch{}, errors.New("backfill source not initialized")
	}

	records := make([]connector.Record, 0, b.batchSize)
	var checkpoint connector.Checkpoint

	for len(records) < b.batchSize {
		if b.rows == nil {
			if b.tableIndex >= len(b.tables) {
				if len(records) == 0 {
					return connector.Batch{}, io.EOF
				}
				break
			}

			fullName := b.tables[b.tableIndex]
			namespace, table, err := splitTable(fullName)
			if err != nil {
				return connector.Batch{}, err
			}
			schema, err := b.loadSchema(ctx, namespace, table)
			if err != nil {
				return connector.Batch{}, err
			}

			rows, err := b.queryTable(ctx, namespace, table)
			if err != nil {
				return connector.Batch{}, err
			}

			b.currentTable = fullName
			b.currentSchema = schema
			b.rows = rows
		}

		if !b.rows.Next() {
			b.rows.Close()
			b.rows = nil
			b.tableIndex++
			if len(records) == 0 {
				continue
			}
			break
		}

		values, err := b.rows.Values()
		if err != nil {
			return connector.Batch{}, fmt.Errorf("read row values: %w", err)
		}

		fields := b.rows.FieldDescriptions()
		row := make(map[string]any, len(fields))
		for idx, field := range fields {
			row[string(field.Name)] = values[idx]
		}

		records = append(records, connector.Record{
			Table:         b.currentSchema.Name,
			Operation:     connector.OpLoad,
			SchemaVersion: b.currentSchema.Version,
			After:         row,
			Timestamp:     time.Now().UTC(),
		})

		checkpoint = connector.Checkpoint{
			Timestamp: time.Now().UTC(),
			Metadata: map[string]string{
				"mode":  "backfill",
				"table": b.currentTable,
			},
		}
	}

	return connector.Batch{
		Records:    records,
		Schema:     b.currentSchema,
		Checkpoint: checkpoint,
		WireFormat: b.wireFormat,
	}, nil
}

func (b *BackfillSource) Ack(_ context.Context, _ connector.Checkpoint) error {
	return nil
}

func (b *BackfillSource) Close(_ context.Context) error {
	if b.rows != nil {
		b.rows.Close()
		b.rows = nil
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

func (b *BackfillSource) queryTable(ctx context.Context, schema, table string) (pgx.Rows, error) {
	identifier := pgx.Identifier{schema, table}.Sanitize()
	query := fmt.Sprintf("SELECT * FROM %s", identifier)
	rows, err := b.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query table %s: %w", identifier, err)
	}
	return rows, nil
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
