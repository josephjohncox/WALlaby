package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// LoadTableSchema reads column definitions for a Postgres table.
func LoadTableSchema(ctx context.Context, dsn, schema, table string, options map[string]string) (connector.Schema, error) {
	pool, err := newPool(ctx, dsn, options)
	if err != nil {
		return connector.Schema{}, fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	rows, err := pool.Query(ctx,
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

// LoadPrimaryKeys returns primary key columns for a table.
func LoadPrimaryKeys(ctx context.Context, dsn, schema, table string, options map[string]string) ([]string, error) {
	pool, err := newPool(ctx, dsn, options)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	return loadPrimaryKeys(ctx, pool, schema, table)
}

func loadPrimaryKeys(ctx context.Context, pool *pgxpool.Pool, schema, table string) ([]string, error) {
	rows, err := pool.Query(ctx,
		`SELECT a.attname
		 FROM pg_index i
		 JOIN pg_class c ON c.oid = i.indrelid
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
		 WHERE i.indisprimary
		   AND n.nspname = $1
		   AND c.relname = $2
		 ORDER BY array_position(i.indkey, a.attnum)`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("load primary keys: %w", err)
	}
	defer rows.Close()

	keys := make([]string, 0)
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scan primary key: %w", err)
		}
		keys = append(keys, col)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate primary keys: %w", err)
	}
	return keys, nil
}
