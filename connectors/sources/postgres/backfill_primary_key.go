package postgres

import (
	"context"
	"fmt"
)

func (b *BackfillSource) loadPrimaryKeyColumns(ctx context.Context, schema, table string) ([]string, error) {
	rows, err := b.pool.Query(ctx,
		`SELECT a.attname
		 FROM pg_catalog.pg_index AS i
		 JOIN pg_catalog.pg_class AS c ON c.oid = i.indrelid
		 JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		 JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS key(attnum, position) ON true
		 JOIN pg_catalog.pg_attribute AS a
		   ON a.attrelid = c.oid AND a.attnum = key.attnum
		 WHERE n.nspname = $1
		   AND c.relname = $2
		   AND i.indisprimary
		   AND key.position <= i.indnkeyatts
		 ORDER BY key.position`,
		schema, table,
	)
	if err != nil {
		return nil, fmt.Errorf("load primary key for %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	columns := make([]string, 0)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("scan primary key for %s.%s: %w", schema, table, err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate primary key for %s.%s: %w", schema, table, err)
	}
	return columns, nil
}
