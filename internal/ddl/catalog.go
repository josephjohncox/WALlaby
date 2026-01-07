package ddl

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/registry"
	"github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// CatalogScanner polls pg_catalog to discover schema changes.
type CatalogScanner struct {
	Pool        *pgxpool.Pool
	Registry    registry.Store
	Schemas     []string
	AutoApprove bool
	last        map[string]connector.Schema
}

func (c *CatalogScanner) RunOnce(ctx context.Context) error {
	if c.Pool == nil {
		return fmt.Errorf("catalog scanner requires a pool")
	}
	if c.Registry == nil {
		return nil
	}

	current, err := c.scan(ctx)
	if err != nil {
		return err
	}
	if c.last == nil {
		c.last = map[string]connector.Schema{}
	}

	for key, newSchema := range current {
		oldSchema, ok := c.last[key]
		if !ok {
			_ = c.Registry.RegisterSchema(ctx, newSchema)
			c.last[key] = newSchema
			continue
		}

		plan := schema.Diff(oldSchema, newSchema)
		if plan.HasChanges() {
			status := registry.StatusPending
			if c.AutoApprove {
				status = registry.StatusApproved
			}
			_, _ = c.Registry.RecordDDL(ctx, "", plan, "", status)
			_ = c.Registry.RegisterSchema(ctx, newSchema)
			c.last[key] = newSchema
		}
	}

	return nil
}

func (c *CatalogScanner) scan(ctx context.Context) (map[string]connector.Schema, error) {
	schemas := c.Schemas
	if len(schemas) == 0 {
		schemas = []string{"public"}
	}

	rows, err := c.Pool.Query(ctx,
		`SELECT ns.nspname AS table_schema,
		        c.relname AS table_name,
		        a.attname AS column_name,
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
		 WHERE ns.nspname = ANY($1::text[])
		   AND a.attnum > 0
		   AND NOT a.attisdropped
		   AND c.relkind IN ('r','p')
		 ORDER BY ns.nspname, c.relname, a.attnum`, schemas)
	if err != nil {
		return nil, fmt.Errorf("scan catalog: %w", err)
	}
	defer rows.Close()

	result := make(map[string]connector.Schema)
	for rows.Next() {
		var namespace, table, column, dataType, typeSchema string
		var generated any
		var nullable bool
		var expression *string
		var extension *string
		if err := rows.Scan(&namespace, &table, &column, &nullable, &dataType, &generated, &expression, &typeSchema, &extension); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		generatedText := normalizeGeneratedValue(generated)
		key := fmt.Sprintf("%s.%s", namespace, table)
		current := result[key]
		if current.Name == "" {
			current = connector.Schema{
				Name:              table,
				Namespace:         namespace,
				Version:           0,
				QuotedIdentifiers: map[string]bool{namespace: true, table: true},
			}
		}

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
		current.Columns = append(current.Columns, col)
		if current.QuotedIdentifiers == nil {
			current.QuotedIdentifiers = map[string]bool{}
		}
		if column != "" {
			current.QuotedIdentifiers[column] = true
		}
		result[key] = current
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate catalog: %w", err)
	}

	return result, nil
}

func formatTypeName(schema, formatted string) string {
	formatted = strings.ToLower(strings.TrimSpace(formatted))
	schema = strings.ToLower(strings.TrimSpace(schema))
	if formatted == "" {
		return formatted
	}
	if schema == "" || schema == "pg_catalog" || schema == "pg_toast" {
		return formatted
	}
	if strings.Contains(formatted, ".") {
		return formatted
	}
	return schema + "." + formatted
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
