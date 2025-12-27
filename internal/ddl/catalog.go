package ddl

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/ductstream/internal/registry"
	"github.com/josephjohncox/ductstream/internal/schema"
	"github.com/josephjohncox/ductstream/pkg/connector"
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
		`SELECT table_schema, table_name, column_name, is_nullable, data_type, is_generated, generation_expression
		 FROM information_schema.columns
		 WHERE table_schema = ANY($1::text[])
		 ORDER BY table_schema, table_name, ordinal_position`, schemas)
	if err != nil {
		return nil, fmt.Errorf("scan catalog: %w", err)
	}
	defer rows.Close()

	result := make(map[string]connector.Schema)
	for rows.Next() {
		var namespace, table, column, nullable, dataType, isGenerated string
		var expression *string
		if err := rows.Scan(&namespace, &table, &column, &nullable, &dataType, &isGenerated, &expression); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		key := fmt.Sprintf("%s.%s", namespace, table)
		current := result[key]
		if current.Name == "" {
			current = connector.Schema{
				Name:      table,
				Namespace: namespace,
				Version:   0,
			}
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
		current.Columns = append(current.Columns, col)
		result[key] = current
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate catalog: %w", err)
	}

	return result, nil
}
