package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type ddlColumnState struct {
	typeName  string
	nullable  bool
	generated bool
}

type ddlTableState struct {
	exists  bool
	columns map[string]ddlColumnState
}

type ddlCatalogState map[string]ddlTableState

// ReconcileDDL inspects Postgres catalog state after an ambiguous execution.
// Raw SQL and schema operations without enough identity fail closed.
func (d *Destination) ReconcileDDL(ctx context.Context, schemaDef connector.Schema, record connector.Record) (connector.DDLReconcileResult, error) {
	if d.pool == nil {
		return connector.DDLReconcileIndeterminate, errors.New("postgres destination not initialized")
	}
	if len(record.DDLPlan) == 0 {
		return connector.DDLReconcileIndeterminate, nil
	}
	var plan internalschema.Plan
	if err := json.Unmarshal(record.DDLPlan, &plan); err != nil {
		return connector.DDLReconcileIndeterminate, fmt.Errorf("decode DDL reconciliation plan: %w", err)
	}
	if len(plan.Changes) == 0 {
		return connector.DDLReconcileIndeterminate, nil
	}
	if schemaDef.Name == "" {
		schemaDef.Name = record.Table
	}

	catalog := make(ddlCatalogState)
	for _, change := range plan.Changes {
		namespace, table := ddlChangeTable(schemaDef, change)
		if table == "" {
			return connector.DDLReconcileIndeterminate, nil
		}
		key := ddlCatalogKey(namespace, table)
		if _, ok := catalog[key]; ok {
			continue
		}
		state, err := d.loadDDLTableState(ctx, namespace, table)
		if err != nil {
			return connector.DDLReconcileIndeterminate, err
		}
		catalog[key] = state
	}
	return reconcilePostgresDDLPlan(schemaDef, plan, catalog), nil
}

func (d *Destination) loadDDLTableState(ctx context.Context, namespace, table string) (ddlTableState, error) {
	if namespace == "" {
		namespace = "public"
	}
	state := ddlTableState{columns: make(map[string]ddlColumnState)}
	if err := d.pool.QueryRow(ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM information_schema.tables
		   WHERE table_schema = $1 AND table_name = $2
		 )`,
		namespace, table,
	).Scan(&state.exists); err != nil {
		return ddlTableState{}, fmt.Errorf("inspect DDL target table: %w", err)
	}
	if !state.exists {
		return state, nil
	}
	rows, err := d.pool.Query(ctx,
		`SELECT a.attname,
		        pg_catalog.format_type(a.atttypid, a.atttypmod),
		        NOT a.attnotnull,
		        a.attgenerated <> ''
		 FROM pg_catalog.pg_attribute AS a
		 JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid
		 JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		 WHERE n.nspname = $1
		   AND c.relname = $2
		   AND a.attnum > 0
		   AND NOT a.attisdropped`,
		namespace, table,
	)
	if err != nil {
		return ddlTableState{}, fmt.Errorf("inspect DDL target columns: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, typeName string
		var nullable, generated bool
		if err := rows.Scan(&name, &typeName, &nullable, &generated); err != nil {
			return ddlTableState{}, fmt.Errorf("scan DDL target column: %w", err)
		}
		state.columns[name] = ddlColumnState{
			typeName:  normalizePostgresDDLType(typeName),
			nullable:  nullable,
			generated: generated,
		}
	}
	if err := rows.Err(); err != nil {
		return ddlTableState{}, fmt.Errorf("iterate DDL target columns: %w", err)
	}
	return state, nil
}

func reconcilePostgresDDLPlan(schemaDef connector.Schema, plan internalschema.Plan, catalog ddlCatalogState) connector.DDLReconcileResult {
	result := connector.DDLReconcileIndeterminate
	for index, change := range plan.Changes {
		namespace, table := ddlChangeTable(schemaDef, change)
		state, ok := catalog[ddlCatalogKey(namespace, table)]
		if !ok {
			return connector.DDLReconcileIndeterminate
		}
		changeResult := reconcilePostgresDDLPlanChange(schemaDef, plan, index, change, state)
		if !changeResult.Valid() {
			return connector.DDLReconcileIndeterminate
		}
		if result == connector.DDLReconcileIndeterminate {
			result = changeResult
			continue
		}
		if result != changeResult {
			return connector.DDLReconcileIndeterminate
		}
	}
	return result
}

func reconcilePostgresDDLPlanChange(schemaDef connector.Schema, plan internalschema.Plan, index int, change internalschema.Change, table ddlTableState) connector.DDLReconcileResult {
	finalColumn := change.Column
	for _, later := range plan.Changes[index+1:] {
		changeNamespace, changeTable := ddlChangeTable(schemaDef, change)
		laterNamespace, laterTable := ddlChangeTable(schemaDef, later)
		if later.Type == internalschema.ChangeRenameColumn &&
			changeNamespace == laterNamespace &&
			changeTable == laterTable &&
			finalColumn == later.Column {
			finalColumn = later.ToColumn
		}
	}
	if finalColumn == change.Column {
		return reconcilePostgresDDLChange(change, table)
	}
	_, originalExists := table.columns[change.Column]
	_, finalExists := table.columns[finalColumn]
	if originalExists && finalExists {
		return connector.DDLReconcileIndeterminate
	}
	appliedChange := change
	appliedChange.Column = finalColumn
	if change.Type == internalschema.ChangeRenameColumn {
		appliedChange.ToColumn = finalColumn
	}
	if result := reconcilePostgresDDLChange(appliedChange, table); result == connector.DDLReconcileApplied {
		return result
	}
	return reconcilePostgresDDLChange(change, table)
}

func reconcilePostgresDDLChange(change internalschema.Change, table ddlTableState) connector.DDLReconcileResult {
	column := change.Column
	toColumn := change.ToColumn
	actual, columnExists := table.columns[column]
	switch change.Type {
	case internalschema.ChangeCreateTable:
		if table.exists {
			return connector.DDLReconcileApplied
		}
		return connector.DDLReconcileNotApplied
	case internalschema.ChangeDropTable:
		if !table.exists {
			return connector.DDLReconcileApplied
		}
		return connector.DDLReconcileNotApplied
	case internalschema.ChangeAddColumn:
		if !columnExists {
			return connector.DDLReconcileNotApplied
		}
		if change.ToType != "" && !postgresDDLTypesEquivalent(actual.typeName, change.ToType) {
			return connector.DDLReconcileIndeterminate
		}
		if actual.nullable != change.Nullable {
			return connector.DDLReconcileIndeterminate
		}
		return connector.DDLReconcileApplied
	case internalschema.ChangeDropColumn:
		if !columnExists {
			return connector.DDLReconcileApplied
		}
		return connector.DDLReconcileNotApplied
	case internalschema.ChangeAlterColumn:
		if !columnExists {
			return connector.DDLReconcileIndeterminate
		}
		typeChanged := strings.TrimSpace(change.FromType) != "" &&
			strings.TrimSpace(change.ToType) != "" &&
			!postgresDDLTypesEquivalent(change.FromType, change.ToType)
		nullabilityChanged := change.FromNullable != change.Nullable
		if typeChanged && !postgresDDLTypesEquivalent(actual.typeName, change.ToType) {
			return connector.DDLReconcileNotApplied
		}
		if nullabilityChanged && actual.nullable != change.Nullable {
			return connector.DDLReconcileNotApplied
		}
		return connector.DDLReconcileApplied
	case internalschema.ChangeRenameColumn:
		_, targetExists := table.columns[toColumn]
		switch {
		case columnExists && !targetExists:
			return connector.DDLReconcileNotApplied
		case !columnExists && targetExists:
			return connector.DDLReconcileApplied
		default:
			return connector.DDLReconcileIndeterminate
		}
	case internalschema.ChangeSetGenerated:
		if !columnExists {
			return connector.DDLReconcileIndeterminate
		}
		if actual.generated {
			return connector.DDLReconcileApplied
		}
		return connector.DDLReconcileNotApplied
	case internalschema.ChangeDropGenerated:
		if !columnExists {
			return connector.DDLReconcileIndeterminate
		}
		if !actual.generated {
			return connector.DDLReconcileApplied
		}
		return connector.DDLReconcileNotApplied
	default:
		return connector.DDLReconcileIndeterminate
	}
}

func ddlChangeTable(schemaDef connector.Schema, change internalschema.Change) (string, string) {
	namespace := change.Namespace
	if namespace == "" {
		namespace = schemaDef.Namespace
	}
	table := change.Table
	if table == "" {
		table = schemaDef.Name
	}
	return namespace, table
}

func ddlCatalogKey(namespace, table string) string {
	return namespace + "\x00" + table
}

func postgresDDLTypesEquivalent(actual, expected string) bool {
	return normalizePostgresDDLType(actual) == normalizePostgresDDLType(expected)
}

func normalizePostgresDDLType(value string) string {
	value = strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(value))), " ")
	value = strings.ReplaceAll(value, ", ", ",")
	value = strings.ReplaceAll(value, "timestamp without time zone", "timestamp")
	value = strings.ReplaceAll(value, "timestamp with time zone", "timestamptz")
	value = strings.ReplaceAll(value, "time without time zone", "time")
	value = strings.ReplaceAll(value, "time with time zone", "timetz")
	value = strings.ReplaceAll(value, " without time zone", "")
	if strings.HasPrefix(value, "timestamp(") && strings.HasSuffix(value, " with time zone") {
		value = "timestamptz" + strings.TrimSuffix(strings.TrimPrefix(value, "timestamp"), " with time zone")
	}
	if strings.HasPrefix(value, "time(") && strings.HasSuffix(value, " with time zone") {
		value = "timetz" + strings.TrimSuffix(strings.TrimPrefix(value, "time"), " with time zone")
	}
	arraySuffix := ""
	for strings.HasSuffix(value, "[]") {
		arraySuffix += "[]"
		value = strings.TrimSpace(strings.TrimSuffix(value, "[]"))
	}

	base, modifier := value, ""
	if index := strings.IndexByte(value, '('); index >= 0 {
		base, modifier = strings.TrimSpace(value[:index]), value[index:]
	}
	var normalized string
	switch base {
	case "bigint", "int8", "bigserial", "serial8":
		normalized = "int8"
	case "integer", "int", "int4", "serial", "serial4":
		normalized = "int4"
	case "smallint", "int2", "smallserial", "serial2":
		normalized = "int2"
	case "boolean", "bool":
		normalized = "bool"
	case "character varying", "varchar":
		normalized = "varchar"
	case "character", "char", "bpchar":
		normalized = "bpchar"
	case "double precision", "float8":
		normalized = "float8"
	case "real", "float4":
		normalized = "float4"
	case "decimal", "numeric":
		normalized = "numeric"
	case "timestamp without time zone", "timestamp":
		normalized = "timestamp"
	case "timestamp with time zone", "timestamptz":
		normalized = "timestamptz"
	default:
		normalized = base
	}
	return normalized + modifier + arraySuffix
}
