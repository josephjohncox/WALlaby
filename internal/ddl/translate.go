package ddl

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"gopkg.in/yaml.v3"
)

const (
	optTypeMappings     = "type_mappings"
	optTypeMappingsFile = "type_mappings_file"
)

// Dialect identifies a downstream SQL dialect.
type Dialect string

const (
	DialectSnowflake  Dialect = "snowflake"
	DialectClickHouse Dialect = "clickhouse"
	DialectDuckDB     Dialect = "duckdb"
	DialectPostgres   Dialect = "postgres"
)

// DialectConfig describes SQL formatting for a destination.
type DialectConfig struct {
	Name               Dialect
	Quote              string
	AlterTypeTemplate  string
	SetNotNullTemplate string
	DropNotNullTpl     string
	SetDefaultTpl      string
	DropDefaultTpl     string
	AddColumnTemplate  string
	DropColumnTemplate string
	RenameColumnTpl    string
	RenameTableTpl     string
	TruncateTemplate   string
	ArrayType          func(string) string
	CreateSuffix       string
}

func DialectConfigFor(d Dialect) DialectConfig {
	switch d {
	case DialectClickHouse:
		return DialectConfig{
			Name:               d,
			Quote:              "`",
			AlterTypeTemplate:  "ALTER TABLE %s MODIFY COLUMN %s %s",
			AddColumnTemplate:  "ALTER TABLE %s ADD COLUMN %s",
			DropColumnTemplate: "ALTER TABLE %s DROP COLUMN %s",
			RenameColumnTpl:    "ALTER TABLE %s RENAME COLUMN %s TO %s",
			RenameTableTpl:     "RENAME TABLE %s TO %s",
			TruncateTemplate:   "TRUNCATE TABLE %s",
			ArrayType: func(inner string) string {
				return "Array(" + inner + ")"
			},
			CreateSuffix: " ENGINE = MergeTree() ORDER BY tuple()",
		}
	case DialectPostgres:
		return DialectConfig{
			Name:               d,
			Quote:              "\"",
			AlterTypeTemplate:  "ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s",
			SetNotNullTemplate: "ALTER TABLE %s ALTER COLUMN %s SET NOT NULL",
			DropNotNullTpl:     "ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL",
			SetDefaultTpl:      "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s",
			DropDefaultTpl:     "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT",
			AddColumnTemplate:  "ALTER TABLE %s ADD COLUMN %s",
			DropColumnTemplate: "ALTER TABLE %s DROP COLUMN %s",
			RenameColumnTpl:    "ALTER TABLE %s RENAME COLUMN %s TO %s",
			RenameTableTpl:     "ALTER TABLE %s RENAME TO %s",
			TruncateTemplate:   "TRUNCATE TABLE %s",
			ArrayType: func(inner string) string {
				return inner + "[]"
			},
		}
	case DialectDuckDB:
		return DialectConfig{
			Name:               d,
			Quote:              "\"",
			AlterTypeTemplate:  "ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s",
			SetNotNullTemplate: "ALTER TABLE %s ALTER COLUMN %s SET NOT NULL",
			DropNotNullTpl:     "ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL",
			SetDefaultTpl:      "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s",
			DropDefaultTpl:     "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT",
			AddColumnTemplate:  "ALTER TABLE %s ADD COLUMN %s",
			DropColumnTemplate: "ALTER TABLE %s DROP COLUMN %s",
			RenameColumnTpl:    "ALTER TABLE %s RENAME COLUMN %s TO %s",
			RenameTableTpl:     "ALTER TABLE %s RENAME TO %s",
			TruncateTemplate:   "TRUNCATE TABLE %s",
			ArrayType: func(inner string) string {
				return inner + "[]"
			},
		}
	default:
		return DialectConfig{
			Name:               DialectSnowflake,
			Quote:              "\"",
			AlterTypeTemplate:  "ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s",
			SetNotNullTemplate: "ALTER TABLE %s ALTER COLUMN %s SET NOT NULL",
			DropNotNullTpl:     "ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL",
			SetDefaultTpl:      "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s",
			DropDefaultTpl:     "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT",
			AddColumnTemplate:  "ALTER TABLE %s ADD COLUMN %s",
			DropColumnTemplate: "ALTER TABLE %s DROP COLUMN %s",
			RenameColumnTpl:    "ALTER TABLE %s RENAME COLUMN %s TO %s",
			RenameTableTpl:     "ALTER TABLE %s RENAME TO %s",
			TruncateTemplate:   "TRUNCATE TABLE %s",
			ArrayType: func(inner string) string {
				return "ARRAY"
			},
		}
	}
}

// TranslatePostgresDDL maps a Postgres DDL statement to a destination dialect.
func TranslatePostgresDDL(ddl string, dialect DialectConfig, mappings map[string]string) ([]string, error) {
	statement := strings.TrimSpace(strings.TrimSuffix(ddl, ";"))
	if statement == "" {
		return []string{}, nil
	}
	upper := strings.ToUpper(statement)

	switch {
	case strings.HasPrefix(upper, "CREATE TABLE"):
		stmt, err := translateCreateTable(statement, dialect, mappings)
		if err != nil {
			return nil, err
		}
		return []string{stmt}, nil
	case strings.HasPrefix(upper, "RENAME TABLE"):
		stmt, err := translateRenameTable(statement, dialect)
		if err != nil {
			return nil, err
		}
		return []string{stmt}, nil
	case strings.HasPrefix(upper, "ALTER TABLE"):
		return translateAlterTable(statement, dialect, mappings)
	case strings.HasPrefix(upper, "DROP TABLE"):
		stmt, err := translateDropTable(statement, dialect)
		if err != nil {
			return nil, err
		}
		return []string{stmt}, nil
	case strings.HasPrefix(upper, "TRUNCATE"):
		stmt, err := translateTruncate(statement, dialect)
		if err != nil {
			return nil, err
		}
		return []string{stmt}, nil
	default:
		return nil, fmt.Errorf("unsupported ddl statement: %s", statement)
	}
}

// TranslatePlanDDL resolves a schema plan into translated DDL statements.
func TranslatePlanDDL(schemaDef connector.Schema, plan internalschema.Plan, dialect DialectConfig, baseMappings map[string]string, options map[string]string) ([]string, error) {
	overrides, err := LoadTypeMappings(options)
	if err != nil {
		return nil, err
	}
	mappings := MergeTypeMappings(baseMappings, overrides)

	if len(plan.Changes) == 0 {
		return []string{}, nil
	}

	statements := make([]string, 0, len(plan.Changes))
	for _, change := range plan.Changes {
		qualifiedTable, err := planQualifiedTable(schemaDef, change, dialect)
		if err != nil {
			return nil, err
		}

		switch change.Type {
		case internalschema.ChangeAddColumn:
			mapped := mapType(change.ToType, mappings, dialect)
			if mapped == "" {
				return nil, fmt.Errorf("add column missing type for %s", change.Column)
			}
			definition := quoteIdentPreserve(change.Column, dialect.Quote)
			definition = fmt.Sprintf("%s %s", definition, mapped)
			if !change.Nullable {
				definition += " NOT NULL"
			}
			statements = append(statements, fmt.Sprintf(dialect.AddColumnTemplate, qualifiedTable, definition))
		case internalschema.ChangeDropColumn:
			statements = append(statements, fmt.Sprintf(dialect.DropColumnTemplate, qualifiedTable, quoteIdentPreserve(change.Column, dialect.Quote)))
		case internalschema.ChangeAlterColumn:
			typeChanged := strings.TrimSpace(change.FromType) != "" && strings.TrimSpace(change.ToType) != "" && strings.TrimSpace(change.FromType) != strings.TrimSpace(change.ToType)
			nullabilityChanged := change.FromNullable != change.Nullable
			if typeChanged {
				mapped := mapType(change.ToType, mappings, dialect)
				if mapped == "" {
					return nil, fmt.Errorf("alter column missing target type for %s", change.Column)
				}
				statements = append(statements, fmt.Sprintf(dialect.AlterTypeTemplate, qualifiedTable, quoteIdentPreserve(change.Column, dialect.Quote), mapped))
			}
			if nullabilityChanged {
				if change.Nullable {
					if dialect.DropNotNullTpl == "" {
						return nil, fmt.Errorf("drop not null not supported for %s", dialect.Name)
					}
					statements = append(statements, fmt.Sprintf(dialect.DropNotNullTpl, qualifiedTable, quoteIdentPreserve(change.Column, dialect.Quote)))
				} else {
					if dialect.SetNotNullTemplate == "" {
						return nil, fmt.Errorf("set not null not supported for %s", dialect.Name)
					}
					statements = append(statements, fmt.Sprintf(dialect.SetNotNullTemplate, qualifiedTable, quoteIdentPreserve(change.Column, dialect.Quote)))
				}
			}
			if !typeChanged && !nullabilityChanged {
				continue
			}
		case internalschema.ChangeRenameColumn:
			newName := strings.TrimSpace(change.ToColumn)
			if newName == "" {
				newName = strings.TrimSpace(change.ToType)
			}
			if newName == "" {
				return nil, fmt.Errorf("rename column missing target name for %s", change.Column)
			}
			statements = append(statements, fmt.Sprintf(dialect.RenameColumnTpl, qualifiedTable, quoteIdentPreserve(change.Column, dialect.Quote), quoteIdentPreserve(newName, dialect.Quote)))
		case internalschema.ChangeSetGenerated:
			return nil, fmt.Errorf("set generated columns are not supported for automatic apply")
		case internalschema.ChangeDropGenerated:
			return nil, fmt.Errorf("drop generated columns are not supported for automatic apply")
		case internalschema.ChangeDropTable:
			statements = append(statements, fmt.Sprintf("DROP TABLE IF EXISTS %s", qualifiedTable))
		case internalschema.ChangeCreateTable:
			return nil, fmt.Errorf("create table changes are not supported for automatic apply")
		case internalschema.ChangeCreateIndex:
			return nil, fmt.Errorf("index changes are not supported for automatic apply")
		case internalschema.ChangeDropIndex:
			return nil, fmt.Errorf("index changes are not supported for automatic apply")
		case internalschema.ChangeAlterPrimaryKey:
			return nil, fmt.Errorf("primary key changes are not supported for automatic apply")
		case internalschema.ChangeAlterForeignKey:
			return nil, fmt.Errorf("foreign key changes are not supported for automatic apply")
		case internalschema.ChangeAlterConstraints:
			return nil, fmt.Errorf("constraint changes are not supported for automatic apply")
		default:
			return nil, fmt.Errorf("unsupported schema change: %s", change.Type)
		}
	}

	return statements, nil
}

func planQualifiedTable(schemaDef connector.Schema, change internalschema.Change, dialect DialectConfig) (string, error) {
	table := strings.TrimSpace(change.Table)
	if table == "" {
		table = strings.TrimSpace(schemaDef.Name)
	}
	if table == "" {
		return "", fmt.Errorf("change has no table")
	}
	if !strings.Contains(table, ".") && strings.TrimSpace(change.Namespace) != "" {
		table = change.Namespace + "." + table
	}
	qualified, err := quoteTableNamePreserve(table, schemaDef, dialect)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(qualified) != "" {
		return qualified, nil
	}
	if table == "" {
		return "", fmt.Errorf("invalid table name")
	}
	return qualified, nil
}

// TranslateRecordDDL resolves a change record into translated DDL statements.
func TranslateRecordDDL(schemaDef connector.Schema, record connector.Record, dialect DialectConfig, baseMappings map[string]string, options map[string]string) ([]string, error) {
	overrides, err := LoadTypeMappings(options)
	if err != nil {
		return nil, err
	}
	mappings := MergeTypeMappings(baseMappings, overrides)
	ddlText := strings.TrimSpace(record.DDL)
	if ddlText == "" && len(record.DDLPlan) > 0 {
		var plan internalschema.Plan
		if err := json.Unmarshal(record.DDLPlan, &plan); err != nil {
			return nil, fmt.Errorf("unmarshal ddl plan: %w", err)
		}
		return TranslatePlanDDL(schemaDef, plan, dialect, baseMappings, options)
	}
	if ddlText == "" {
		table := strings.TrimSpace(record.Table)
		if table == "" {
			table = strings.TrimSpace(schemaDef.Name)
		}
		if table == "" {
			return []string{}, nil
		}
		if !strings.Contains(table, ".") && strings.TrimSpace(schemaDef.Namespace) != "" {
			table = schemaDef.Namespace + "." + table
		}
		qualified, err := quoteTableNamePreserve(table, schemaDef, dialect)
		if err != nil {
			return nil, err
		}
		ddlText = "TRUNCATE TABLE " + qualified
	}
	return TranslatePostgresDDL(ddlText, dialect, mappings)
}

func translateCreateTable(stmt string, dialect DialectConfig, mappings map[string]string) (string, error) {
	re := regexp.MustCompile(`(?is)^create\s+table\s+(if\s+not\s+exists\s+)?(.+?)\s*\((.*)\)\s*$`)
	matches := re.FindStringSubmatch(stmt)
	if len(matches) != 4 {
		return "", fmt.Errorf("unsupported create table ddl: %s", stmt)
	}
	tableName := strings.TrimSpace(matches[2])
	columnsRaw := strings.TrimSpace(matches[3])
	if tableName == "" {
		return "", errors.New("create table missing table name")
	}

	qualified, err := quoteTableName(tableName, dialect)
	if err != nil {
		return "", err
	}

	columns := splitTopLevel(columnsRaw, ',')
	colDefs := make([]string, 0, len(columns))
	for _, col := range columns {
		def := strings.TrimSpace(col)
		if def == "" {
			continue
		}
		if isTableConstraint(def) {
			continue
		}
		colDef, err := translateColumnDef(def, dialect, mappings)
		if err != nil {
			return "", err
		}
		if colDef != "" {
			colDefs = append(colDefs, colDef)
		}
	}
	if len(colDefs) == 0 {
		return "", errors.New("create table has no columns")
	}

	suffix := dialect.CreateSuffix
	if suffix != "" {
		suffix = " " + strings.TrimSpace(suffix)
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)%s", qualified, strings.Join(colDefs, ", "), suffix), nil
}

func translateAlterTable(stmt string, dialect DialectConfig, mappings map[string]string) ([]string, error) {
	re := regexp.MustCompile(`(?is)^alter\s+table\s+(if\s+exists\s+)?(.+?)\s+(.*)$`)
	matches := re.FindStringSubmatch(stmt)
	if len(matches) != 4 {
		return nil, fmt.Errorf("unsupported alter table ddl: %s", stmt)
	}
	tableName := strings.TrimSpace(matches[2])
	actionsRaw := strings.TrimSpace(matches[3])
	if tableName == "" || actionsRaw == "" {
		return nil, fmt.Errorf("alter table missing table name/actions: %s", stmt)
	}

	qualified, err := quoteTableName(tableName, dialect)
	if err != nil {
		return nil, err
	}

	actions := splitTopLevel(actionsRaw, ',')
	statements := make([]string, 0, len(actions))
	for _, action := range actions {
		entry := strings.TrimSpace(action)
		if entry == "" {
			continue
		}
		upper := strings.ToUpper(entry)

		switch {
		case strings.HasPrefix(upper, "RENAME TO"):
			newName := strings.TrimSpace(entry[len("RENAME TO"):])
			if newName == "" {
				return nil, fmt.Errorf("rename table missing new name: %s", entry)
			}
			qualifiedNew, err := quoteTableName(newName, dialect)
			if err != nil {
				return nil, err
			}
			if dialect.RenameTableTpl == "" {
				return nil, fmt.Errorf("rename table not supported for dialect %s", dialect.Name)
			}
			statements = append(statements, fmt.Sprintf(dialect.RenameTableTpl, qualified, qualifiedNew))
		case strings.HasPrefix(upper, "ADD COLUMN"):
			colDef := strings.TrimSpace(entry[len("ADD COLUMN"):])
			colDef = trimPrefixFold(colDef, "IF NOT EXISTS")
			translated, err := translateColumnDef(colDef, dialect, mappings)
			if err != nil {
				return nil, err
			}
			if translated != "" {
				statements = append(statements, fmt.Sprintf(dialect.AddColumnTemplate, qualified, translated))
			}
		case strings.HasPrefix(upper, "ADD "):
			colDef := strings.TrimSpace(entry[len("ADD "):])
			colDef = trimPrefixFold(colDef, "IF NOT EXISTS")
			translated, err := translateColumnDef(colDef, dialect, mappings)
			if err != nil {
				return nil, err
			}
			if translated != "" {
				statements = append(statements, fmt.Sprintf(dialect.AddColumnTemplate, qualified, translated))
			}
		case strings.HasPrefix(upper, "DROP COLUMN"):
			colName := strings.TrimSpace(entry[len("DROP COLUMN"):])
			colName = trimPrefixFold(colName, "IF EXISTS")
			colName = strings.TrimSuffix(colName, "CASCADE")
			colName = strings.TrimSpace(colName)
			if colName == "" {
				return nil, fmt.Errorf("drop column missing name: %s", entry)
			}
			statements = append(statements, fmt.Sprintf(dialect.DropColumnTemplate, qualified, quoteIdent(colName, dialect.Quote)))
		case strings.HasPrefix(upper, "DROP "):
			colName := strings.TrimSpace(entry[len("DROP "):])
			colName = trimPrefixFold(colName, "IF EXISTS")
			colName = strings.TrimSuffix(colName, "CASCADE")
			colName = strings.TrimSpace(colName)
			if colName == "" {
				return nil, fmt.Errorf("drop column missing name: %s", entry)
			}
			statements = append(statements, fmt.Sprintf(dialect.DropColumnTemplate, qualified, quoteIdent(colName, dialect.Quote)))
		case strings.HasPrefix(upper, "ALTER COLUMN"):
			action, err := parseAlterColumn(entry)
			if err != nil {
				return nil, err
			}
			quotedCol := quoteIdent(action.Name, dialect.Quote)
			switch action.Kind {
			case alterColumnSetType:
				mapped := mapType(action.Value, mappings, dialect)
				statements = append(statements, fmt.Sprintf(dialect.AlterTypeTemplate, qualified, quotedCol, mapped))
			case alterColumnSetNotNull:
				if dialect.SetNotNullTemplate == "" {
					return nil, fmt.Errorf("set not null not supported for dialect %s", dialect.Name)
				}
				statements = append(statements, fmt.Sprintf(dialect.SetNotNullTemplate, qualified, quotedCol))
			case alterColumnDropNotNull:
				if dialect.DropNotNullTpl == "" {
					return nil, fmt.Errorf("drop not null not supported for dialect %s", dialect.Name)
				}
				statements = append(statements, fmt.Sprintf(dialect.DropNotNullTpl, qualified, quotedCol))
			case alterColumnSetDefault:
				if dialect.SetDefaultTpl == "" {
					return nil, fmt.Errorf("set default not supported for dialect %s", dialect.Name)
				}
				statements = append(statements, fmt.Sprintf(dialect.SetDefaultTpl, qualified, quotedCol, action.Value))
			case alterColumnDropDefault:
				if dialect.DropDefaultTpl == "" {
					return nil, fmt.Errorf("drop default not supported for dialect %s", dialect.Name)
				}
				statements = append(statements, fmt.Sprintf(dialect.DropDefaultTpl, qualified, quotedCol))
			default:
				return nil, fmt.Errorf("unsupported alter column action: %s", entry)
			}
		case strings.HasPrefix(upper, "RENAME COLUMN"):
			oldName, newName, err := parseRenameColumn(entry)
			if err != nil {
				return nil, err
			}
			statements = append(statements, fmt.Sprintf(dialect.RenameColumnTpl, qualified, quoteIdent(oldName, dialect.Quote), quoteIdent(newName, dialect.Quote)))
		case strings.HasPrefix(upper, "RENAME "):
			oldName, newName, err := parseRenameColumn("RENAME COLUMN " + strings.TrimSpace(entry[len("RENAME "):]))
			if err != nil {
				return nil, err
			}
			statements = append(statements, fmt.Sprintf(dialect.RenameColumnTpl, qualified, quoteIdent(oldName, dialect.Quote), quoteIdent(newName, dialect.Quote)))
		default:
			return nil, fmt.Errorf("unsupported alter table action: %s", entry)
		}
	}

	if len(statements) == 0 {
		return nil, fmt.Errorf("no alter table actions parsed for: %s", stmt)
	}
	return statements, nil
}

func translateRenameTable(stmt string, dialect DialectConfig) (string, error) {
	re := regexp.MustCompile(`(?is)^rename\s+table\s+(.+?)\s+to\s+(.+?)\s*$`)
	matches := re.FindStringSubmatch(stmt)
	if len(matches) != 3 {
		return "", fmt.Errorf("unsupported rename table ddl: %s", stmt)
	}
	oldName := strings.TrimSpace(matches[1])
	newName := strings.TrimSpace(matches[2])
	if oldName == "" || newName == "" {
		return "", fmt.Errorf("rename table missing names: %s", stmt)
	}
	if dialect.RenameTableTpl == "" {
		return "", fmt.Errorf("rename table not supported for dialect %s", dialect.Name)
	}
	qualifiedOld, err := quoteTableName(oldName, dialect)
	if err != nil {
		return "", err
	}
	qualifiedNew, err := quoteTableName(newName, dialect)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(dialect.RenameTableTpl, qualifiedOld, qualifiedNew), nil
}

func translateDropTable(stmt string, dialect DialectConfig) (string, error) {
	re := regexp.MustCompile(`(?is)^drop\s+table\s+(if\s+exists\s+)?(.+?)\s*$`)
	matches := re.FindStringSubmatch(stmt)
	if len(matches) != 3 {
		return "", fmt.Errorf("unsupported drop table ddl: %s", stmt)
	}
	tableName := strings.TrimSpace(matches[2])
	if tableName == "" {
		return "", errors.New("drop table missing table name")
	}

	qualified, err := quoteTableName(tableName, dialect)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", qualified), nil
}

func translateTruncate(stmt string, dialect DialectConfig) (string, error) {
	re := regexp.MustCompile(`(?is)^truncate\s+(table\s+)?(.+?)\s*$`)
	matches := re.FindStringSubmatch(stmt)
	if len(matches) != 3 {
		return "", fmt.Errorf("unsupported truncate ddl: %s", stmt)
	}
	tableName := strings.TrimSpace(matches[2])
	if tableName == "" {
		return "", errors.New("truncate missing table name")
	}
	qualified, err := quoteTableName(tableName, dialect)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(dialect.TruncateTemplate, qualified), nil
}

type alterColumnKind string

const (
	alterColumnSetType     alterColumnKind = "set_type"
	alterColumnSetNotNull  alterColumnKind = "set_not_null"
	alterColumnDropNotNull alterColumnKind = "drop_not_null"
	alterColumnSetDefault  alterColumnKind = "set_default"
	alterColumnDropDefault alterColumnKind = "drop_default"
)

type alterColumnAction struct {
	Name  string
	Kind  alterColumnKind
	Value string
}

func parseAlterColumn(action string) (alterColumnAction, error) {
	re := regexp.MustCompile(`(?is)^alter\s+column\s+(.+?)\s+(.+)$`)
	matches := re.FindStringSubmatch(strings.TrimSpace(action))
	if len(matches) != 3 {
		return alterColumnAction{}, fmt.Errorf("unsupported alter column action: %s", action)
	}
	name := strings.TrimSpace(matches[1])
	clause := strings.TrimSpace(matches[2])
	if name == "" || clause == "" {
		return alterColumnAction{}, fmt.Errorf("alter column missing name/action: %s", action)
	}

	upper := strings.ToUpper(clause)
	switch {
	case strings.HasPrefix(upper, "SET DATA TYPE"):
		colType := strings.TrimSpace(clause[len("SET DATA TYPE"):])
		colType = stripUsingClause(colType)
		if colType == "" {
			return alterColumnAction{}, fmt.Errorf("alter column missing type: %s", action)
		}
		return alterColumnAction{Name: name, Kind: alterColumnSetType, Value: colType}, nil
	case strings.HasPrefix(upper, "TYPE"):
		colType := strings.TrimSpace(clause[len("TYPE"):])
		colType = stripUsingClause(colType)
		if colType == "" {
			return alterColumnAction{}, fmt.Errorf("alter column missing type: %s", action)
		}
		return alterColumnAction{Name: name, Kind: alterColumnSetType, Value: colType}, nil
	case strings.EqualFold(clause, "SET NOT NULL"):
		return alterColumnAction{Name: name, Kind: alterColumnSetNotNull}, nil
	case strings.EqualFold(clause, "DROP NOT NULL"):
		return alterColumnAction{Name: name, Kind: alterColumnDropNotNull}, nil
	case strings.HasPrefix(upper, "SET DEFAULT"):
		value := strings.TrimSpace(clause[len("SET DEFAULT"):])
		if value == "" {
			return alterColumnAction{}, fmt.Errorf("alter column missing default value: %s", action)
		}
		return alterColumnAction{Name: name, Kind: alterColumnSetDefault, Value: value}, nil
	case strings.EqualFold(clause, "DROP DEFAULT"):
		return alterColumnAction{Name: name, Kind: alterColumnDropDefault}, nil
	default:
		return alterColumnAction{}, fmt.Errorf("unsupported alter column action: %s", action)
	}
}

func stripUsingClause(value string) string {
	value = strings.TrimSpace(value)
	re := regexp.MustCompile(`(?is)^(.+?)\s+using\s+.+$`)
	matches := re.FindStringSubmatch(value)
	if len(matches) == 2 {
		return strings.TrimSpace(matches[1])
	}
	return value
}

func parseRenameColumn(action string) (string, string, error) {
	re := regexp.MustCompile(`(?is)^rename\s+column\s+(.+?)\s+to\s+(.+)$`)
	matches := re.FindStringSubmatch(strings.TrimSpace(action))
	if len(matches) != 3 {
		return "", "", fmt.Errorf("unsupported rename column action: %s", action)
	}
	oldName := strings.TrimSpace(matches[1])
	newName := strings.TrimSpace(matches[2])
	if oldName == "" || newName == "" {
		return "", "", fmt.Errorf("rename column missing name: %s", action)
	}
	return oldName, newName, nil
}

func trimPrefixFold(value, prefix string) string {
	if value == "" || prefix == "" {
		return value
	}
	trimmed := strings.TrimSpace(value)
	if strings.HasPrefix(strings.ToUpper(trimmed), strings.ToUpper(prefix)) {
		return strings.TrimSpace(trimmed[len(prefix):])
	}
	return value
}

type columnDef struct {
	Name    string
	Type    string
	NotNull bool
}

func translateColumnDef(def string, dialect DialectConfig, mappings map[string]string) (string, error) {
	col, err := parseColumnDef(def)
	if err != nil {
		return "", err
	}
	if col.Name == "" || col.Type == "" {
		return "", nil
	}
	mapped := mapType(col.Type, mappings, dialect)
	quoted := quoteIdent(col.Name, dialect.Quote)
	if col.NotNull {
		return fmt.Sprintf("%s %s NOT NULL", quoted, mapped), nil
	}
	return fmt.Sprintf("%s %s", quoted, mapped), nil
}

func parseColumnDef(def string) (columnDef, error) {
	def = strings.TrimSpace(def)
	if def == "" {
		return columnDef{}, nil
	}
	tokens := splitTokens(def)
	if len(tokens) < 2 {
		return columnDef{}, fmt.Errorf("invalid column definition: %s", def)
	}
	name := tokens[0]
	idx := 1
	var typeTokens []string
	for idx < len(tokens) {
		if isConstraintToken(tokens[idx]) {
			break
		}
		typeTokens = append(typeTokens, tokens[idx])
		idx++
	}
	colType := strings.Join(typeTokens, " ")
	colType = strings.TrimSpace(colType)
	if colType == "" {
		return columnDef{}, fmt.Errorf("missing type for column: %s", def)
	}

	notNull := false
	for i := idx; i < len(tokens)-1; i++ {
		if strings.EqualFold(tokens[i], "not") && strings.EqualFold(tokens[i+1], "null") {
			notNull = true
			break
		}
	}

	return columnDef{Name: name, Type: colType, NotNull: notNull}, nil
}

func splitTokens(input string) []string {
	var tokens []string
	var buf strings.Builder
	inSingle := false
	inDouble := false
	for i := 0; i < len(input); i++ {
		ch := input[i]
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
			buf.WriteByte(ch)
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
			buf.WriteByte(ch)
		case ' ', '\t', '\n', '\r':
			if inSingle || inDouble {
				buf.WriteByte(ch)
				continue
			}
			if buf.Len() > 0 {
				tokens = append(tokens, buf.String())
				buf.Reset()
			}
		default:
			buf.WriteByte(ch)
		}
	}
	if buf.Len() > 0 {
		tokens = append(tokens, buf.String())
	}
	return tokens
}

func splitTopLevel(input string, sep rune) []string {
	var parts []string
	var buf strings.Builder
	depth := 0
	inSingle := false
	inDouble := false
	for _, ch := range input {
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
			buf.WriteRune(ch)
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
			buf.WriteRune(ch)
		case '(':
			if !inSingle && !inDouble {
				depth++
			}
			buf.WriteRune(ch)
		case ')':
			if !inSingle && !inDouble && depth > 0 {
				depth--
			}
			buf.WriteRune(ch)
		default:
			if ch == sep && depth == 0 && !inSingle && !inDouble {
				parts = append(parts, buf.String())
				buf.Reset()
				continue
			}
			buf.WriteRune(ch)
		}
	}
	if buf.Len() > 0 {
		parts = append(parts, buf.String())
	}
	return parts
}

func isTableConstraint(def string) bool {
	upper := strings.ToUpper(strings.TrimSpace(def))
	return strings.HasPrefix(upper, "CONSTRAINT ") ||
		strings.HasPrefix(upper, "PRIMARY KEY") ||
		strings.HasPrefix(upper, "UNIQUE ") ||
		strings.HasPrefix(upper, "FOREIGN KEY") ||
		strings.HasPrefix(upper, "CHECK ")
}

func isConstraintToken(token string) bool {
	upper := strings.ToUpper(token)
	switch upper {
	case "CONSTRAINT", "PRIMARY", "UNIQUE", "REFERENCES", "CHECK", "DEFAULT", "COLLATE", "GENERATED", "IDENTITY", "NOT", "NULL":
		return true
	default:
		return false
	}
}

func quoteTableName(name string, dialect DialectConfig) (string, error) {
	schema, table := splitQualifiedName(name)
	if table == "" {
		return "", fmt.Errorf("invalid table name %q", name)
	}
	if schema != "" {
		return quoteIdent(schema, dialect.Quote) + "." + quoteIdent(table, dialect.Quote), nil
	}
	return quoteIdent(table, dialect.Quote), nil
}

func splitQualifiedName(name string) (string, string) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", ""
	}
	parts := splitTopLevel(name, '.')
	if len(parts) == 1 {
		return "", strings.TrimSpace(parts[0])
	}
	if len(parts) >= 2 {
		schema := strings.TrimSpace(parts[0])
		table := strings.TrimSpace(parts[len(parts)-1])
		return schema, table
	}
	return "", strings.TrimSpace(name)
}

func quoteIdent(value, quote string) string {
	ident, quoted := parseIdent(value)
	if ident == "" {
		return ""
	}
	if !quoted {
		ident = strings.ToLower(ident)
	}
	escaped := strings.ReplaceAll(ident, quote, quote+quote)
	return quote + escaped + quote
}

func quoteIdentPreserve(value, quote string) string {
	ident, _ := parseIdent(value)
	if ident == "" {
		return ""
	}
	escaped := strings.ReplaceAll(ident, quote, quote+quote)
	return quote + escaped + quote
}

func quoteTableNamePreserve(name string, schema connector.Schema, dialect DialectConfig) (string, error) {
	if strings.TrimSpace(name) == "" {
		return "", errors.New("truncate missing table name")
	}
	schemaName, tableName := splitQualifiedName(name)
	if tableName == "" {
		return "", fmt.Errorf("invalid table name %q", name)
	}
	if schemaName == "" {
		schemaName = strings.TrimSpace(schema.Namespace)
	}
	if schemaName != "" {
		return quoteIdentPreserve(schemaName, dialect.Quote) + "." + quoteIdentPreserve(tableName, dialect.Quote), nil
	}
	return quoteIdentPreserve(tableName, dialect.Quote), nil
}

func parseIdent(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if len(value) >= 2 {
		if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '`' && value[len(value)-1] == '`') {
			return value[1 : len(value)-1], true
		}
	}
	return value, false
}

func mapType(value string, mappings map[string]string, dialect DialectConfig) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return value
	}

	isArray := strings.HasSuffix(strings.TrimSpace(value), "[]")
	baseValue := strings.TrimSuffix(strings.TrimSpace(value), "[]")

	baseKey, suffix := splitTypeSuffix(baseValue)
	mapped := mapTypeKey(baseKey, mappings)
	if mapped == "" {
		mapped = baseValue
	}
	if strings.EqualFold(mapped, "Decimal") && suffix == "" {
		suffix = "(38,9)"
	}
	if suffix != "" && !strings.Contains(mapped, "(") {
		mapped += suffix
	}

	if isArray && dialect.ArrayType != nil {
		return dialect.ArrayType(mapped)
	}
	return mapped
}

func splitTypeSuffix(value string) (string, string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return value, ""
	}
	idx := strings.IndexRune(value, '(')
	if idx <= 0 {
		return normalizeTypeKey(value), ""
	}
	return normalizeTypeKey(value[:idx]), value[idx:]
}

func mapTypeKey(value string, mappings map[string]string) string {
	if len(mappings) == 0 {
		return ""
	}
	key := normalizeTypeKey(value)
	if mapped, ok := mappings[key]; ok {
		return mapped
	}
	if idx := strings.LastIndex(key, "."); idx > 0 {
		if mapped, ok := mappings[key[idx+1:]]; ok {
			return mapped
		}
	}
	return ""
}

func normalizeTypeKey(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	parts := strings.Fields(strings.ToLower(value))
	return strings.Join(parts, " ")
}

// LoadTypeMappings loads per-destination type mappings from connector options.
func LoadTypeMappings(options map[string]string) (map[string]string, error) {
	if options == nil {
		return nil, nil //nolint:nilnil // absence of mappings is not an error
	}
	if raw := strings.TrimSpace(options[optTypeMappings]); raw != "" {
		return parseTypeMappings(raw)
	}
	if path := strings.TrimSpace(options[optTypeMappingsFile]); path != "" {
		// #nosec G304 -- path is user-configured and explicitly opted-in.
		data, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			return nil, fmt.Errorf("read type mappings file: %w", err)
		}
		return parseTypeMappings(string(data))
	}
	return nil, nil //nolint:nilnil // absence of mappings is not an error
}

// MergeTypeMappings merges base and overrides, with overrides winning.
func MergeTypeMappings(base, override map[string]string) map[string]string {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(override))
	for key, value := range base {
		if key == "" {
			continue
		}
		out[normalizeTypeKey(key)] = strings.TrimSpace(value)
	}
	for key, value := range override {
		if key == "" {
			continue
		}
		out[normalizeTypeKey(key)] = strings.TrimSpace(value)
	}
	return out
}

func parseTypeMappings(raw string) (map[string]string, error) {
	var mappings map[string]string
	data := []byte(raw)
	if err := json.Unmarshal(data, &mappings); err != nil {
		if err := yaml.Unmarshal(data, &mappings); err != nil {
			return nil, fmt.Errorf("parse type_mappings: %w", err)
		}
	}
	out := make(map[string]string, len(mappings))
	for key, value := range mappings {
		normalized := normalizeTypeKey(key)
		if normalized == "" {
			continue
		}
		out[normalized] = strings.TrimSpace(value)
	}
	return out, nil
}
