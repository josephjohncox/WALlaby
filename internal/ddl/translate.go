package ddl

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

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
)

// DialectConfig describes SQL formatting for a destination.
type DialectConfig struct {
	Name               Dialect
	Quote              string
	AlterTypeTemplate  string
	AddColumnTemplate  string
	DropColumnTemplate string
	RenameColumnTpl    string
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
			TruncateTemplate:   "TRUNCATE TABLE %s",
			ArrayType: func(inner string) string {
				return "Array(" + inner + ")"
			},
			CreateSuffix: " ENGINE = MergeTree() ORDER BY tuple()",
		}
	case DialectDuckDB:
		return DialectConfig{
			Name:               d,
			Quote:              "\"",
			AlterTypeTemplate:  "ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s",
			AddColumnTemplate:  "ALTER TABLE %s ADD COLUMN %s",
			DropColumnTemplate: "ALTER TABLE %s DROP COLUMN %s",
			RenameColumnTpl:    "ALTER TABLE %s RENAME COLUMN %s TO %s",
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
			AddColumnTemplate:  "ALTER TABLE %s ADD COLUMN %s",
			DropColumnTemplate: "ALTER TABLE %s DROP COLUMN %s",
			RenameColumnTpl:    "ALTER TABLE %s RENAME COLUMN %s TO %s",
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
		return nil, nil
	}
	upper := strings.ToUpper(statement)

	switch {
	case strings.HasPrefix(upper, "CREATE TABLE"):
		stmt, err := translateCreateTable(statement, dialect, mappings)
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

// TranslateRecordDDL resolves a change record into translated DDL statements.
func TranslateRecordDDL(schema connector.Schema, record connector.Record, dialect DialectConfig, baseMappings map[string]string, options map[string]string) ([]string, error) {
	overrides, err := LoadTypeMappings(options)
	if err != nil {
		return nil, err
	}
	mappings := MergeTypeMappings(baseMappings, overrides)
	ddlText := strings.TrimSpace(record.DDL)
	if ddlText == "" {
		table := strings.TrimSpace(record.Table)
		if table == "" {
			table = strings.TrimSpace(schema.Name)
		}
		if table == "" {
			return nil, nil
		}
		if !strings.Contains(table, ".") && strings.TrimSpace(schema.Namespace) != "" {
			table = schema.Namespace + "." + table
		}
		qualified, err := quoteTableNamePreserve(table, schema, dialect)
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
			colName, colType, err := parseAlterColumn(entry)
			if err != nil {
				return nil, err
			}
			mapped := mapType(colType, mappings, dialect)
			statements = append(statements, fmt.Sprintf(dialect.AlterTypeTemplate, qualified, quoteIdent(colName, dialect.Quote), mapped))
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

func parseAlterColumn(action string) (string, string, error) {
	re := regexp.MustCompile(`(?is)^alter\s+column\s+(.+?)\s+(set\s+data\s+type|type)\s+(.+)$`)
	matches := re.FindStringSubmatch(strings.TrimSpace(action))
	if len(matches) != 4 {
		return "", "", fmt.Errorf("unsupported alter column action: %s", action)
	}
	name := strings.TrimSpace(matches[1])
	colType := strings.TrimSpace(matches[3])
	if name == "" || colType == "" {
		return "", "", fmt.Errorf("alter column missing name/type: %s", action)
	}
	return name, colType, nil
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

func stripQuotes(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 2 {
		if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '`' && value[len(value)-1] == '`') {
			return value[1 : len(value)-1]
		}
	}
	return value
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
		mapped = mapped + suffix
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
		return nil, nil
	}
	if raw := strings.TrimSpace(options[optTypeMappings]); raw != "" {
		return parseTypeMappings(raw)
	}
	if path := strings.TrimSpace(options[optTypeMappingsFile]); path != "" {
		data, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			return nil, fmt.Errorf("read type mappings file: %w", err)
		}
		return parseTypeMappings(string(data))
	}
	return nil, nil
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
