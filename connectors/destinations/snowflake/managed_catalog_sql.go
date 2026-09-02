package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type managedSnowflakeCatalogQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (d *Destination) loadManagedSnowflakeCatalogWith(ctx context.Context, queryer managedSnowflakeCatalogQueryer) (managedCatalogSnapshot, error) {
	target, err := d.loadManagedSnowflakeTableWith(ctx, queryer, d.managedConfig.table)
	if err != nil {
		return managedCatalogSnapshot{}, fmt.Errorf("inspect managed Snowflake target: %w", err)
	}
	receipts, err := d.loadManagedSnowflakeTableWith(ctx, queryer, d.managedConfig.receiptsTable)
	if err != nil {
		return managedCatalogSnapshot{}, fmt.Errorf("inspect managed Snowflake receipts: %w", err)
	}
	var taskCount int
	// #nosec G202 -- the database identifier is restricted to one validated unquoted uppercase identifier.
	if err := queryer.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM TABLE("+quoteIdent(d.managedConfig.database, '"')+".INFORMATION_SCHEMA.TASKS()) WHERE SCHEMA_NAME = ?",
		d.managedConfig.schema,
	).Scan(&taskCount); err != nil {
		return managedCatalogSnapshot{}, fmt.Errorf("inspect managed Snowflake schema tasks: %w", err)
	}
	return managedCatalogSnapshot{target: target, receipts: receipts, taskCount: taskCount}, nil
}

const managedSnowflakeCatalogTimestampFormat = `YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM`

func (d *Destination) loadManagedSnowflakeTableWith(ctx context.Context, queryer managedSnowflakeCatalogQueryer, table string) (managedTableSnapshot, error) {
	snapshot := managedTableSnapshot{columns: make(map[string]managedColumnSnapshot), grants: make(map[string][]string)}
	informationSchema := quoteIdent(d.managedConfig.database, '"') + ".INFORMATION_SCHEMA."
	var isHybrid string
	// #nosec G202 -- the database identifier is restricted to one validated unquoted uppercase identifier.
	if err := queryer.QueryRowContext(ctx,
		"SELECT IS_HYBRID, COALESCE(COMMENT, ''), TO_VARCHAR(CREATED, '"+managedSnowflakeCatalogTimestampFormat+"') FROM "+informationSchema+"TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		d.managedConfig.schema, table,
	).Scan(&isHybrid, &snapshot.comment, &snapshot.createdOn); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return managedTableSnapshot{}, fmt.Errorf("table %s.%s.%s does not exist", d.managedConfig.database, d.managedConfig.schema, table)
		}
		return managedTableSnapshot{}, err
	}
	if strings.EqualFold(isHybrid, "YES") {
		snapshot.kind = "HYBRID TABLE"
	} else {
		snapshot.kind = "TABLE"
	}
	owner, grants, err := d.loadManagedSnowflakeGrants(ctx, queryer, table)
	if err != nil {
		return managedTableSnapshot{}, err
	}
	snapshot.ownerRole = owner
	snapshot.grants = grants

	// #nosec G202 -- the database identifier is restricted to one validated unquoted uppercase identifier.
	rows, err := queryer.QueryContext(ctx, `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, IS_IDENTITY,
       NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_MAXIMUM_LENGTH
FROM `+informationSchema+`COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION`, d.managedConfig.schema, table)
	if err != nil {
		return managedTableSnapshot{}, fmt.Errorf("query columns: %w", err)
	}
	for rows.Next() {
		var name, dataType, nullable, identity string
		var defaultValue sql.NullString
		var precision, scale, datetimePrecision, characterMaximumLength sql.NullInt64
		if err := rows.Scan(&name, &dataType, &nullable, &defaultValue, &identity, &precision, &scale, &datetimePrecision, &characterMaximumLength); err != nil {
			_ = rows.Close()
			return managedTableSnapshot{}, fmt.Errorf("scan column: %w", err)
		}
		if strings.EqualFold(dataType, "NUMBER") && precision.Valid && scale.Valid {
			dataType = fmt.Sprintf("NUMBER(%d,%d)", precision.Int64, scale.Int64)
		} else if strings.HasPrefix(strings.ToUpper(dataType), "TIMESTAMP_") && datetimePrecision.Valid {
			dataType = fmt.Sprintf("%s(%d)", dataType, datetimePrecision.Int64)
		}
		canonicalName, err := canonicalManagedSnowflakeCatalogIdentifier(name)
		if err != nil {
			_ = rows.Close()
			return managedTableSnapshot{}, fmt.Errorf("catalog column: %w", err)
		}
		if _, duplicate := snapshot.columns[canonicalName]; duplicate {
			_ = rows.Close()
			return managedTableSnapshot{}, fmt.Errorf("catalog repeats column %q", canonicalName)
		}
		snapshot.columns[canonicalName] = managedColumnSnapshot{
			dataType: dataType, characterMaximumLength: characterMaximumLength.Int64,
			numericPrecision: precision.Int64, numericScale: scale.Int64, datetimePrecision: datetimePrecision.Int64,
			nullable: strings.EqualFold(nullable, "YES"), hasDefault: defaultValue.Valid,
			generated: strings.EqualFold(identity, "YES"),
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return managedTableSnapshot{}, fmt.Errorf("iterate columns: %w", err)
	}
	if err := rows.Close(); err != nil {
		return managedTableSnapshot{}, fmt.Errorf("close columns: %w", err)
	}

	constraints, err := d.loadManagedSnowflakeConstraints(ctx, queryer, informationSchema, table)
	if err != nil {
		return managedTableSnapshot{}, err
	}
	snapshot.constraints = constraints
	// #nosec G202 -- the database identifier is restricted to one validated unquoted uppercase identifier.
	if err := queryer.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+informationSchema+"TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_TYPE NOT IN ('PRIMARY KEY','UNIQUE')", d.managedConfig.schema, table).Scan(&snapshot.otherConstraintCount); err != nil {
		return managedTableSnapshot{}, fmt.Errorf("query unsupported constraints: %w", err)
	}
	return snapshot, nil
}

func (d *Destination) loadManagedSnowflakeGrants(ctx context.Context, queryer managedSnowflakeCatalogQueryer, table string) (string, map[string][]string, error) {
	rows, err := queryer.QueryContext(ctx, "SHOW GRANTS ON TABLE "+managedSnowflakeQualifiedTable(d.managedConfig, table))
	if err != nil {
		return "", nil, fmt.Errorf("show table grants: %w", err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return "", nil, fmt.Errorf("read grant columns: %w", err)
	}
	indexes := make(map[string]int, len(columns))
	for index, column := range columns {
		indexes[strings.ToLower(column)] = index
	}
	privilegeIndex, hasPrivilege := indexes["privilege"]
	granteeIndex, hasGrantee := indexes["grantee_name"]
	if !hasPrivilege || !hasGrantee {
		return "", nil, errors.New("snowflake SHOW GRANTS omitted privilege or grantee_name")
	}
	owners := make(map[string]struct{})
	grants := make(map[string][]string)
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for index := range values {
			pointers[index] = &values[index]
		}
		if err := rows.Scan(pointers...); err != nil {
			return "", nil, fmt.Errorf("scan table grant: %w", err)
		}
		role, err := canonicalManagedSnowflakeCatalogIdentifier(sqlValueString(values[granteeIndex]))
		if err != nil {
			return "", nil, fmt.Errorf("catalog grantee role: %w", err)
		}
		privilege := strings.ToUpper(strings.TrimSpace(sqlValueString(values[privilegeIndex])))
		if privilege == "" {
			return "", nil, errors.New("snowflake SHOW GRANTS returned an empty privilege")
		}
		for _, existing := range grants[role] {
			if existing == privilege {
				return "", nil, fmt.Errorf("snowflake SHOW GRANTS repeated %s for role %s", privilege, role)
			}
		}
		grants[role] = append(grants[role], privilege)
		if privilege == "OWNERSHIP" {
			owners[role] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return "", nil, fmt.Errorf("iterate table grants: %w", err)
	}
	if len(owners) != 1 {
		return "", nil, fmt.Errorf("table has %d ownership grants, want exactly one", len(owners))
	}
	for role := range grants {
		sort.Strings(grants[role])
	}
	for owner := range owners {
		return owner, grants, nil
	}
	return "", nil, errors.New("table ownership grant is absent")
}

func (d *Destination) loadManagedSnowflakeConstraints(ctx context.Context, queryer managedSnowflakeCatalogQueryer, informationSchema, table string) ([]managedConstraintSnapshot, error) {
	// #nosec G202 -- the database identifier is restricted to one validated unquoted uppercase identifier.
	rows, err := queryer.QueryContext(ctx, `SELECT TC.CONSTRAINT_NAME, TC.CONSTRAINT_TYPE, TC.ENFORCED,
       KCU.COLUMN_NAME, KCU.ORDINAL_POSITION
FROM `+informationSchema+`TABLE_CONSTRAINTS AS TC
JOIN `+informationSchema+`KEY_COLUMN_USAGE AS KCU
  ON KCU.CONSTRAINT_CATALOG = TC.CONSTRAINT_CATALOG
 AND KCU.CONSTRAINT_SCHEMA = TC.CONSTRAINT_SCHEMA
 AND KCU.CONSTRAINT_NAME = TC.CONSTRAINT_NAME
WHERE TC.TABLE_SCHEMA = ? AND TC.TABLE_NAME = ?
  AND TC.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
ORDER BY TC.CONSTRAINT_NAME, KCU.ORDINAL_POSITION`, d.managedConfig.schema, table)
	if err != nil {
		return nil, fmt.Errorf("query enforced constraints: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type namedConstraint struct {
		name     string
		kind     string
		enforced bool
		columns  map[int]string
	}
	byName := make(map[string]*namedConstraint)
	for rows.Next() {
		var name, kind string
		var enforced any
		var column string
		var ordinal int
		if err := rows.Scan(&name, &kind, &enforced, &column, &ordinal); err != nil {
			return nil, fmt.Errorf("scan constraint: %w", err)
		}
		canonicalName, err := canonicalManagedSnowflakeCatalogIdentifier(name)
		if err != nil {
			return nil, fmt.Errorf("catalog constraint name: %w", err)
		}
		constraint := byName[canonicalName]
		if constraint == nil {
			constraint = &namedConstraint{name: canonicalName, kind: kind, enforced: sqlValueBool(enforced), columns: make(map[int]string)}
			byName[canonicalName] = constraint
		}
		if constraint.kind != kind || constraint.enforced != sqlValueBool(enforced) {
			return nil, fmt.Errorf("constraint %s metadata changed within result", name)
		}
		canonicalColumn, err := canonicalManagedSnowflakeCatalogIdentifier(column)
		if err != nil {
			return nil, fmt.Errorf("constraint %s column: %w", name, err)
		}
		if _, duplicate := constraint.columns[ordinal]; duplicate {
			return nil, fmt.Errorf("constraint %s repeats ordinal %d", name, ordinal)
		}
		constraint.columns[ordinal] = canonicalColumn
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate constraints: %w", err)
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]managedConstraintSnapshot, 0, len(names))
	for _, name := range names {
		constraint := byName[name]
		ordinals := make([]int, 0, len(constraint.columns))
		for ordinal := range constraint.columns {
			ordinals = append(ordinals, ordinal)
		}
		sort.Ints(ordinals)
		columns := make([]string, 0, len(ordinals))
		for _, ordinal := range ordinals {
			columns = append(columns, constraint.columns[ordinal])
		}
		result = append(result, managedConstraintSnapshot{name: constraint.name, constraintType: constraint.kind, enforced: constraint.enforced, columns: columns})
	}
	return result, nil
}

func canonicalManagedSnowflakeCatalogIdentifier(value string) (string, error) {
	if strings.TrimSpace(value) != value {
		return "", fmt.Errorf("managed Snowflake catalog identifier %q contains surrounding whitespace", value)
	}
	if err := validateManagedSnowflakeUnquotedIdentifier("catalog identifier", value); err != nil {
		return "", fmt.Errorf("managed Snowflake rejects quoted or noncanonical catalog identifier %q: %w", value, err)
	}
	return value, nil
}

func sqlValueString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(typed)
	}
}

func sqlValueBool(value any) bool {
	raw := strings.TrimSpace(strings.ToLower(sqlValueString(value)))
	if raw == "yes" || raw == "y" || raw == "true" || raw == "1" {
		return true
	}
	parsed, _ := strconv.ParseBool(raw)
	return parsed
}
