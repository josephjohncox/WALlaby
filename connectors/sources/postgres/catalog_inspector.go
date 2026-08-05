package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// CatalogScope is an explicit, read-only catalog selection. At least one table,
// schema, or publication is required; the inspector never scans public or the
// search path implicitly.
type CatalogScope struct {
	// TableSelectors and SchemaSelectors are PostgreSQL identifier text parsed
	// by parse_ident(..., true) in the same read-only catalog transaction.
	TableSelectors  []string
	SchemaSelectors []string
	Publication     string
}

type CatalogTableName struct {
	Schema string
	Table  string
}

type CatalogColumn struct {
	Attnum               int16
	Name                 string
	TypeOID              uint32
	TypeSchema           string
	TypeName             string
	FormattedType        string
	Nullable             bool
	GeneratedKind        string
	IdentityKind         string
	GenerationExpression string
	HasDefault           bool
	DefaultExpression    string
	Extension            string
}

type CatalogTable struct {
	Schema                 string
	Table                  string
	RelationOID            uint32
	ReplicaIdentity        string
	ReplicaIdentityColumns []string
	PrimaryKeyColumns      []string
	Columns                []CatalogColumn
}

// InspectCatalog opens one IAM-aware pool and reads the selected catalog in a
// repeatable-read, read-only transaction.
func InspectCatalog(ctx context.Context, dsn string, options map[string]string, scope CatalogScope) ([]CatalogTable, error) {
	if err := scope.validate(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("postgres dsn is required")
	}
	pool, err := newPool(ctx, dsn, options)
	if err != nil {
		return nil, fmt.Errorf("connect postgres catalog: %w", err)
	}
	defer pool.Close()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin read-only catalog inspection: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tables, err := inspectCatalogTx(ctx, tx, scope)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit read-only catalog inspection: %w", err)
	}
	return tables, nil
}

// ParseCatalogTableName strictly parses a qualified identifier for override
// keys. Catalog selection itself is parsed by PostgreSQL parse_ident.
func ParseCatalogTableName(value string) (CatalogTableName, error) {
	parts, err := parseIdentifierText(value)
	if err != nil {
		return CatalogTableName{}, err
	}
	if len(parts) != 2 {
		return CatalogTableName{}, fmt.Errorf("table %q must contain exactly schema.table", value)
	}
	return CatalogTableName{Schema: parts[0], Table: parts[1]}, nil
}

// ParseCatalogColumnName parses one PostgreSQL identifier exactly. Unquoted
// input follows PostgreSQL folding rules; quoted input preserves all bytes,
// including leading, trailing, or whitespace-only names.
func ParseCatalogColumnName(value string) (string, error) {
	parts, err := parseIdentifierText(value)
	if err != nil {
		return "", err
	}
	if len(parts) != 1 {
		return "", fmt.Errorf("column %q must contain exactly one identifier", value)
	}
	return parts[0], nil
}

// ParseCatalogColumnNames parses a comma-separated list of PostgreSQL column
// identifiers without treating commas or whitespace inside quotes as syntax.
func ParseCatalogColumnNames(value string) ([]string, error) {
	tokens := make([]string, 0, 1)
	start := 0
	quoted := false
	for index := 0; index < len(value); index++ {
		switch value[index] {
		case '"':
			if quoted && index+1 < len(value) && value[index+1] == '"' {
				index++
				continue
			}
			quoted = !quoted
		case ',':
			if !quoted {
				tokens = append(tokens, value[start:index])
				start = index + 1
			}
		}
	}
	if quoted {
		return nil, fmt.Errorf("unterminated quoted identifier in %q", value)
	}
	tokens = append(tokens, value[start:])
	columns := make([]string, 0, len(tokens))
	for _, token := range tokens {
		column, err := ParseCatalogColumnName(token)
		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	return columns, nil
}

func parseIdentifierText(value string) ([]string, error) {
	if strings.IndexByte(value, 0) >= 0 {
		return nil, fmt.Errorf("identifier %q cannot contain NUL", value)
	}
	parts := make([]string, 0, 2)
	i := 0
	skipSpace := func() {
		for i < len(value) {
			r, size := utf8.DecodeRuneInString(value[i:])
			if !unicode.IsSpace(r) {
				break
			}
			i += size
		}
	}
	skipSpace()
	for i < len(value) {
		var part strings.Builder
		if value[i] == '"' {
			i++
			closed := false
			for i < len(value) {
				r, size := utf8.DecodeRuneInString(value[i:])
				i += size
				if r == '"' {
					if i < len(value) && value[i] == '"' {
						part.WriteByte('"')
						i++
						continue
					}
					closed = true
					break
				}
				part.WriteRune(r)
			}
			if !closed {
				return nil, fmt.Errorf("unterminated quoted identifier in %q", value)
			}
			if part.Len() == 0 {
				return nil, fmt.Errorf("empty quoted identifier in %q", value)
			}
		} else {
			r, size := utf8.DecodeRuneInString(value[i:])
			if !identifierStart(r) {
				return nil, fmt.Errorf("invalid unquoted identifier in %q", value)
			}
			part.WriteRune(unicode.ToLower(r))
			i += size
			for i < len(value) {
				r, size = utf8.DecodeRuneInString(value[i:])
				if !identifierContinue(r) {
					break
				}
				part.WriteRune(unicode.ToLower(r))
				i += size
			}
		}
		parts = append(parts, part.String())
		skipSpace()
		if i == len(value) {
			break
		}
		if value[i] != '.' {
			return nil, fmt.Errorf("trailing token in identifier %q", value)
		}
		i++
		skipSpace()
		if i == len(value) {
			return nil, fmt.Errorf("identifier %q ends after '.'", value)
		}
	}
	return parts, nil
}
func identifierStart(r rune) bool    { return r == '_' || unicode.IsLetter(r) || r >= utf8.RuneSelf }
func identifierContinue(r rune) bool { return identifierStart(r) || unicode.IsDigit(r) || r == '$' }

func (s CatalogScope) validate() error {
	if len(s.TableSelectors) == 0 && len(s.SchemaSelectors) == 0 && s.Publication == "" {
		return errors.New("catalog inspection requires explicit table, schema, or publication scope")
	}
	for _, selector := range append(append([]string(nil), s.TableSelectors...), s.SchemaSelectors...) {
		if selector == "" {
			return errors.New("catalog identifier selector cannot be empty")
		}
		if strings.IndexByte(selector, 0) >= 0 {
			return errors.New("catalog identifier selector cannot contain NUL")
		}
	}
	if strings.IndexByte(s.Publication, 0) >= 0 {
		return errors.New("catalog publication identifier cannot contain NUL")
	}
	return nil
}

func parseIdentifierServer(ctx context.Context, tx pgx.Tx, value string, want int) ([]string, error) {
	var parts []string
	if err := tx.QueryRow(ctx, `SELECT pg_catalog.parse_ident($1,true)`, value).Scan(&parts); err != nil {
		return nil, err
	}
	if len(parts) != want {
		return nil, fmt.Errorf("identifier must contain exactly %d component(s), got %d", want, len(parts))
	}
	return parts, nil
}

type publicationTableSelection struct {
	allColumns bool
	attributes map[int16]struct{}
}

func publicationTablesQuery(serverVersion int) string {
	if serverVersion >= 150000 {
		return `SELECT g.relid::bigint,CASE WHEN g.attrs IS NULL THEN NULL ELSE g.attrs::smallint[] END FROM pg_catalog.pg_get_publication_tables($1) AS g`
	}
	return `SELECT published.relid::bigint FROM pg_catalog.pg_get_publication_tables($1) AS published(relid)`
}
func loadEffectivePublicationTables(ctx context.Context, tx pgx.Tx, publication string) (map[uint32]publicationTableSelection, error) {
	if publication == "" {
		return nil, nil
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_publication WHERE pubname=$1)`, publication).Scan(&exists); err != nil {
		return nil, fmt.Errorf("verify publication %q: %w", publication, err)
	}
	if !exists {
		return nil, fmt.Errorf("publication %q does not exist", publication)
	}
	var version int
	if err := tx.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&version); err != nil {
		return nil, fmt.Errorf("read server version: %w", err)
	}
	rows, err := tx.Query(ctx, publicationTablesQuery(version), publication)
	if err != nil {
		return nil, fmt.Errorf("read effective publication tables %q: %w", publication, err)
	}
	defer rows.Close()
	out := map[uint32]publicationTableSelection{}
	for rows.Next() {
		var oid int64
		var attrs []int16
		if version >= 150000 {
			if err := rows.Scan(&oid, &attrs); err != nil {
				return nil, err
			}
		} else if err := rows.Scan(&oid); err != nil {
			return nil, err
		}
		key := uint32(oid)
		current, seen := out[key]
		if !seen {
			current = publicationTableSelection{attributes: map[int16]struct{}{}}
		}
		if version < 150000 || attrs == nil {
			current.allColumns = true
			current.attributes = nil
		} else if !current.allColumns {
			for _, attnum := range attrs {
				current.attributes[attnum] = struct{}{}
			}
		}
		out[key] = current
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func inspectCatalogTx(ctx context.Context, tx pgx.Tx, scope CatalogScope) ([]CatalogTable, error) {
	resolvedTables := make([]CatalogTableName, 0, len(scope.TableSelectors))
	resolvedTableOIDs := make([]uint32, 0, len(scope.TableSelectors))
	for _, selector := range scope.TableSelectors {
		parts, err := parseIdentifierServer(ctx, tx, selector, 2)
		if err != nil {
			return nil, fmt.Errorf("parse table selector %q: %w", selector, err)
		}
		var oid *int64
		if err := tx.QueryRow(ctx, `SELECT pg_catalog.to_regclass($1)::oid::bigint`, selector).Scan(&oid); err != nil {
			return nil, fmt.Errorf("resolve table selector %q: %w", selector, err)
		}
		if oid == nil {
			return nil, fmt.Errorf("selected table %q does not exist", selector)
		}
		resolvedTables = append(resolvedTables, CatalogTableName{Schema: parts[0], Table: parts[1]})
		resolvedTableOIDs = append(resolvedTableOIDs, uint32(*oid))
	}
	resolvedSchemas := make([]string, 0, len(scope.SchemaSelectors))
	for _, selector := range scope.SchemaSelectors {
		parts, err := parseIdentifierServer(ctx, tx, selector, 1)
		if err != nil {
			return nil, fmt.Errorf("parse schema selector %q: %w", selector, err)
		}
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname=$1)`, parts[0]).Scan(&exists); err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("selected schema %q does not exist", selector)
		}
		resolvedSchemas = append(resolvedSchemas, parts[0])
	}
	explicitSchemas := map[string]struct{}{}
	for _, schema := range resolvedSchemas {
		explicitSchemas[schema] = struct{}{}
	}
	schemas := resolvedSchemas
	publication := scope.Publication
	published, err := loadEffectivePublicationTables(ctx, tx, publication)
	if err != nil {
		return nil, err
	}
	publicationOIDs := make([]uint32, 0, len(published))
	for oid := range published {
		publicationOIDs = append(publicationOIDs, oid)
	}
	sort.Slice(publicationOIDs, func(i, j int) bool { return publicationOIDs[i] < publicationOIDs[j] })
	rows, err := tx.Query(ctx, `
SELECT n.nspname,c.relname,c.oid::bigint,c.relreplident::text
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
WHERE c.relkind IN ('r','p')
  AND (
    c.oid=ANY($1::oid[])
    OR n.nspname=ANY($2::text[])
    OR c.oid=ANY($3::oid[])
  )
ORDER BY n.nspname,c.relname`, resolvedTableOIDs, schemas, publicationOIDs)
	if err != nil {
		return nil, fmt.Errorf("select catalog tables: %w", err)
	}
	defer rows.Close()
	tables := make([]CatalogTable, 0)
	byOID := map[uint32]int{}
	foundOIDs := map[uint32]struct{}{}
	foundSchemas := map[string]struct{}{}
	explicitOIDs := map[uint32]struct{}{}
	for _, oid := range resolvedTableOIDs {
		explicitOIDs[oid] = struct{}{}
	}
	for rows.Next() {
		var table CatalogTable
		var oid int64
		if err := rows.Scan(&table.Schema, &table.Table, &oid, &table.ReplicaIdentity); err != nil {
			return nil, fmt.Errorf("scan catalog table: %w", err)
		}
		table.RelationOID = uint32(oid)
		byOID[table.RelationOID] = len(tables)
		foundOIDs[table.RelationOID] = struct{}{}
		foundSchemas[table.Schema] = struct{}{}
		if _, ok := explicitSchemas[table.Schema]; ok {
			explicitOIDs[table.RelationOID] = struct{}{}
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate catalog tables: %w", err)
	}
	for index, selectedOID := range resolvedTableOIDs {
		if _, ok := foundOIDs[selectedOID]; !ok {
			selected := resolvedTables[index]
			return nil, fmt.Errorf("selected table %s.%s is not a publishable table", selected.Schema, selected.Table)
		}
	}
	for _, schema := range resolvedSchemas {
		if _, ok := foundSchemas[schema]; !ok {
			return nil, fmt.Errorf("selected schema %s contains no publishable tables", schema)
		}
	}
	if len(tables) == 0 {
		return nil, errors.New("explicit catalog scope selected no tables")
	}
	oids := make([]uint32, 0, len(tables))
	for _, table := range tables {
		oids = append(oids, table.RelationOID)
	}
	columnRows, err := tx.Query(ctx, `
SELECT a.attrelid::bigint,a.attnum::smallint,a.attname,a.atttypid::bigint,
       tn.nspname,t.typname,pg_catalog.format_type(a.atttypid,a.atttypmod),
       NOT (a.attnotnull OR COALESCE(tm.domain_not_null,false)),a.attgenerated::text,a.attidentity::text,
       CASE WHEN a.attgenerated<>'' THEN pg_catalog.pg_get_expr(ad.adbin,ad.adrelid) ELSE NULL END,
       ad.oid IS NOT NULL AND a.attgenerated='',
       CASE WHEN a.attgenerated='' THEN pg_catalog.pg_get_expr(ad.adbin,ad.adrelid) ELSE NULL END,
       tm.extension
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_type t ON t.oid=a.atttypid
JOIN pg_catalog.pg_namespace tn ON tn.oid=t.typnamespace
LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid=a.attrelid AND ad.adnum=a.attnum
LEFT JOIN LATERAL (
  WITH RECURSIVE type_chain(oid) AS (
    SELECT a.atttypid
    UNION
    SELECT next_type.oid
    FROM type_chain chain
    JOIN pg_catalog.pg_type current_type ON current_type.oid=chain.oid
    JOIN LATERAL (VALUES(current_type.typbasetype),(current_type.typelem)) next_type(oid) ON next_type.oid<>0
  )
  SELECT COALESCE(bool_or(chain_type.typtype='d' AND chain_type.typnotnull),false) AS domain_not_null,
         (array_agg(DISTINCT ext.extname ORDER BY ext.extname) FILTER (WHERE ext.extname IS NOT NULL))[1] AS extension
  FROM type_chain chain
  JOIN pg_catalog.pg_type chain_type ON chain_type.oid=chain.oid
  LEFT JOIN pg_catalog.pg_depend dep ON dep.classid='pg_type'::regclass AND dep.objid=chain.oid
    AND dep.refclassid='pg_extension'::regclass AND dep.deptype='e'
  LEFT JOIN pg_catalog.pg_extension ext ON ext.oid=dep.refobjid
) tm ON true
WHERE a.attrelid=ANY($1::oid[]) AND a.attnum>0 AND NOT a.attisdropped
ORDER BY a.attrelid,a.attnum`, oids)
	if err != nil {
		return nil, fmt.Errorf("select catalog columns: %w", err)
	}
	for columnRows.Next() {
		var relationOID, typeOID int64
		var column CatalogColumn
		var generatedExpression, defaultExpression, extension *string
		if err := columnRows.Scan(&relationOID, &column.Attnum, &column.Name, &typeOID, &column.TypeSchema, &column.TypeName, &column.FormattedType, &column.Nullable, &column.GeneratedKind, &column.IdentityKind, &generatedExpression, &column.HasDefault, &defaultExpression, &extension); err != nil {
			columnRows.Close()
			return nil, fmt.Errorf("scan catalog column: %w", err)
		}
		relationKey := uint32(relationOID)
		if selection, ok := published[relationKey]; ok {
			if _, explicit := explicitOIDs[relationKey]; !explicit && !selection.allColumns {
				if _, included := selection.attributes[column.Attnum]; !included {
					continue
				}
			}
		}
		column.TypeOID = uint32(typeOID)
		if generatedExpression != nil {
			column.GenerationExpression = *generatedExpression
		}
		if defaultExpression != nil {
			column.DefaultExpression = *defaultExpression
		}
		if extension != nil {
			column.Extension = strings.ToLower(*extension)
		}
		index, ok := byOID[relationKey]
		if !ok {
			columnRows.Close()
			return nil, errors.New("catalog column references an unselected relation")
		}
		tables[index].Columns = append(tables[index].Columns, column)
	}
	if err := columnRows.Err(); err != nil {
		columnRows.Close()
		return nil, fmt.Errorf("iterate catalog columns: %w", err)
	}
	columnRows.Close()
	keyRows, err := tx.Query(ctx, `
SELECT i.indrelid::bigint,i.indisprimary,i.indisreplident,a.attname,k.ord::int
FROM pg_catalog.pg_index i
JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,ord) ON k.ord<=i.indnkeyatts
JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
WHERE i.indrelid=ANY($1::oid[]) AND (i.indisprimary OR i.indisreplident)
ORDER BY i.indrelid,i.indisprimary DESC,k.ord`, oids)
	if err != nil {
		return nil, fmt.Errorf("select catalog keys: %w", err)
	}
	for keyRows.Next() {
		var oid int64
		var primary, replica bool
		var name string
		var ordinal int
		if err := keyRows.Scan(&oid, &primary, &replica, &name, &ordinal); err != nil {
			keyRows.Close()
			return nil, err
		}
		index := byOID[uint32(oid)]
		if primary {
			tables[index].PrimaryKeyColumns = append(tables[index].PrimaryKeyColumns, name)
		}
		if replica {
			tables[index].ReplicaIdentityColumns = append(tables[index].ReplicaIdentityColumns, name)
		}
	}
	if err := keyRows.Err(); err != nil {
		keyRows.Close()
		return nil, err
	}
	keyRows.Close()
	for i := range tables {
		if tables[i].ReplicaIdentity == "d" {
			tables[i].ReplicaIdentityColumns = append([]string(nil), tables[i].PrimaryKeyColumns...)
		}
		if tables[i].ReplicaIdentity == "f" {
			for _, column := range tables[i].Columns {
				tables[i].ReplicaIdentityColumns = append(tables[i].ReplicaIdentityColumns, column.Name)
			}
		}
	}
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Schema != tables[j].Schema {
			return tables[i].Schema < tables[j].Schema
		}
		return tables[i].Table < tables[j].Table
	})
	return tables, nil
}
