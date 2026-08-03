package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// PublicationRelation is one canonical publication membership entry. Columns
// and RowFilter are empty for an unrestricted table entry.
type PublicationRelation struct {
	Namespace string
	Table     string
	Columns   string
	RowFilter string
}

// PublicationDefinition contains every PostgreSQL publication attribute that
// changes pgoutput membership or behavior.
type PublicationDefinition struct {
	Name             string
	AllTables        bool
	Insert           bool
	Update           bool
	Delete           bool
	Truncate         bool
	ViaPartitionRoot bool
	Relations        []PublicationRelation
}

// PublicationQuerier is the narrow pgx query surface used to read a live
// publication definition.
type PublicationQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

// PublicationFingerprint hashes the complete canonical definition. Relation
// ordering does not affect the result; column-list and row-filter text is the
// canonical text exposed by pg_publication_tables.
func PublicationFingerprint(definition PublicationDefinition) (string, error) {
	definition.Name = strings.TrimSpace(definition.Name)
	if definition.Name == "" {
		return "", errors.New("publication name is required")
	}
	relations := append([]PublicationRelation(nil), definition.Relations...)
	sort.Slice(relations, func(i, j int) bool {
		left := relations[i]
		right := relations[j]
		if left.Namespace != right.Namespace {
			return left.Namespace < right.Namespace
		}
		if left.Table != right.Table {
			return left.Table < right.Table
		}
		if left.Columns != right.Columns {
			return left.Columns < right.Columns
		}
		return left.RowFilter < right.RowFilter
	})
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "%s\x00%t\x00%t\x00%t\x00%t\x00%t\x00%t\n", definition.Name, definition.AllTables, definition.Insert, definition.Update, definition.Delete, definition.Truncate, definition.ViaPartitionRoot)
	for _, relation := range relations {
		_, _ = fmt.Fprintf(hash, "%s\x00%s\x00%s\x00%s\n", relation.Namespace, relation.Table, relation.Columns, relation.RowFilter)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// ReadPublicationDefinition reads all canonical publication behavior and
// membership fields from PostgreSQL.
func ReadPublicationDefinition(ctx context.Context, querier PublicationQuerier, name string) (PublicationDefinition, error) {
	name = strings.TrimSpace(name)
	if querier == nil || name == "" {
		return PublicationDefinition{}, errors.New("publication query connection and name are required")
	}
	definition := PublicationDefinition{Name: name}
	if err := querier.QueryRow(ctx, `
SELECT puballtables,pubinsert,pubupdate,pubdelete,pubtruncate,pubviaroot
FROM pg_catalog.pg_publication WHERE pubname=$1`, name).Scan(
		&definition.AllTables,
		&definition.Insert,
		&definition.Update,
		&definition.Delete,
		&definition.Truncate,
		&definition.ViaPartitionRoot,
	); err != nil {
		return PublicationDefinition{}, fmt.Errorf("read publication definition: %w", err)
	}
	var serverVersion int
	if err := querier.QueryRow(ctx, `SELECT current_setting('server_version_num')::integer`).Scan(&serverVersion); err != nil {
		return PublicationDefinition{}, fmt.Errorf("read publication server version: %w", err)
	}
	membershipColumns := "''::text,''::text"
	if serverVersion >= 150000 {
		// Column lists and row filters were added to pg_publication_rel in
		// PostgreSQL 15. Keep the PostgreSQL 14 query parseable by selecting
		// version-specific expressions instead of referencing absent columns.
		membershipColumns = `CASE WHEN membership.prattrs IS NULL THEN '' ELSE COALESCE((
         SELECT string_agg(attribute.attname,',' ORDER BY selected.ordinality)
         FROM unnest(membership.prattrs::smallint[]) WITH ORDINALITY AS selected(attnum,ordinality)
         JOIN pg_catalog.pg_attribute AS attribute
           ON attribute.attrelid=membership.prrelid AND attribute.attnum=selected.attnum
       ),'') END,
       COALESCE(pg_catalog.pg_get_expr(membership.prqual,membership.prrelid),'')`
	}
	query := fmt.Sprintf(`
SELECT namespace.nspname,relation.relname,%s
FROM pg_catalog.pg_publication AS publication
JOIN pg_catalog.pg_publication_rel AS membership ON membership.prpubid=publication.oid
JOIN pg_catalog.pg_class AS relation ON relation.oid=membership.prrelid
JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace
WHERE publication.pubname=$1
ORDER BY namespace.nspname,relation.relname`, membershipColumns)
	rows, err := querier.Query(ctx, query, name)
	if err != nil {
		return PublicationDefinition{}, fmt.Errorf("read publication membership: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var relation PublicationRelation
		if err := rows.Scan(&relation.Namespace, &relation.Table, &relation.Columns, &relation.RowFilter); err != nil {
			return PublicationDefinition{}, fmt.Errorf("scan publication membership: %w", err)
		}
		definition.Relations = append(definition.Relations, relation)
	}
	if err := rows.Err(); err != nil {
		return PublicationDefinition{}, fmt.Errorf("iterate publication membership: %w", err)
	}
	return definition, nil
}

// LivePublicationFingerprint reads and hashes one live PostgreSQL publication.
func LivePublicationFingerprint(ctx context.Context, querier PublicationQuerier, name string) (string, error) {
	definition, err := ReadPublicationDefinition(ctx, querier, name)
	if err != nil {
		return "", err
	}
	return PublicationFingerprint(definition)
}
