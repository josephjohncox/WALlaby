package iceberg

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	iceberggo "github.com/apache/iceberg-go"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// The Iceberg catalog owns table field IDs. A real Apache Iceberg REST catalog
// replaces caller-supplied IDs with fresh sequential table field IDs on create
// and on additive evolution. Wallaby therefore never depends on a catalog
// preserving its hash-derived canonical field IDs. Instead it persists a stable
// canonical identity in each Iceberg field's immutable doc, rebuilds an
// authoritative canonical-field-to-catalog-field mapping from the schema the
// catalog returns, and rewrites data-file PARQUET:field_id values with the
// catalog-assigned IDs before committing them.

const icebergIdentityDocPrefix = "wallaby.identity="

// stableFieldIdentity derives an evolution-stable identity for a canonical
// field. User columns key on PostgreSQL source relation/column identity, which
// survives supported renames without inferring the rename from the column name.
// Canonical envelope fields have no source identity and key on their frozen
// name; those fields are never renamed.
func stableFieldIdentity(field artifactlog.CanonicalField) string {
	relation := strings.TrimSpace(field.Metadata["source_relation_id"])
	column := strings.TrimSpace(field.Metadata["source_column_id"])
	if relation != "" && column != "" {
		return "src:" + relation + ":" + column
	}
	return "name:" + field.Name
}

func identityDoc(identity string) string {
	return icebergIdentityDocPrefix + identity
}

func identityFromDoc(doc string) (string, bool) {
	if strings.HasPrefix(doc, icebergIdentityDocPrefix) {
		return strings.TrimPrefix(doc, icebergIdentityDocPrefix), true
	}
	return "", false
}

// schemaWithIdentityDocs returns a copy of schema whose top-level fields carry
// the canonical stable identity in their doc. The canonical projection is flat,
// so a nested field is a hard error rather than a silently unmapped write.
func schemaWithIdentityDocs(schema *iceberggo.Schema, identityByName map[string]string) (*iceberggo.Schema, error) {
	fields := make([]iceberggo.NestedField, 0, schema.NumFields())
	for index := 0; index < schema.NumFields(); index++ {
		field := schema.Field(index)
		if isNestedType(field.Type) {
			return nil, fmt.Errorf("iceberg field %q has nested type %s; the canonical projection must be flat", field.Name, field.Type)
		}
		identity, ok := identityByName[field.Name]
		if !ok {
			return nil, fmt.Errorf("iceberg field %q has no canonical identity", field.Name)
		}
		field.Doc = identityDoc(identity)
		fields = append(fields, field)
	}
	return iceberggo.NewSchema(schema.ID, fields...), nil
}

func isNestedType(dataType iceberggo.Type) bool {
	switch dataType.(type) {
	case *iceberggo.StructType, *iceberggo.ListType, *iceberggo.MapType:
		return true
	default:
		return false
	}
}

// fieldIdentity resolves the stable identity of a catalog field. A field that
// Wallaby created or evolved carries the identity doc; a pre-existing field
// without one falls back to its name, which is unambiguous when no rename is in
// flight.
func fieldIdentity(field iceberggo.NestedField) string {
	if identity, ok := identityFromDoc(field.Doc); ok {
		return identity
	}
	return "name:" + field.Name
}

type renameOp struct {
	from string
	to   string
}

// evolutionPlan compares the catalog-owned current schema with the desired
// canonical schema and returns the additive columns and supported renames
// needed to make the table represent every canonical field. Renames are keyed
// on stable identity, never inferred from names. Incompatible type changes and
// ambiguous collisions fail closed.
func evolutionPlan(current, desired *iceberggo.Schema) ([]iceberggo.NestedField, []renameOp, error) {
	currentByIdentity := make(map[string]iceberggo.NestedField, current.NumFields())
	currentByName := make(map[string]iceberggo.NestedField, current.NumFields())
	for index := 0; index < current.NumFields(); index++ {
		field := current.Field(index)
		currentByName[field.Name] = field
		identity := fieldIdentity(field)
		if _, clash := currentByIdentity[identity]; clash {
			return nil, nil, fmt.Errorf("%w: catalog field identity %q is not unique", connector.ErrDeliveryConflict, identity)
		}
		currentByIdentity[identity] = field
	}

	var adds []iceberggo.NestedField
	var renames []renameOp
	claimedRenameTargets := make(map[string]struct{})
	for index := 0; index < desired.NumFields(); index++ {
		want := desired.Field(index)
		identity := fieldIdentity(want)
		if existing, ok := currentByIdentity[identity]; ok {
			if !existing.Type.Equals(want.Type) {
				return nil, nil, fmt.Errorf("%w: field identity %q type changed from %s to %s (unsupported)", connector.ErrDeliveryConflict, identity, existing.Type, want.Type)
			}
			if existing.Name != want.Name {
				if _, taken := currentByName[want.Name]; taken {
					return nil, nil, fmt.Errorf("%w: rename target %q already exists on the table", connector.ErrDeliveryConflict, want.Name)
				}
				if _, taken := claimedRenameTargets[want.Name]; taken {
					return nil, nil, fmt.Errorf("%w: two canonical fields rename to %q", connector.ErrDeliveryConflict, want.Name)
				}
				claimedRenameTargets[want.Name] = struct{}{}
				renames = append(renames, renameOp{from: existing.Name, to: want.Name})
			}
			continue
		}
		// A brand-new logical field. If its name already exists under a
		// different identity the schemas are incompatible.
		if _, taken := currentByName[want.Name]; taken {
			return nil, nil, fmt.Errorf("%w: canonical field %q collides with an existing table field of different identity", connector.ErrDeliveryConflict, want.Name)
		}
		add := want
		// Additive evolution must be nullable so existing rows remain valid.
		add.Required = false
		adds = append(adds, add)
	}
	sort.Slice(renames, func(i, j int) bool { return renames[i].from < renames[j].from })
	sort.Slice(adds, func(i, j int) bool { return adds[i].Name < adds[j].Name })
	return adds, renames, nil
}

// buildFieldMapping produces the authoritative canonical-column-name to
// catalog-field-ID mapping from the schema the catalog returned. It validates
// stable identity, name, type, and requiredness, and fails closed on missing
// fields or collisions. The result drives both data-file field-ID rewriting and
// commit-metadata persistence.
func buildFieldMapping(current, desired *iceberggo.Schema) (map[string]int, error) {
	currentByIdentity := make(map[string]iceberggo.NestedField, current.NumFields())
	currentByName := make(map[string]iceberggo.NestedField, current.NumFields())
	for index := 0; index < current.NumFields(); index++ {
		field := current.Field(index)
		currentByName[field.Name] = field
		identity := fieldIdentity(field)
		if _, clash := currentByIdentity[identity]; clash {
			return nil, fmt.Errorf("%w: catalog field identity %q is not unique", connector.ErrDeliveryConflict, identity)
		}
		currentByIdentity[identity] = field
	}

	mapping := make(map[string]int, desired.NumFields())
	claimedIDs := make(map[int]string, desired.NumFields())
	for index := 0; index < desired.NumFields(); index++ {
		want := desired.Field(index)
		if isNestedType(want.Type) {
			return nil, fmt.Errorf("iceberg field %q has nested type %s; the canonical projection must be flat", want.Name, want.Type)
		}
		identity := fieldIdentity(want)
		field, ok := currentByIdentity[identity]
		if !ok {
			if field, ok = currentByName[want.Name]; !ok {
				return nil, fmt.Errorf("stable field identity %q (%s) is missing from the catalog schema; evolve the table first", identity, want.Name)
			}
		}
		if field.Name != want.Name {
			return nil, fmt.Errorf("%w: field identity %q maps to catalog name %q but canonical name is %q; apply the rename first", connector.ErrDeliveryConflict, identity, field.Name, want.Name)
		}
		if !field.Type.Equals(want.Type) {
			return nil, fmt.Errorf("%w: field %q type differs: table=%s canonical=%s", connector.ErrDeliveryConflict, want.Name, field.Type, want.Type)
		}
		if field.Required && !want.Required {
			return nil, fmt.Errorf("%w: table field %q is required but the changelog projection is nullable", connector.ErrDeliveryConflict, want.Name)
		}
		if prior, taken := claimedIDs[field.ID]; taken {
			return nil, fmt.Errorf("%w: canonical fields %q and %q both map to catalog field ID %d", connector.ErrDeliveryConflict, prior, want.Name, field.ID)
		}
		claimedIDs[field.ID] = want.Name
		mapping[want.Name] = field.ID
	}
	return mapping, nil
}

// mappingFingerprint is a deterministic, order-independent digest of a
// canonical-to-catalog field-ID mapping. It is recorded in immutable commit
// metadata so a retry proves it rewrote data files against the same
// catalog-assigned identity even if the catalog re-derives IDs differently.
func mappingFingerprint(mapping map[string]int) string {
	names := make([]string, 0, len(mapping))
	for name := range mapping {
		names = append(names, name)
	}
	sort.Strings(names)
	var builder strings.Builder
	for _, name := range names {
		builder.WriteString(name)
		builder.WriteByte('=')
		builder.WriteString(strconv.Itoa(mapping[name]))
		builder.WriteByte(0)
	}
	return builder.String()
}

// rewriteRecordFieldIDs returns record batches whose Arrow schema carries the
// catalog-assigned PARQUET:field_id values from mapping, so the data files
// committed to Iceberg carry the catalog's field IDs rather than the canonical
// hash-derived IDs. A column absent from the mapping fails closed.
func rewriteRecordFieldIDs(records []arrow.RecordBatch, mapping map[string]int) ([]arrow.RecordBatch, error) {
	if len(records) == 0 {
		return nil, nil
	}
	source := records[0].Schema()
	fields := make([]arrow.Field, source.NumFields())
	for index := 0; index < source.NumFields(); index++ {
		field := source.Field(index)
		id, ok := mapping[field.Name]
		if !ok {
			return nil, fmt.Errorf("%w: canonical column %q has no catalog field ID", connector.ErrDeliveryConflict, field.Name)
		}
		field.Metadata = arrow.MetadataFrom(map[string]string{
			"PARQUET:field_id": strconv.Itoa(id),
		})
		fields[index] = field
	}
	metadata := source.Metadata()
	rewrittenSchema := arrow.NewSchema(fields, &metadata)
	rewritten := make([]arrow.RecordBatch, 0, len(records))
	for _, record := range records {
		if !record.Schema().Equal(source) {
			releaseRecordBatches(rewritten)
			return nil, fmt.Errorf("%w: canonical record batches have inconsistent schemas", connector.ErrDeliveryConflict)
		}
		projected := array.NewRecordBatch(rewrittenSchema, record.Columns(), record.NumRows())
		rewritten = append(rewritten, projected)
	}
	return rewritten, nil
}

func releaseRecordBatches(records []arrow.RecordBatch) {
	for _, record := range records {
		record.Release()
	}
}
