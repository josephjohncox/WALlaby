package iceberg

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	iceberggo "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// CanonicalObjectReader reads only the exact immutable object version rooted in
// PostgreSQL. Implementations must not resolve a mutable key or object listing.
type CanonicalObjectReader interface {
	ReadVersion(context.Context, artifactlog.ObjectEvidence) ([]byte, error)
}

type projectionPlan struct {
	groups []*projectionGroup
}

type projectionGroup struct {
	id                string
	target            table.Identifier
	schemaFingerprint string
	schema            *iceberggo.Schema
	records           []arrow.RecordBatch
	firstOrdinal      uint64
	barrier           bool
}

func (p *projectionPlan) release() {
	for _, group := range p.groups {
		for _, record := range group.records {
			record.Release()
		}
	}
}

type canonicalSchemaDocument struct {
	ProjectionID       string                       `json:"projection_id"`
	MappingFingerprint string                       `json:"mapping_fingerprint"`
	SourceLineageID    string                       `json:"source_lineage_id"`
	Namespace          string                       `json:"namespace"`
	Table              string                       `json:"table"`
	Fields             []artifactlog.CanonicalField `json:"fields"`
}

func buildProjection(ctx context.Context, request artifactlog.CommitRequest, objects CanonicalObjectReader, cfg Config) (*projectionPlan, error) {
	if err := validateMaterializedProjectionIdentity(request.ProjectionID, request.MappingFingerprint); err != nil {
		return nil, err
	}
	if objects == nil {
		return nil, errors.New("canonical object reader is required")
	}
	plan := &projectionPlan{}
	controlTarget, err := cfg.controlTarget()
	if err != nil {
		return nil, err
	}
	groups := make(map[string]*projectionGroup)
	targetSchemas := map[string]string{strings.Join(controlTarget, "\x00"): controlSchemaFingerprint}
	if len(request.Barriers) > 0 {
		group, err := projectBarriers(request, cfg)
		if err != nil {
			return nil, err
		}
		plan.groups = append(plan.groups, group)
	}

	rooted := append([]artifactlog.RootedArtifact(nil), request.Objects...)
	sort.Slice(rooted, func(i, j int) bool {
		if rooted[i].FirstRecordOrdinal == rooted[j].FirstRecordOrdinal {
			return rooted[i].ArtifactID < rooted[j].ArtifactID
		}
		return rooted[i].FirstRecordOrdinal < rooted[j].FirstRecordOrdinal
	})
	var previousEnd uint64
	for index, object := range rooted {
		if object.LogicalBatchID != request.LogicalBatchID {
			plan.release()
			return nil, fmt.Errorf("%w: rooted object logical batch differs", connector.ErrDeliveryConflict)
		}
		if index > 0 && object.FirstRecordOrdinal < previousEnd {
			plan.release()
			return nil, fmt.Errorf("%w: rooted object record ordinals overlap", connector.ErrDeliveryConflict)
		}
		previousEnd = object.FirstRecordOrdinal + object.RecordCount
		target, err := cfg.target(object.Namespace, object.Table)
		if err != nil {
			plan.release()
			return nil, err
		}
		targetKey := strings.Join(target, "\x00")
		if existingSchemaID, exists := targetSchemas[targetKey]; exists && existingSchemaID != object.SchemaID {
			plan.release()
			return nil, fmt.Errorf("%w: multiple schema projections target Iceberg table %s in publication %s", connector.ErrDeliveryConflict, strings.Join(target, "."), request.PublicationID)
		}
		targetSchemas[targetKey] = object.SchemaID
		groupID := projectionGroupID(target, object.SchemaID, false)
		projected, err := projectObject(ctx, request, object, objects)
		if err != nil {
			plan.release()
			return nil, err
		}
		group := groups[groupID]
		if group == nil {
			group = &projectionGroup{
				id: groupID, target: target, schemaFingerprint: object.SchemaID,
				schema: projected.schema, firstOrdinal: object.FirstRecordOrdinal,
			}
			groups[groupID] = group
			plan.groups = append(plan.groups, group)
		} else if !group.schema.Equals(projected.schema) {
			projected.release()
			plan.release()
			return nil, fmt.Errorf("%w: schema fingerprint %s decoded inconsistently", connector.ErrDeliveryConflict, object.SchemaID)
		}
		group.records = append(group.records, projected.records...)
		projected.records = nil
	}
	sort.SliceStable(plan.groups, func(i, j int) bool {
		if plan.groups[i].barrier != plan.groups[j].barrier {
			return plan.groups[i].barrier
		}
		return plan.groups[i].firstOrdinal < plan.groups[j].firstOrdinal
	})
	if len(plan.groups) == 0 {
		return nil, errors.New("canonical publication has no projection groups")
	}
	return plan, nil
}

type projectedObject struct {
	schema  *iceberggo.Schema
	records []arrow.RecordBatch
}

func (p *projectedObject) release() {
	for _, record := range p.records {
		record.Release()
	}
	p.records = nil
}

func projectObject(ctx context.Context, request artifactlog.CommitRequest, object artifactlog.RootedArtifact, objects CanonicalObjectReader) (*projectedObject, error) {
	schemaDigest := sha256.Sum256(object.SchemaJSON)
	if hex.EncodeToString(schemaDigest[:]) != object.SchemaID {
		return nil, fmt.Errorf("%w: canonical schema checksum differs for %s", connector.ErrDeliveryConflict, object.ArtifactID)
	}
	var document canonicalSchemaDocument
	if err := json.Unmarshal(object.SchemaJSON, &document); err != nil {
		return nil, fmt.Errorf("decode canonical schema %s: %w", object.SchemaID, err)
	}
	if document.ProjectionID != request.ProjectionID || document.MappingFingerprint != request.MappingFingerprint || strings.TrimSpace(document.SourceLineageID) == "" || document.Namespace != object.Namespace || document.Table != object.Table {
		return nil, fmt.Errorf("%w: canonical schema identity differs for %s", connector.ErrDeliveryConflict, object.ArtifactID)
	}
	for _, field := range document.Fields {
		if (field.SourceRelationID != 0 || field.SyntheticSourceRelation != "") && field.SourceLineageID != document.SourceLineageID {
			return nil, fmt.Errorf("%w: canonical field %q lineage differs from schema lineage", connector.ErrDeliveryConflict, field.Name)
		}
	}
	body, err := objects.ReadVersion(ctx, object.Evidence)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(body)
	if actual := hex.EncodeToString(digest[:]); actual != object.EncodedByteHash || actual != object.Evidence.ChecksumSHA256 {
		return nil, fmt.Errorf("%w: canonical object checksum differs for %s", connector.ErrDeliveryConflict, object.ArtifactID)
	}
	if int64(len(body)) != object.Evidence.Length {
		return nil, fmt.Errorf("%w: canonical object length differs for %s", connector.ErrDeliveryConflict, object.ArtifactID)
	}

	parquetReader, err := file.NewParquetReader(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("open canonical Parquet %s: %w", object.ArtifactID, err)
	}
	defer func() { _ = parquetReader.Close() }()
	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	if err != nil {
		return nil, fmt.Errorf("open canonical Arrow projection %s: %w", object.ArtifactID, err)
	}
	arrowTable, err := arrowReader.ReadTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("read canonical Arrow projection %s: %w", object.ArtifactID, err)
	}
	defer arrowTable.Release()
	actualRows := uint64(arrowTable.NumRows()) // #nosec G115 -- Parquet row count is nonnegative.
	if actualRows != object.RecordCount {
		return nil, fmt.Errorf("%w: canonical object %s has %d rows, expected %d", connector.ErrDeliveryConflict, object.ArtifactID, arrowTable.NumRows(), object.RecordCount)
	}
	projectedSchema, icebergSchema, err := projectionSchema(arrowTable.Schema(), document.Fields)
	if err != nil {
		return nil, fmt.Errorf("project canonical schema %s: %w", object.SchemaID, err)
	}

	result := &projectedObject{schema: icebergSchema}
	tableReader := array.NewTableReader(arrowTable, 64*1024)
	defer tableReader.Release()
	var observed uint64
	for tableReader.Next() {
		record := tableReader.RecordBatch()
		projected := array.NewRecordBatch(projectedSchema, record.Columns(), record.NumRows())
		if err := validateCanonicalRows(projected, request, object, observed); err != nil {
			projected.Release()
			result.release()
			return nil, err
		}
		observed += uint64(projected.NumRows()) // #nosec G115 -- record batch row count is nonnegative.
		result.records = append(result.records, projected)
	}
	if err := tableReader.Err(); err != nil {
		result.release()
		return nil, fmt.Errorf("read canonical record batches: %w", err)
	}
	return result, nil
}

func projectionSchema(source *arrow.Schema, fields []artifactlog.CanonicalField) (*arrow.Schema, *iceberggo.Schema, error) {
	if len(source.Fields()) != len(fields) {
		return nil, nil, fmt.Errorf("parquet fields=%d, canonical fields=%d", len(source.Fields()), len(fields))
	}
	byName := make(map[string]artifactlog.CanonicalField, len(fields))
	identityByName := make(map[string]string, len(fields))
	for _, field := range fields {
		if field.ID <= 0 || strings.TrimSpace(field.Name) == "" {
			return nil, nil, errors.New("canonical field ID and name are required")
		}
		if _, exists := byName[field.Name]; exists {
			return nil, nil, fmt.Errorf("duplicate canonical field %q", field.Name)
		}
		byName[field.Name] = field
		identityByName[field.Name] = stableFieldIdentity(field)
	}
	projectedFields := make([]arrow.Field, len(source.Fields()))
	for index, sourceField := range source.Fields() {
		canonical, ok := byName[sourceField.Name]
		if !ok {
			return nil, nil, fmt.Errorf("parquet field %q is absent from canonical schema", sourceField.Name)
		}
		if value, ok := sourceField.Metadata.GetValue("wallaby.field_id"); ok && value != strconv.FormatInt(int64(canonical.ID), 10) {
			return nil, nil, fmt.Errorf("field %q metadata ID %s differs from canonical ID %d", sourceField.Name, value, canonical.ID)
		}
		projectedFields[index] = sourceField
		// The canonical ID is only a pre-rewrite placeholder. The committer
		// rewrites these to the catalog-assigned field IDs before appending.
		projectedFields[index].Metadata = arrow.MetadataFrom(map[string]string{
			"PARQUET:field_id": strconv.FormatInt(int64(canonical.ID), 10),
		})
	}
	metadata := source.Metadata()
	projected := arrow.NewSchema(projectedFields, &metadata)
	icebergSchema, err := table.ArrowSchemaToIceberg(projected, false, nil)
	if err != nil {
		return nil, nil, err
	}
	icebergSchema, err = schemaWithIdentityDocs(icebergSchema, identityByName)
	if err != nil {
		return nil, nil, err
	}
	return projected, icebergSchema, nil
}

func validateCanonicalRows(record arrow.RecordBatch, request artifactlog.CommitRequest, object artifactlog.RootedArtifact, offset uint64) error {
	logicalBatch, err := stringColumn(record, "__wallaby_logical_batch_id")
	if err != nil {
		return err
	}
	recordOrdinal, err := int64Column(record, "__wallaby_record_ordinal")
	if err != nil {
		return err
	}
	namespace, err := stringColumn(record, "__namespace")
	if err != nil {
		return err
	}
	tableName, err := stringColumn(record, "__table")
	if err != nil {
		return err
	}
	operation, err := stringColumn(record, "__op")
	if err != nil {
		return err
	}
	for row := 0; row < int(record.NumRows()); row++ {
		if logicalBatch.IsNull(row) || logicalBatch.Value(row) != request.LogicalBatchID {
			return fmt.Errorf("%w: logical batch differs at artifact %s row %d", connector.ErrDeliveryConflict, object.ArtifactID, row)
		}
		rowOffset := uint64(row) // #nosec G115 -- row is a nonnegative loop index.
		expectedOrdinal := object.FirstRecordOrdinal + offset + rowOffset
		if recordOrdinal.IsNull(row) || recordOrdinal.Value(row) < 0 {
			return fmt.Errorf("%w: record ordinal differs at artifact %s row %d", connector.ErrDeliveryConflict, object.ArtifactID, row)
		}
		actualOrdinal := uint64(recordOrdinal.Value(row)) // #nosec G115 -- negative ordinals are rejected above.
		if actualOrdinal != expectedOrdinal {
			return fmt.Errorf("%w: record ordinal differs at artifact %s row %d", connector.ErrDeliveryConflict, object.ArtifactID, row)
		}
		if namespace.IsNull(row) || namespace.Value(row) != object.Namespace || tableName.IsNull(row) || tableName.Value(row) != object.Table {
			return fmt.Errorf("%w: source relation differs at artifact %s row %d", connector.ErrDeliveryConflict, object.ArtifactID, row)
		}
		if operation.IsNull(row) {
			return fmt.Errorf("%w: missing operation at artifact %s row %d", connector.ErrDeliveryConflict, object.ArtifactID, row)
		}
		switch connector.Operation(operation.Value(row)) {
		case connector.OpInsert, connector.OpUpdate, connector.OpDelete, connector.OpLoad:
		default:
			return fmt.Errorf("unsupported changelog operation %q", operation.Value(row))
		}
	}
	return nil
}

func stringColumn(record arrow.RecordBatch, name string) (*array.String, error) {
	indices := record.Schema().FieldIndices(name)
	if len(indices) != 1 {
		return nil, fmt.Errorf("canonical field %q is missing or ambiguous", name)
	}
	column, ok := record.Column(indices[0]).(*array.String)
	if !ok {
		return nil, fmt.Errorf("canonical field %q has type %s, expected string", name, record.Column(indices[0]).DataType())
	}
	return column, nil
}

func int64Column(record arrow.RecordBatch, name string) (*array.Int64, error) {
	indices := record.Schema().FieldIndices(name)
	if len(indices) != 1 {
		return nil, fmt.Errorf("canonical field %q is missing or ambiguous", name)
	}
	column, ok := record.Column(indices[0]).(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("canonical field %q has type %s, expected int64", name, record.Column(indices[0]).DataType())
	}
	return column, nil
}

func projectBarriers(request artifactlog.CommitRequest, cfg Config) (*projectionGroup, error) {
	target, err := cfg.controlTarget()
	if err != nil {
		return nil, err
	}
	fields := []arrow.Field{
		fieldWithID("record_ordinal", arrow.PrimitiveTypes.Int64, false, 1),
		fieldWithID("kind", arrow.BinaryTypes.String, false, 2),
		fieldWithID("source_namespace", arrow.BinaryTypes.String, false, 3),
		fieldWithID("source_table", arrow.BinaryTypes.String, false, 4),
		fieldWithID("schema_fingerprint", arrow.BinaryTypes.String, true, 5),
		fieldWithID("ddl", arrow.BinaryTypes.String, true, 6),
		fieldWithID("ddl_plan", arrow.BinaryTypes.Binary, true, 7),
		fieldWithID("content_hash", arrow.BinaryTypes.String, false, 8),
		fieldWithID("logical_batch_id", arrow.BinaryTypes.String, false, 9),
		fieldWithID("publication_id", arrow.BinaryTypes.String, false, 10),
	}
	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()
	barriers := append([]artifactlog.Barrier(nil), request.Barriers...)
	sort.Slice(barriers, func(i, j int) bool { return barriers[i].RecordOrdinal < barriers[j].RecordOrdinal })
	for _, barrier := range barriers {
		builder.Field(0).(*array.Int64Builder).Append(int64(barrier.RecordOrdinal)) // #nosec G115 -- bounded by canonical planner.
		builder.Field(1).(*array.StringBuilder).Append(barrier.Kind)
		builder.Field(2).(*array.StringBuilder).Append(barrier.Namespace)
		builder.Field(3).(*array.StringBuilder).Append(barrier.Table)
		appendNullableString(builder.Field(4).(*array.StringBuilder), barrier.SchemaID)
		appendNullableString(builder.Field(5).(*array.StringBuilder), barrier.DDL)
		if len(barrier.DDLPlan) == 0 {
			builder.Field(6).AppendNull()
		} else {
			builder.Field(6).(*array.BinaryBuilder).Append(barrier.DDLPlan)
		}
		builder.Field(7).(*array.StringBuilder).Append(barrier.ContentHash)
		builder.Field(8).(*array.StringBuilder).Append(request.LogicalBatchID)
		builder.Field(9).(*array.StringBuilder).Append(request.PublicationID.String())
	}
	record := builder.NewRecordBatch()
	icebergSchema, err := table.ArrowSchemaToIceberg(schema, false, nil)
	if err != nil {
		record.Release()
		return nil, err
	}
	controlIdentities := make(map[string]string, len(fields))
	for _, field := range fields {
		controlIdentities[field.Name] = "name:" + field.Name
	}
	icebergSchema, err = schemaWithIdentityDocs(icebergSchema, controlIdentities)
	if err != nil {
		record.Release()
		return nil, err
	}
	firstOrdinal := uint64(0)
	if len(barriers) > 0 {
		firstOrdinal = barriers[0].RecordOrdinal
	}
	return &projectionGroup{
		id: projectionGroupID(target, controlSchemaFingerprint, true), target: target,
		schemaFingerprint: controlSchemaFingerprint, schema: icebergSchema,
		records: []arrow.RecordBatch{record}, firstOrdinal: firstOrdinal, barrier: true,
	}, nil
}

func fieldWithID(name string, dataType arrow.DataType, nullable bool, id int) arrow.Field {
	return arrow.Field{Name: name, Type: dataType, Nullable: nullable, Metadata: arrow.MetadataFrom(map[string]string{
		"PARQUET:field_id": strconv.Itoa(id),
	})}
}

func appendNullableString(builder *array.StringBuilder, value string) {
	if value == "" {
		builder.AppendNull()
		return
	}
	builder.Append(value)
}

func projectionGroupID(target table.Identifier, schemaFingerprint string, barrier bool) string {
	kind := "data"
	if barrier {
		kind = "barrier"
	}
	digest := sha256.Sum256([]byte(strings.Join(append([]string{kind}, append(target, schemaFingerprint)...), "\x00")))
	return kind + ":" + hex.EncodeToString(digest[:])
}
