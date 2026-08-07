package tablemap

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strings"

	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/mappingtemplate"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

type Projector struct {
	mapping               flow.DestinationTableMappings
	fingerprint           string
	futureTableTemplates  compiledTableTemplates
	futureColumnTemplates map[string]mappingtemplate.Template
}

type compiledTableTemplates struct {
	targetSchema mappingtemplate.Template
	targetTable  mappingtemplate.Template
	targetColumn mappingtemplate.Template
}

var _ stream.Projector = (*Projector)(nil)
var _ connector.ManagedBootstrapProjector = (*Projector)(nil)

var errRecordKeyNotFound = errors.New("record key not found")

type resolvedColumn struct {
	source string
	target string
}

type resolvedTable struct {
	included             bool
	sourceSchema         string
	sourceTable          string
	targetSchema         string
	targetTable          string
	columns              []resolvedColumn
	bySource             map[string]string
	write                flow.TableWritePolicy
	futureColumns        flow.FutureColumnMapping
	futureColumnTemplate mappingtemplate.Template
	exactColumns         map[string]flow.ColumnMapping
	nonidentity          bool
}

func New(policy flow.TableMappings, destination string) (*Projector, error) {
	mapping, ok := policy.ForDestination(destination)
	if !ok {
		return nil, fmt.Errorf("destination %q has no table mappings", destination)
	}
	// The projector is immutable after construction even if the caller mutates
	// its policy object or any nested slices.
	mapping = (flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: []flow.DestinationTableMappings{mapping}}).Clone().Destinations[0]
	fingerprint, err := policy.Fingerprint()
	if err != nil {
		return nil, err
	}
	projector := &Projector{
		mapping:               mapping,
		fingerprint:           fingerprint,
		futureColumnTemplates: make(map[string]mappingtemplate.Template),
	}
	if mapping.FutureTables.Action == flow.MappingActionInclude {
		projector.futureTableTemplates.targetSchema, err = mappingtemplate.Parse(mapping.FutureTables.TargetSchema, mappingtemplate.Schema)
		if err != nil {
			return nil, fmt.Errorf("compile future target_schema: %w", err)
		}
		projector.futureTableTemplates.targetTable, err = mappingtemplate.Parse(mapping.FutureTables.TargetTable, mappingtemplate.Table)
		if err != nil {
			return nil, fmt.Errorf("compile future target_table: %w", err)
		}
		if mapping.FutureTables.FutureColumns.Action == flow.MappingActionInclude {
			projector.futureTableTemplates.targetColumn, err = mappingtemplate.Parse(mapping.FutureTables.FutureColumns.TargetColumn, mappingtemplate.Column)
			if err != nil {
				return nil, fmt.Errorf("compile future target_column: %w", err)
			}
		}
	}
	for _, table := range mapping.Tables {
		if table.Action != flow.MappingActionInclude || table.FutureColumns.Action != flow.MappingActionInclude {
			continue
		}
		compiled, err := mappingtemplate.Parse(table.FutureColumns.TargetColumn, mappingtemplate.Column)
		if err != nil {
			return nil, fmt.Errorf("compile table %s.%s future target_column: %w", table.SourceSchema, table.SourceTable, err)
		}
		projector.futureColumnTemplates[table.SourceSchema+"\x00"+table.SourceTable] = compiled
	}
	return projector, nil
}

func (p *Projector) Fingerprint() string { return p.fingerprint }

func (p *Projector) IncludeBootstrapRelation(namespace, table string) (bool, error) {
	resolved, err := p.resolve(connector.Schema{Namespace: namespace, Name: table}, true)
	if err != nil {
		return false, err
	}
	return resolved.included, nil
}

func (p *Projector) ProjectBootstrapSchema(schema connector.Schema) (connector.Schema, connector.TableWritePolicy, bool, error) {
	resolved, err := p.resolve(schema, false)
	if err != nil {
		return connector.Schema{}, connector.TableWritePolicy{}, false, err
	}
	if !resolved.included {
		return connector.Schema{}, connector.TableWritePolicy{}, false, nil
	}
	mapped := projectSchema(schema, resolved)
	keys := make([]string, 0, len(resolved.write.KeyColumns))
	for _, key := range resolved.write.KeyColumns {
		keys = append(keys, resolved.bySource[key])
	}
	watermark := ""
	if resolved.write.WatermarkColumn != "" {
		watermark = resolved.bySource[resolved.write.WatermarkColumn]
	}
	policy := connector.TableWritePolicy{Mode: connector.ResolvedWriteMode(resolved.write.Mode), KeyColumns: keys, WatermarkColumn: watermark, ProjectionFingerprint: p.fingerprint}
	return mapped, policy, true, nil
}

func (p *Projector) ProjectBootstrapBatch(batch connector.Batch) (connector.Batch, bool, error) {
	mapped, decision, err := p.ProjectBatch(batch)
	return mapped, decision == stream.ProjectionIncluded, err
}

func (p *Projector) ProjectBatch(batch connector.Batch) (connector.Batch, stream.ProjectionDecision, error) {
	if len(batch.Records) > 0 {
		if err := connector.ValidateBatch(batch); err != nil {
			return connector.Batch{}, stream.ProjectionFiltered, fmt.Errorf("validate batch before projection: %w", err)
		}
	}
	position := strings.TrimSpace(batch.Checkpoint.LSN)
	if position == "" && len(batch.Checkpoint.Metadata) > 0 {
		id, err := connector.CheckpointPositionID(batch.Checkpoint)
		if err != nil {
			return connector.Batch{}, stream.ProjectionFiltered, fmt.Errorf("derive append checkpoint position: %w", err)
		}
		hexValue := strings.TrimPrefix(id, "checkpoint:")
		value, ok := new(big.Int).SetString(hexValue, 16)
		if !ok {
			return connector.Batch{}, stream.ProjectionFiltered, fmt.Errorf("derive append checkpoint position: invalid identity %q", id)
		}
		position = value.String()
	}
	return p.projectBatch(batch, position)
}

func (p *Projector) projectBatch(batch connector.Batch, fallbackPosition string) (connector.Batch, stream.ProjectionDecision, error) {
	if batch.Schema.Name == "" {
		return p.projectTablelessDDLBatch(batch)
	}
	resolved, err := p.resolve(batch.Schema, false)
	if err != nil {
		return connector.Batch{}, stream.ProjectionFiltered, err
	}
	if !resolved.included {
		return connector.Batch{Checkpoint: batch.Checkpoint, WireFormat: batch.WireFormat}, stream.ProjectionFiltered, nil
	}
	mappedSchema := projectSchema(batch.Schema, resolved)
	mappedRecords := make([]connector.Record, 0, len(batch.Records))
	for _, record := range batch.Records {
		records, err := p.projectRecord(batch.Schema, mappedSchema, resolved, record, fallbackPosition)
		if err != nil {
			return connector.Batch{}, stream.ProjectionFiltered, err
		}
		mappedRecords = append(mappedRecords, records...)
	}
	if len(mappedRecords) == 0 {
		return connector.Batch{Schema: mappedSchema, Checkpoint: batch.Checkpoint, WireFormat: batch.WireFormat}, stream.ProjectionFiltered, nil
	}
	keyColumns := make([]string, 0, len(resolved.write.KeyColumns))
	for _, source := range resolved.write.KeyColumns {
		target, ok := resolved.bySource[source]
		if !ok {
			return connector.Batch{}, stream.ProjectionFiltered, fmt.Errorf("resolved key column %q is not included", source)
		}
		keyColumns = append(keyColumns, target)
	}
	watermark := ""
	if resolved.write.WatermarkColumn != "" {
		watermark = resolved.bySource[resolved.write.WatermarkColumn]
	}
	mode := connector.ResolvedWriteMode(resolved.write.Mode)
	return connector.Batch{
		Records: mappedRecords, Schema: mappedSchema, Checkpoint: batch.Checkpoint, WireFormat: batch.WireFormat,
		WritePolicy: connector.TableWritePolicy{Mode: mode, KeyColumns: keyColumns, WatermarkColumn: watermark, ProjectionFingerprint: p.fingerprint},
	}, stream.ProjectionIncluded, nil
}

func (p *Projector) projectTablelessDDLBatch(batch connector.Batch) (connector.Batch, stream.ProjectionDecision, error) {
	out := connector.Batch{Checkpoint: batch.Checkpoint, WireFormat: batch.WireFormat, WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend, ProjectionFingerprint: p.fingerprint}}
	var mappedRecords []connector.Record
	for index, record := range batch.Records {
		source, err := tablelessDDLSourceSchema(record)
		if err != nil {
			return connector.Batch{}, stream.ProjectionFiltered, fmt.Errorf("resolve tableless DDL record %d: %w", index, err)
		}
		resolved, err := p.resolve(source, true)
		if err != nil {
			return connector.Batch{}, stream.ProjectionFiltered, err
		}
		if !resolved.included {
			continue
		}
		target := connector.Schema{Name: resolved.targetTable, Namespace: resolved.targetSchema}
		mapped, included, err := projectDDLRecord(source, target, resolved, record)
		if err != nil {
			return connector.Batch{}, stream.ProjectionFiltered, err
		}
		if included {
			// The structured plan owns the target relation. Keeping Record.Table
			// empty preserves the valid tableless-control batch contract when one
			// batch contains multiple projected relations.
			mapped.Table = ""
			mappedRecords = append(mappedRecords, mapped)
		}
	}
	if len(mappedRecords) == 0 {
		return out, stream.ProjectionFiltered, nil
	}
	out.Records = mappedRecords
	return out, stream.ProjectionIncluded, nil
}

func tablelessDDLSourceSchema(record connector.Record) (connector.Schema, error) {
	if len(record.DDLPlan) == 0 {
		return connector.Schema{}, errors.New("tableless raw SQL DDL is ambiguous")
	}
	var plan internalschema.Plan
	if err := json.Unmarshal(record.DDLPlan, &plan); err != nil {
		return connector.Schema{}, fmt.Errorf("decode structured DDL plan: %w", err)
	}
	if len(plan.Changes) == 0 {
		return connector.Schema{}, errors.New("structured DDL plan has no changes")
	}
	var namespace, table string
	for _, change := range plan.Changes {
		if change.Namespace == "" || change.Table == "" {
			return connector.Schema{}, errors.New("tableless structured DDL change requires source namespace and table")
		}
		if namespace == "" {
			namespace, table = change.Namespace, change.Table
			continue
		}
		if namespace != change.Namespace || table != change.Table {
			return connector.Schema{}, errors.New("one structured DDL plan cannot span multiple source relations")
		}
	}
	return connector.Schema{Name: table, Namespace: namespace}, nil
}

func (p *Projector) ProjectTransaction(transaction connector.SourceTransaction) (connector.SourceTransaction, stream.ProjectionDecision, error) {
	if err := transaction.Validate(); err != nil {
		return connector.SourceTransaction{}, stream.ProjectionFiltered, fmt.Errorf("validate transaction before projection: %w", err)
	}
	out := transaction
	out.Fragments = make([]connector.TransactionFragment, 0, len(transaction.Fragments))
	fallback := strings.TrimSpace(transaction.Checkpoint.LSN)
	for _, fragment := range transaction.Fragments {
		batch, decision, err := p.projectBatch(fragment.Batch, fallback)
		if err != nil {
			return connector.SourceTransaction{}, stream.ProjectionFiltered, fmt.Errorf("project fragment %d: %w", fragment.Ordinal, err)
		}
		if decision == stream.ProjectionFiltered {
			continue
		}
		out.Fragments = append(out.Fragments, connector.TransactionFragment{Ordinal: uint64(len(out.Fragments)), Batch: batch})
	}
	if len(out.Fragments) == 0 {
		return out, stream.ProjectionFiltered, nil
	}
	return out, stream.ProjectionIncluded, nil
}

func (p *Projector) resolve(schema connector.Schema, allowEmptyColumns bool) (resolvedTable, error) {
	var exact *flow.TableMapping
	for index := range p.mapping.Tables {
		candidate := &p.mapping.Tables[index]
		if candidate.SourceSchema == schema.Namespace && candidate.SourceTable == schema.Name {
			exact = candidate
			break
		}
	}
	resolved := resolvedTable{sourceSchema: schema.Namespace, sourceTable: schema.Name, bySource: make(map[string]string), exactColumns: make(map[string]flow.ColumnMapping)}
	var futureColumns flow.FutureColumnMapping
	if exact == nil {
		future := p.mapping.FutureTables
		if future.Action == flow.MappingActionExclude {
			return resolved, nil
		}
		resolved.included = true
		data := mappingtemplate.Data{Schema: schema.Namespace, Table: schema.Name}
		var err error
		resolved.targetSchema, err = p.futureTableTemplates.targetSchema.Expand(data)
		if err != nil {
			return resolvedTable{}, fmt.Errorf("expand future target_schema for %s.%s: %w", schema.Namespace, schema.Name, err)
		}
		resolved.targetTable, err = p.futureTableTemplates.targetTable.Expand(data)
		if err != nil {
			return resolvedTable{}, fmt.Errorf("expand future target_table for %s.%s: %w", schema.Namespace, schema.Name, err)
		}
		resolved.write = future.Write
		futureColumns = future.FutureColumns
		resolved.futureColumnTemplate = p.futureTableTemplates.targetColumn
	} else {
		if exact.Action == flow.MappingActionExclude {
			return resolved, nil
		}
		resolved.included = true
		resolved.targetSchema = exact.TargetSchema
		resolved.targetTable = exact.TargetTable
		resolved.write = exact.Write
		futureColumns = exact.FutureColumns
		resolved.futureColumnTemplate = p.futureColumnTemplates[schema.Namespace+"\x00"+schema.Name]
		for _, column := range exact.Columns {
			resolved.exactColumns[column.SourceColumn] = column
		}
	}
	resolved.futureColumns = futureColumns
	if resolved.targetTable == "" {
		return resolvedTable{}, errors.New("projected target table is empty")
	}
	seenTargets := make(map[string]string, len(schema.Columns))
	shapeChanged := resolved.targetSchema != schema.Namespace || resolved.targetTable != schema.Name
	for _, column := range schema.Columns {
		action := futureColumns.Action
		target := ""
		if action == flow.MappingActionInclude {
			var err error
			target, err = resolved.futureColumnTemplate.Expand(mappingtemplate.Data{Schema: schema.Namespace, Table: schema.Name, Column: column.Name})
			if err != nil {
				return resolvedTable{}, fmt.Errorf("expand future target_column for %s.%s.%s: %w", schema.Namespace, schema.Name, column.Name, err)
			}
		}
		if exact != nil {
			for _, configured := range exact.Columns {
				if configured.SourceColumn == column.Name {
					action = configured.Action
					target = configured.TargetColumn
					break
				}
			}
		}
		if action == flow.MappingActionExclude {
			shapeChanged = true
			continue
		}
		if target == "" {
			return resolvedTable{}, fmt.Errorf("column %q resolves to an empty target", column.Name)
		}
		if prior, collision := seenTargets[target]; collision {
			return resolvedTable{}, fmt.Errorf("source columns %q and %q resolve to target column %q", prior, column.Name, target)
		}
		seenTargets[target] = column.Name
		resolved.columns = append(resolved.columns, resolvedColumn{source: column.Name, target: target})
		resolved.bySource[column.Name] = target
		if target != column.Name {
			shapeChanged = true
		}
	}
	if len(resolved.columns) == 0 && !allowEmptyColumns {
		return resolvedTable{}, fmt.Errorf("included table %s.%s has no included columns", schema.Namespace, schema.Name)
	}
	if resolved.write.Mode == flow.TableWriteModeUpsert && !allowEmptyColumns {
		for _, keyColumn := range resolved.write.KeyColumns {
			if _, included := resolved.bySource[keyColumn]; !included {
				return resolvedTable{}, fmt.Errorf("configured key column %q is absent from the schema or projection", keyColumn)
			}
			oldImageAvailable := false
			for _, column := range schema.Columns {
				if column.Name == keyColumn {
					oldImageAvailable = column.TypeMetadata["replica_identity"] == "true"
					break
				}
			}
			if !oldImageAvailable {
				return resolvedTable{}, fmt.Errorf("configured upsert key column %q must be part of PostgreSQL replica identity or a full old-row image", keyColumn)
			}
		}
	}
	if resolved.write.WatermarkColumn != "" && !allowEmptyColumns {
		if _, included := resolved.bySource[resolved.write.WatermarkColumn]; !included {
			return resolvedTable{}, fmt.Errorf("configured watermark column %q is absent from the schema or projection", resolved.write.WatermarkColumn)
		}
		for _, column := range schema.Columns {
			if resolved.write.Mode != flow.TableWriteModeUpsert || column.Name != resolved.write.WatermarkColumn {
				continue
			}
			if column.Nullable {
				return resolvedTable{}, fmt.Errorf("configured watermark column %q must be non-nullable", resolved.write.WatermarkColumn)
			}
			if column.TypeMetadata["replica_identity"] != "true" {
				return resolvedTable{}, fmt.Errorf("configured watermark column %q must be part of PostgreSQL replica identity or a full old-row image", resolved.write.WatermarkColumn)
			}
		}
	}
	if shapeChanged {
		for _, column := range schema.Columns {
			if column.Generated && strings.TrimSpace(column.Expression) != "" {
				if _, included := resolved.bySource[column.Name]; included {
					return resolvedTable{}, fmt.Errorf("generated column %q expression cannot be projected after a table or column shape change", column.Name)
				}
			}
		}
	}
	for _, reserved := range []string{connector.AppendOperationColumn, connector.AppendDeletedColumn, connector.AppendSourcePositionColumn} {
		if _, collision := seenTargets[reserved]; collision && resolved.write.Mode == flow.TableWriteModeAppend {
			return resolvedTable{}, fmt.Errorf("append metadata column %q collides with a projected source column", reserved)
		}
	}
	resolved.nonidentity = shapeChanged || resolved.write.Mode == flow.TableWriteModeAppend
	return resolved, nil
}

func projectSchema(schema connector.Schema, resolved resolvedTable) connector.Schema {
	out := schema
	out.Namespace = resolved.targetSchema
	out.Name = resolved.targetTable
	out.Columns = make([]connector.Column, 0, len(resolved.columns)+3)
	out.QuotedIdentifiers = make(map[string]bool)
	byName := make(map[string]connector.Column, len(schema.Columns))
	for _, column := range schema.Columns {
		byName[column.Name] = column
	}
	for _, mapping := range resolved.columns {
		column := byName[mapping.source]
		column.Name = mapping.target
		if column.TypeMetadata != nil {
			metadata := make(map[string]string, len(column.TypeMetadata))
			for key, value := range column.TypeMetadata {
				if resolved.write.Mode == flow.TableWriteModeAppend && (key == "primary_key" || key == "primary_key_ordinal" || key == "replica_identity") {
					continue
				}
				metadata[key] = value
			}
			if len(metadata) == 0 {
				column.TypeMetadata = nil
			} else {
				column.TypeMetadata = metadata
			}
		}
		out.Columns = append(out.Columns, column)
		if schema.QuotedIdentifiers[mapping.source] {
			out.QuotedIdentifiers[mapping.target] = true
		}
	}
	if resolved.write.Mode == flow.TableWriteModeAppend {
		sourceRelation := schema.Namespace + "\x00" + schema.Name
		for _, column := range schema.Columns {
			if relation := strings.TrimSpace(column.TypeMetadata["source_relation_id"]); relation != "" {
				sourceRelation = "relation:" + relation
				break
			}
		}
		metadata := func(identity string) map[string]string {
			return map[string]string{"wallaby.synthetic_identity": identity, "wallaby.synthetic_source_relation": sourceRelation}
		}
		out.Columns = append(out.Columns,
			connector.Column{Name: connector.AppendOperationColumn, Type: "text", Nullable: false, TypeMetadata: metadata("append.operation.v1")},
			connector.Column{Name: connector.AppendDeletedColumn, Type: "boolean", Nullable: false, TypeMetadata: metadata("append.deleted.v1")},
			connector.Column{Name: connector.AppendSourcePositionColumn, Type: "text", Nullable: false, TypeMetadata: metadata("append.source_position.v1")},
		)
	}
	if len(out.QuotedIdentifiers) == 0 {
		out.QuotedIdentifiers = nil
	}
	return out
}

func (p *Projector) projectRecord(sourceSchema, targetSchema connector.Schema, resolved resolvedTable, record connector.Record, fallbackPosition string) ([]connector.Record, error) {
	if record.Operation == connector.OpDDL || record.DDL != "" || len(record.DDLPlan) > 0 {
		mapped, included, err := projectDDLRecord(sourceSchema, targetSchema, resolved, record)
		if err != nil || !included {
			return nil, err
		}
		return []connector.Record{mapped}, nil
	}
	keyChanged := false
	if resolved.write.Mode == flow.TableWriteModeUpsert && record.Operation == connector.OpUpdate {
		var err error
		keyChanged, err = keysChanged(record, resolved.write.KeyColumns)
		if err != nil {
			return nil, err
		}
	}
	if keyChanged {
		oldRecord := record
		oldRecord.Operation = connector.OpDelete
		oldRecord.After = nil
		newRecord := record
		newRecord.Operation = connector.OpInsert
		newRecord.Before = nil
		first, err := p.projectDataRecord(resolved, oldRecord, fallbackPosition, true)
		if err != nil {
			return nil, err
		}
		second, err := p.projectDataRecord(resolved, newRecord, fallbackPosition, false)
		if err != nil {
			return nil, err
		}
		return []connector.Record{first, second}, nil
	}
	mapped, err := p.projectDataRecord(resolved, record, fallbackPosition, record.Operation == connector.OpDelete)
	if err != nil {
		return nil, err
	}
	return []connector.Record{mapped}, nil
}

func (p *Projector) projectDataRecord(resolved resolvedTable, record connector.Record, fallbackPosition string, useOldKey bool) (connector.Record, error) {
	out := record
	out.Table = resolved.targetTable
	out.Before = projectImage(record.Before, resolved)
	out.After = projectImage(record.After, resolved)
	out.Unchanged = projectUnchanged(record.Unchanged, resolved)
	if resolved.nonidentity {
		out.Payload = nil
	}
	originalKey, err := decodeKey(record.Key)
	if errors.Is(err, errRecordKeyNotFound) {
		originalKey = map[string]any{}
	} else if err != nil {
		return connector.Record{}, err
	}
	if resolved.write.Mode == flow.TableWriteModeUpsert {
		key, err := resolveKey(record, originalKey, resolved, useOldKey)
		if err != nil {
			return connector.Record{}, err
		}
		out.Key, err = json.Marshal(key)
		if err != nil {
			return connector.Record{}, fmt.Errorf("encode projected record key: %w", err)
		}
		if resolved.write.WatermarkColumn != "" {
			position := strings.TrimSpace(record.SourcePosition)
			if position == "" {
				position = strings.TrimSpace(fallbackPosition)
			}
			if position == "" {
				return connector.Record{}, errors.New("watermark projection requires a stable PostgreSQL source position")
			}
			position, err = connector.CanonicalizeCheckpointPosition(position)
			if err != nil {
				return connector.Record{}, fmt.Errorf("canonicalize watermark source position: %w", err)
			}
			if !strings.Contains(position, "/") {
				return connector.Record{}, errors.New("watermark projection requires a canonical PostgreSQL LSN")
			}
			out.SourcePosition = position
			image := record.After
			if record.Operation == connector.OpDelete || useOldKey {
				image = record.Before
			}
			value, ok := image[resolved.write.WatermarkColumn]
			if !ok || value == nil {
				return connector.Record{}, fmt.Errorf("record %s requires non-null watermark column %q", record.Operation, resolved.write.WatermarkColumn)
			}
		}
	} else {
		out.Key, err = projectKey(originalKey, resolved)
		if err != nil {
			return connector.Record{}, err
		}
		position := strings.TrimSpace(record.SourcePosition)
		if position == "" {
			position = strings.TrimSpace(fallbackPosition)
		}
		if position == "" {
			return connector.Record{}, errors.New("append projection requires a stable source position")
		}
		position, err = connector.CanonicalizeCheckpointPosition(position)
		if err != nil {
			return connector.Record{}, fmt.Errorf("canonicalize append source position: %w", err)
		}
		originalOperation := record.Operation
		deleted := originalOperation == connector.OpDelete
		if deleted {
			out.After = projectImage(record.Before, resolved)
		}
		if out.After == nil {
			out.After = make(map[string]any)
		}
		out.After[connector.AppendOperationColumn] = string(originalOperation)
		out.After[connector.AppendDeletedColumn] = deleted
		out.After[connector.AppendSourcePositionColumn] = position
		out.SourcePosition = position
		out.Operation = connector.OpInsert
	}
	return out, nil
}

func projectImage(image map[string]any, resolved resolvedTable) map[string]any {
	if image == nil {
		return nil
	}
	out := make(map[string]any, len(resolved.columns))
	for _, column := range resolved.columns {
		if value, ok := image[column.source]; ok {
			out[column.target] = value
		}
	}
	return out
}

func projectUnchanged(columns []string, resolved resolvedTable) []string {
	if columns == nil {
		return nil
	}
	out := make([]string, 0, len(columns))
	for _, column := range columns {
		if target, ok := resolved.bySource[column]; ok {
			out = append(out, target)
		}
	}
	return out
}

func decodeKey(raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return nil, errRecordKeyNotFound
	}
	var key map[string]any
	if err := json.Unmarshal(raw, &key); err != nil {
		return nil, fmt.Errorf("decode record key for projection: %w", err)
	}
	return key, nil
}

func projectKey(key map[string]any, resolved resolvedTable) ([]byte, error) {
	if len(key) == 0 {
		return nil, nil
	}
	mapped := make(map[string]any)
	for source, value := range key {
		if target, ok := resolved.bySource[source]; ok {
			mapped[target] = value
		}
	}
	if len(mapped) == 0 {
		return nil, nil
	}
	encoded, err := json.Marshal(mapped)
	if err != nil {
		return nil, fmt.Errorf("encode projected key: %w", err)
	}
	return encoded, nil
}

func resolveKey(record connector.Record, original map[string]any, resolved resolvedTable, useOld bool) (map[string]any, error) {
	key := make(map[string]any, len(resolved.write.KeyColumns))
	for _, source := range resolved.write.KeyColumns {
		var value any
		var ok bool
		if useOld {
			value, ok = record.Before[source]
		} else {
			value, ok = record.After[source]
			if !ok {
				value, ok = record.Before[source]
			}
		}
		if !ok {
			value, ok = original[source]
		}
		if !ok || value == nil {
			return nil, fmt.Errorf("record %s lacks non-null key column %q", record.Operation, source)
		}
		key[resolved.bySource[source]] = value
	}
	return key, nil
}

func keysChanged(record connector.Record, columns []string) (bool, error) {
	if len(columns) == 0 {
		return false, nil
	}
	original, err := decodeKey(record.Key)
	if errors.Is(err, errRecordKeyNotFound) {
		original = map[string]any{}
	} else if err != nil {
		return false, err
	}
	for _, column := range columns {
		before, beforeOK := record.Before[column]
		if !beforeOK {
			before, beforeOK = original[column]
		}
		if !beforeOK || before == nil {
			return false, fmt.Errorf("configured-key update cannot reconstruct old match column %q", column)
		}
		after, afterOK := record.After[column]
		if !afterOK {
			after, afterOK = before, beforeOK
		}
		if beforeOK && afterOK && !logicalValuesEqual(before, after) {
			return true, nil
		}
	}
	return false, nil
}

func logicalValuesEqual(left, right any) bool {
	if reflect.DeepEqual(left, right) {
		return true
	}
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}
