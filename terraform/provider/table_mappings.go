package main

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	flowmodel "github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type tableMappingsModel struct {
	Version      types.Int64 `tfsdk:"version"`
	Destinations types.List  `tfsdk:"destinations"`
}

type destinationTableMappingsModel struct {
	Destination  types.String `tfsdk:"destination"`
	FutureTables types.Object `tfsdk:"future_tables"`
	Tables       types.List   `tfsdk:"tables"`
}

type futureTableMappingModel struct {
	Action        types.String `tfsdk:"action"`
	TargetSchema  types.String `tfsdk:"target_schema"`
	TargetTable   types.String `tfsdk:"target_table"`
	FutureColumns types.Object `tfsdk:"future_columns"`
	Write         types.Object `tfsdk:"write"`
}

type exactTableMappingModel struct {
	SourceSchema  types.String `tfsdk:"source_schema"`
	SourceTable   types.String `tfsdk:"source_table"`
	Action        types.String `tfsdk:"action"`
	TargetSchema  types.String `tfsdk:"target_schema"`
	TargetTable   types.String `tfsdk:"target_table"`
	FutureColumns types.Object `tfsdk:"future_columns"`
	Columns       types.List   `tfsdk:"columns"`
	Write         types.Object `tfsdk:"write"`
}

type futureColumnMappingModel struct {
	Action       types.String `tfsdk:"action"`
	TargetColumn types.String `tfsdk:"target_column"`
}

type columnMappingModel struct {
	SourceColumn types.String `tfsdk:"source_column"`
	Action       types.String `tfsdk:"action"`
	TargetColumn types.String `tfsdk:"target_column"`
}

type tableWritePolicyModel struct {
	Mode            types.String `tfsdk:"mode"`
	KeyColumns      types.List   `tfsdk:"key_columns"`
	WatermarkColumn types.String `tfsdk:"watermark_column"`
}

func futureColumnAttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{"action": types.StringType, "target_column": types.StringType}
}

func writeAttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{"mode": types.StringType, "key_columns": types.ListType{ElemType: types.StringType}, "watermark_column": types.StringType}
}

func columnAttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{"source_column": types.StringType, "action": types.StringType, "target_column": types.StringType}
}

func futureTableAttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"action": types.StringType, "target_schema": types.StringType, "target_table": types.StringType,
		"future_columns": types.ObjectType{AttrTypes: futureColumnAttributeTypes()},
		"write":          types.ObjectType{AttrTypes: writeAttributeTypes()},
	}
}

func exactTableAttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"source_schema": types.StringType, "source_table": types.StringType, "action": types.StringType,
		"target_schema": types.StringType, "target_table": types.StringType,
		"future_columns": types.ObjectType{AttrTypes: futureColumnAttributeTypes()},
		"columns":        types.ListType{ElemType: types.ObjectType{AttrTypes: columnAttributeTypes()}},
		"write":          types.ObjectType{AttrTypes: writeAttributeTypes()},
	}
}

func destinationMappingAttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"destination":   types.StringType,
		"future_tables": types.ObjectType{AttrTypes: futureTableAttributeTypes()},
		"tables":        types.ListType{ElemType: types.ObjectType{AttrTypes: exactTableAttributeTypes()}},
	}
}

func tableMappingsAttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"version":      types.Int64Type,
		"destinations": types.ListType{ElemType: types.ObjectType{AttrTypes: destinationMappingAttributeTypes()}},
	}
}

func tableMappingsSchema() schema.SingleNestedAttribute {
	futureColumn := func(required bool) schema.SingleNestedAttribute {
		attribute := schema.SingleNestedAttribute{Attributes: map[string]schema.Attribute{
			"action":        schema.StringAttribute{Required: true},
			"target_column": schema.StringAttribute{Optional: true},
		}}
		attribute.Required = required
		attribute.Optional = !required
		return attribute
	}
	write := func(required bool) schema.SingleNestedAttribute {
		attribute := schema.SingleNestedAttribute{Attributes: map[string]schema.Attribute{
			"mode":             schema.StringAttribute{Required: true},
			"key_columns":      schema.ListAttribute{Required: true, ElementType: types.StringType},
			"watermark_column": schema.StringAttribute{Optional: true},
		}}
		attribute.Required = required
		attribute.Optional = !required
		return attribute
	}
	return schema.SingleNestedAttribute{
		Required: true,
		Attributes: map[string]schema.Attribute{
			"version": schema.Int64Attribute{Required: true},
			"destinations": schema.ListNestedAttribute{
				Required: true,
				NestedObject: schema.NestedAttributeObject{Attributes: map[string]schema.Attribute{
					"destination": schema.StringAttribute{Required: true},
					"future_tables": schema.SingleNestedAttribute{Required: true, Attributes: map[string]schema.Attribute{
						"action":         schema.StringAttribute{Required: true},
						"target_schema":  schema.StringAttribute{Optional: true},
						"target_table":   schema.StringAttribute{Optional: true},
						"future_columns": futureColumn(false),
						"write":          write(false),
					}},
					"tables": schema.ListNestedAttribute{Required: true, NestedObject: schema.NestedAttributeObject{Attributes: map[string]schema.Attribute{
						"source_schema":  schema.StringAttribute{Required: true},
						"source_table":   schema.StringAttribute{Required: true},
						"action":         schema.StringAttribute{Required: true},
						"target_schema":  schema.StringAttribute{Optional: true},
						"target_table":   schema.StringAttribute{Optional: true},
						"future_columns": futureColumn(false),
						"columns": schema.ListNestedAttribute{Required: true, NestedObject: schema.NestedAttributeObject{Attributes: map[string]schema.Attribute{
							"source_column": schema.StringAttribute{Required: true},
							"action":        schema.StringAttribute{Required: true},
							"target_column": schema.StringAttribute{Optional: true},
						}}},
						"write": write(false),
					}}},
				}},
			},
		},
	}
}

// tableMappingsModelToInternal decodes framework-native values only after their
// null and unknown states are checked. Validation calls it with deferUnknown so
// values computed later in planning do not produce Config.Get conversion errors.
func tableMappingsModelToInternal(ctx context.Context, value types.Object, deferUnknown bool) (flowmodel.TableMappings, bool, diag.Diagnostics) {
	var diagnostics diag.Diagnostics
	if value.IsNull() {
		diagnostics.AddError("Missing table mappings", "config.table_mappings is required")
		return flowmodel.TableMappings{}, false, diagnostics
	}
	if value.IsUnknown() {
		return flowmodel.TableMappings{}, handleUnknown(deferUnknown, "Unknown table mappings", "config.table_mappings must be known before apply", &diagnostics), diagnostics
	}
	var model tableMappingsModel
	diagnostics.Append(value.As(ctx, &model, basetypes.ObjectAsOptions{})...)
	if diagnostics.HasError() {
		return flowmodel.TableMappings{}, false, diagnostics
	}
	if model.Version.IsUnknown() {
		return flowmodel.TableMappings{}, handleUnknown(deferUnknown, "Unknown table mappings version", "config.table_mappings.version must be known before apply", &diagnostics), diagnostics
	}
	if model.Version.IsNull() || model.Version.ValueInt64() < 0 || model.Version.ValueInt64() > math.MaxUint32 {
		diagnostics.AddError("Invalid table mappings version", "config.table_mappings.version must be a uint32 value")
		return flowmodel.TableMappings{}, false, diagnostics
	}

	destinationModels, deferred := decodeObjectList[destinationTableMappingsModel](ctx, model.Destinations, "config.table_mappings.destinations", deferUnknown, &diagnostics)
	if deferred || diagnostics.HasError() {
		return flowmodel.TableMappings{}, deferred, diagnostics
	}
	out := flowmodel.TableMappings{Version: uint32(model.Version.ValueInt64()), Destinations: make([]flowmodel.DestinationTableMappings, 0, len(destinationModels))}
	for destinationIndex, destination := range destinationModels {
		scope := fmt.Sprintf("config.table_mappings.destinations[%d]", destinationIndex)
		if destination.Destination.IsUnknown() {
			return flowmodel.TableMappings{}, handleUnknown(deferUnknown, "Unknown table mapping destination", scope+".destination must be known before apply", &diagnostics), diagnostics
		}
		future, futurePresent, futureDeferred := decodeOptionalObject[futureTableMappingModel](ctx, destination.FutureTables, scope+".future_tables", deferUnknown, &diagnostics)
		if futureDeferred || diagnostics.HasError() {
			return flowmodel.TableMappings{}, futureDeferred, diagnostics
		}
		mappedFuture, mappedFutureDeferred := futureTableModelToInternal(ctx, future, futurePresent, scope+".future_tables", deferUnknown, &diagnostics)
		if mappedFutureDeferred || diagnostics.HasError() {
			return flowmodel.TableMappings{}, mappedFutureDeferred, diagnostics
		}
		tableModels, tablesDeferred := decodeObjectList[exactTableMappingModel](ctx, destination.Tables, scope+".tables", deferUnknown, &diagnostics)
		if tablesDeferred || diagnostics.HasError() {
			return flowmodel.TableMappings{}, tablesDeferred, diagnostics
		}
		converted := flowmodel.DestinationTableMappings{Destination: knownString(destination.Destination), FutureTables: mappedFuture, Tables: make([]flowmodel.TableMapping, 0, len(tableModels))}
		for tableIndex, table := range tableModels {
			tableScope := fmt.Sprintf("%s.tables[%d]", scope, tableIndex)
			mappedTable, tableDeferred := exactTableModelToInternal(ctx, table, tableScope, deferUnknown, &diagnostics)
			if tableDeferred || diagnostics.HasError() {
				return flowmodel.TableMappings{}, tableDeferred, diagnostics
			}
			converted.Tables = append(converted.Tables, mappedTable)
		}
		out.Destinations = append(out.Destinations, converted)
	}
	return out, false, diagnostics
}

func exactTableModelToInternal(ctx context.Context, model exactTableMappingModel, scope string, deferUnknown bool, diagnostics *diag.Diagnostics) (flowmodel.TableMapping, bool) {
	for name, value := range map[string]types.String{"source_schema": model.SourceSchema, "source_table": model.SourceTable, "action": model.Action, "target_schema": model.TargetSchema, "target_table": model.TargetTable} {
		if value.IsUnknown() {
			return flowmodel.TableMapping{}, handleUnknown(deferUnknown, "Unknown exact table mapping", scope+"."+name+" must be known before apply", diagnostics)
		}
	}
	future, futurePresent, deferred := decodeOptionalObject[futureColumnMappingModel](ctx, model.FutureColumns, scope+".future_columns", deferUnknown, diagnostics)
	if deferred || diagnostics.HasError() {
		return flowmodel.TableMapping{}, deferred
	}
	mappedFuture, deferred := futureColumnModelToInternal(future, futurePresent, scope+".future_columns", deferUnknown, diagnostics)
	if deferred || diagnostics.HasError() {
		return flowmodel.TableMapping{}, deferred
	}
	write, writePresent, deferred := decodeOptionalObject[tableWritePolicyModel](ctx, model.Write, scope+".write", deferUnknown, diagnostics)
	if deferred || diagnostics.HasError() {
		return flowmodel.TableMapping{}, deferred
	}
	mappedWrite, deferred := writeModelToInternal(ctx, write, writePresent, scope+".write", deferUnknown, diagnostics)
	if deferred || diagnostics.HasError() {
		return flowmodel.TableMapping{}, deferred
	}
	columns, deferred := decodeObjectList[columnMappingModel](ctx, model.Columns, scope+".columns", deferUnknown, diagnostics)
	if deferred || diagnostics.HasError() {
		return flowmodel.TableMapping{}, deferred
	}
	mappedColumns := make([]flowmodel.ColumnMapping, 0, len(columns))
	for columnIndex, column := range columns {
		columnScope := fmt.Sprintf("%s.columns[%d]", scope, columnIndex)
		if column.SourceColumn.IsUnknown() || column.Action.IsUnknown() || column.TargetColumn.IsUnknown() {
			return flowmodel.TableMapping{}, handleUnknown(deferUnknown, "Unknown column mapping", columnScope+" fields must be known before apply", diagnostics)
		}
		mappedColumns = append(mappedColumns, flowmodel.ColumnMapping{SourceColumn: knownString(column.SourceColumn), Action: flowmodel.MappingAction(knownString(column.Action)), TargetColumn: knownString(column.TargetColumn)})
	}
	if knownString(model.Action) == string(flowmodel.MappingActionExclude) {
		if futurePresent || writePresent {
			diagnostics.AddError("Invalid excluded table mapping", scope+" cannot contain future_columns or write objects when action=exclude")
			return flowmodel.TableMapping{}, false
		}
	}
	return flowmodel.TableMapping{
		SourceSchema: knownString(model.SourceSchema), SourceTable: knownString(model.SourceTable), Action: flowmodel.MappingAction(knownString(model.Action)),
		TargetSchema: knownString(model.TargetSchema), TargetTable: knownString(model.TargetTable), FutureColumns: mappedFuture, Columns: mappedColumns, Write: mappedWrite,
	}, false
}

func futureTableModelToInternal(ctx context.Context, model futureTableMappingModel, present bool, scope string, deferUnknown bool, diagnostics *diag.Diagnostics) (flowmodel.FutureTableMapping, bool) {
	if !present {
		return flowmodel.FutureTableMapping{}, false
	}
	if model.Action.IsUnknown() || model.TargetSchema.IsUnknown() || model.TargetTable.IsUnknown() {
		return flowmodel.FutureTableMapping{}, handleUnknown(deferUnknown, "Unknown future table mapping", scope+" fields must be known before apply", diagnostics)
	}
	future, futurePresent, deferred := decodeOptionalObject[futureColumnMappingModel](ctx, model.FutureColumns, scope+".future_columns", deferUnknown, diagnostics)
	if deferred || diagnostics.HasError() {
		return flowmodel.FutureTableMapping{}, deferred
	}
	mappedFuture, deferred := futureColumnModelToInternal(future, futurePresent, scope+".future_columns", deferUnknown, diagnostics)
	if deferred || diagnostics.HasError() {
		return flowmodel.FutureTableMapping{}, deferred
	}
	write, writePresent, deferred := decodeOptionalObject[tableWritePolicyModel](ctx, model.Write, scope+".write", deferUnknown, diagnostics)
	if deferred || diagnostics.HasError() {
		return flowmodel.FutureTableMapping{}, deferred
	}
	mappedWrite, deferred := writeModelToInternal(ctx, write, writePresent, scope+".write", deferUnknown, diagnostics)
	if deferred || diagnostics.HasError() {
		return flowmodel.FutureTableMapping{}, deferred
	}
	if knownString(model.Action) == string(flowmodel.MappingActionExclude) && (futurePresent || writePresent) {
		diagnostics.AddError("Invalid excluded future table mapping", scope+" cannot contain future_columns or write objects when action=exclude")
		return flowmodel.FutureTableMapping{}, false
	}
	return flowmodel.FutureTableMapping{
		Action: flowmodel.MappingAction(knownString(model.Action)), TargetSchema: knownString(model.TargetSchema), TargetTable: knownString(model.TargetTable),
		FutureColumns: mappedFuture, Write: mappedWrite,
	}, false
}

func futureColumnModelToInternal(model futureColumnMappingModel, present bool, scope string, deferUnknown bool, diagnostics *diag.Diagnostics) (flowmodel.FutureColumnMapping, bool) {
	if !present {
		return flowmodel.FutureColumnMapping{}, false
	}
	if model.Action.IsUnknown() || model.TargetColumn.IsUnknown() {
		return flowmodel.FutureColumnMapping{}, handleUnknown(deferUnknown, "Unknown future column mapping", scope+" fields must be known before apply", diagnostics)
	}
	return flowmodel.FutureColumnMapping{Action: flowmodel.MappingAction(knownString(model.Action)), TargetColumn: knownString(model.TargetColumn)}, false
}

func writeModelToInternal(ctx context.Context, model tableWritePolicyModel, present bool, scope string, deferUnknown bool, diagnostics *diag.Diagnostics) (flowmodel.TableWritePolicy, bool) {
	if !present {
		return flowmodel.TableWritePolicy{}, false
	}
	if model.Mode.IsUnknown() || model.WatermarkColumn.IsUnknown() {
		return flowmodel.TableWritePolicy{}, handleUnknown(deferUnknown, "Unknown table write policy", scope+" fields must be known before apply", diagnostics)
	}
	if model.KeyColumns.IsUnknown() {
		return flowmodel.TableWritePolicy{}, handleUnknown(deferUnknown, "Unknown table mapping keys", scope+".key_columns must be known before apply", diagnostics)
	}
	if model.KeyColumns.IsNull() {
		diagnostics.AddError("Missing table mapping keys", scope+".key_columns is required; use [] for append mode")
		return flowmodel.TableWritePolicy{}, false
	}
	keys := make([]string, 0, len(model.KeyColumns.Elements()))
	for keyIndex, element := range model.KeyColumns.Elements() {
		key, ok := element.(types.String)
		if !ok {
			diagnostics.AddError("Invalid table mapping key", fmt.Sprintf("%s.key_columns[%d] must be a string", scope, keyIndex))
			return flowmodel.TableWritePolicy{}, false
		}
		if key.IsUnknown() {
			return flowmodel.TableWritePolicy{}, handleUnknown(deferUnknown, "Unknown table mapping key", fmt.Sprintf("%s.key_columns[%d] must be known before apply", scope, keyIndex), diagnostics)
		}
		if key.IsNull() {
			diagnostics.AddError("Invalid table mapping key", fmt.Sprintf("%s.key_columns[%d] cannot be null", scope, keyIndex))
			return flowmodel.TableWritePolicy{}, false
		}
		keys = append(keys, key.ValueString())
	}
	return flowmodel.TableWritePolicy{Mode: flowmodel.TableWriteMode(knownString(model.Mode)), KeyColumns: keys, WatermarkColumn: knownString(model.WatermarkColumn)}, false
}

func decodeOptionalObject[T any](ctx context.Context, value types.Object, scope string, deferUnknown bool, diagnostics *diag.Diagnostics) (T, bool, bool) {
	var out T
	if value.IsNull() {
		return out, false, false
	}
	if value.IsUnknown() {
		return out, false, handleUnknown(deferUnknown, "Unknown nested table mapping", scope+" must be known before apply", diagnostics)
	}
	diagnostics.Append(value.As(ctx, &out, basetypes.ObjectAsOptions{})...)
	return out, true, false
}

func decodeObjectList[T any](ctx context.Context, value types.List, scope string, deferUnknown bool, diagnostics *diag.Diagnostics) ([]T, bool) {
	if value.IsUnknown() {
		return nil, handleUnknown(deferUnknown, "Unknown table mapping collection", scope+" must be known before apply", diagnostics)
	}
	if value.IsNull() {
		diagnostics.AddError("Missing table mapping collection", scope+" is required; use [] when it is empty")
		return nil, false
	}
	out := make([]T, 0, len(value.Elements()))
	for index, element := range value.Elements() {
		object, ok := element.(types.Object)
		if !ok {
			diagnostics.AddError("Invalid table mapping collection", fmt.Sprintf("%s[%d] must be an object", scope, index))
			return nil, false
		}
		if object.IsUnknown() {
			return nil, handleUnknown(deferUnknown, "Unknown table mapping object", fmt.Sprintf("%s[%d] must be known before apply", scope, index), diagnostics)
		}
		if object.IsNull() {
			diagnostics.AddError("Invalid table mapping object", fmt.Sprintf("%s[%d] cannot be null", scope, index))
			return nil, false
		}
		var decoded T
		diagnostics.Append(object.As(ctx, &decoded, basetypes.ObjectAsOptions{})...)
		if diagnostics.HasError() {
			return nil, false
		}
		out = append(out, decoded)
	}
	return out, false
}

func handleUnknown(deferUnknown bool, summary, detail string, diagnostics *diag.Diagnostics) bool {
	if deferUnknown {
		return true
	}
	diagnostics.AddError(summary, detail)
	return false
}

func knownString(value types.String) string {
	if value.IsNull() || value.IsUnknown() {
		return ""
	}
	return value.ValueString()
}

func tableMappingsInternalToProto(model flowmodel.TableMappings) *wallabypb.TableMappings {
	out := &wallabypb.TableMappings{Version: model.Version, Destinations: make([]*wallabypb.DestinationTableMappings, 0, len(model.Destinations))}
	for _, destination := range model.Destinations {
		mapped := &wallabypb.DestinationTableMappings{Destination: destination.Destination, FutureTables: futureTableInternalToProto(destination.FutureTables), Tables: make([]*wallabypb.TableMapping, 0, len(destination.Tables))}
		for _, table := range destination.Tables {
			wireTable := &wallabypb.TableMapping{
				SourceSchema: table.SourceSchema, SourceTable: table.SourceTable, Action: mappingActionToProto(table.Action), TargetSchema: table.TargetSchema, TargetTable: table.TargetTable,
				FutureColumns: futureColumnInternalToProto(table.FutureColumns), Columns: make([]*wallabypb.ColumnMapping, 0, len(table.Columns)), Write: writeInternalToProto(table.Write),
			}
			for _, column := range table.Columns {
				wireTable.Columns = append(wireTable.Columns, &wallabypb.ColumnMapping{SourceColumn: column.SourceColumn, Action: mappingActionToProto(column.Action), TargetColumn: column.TargetColumn})
			}
			mapped.Tables = append(mapped.Tables, wireTable)
		}
		out.Destinations = append(out.Destinations, mapped)
	}
	return out
}

func futureTableInternalToProto(model flowmodel.FutureTableMapping) *wallabypb.FutureTableMapping {
	if model.Action == "" && model.TargetSchema == "" && model.TargetTable == "" && model.FutureColumns == (flowmodel.FutureColumnMapping{}) && model.Write.Mode == "" && len(model.Write.KeyColumns) == 0 && model.Write.WatermarkColumn == "" {
		return nil
	}
	return &wallabypb.FutureTableMapping{Action: mappingActionToProto(model.Action), TargetSchema: model.TargetSchema, TargetTable: model.TargetTable, FutureColumns: futureColumnInternalToProto(model.FutureColumns), Write: writeInternalToProto(model.Write)}
}

func futureColumnInternalToProto(model flowmodel.FutureColumnMapping) *wallabypb.FutureColumnMapping {
	if model == (flowmodel.FutureColumnMapping{}) {
		return nil
	}
	return &wallabypb.FutureColumnMapping{Action: mappingActionToProto(model.Action), TargetColumn: model.TargetColumn}
}

func writeInternalToProto(model flowmodel.TableWritePolicy) *wallabypb.TableWritePolicy {
	if model.Mode == "" && len(model.KeyColumns) == 0 && model.WatermarkColumn == "" {
		return nil
	}
	keys := make([]string, len(model.KeyColumns))
	copy(keys, model.KeyColumns)
	return &wallabypb.TableWritePolicy{Mode: writeModeToProto(model.Mode), KeyColumns: keys, WatermarkColumn: model.WatermarkColumn}
}

func mappingActionToProto(value flowmodel.MappingAction) wallabypb.MappingAction {
	switch value {
	case flowmodel.MappingActionInclude:
		return wallabypb.MappingAction_MAPPING_ACTION_INCLUDE
	case flowmodel.MappingActionExclude:
		return wallabypb.MappingAction_MAPPING_ACTION_EXCLUDE
	default:
		return wallabypb.MappingAction_MAPPING_ACTION_UNSPECIFIED
	}
}

func writeModeToProto(value flowmodel.TableWriteMode) wallabypb.TableWriteMode {
	switch value {
	case flowmodel.TableWriteModeAppend:
		return wallabypb.TableWriteMode_TABLE_WRITE_MODE_APPEND
	case flowmodel.TableWriteModeUpsert:
		return wallabypb.TableWriteMode_TABLE_WRITE_MODE_UPSERT
	default:
		return wallabypb.TableWriteMode_TABLE_WRITE_MODE_UNSPECIFIED
	}
}

func tableMappingsModelFromProto(ctx context.Context, pb *wallabypb.TableMappings) (types.Object, diag.Diagnostics) {
	if pb == nil {
		return types.ObjectNull(tableMappingsAttributeTypes()), nil
	}
	var diagnostics diag.Diagnostics
	destinations := make([]attr.Value, 0, len(pb.Destinations))
	for destinationIndex, destination := range pb.Destinations {
		if destination == nil {
			diagnostics.AddError("Invalid table mappings response", fmt.Sprintf("config.table_mappings.destinations[%d] is null", destinationIndex))
			continue
		}
		futureTables := futureTableModelFromProto(ctx, destination.FutureTables, &diagnostics)
		tables := make([]attr.Value, 0, len(destination.Tables))
		for tableIndex, table := range destination.Tables {
			if table == nil {
				diagnostics.AddError("Invalid table mappings response", fmt.Sprintf("config.table_mappings.destinations[%d].tables[%d] is null", destinationIndex, tableIndex))
				continue
			}
			futureColumns := futureColumnModelFromProto(table.FutureColumns, &diagnostics)
			columns := make([]attr.Value, 0, len(table.Columns))
			for columnIndex, column := range table.Columns {
				if column == nil {
					diagnostics.AddError("Invalid table mappings response", fmt.Sprintf("config.table_mappings.destinations[%d].tables[%d].columns[%d] is null", destinationIndex, tableIndex, columnIndex))
					continue
				}
				columns = append(columns, objectValue(columnAttributeTypes(), map[string]attr.Value{
					"source_column": types.StringValue(column.SourceColumn), "action": types.StringValue(mappingActionFromProto(column.Action)), "target_column": nullableString(column.TargetColumn),
				}, &diagnostics))
			}
			columnList := listValue(types.ObjectType{AttrTypes: columnAttributeTypes()}, columns, &diagnostics)
			tables = append(tables, objectValue(exactTableAttributeTypes(), map[string]attr.Value{
				"source_schema": types.StringValue(table.SourceSchema), "source_table": types.StringValue(table.SourceTable), "action": types.StringValue(mappingActionFromProto(table.Action)),
				"target_schema": nullableString(table.TargetSchema), "target_table": nullableString(table.TargetTable), "future_columns": futureColumns, "columns": columnList,
				"write": writeModelFromProto(table.Write, &diagnostics),
			}, &diagnostics))
		}
		tableList := listValue(types.ObjectType{AttrTypes: exactTableAttributeTypes()}, tables, &diagnostics)
		destinations = append(destinations, objectValue(destinationMappingAttributeTypes(), map[string]attr.Value{
			"destination": types.StringValue(destination.Destination), "future_tables": futureTables, "tables": tableList,
		}, &diagnostics))
	}
	destinationList := listValue(types.ObjectType{AttrTypes: destinationMappingAttributeTypes()}, destinations, &diagnostics)
	return objectValue(tableMappingsAttributeTypes(), map[string]attr.Value{"version": types.Int64Value(int64(pb.Version)), "destinations": destinationList}, &diagnostics), diagnostics
}

func futureTableModelFromProto(_ context.Context, pb *wallabypb.FutureTableMapping, diagnostics *diag.Diagnostics) types.Object {
	if pb == nil {
		return types.ObjectNull(futureTableAttributeTypes())
	}
	return objectValue(futureTableAttributeTypes(), map[string]attr.Value{
		"action": types.StringValue(mappingActionFromProto(pb.Action)), "target_schema": nullableString(pb.TargetSchema), "target_table": nullableString(pb.TargetTable),
		"future_columns": futureColumnModelFromProto(pb.FutureColumns, diagnostics), "write": writeModelFromProto(pb.Write, diagnostics),
	}, diagnostics)
}

func futureColumnModelFromProto(pb *wallabypb.FutureColumnMapping, diagnostics *diag.Diagnostics) types.Object {
	if pb == nil {
		return types.ObjectNull(futureColumnAttributeTypes())
	}
	return objectValue(futureColumnAttributeTypes(), map[string]attr.Value{"action": types.StringValue(mappingActionFromProto(pb.Action)), "target_column": nullableString(pb.TargetColumn)}, diagnostics)
}

func writeModelFromProto(pb *wallabypb.TableWritePolicy, diagnostics *diag.Diagnostics) types.Object {
	if pb == nil {
		return types.ObjectNull(writeAttributeTypes())
	}
	return objectValue(writeAttributeTypes(), map[string]attr.Value{
		"mode": types.StringValue(writeModeFromProto(pb.Mode)), "key_columns": stringListValue(pb.KeyColumns), "watermark_column": nullableString(pb.WatermarkColumn),
	}, diagnostics)
}

func objectValue(attributeTypes map[string]attr.Type, attributes map[string]attr.Value, diagnostics *diag.Diagnostics) types.Object {
	value, valueDiagnostics := types.ObjectValue(attributeTypes, attributes)
	diagnostics.Append(valueDiagnostics...)
	return value
}

func listValue(elementType attr.Type, elements []attr.Value, diagnostics *diag.Diagnostics) types.List {
	value, valueDiagnostics := types.ListValue(elementType, elements)
	diagnostics.Append(valueDiagnostics...)
	return value
}

func stringListValue(items []string) types.List {
	values := make([]attr.Value, 0, len(items))
	for _, item := range items {
		values = append(values, types.StringValue(item))
	}
	return types.ListValueMust(types.StringType, values)
}

func nullableString(value string) types.String {
	if value == "" {
		return types.StringNull()
	}
	return types.StringValue(value)
}

func mappingActionFromProto(value wallabypb.MappingAction) string {
	switch value {
	case wallabypb.MappingAction_MAPPING_ACTION_INCLUDE:
		return "include"
	case wallabypb.MappingAction_MAPPING_ACTION_EXCLUDE:
		return "exclude"
	default:
		return ""
	}
}

func writeModeFromProto(value wallabypb.TableWriteMode) string {
	switch value {
	case wallabypb.TableWriteMode_TABLE_WRITE_MODE_APPEND:
		return "append"
	case wallabypb.TableWriteMode_TABLE_WRITE_MODE_UPSERT:
		return "upsert"
	default:
		return ""
	}
}

func validateTerraformTableMappings(ctx context.Context, model flowResourceModel) diag.Diagnostics {
	var diagnostics diag.Diagnostics
	if model.Config == nil {
		diagnostics.AddError("Missing table mappings", "config.table_mappings is required and must contain one mapping for every destination")
		return diagnostics
	}
	mappings, deferred, conversionDiagnostics := tableMappingsModelToInternal(ctx, model.Config.TableMappings, true)
	diagnostics.Append(conversionDiagnostics...)
	if deferred || diagnostics.HasError() {
		return diagnostics
	}
	destinations := make([]connector.Spec, 0, len(model.Destinations))
	for index, destination := range model.Destinations {
		if destination.Name.IsUnknown() || destination.Type.IsUnknown() || destination.Options.IsUnknown() {
			return diagnostics
		}
		options := map[string]string{}
		if !destination.Options.IsNull() {
			optionDiagnostics := destination.Options.ElementsAs(ctx, &options, false)
			diagnostics.Append(optionDiagnostics...)
			if optionDiagnostics.HasError() {
				return diagnostics
			}
		}
		name := knownString(destination.Name)
		if strings.TrimSpace(name) == "" {
			diagnostics.AddError("Invalid destination name", fmt.Sprintf("destinations[%d].name must be a nonblank identifier", index))
			continue
		}
		destinations = append(destinations, connector.Spec{Name: name, Type: connector.EndpointType(strings.ToLower(strings.TrimSpace(knownString(destination.Type)))), Options: options})
	}
	if diagnostics.HasError() {
		return diagnostics
	}
	if err := mappings.Validate(destinations); err != nil {
		diagnostics.AddError("Invalid table mappings", err.Error())
	}
	return diagnostics
}
