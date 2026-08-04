package main

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-go/tftypes"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func appendMappingProto(destination string) *wallabypb.TableMappings {
	return &wallabypb.TableMappings{Version: 1, Destinations: []*wallabypb.DestinationTableMappings{{
		Destination: destination,
		FutureTables: &wallabypb.FutureTableMapping{
			Action: wallabypb.MappingAction_MAPPING_ACTION_INCLUDE, TargetSchema: "{schema}", TargetTable: "{table}",
			FutureColumns: &wallabypb.FutureColumnMapping{Action: wallabypb.MappingAction_MAPPING_ACTION_INCLUDE, TargetColumn: "{column}"},
			Write:         &wallabypb.TableWritePolicy{Mode: wallabypb.TableWriteMode_TABLE_WRITE_MODE_APPEND},
		},
	}}}
}

func completeMappingProto() *wallabypb.TableMappings {
	mapping := appendMappingProto("target")
	mapping.Destinations[0].Tables = []*wallabypb.TableMapping{
		{
			SourceSchema: "sales", SourceTable: "orders", Action: wallabypb.MappingAction_MAPPING_ACTION_INCLUDE, TargetSchema: "analytics", TargetTable: "facts",
			FutureColumns: &wallabypb.FutureColumnMapping{Action: wallabypb.MappingAction_MAPPING_ACTION_INCLUDE, TargetColumn: "new_{column}"},
			Columns: []*wallabypb.ColumnMapping{
				{SourceColumn: "id", Action: wallabypb.MappingAction_MAPPING_ACTION_INCLUDE, TargetColumn: "order_id"},
				{SourceColumn: "tenant_id", Action: wallabypb.MappingAction_MAPPING_ACTION_INCLUDE, TargetColumn: "tenant"},
				{SourceColumn: "secret", Action: wallabypb.MappingAction_MAPPING_ACTION_EXCLUDE},
			},
			Write: &wallabypb.TableWritePolicy{Mode: wallabypb.TableWriteMode_TABLE_WRITE_MODE_UPSERT, KeyColumns: []string{"tenant_id", "id"}, WatermarkColumn: "updated_at"},
		},
		{SourceSchema: "sales", SourceTable: "discarded", Action: wallabypb.MappingAction_MAPPING_ACTION_EXCLUDE},
	}
	mapping.Destinations = append(mapping.Destinations, appendMappingProto("archive").Destinations[0])
	return mapping
}

func mappingObject(t *testing.T, mapping *wallabypb.TableMappings) types.Object {
	t.Helper()
	value, diagnostics := tableMappingsModelFromProto(context.Background(), mapping)
	if diagnostics.HasError() {
		t.Fatalf("mapping state diagnostics=%v", diagnostics)
	}
	return value
}

func testAppendMappings(destination string) types.Object { // used by resource_flow_test.go
	value, diagnostics := tableMappingsModelFromProto(context.Background(), appendMappingProto(destination))
	if diagnostics.HasError() {
		panic(diagnostics)
	}
	return value
}

func testCompleteMappingModel() flowResourceModel {
	return flowResourceModel{
		ID: types.StringValue("flow-1"), Name: types.StringValue("mapped"), WireFormat: types.StringValue("arrow"), Parallelism: types.Int64Value(3), State: types.StringValue("running"), StartImmediately: types.BoolValue(true),
		Source: endpointModel{Name: types.StringValue("source"), Type: types.StringValue("postgres"), Options: types.MapNull(types.StringType)},
		Destinations: []endpointModel{
			{Name: types.StringValue("target"), Type: types.StringValue("postgres"), Options: types.MapNull(types.StringType)},
			{Name: types.StringValue("archive"), Type: types.StringValue("s3"), Options: types.MapNull(types.StringType)},
		},
		Config: &flowConfigModel{
			AckPolicy: types.StringValue("primary"), PrimaryDestination: types.StringValue("target"), FailureMode: types.StringValue("hold_slot"), GiveUpPolicy: types.StringValue("never"),
			SchemaRegistrySubject: types.StringValue("orders-value"), SchemaRegistryProtoTypesSubject: types.StringValue("wallaby-types"), SchemaRegistrySubjectMode: types.StringValue("record"),
			DDL: &flowDDLConfigModel{Gate: types.BoolValue(true), AutoApprove: types.BoolValue(false), AutoApply: types.BoolValue(true)}, TableMappings: testObjectFromProto(completeMappingProto()),
		},
	}
}

func testObjectFromProto(mapping *wallabypb.TableMappings) types.Object {
	value, diagnostics := tableMappingsModelFromProto(context.Background(), mapping)
	if diagnostics.HasError() {
		panic(diagnostics)
	}
	return value
}

func objectField(t *testing.T, object types.Object, name string) attr.Value {
	t.Helper()
	value, ok := object.Attributes()[name]
	if !ok {
		t.Fatalf("missing object field %q", name)
	}
	return value
}

func objectList(t *testing.T, value attr.Value) []types.Object {
	t.Helper()
	list, ok := value.(types.List)
	if !ok || list.IsNull() || list.IsUnknown() {
		t.Fatalf("value is not a known list: %T %v", value, value)
	}
	out := make([]types.Object, 0, len(list.Elements()))
	for _, element := range list.Elements() {
		object, ok := element.(types.Object)
		if !ok {
			t.Fatalf("list element is %T", element)
		}
		out = append(out, object)
	}
	return out
}

func replaceObjectField(t *testing.T, object types.Object, name string, value attr.Value) types.Object {
	t.Helper()
	attributes := make(map[string]attr.Value, len(object.Attributes()))
	for key, current := range object.Attributes() {
		attributes[key] = current
	}
	attributes[name] = value
	return types.ObjectValueMust(object.AttributeTypes(context.Background()), attributes)
}

func replaceListElement(t *testing.T, list types.List, index int, value attr.Value) types.List {
	t.Helper()
	elements := append([]attr.Value(nil), list.Elements()...)
	elements[index] = value
	return types.ListValueMust(list.ElementType(context.Background()), elements)
}

func mappingWithDestination(t *testing.T, root types.Object, index int, transform func(types.Object) types.Object) types.Object {
	t.Helper()
	list := objectField(t, root, "destinations").(types.List)
	destination := list.Elements()[index].(types.Object)
	return replaceObjectField(t, root, "destinations", replaceListElement(t, list, index, transform(destination)))
}

func destinationWithTable(t *testing.T, destination types.Object, index int, transform func(types.Object) types.Object) types.Object {
	t.Helper()
	list := objectField(t, destination, "tables").(types.List)
	table := list.Elements()[index].(types.Object)
	return replaceObjectField(t, destination, "tables", replaceListElement(t, list, index, transform(table)))
}

func tableWithWrite(t *testing.T, table types.Object, transform func(types.Object) types.Object) types.Object {
	t.Helper()
	return replaceObjectField(t, table, "write", transform(objectField(t, table, "write").(types.Object)))
}

func TestTableMappingsModelProtoStateRoundTripPreservesEveryFieldAndOrder(t *testing.T) {
	model := testCompleteMappingModel()
	wire, diagnostics := flowModelToProto(context.Background(), model)
	if diagnostics.HasError() {
		t.Fatal(diagnostics)
	}
	if got := wire.Config.TableMappings.Destinations; len(got) != 2 || got[0].Destination != "target" || got[1].Destination != "archive" {
		t.Fatalf("destination order changed: %+v", got)
	}
	mapping := wire.Config.TableMappings.Destinations[0]
	if len(mapping.Tables) != 2 || mapping.Tables[0].SourceTable != "orders" || mapping.Tables[1].SourceTable != "discarded" {
		t.Fatalf("table order changed: %+v", mapping.Tables)
	}
	if got := mapping.Tables[0].Write.KeyColumns; !reflect.DeepEqual(got, []string{"tenant_id", "id"}) {
		t.Fatalf("key order=%v", got)
	}
	if got := mapping.Tables[0].Columns; len(got) != 3 || got[0].SourceColumn != "id" || got[1].SourceColumn != "tenant_id" || got[2].SourceColumn != "secret" {
		t.Fatalf("column order changed: %+v", got)
	}
	stateConfig, stateDiagnostics := flowConfigModelFromProto(context.Background(), wire.Config)
	if stateDiagnostics.HasError() {
		t.Fatal(stateDiagnostics)
	}
	refreshed := model
	refreshed.Config = stateConfig
	roundTrip, diagnostics := flowModelToProto(context.Background(), refreshed)
	if diagnostics.HasError() || !proto.Equal(wire.Config, roundTrip.Config) {
		t.Fatalf("config round trip diagnostics=%v\nfirst=%v\nsecond=%v", diagnostics, wire.Config, roundTrip.Config)
	}
}

func TestCanonicalMappingStateUsesKnownEmptyCollectionsAfterWireRoundTrip(t *testing.T) {
	emptyRoot := mappingObject(t, &wallabypb.TableMappings{Version: 1})
	emptyDestinations := objectField(t, emptyRoot, "destinations").(types.List)
	if emptyDestinations.IsNull() || emptyDestinations.IsUnknown() || len(emptyDestinations.Elements()) != 0 {
		t.Fatalf("empty destinations state=%v", emptyDestinations)
	}
	mapping := appendMappingProto("target")
	mapping.Destinations[0].Tables = []*wallabypb.TableMapping{
		{SourceSchema: "public", SourceTable: "events", Action: wallabypb.MappingAction_MAPPING_ACTION_INCLUDE, TargetSchema: "public", TargetTable: "events", FutureColumns: &wallabypb.FutureColumnMapping{Action: wallabypb.MappingAction_MAPPING_ACTION_INCLUDE, TargetColumn: "{column}"}, Write: &wallabypb.TableWritePolicy{Mode: wallabypb.TableWriteMode_TABLE_WRITE_MODE_APPEND}},
		{SourceSchema: "public", SourceTable: "ignored", Action: wallabypb.MappingAction_MAPPING_ACTION_EXCLUDE},
	}
	encoded, err := proto.Marshal(mapping)
	if err != nil {
		t.Fatal(err)
	}
	decoded := &wallabypb.TableMappings{}
	if err := proto.Unmarshal(encoded, decoded); err != nil {
		t.Fatal(err)
	}
	root := mappingObject(t, decoded)
	destinations := objectList(t, objectField(t, root, "destinations"))
	if len(destinations) != 1 {
		t.Fatalf("destinations=%d", len(destinations))
	}
	tablesValue := objectField(t, destinations[0], "tables").(types.List)
	if tablesValue.IsNull() || tablesValue.IsUnknown() || len(tablesValue.Elements()) != 2 {
		t.Fatalf("tables state=%v", tablesValue)
	}
	tables := objectList(t, tablesValue)
	columns := objectField(t, tables[0], "columns").(types.List)
	if columns.IsNull() || columns.IsUnknown() || len(columns.Elements()) != 0 {
		t.Fatalf("columns state=%v", columns)
	}
	write := objectField(t, tables[0], "write").(types.Object)
	keys := objectField(t, write, "key_columns").(types.List)
	if keys.IsNull() || keys.IsUnknown() || len(keys.Elements()) != 0 {
		t.Fatalf("key_columns state=%v", keys)
	}
	archiveRoot := mappingObject(t, appendMappingProto("archive"))
	archive := objectList(t, objectField(t, archiveRoot, "destinations"))[0]
	archiveTables := objectField(t, archive, "tables").(types.List)
	if archiveTables.IsNull() || archiveTables.IsUnknown() || len(archiveTables.Elements()) != 0 {
		t.Fatalf("empty tables state=%v", archiveTables)
	}
	if future := objectField(t, tables[1], "future_columns").(types.Object); !future.IsNull() {
		t.Fatalf("excluded future_columns=%v", future)
	}
	if write := objectField(t, tables[1], "write").(types.Object); !write.IsNull() {
		t.Fatalf("excluded write=%v", write)
	}
	internal, deferred, diagnostics := tableMappingsModelToInternal(context.Background(), root, false)
	if deferred || diagnostics.HasError() {
		t.Fatalf("canonical conversion deferred=%t diagnostics=%v", deferred, diagnostics)
	}
	if internal.Destinations == nil || internal.Destinations[0].Tables == nil || internal.Destinations[0].Tables[0].Columns == nil || internal.Destinations[0].Tables[0].Write.KeyColumns == nil {
		t.Fatalf("canonical empty slices lost: %+v", internal)
	}
}

func TestRequiredMappingCollectionsRejectOmission(t *testing.T) {
	base := testCompleteMappingModel()
	root := base.Config.TableMappings
	variants := map[string]types.Object{"destinations": replaceObjectField(t, root, "destinations", types.ListNull(types.ObjectType{AttrTypes: destinationMappingAttributeTypes()})), "tables": mappingWithDestination(t, root, 0, func(destination types.Object) types.Object {
		return replaceObjectField(t, destination, "tables", types.ListNull(types.ObjectType{AttrTypes: exactTableAttributeTypes()}))
	}), "columns": mappingWithDestination(t, root, 0, func(destination types.Object) types.Object {
		return destinationWithTable(t, destination, 0, func(table types.Object) types.Object {
			return replaceObjectField(t, table, "columns", types.ListNull(types.ObjectType{AttrTypes: columnAttributeTypes()}))
		})
	}), "key_columns": mappingWithDestination(t, root, 0, func(destination types.Object) types.Object {
		return destinationWithTable(t, destination, 0, func(table types.Object) types.Object {
			return tableWithWrite(t, table, func(write types.Object) types.Object {
				return replaceObjectField(t, write, "key_columns", types.ListNull(types.StringType))
			})
		})
	})}
	ctx := context.Background()
	providerSchema := resourceSchema(t)
	instance := &flowResource{}
	for name, variant := range variants {
		t.Run(name, func(t *testing.T) {
			model := base
			config := *base.Config
			config.TableMappings = variant
			model.Config = &config
			plan := tfsdk.Plan{Schema: providerSchema}
			if diagnostics := plan.Set(ctx, model); diagnostics.HasError() {
				t.Fatal(diagnostics)
			}
			var response resource.ValidateConfigResponse
			instance.ValidateConfig(ctx, resource.ValidateConfigRequest{Config: tfsdk.Config{Schema: providerSchema, Raw: plan.Raw}}, &response)
			if !response.Diagnostics.HasError() || !strings.Contains(strings.ToLower(response.Diagnostics.Errors()[0].Detail()), "required") {
				t.Fatalf("omitted %s diagnostics=%v", name, response.Diagnostics)
			}
		})
	}
}

func TestExplicitEmptyCollectionsRemainStableAcrossFrameworkApplyAndRefresh(t *testing.T) {
	ctx := context.Background()
	model := testCompleteMappingModel()
	model.Destinations = []endpointModel{{Name: types.StringValue("archive"), Type: types.StringValue("s3"), Options: types.MapNull(types.StringType)}}
	config := *model.Config
	config.AckPolicy = types.StringValue("all")
	config.PrimaryDestination = types.StringNull()
	config.TableMappings = mappingObject(t, appendMappingProto("archive"))
	model.Config = &config
	client := &flowRPCFake{}
	instance := &flowResource{client: &Client{Flow: client}}
	createResponse := resource.CreateResponse{State: stateForModel(t, model)}
	instance.Create(ctx, resource.CreateRequest{Plan: planForModel(t, model)}, &createResponse)
	if createResponse.Diagnostics.HasError() {
		t.Fatal(createResponse.Diagnostics)
	}
	encoded, err := proto.Marshal(client.created)
	if err != nil {
		t.Fatal(err)
	}
	client.flow = &wallabypb.Flow{}
	if err := proto.Unmarshal(encoded, client.flow); err != nil {
		t.Fatal(err)
	}
	readResponse := resource.ReadResponse{State: createResponse.State}
	instance.Read(ctx, resource.ReadRequest{State: createResponse.State}, &readResponse)
	if readResponse.Diagnostics.HasError() {
		t.Fatal(readResponse.Diagnostics)
	}
	var final flowResourceModel
	if diagnostics := readResponse.State.Get(ctx, &final); diagnostics.HasError() {
		t.Fatal(diagnostics)
	}
	destinations := objectList(t, objectField(t, final.Config.TableMappings, "destinations"))
	tables := objectField(t, destinations[0], "tables").(types.List)
	future := objectField(t, destinations[0], "future_tables").(types.Object)
	write := objectField(t, future, "write").(types.Object)
	keys := objectField(t, write, "key_columns").(types.List)
	if tables.IsNull() || tables.IsUnknown() || len(tables.Elements()) != 0 || keys.IsNull() || keys.IsUnknown() || len(keys.Elements()) != 0 {
		t.Fatalf("explicit empty collections drifted: tables=%v keys=%v", tables, keys)
	}
}

func TestUnknownMappingObjectsAndCollectionsDeferWithoutConfigGetErrors(t *testing.T) {
	ctx := context.Background()
	base := testCompleteMappingModel()
	root := base.Config.TableMappings
	destinationType := types.ObjectType{AttrTypes: destinationMappingAttributeTypes()}
	tableType := types.ObjectType{AttrTypes: exactTableAttributeTypes()}
	columnType := types.ObjectType{AttrTypes: columnAttributeTypes()}
	variants := map[string]types.Object{
		"whole mapping object": types.ObjectUnknown(tableMappingsAttributeTypes()),
		"destinations list":    replaceObjectField(t, root, "destinations", types.ListUnknown(destinationType)),
		"destination object": replaceObjectField(t, root, "destinations", replaceListElement(t, objectField(t, root, "destinations").(types.List), 0,
			types.ObjectUnknown(destinationMappingAttributeTypes()))),
		"future tables object": mappingWithDestination(t, root, 0, func(destination types.Object) types.Object {
			return replaceObjectField(t, destination, "future_tables", types.ObjectUnknown(futureTableAttributeTypes()))
		}),
		"tables list": mappingWithDestination(t, root, 0, func(destination types.Object) types.Object {
			return replaceObjectField(t, destination, "tables", types.ListUnknown(tableType))
		}),
		"table object": mappingWithDestination(t, root, 0, func(destination types.Object) types.Object {
			return replaceObjectField(t, destination, "tables", replaceListElement(t, objectField(t, destination, "tables").(types.List), 0, types.ObjectUnknown(exactTableAttributeTypes())))
		}),
		"future table future columns": mappingWithDestination(t, root, 0, func(destination types.Object) types.Object {
			future := objectField(t, destination, "future_tables").(types.Object)
			return replaceObjectField(t, destination, "future_tables", replaceObjectField(t, future, "future_columns", types.ObjectUnknown(futureColumnAttributeTypes())))
		}),
		"future table write": mappingWithDestination(t, root, 0, func(destination types.Object) types.Object {
			future := objectField(t, destination, "future_tables").(types.Object)
			return replaceObjectField(t, destination, "future_tables", replaceObjectField(t, future, "write", types.ObjectUnknown(writeAttributeTypes())))
		}),
		"table future columns": mappingWithDestination(t, root, 0, func(destination types.Object) types.Object {
			return destinationWithTable(t, destination, 0, func(table types.Object) types.Object {
				return replaceObjectField(t, table, "future_columns", types.ObjectUnknown(futureColumnAttributeTypes()))
			})
		}),
		"columns list": mappingWithDestination(t, root, 0, func(destination types.Object) types.Object {
			return destinationWithTable(t, destination, 0, func(table types.Object) types.Object {
				return replaceObjectField(t, table, "columns", types.ListUnknown(columnType))
			})
		}),
		"column object": mappingWithDestination(t, root, 0, func(destination types.Object) types.Object {
			return destinationWithTable(t, destination, 0, func(table types.Object) types.Object {
				columns := objectField(t, table, "columns").(types.List)
				return replaceObjectField(t, table, "columns", replaceListElement(t, columns, 0, types.ObjectUnknown(columnAttributeTypes())))
			})
		}),
		"write object": mappingWithDestination(t, root, 0, func(destination types.Object) types.Object {
			return destinationWithTable(t, destination, 0, func(table types.Object) types.Object {
				return replaceObjectField(t, table, "write", types.ObjectUnknown(writeAttributeTypes()))
			})
		}),
		"key list": mappingWithDestination(t, root, 0, func(destination types.Object) types.Object {
			return destinationWithTable(t, destination, 0, func(table types.Object) types.Object {
				return tableWithWrite(t, table, func(write types.Object) types.Object {
					return replaceObjectField(t, write, "key_columns", types.ListUnknown(types.StringType))
				})
			})
		}),
		"key element": mappingWithDestination(t, root, 0, func(destination types.Object) types.Object {
			return destinationWithTable(t, destination, 0, func(table types.Object) types.Object {
				return tableWithWrite(t, table, func(write types.Object) types.Object {
					keys := objectField(t, write, "key_columns").(types.List)
					return replaceObjectField(t, write, "key_columns", replaceListElement(t, keys, 0, types.StringUnknown()))
				})
			})
		}),
	}
	var schemaResponse resource.SchemaResponse
	instance := &flowResource{}
	instance.Schema(ctx, resource.SchemaRequest{}, &schemaResponse)
	for name, variant := range variants {
		t.Run(name, func(t *testing.T) {
			model := base
			config := *base.Config
			config.TableMappings = variant
			model.Config = &config
			plan := tfsdk.Plan{Schema: schemaResponse.Schema}
			if diagnostics := plan.Set(ctx, model); diagnostics.HasError() {
				t.Fatalf("framework-native state failed before validation: %v", diagnostics)
			}
			tfConfig := tfsdk.Config{Schema: schemaResponse.Schema, Raw: plan.Raw}
			var response resource.ValidateConfigResponse
			instance.ValidateConfig(ctx, resource.ValidateConfigRequest{Config: tfConfig}, &response)
			if response.Diagnostics.HasError() {
				t.Fatalf("unknown value was not deferred: %v", response.Diagnostics)
			}
			if _, diagnostics := flowModelToProto(ctx, model); !diagnostics.HasError() {
				t.Fatal("unknown value was silently collapsed during apply conversion")
			}
		})
	}
}

func TestTerraformAppendWatermarkIsMetadata(t *testing.T) {
	mapping := appendMappingProto("archive")
	mapping.Destinations[0].FutureTables.Write.WatermarkColumn = "observed_at"
	model := testCompleteMappingModel()
	model.Destinations = []endpointModel{{Name: types.StringValue("archive"), Type: types.StringValue("s3"), Options: types.MapNull(types.StringType)}}
	model.Config.TableMappings = mappingObject(t, mapping)
	if diagnostics := validateTerraformTableMappings(context.Background(), model); diagnostics.HasError() {
		t.Fatalf("append watermark metadata rejected: %v", diagnostics)
	}
}

func TestPresentButMeaninglessNestedObjectsFailClosed(t *testing.T) {
	tests := []struct {
		name string
		edit func(*wallabypb.TableMappings)
		want string
	}{
		{"excluded future has empty future-column message", func(mapping *wallabypb.TableMappings) {
			mapping.Destinations[0].FutureTables = &wallabypb.FutureTableMapping{Action: wallabypb.MappingAction_MAPPING_ACTION_EXCLUDE, FutureColumns: &wallabypb.FutureColumnMapping{}}
		}, "cannot contain"},
		{"excluded table has empty write message", func(mapping *wallabypb.TableMappings) {
			mapping.Destinations[0].Tables = []*wallabypb.TableMapping{{SourceSchema: "public", SourceTable: "ignored", Action: wallabypb.MappingAction_MAPPING_ACTION_EXCLUDE, Write: &wallabypb.TableWritePolicy{}}}
		}, "cannot contain"},
		{"included table has empty write message", func(mapping *wallabypb.TableMappings) {
			mapping.Destinations[0].Tables = []*wallabypb.TableMapping{{SourceSchema: "public", SourceTable: "events", Action: wallabypb.MappingAction_MAPPING_ACTION_INCLUDE, TargetSchema: "public", TargetTable: "events", FutureColumns: &wallabypb.FutureColumnMapping{Action: wallabypb.MappingAction_MAPPING_ACTION_INCLUDE, TargetColumn: "{column}"}, Write: &wallabypb.TableWritePolicy{}}}
		}, "write mode"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mapping := appendMappingProto("target")
			test.edit(mapping)
			model := testCompleteMappingModel()
			model.Destinations = model.Destinations[:1]
			model.Config.TableMappings = mappingObject(t, mapping)
			if diagnostics := validateTerraformTableMappings(context.Background(), model); !diagnostics.HasError() || !strings.Contains(strings.ToLower(diagnostics.Errors()[0].Detail()), strings.ToLower(test.want)) {
				t.Fatalf("diagnostics=%v want=%q", diagnostics, test.want)
			}
		})
	}
}

type flowRPCFake struct {
	wallabypb.FlowServiceClient
	flow                           *wallabypb.Flow
	getErr, deleteErr              error
	created, updated, reconfigured *wallabypb.Flow
	reconfigureRequest             *wallabypb.ReconfigureFlowRequest
}

func (c *flowRPCFake) CreateFlow(_ context.Context, request *wallabypb.CreateFlowRequest, _ ...grpc.CallOption) (*wallabypb.Flow, error) {
	c.created = proto.Clone(request.Flow).(*wallabypb.Flow)
	c.created.Id = "flow-1"
	c.created.State = wallabypb.FlowState_FLOW_STATE_CREATED
	return proto.Clone(c.created).(*wallabypb.Flow), nil
}
func (c *flowRPCFake) GetFlow(context.Context, *wallabypb.GetFlowRequest, ...grpc.CallOption) (*wallabypb.Flow, error) {
	if c.getErr != nil {
		return nil, c.getErr
	}
	return proto.Clone(c.flow).(*wallabypb.Flow), nil
}
func (c *flowRPCFake) UpdateFlow(_ context.Context, request *wallabypb.UpdateFlowRequest, _ ...grpc.CallOption) (*wallabypb.Flow, error) {
	c.updated = proto.Clone(request.Flow).(*wallabypb.Flow)
	return proto.Clone(request.Flow).(*wallabypb.Flow), nil
}
func (c *flowRPCFake) ReconfigureFlow(_ context.Context, request *wallabypb.ReconfigureFlowRequest, _ ...grpc.CallOption) (*wallabypb.Flow, error) {
	c.reconfigureRequest = proto.Clone(request).(*wallabypb.ReconfigureFlowRequest)
	c.reconfigured = proto.Clone(request.Flow).(*wallabypb.Flow)
	c.reconfigured.State = wallabypb.FlowState_FLOW_STATE_RUNNING
	return proto.Clone(c.reconfigured).(*wallabypb.Flow), nil
}
func (c *flowRPCFake) DeleteFlow(context.Context, *wallabypb.DeleteFlowRequest, ...grpc.CallOption) (*wallabypb.DeleteFlowResponse, error) {
	if c.deleteErr != nil {
		return nil, c.deleteErr
	}
	return &wallabypb.DeleteFlowResponse{}, nil
}

func resourceSchema(t *testing.T) schema.Schema {
	t.Helper()
	var response resource.SchemaResponse
	(&flowResource{}).Schema(context.Background(), resource.SchemaRequest{}, &response)
	if response.Diagnostics.HasError() {
		t.Fatal(response.Diagnostics)
	}
	return response.Schema
}

func stateForModel(t *testing.T, model flowResourceModel) tfsdk.State {
	t.Helper()
	state := tfsdk.State{Schema: resourceSchema(t)}
	if diagnostics := state.Set(context.Background(), model); diagnostics.HasError() {
		t.Fatal(diagnostics)
	}
	return state
}

func idOnlyState(t *testing.T, id string) tfsdk.State {
	t.Helper()
	ctx := context.Background()
	resourceSchema := resourceSchema(t)
	objectType := resourceSchema.Type().(types.ObjectType)
	attributes := make(map[string]attr.Value, len(objectType.AttrTypes))
	for name, attributeType := range objectType.AttrTypes {
		terraformValue := tftypes.NewValue(attributeType.TerraformType(ctx), nil)
		value, err := attributeType.ValueFromTerraform(ctx, terraformValue)
		if err != nil {
			t.Fatal(err)
		}
		attributes[name] = value
	}
	attributes["id"] = types.StringValue(id)
	root := types.ObjectValueMust(objectType.AttrTypes, attributes)
	raw, err := root.ToTerraformValue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return tfsdk.State{Schema: resourceSchema, Raw: raw}
}

func planForModel(t *testing.T, model flowResourceModel) tfsdk.Plan {
	t.Helper()
	plan := tfsdk.Plan{Schema: resourceSchema(t)}
	if diagnostics := plan.Set(context.Background(), model); diagnostics.HasError() {
		t.Fatal(diagnostics)
	}
	return plan
}

func TestMappingAndConfigChangesUseControlledReconfigureWhileOrdinaryUpdatesRemainUpdate(t *testing.T) {
	ctx := context.Background()
	prior := testCompleteMappingModel()
	planned := prior
	planned.Config = &flowConfigModel{}
	*planned.Config = *prior.Config
	changedMapping := proto.Clone(completeMappingProto()).(*wallabypb.TableMappings)
	changedMapping.Destinations[0].Tables[0].TargetTable = "facts_v2"
	planned.Config.TableMappings = mappingObject(t, changedMapping)
	client := &flowRPCFake{}
	instance := &flowResource{client: &Client{Flow: client}}
	response := resource.UpdateResponse{State: stateForModel(t, prior)}
	instance.Update(ctx, resource.UpdateRequest{State: stateForModel(t, prior), Plan: planForModel(t, planned)}, &response)
	if response.Diagnostics.HasError() {
		t.Fatal(response.Diagnostics)
	}
	if client.updated != nil || client.reconfigured == nil || client.reconfigureRequest.PauseFirst == nil || !*client.reconfigureRequest.PauseFirst || client.reconfigureRequest.ResumeAfter == nil || !*client.reconfigureRequest.ResumeAfter {
		t.Fatalf("config change did not use controlled reconfigure: %+v", client)
	}
	if client.reconfigured.Config.TableMappings.Destinations[0].Tables[0].TargetTable != "facts_v2" || client.reconfigured.State != wallabypb.FlowState_FLOW_STATE_RUNNING {
		t.Fatalf("reconfigured flow=%v", client.reconfigured)
	}

	ordinary := prior
	ordinary.Parallelism = types.Int64Value(9)
	ordinaryClient := &flowRPCFake{}
	ordinaryInstance := &flowResource{client: &Client{Flow: ordinaryClient}}
	ordinaryResponse := resource.UpdateResponse{State: stateForModel(t, prior)}
	ordinaryInstance.Update(ctx, resource.UpdateRequest{State: stateForModel(t, prior), Plan: planForModel(t, ordinary)}, &ordinaryResponse)
	if ordinaryResponse.Diagnostics.HasError() || ordinaryClient.updated == nil || ordinaryClient.reconfigured != nil {
		t.Fatalf("ordinary update diagnostics=%v client=%+v", ordinaryResponse.Diagnostics, ordinaryClient)
	}

	wireChange := prior
	wireChange.WireFormat = types.StringValue("json")
	wireClient := &flowRPCFake{}
	wireInstance := &flowResource{client: &Client{Flow: wireClient}}
	wireResponse := resource.UpdateResponse{State: stateForModel(t, prior)}
	wireInstance.Update(ctx, resource.UpdateRequest{State: stateForModel(t, prior), Plan: planForModel(t, wireChange)}, &wireResponse)
	if wireResponse.Diagnostics.HasError() || wireClient.reconfigured == nil || wireClient.updated != nil || wireClient.reconfigureRequest.GetPauseFirst() != true || wireClient.reconfigureRequest.GetResumeAfter() != true {
		t.Fatalf("wire-format change diagnostics=%v client=%+v", wireResponse.Diagnostics, wireClient)
	}

	identityChange := prior
	identityChange.Source.Options = types.MapValueMust(types.StringType, map[string]attr.Value{"dsn": types.StringValue("postgres://changed")})
	identityClient := &flowRPCFake{}
	identityInstance := &flowResource{client: &Client{Flow: identityClient}}
	identityResponse := resource.UpdateResponse{State: stateForModel(t, prior)}
	identityInstance.Update(ctx, resource.UpdateRequest{State: stateForModel(t, prior), Plan: planForModel(t, identityChange)}, &identityResponse)
	if identityResponse.Diagnostics.HasError() || identityClient.reconfigured == nil || identityClient.updated != nil {
		t.Fatalf("source identity change diagnostics=%v client=%+v", identityResponse.Diagnostics, identityClient)
	}

	nameChange := prior
	nameChange.Name = types.StringValue("renamed")
	nameClient := &flowRPCFake{}
	nameInstance := &flowResource{client: &Client{Flow: nameClient}}
	nameResponse := resource.UpdateResponse{State: stateForModel(t, prior)}
	nameInstance.Update(ctx, resource.UpdateRequest{State: stateForModel(t, prior), Plan: planForModel(t, nameChange)}, &nameResponse)
	if nameResponse.Diagnostics.HasError() || nameClient.updated == nil || nameClient.reconfigured != nil || nameClient.updated.Name != "renamed" {
		t.Fatalf("name change diagnostics=%v client=%+v", nameResponse.Diagnostics, nameClient)
	}
}

func TestReadAndDeletePreserveStateExceptAuthoritativeNotFound(t *testing.T) {
	ctx := context.Background()
	model := testCompleteMappingModel()
	wire, diagnostics := flowModelToProto(ctx, model)
	if diagnostics.HasError() {
		t.Fatal(diagnostics)
	}
	for _, test := range []struct {
		name   string
		code   codes.Code
		remove bool
	}{
		{"not found", codes.NotFound, true}, {"unavailable", codes.Unavailable, false}, {"unauthenticated", codes.Unauthenticated, false},
	} {
		t.Run("read "+test.name, func(t *testing.T) {
			client := &flowRPCFake{flow: wire, getErr: status.Error(test.code, test.name)}
			instance := &flowResource{client: &Client{Flow: client}}
			priorState := stateForModel(t, model)
			response := resource.ReadResponse{State: priorState}
			instance.Read(ctx, resource.ReadRequest{State: priorState}, &response)
			if test.remove {
				if response.Diagnostics.HasError() || !response.State.Raw.IsNull() {
					t.Fatalf("NotFound read state=%v diagnostics=%v", response.State.Raw, response.Diagnostics)
				}
			} else {
				if !response.Diagnostics.HasError() || response.State.Raw.IsNull() {
					t.Fatalf("transient read state=%v diagnostics=%v", response.State.Raw, response.Diagnostics)
				}
			}
		})
		t.Run("delete "+test.name, func(t *testing.T) {
			client := &flowRPCFake{deleteErr: status.Error(test.code, test.name)}
			instance := &flowResource{client: &Client{Flow: client}}
			priorState := stateForModel(t, model)
			response := resource.DeleteResponse{State: priorState}
			instance.Delete(ctx, resource.DeleteRequest{State: priorState}, &response)
			if test.remove {
				if response.Diagnostics.HasError() || !response.State.Raw.IsNull() {
					t.Fatalf("NotFound delete state=%v diagnostics=%v", response.State.Raw, response.Diagnostics)
				}
			} else {
				if !response.Diagnostics.HasError() || response.State.Raw.IsNull() {
					t.Fatalf("transient delete state=%v diagnostics=%v", response.State.Raw, response.Diagnostics)
				}
			}
		})
	}
	client := &flowRPCFake{}
	instance := &flowResource{client: &Client{Flow: client}}
	priorState := stateForModel(t, model)
	response := resource.DeleteResponse{State: priorState}
	instance.Delete(ctx, resource.DeleteRequest{State: priorState}, &response)
	if response.Diagnostics.HasError() || !response.State.Raw.IsNull() {
		t.Fatalf("successful delete state=%v diagnostics=%v", response.State.Raw, response.Diagnostics)
	}
}

func TestFlowImportThenReadRestoresCompleteMappingState(t *testing.T) {
	ctx := context.Background()
	model := testCompleteMappingModel()
	wire, diagnostics := flowModelToProto(ctx, model)
	if diagnostics.HasError() {
		t.Fatal(diagnostics)
	}
	encoded, err := proto.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	apiFlow := &wallabypb.Flow{}
	if err := proto.Unmarshal(encoded, apiFlow); err != nil {
		t.Fatal(err)
	}
	instance := &flowResource{client: &Client{Flow: &flowRPCFake{flow: apiFlow}}}
	initialState := idOnlyState(t, "")
	importResponse := resource.ImportStateResponse{State: initialState}
	instance.ImportState(ctx, resource.ImportStateRequest{ID: wire.Id}, &importResponse)
	if importResponse.Diagnostics.HasError() {
		t.Fatal(importResponse.Diagnostics)
	}
	var importedID types.String
	if diagnostics := importResponse.State.GetAttribute(ctx, path.Root("id"), &importedID); diagnostics.HasError() || importedID.ValueString() != wire.Id {
		t.Fatalf("imported id=%v diagnostics=%v", importedID, diagnostics)
	}
	readResponse := resource.ReadResponse{State: importResponse.State}
	instance.Read(ctx, resource.ReadRequest{State: importResponse.State}, &readResponse)
	if readResponse.Diagnostics.HasError() {
		t.Fatal(readResponse.Diagnostics)
	}
	var refreshed flowResourceModel
	if diagnostics := readResponse.State.Get(ctx, &refreshed); diagnostics.HasError() {
		t.Fatal(diagnostics)
	}
	refreshedDestinations := objectList(t, objectField(t, refreshed.Config.TableMappings, "destinations"))
	refreshedTables := objectList(t, objectField(t, refreshedDestinations[0], "tables"))
	if future := objectField(t, refreshedTables[1], "future_columns").(types.Object); !future.IsNull() {
		t.Fatalf("excluded future_columns changed across import: %v", future)
	}
	archiveTables := objectField(t, refreshedDestinations[1], "tables").(types.List)
	if archiveTables.IsNull() || len(archiveTables.Elements()) != 0 {
		t.Fatalf("empty tables changed across import: %v", archiveTables)
	}
	roundTrip, conversionDiagnostics := flowModelToProto(ctx, refreshed)
	if conversionDiagnostics.HasError() || !proto.Equal(wire.Config, roundTrip.Config) {
		t.Fatalf("import/read config diagnostics=%v\nwant=%v\ngot=%v", conversionDiagnostics, wire.Config, roundTrip.Config)
	}
}

func TestFlowResourceSchemaExposesDurableMappingsWithoutReplacementOrFilePath(t *testing.T) {
	var response resource.SchemaResponse
	(&flowResource{}).Schema(context.Background(), resource.SchemaRequest{}, &response)
	if diagnostics := response.Schema.ValidateImplementation(context.Background()); response.Diagnostics.HasError() || diagnostics.HasError() {
		t.Fatalf("schema diagnostics=%v implementation=%v", response.Diagnostics, diagnostics)
	}
	config, ok := response.Schema.Attributes["config"].(schema.SingleNestedAttribute)
	if !ok || !config.Required {
		t.Fatalf("config schema=%T %+v", response.Schema.Attributes["config"], config)
	}
	for _, name := range []string{"schema_registry_subject", "schema_registry_proto_types_subject", "schema_registry_subject_mode"} {
		if _, ok := config.Attributes[name]; !ok {
			t.Fatalf("missing config attribute %q", name)
		}
	}
	mappings, ok := config.Attributes["table_mappings"].(schema.SingleNestedAttribute)
	if !ok || !mappings.Required || len(mappings.PlanModifiers) != 0 {
		t.Fatalf("table_mappings schema=%T %+v", config.Attributes["table_mappings"], mappings)
	}
	for _, name := range []string{"name", "wire_format"} {
		attribute, ok := response.Schema.Attributes[name].(schema.StringAttribute)
		if !ok || len(attribute.PlanModifiers) != 0 {
			t.Fatalf("top-level %s unexpectedly requires replacement: %+v", name, attribute)
		}
	}
	destinationMappings, ok := mappings.Attributes["destinations"].(schema.ListNestedAttribute)
	if !ok || !destinationMappings.Required {
		t.Fatalf("mapping destinations must be required: %+v", mappings.Attributes["destinations"])
	}
	tables, ok := destinationMappings.NestedObject.Attributes["tables"].(schema.ListNestedAttribute)
	if !ok || !tables.Required {
		t.Fatalf("destination tables must be required: %+v", destinationMappings.NestedObject.Attributes["tables"])
	}
	columns, ok := tables.NestedObject.Attributes["columns"].(schema.ListNestedAttribute)
	if !ok || !columns.Required {
		t.Fatalf("table columns must be required: %+v", tables.NestedObject.Attributes["columns"])
	}
	futureTables := destinationMappings.NestedObject.Attributes["future_tables"].(schema.SingleNestedAttribute)
	futureWrite := futureTables.Attributes["write"].(schema.SingleNestedAttribute)
	futureKeys := futureWrite.Attributes["key_columns"].(schema.ListAttribute)
	exactWrite := tables.NestedObject.Attributes["write"].(schema.SingleNestedAttribute)
	exactKeys := exactWrite.Attributes["key_columns"].(schema.ListAttribute)
	if !futureKeys.Required || !exactKeys.Required {
		t.Fatalf("write key_columns must be required: future=%+v exact=%+v", futureKeys, exactKeys)
	}
	if _, ok := config.Attributes["table_mappings_file"]; ok {
		t.Fatal("table_mappings_file leaked into Terraform schema")
	}
	if _, ok := interface{}(&flowResource{}).(resource.ResourceWithImportState); !ok {
		t.Fatal("flow resource does not implement import state")
	}
}
