package main

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/listdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/mapdefault"
	"github.com/hashicorp/terraform-plugin-framework/types"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
)

type endpointModel struct {
	Name                       types.String `tfsdk:"name"`
	PostgresSource             types.Object `tfsdk:"postgres_source"`
	PostgresDestination        types.Object `tfsdk:"postgres_destination"`
	PGStream                   types.Object `tfsdk:"pgstream"`
	Kafka                      types.Object `tfsdk:"kafka"`
	Redpanda                   types.Object `tfsdk:"redpanda"`
	S3                         types.Object `tfsdk:"s3"`
	HTTP                       types.Object `tfsdk:"http"`
	GRPC                       types.Object `tfsdk:"grpc"`
	Snowflake                  types.Object `tfsdk:"snowflake"`
	SnowflakePostgresSQL       types.Object `tfsdk:"snowflake_postgres_sql"`
	SnowflakePostgresStaged    types.Object `tfsdk:"snowflake_postgres_staged"`
	SnowflakePostgresStreaming types.Object `tfsdk:"snowflake_postgres_streaming"`
	Snowpipe                   types.Object `tfsdk:"snowpipe"`
	ClickHouse                 types.Object `tfsdk:"clickhouse"`
	ClickHousePostgresAppend   types.Object `tfsdk:"clickhouse_postgres_append"`
	DuckDB                     types.Object `tfsdk:"duckdb"`
	DuckLake                   types.Object `tfsdk:"ducklake"`
	Iceberg                    types.Object `tfsdk:"iceberg"`
	Custom                     types.Object `tfsdk:"custom"`
}

func endpointSchemaAttributes() map[string]schema.Attribute {
	attributes := map[string]schema.Attribute{"name": schema.StringAttribute{Optional: true}}
	descriptor := (&wallabypb.Endpoint{}).ProtoReflect().Descriptor()
	fields := descriptor.Oneofs().ByName("config").Fields()
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		attributes[string(field.Name())] = schema.SingleNestedAttribute{Optional: true, Attributes: protoMessageSchema(field.Message())}
	}
	return attributes
}

func protoMessageSchema(descriptor protoreflect.MessageDescriptor) map[string]schema.Attribute {
	attributes := make(map[string]schema.Attribute, descriptor.Fields().Len())
	for index := 0; index < descriptor.Fields().Len(); index++ {
		field := descriptor.Fields().Get(index)
		name := string(field.Name())
		sensitive := sensitiveProtoField(field)
		switch {
		case field.IsMap():
			elementType := terraformScalarType(field.MapValue())
			attributes[name] = schema.MapAttribute{
				Optional: true, Computed: true, ElementType: elementType, Sensitive: sensitive,
				Default: mapdefault.StaticValue(types.MapValueMust(elementType, map[string]attr.Value{})),
			}
		case field.IsList():
			elementType := terraformScalarType(field)
			attributes[name] = schema.ListAttribute{
				Optional: true, Computed: true, ElementType: elementType, Sensitive: sensitive,
				Default: listdefault.StaticValue(types.ListValueMust(elementType, []attr.Value{})),
			}
		case field.Kind() == protoreflect.MessageKind && field.Message().FullName() == "google.protobuf.Duration":
			attributes[name] = schema.StringAttribute{Optional: true}
		case field.Kind() == protoreflect.MessageKind:
			attributes[name] = schema.SingleNestedAttribute{Optional: true, Attributes: protoMessageSchema(field.Message()), Sensitive: sensitive}
		case field.Kind() == protoreflect.BoolKind:
			attributes[name] = schema.BoolAttribute{Optional: true, Sensitive: sensitive}
		case field.Kind() == protoreflect.DoubleKind || field.Kind() == protoreflect.FloatKind:
			attributes[name] = schema.Float64Attribute{Optional: true, Sensitive: sensitive}
		case field.Kind() == protoreflect.StringKind || field.Kind() == protoreflect.EnumKind:
			required := descriptor.FullName() == "wallaby.v1.CustomEndpointConfig" && field.Name() == "connector_type"
			attributes[name] = schema.StringAttribute{Optional: !required, Required: required, Sensitive: sensitive}
		default:
			attributes[name] = schema.Int64Attribute{Optional: true, Sensitive: sensitive}
		}
	}
	return attributes
}

func sensitiveProtoField(field protoreflect.FieldDescriptor) bool {
	name := strings.ToLower(string(field.Name()))
	if field.ContainingMessage().FullName() == "wallaby.v1.CustomEndpointConfig" && field.Name() == "options" {
		return true
	}
	for _, marker := range []string{"dsn", "password", "secret", "token", "credential", "access_key", "private_key", "key_file", "external_id", "authorization", "headers", "metadata"} {
		if strings.Contains(name, marker) {
			return true
		}
	}
	return false
}

func terraformScalarType(field protoreflect.FieldDescriptor) attr.Type {
	switch field.Kind() {
	case protoreflect.BoolKind:
		return types.BoolType
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		return types.Float64Type
	case protoreflect.StringKind, protoreflect.EnumKind:
		return types.StringType
	default:
		return types.Int64Type
	}
}

func endpointModelToProto(ctx context.Context, model endpointModel, source bool) (*wallabypb.Endpoint, diag.Diagnostics) {
	var diagnostics diag.Diagnostics
	if model.Name.IsUnknown() {
		diagnostics.AddError("Unknown endpoint name", "endpoint name must be known before apply")
		return nil, diagnostics
	}
	endpoint := &wallabypb.Endpoint{}
	if !model.Name.IsNull() {
		endpoint.Name = model.Name.ValueString()
	}
	objects := model.branchObjects()
	descriptor := endpoint.ProtoReflect().Descriptor()
	oneof := descriptor.Oneofs().ByName("config")
	selected := 0
	for index := 0; index < oneof.Fields().Len(); index++ {
		field := oneof.Fields().Get(index)
		object := objects[string(field.Name())]
		if object.IsUnknown() {
			diagnostics.AddError("Unknown endpoint branch", fmt.Sprintf("%s must be known before apply", field.Name()))
			continue
		}
		if object.IsNull() {
			continue
		}
		selected++
		child := endpoint.ProtoReflect().NewField(field).Message()
		populateProtoMessage(ctx, child, object, string(field.Name()), &diagnostics)
		endpoint.ProtoReflect().Set(field, protoreflect.ValueOfMessage(child))
	}
	if selected != 1 {
		diagnostics.AddError("Invalid endpoint branches", fmt.Sprintf("exactly one typed endpoint branch must be configured; found %d", selected))
		return nil, diagnostics
	}
	if source {
		switch endpoint.GetConfig().(type) {
		case *wallabypb.Endpoint_PostgresSource, *wallabypb.Endpoint_Custom:
		default:
			diagnostics.AddError("Invalid source endpoint", "source must use postgres_source or custom")
		}
	} else {
		if _, invalid := endpoint.GetConfig().(*wallabypb.Endpoint_PostgresSource); invalid {
			diagnostics.AddError("Invalid destination endpoint", "postgres_source is not a destination branch")
		}
	}
	return endpoint, diagnostics
}

func (model endpointModel) branchObjects() map[string]types.Object {
	return map[string]types.Object{
		"postgres_source": model.PostgresSource, "postgres_destination": model.PostgresDestination,
		"pgstream": model.PGStream, "kafka": model.Kafka, "redpanda": model.Redpanda, "s3": model.S3,
		"http": model.HTTP, "grpc": model.GRPC, "snowflake": model.Snowflake,
		"snowflake_postgres_sql": model.SnowflakePostgresSQL, "snowflake_postgres_staged": model.SnowflakePostgresStaged,
		"snowflake_postgres_streaming": model.SnowflakePostgresStreaming, "snowpipe": model.Snowpipe,
		"clickhouse": model.ClickHouse, "clickhouse_postgres_append": model.ClickHousePostgresAppend,
		"duckdb": model.DuckDB, "ducklake": model.DuckLake, "iceberg": model.Iceberg, "custom": model.Custom,
	}
}

func populateProtoMessage(ctx context.Context, message protoreflect.Message, object types.Object, path string, diagnostics *diag.Diagnostics) {
	attributes := object.Attributes()
	descriptor := message.Descriptor()
	for oneofIndex := 0; oneofIndex < descriptor.Oneofs().Len(); oneofIndex++ {
		oneof := descriptor.Oneofs().Get(oneofIndex)
		if oneof.IsSynthetic() {
			continue
		}
		selected := 0
		unknown := false
		for fieldIndex := 0; fieldIndex < oneof.Fields().Len(); fieldIndex++ {
			field := oneof.Fields().Get(fieldIndex)
			value, exists := attributes[string(field.Name())]
			if !exists || value.IsNull() {
				continue
			}
			if value.IsUnknown() {
				unknown = true
				continue
			}
			selected++
		}
		if unknown {
			diagnostics.AddError("Unknown nested oneof branch", fmt.Sprintf("%s.%s branches must be fully known before apply", path, oneof.Name()))
			return
		}
		if selected != 1 {
			diagnostics.AddError("Invalid nested oneof", fmt.Sprintf("%s.%s requires exactly one branch; found %d", path, oneof.Name(), selected))
			return
		}
	}
	for index := 0; index < descriptor.Fields().Len(); index++ {
		field := descriptor.Fields().Get(index)
		value, exists := attributes[string(field.Name())]
		if !exists || value.IsNull() {
			continue
		}
		fieldPath := path + "." + string(field.Name())
		if value.IsUnknown() {
			diagnostics.AddError("Unknown endpoint value", fieldPath+" must be known before apply")
			continue
		}
		switch {
		case field.IsMap():
			values := map[string]string{}
			mapValue, ok := value.(types.Map)
			if !ok {
				diagnostics.AddError("Invalid endpoint map", fieldPath+" must be a map")
				continue
			}
			diagnostics.Append(mapValue.ElementsAs(ctx, &values, false)...)
			target := message.Mutable(field).Map()
			for key, item := range values {
				target.Set(protoreflect.ValueOfString(key).MapKey(), protoreflect.ValueOfString(item))
			}
		case field.IsList():
			listValue, ok := value.(types.List)
			if !ok {
				diagnostics.AddError("Invalid endpoint list", fieldPath+" must be a list")
				continue
			}
			var values []string
			diagnostics.Append(listValue.ElementsAs(ctx, &values, false)...)
			target := message.Mutable(field).List()
			for _, item := range values {
				target.Append(protoreflect.ValueOfString(item))
			}
		case field.Kind() == protoreflect.MessageKind && field.Message().FullName() == "google.protobuf.Duration":
			raw := value.(types.String).ValueString()
			duration, err := time.ParseDuration(raw)
			if err != nil {
				diagnostics.AddError("Invalid duration", fmt.Sprintf("%s: %v", fieldPath, err))
				continue
			}
			parsed := durationpb.New(duration)
			if err := parsed.CheckValid(); err != nil {
				diagnostics.AddError("Invalid duration", fmt.Sprintf("%s: %v", fieldPath, err))
				continue
			}
			message.Set(field, protoreflect.ValueOfMessage(parsed.ProtoReflect()))
		case field.Kind() == protoreflect.MessageKind:
			childObject, ok := value.(types.Object)
			if !ok {
				diagnostics.AddError("Invalid endpoint object", fieldPath+" must be an object")
				continue
			}
			child := message.NewField(field).Message()
			populateProtoMessage(ctx, child, childObject, fieldPath, diagnostics)
			message.Set(field, protoreflect.ValueOfMessage(child))
		case field.Kind() == protoreflect.StringKind:
			message.Set(field, protoreflect.ValueOfString(value.(types.String).ValueString()))
		case field.Kind() == protoreflect.EnumKind:
			name := value.(types.String).ValueString()
			enum := field.Enum().Values().ByName(protoreflect.Name(name))
			if enum == nil {
				diagnostics.AddError("Invalid endpoint enum", fmt.Sprintf("%s has unknown value %q", fieldPath, name))
				continue
			}
			message.Set(field, protoreflect.ValueOfEnum(enum.Number()))
		case field.Kind() == protoreflect.BoolKind:
			message.Set(field, protoreflect.ValueOfBool(value.(types.Bool).ValueBool()))
		case field.Kind() == protoreflect.DoubleKind, field.Kind() == protoreflect.FloatKind:
			number := value.(types.Float64).ValueFloat64()
			if math.IsNaN(number) || math.IsInf(number, 0) {
				diagnostics.AddError("Invalid endpoint number", fieldPath+" must be finite")
				continue
			}
			message.Set(field, protoreflect.ValueOfFloat64(number))
		default:
			setIntegerField(message, field, value.(types.Int64).ValueInt64(), fieldPath, diagnostics)
		}
	}
}

func setIntegerField(message protoreflect.Message, field protoreflect.FieldDescriptor, value int64, path string, diagnostics *diag.Diagnostics) {
	switch field.Kind() {
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		if value < 0 || value > math.MaxUint32 {
			diagnostics.AddError("Endpoint integer out of range", path+" must fit uint32")
			return
		}
		message.Set(field, protoreflect.ValueOfUint32(uint32(value)))
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		if value < 0 {
			diagnostics.AddError("Endpoint integer out of range", path+" must be non-negative")
			return
		}
		message.Set(field, protoreflect.ValueOfUint64(uint64(value)))
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		if value < math.MinInt32 || value > math.MaxInt32 {
			diagnostics.AddError("Endpoint integer out of range", path+" must fit int32")
			return
		}
		message.Set(field, protoreflect.ValueOfInt32(int32(value)))
	default:
		message.Set(field, protoreflect.ValueOfInt64(value))
	}
}

func endpointFromProto(item *wallabypb.Endpoint) endpointModel {
	model := nullEndpointModel()
	if item == nil {
		return model
	}
	model.Name = types.StringValue(item.GetName())
	field := item.ProtoReflect().WhichOneof(item.ProtoReflect().Descriptor().Oneofs().ByName("config"))
	if field == nil {
		return model
	}
	object := protoMessageToObject(item.ProtoReflect().Get(field).Message())
	model.setBranchObject(string(field.Name()), object)
	return model
}

func endpointsFromProto(items []*wallabypb.Endpoint) []endpointModel {
	out := make([]endpointModel, 0, len(items))
	for _, item := range items {
		out = append(out, endpointFromProto(item))
	}
	return out
}

func nullEndpointModel() endpointModel {
	model := endpointModel{Name: types.StringNull()}
	descriptor := (&wallabypb.Endpoint{}).ProtoReflect().Descriptor().Oneofs().ByName("config")
	for index := 0; index < descriptor.Fields().Len(); index++ {
		field := descriptor.Fields().Get(index)
		model.setBranchObject(string(field.Name()), types.ObjectNull(protoObjectAttributeTypes(field.Message())))
	}
	return model
}

func (model *endpointModel) setBranchObject(name string, value types.Object) {
	switch name {
	case "postgres_source":
		model.PostgresSource = value
	case "postgres_destination":
		model.PostgresDestination = value
	case "pgstream":
		model.PGStream = value
	case "kafka":
		model.Kafka = value
	case "redpanda":
		model.Redpanda = value
	case "s3":
		model.S3 = value
	case "http":
		model.HTTP = value
	case "grpc":
		model.GRPC = value
	case "snowflake":
		model.Snowflake = value
	case "snowflake_postgres_sql":
		model.SnowflakePostgresSQL = value
	case "snowflake_postgres_staged":
		model.SnowflakePostgresStaged = value
	case "snowflake_postgres_streaming":
		model.SnowflakePostgresStreaming = value
	case "snowpipe":
		model.Snowpipe = value
	case "clickhouse":
		model.ClickHouse = value
	case "clickhouse_postgres_append":
		model.ClickHousePostgresAppend = value
	case "duckdb":
		model.DuckDB = value
	case "ducklake":
		model.DuckLake = value
	case "iceberg":
		model.Iceberg = value
	case "custom":
		model.Custom = value
	}
}

func protoObjectAttributeTypes(descriptor protoreflect.MessageDescriptor) map[string]attr.Type {
	result := make(map[string]attr.Type, descriptor.Fields().Len())
	for index := 0; index < descriptor.Fields().Len(); index++ {
		field := descriptor.Fields().Get(index)
		switch {
		case field.IsMap():
			result[string(field.Name())] = types.MapType{ElemType: terraformScalarType(field.MapValue())}
		case field.IsList():
			result[string(field.Name())] = types.ListType{ElemType: terraformScalarType(field)}
		case field.Kind() == protoreflect.MessageKind && field.Message().FullName() == "google.protobuf.Duration":
			result[string(field.Name())] = types.StringType
		case field.Kind() == protoreflect.MessageKind:
			result[string(field.Name())] = types.ObjectType{AttrTypes: protoObjectAttributeTypes(field.Message())}
		default:
			result[string(field.Name())] = terraformScalarType(field)
		}
	}
	return result
}

func protoMessageToObject(message protoreflect.Message) types.Object {
	descriptor := message.Descriptor()
	attributeTypes := protoObjectAttributeTypes(descriptor)
	attributes := make(map[string]attr.Value, descriptor.Fields().Len())
	for index := 0; index < descriptor.Fields().Len(); index++ {
		field := descriptor.Fields().Get(index)
		name := string(field.Name())
		value := message.Get(field)
		switch {
		case field.IsMap():
			items := map[string]attr.Value{}
			value.Map().Range(func(key protoreflect.MapKey, item protoreflect.Value) bool {
				items[key.String()] = types.StringValue(item.String())
				return true
			})
			attributes[name] = types.MapValueMust(types.StringType, items)
			continue
		case field.IsList():
			items := make([]attr.Value, 0, value.List().Len())
			for item := 0; item < value.List().Len(); item++ {
				items = append(items, types.StringValue(value.List().Get(item).String()))
			}
			attributes[name] = types.ListValueMust(types.StringType, items)
			continue
		}
		if !message.Has(field) {
			attributes[name] = nullProtoFieldValue(field)
			continue
		}
		switch {
		case field.Kind() == protoreflect.MessageKind && field.Message().FullName() == "google.protobuf.Duration":
			duration := durationpb.Duration{Seconds: value.Message().Get(field.Message().Fields().ByName("seconds")).Int(), Nanos: int32(value.Message().Get(field.Message().Fields().ByName("nanos")).Int())}
			attributes[name] = types.StringValue(duration.AsDuration().String())
		case field.Kind() == protoreflect.MessageKind:
			attributes[name] = protoMessageToObject(value.Message())
		case field.Kind() == protoreflect.StringKind:
			attributes[name] = types.StringValue(value.String())
		case field.Kind() == protoreflect.EnumKind:
			attributes[name] = types.StringValue(string(field.Enum().Values().ByNumber(value.Enum()).Name()))
		case field.Kind() == protoreflect.BoolKind:
			attributes[name] = types.BoolValue(value.Bool())
		case field.Kind() == protoreflect.DoubleKind, field.Kind() == protoreflect.FloatKind:
			attributes[name] = types.Float64Value(value.Float())
		case field.Kind() == protoreflect.Uint32Kind, field.Kind() == protoreflect.Fixed32Kind, field.Kind() == protoreflect.Uint64Kind, field.Kind() == protoreflect.Fixed64Kind:
			if value.Uint() > math.MaxInt64 {
				attributes[name] = types.Int64Unknown()
			} else {
				attributes[name] = types.Int64Value(int64(value.Uint()))
			}
		default:
			attributes[name] = types.Int64Value(value.Int())
		}
	}
	return types.ObjectValueMust(attributeTypes, attributes)
}

func nullProtoFieldValue(field protoreflect.FieldDescriptor) attr.Value {
	switch {
	case field.IsMap():
		return types.MapNull(terraformScalarType(field.MapValue()))
	case field.IsList():
		return types.ListNull(terraformScalarType(field))
	case field.Kind() == protoreflect.MessageKind && field.Message().FullName() == "google.protobuf.Duration":
		return types.StringNull()
	case field.Kind() == protoreflect.MessageKind:
		return types.ObjectNull(protoObjectAttributeTypes(field.Message()))
	case field.Kind() == protoreflect.BoolKind:
		return types.BoolNull()
	case field.Kind() == protoreflect.DoubleKind, field.Kind() == protoreflect.FloatKind:
		return types.Float64Null()
	case field.Kind() == protoreflect.StringKind, field.Kind() == protoreflect.EnumKind:
		return types.StringNull()
	default:
		return types.Int64Null()
	}
}

var _ = strconv.IntSize
