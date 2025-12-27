package wire

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func buildArrowRecord(batch connector.Batch) (arrow.Record, error) {
	if len(batch.Records) == 0 {
		return nil, nil
	}

	schema, fieldIndex, err := arrowSchemaFor(batch.Schema)
	if err != nil {
		return nil, err
	}

	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	for _, record := range batch.Records {
		appendString(builder.Field(fieldIndex["__op"]).(*array.StringBuilder), string(record.Operation))
		appendTimestamp(builder.Field(fieldIndex["__ts"]).(*array.TimestampBuilder), record.Timestamp)
		appendInt64(builder.Field(fieldIndex["__schema_version"]).(*array.Int64Builder), record.SchemaVersion)
		appendString(builder.Field(fieldIndex["__table"]).(*array.StringBuilder), record.Table)
		appendString(builder.Field(fieldIndex["__namespace"]).(*array.StringBuilder), batch.Schema.Namespace)
		appendBinary(builder.Field(fieldIndex["__key"]).(*array.BinaryBuilder), record.Key)

		var beforeJSON []byte
		if record.Before != nil {
			beforeJSON, _ = json.Marshal(record.Before)
		}
		appendBinary(builder.Field(fieldIndex["__before_json"]).(*array.BinaryBuilder), beforeJSON)

		for _, col := range batch.Schema.Columns {
			idx, ok := fieldIndex[col.Name]
			if !ok {
				continue
			}
			val := record.After[col.Name]
			if record.Operation == connector.OpDelete {
				val = nil
			}
			if err := appendValue(builder.Field(idx), val); err != nil {
				return nil, fmt.Errorf("append field %s: %w", col.Name, err)
			}
		}
	}

	rec := builder.NewRecord()
	return rec, nil
}

func arrowSchemaFor(schema connector.Schema) (*arrow.Schema, map[string]int, error) {
	fields := []arrow.Field{
		{Name: "__op", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "__ts", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: true},
		{Name: "__schema_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "__table", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "__namespace", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "__key", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "__before_json", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}

	fieldIndex := map[string]int{}
	for idx, field := range fields {
		fieldIndex[field.Name] = idx
	}

	for _, col := range schema.Columns {
		if _, exists := fieldIndex[col.Name]; exists {
			return nil, nil, fmt.Errorf("column name collides with metadata field: %s", col.Name)
		}
		fields = append(fields, arrow.Field{
			Name:     col.Name,
			Type:     arrowTypeFor(col.Type),
			Nullable: true,
		})
		fieldIndex[col.Name] = len(fields) - 1
	}

	return arrow.NewSchema(fields, nil), fieldIndex, nil
}

func arrowTypeFor(pgType string) arrow.DataType {
	base := strings.ToLower(pgType)
	if idx := strings.Index(base, "("); idx > 0 {
		base = base[:idx]
	}
	if idx := strings.LastIndex(base, "."); idx > 0 {
		base = base[idx+1:]
	}

	switch base {
	case "int2", "smallint":
		return arrow.PrimitiveTypes.Int16
	case "int4", "integer":
		return arrow.PrimitiveTypes.Int32
	case "int8", "bigint":
		return arrow.PrimitiveTypes.Int64
	case "float4", "real":
		return arrow.PrimitiveTypes.Float32
	case "float8", "double precision":
		return arrow.PrimitiveTypes.Float64
	case "bool", "boolean":
		return arrow.FixedWidthTypes.Boolean
	case "text", "varchar", "character varying", "bpchar", "char":
		return arrow.BinaryTypes.String
	case "uuid":
		return arrow.BinaryTypes.String
	case "bytea":
		return arrow.BinaryTypes.Binary
	case "timestamp", "timestamptz", "timestamp without time zone", "timestamp with time zone":
		return arrow.FixedWidthTypes.Timestamp_ms
	case "date":
		return arrow.FixedWidthTypes.Date32
	default:
		return arrow.BinaryTypes.String
	}
}

func appendString(builder *array.StringBuilder, value string) {
	if value == "" {
		builder.AppendNull()
		return
	}
	builder.Append(value)
}

func appendTimestamp(builder *array.TimestampBuilder, value time.Time) {
	if value.IsZero() {
		builder.AppendNull()
		return
	}
	builder.Append(arrow.Timestamp(value.UnixMilli()))
}

func appendInt64(builder *array.Int64Builder, value int64) {
	builder.Append(value)
}

func appendBinary(builder *array.BinaryBuilder, value []byte) {
	if len(value) == 0 {
		builder.AppendNull()
		return
	}
	builder.Append(value)
}

func appendValue(builder array.Builder, value any) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	case *array.StringBuilder:
		switch v := value.(type) {
		case string:
			b.Append(v)
		case []byte:
			b.Append(string(v))
		case json.RawMessage:
			b.Append(string(v))
		case map[string]any, []any:
			payload, err := json.Marshal(v)
			if err != nil {
				return fmt.Errorf("marshal json value: %w", err)
			}
			b.Append(string(payload))
		default:
			b.Append(fmt.Sprint(value))
		}
	case *array.Int16Builder:
		b.Append(int16(asInt64(value)))
	case *array.Int32Builder:
		b.Append(int32(asInt64(value)))
	case *array.Int64Builder:
		b.Append(asInt64(value))
	case *array.Float32Builder:
		b.Append(float32(asFloat64(value)))
	case *array.Float64Builder:
		b.Append(asFloat64(value))
	case *array.BooleanBuilder:
		b.Append(asBool(value))
	case *array.BinaryBuilder:
		switch v := value.(type) {
		case []byte:
			b.Append(v)
		default:
			b.Append([]byte(fmt.Sprint(value)))
		}
	case *array.TimestampBuilder:
		switch v := value.(type) {
		case time.Time:
			appendTimestamp(b, v)
		default:
			return fmt.Errorf("unsupported timestamp type %T", value)
		}
	case *array.Date32Builder:
		switch v := value.(type) {
		case time.Time:
			b.Append(arrow.Date32FromTime(v))
		default:
			return fmt.Errorf("unsupported date type %T", value)
		}
	default:
		return fmt.Errorf("unsupported builder type %T", builder)
	}

	return nil
}

func asInt64(value any) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		return int64(v)
	case float32:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func asFloat64(value any) float64 {
	switch v := value.(type) {
	case int:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint:
		return float64(v)
	case uint16:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	default:
		return 0
	}
}

func asBool(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return v == "true" || v == "1"
	case int:
		return v != 0
	case int64:
		return v != 0
	default:
		return false
	}
}
