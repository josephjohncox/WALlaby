package wire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/hamba/avro/v2/ocf"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// AvroCodec encodes batches as Avro OCF.
type AvroCodec struct{}

func (c *AvroCodec) Name() connector.WireFormat {
	return connector.WireFormatAvro
}

func (c *AvroCodec) ContentType() string {
	return "application/avro"
}

func (c *AvroCodec) Encode(batch connector.Batch) ([]byte, error) {
	if len(batch.Records) == 0 {
		return nil, nil
	}

	schema := avroSchemaFor(batch.Schema)

	buf := bytes.NewBuffer(nil)
	writer, err := ocf.NewEncoder(schema, buf)
	if err != nil {
		return nil, fmt.Errorf("avro encoder: %w", err)
	}

	for _, record := range batch.Records {
		row := make(map[string]any)
		row["__op"] = string(record.Operation)
		row["__ts"] = record.Timestamp.UnixMilli()
		row["__schema_version"] = record.SchemaVersion
		row["__table"] = record.Table
		row["__namespace"] = batch.Schema.Namespace
		row["__key"] = record.Key
		if record.Before != nil {
			beforeJSON, _ := json.Marshal(record.Before)
			row["__before_json"] = string(beforeJSON)
		} else {
			row["__before_json"] = ""
		}

		for _, col := range batch.Schema.Columns {
			val := record.After[col.Name]
			if record.Operation == connector.OpDelete {
				val = nil
			}
			row[col.Name] = normalizeAvroValue(col.Type, val)
		}

		if err := writer.Encode(row); err != nil {
			return nil, fmt.Errorf("encode avro row: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close avro writer: %w", err)
	}

	return buf.Bytes(), nil
}

func avroSchemaFor(schema connector.Schema) string {
	fields := []string{
		fieldJSON("__op", []string{"string"}),
		fieldJSON("__ts", []string{"long"}),
		fieldJSON("__schema_version", []string{"long"}),
		fieldJSON("__table", []string{"string"}),
		fieldJSON("__namespace", []string{"string"}),
		fieldJSON("__key", []string{"null", "bytes"}),
		fieldJSON("__before_json", []string{"string"}),
	}

	for _, col := range schema.Columns {
		fields = append(fields, fieldJSON(col.Name, avroTypeFor(col.Type, col.Nullable)))
	}

	return fmt.Sprintf(`{"type":"record","name":"wallaby_record","fields":[%s]}`, strings.Join(fields, ","))
}

func fieldJSON(name string, types []string) string {
	var typeJSON string
	if len(types) == 1 {
		typeJSON = fmt.Sprintf("\"%s\"", types[0])
	} else {
		parts := make([]string, 0, len(types))
		for _, t := range types {
			parts = append(parts, fmt.Sprintf("\"%s\"", t))
		}
		typeJSON = fmt.Sprintf("[%s]", strings.Join(parts, ","))
	}
	return fmt.Sprintf(`{"name":"%s","type":%s}`, name, typeJSON)
}

func avroTypeFor(pgType string, nullable bool) []string {
	base := strings.ToLower(pgType)
	if idx := strings.Index(base, "("); idx > 0 {
		base = base[:idx]
	}
	if idx := strings.LastIndex(base, "."); idx > 0 {
		base = base[idx+1:]
	}

	avroType := "string"
	switch base {
	case "int2", "smallint", "int4", "integer", "int8", "bigint":
		avroType = "long"
	case "float4", "real", "float8", "double precision":
		avroType = "double"
	case "bool", "boolean":
		avroType = "boolean"
	case "bytea":
		avroType = "bytes"
	case "timestamp", "timestamptz", "timestamp without time zone", "timestamp with time zone", "date":
		avroType = "long"
	default:
		avroType = "string"
	}

	if nullable {
		return []string{"null", avroType}
	}
	return []string{avroType}
}

func normalizeAvroValue(colType string, val any) any {
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case time.Time:
		return v.UnixMilli()
	case json.RawMessage:
		return string(v)
	case []byte:
		if isByteaType(colType) {
			return v
		}
		return string(v)
	}

	if isJSONType(colType) {
		if payload, err := json.Marshal(val); err == nil {
			return string(payload)
		}
	}

	rv := reflect.ValueOf(val)
	switch rv.Kind() {
	case reflect.Map, reflect.Slice, reflect.Array:
		if payload, err := json.Marshal(val); err == nil {
			return string(payload)
		}
	}

	return val
}

func isJSONType(pgType string) bool {
	normalized := strings.ToLower(strings.TrimSpace(pgType))
	normalized = strings.TrimPrefix(normalized, "_")
	normalized = strings.TrimSuffix(normalized, "[]")
	if idx := strings.Index(normalized, "("); idx > 0 {
		normalized = normalized[:idx]
	}
	if idx := strings.LastIndex(normalized, "."); idx > 0 {
		normalized = normalized[idx+1:]
	}
	return normalized == "json" || normalized == "jsonb"
}

func isByteaType(pgType string) bool {
	normalized := strings.ToLower(strings.TrimSpace(pgType))
	if idx := strings.Index(normalized, "("); idx > 0 {
		normalized = normalized[:idx]
	}
	if idx := strings.LastIndex(normalized, "."); idx > 0 {
		normalized = normalized[idx+1:]
	}
	return normalized == "bytea"
}
