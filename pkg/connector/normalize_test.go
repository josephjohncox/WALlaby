package connector

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestNormalizePostgresRecord_JSON(t *testing.T) {
	schema := Schema{
		Name:      "widgets",
		Namespace: "public",
		Columns: []Column{
			{Name: "payload", Type: "jsonb"},
		},
	}
	values := map[string]any{
		"payload": `{"a":1,"b":[true,false]}`,
	}
	if err := NormalizePostgresRecord(schema, values); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	raw, ok := values["payload"].(json.RawMessage)
	if !ok {
		t.Fatalf("expected json.RawMessage, got %T", values["payload"])
	}
	if !bytes.Equal(raw, []byte(`{"a":1,"b":[true,false]}`)) {
		t.Fatalf("unexpected json payload: %s", string(raw))
	}
}

func TestNormalizePostgresRecord_UUID(t *testing.T) {
	schema := Schema{
		Name:      "widgets",
		Namespace: "public",
		Columns: []Column{
			{Name: "id", Type: "uuid"},
		},
	}
	values := map[string]any{
		"id": [16]byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef},
	}
	if err := NormalizePostgresRecord(schema, values); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	got, ok := values["id"].(string)
	if !ok {
		t.Fatalf("expected uuid string, got %T", values["id"])
	}
	if got != "12345678-90ab-cdef-0123-456789abcdef" {
		t.Fatalf("unexpected uuid: %s", got)
	}
}

func TestNormalizePostgresRecord_Numeric(t *testing.T) {
	schema := Schema{
		Name:      "widgets",
		Namespace: "public",
		Columns: []Column{
			{Name: "price", Type: "numeric(10,2)"},
			{Name: "nan", Type: "numeric"},
		},
	}
	values := map[string]any{
		"price": "123.45",
		"nan":   "NaN",
	}
	if err := NormalizePostgresRecord(schema, values); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if _, ok := values["price"].(json.Number); !ok {
		t.Fatalf("expected json.Number, got %T", values["price"])
	}
	if _, ok := values["nan"].(string); !ok {
		t.Fatalf("expected string for NaN, got %T", values["nan"])
	}
}

func TestNormalizePostgresRecord_Array(t *testing.T) {
	schema := Schema{
		Name:      "widgets",
		Namespace: "public",
		Columns: []Column{
			{Name: "ids", Type: "uuid[]"},
		},
	}
	values := map[string]any{
		"ids": []any{
			[16]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
			[16]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02},
		},
	}
	if err := NormalizePostgresRecord(schema, values); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	ids, ok := values["ids"].([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", values["ids"])
	}
	if ids[0].(string) != "00000000-0000-0000-0000-000000000001" {
		t.Fatalf("unexpected uuid[0]: %v", ids[0])
	}
	if ids[1].(string) != "00000000-0000-0000-0000-000000000002" {
		t.Fatalf("unexpected uuid[1]: %v", ids[1])
	}
}

func TestNormalizePostgresRecord_IntegerBool(t *testing.T) {
	schema := Schema{
		Name:      "widgets",
		Namespace: "public",
		Columns: []Column{
			{Name: "id", Type: "bigint"},
			{Name: "active", Type: "boolean"},
		},
	}
	values := map[string]any{
		"id":     "42",
		"active": "true",
	}
	if err := NormalizePostgresRecord(schema, values); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if got, ok := values["id"].(int64); !ok || got != 42 {
		t.Fatalf("expected int64(42), got %T %v", values["id"], values["id"])
	}
	if got, ok := values["active"].(bool); !ok || !got {
		t.Fatalf("expected true bool, got %T %v", values["active"], values["active"])
	}
}

func TestNormalizeKeyForSchema(t *testing.T) {
	schema := Schema{
		Name:      "widgets",
		Namespace: "public",
		Columns: []Column{
			{Name: "id", Type: "int8"},
			{Name: "active", Type: "bool"},
		},
	}
	key := map[string]any{
		"id":     "7",
		"active": "f",
	}
	normalized, err := NormalizeKeyForSchema(schema, key)
	if err != nil {
		t.Fatalf("normalize key: %v", err)
	}
	if got, ok := normalized["id"].(int64); !ok || got != 7 {
		t.Fatalf("expected int64(7), got %T %v", normalized["id"], normalized["id"])
	}
	if got, ok := normalized["active"].(bool); !ok || got {
		t.Fatalf("expected false bool, got %T %v", normalized["active"], normalized["active"])
	}
}
