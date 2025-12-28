package connector

import (
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
)

var jsonNumberPattern = regexp.MustCompile(`^-?(0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?$`)

// NormalizePostgresRecord coerces row values into stable, structure-preserving types
// based on the Postgres column types in the provided schema.
func NormalizePostgresRecord(schema Schema, values map[string]any) error {
	if values == nil {
		return nil
	}
	for _, col := range schema.Columns {
		val, ok := values[col.Name]
		if !ok {
			continue
		}
		normalized, err := normalizePostgresValueWithColumn(col, val)
		if err != nil {
			return fmt.Errorf("normalize %s: %w", col.Name, err)
		}
		values[col.Name] = normalized
	}
	return nil
}

func normalizePostgresValueWithColumn(col Column, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	base, isArray := splitPostgresType(col.Type)
	if isArray {
		return normalizePostgresArray(base, value)
	}

	if isExtensionType(col, base) {
		return normalizeExtensionValue(base, value)
	}

	switch base {
	case "json", "jsonb":
		return normalizeJSONValue(value)
	case "uuid":
		return normalizeUUIDValue(value)
	case "numeric", "decimal", "money":
		return normalizeNumericValue(value)
	case "inet", "cidr":
		return normalizeInetValue(value)
	default:
		return value, nil
	}
}

func isExtensionType(col Column, base string) bool {
	if col.TypeMetadata != nil {
		if ext := strings.TrimSpace(col.TypeMetadata["extension"]); ext != "" {
			return true
		}
	}
	if strings.Contains(col.Type, ".") {
		return true
	}
	switch base {
	case "hstore", "geometry", "geography", "vector":
		return true
	default:
		return false
	}
}

func normalizeExtensionValue(base string, value any) (any, error) {
	switch base {
	case "hstore":
		return normalizeHstoreValue(value)
	case "vector":
		return normalizeVectorValue(value)
	case "geometry", "geography":
		return normalizeGeometryValue(value)
	default:
		return value, nil
	}
}

func normalizeHstoreValue(value any) (any, error) {
	switch v := value.(type) {
	case pgtype.Hstore:
		return hstoreToMap(v), nil
	case map[string]*string:
		return hstoreToMap(pgtype.Hstore(v)), nil
	case map[string]string:
		out := make(map[string]any, len(v))
		for key, val := range v {
			out[key] = val
		}
		return out, nil
	case string:
		var h pgtype.Hstore
		if err := h.Scan(v); err != nil {
			return v, nil
		}
		return hstoreToMap(h), nil
	case []byte:
		var h pgtype.Hstore
		if err := h.Scan(string(v)); err != nil {
			return v, nil
		}
		return hstoreToMap(h), nil
	default:
		return value, nil
	}
}

func hstoreToMap(h pgtype.Hstore) map[string]any {
	out := make(map[string]any, len(h))
	for key, val := range h {
		if val == nil {
			out[key] = nil
		} else {
			out[key] = *val
		}
	}
	return out
}

func normalizeVectorValue(value any) (any, error) {
	switch v := value.(type) {
	case []float32:
		return v, nil
	case []float64:
		out := make([]float32, len(v))
		for i, f := range v {
			out[i] = float32(f)
		}
		return out, nil
	case string:
		return parseVectorText(v)
	case []byte:
		return parseVectorText(string(v))
	default:
		return value, nil
	}
}

func parseVectorText(raw string) ([]float32, error) {
	trimmed := strings.TrimSpace(raw)
	trimmed = strings.TrimPrefix(trimmed, "[")
	trimmed = strings.TrimSuffix(trimmed, "]")
	trimmed = strings.TrimPrefix(trimmed, "{")
	trimmed = strings.TrimSuffix(trimmed, "}")
	if trimmed == "" {
		return []float32{}, nil
	}
	parts := strings.Split(trimmed, ",")
	out := make([]float32, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		f, err := strconv.ParseFloat(part, 32)
		if err != nil {
			return nil, err
		}
		out = append(out, float32(f))
	}
	return out, nil
}

func normalizeGeometryValue(value any) (any, error) {
	switch v := value.(type) {
	case []byte:
		raw := make([]byte, len(v))
		copy(raw, v)
		return raw, nil
	case string:
		trimmed := strings.TrimSpace(v)
		if looksLikeHex(trimmed) {
			if decoded, err := hex.DecodeString(strings.TrimPrefix(trimmed, "\\x")); err == nil {
				return decoded, nil
			}
		}
		return v, nil
	default:
		return value, nil
	}
}

func looksLikeHex(value string) bool {
	if strings.HasPrefix(value, "\\x") {
		value = strings.TrimPrefix(value, "\\x")
	}
	if len(value)%2 != 0 || value == "" {
		return false
	}
	for _, r := range value {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		case r >= 'A' && r <= 'F':
		default:
			return false
		}
	}
	return true
}

func normalizePostgresArray(base string, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return value, nil
	}
	if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
		return value, nil
	}

	out := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		elem := rv.Index(i).Interface()
		normalized, err := normalizePostgresValue(base, elem)
		if err != nil {
			return nil, err
		}
		out[i] = normalized
	}
	return out, nil
}

func normalizePostgresValue(base string, value any) (any, error) {
	return normalizePostgresValueWithColumn(Column{Type: base}, value)
}

func splitPostgresType(value string) (string, bool) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if idx := strings.Index(normalized, "("); idx > 0 {
		normalized = normalized[:idx]
	}
	if idx := strings.LastIndex(normalized, "."); idx > 0 {
		normalized = normalized[idx+1:]
	}

	isArray := false
	if strings.HasSuffix(normalized, "[]") {
		normalized = strings.TrimSuffix(normalized, "[]")
		isArray = true
	} else if strings.HasPrefix(normalized, "_") {
		normalized = strings.TrimPrefix(normalized, "_")
		isArray = true
	}

	return normalized, isArray
}

func normalizeJSONValue(value any) (any, error) {
	switch v := value.(type) {
	case json.RawMessage:
		return v, nil
	case []byte:
		if len(v) == 0 {
			return json.RawMessage(nil), nil
		}
		return json.RawMessage(append([]byte(nil), v...)), nil
	case string:
		if v == "" {
			return json.RawMessage(nil), nil
		}
		return json.RawMessage([]byte(v)), nil
	default:
		payload, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("marshal json value: %w", err)
		}
		return json.RawMessage(payload), nil
	}
}

func normalizeUUIDValue(value any) (any, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case [16]byte:
		return formatUUIDBytes(v[:]), nil
	case []byte:
		if len(v) == 16 {
			return formatUUIDBytes(v), nil
		}
		return string(v), nil
	case fmt.Stringer:
		return v.String(), nil
	case driver.Valuer:
		val, err := v.Value()
		if err != nil {
			return nil, err
		}
		return normalizeUUIDValue(val)
	default:
		return value, nil
	}
}

func normalizeNumericValue(value any) (any, error) {
	switch v := value.(type) {
	case json.Number:
		return v, nil
	case string:
		if v == "" {
			return v, nil
		}
		if jsonNumberPattern.MatchString(v) {
			return json.Number(v), nil
		}
		return v, nil
	case []byte:
		return normalizeNumericValue(string(v))
	case driver.Valuer:
		val, err := v.Value()
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		return normalizeNumericValue(val)
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return json.Number(fmt.Sprint(value)), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return json.Number(fmt.Sprint(value)), nil
	case reflect.Float32, reflect.Float64:
		return json.Number(fmt.Sprint(value)), nil
	}

	if m, ok := value.(json.Marshaler); ok {
		if payload, err := m.MarshalJSON(); err == nil {
			text := strings.TrimSpace(string(payload))
			if jsonNumberPattern.MatchString(text) {
				return json.Number(text), nil
			}
		}
	}

	return value, nil
}

func normalizeInetValue(value any) (any, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case net.IP:
		return v.String(), nil
	case net.IPNet:
		return v.String(), nil
	case *net.IPNet:
		if v == nil {
			return nil, nil
		}
		return v.String(), nil
	case driver.Valuer:
		val, err := v.Value()
		if err != nil {
			return nil, err
		}
		return normalizeInetValue(val)
	default:
		return value, nil
	}
}

func formatUUIDBytes(input []byte) string {
	if len(input) != 16 {
		return string(input)
	}
	var buf [36]byte
	hex.Encode(buf[0:8], input[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], input[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], input[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], input[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:], input[10:16])
	return string(buf[:])
}
