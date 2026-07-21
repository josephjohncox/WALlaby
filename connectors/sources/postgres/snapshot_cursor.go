package postgres

import (
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/internal/bootstrap"
)

const managedSnapshotCursorVersion = "wallaby.postgres.pk-cursor/v1"

type managedSnapshotCursor struct {
	Version string                     `json:"version"`
	Keys    []managedSnapshotCursorKey `json:"keys"`
}

type managedSnapshotCursorKey struct {
	Name     string `json:"name"`
	Type     string `json:"postgres_type"`
	Encoding string `json:"encoding"`
	Value    string `json:"value"`
}

func encodeManagedSnapshotCursor(task bootstrap.SnapshotTask, row map[string]any) ([]byte, error) {
	cursor := managedSnapshotCursor{Version: managedSnapshotCursorVersion, Keys: make([]managedSnapshotCursorKey, len(task.KeyColumns))}
	for i, name := range task.KeyColumns {
		postgresType, err := managedSnapshotKeyType(task, name)
		if err != nil {
			return nil, err
		}
		value, exists := row[name]
		if !exists || value == nil {
			return nil, fmt.Errorf("snapshot cursor primary key %q is missing or null", name)
		}
		encoding, text, err := encodeManagedSnapshotCursorValue(postgresType, value)
		if err != nil {
			return nil, fmt.Errorf("encode snapshot cursor key %q: %w", name, err)
		}
		cursor.Keys[i] = managedSnapshotCursorKey{Name: name, Type: postgresType, Encoding: encoding, Value: text}
	}
	return json.Marshal(cursor)
}

func decodeManagedSnapshotCursor(task bootstrap.SnapshotTask, encoded []byte) ([]any, error) {
	var cursor managedSnapshotCursor
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil {
		return nil, fmt.Errorf("decode snapshot cursor: %w", err)
	}
	if cursor.Version != managedSnapshotCursorVersion {
		return nil, fmt.Errorf("unsupported snapshot cursor version %q", cursor.Version)
	}
	if len(cursor.Keys) != len(task.KeyColumns) {
		return nil, fmt.Errorf("snapshot cursor key arity=%d, want %d", len(cursor.Keys), len(task.KeyColumns))
	}
	values := make([]any, len(cursor.Keys))
	for i, key := range cursor.Keys {
		name := task.KeyColumns[i]
		postgresType, err := managedSnapshotKeyType(task, name)
		if err != nil {
			return nil, err
		}
		if key.Name != name || key.Type != postgresType {
			return nil, fmt.Errorf("snapshot cursor key %d identity/type changed", i)
		}
		switch key.Encoding {
		case "text":
			values[i] = key.Value
		case "base64":
			if !isByteaType(postgresType) {
				return nil, fmt.Errorf("snapshot cursor key %q uses byte encoding for %s", name, postgresType)
			}
			value, err := base64.StdEncoding.DecodeString(key.Value)
			if err != nil {
				return nil, fmt.Errorf("decode snapshot cursor bytea key %q: %w", name, err)
			}
			values[i] = value
		default:
			return nil, fmt.Errorf("snapshot cursor key %q has unsupported encoding %q", name, key.Encoding)
		}
	}
	return values, nil
}

func managedSnapshotKeyType(task bootstrap.SnapshotTask, name string) (string, error) {
	for _, column := range task.Schema.Columns {
		if column.Name == name {
			postgresType := strings.ToLower(strings.Join(strings.Fields(column.Type), " "))
			if postgresType == "" {
				return "", fmt.Errorf("snapshot cursor primary key %q has no PostgreSQL type", name)
			}
			return postgresType, nil
		}
	}
	return "", fmt.Errorf("snapshot cursor primary key %q is absent from frozen schema", name)
}

func encodeManagedSnapshotCursorValue(postgresType string, value any) (string, string, error) {
	if isByteaType(postgresType) {
		bytes, ok := value.([]byte)
		if !ok {
			return "", "", fmt.Errorf("bytea value has Go type %T", value)
		}
		return "base64", base64.StdEncoding.EncodeToString(bytes), nil
	}
	switch typed := value.(type) {
	case string:
		return "text", typed, nil
	case time.Time:
		return "text", typed.Format(time.RFC3339Nano), nil
	case *big.Rat:
		text, err := finiteRatDecimal(typed)
		if scale, ok := numericTypeScale(postgresType); ok {
			text = typed.FloatString(scale)
		}
		return "text", text, err
	case driver.Valuer:
		value, err := typed.Value()
		if err != nil {
			return "", "", err
		}
		if value == nil {
			return "", "", errors.New("driver value is null")
		}
		return "text", fmt.Sprint(value), nil
	case fmt.Stringer:
		return "text", typed.String(), nil
	}
	kind := reflect.ValueOf(value).Kind()
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "text", strconv.FormatInt(reflect.ValueOf(value).Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "text", strconv.FormatUint(reflect.ValueOf(value).Uint(), 10), nil
	case reflect.Float32:
		return "text", strconv.FormatFloat(reflect.ValueOf(value).Float(), 'g', -1, 32), nil
	case reflect.Float64:
		return "text", strconv.FormatFloat(reflect.ValueOf(value).Float(), 'g', -1, 64), nil
	case reflect.Bool:
		return "text", strconv.FormatBool(reflect.ValueOf(value).Bool()), nil
	default:
		return "", "", fmt.Errorf("unsupported Go type %T", value)
	}
}

func finiteRatDecimal(value *big.Rat) (string, error) {
	if value == nil {
		return "", errors.New("numeric value is nil")
	}
	denominator := new(big.Int).Set(value.Denom())
	twos, fives := 0, 0
	for new(big.Int).Mod(denominator, big.NewInt(2)).Sign() == 0 {
		denominator.Div(denominator, big.NewInt(2))
		twos++
	}
	for new(big.Int).Mod(denominator, big.NewInt(5)).Sign() == 0 {
		denominator.Div(denominator, big.NewInt(5))
		fives++
	}
	if denominator.Cmp(big.NewInt(1)) != 0 {
		return "", errors.New("numeric value has a non-terminating decimal representation")
	}
	scale := twos
	if fives > scale {
		scale = fives
	}
	return value.FloatString(scale), nil
}

func numericTypeScale(postgresType string) (int, bool) {
	open := strings.IndexByte(postgresType, '(')
	comma := strings.IndexByte(postgresType, ',')
	close := strings.IndexByte(postgresType, ')')
	if open < 0 || comma < open || close < comma {
		return 0, false
	}
	scale, err := strconv.Atoi(strings.TrimSpace(postgresType[comma+1 : close]))
	return scale, err == nil && scale >= 0
}

func isByteaType(postgresType string) bool {
	return postgresType == "bytea" || strings.HasSuffix(postgresType, ".bytea")
}
