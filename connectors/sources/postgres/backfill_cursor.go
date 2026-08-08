package postgres

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const backfillCursorPrefix = "wallaby-v1:"

type backfillCursorValue struct {
	Text string `json:"text,omitempty"`
	Null bool   `json:"null,omitempty"`
}

type backfillCursorEnvelope struct {
	Version int                   `json:"version"`
	Values  []backfillCursorValue `json:"values"`
}

func backfillCursorColumns(partitionColumn string, primaryKey []string) []string {
	if partitionColumn == "" {
		return nil
	}
	columns := []string{partitionColumn}
	for _, column := range primaryKey {
		if column != partitionColumn {
			columns = append(columns, column)
		}
	}
	return columns
}

func encodeBackfillCursor(row map[string]any, columns []string) (string, error) {
	if len(columns) == 0 {
		return "", nil
	}
	envelope := backfillCursorEnvelope{Version: 1, Values: make([]backfillCursorValue, len(columns))}
	for index, column := range columns {
		value, ok := row[column]
		if !ok {
			return "", fmt.Errorf("backfill cursor column %q missing from row", column)
		}
		if value == nil {
			envelope.Values[index].Null = true
			continue
		}
		envelope.Values[index].Text = backfillCursorText(value)
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("encode backfill cursor: %w", err)
	}
	return backfillCursorPrefix + base64.RawURLEncoding.EncodeToString(payload), nil
}

func backfillCursorText(value any) string {
	switch typed := value.(type) {
	case []byte:
		return `\\x` + hex.EncodeToString(typed)
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(value)
	}
}

func decodeBackfillCursor(cursor string, columnCount int) ([]backfillCursorValue, bool, error) {
	if !strings.HasPrefix(cursor, backfillCursorPrefix) {
		return nil, false, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(cursor, backfillCursorPrefix))
	if err != nil {
		return nil, true, fmt.Errorf("decode backfill cursor payload: %w", err)
	}
	var envelope backfillCursorEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, true, fmt.Errorf("decode backfill cursor: %w", err)
	}
	if envelope.Version != 1 || len(envelope.Values) != columnCount {
		return nil, true, fmt.Errorf("invalid backfill cursor version or arity")
	}
	return envelope.Values, true, nil
}

func buildBackfillResumeClause(columns []string, values []backfillCursorValue, firstArg int) (string, []any, error) {
	if len(columns) == 0 || len(columns) != len(values) {
		err := fmt.Errorf("invalid backfill cursor columns")
		return "", nil, err
	}
	partition := pgx.Identifier{columns[0]}.Sanitize()
	args := make([]any, 0, len(values))
	if len(columns) == 1 {
		if values[0].Null {
			return partition + " IS NULL", nil, nil
		}
		args = append(args, values[0].Text)
		clause := fmt.Sprintf("(%s >= $%d OR %s IS NULL)", partition, firstArg, partition)
		return clause, args, nil
	}
	for index, value := range values {
		if index > 0 && value.Null {
			err := fmt.Errorf("primary-key cursor column %q is null", columns[index])
			return "", nil, err
		}
		if !value.Null {
			args = append(args, value.Text)
		}
	}

	keyColumns := make([]string, 0, len(columns)-1)
	keyArgs := make([]string, 0, len(columns)-1)
	partitionArgCount := 0
	if !values[0].Null {
		partitionArgCount = 1
	}
	for index, column := range columns[1:] {
		keyColumns = append(keyColumns, pgx.Identifier{column}.Sanitize())
		keyArgs = append(keyArgs, fmt.Sprintf("$%d", firstArg+partitionArgCount+index))
	}
	keyResume := fmt.Sprintf("ROW(%s) >= ROW(%s)", strings.Join(keyColumns, ", "), strings.Join(keyArgs, ", "))
	if values[0].Null {
		clause := fmt.Sprintf("(%s IS NULL AND %s)", partition, keyResume)
		return clause, args, nil
	}
	clause := fmt.Sprintf("(%s > $%d OR %s IS NULL OR (%s = $%d AND %s))", partition, firstArg, partition, partition, firstArg, keyResume)
	return clause, args, nil
}
