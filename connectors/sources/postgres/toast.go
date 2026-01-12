package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/internal/replication"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

const (
	toastFetchOff    = "off"
	toastFetchSource = "source"
	toastFetchCache  = "cache"
	toastFetchFull   = "full"
)

func (s *Source) handleToast(ctx context.Context, change replication.Change, record *connector.Record) error {
	switch s.toastFetch {
	case toastFetchSource:
		if err := s.rehydrateToast(ctx, change, record, false); err != nil {
			return err
		}
	case toastFetchFull:
		if err := s.rehydrateToast(ctx, change, record, true); err != nil {
			return err
		}
	case toastFetchCache:
		if err := s.applyToastCache(record); err != nil {
			return err
		}
	default:
	}
	return nil
}

func (s *Source) rehydrateToast(ctx context.Context, change replication.Change, record *connector.Record, full bool) error {
	if s.toastPool == nil || record == nil {
		return nil
	}
	if record.Operation != connector.OpUpdate {
		return nil
	}
	if !full && len(record.Unchanged) == 0 {
		return nil
	}
	if change.Schema == "" || change.Table == "" {
		return nil
	}

	keyMap, err := decodeKey(record.Key)
	if err != nil {
		return err
	}
	if len(keyMap) == 0 {
		return nil
	}

	keyCols := make([]string, 0, len(keyMap))
	for col := range keyMap {
		keyCols = append(keyCols, col)
	}
	sort.Strings(keyCols)

	keyVals := make([]any, 0, len(keyCols))
	for _, col := range keyCols {
		val := keyMap[col]
		if record.After != nil {
			if afterVal, ok := record.After[col]; ok {
				val = afterVal
			}
		}
		keyVals = append(keyVals, val)
	}

	columns := make([]string, 0)
	seen := map[string]struct{}{}
	if full {
		if change.SchemaDef != nil {
			for _, col := range change.SchemaDef.Columns {
				if col.Name == "" {
					continue
				}
				if _, ok := seen[col.Name]; ok {
					continue
				}
				seen[col.Name] = struct{}{}
				columns = append(columns, col.Name)
			}
		}
	} else {
		for _, col := range record.Unchanged {
			if col == "" {
				continue
			}
			if record.After != nil {
				if _, ok := record.After[col]; ok {
					continue
				}
			}
			if _, ok := seen[col]; ok {
				continue
			}
			seen[col] = struct{}{}
			columns = append(columns, col)
		}
	}
	if len(columns) == 0 {
		return nil
	}

	selectCols := quoteColumns(columns)
	target := quoteQualified(change.Schema, change.Table)
	where := whereClause(keyCols, 0)
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s", selectCols, target, where)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := s.toastPool.Query(ctx, query, keyVals...)
	if err != nil {
		return fmt.Errorf("rehydrate toast: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil
	}
	values, err := rows.Values()
	if err != nil {
		return fmt.Errorf("rehydrate toast values: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rehydrate toast rows: %w", err)
	}
	if record.After == nil {
		record.After = map[string]any{}
	}
	for idx, col := range columns {
		if idx >= len(values) {
			break
		}
		record.After[col] = values[idx]
	}

	if change.SchemaDef != nil {
		if err := connector.NormalizePostgresRecord(*change.SchemaDef, record.After); err != nil {
			return err
		}
	}
	record.Unchanged = nil
	s.updateToastCache(record)

	return nil
}

func (s *Source) applyToastCache(record *connector.Record) error {
	if s.toastCache == nil || record == nil {
		return nil
	}
	key := cacheKeyForRecord(record)
	if key == "" {
		return nil
	}

	switch record.Operation {
	case connector.OpDelete:
		s.toastCache.Delete(key)
	case connector.OpInsert, connector.OpLoad:
		if record.After != nil {
			s.toastCache.Put(key, record.After)
		}
	case connector.OpUpdate:
		entry, ok := s.toastCache.Get(key)
		if ok {
			merged := mergeRow(entry, record.After)
			record.After = merged
			record.Unchanged = nil
			s.toastCache.Put(key, merged)
		} else if record.After != nil {
			s.toastCache.Put(key, record.After)
		}
	}

	return nil
}

func (s *Source) updateToastCache(record *connector.Record) {
	if s.toastCache == nil || record == nil {
		return
	}
	key := cacheKeyForRecord(record)
	if key == "" {
		return
	}
	switch record.Operation {
	case connector.OpDelete:
		s.toastCache.Delete(key)
	case connector.OpInsert, connector.OpLoad, connector.OpUpdate:
		if record.After != nil {
			s.toastCache.Put(key, record.After)
		}
	}
}

func decodeKey(raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var out map[string]any
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("decode record key: %w", err)
	}
	return out, nil
}

func cacheKeyForRecord(record *connector.Record) string {
	if record == nil || len(record.Key) == 0 {
		return ""
	}
	keyMap, err := decodeKey(record.Key)
	if err != nil || len(keyMap) == 0 {
		return ""
	}
	keys := make([]string, 0, len(keyMap))
	for key := range keyMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	buf := &bytes.Buffer{}
	buf.WriteByte('{')
	for idx, key := range keys {
		if idx > 0 {
			buf.WriteByte(',')
		}
		name, _ := json.Marshal(key)
		val := keyMap[key]
		if record.After != nil {
			if afterVal, ok := record.After[key]; ok {
				val = afterVal
			}
		}
		value, _ := json.Marshal(val)
		buf.Write(name)
		buf.WriteByte(':')
		buf.Write(value)
	}
	buf.WriteByte('}')
	return buf.String()
}

func mergeRow(base map[string]any, update map[string]any) map[string]any {
	if base == nil && update == nil {
		return nil
	}
	out := make(map[string]any, len(base)+len(update))
	for key, val := range base {
		out[key] = val
	}
	for key, val := range update {
		out[key] = val
	}
	return out
}

func quoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func quoteQualified(schema, table string) string {
	if schema == "" {
		return quoteIdent(table)
	}
	return quoteIdent(schema) + "." + quoteIdent(table)
}

func quoteColumns(cols []string) string {
	quoted := make([]string, 0, len(cols))
	for _, col := range cols {
		quoted = append(quoted, quoteIdent(col))
	}
	return strings.Join(quoted, ", ")
}

func whereClause(cols []string, offset int) string {
	parts := make([]string, 0, len(cols))
	for idx, col := range cols {
		parts = append(parts, fmt.Sprintf("%s = $%d", quoteIdent(col), offset+idx+1))
	}
	return strings.Join(parts, " AND ")
}
