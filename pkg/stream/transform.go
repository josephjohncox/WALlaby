package stream

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"gopkg.in/yaml.v3"
)

const (
	optTypeMappings     = "type_mappings"
	optTypeMappingsFile = "type_mappings_file"
)

func transformBatchForDestination(batch connector.Batch, spec connector.Spec, baseMappings map[string]string) (connector.Batch, bool, error) {
	overrides, err := loadTypeMappings(spec.Options)
	if err != nil {
		return connector.Batch{}, false, err
	}
	mappings := mergeTypeMappings(baseMappings, overrides)
	if len(mappings) == 0 {
		return batch, false, nil
	}
	out := batch
	out.Schema = applyTypeMappings(batch.Schema, mappings)
	return out, true, nil
}

func mergeTypeMappings(base, override map[string]string) map[string]string {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(override))
	for key, value := range base {
		if strings.TrimSpace(key) == "" {
			continue
		}
		out[normalizeTypeKey(key)] = value
	}
	for key, value := range override {
		if strings.TrimSpace(key) == "" {
			continue
		}
		out[normalizeTypeKey(key)] = value
	}
	return out
}

func applyTypeMappings(schema connector.Schema, mappings map[string]string) connector.Schema {
	if len(mappings) == 0 {
		return schema
	}

	cols := make([]connector.Column, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		next := col
		next.Type = mapTypeForColumn(col, mappings)
		cols = append(cols, next)
	}
	schema.Columns = cols
	return schema
}

func mapTypeForColumn(col connector.Column, mappings map[string]string) string {
	if col.Type == "" || len(mappings) == 0 {
		return col.Type
	}
	for _, key := range typeMappingKeys(col) {
		if mapped, ok := mappings[key]; ok {
			return mapped
		}
	}
	return col.Type
}

func normalizeTypeKey(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func typeMappingKeys(col connector.Column) []string {
	seen := map[string]struct{}{}
	var keys []string

	add := func(value string) {
		value = normalizeTypeKey(value)
		if value == "" {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		keys = append(keys, value)
	}

	raw := normalizeTypeKey(col.Type)
	add(raw)

	base, isArray := baseTypeKey(raw)
	if base != raw {
		add(base)
	}
	if isArray {
		add(base + "[]")
	}

	if col.TypeMetadata != nil {
		if schema := strings.TrimSpace(col.TypeMetadata["type_schema"]); schema != "" {
			add(schema + "." + base)
			if isArray {
				add(schema + "." + base + "[]")
			}
		}
		if ext := strings.TrimSpace(col.TypeMetadata["extension"]); ext != "" {
			add("ext:" + ext + "." + base)
			add("ext:" + ext)
		}
	}

	if idx := strings.LastIndex(raw, "."); idx > 0 {
		add(raw[idx+1:])
	}

	return keys
}

func baseTypeKey(value string) (string, bool) {
	normalized := normalizeTypeKey(value)
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

type cachedTypeMapping struct {
	modTime  time.Time
	mappings map[string]string
}

var typeMappingCache sync.Map

func loadTypeMappings(options map[string]string) (map[string]string, error) {
	if options == nil {
		return nil, nil //nolint:nilnil // absence of mappings is not an error
	}
	if raw := strings.TrimSpace(options[optTypeMappings]); raw != "" {
		return parseTypeMappings(raw)
	}
	if path := strings.TrimSpace(options[optTypeMappingsFile]); path != "" {
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("stat type mapping file: %w", err)
		}
		if cached, ok := typeMappingCache.Load(path); ok {
			entry := cached.(cachedTypeMapping)
			if info.ModTime().Equal(entry.modTime) {
				return entry.mappings, nil
			}
		}
		// #nosec G304 -- path is user-configured and explicitly opted-in.
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read type mapping file: %w", err)
		}
		mappings, err := parseTypeMappings(string(data))
		if err != nil {
			return nil, err
		}
		typeMappingCache.Store(path, cachedTypeMapping{modTime: info.ModTime(), mappings: mappings})
		return mappings, nil
	}
	return nil, nil //nolint:nilnil // absence of mappings is not an error
}

func parseTypeMappings(raw string) (map[string]string, error) {
	var mappings map[string]string
	data := []byte(raw)
	if err := json.Unmarshal(data, &mappings); err != nil {
		if err := yaml.Unmarshal(data, &mappings); err != nil {
			return nil, fmt.Errorf("parse type_mappings: %w", err)
		}
	}
	out := make(map[string]string, len(mappings))
	for key, value := range mappings {
		normalized := normalizeTypeKey(key)
		if normalized == "" {
			continue
		}
		out[normalized] = strings.TrimSpace(value)
	}
	return out, nil
}
