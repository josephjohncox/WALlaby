package stream

import (
	"sort"
	"strings"

	"github.com/josephjohncox/wallaby/internal/typemapping"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func transformBatchForDestination(batch connector.Batch, spec connector.RuntimeSpec, baseMappings map[string]string) (connector.Batch, bool, error) {
	overrides, err := typemapping.Load(spec.Options)
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
	merge := func(mappings map[string]string) {
		keys := make([]string, 0, len(mappings))
		for key := range mappings {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			normalized := typemapping.NormalizeKey(key)
			if normalized != "" {
				out[normalized] = strings.TrimSpace(mappings[key])
			}
		}
	}
	merge(base)
	merge(override)
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

func typeMappingKeys(col connector.Column) []string {
	seen := map[string]struct{}{}
	var keys []string

	add := func(value string) {
		value = typemapping.NormalizeKey(value)
		if value == "" {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		keys = append(keys, value)
	}

	raw := typemapping.NormalizeKey(col.Type)
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
	normalized := typemapping.NormalizeKey(value)
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
