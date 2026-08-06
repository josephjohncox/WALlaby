// Package typemapping parses and canonicalizes destination type mappings.
package typemapping

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	OptTypeMappings     = "type_mappings"
	OptTypeMappingsFile = "type_mappings_file"
)

// Load resolves type mappings from connector options. A nonempty inline value
// takes precedence over a file. Files are read on every call so replacements
// are observed even when modification times are preserved.
func Load(options map[string]string) (map[string]string, error) {
	if options == nil {
		return nil, nil //nolint:nilnil // absence of mappings is not an error
	}
	if raw := strings.TrimSpace(options[OptTypeMappings]); raw != "" {
		return Parse(raw)
	}
	path := strings.TrimSpace(options[OptTypeMappingsFile])
	if path == "" {
		return nil, nil //nolint:nilnil // absence of mappings is not an error
	}
	// #nosec G304 -- path is user-configured and explicitly opted-in.
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("read type mappings file: %w", err)
	}
	return Parse(string(data))
}

// NormalizeKey returns the canonical type-mapping lookup key.
func NormalizeKey(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(value))), " ")
}

// Parse decodes JSON or YAML type mappings and returns canonical keys. When
// source keys canonicalize to the same key, lexicographically later source keys
// win so the result does not depend on Go map iteration order.
func Parse(raw string) (map[string]string, error) {
	var mappings map[string]string
	data := []byte(raw)
	if err := json.Unmarshal(data, &mappings); err != nil {
		if err := yaml.Unmarshal(data, &mappings); err != nil {
			return nil, fmt.Errorf("parse type_mappings: %w", err)
		}
	}

	keys := make([]string, 0, len(mappings))
	for key := range mappings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]string, len(mappings))
	for _, key := range keys {
		normalized := NormalizeKey(key)
		if normalized == "" {
			continue
		}
		out[normalized] = strings.TrimSpace(mappings[key])
	}
	return out, nil
}
