// Package options provides strict, typed decoding for string-valued options.
package options

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/http/httpguts"
)

// Decoder decodes typed values from a string map and accumulates errors.
type Decoder struct {
	path   string
	values map[string]string
	errs   []error
}

// NewDecoder creates a decoder. Path is prepended to errors to identify the
// option owner, for example "grpc options".
func NewDecoder(path string, values map[string]string) *Decoder {
	return &Decoder{path: strings.TrimSpace(path), values: values}
}

// Raw returns the exact configured bytes for key, or fallback when key is
// absent. It never trims the configured value.
func (d *Decoder) Raw(key, fallback string) string {
	value, ok := d.values[key]
	if !ok {
		return fallback
	}
	return value
}

// String returns the trimmed configured value for key, or fallback when key is
// absent.
func (d *Decoder) String(key, fallback string) string {
	value, ok := d.values[key]
	if !ok {
		return fallback
	}
	return strings.TrimSpace(value)
}

// Bool parses a boolean or returns fallback when key is absent.
func (d *Decoder) Bool(key string, fallback bool) bool {
	raw, ok := d.values[key]
	if !ok {
		return fallback
	}
	value, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		d.add(key, "parse bool", err)
		return fallback
	}
	return value
}

// Int parses an integer or returns fallback when key is absent.
func (d *Decoder) Int(key string, fallback int) int {
	raw, ok := d.values[key]
	if !ok {
		return fallback
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		d.add(key, "parse int", err)
		return fallback
	}
	return value
}

// Float64 parses a float64 or returns fallback when key is absent.
func (d *Decoder) Float64(key string, fallback float64) float64 {
	raw, ok := d.values[key]
	if !ok {
		return fallback
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		d.add(key, "parse float64", err)
		return fallback
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		d.add(key, "parse float64", fmt.Errorf("value %q must be finite", raw))
		return fallback
	}
	return value
}

// AliasedEnum parses a case-insensitive, trimmed enum whose accepted spellings
// map to canonical values. It returns fallback when key is absent.
func (d *Decoder) AliasedEnum(key, fallback string, aliases map[string]string) string {
	raw, ok := d.values[key]
	if !ok {
		return fallback
	}
	normalized := strings.ToLower(strings.TrimSpace(raw))
	value, ok := aliases[normalized]
	if !ok {
		accepted := make([]string, 0, len(aliases))
		for alias := range aliases {
			if alias == "" {
				alias = "<empty>"
			}
			accepted = append(accepted, alias)
		}
		sort.Strings(accepted)
		d.add(key, "parse enum", fmt.Errorf("value %q is not one of %s", raw, strings.Join(accepted, ", ")))
		return fallback
	}
	return value
}

// Duration parses a duration or returns fallback when key is absent.
func (d *Decoder) Duration(key string, fallback time.Duration) time.Duration {
	raw, ok := d.values[key]
	if !ok {
		return fallback
	}
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		d.add(key, "parse duration", err)
		return fallback
	}
	return value
}

// KeyValueList parses a strict comma-separated key/value list. An absent or
// empty option produces an empty map.
func (d *Decoder) KeyValueList(key string) map[string]string {
	raw, ok := d.values[key]
	if !ok || raw == "" {
		return map[string]string{}
	}
	value, err := ParseKeyValueList(raw)
	if err != nil {
		d.add(key, "parse key/value list", err)
		return map[string]string{}
	}
	return value
}

// CaseInsensitiveKeyValueList parses a comma-separated key/value list and
// lowercases keys before duplicate detection without imposing value syntax.
func (d *Decoder) CaseInsensitiveKeyValueList(key string) map[string]string {
	raw, ok := d.values[key]
	if !ok || raw == "" {
		return map[string]string{}
	}
	value, err := ParseCaseInsensitiveKeyValueList(raw)
	if err != nil {
		d.add(key, "parse case-insensitive key/value list", err)
		return map[string]string{}
	}
	return value
}

// HeaderList parses and validates a comma-separated HTTP-style header list.
func (d *Decoder) HeaderList(key string) map[string]string {
	raw, ok := d.values[key]
	if !ok || raw == "" {
		return map[string]string{}
	}
	value, err := ParseHeaderList(raw)
	if err != nil {
		d.add(key, "parse header list", err)
		return map[string]string{}
	}
	return value
}

// Err returns all accumulated decode errors joined together.
func (d *Decoder) Err() error {
	return errors.Join(d.errs...)
}

func (d *Decoder) add(key, operation string, err error) {
	name := key
	if d.path != "" {
		name = d.path + "." + key
	}
	d.errs = append(d.errs, fmt.Errorf("%s: %s: %w", name, operation, err))
}

// ParseKeyValueList parses a CSV record whose fields are key:value items.
// Quoting follows encoding/csv, each decoded item is split at its first colon,
// and keys must be nonempty and unique after trimming.
func ParseKeyValueList(raw string) (map[string]string, error) {
	if raw == "" {
		return map[string]string{}, nil
	}
	reader := csv.NewReader(strings.NewReader(raw))
	reader.FieldsPerRecord = -1
	reader.TrimLeadingSpace = true
	record, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read CSV: %w", err)
	}
	if _, err := reader.Read(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("multiple CSV records are not allowed")
		}
		return nil, fmt.Errorf("read CSV: %w", err)
	}

	out := make(map[string]string, len(record))
	for index, field := range record {
		item := strings.TrimSpace(field)
		if item == "" {
			return nil, fmt.Errorf("item %d is empty", index+1)
		}
		key, value, ok := strings.Cut(item, ":")
		if !ok {
			return nil, fmt.Errorf("item %d %q is missing ':'", index+1, item)
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" {
			return nil, fmt.Errorf("item %d has an empty key", index+1)
		}
		if _, duplicate := out[key]; duplicate {
			return nil, fmt.Errorf("duplicate key %q", key)
		}
		out[key] = value
	}
	return out, nil
}

// ParseCaseInsensitiveKeyValueList parses a key/value list, lowercases keys,
// and rejects collisions after normalization. Values are otherwise untouched
// beyond the key/value parser's documented trimming.
func ParseCaseInsensitiveKeyValueList(raw string) (map[string]string, error) {
	parsed, err := ParseKeyValueList(raw)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(parsed))
	for key := range parsed {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]string, len(parsed))
	for _, key := range keys {
		normalized := strings.ToLower(key)
		if _, duplicate := out[normalized]; duplicate {
			return nil, fmt.Errorf("duplicate key %q after case normalization", normalized)
		}
		out[normalized] = parsed[key]
	}
	return out, nil
}

// ParseHeaderList parses a key/value list as case-insensitive HTTP headers.
// Both names and values are validated according to HTTP field syntax.
func ParseHeaderList(raw string) (map[string]string, error) {
	parsed, err := ParseCaseInsensitiveKeyValueList(raw)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(parsed))
	for key := range parsed {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := parsed[key]
		if !httpguts.ValidHeaderFieldName(key) {
			return nil, fmt.Errorf("invalid header name %q", key)
		}
		if !httpguts.ValidHeaderFieldValue(value) {
			return nil, fmt.Errorf("header %q has an invalid value", key)
		}
	}
	return parsed, nil
}
