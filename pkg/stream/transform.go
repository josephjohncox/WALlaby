package stream

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

const (
	optMetaEnabled      = "meta_enabled"
	optMetaSyncedAt     = "meta_synced_at"
	optMetaDeleted      = "meta_deleted"
	optMetaWatermark    = "meta_watermark"
	optMetaOp           = "meta_op"
	optWatermarkSource  = "watermark_source"
	optAppendMode       = "append_mode"
	optSoftDelete       = "soft_delete"
	optTypeMappings     = "type_mappings"
	optTypeMappingsFile = "type_mappings_file"
)

var reservedMetaColumns = map[string]struct{}{
	"__op":             {},
	"__ts":             {},
	"__schema_version": {},
	"__table":          {},
	"__namespace":      {},
	"__key":            {},
	"__before_json":    {},
}

type destTransform struct {
	metaEnabled     bool
	appendMode      bool
	softDelete      bool
	syncedAtColumn  string
	deletedColumn   string
	watermarkColumn string
	opColumn        string
	watermarkSource string
	typeMappings    map[string]string
}

func transformBatchForDestination(batch connector.Batch, spec connector.Spec, baseMappings map[string]string) (connector.Batch, bool, error) {
	cfg, err := parseDestTransform(spec.Options, baseMappings)
	if err != nil {
		return connector.Batch{}, false, err
	}

	metaCols, err := metadataColumns(cfg)
	if err != nil {
		return connector.Batch{}, false, err
	}

	schema := batch.Schema
	records := batch.Records
	changed := false
	hasData := false
	for _, record := range batch.Records {
		if record.Operation != connector.OpDDL {
			hasData = true
			break
		}
	}

	if hasData && (cfg.metaEnabled || cfg.appendMode || cfg.softDelete) {
		updatedSchema, err := applyMetadataSchema(schema, metaCols)
		if err != nil {
			return connector.Batch{}, false, err
		}
		schema = updatedSchema
		records = make([]connector.Record, 0, len(batch.Records))
		for _, record := range batch.Records {
			if record.Operation == connector.OpDDL {
				records = append(records, record)
				continue
			}
			updated, err := applyRecordTransform(record, batch.Checkpoint, cfg, metaCols)
			if err != nil {
				return connector.Batch{}, false, err
			}
			records = append(records, updated)
		}
		changed = true
	}

	if hasData && len(cfg.typeMappings) > 0 {
		schema = applyTypeMappings(schema, cfg.typeMappings)
		changed = true
	}

	if !changed {
		return batch, false, nil
	}

	return connector.Batch{
		Records:    records,
		Schema:     schema,
		Checkpoint: batch.Checkpoint,
		WireFormat: batch.WireFormat,
	}, true, nil
}

func parseDestTransform(options map[string]string, baseMappings map[string]string) (destTransform, error) {
	cfg := destTransform{
		metaEnabled:     parseBool(options[optMetaEnabled], false),
		appendMode:      parseBool(options[optAppendMode], false),
		softDelete:      parseBool(options[optSoftDelete], false),
		syncedAtColumn:  options[optMetaSyncedAt],
		deletedColumn:   options[optMetaDeleted],
		watermarkColumn: options[optMetaWatermark],
		opColumn:        options[optMetaOp],
		watermarkSource: options[optWatermarkSource],
	}

	if cfg.syncedAtColumn == "" {
		cfg.syncedAtColumn = "__ds_synced_at"
	}
	if cfg.deletedColumn == "" {
		cfg.deletedColumn = "__ds_is_deleted"
	}
	if cfg.watermarkColumn == "" {
		cfg.watermarkColumn = "__ds_watermark"
	}
	if cfg.opColumn == "" {
		cfg.opColumn = "__ds_op"
	}
	if cfg.watermarkSource == "" {
		cfg.watermarkSource = "timestamp"
	}
	if cfg.appendMode || cfg.softDelete || options[optMetaSyncedAt] != "" || options[optMetaDeleted] != "" || options[optMetaWatermark] != "" || options[optMetaOp] != "" {
		cfg.metaEnabled = true
	}

	mappings, err := loadTypeMappings(options)
	if err != nil {
		return destTransform{}, err
	}
	cfg.typeMappings = mergeTypeMappings(baseMappings, mappings)

	return cfg, nil
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

type metadataColumn struct {
	Name string
	Type string
}

func metadataColumns(cfg destTransform) ([]metadataColumn, error) {
	if !cfg.metaEnabled {
		return nil, nil
	}

	cols := []metadataColumn{
		{Name: cfg.syncedAtColumn, Type: "timestamptz"},
		{Name: cfg.deletedColumn, Type: "boolean"},
	}

	if cfg.appendMode || cfg.opColumn != "" && cfg.opColumn != "-" {
		cols = append(cols, metadataColumn{Name: cfg.opColumn, Type: "text"})
	}

	if cfg.watermarkColumn != "" && cfg.watermarkColumn != "-" {
		if cfg.watermarkSource != "none" && cfg.watermarkSource != "disabled" {
			colType := "timestamptz"
			if cfg.watermarkSource == "lsn" {
				colType = "text"
			}
			cols = append(cols, metadataColumn{Name: cfg.watermarkColumn, Type: colType})
		}
	}

	seen := map[string]struct{}{}
	for _, col := range cols {
		if col.Name == "" {
			continue
		}
		if _, ok := reservedMetaColumns[col.Name]; ok {
			return nil, fmt.Errorf("metadata column name %q is reserved", col.Name)
		}
		lower := strings.ToLower(col.Name)
		if _, ok := seen[lower]; ok {
			return nil, fmt.Errorf("duplicate metadata column name %q", col.Name)
		}
		seen[lower] = struct{}{}
	}

	return cols, nil
}

func applyMetadataSchema(schema connector.Schema, cols []metadataColumn) (connector.Schema, error) {
	if len(cols) == 0 {
		return schema, nil
	}

	existing := map[string]struct{}{}
	for _, col := range schema.Columns {
		existing[strings.ToLower(col.Name)] = struct{}{}
	}

	for _, col := range cols {
		if col.Name == "" {
			continue
		}
		if _, ok := existing[strings.ToLower(col.Name)]; ok {
			return connector.Schema{}, fmt.Errorf("metadata column %q collides with existing schema column", col.Name)
		}
	}

	next := connector.Schema{
		Name:      schema.Name,
		Namespace: schema.Namespace,
		Version:   schema.Version,
		Columns:   make([]connector.Column, 0, len(schema.Columns)+len(cols)),
	}
	next.Columns = append(next.Columns, schema.Columns...)
	for _, col := range cols {
		if col.Name == "" {
			continue
		}
		next.Columns = append(next.Columns, connector.Column{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: true,
		})
	}

	return next, nil
}

func applyRecordTransform(record connector.Record, checkpoint connector.Checkpoint, cfg destTransform, cols []metadataColumn) (connector.Record, error) {
	if !cfg.metaEnabled && !cfg.appendMode && !cfg.softDelete {
		return record, nil
	}

	out := record
	origOp := record.Operation

	after := cloneMap(record.After)
	if after == nil {
		if record.Operation == connector.OpDelete && (cfg.softDelete || cfg.appendMode) && record.Before != nil {
			after = cloneMap(record.Before)
		} else {
			after = map[string]any{}
		}
	}

	deleted := false
	if record.Operation == connector.OpDelete {
		deleted = true
		if cfg.softDelete {
			out.Operation = connector.OpUpdate
		}
		if cfg.appendMode {
			out.Operation = connector.OpInsert
		}
	}
	if record.Operation == connector.OpUpdate && cfg.appendMode {
		out.Operation = connector.OpInsert
	}

	if cfg.metaEnabled {
		syncedAt := record.Timestamp
		if syncedAt.IsZero() {
			syncedAt = time.Now().UTC()
		}
		setIfPresent(cols, cfg.syncedAtColumn, after, syncedAt)
		setIfPresent(cols, cfg.deletedColumn, after, deleted)

		if cfg.appendMode {
			setIfPresent(cols, cfg.opColumn, after, string(origOp))
		}

		if cfg.watermarkColumn != "" && cfg.watermarkColumn != "-" && cfg.watermarkSource != "none" && cfg.watermarkSource != "disabled" {
			switch cfg.watermarkSource {
			case "lsn":
				if checkpoint.LSN != "" {
					setIfPresent(cols, cfg.watermarkColumn, after, checkpoint.LSN)
				}
			default:
				setIfPresent(cols, cfg.watermarkColumn, after, syncedAt)
			}
		}
	}

	out.After = after

	return out, nil
}

func setIfPresent(cols []metadataColumn, name string, target map[string]any, value any) {
	if target == nil || name == "" {
		return
	}
	for _, col := range cols {
		if col.Name == name {
			target[name] = value
			return
		}
	}
}

func cloneMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	out := make(map[string]any, len(source))
	for key, value := range source {
		out[key] = value
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
		next.Type = mapType(col.Type, mappings)
		cols = append(cols, next)
	}
	schema.Columns = cols
	return schema
}

func mapType(value string, mappings map[string]string) string {
	if value == "" || len(mappings) == 0 {
		return value
	}
	key := normalizeTypeKey(value)
	if mapped, ok := mappings[key]; ok {
		return mapped
	}
	if idx := strings.LastIndex(key, "."); idx > 0 {
		if mapped, ok := mappings[key[idx+1:]]; ok {
			return mapped
		}
	}
	return value
}

func normalizeTypeKey(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

type cachedTypeMapping struct {
	modTime  time.Time
	mappings map[string]string
}

var typeMappingCache sync.Map

func loadTypeMappings(options map[string]string) (map[string]string, error) {
	if options == nil {
		return nil, nil
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
	return nil, nil
}

func parseTypeMappings(raw string) (map[string]string, error) {
	var mappings map[string]string
	if err := json.Unmarshal([]byte(raw), &mappings); err != nil {
		return nil, fmt.Errorf("parse type_mappings: %w", err)
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

func parseBool(value string, fallback bool) bool {
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
