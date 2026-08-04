package mappinggen

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/josephjohncox/wallaby/internal/flow"
)

type TableRef struct {
	Schema string
	Table  string
}

type Request struct {
	Destination  string
	Tables       []CatalogTable
	Watermarks   map[TableRef]string
	MatchColumns map[TableRef][]string
}

// Generate returns the canonical destination mapping implied by one exact
// catalog snapshot. It never copies connection or credential material.
func Generate(request Request) (flow.TableMappings, error) {
	destination := strings.TrimSpace(request.Destination)
	if destination == "" {
		return flow.TableMappings{}, errors.New("destination is required")
	}
	tables := append([]CatalogTable(nil), request.Tables...)
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Schema != tables[j].Schema {
			return tables[i].Schema < tables[j].Schema
		}
		return tables[i].Table < tables[j].Table
	})
	selected := make(map[TableRef]map[string]struct{}, len(tables))
	result := flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: []flow.DestinationTableMappings{{Destination: destination, FutureTables: flow.FutureTableMapping{Action: flow.MappingActionInclude, TargetSchema: "{schema}", TargetTable: "{table}", FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{column}"}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeAppend}}}}}
	mapping := &result.Destinations[0]
	for _, table := range tables {
		ref := TableRef{Schema: table.Schema, Table: table.Table}
		if _, duplicate := selected[ref]; duplicate {
			return flow.TableMappings{}, fmt.Errorf("duplicate catalog table %s.%s", table.Schema, table.Table)
		}
		columns := append([]CatalogColumn(nil), table.Columns...)
		sort.Slice(columns, func(i, j int) bool { return columns[i].Attnum < columns[j].Attnum })
		names := make(map[string]struct{}, len(columns))
		columnMappings := make([]flow.ColumnMapping, 0, len(columns))
		for _, column := range columns {
			if _, duplicate := names[column.Name]; duplicate {
				return flow.TableMappings{}, fmt.Errorf("duplicate catalog column %s.%s.%s", table.Schema, table.Table, column.Name)
			}
			names[column.Name] = struct{}{}
			columnMappings = append(columnMappings, flow.ColumnMapping{SourceColumn: column.Name, Action: flow.MappingActionInclude, TargetColumn: column.Name})
		}
		selected[ref] = names
		keys := append([]string(nil), table.PrimaryKeyColumns...)
		if explicit, ok := request.MatchColumns[ref]; ok {
			if len(explicit) == 0 {
				return flow.TableMappings{}, fmt.Errorf("match-column override for %s.%s is empty", table.Schema, table.Table)
			}
			keys = append([]string(nil), explicit...)
		}
		seenKeys := make(map[string]struct{}, len(keys))
		for _, key := range keys {
			if _, duplicate := seenKeys[key]; duplicate {
				return flow.TableMappings{}, fmt.Errorf("duplicate match column %s for %s.%s", key, table.Schema, table.Table)
			}
			seenKeys[key] = struct{}{}
			if _, ok := names[key]; !ok {
				return flow.TableMappings{}, fmt.Errorf("match column %s for %s.%s is not a selected real column", key, table.Schema, table.Table)
			}
		}
		policy := flow.TableWritePolicy{Mode: flow.TableWriteModeAppend}
		if len(keys) > 0 {
			policy.Mode = flow.TableWriteModeUpsert
			policy.KeyColumns = keys
		}
		if watermark, ok := request.Watermarks[ref]; ok {
			if _, exists := names[watermark]; !exists {
				return flow.TableMappings{}, fmt.Errorf("watermark column %s for %s.%s is not a selected real column", watermark, table.Schema, table.Table)
			}
			policy.WatermarkColumn = watermark
		}
		mapping.Tables = append(mapping.Tables, flow.TableMapping{SourceSchema: table.Schema, SourceTable: table.Table, Action: flow.MappingActionInclude, TargetSchema: table.Schema, TargetTable: table.Table, FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{column}"}, Columns: columnMappings, Write: policy})
	}
	for ref := range request.MatchColumns {
		if _, ok := selected[ref]; !ok {
			return flow.TableMappings{}, fmt.Errorf("match-column override references unselected table %s.%s", ref.Schema, ref.Table)
		}
	}
	for ref := range request.Watermarks {
		if _, ok := selected[ref]; !ok {
			return flow.TableMappings{}, fmt.Errorf("watermark override references unselected table %s.%s", ref.Schema, ref.Table)
		}
	}
	return result, nil
}
