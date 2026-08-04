package main

import (
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
)

func mappingsToProto(m *flow.TableMappings) *wallabypb.TableMappings {
	if m == nil {
		return nil
	}
	out := &wallabypb.TableMappings{Version: m.Version}
	if m.Destinations != nil {
		out.Destinations = make([]*wallabypb.DestinationTableMappings, 0, len(m.Destinations))
	}
	for _, d := range m.Destinations {
		pd := &wallabypb.DestinationTableMappings{Destination: d.Destination, FutureTables: futureTableToProto(d.FutureTables)}
		if d.Tables != nil {
			pd.Tables = make([]*wallabypb.TableMapping, 0, len(d.Tables))
		}
		for _, t := range d.Tables {
			pt := &wallabypb.TableMapping{SourceSchema: t.SourceSchema, SourceTable: t.SourceTable, Action: mappingActionToProto(t.Action), TargetSchema: t.TargetSchema, TargetTable: t.TargetTable, FutureColumns: futureColumnToProto(t.FutureColumns), Write: writePolicyToProto(t.Write)}
			if t.Columns != nil {
				pt.Columns = make([]*wallabypb.ColumnMapping, 0, len(t.Columns))
			}
			for _, c := range t.Columns {
				pt.Columns = append(pt.Columns, &wallabypb.ColumnMapping{SourceColumn: c.SourceColumn, Action: mappingActionToProto(c.Action), TargetColumn: c.TargetColumn})
			}
			pd.Tables = append(pd.Tables, pt)
		}
		out.Destinations = append(out.Destinations, pd)
	}
	return out
}

func mappingsFromProto(m *wallabypb.TableMappings) *flow.TableMappings {
	if m == nil {
		return nil
	}
	out := &flow.TableMappings{Version: m.Version}
	if m.Destinations != nil {
		out.Destinations = make([]flow.DestinationTableMappings, 0, len(m.Destinations))
	}
	for _, d := range m.Destinations {
		if d == nil {
			continue
		}
		pd := flow.DestinationTableMappings{Destination: d.Destination, FutureTables: futureTableFromProto(d.FutureTables)}
		if d.Tables != nil {
			pd.Tables = make([]flow.TableMapping, 0, len(d.Tables))
		}
		for _, t := range d.Tables {
			if t == nil {
				continue
			}
			pt := flow.TableMapping{SourceSchema: t.SourceSchema, SourceTable: t.SourceTable, Action: mappingActionFromProto(t.Action), TargetSchema: t.TargetSchema, TargetTable: t.TargetTable, FutureColumns: futureColumnFromProto(t.FutureColumns), Write: writePolicyFromProto(t.Write)}
			if t.Columns != nil {
				pt.Columns = make([]flow.ColumnMapping, 0, len(t.Columns))
			}
			for _, c := range t.Columns {
				if c != nil {
					pt.Columns = append(pt.Columns, flow.ColumnMapping{SourceColumn: c.SourceColumn, Action: mappingActionFromProto(c.Action), TargetColumn: c.TargetColumn})
				}
			}
			pd.Tables = append(pd.Tables, pt)
		}
		out.Destinations = append(out.Destinations, pd)
	}
	return out
}
func futureTableToProto(v flow.FutureTableMapping) *wallabypb.FutureTableMapping {
	return &wallabypb.FutureTableMapping{Action: mappingActionToProto(v.Action), TargetSchema: v.TargetSchema, TargetTable: v.TargetTable, FutureColumns: futureColumnToProto(v.FutureColumns), Write: writePolicyToProto(v.Write)}
}
func futureTableFromProto(v *wallabypb.FutureTableMapping) flow.FutureTableMapping {
	if v == nil {
		return flow.FutureTableMapping{}
	}
	return flow.FutureTableMapping{Action: mappingActionFromProto(v.Action), TargetSchema: v.TargetSchema, TargetTable: v.TargetTable, FutureColumns: futureColumnFromProto(v.FutureColumns), Write: writePolicyFromProto(v.Write)}
}
func futureColumnToProto(v flow.FutureColumnMapping) *wallabypb.FutureColumnMapping {
	return &wallabypb.FutureColumnMapping{Action: mappingActionToProto(v.Action), TargetColumn: v.TargetColumn}
}
func futureColumnFromProto(v *wallabypb.FutureColumnMapping) flow.FutureColumnMapping {
	if v == nil {
		return flow.FutureColumnMapping{}
	}
	return flow.FutureColumnMapping{Action: mappingActionFromProto(v.Action), TargetColumn: v.TargetColumn}
}
func writePolicyToProto(v flow.TableWritePolicy) *wallabypb.TableWritePolicy {
	p := &wallabypb.TableWritePolicy{Mode: writeModeToProto(v.Mode), WatermarkColumn: v.WatermarkColumn}
	if v.KeyColumns != nil {
		p.KeyColumns = append([]string(nil), v.KeyColumns...)
	}
	return p
}
func writePolicyFromProto(v *wallabypb.TableWritePolicy) flow.TableWritePolicy {
	if v == nil {
		return flow.TableWritePolicy{}
	}
	p := flow.TableWritePolicy{Mode: writeModeFromProto(v.Mode), WatermarkColumn: v.WatermarkColumn}
	if v.KeyColumns != nil {
		p.KeyColumns = append([]string(nil), v.KeyColumns...)
	}
	return p
}
func mappingActionToProto(v flow.MappingAction) wallabypb.MappingAction {
	if v == flow.MappingActionInclude {
		return wallabypb.MappingAction_MAPPING_ACTION_INCLUDE
	}
	if v == flow.MappingActionExclude {
		return wallabypb.MappingAction_MAPPING_ACTION_EXCLUDE
	}
	return wallabypb.MappingAction_MAPPING_ACTION_UNSPECIFIED
}
func mappingActionFromProto(v wallabypb.MappingAction) flow.MappingAction {
	if v == wallabypb.MappingAction_MAPPING_ACTION_INCLUDE {
		return flow.MappingActionInclude
	}
	if v == wallabypb.MappingAction_MAPPING_ACTION_EXCLUDE {
		return flow.MappingActionExclude
	}
	return ""
}
func writeModeToProto(v flow.TableWriteMode) wallabypb.TableWriteMode {
	if v == flow.TableWriteModeAppend {
		return wallabypb.TableWriteMode_TABLE_WRITE_MODE_APPEND
	}
	if v == flow.TableWriteModeUpsert {
		return wallabypb.TableWriteMode_TABLE_WRITE_MODE_UPSERT
	}
	return wallabypb.TableWriteMode_TABLE_WRITE_MODE_UNSPECIFIED
}
func writeModeFromProto(v wallabypb.TableWriteMode) flow.TableWriteMode {
	if v == wallabypb.TableWriteMode_TABLE_WRITE_MODE_APPEND {
		return flow.TableWriteModeAppend
	}
	if v == wallabypb.TableWriteMode_TABLE_WRITE_MODE_UPSERT {
		return flow.TableWriteModeUpsert
	}
	return ""
}
