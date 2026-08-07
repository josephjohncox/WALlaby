package grpc

import (
	"errors"
	"fmt"
	"math"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func flowToProto(f flow.Flow, registry *connector.Registry) (*wallabypb.Flow, error) {
	if _, err := endpointcodec.DecodeWithRegistry(f.Source, endpointcodec.RoleSource, registry); err != nil {
		return nil, fmt.Errorf("encode durable source endpoint: %w", err)
	}
	destinations := make([]*wallabypb.Endpoint, 0, len(f.Destinations))
	for index, endpoint := range f.Destinations {
		if _, err := endpointcodec.DecodeWithRegistry(endpoint, endpointcodec.RoleDestination, registry); err != nil {
			return nil, fmt.Errorf("encode durable destination %d: %w", index, err)
		}
		destinations = append(destinations, endpointcodec.Clone(endpoint))
	}
	return &wallabypb.Flow{
		Id:           f.ID,
		Name:         f.Name,
		Source:       endpointcodec.Clone(f.Source),
		Destinations: destinations,
		State:        flowStateToProto(f.State),
		WireFormat:   wireFormatToProto(f.WireFormat),
		Parallelism:  safeInt32(f.Parallelism),
		Config:       flowConfigToProto(f.Config),
	}, nil
}

func flowFromProtoWithRegistry(pb *wallabypb.Flow, registry *connector.Registry) (flow.Flow, error) {
	if pb == nil {
		return flow.Flow{}, errors.New("flow is required")
	}
	if err := validateKnownProtoEnums(pb.ProtoReflect(), "flow"); err != nil {
		return flow.Flow{}, err
	}
	if pb.Config != nil {
		switch pb.Config.AckPolicy {
		case wallabypb.AckPolicy_ACK_POLICY_UNSPECIFIED,
			wallabypb.AckPolicy_ACK_POLICY_ALL,
			wallabypb.AckPolicy_ACK_POLICY_PRIMARY,
			wallabypb.AckPolicy_ACK_POLICY_MATERIALIZED:
		default:
			return flow.Flow{}, fmt.Errorf("unsupported acknowledgement policy %d", pb.Config.AckPolicy)
		}
	}

	if _, err := endpointcodec.DecodeWithRegistry(pb.Source, endpointcodec.RoleSource, registry); err != nil {
		return flow.Flow{}, err
	}

	dests := make([]*wallabypb.Endpoint, 0, len(pb.Destinations))
	for _, dest := range pb.Destinations {
		if _, err := endpointcodec.DecodeWithRegistry(dest, endpointcodec.RoleDestination, registry); err != nil {
			return flow.Flow{}, err
		}
		dests = append(dests, endpointcodec.Clone(dest))
	}
	config, err := flowConfigFromProto(pb.Config)
	if err != nil {
		return flow.Flow{}, err
	}

	return flow.Flow{
		ID:           pb.Id,
		Name:         pb.Name,
		Source:       endpointcodec.Clone(pb.Source),
		Destinations: dests,
		State:        flowStateFromProto(pb.State),
		WireFormat:   wireFormatFromProto(pb.WireFormat),
		Parallelism:  int(pb.Parallelism),
		Config:       config,
	}, nil
}

func validateKnownProtoEnums(message protoreflect.Message, path string) error {
	var validationErr error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		fieldPath := path + "." + string(field.Name())
		switch {
		case field.IsMap():
			if field.MapValue().Kind() == protoreflect.MessageKind {
				value.Map().Range(func(key protoreflect.MapKey, item protoreflect.Value) bool {
					if err := validateKnownProtoEnums(item.Message(), fmt.Sprintf("%s[%q]", fieldPath, key.String())); err != nil {
						validationErr = err
						return false
					}
					return true
				})
			}
		case field.IsList():
			for index := 0; index < value.List().Len(); index++ {
				item := value.List().Get(index)
				if field.Kind() == protoreflect.EnumKind && field.Enum().Values().ByNumber(item.Enum()) == nil {
					validationErr = fmt.Errorf("%s[%d] contains unknown enum value %d", fieldPath, index, item.Enum())
					return false
				}
				if field.Kind() == protoreflect.MessageKind {
					if err := validateKnownProtoEnums(item.Message(), fmt.Sprintf("%s[%d]", fieldPath, index)); err != nil {
						validationErr = err
						return false
					}
				}
			}
		case field.Kind() == protoreflect.EnumKind:
			if field.Enum().Values().ByNumber(value.Enum()) == nil {
				validationErr = fmt.Errorf("%s contains unknown enum value %d", fieldPath, value.Enum())
				return false
			}
		case field.Kind() == protoreflect.MessageKind:
			if err := validateKnownProtoEnums(value.Message(), fieldPath); err != nil {
				validationErr = err
				return false
			}
		}
		return true
	})
	return validationErr
}

func safeInt32(value int) int32 {
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	if value < math.MinInt32 {
		return math.MinInt32
	}
	// #nosec G115 -- value clamped to int32 range above.
	return int32(value)
}

func flowConfigToProto(cfg flow.Config) *wallabypb.FlowConfig {
	if cfg.IsZero() {
		return nil
	}
	return &wallabypb.FlowConfig{
		AckPolicy:          ackPolicyToProto(cfg.AckPolicy),
		PrimaryDestination: cfg.PrimaryDestination,
		FailureMode:        failureModeToProto(cfg.FailureMode),
		GiveUpPolicy:       giveUpPolicyToProto(cfg.GiveUpPolicy),
		Ddl:                ddlPolicyToProto(cfg.DDL),
		Materialization:    materializationPolicyToProto(cfg.Materialization),
		TableMappings:      tableMappingsToProto(cfg.TableMappings),
	}
}

func flowConfigFromProto(cfg *wallabypb.FlowConfig) (flow.Config, error) {
	if cfg == nil {
		return flow.Config{}, nil
	}
	mappings, err := tableMappingsFromProto(cfg.TableMappings)
	if err != nil {
		return flow.Config{}, err
	}
	return flow.Config{
		AckPolicy:          ackPolicyFromProto(cfg.AckPolicy),
		PrimaryDestination: cfg.PrimaryDestination,
		FailureMode:        failureModeFromProto(cfg.FailureMode),
		GiveUpPolicy:       giveUpPolicyFromProto(cfg.GiveUpPolicy),
		DDL:                ddlPolicyFromProto(cfg.Ddl),
		Materialization:    materializationPolicyFromProto(cfg.Materialization),
		TableMappings:      mappings,
	}, nil
}

func tableMappingsToProto(mappings flow.TableMappings) *wallabypb.TableMappings {
	if mappings.Version == 0 && len(mappings.Destinations) == 0 {
		return nil
	}
	out := &wallabypb.TableMappings{Version: mappings.Version, Destinations: make([]*wallabypb.DestinationTableMappings, 0, len(mappings.Destinations))}
	for _, destination := range mappings.Destinations {
		mapped := &wallabypb.DestinationTableMappings{
			Destination:  destination.Destination,
			FutureTables: futureTableMappingToProto(destination.FutureTables),
			Tables:       make([]*wallabypb.TableMapping, 0, len(destination.Tables)),
		}
		for _, table := range destination.Tables {
			mappedTable := &wallabypb.TableMapping{
				SourceSchema: table.SourceSchema, SourceTable: table.SourceTable, Action: mappingActionToProto(table.Action),
				TargetSchema: table.TargetSchema, TargetTable: table.TargetTable,
				FutureColumns: futureColumnMappingToProto(table.FutureColumns), Write: tableWritePolicyToProto(table.Write),
				Columns: make([]*wallabypb.ColumnMapping, 0, len(table.Columns)),
			}
			for _, column := range table.Columns {
				mappedTable.Columns = append(mappedTable.Columns, &wallabypb.ColumnMapping{SourceColumn: column.SourceColumn, Action: mappingActionToProto(column.Action), TargetColumn: column.TargetColumn})
			}
			mapped.Tables = append(mapped.Tables, mappedTable)
		}
		out.Destinations = append(out.Destinations, mapped)
	}
	return out
}

func tableMappingsFromProto(mappings *wallabypb.TableMappings) (flow.TableMappings, error) {
	if mappings == nil {
		return flow.TableMappings{}, nil
	}
	out := flow.TableMappings{Version: mappings.Version, Destinations: make([]flow.DestinationTableMappings, 0, len(mappings.Destinations))}
	for destinationIndex, destination := range mappings.Destinations {
		if destination == nil {
			return flow.TableMappings{}, fmt.Errorf("table mappings destination entry %d is nil", destinationIndex)
		}
		mapped := flow.DestinationTableMappings{
			Destination: destination.Destination, FutureTables: futureTableMappingFromProto(destination.FutureTables),
			Tables: make([]flow.TableMapping, 0, len(destination.Tables)),
		}
		for tableIndex, table := range destination.Tables {
			if table == nil {
				return flow.TableMappings{}, fmt.Errorf("table mappings destination entry %d table entry %d is nil", destinationIndex, tableIndex)
			}
			mappedTable := flow.TableMapping{
				SourceSchema: table.SourceSchema, SourceTable: table.SourceTable, Action: mappingActionFromProto(table.Action),
				TargetSchema: table.TargetSchema, TargetTable: table.TargetTable,
				FutureColumns: futureColumnMappingFromProto(table.FutureColumns), Write: tableWritePolicyFromProto(table.Write),
				Columns: make([]flow.ColumnMapping, 0, len(table.Columns)),
			}
			for columnIndex, column := range table.Columns {
				if column == nil {
					return flow.TableMappings{}, fmt.Errorf("table mappings destination entry %d table entry %d column entry %d is nil", destinationIndex, tableIndex, columnIndex)
				}
				mappedTable.Columns = append(mappedTable.Columns, flow.ColumnMapping{SourceColumn: column.SourceColumn, Action: mappingActionFromProto(column.Action), TargetColumn: column.TargetColumn})
			}
			mapped.Tables = append(mapped.Tables, mappedTable)
		}
		out.Destinations = append(out.Destinations, mapped)
	}
	return out, nil
}

func futureTableMappingToProto(mapping flow.FutureTableMapping) *wallabypb.FutureTableMapping {
	if mapping.Action == "" && mapping.TargetSchema == "" && mapping.TargetTable == "" && mapping.FutureColumns == (flow.FutureColumnMapping{}) &&
		mapping.Write.Mode == "" && len(mapping.Write.KeyColumns) == 0 && mapping.Write.WatermarkColumn == "" {
		return nil
	}
	return &wallabypb.FutureTableMapping{Action: mappingActionToProto(mapping.Action), TargetSchema: mapping.TargetSchema, TargetTable: mapping.TargetTable, FutureColumns: futureColumnMappingToProto(mapping.FutureColumns), Write: tableWritePolicyToProto(mapping.Write)}
}

func futureTableMappingFromProto(mapping *wallabypb.FutureTableMapping) flow.FutureTableMapping {
	if mapping == nil {
		return flow.FutureTableMapping{}
	}
	return flow.FutureTableMapping{Action: mappingActionFromProto(mapping.Action), TargetSchema: mapping.TargetSchema, TargetTable: mapping.TargetTable, FutureColumns: futureColumnMappingFromProto(mapping.FutureColumns), Write: tableWritePolicyFromProto(mapping.Write)}
}

func futureColumnMappingToProto(mapping flow.FutureColumnMapping) *wallabypb.FutureColumnMapping {
	if mapping == (flow.FutureColumnMapping{}) {
		return nil
	}
	return &wallabypb.FutureColumnMapping{Action: mappingActionToProto(mapping.Action), TargetColumn: mapping.TargetColumn}
}

func futureColumnMappingFromProto(mapping *wallabypb.FutureColumnMapping) flow.FutureColumnMapping {
	if mapping == nil {
		return flow.FutureColumnMapping{}
	}
	return flow.FutureColumnMapping{Action: mappingActionFromProto(mapping.Action), TargetColumn: mapping.TargetColumn}
}

func tableWritePolicyToProto(policy flow.TableWritePolicy) *wallabypb.TableWritePolicy {
	if policy.Mode == "" && len(policy.KeyColumns) == 0 && policy.WatermarkColumn == "" {
		return nil
	}
	return &wallabypb.TableWritePolicy{Mode: tableWriteModeToProto(policy.Mode), KeyColumns: append([]string(nil), policy.KeyColumns...), WatermarkColumn: policy.WatermarkColumn}
}

func tableWritePolicyFromProto(policy *wallabypb.TableWritePolicy) flow.TableWritePolicy {
	if policy == nil {
		return flow.TableWritePolicy{}
	}
	return flow.TableWritePolicy{Mode: tableWriteModeFromProto(policy.Mode), KeyColumns: append([]string(nil), policy.KeyColumns...), WatermarkColumn: policy.WatermarkColumn}
}

func mappingActionToProto(action flow.MappingAction) wallabypb.MappingAction {
	switch action {
	case flow.MappingActionInclude:
		return wallabypb.MappingAction_MAPPING_ACTION_INCLUDE
	case flow.MappingActionExclude:
		return wallabypb.MappingAction_MAPPING_ACTION_EXCLUDE
	default:
		return wallabypb.MappingAction_MAPPING_ACTION_UNSPECIFIED
	}
}
func mappingActionFromProto(action wallabypb.MappingAction) flow.MappingAction {
	switch action {
	case wallabypb.MappingAction_MAPPING_ACTION_INCLUDE:
		return flow.MappingActionInclude
	case wallabypb.MappingAction_MAPPING_ACTION_EXCLUDE:
		return flow.MappingActionExclude
	default:
		return ""
	}
}
func tableWriteModeToProto(mode flow.TableWriteMode) wallabypb.TableWriteMode {
	switch mode {
	case flow.TableWriteModeAppend:
		return wallabypb.TableWriteMode_TABLE_WRITE_MODE_APPEND
	case flow.TableWriteModeUpsert:
		return wallabypb.TableWriteMode_TABLE_WRITE_MODE_UPSERT
	default:
		return wallabypb.TableWriteMode_TABLE_WRITE_MODE_UNSPECIFIED
	}
}
func tableWriteModeFromProto(mode wallabypb.TableWriteMode) flow.TableWriteMode {
	switch mode {
	case wallabypb.TableWriteMode_TABLE_WRITE_MODE_APPEND:
		return flow.TableWriteModeAppend
	case wallabypb.TableWriteMode_TABLE_WRITE_MODE_UPSERT:
		return flow.TableWriteModeUpsert
	default:
		return ""
	}
}
func materializationPolicyToProto(policy flow.MaterializationPolicy) *wallabypb.MaterializationPolicy {
	if policy == (flow.MaterializationPolicy{}) {
		return nil
	}
	return &wallabypb.MaterializationPolicy{ProjectionId: policy.ProjectionID}
}

func materializationPolicyFromProto(policy *wallabypb.MaterializationPolicy) flow.MaterializationPolicy {
	if policy == nil {
		return flow.MaterializationPolicy{}
	}
	return flow.MaterializationPolicy{ProjectionID: policy.ProjectionId}
}

func ddlPolicyToProto(policy flow.DDLPolicy) *wallabypb.DDLPolicy {
	if policy == (flow.DDLPolicy{}) {
		return nil
	}
	out := &wallabypb.DDLPolicy{}
	if policy.Gate != nil {
		out.Gate = policy.Gate
	}
	if policy.AutoApprove != nil {
		out.AutoApprove = policy.AutoApprove
	}
	if policy.AutoApply != nil {
		out.AutoApply = policy.AutoApply
	}
	return out
}

func ddlPolicyFromProto(pb *wallabypb.DDLPolicy) flow.DDLPolicy {
	if pb == nil {
		return flow.DDLPolicy{}
	}
	return flow.DDLPolicy{
		Gate:        pb.Gate,
		AutoApprove: pb.AutoApprove,
		AutoApply:   pb.AutoApply,
	}
}

func ackPolicyToProto(policy stream.AckPolicy) wallabypb.AckPolicy {
	switch policy {
	case stream.AckPolicyAll:
		return wallabypb.AckPolicy_ACK_POLICY_ALL
	case stream.AckPolicyPrimary:
		return wallabypb.AckPolicy_ACK_POLICY_PRIMARY
	case stream.AckPolicyMaterialized:
		return wallabypb.AckPolicy_ACK_POLICY_MATERIALIZED
	default:
		return wallabypb.AckPolicy_ACK_POLICY_UNSPECIFIED
	}
}

func ackPolicyFromProto(policy wallabypb.AckPolicy) stream.AckPolicy {
	switch policy {
	case wallabypb.AckPolicy_ACK_POLICY_ALL:
		return stream.AckPolicyAll
	case wallabypb.AckPolicy_ACK_POLICY_PRIMARY:
		return stream.AckPolicyPrimary
	case wallabypb.AckPolicy_ACK_POLICY_MATERIALIZED:
		return stream.AckPolicyMaterialized
	default:
		return ""
	}
}

func failureModeToProto(mode stream.FailureMode) wallabypb.FailureMode {
	switch mode {
	case stream.FailureModeHoldSlot:
		return wallabypb.FailureMode_FAILURE_MODE_HOLD_SLOT
	case stream.FailureModeDropSlot:
		return wallabypb.FailureMode_FAILURE_MODE_DROP_SLOT
	default:
		return wallabypb.FailureMode_FAILURE_MODE_UNSPECIFIED
	}
}

func failureModeFromProto(mode wallabypb.FailureMode) stream.FailureMode {
	switch mode {
	case wallabypb.FailureMode_FAILURE_MODE_HOLD_SLOT:
		return stream.FailureModeHoldSlot
	case wallabypb.FailureMode_FAILURE_MODE_DROP_SLOT:
		return stream.FailureModeDropSlot
	default:
		return ""
	}
}

func giveUpPolicyToProto(policy stream.GiveUpPolicy) wallabypb.GiveUpPolicy {
	switch policy {
	case stream.GiveUpPolicyNever:
		return wallabypb.GiveUpPolicy_GIVE_UP_POLICY_NEVER
	case stream.GiveUpPolicyOnRetryExhaustion:
		return wallabypb.GiveUpPolicy_GIVE_UP_POLICY_ON_RETRY_EXHAUSTION
	default:
		return wallabypb.GiveUpPolicy_GIVE_UP_POLICY_UNSPECIFIED
	}
}

func giveUpPolicyFromProto(policy wallabypb.GiveUpPolicy) stream.GiveUpPolicy {
	switch policy {
	case wallabypb.GiveUpPolicy_GIVE_UP_POLICY_NEVER:
		return stream.GiveUpPolicyNever
	case wallabypb.GiveUpPolicy_GIVE_UP_POLICY_ON_RETRY_EXHAUSTION:
		return stream.GiveUpPolicyOnRetryExhaustion
	default:
		return ""
	}
}

func flowStateToProto(state flow.State) wallabypb.FlowState {
	switch state {
	case flow.StateCreated:
		return wallabypb.FlowState_FLOW_STATE_CREATED
	case flow.StateRunning:
		return wallabypb.FlowState_FLOW_STATE_RUNNING
	case flow.StatePaused:
		return wallabypb.FlowState_FLOW_STATE_PAUSED
	case flow.StateStopping:
		return wallabypb.FlowState_FLOW_STATE_STOPPING
	case flow.StateStopped:
		return wallabypb.FlowState_FLOW_STATE_STOPPED
	case flow.StateFailed:
		return wallabypb.FlowState_FLOW_STATE_FAILED
	default:
		return wallabypb.FlowState_FLOW_STATE_UNSPECIFIED
	}
}

func flowStateFromProto(state wallabypb.FlowState) flow.State {
	switch state {
	case wallabypb.FlowState_FLOW_STATE_CREATED:
		return flow.StateCreated
	case wallabypb.FlowState_FLOW_STATE_RUNNING:
		return flow.StateRunning
	case wallabypb.FlowState_FLOW_STATE_PAUSED:
		return flow.StatePaused
	case wallabypb.FlowState_FLOW_STATE_STOPPING:
		return flow.StateStopping
	case wallabypb.FlowState_FLOW_STATE_STOPPED:
		return flow.StateStopped
	case wallabypb.FlowState_FLOW_STATE_FAILED:
		return flow.StateFailed
	default:
		return ""
	}
}

func wireFormatToProto(format connector.WireFormat) wallabypb.WireFormat {
	switch format {
	case connector.WireFormatArrow:
		return wallabypb.WireFormat_WIRE_FORMAT_ARROW
	case connector.WireFormatParquet:
		return wallabypb.WireFormat_WIRE_FORMAT_PARQUET
	case connector.WireFormatProto:
		return wallabypb.WireFormat_WIRE_FORMAT_PROTO
	case connector.WireFormatAvro:
		return wallabypb.WireFormat_WIRE_FORMAT_AVRO
	case connector.WireFormatJSON:
		return wallabypb.WireFormat_WIRE_FORMAT_JSON
	default:
		return wallabypb.WireFormat_WIRE_FORMAT_UNSPECIFIED
	}
}

func wireFormatFromProto(format wallabypb.WireFormat) connector.WireFormat {
	switch format {
	case wallabypb.WireFormat_WIRE_FORMAT_ARROW:
		return connector.WireFormatArrow
	case wallabypb.WireFormat_WIRE_FORMAT_PARQUET:
		return connector.WireFormatParquet
	case wallabypb.WireFormat_WIRE_FORMAT_PROTO:
		return connector.WireFormatProto
	case wallabypb.WireFormat_WIRE_FORMAT_AVRO:
		return connector.WireFormatAvro
	case wallabypb.WireFormat_WIRE_FORMAT_JSON:
		return connector.WireFormatJSON
	default:
		return ""
	}
}
