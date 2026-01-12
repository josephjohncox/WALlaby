package grpc

import (
	"errors"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func flowToProto(f flow.Flow) *wallabypb.Flow {
	return &wallabypb.Flow{
		Id:           f.ID,
		Name:         f.Name,
		Source:       endpointToProto(f.Source),
		Destinations: endpointsToProto(f.Destinations),
		State:        flowStateToProto(f.State),
		WireFormat:   wireFormatToProto(f.WireFormat),
		Parallelism:  int32(f.Parallelism),
		Config:       flowConfigToProto(f.Config),
	}
}

func endpointsToProto(specs []connector.Spec) []*wallabypb.Endpoint {
	items := make([]*wallabypb.Endpoint, 0, len(specs))
	for _, spec := range specs {
		items = append(items, endpointToProto(spec))
	}
	return items
}

func flowFromProto(pb *wallabypb.Flow) (flow.Flow, error) {
	if pb == nil {
		return flow.Flow{}, errors.New("flow is required")
	}

	source, err := endpointFromProto(pb.Source)
	if err != nil {
		return flow.Flow{}, err
	}

	dests := make([]connector.Spec, 0, len(pb.Destinations))
	for _, dest := range pb.Destinations {
		spec, err := endpointFromProto(dest)
		if err != nil {
			return flow.Flow{}, err
		}
		dests = append(dests, spec)
	}

	return flow.Flow{
		ID:           pb.Id,
		Name:         pb.Name,
		Source:       source,
		Destinations: dests,
		State:        flowStateFromProto(pb.State),
		WireFormat:   wireFormatFromProto(pb.WireFormat),
		Parallelism:  int(pb.Parallelism),
		Config:       flowConfigFromProto(pb.Config),
	}, nil
}

func flowConfigToProto(cfg flow.Config) *wallabypb.FlowConfig {
	if cfg == (flow.Config{}) {
		return nil
	}
	return &wallabypb.FlowConfig{
		AckPolicy:          ackPolicyToProto(cfg.AckPolicy),
		PrimaryDestination: cfg.PrimaryDestination,
		FailureMode:        failureModeToProto(cfg.FailureMode),
		GiveUpPolicy:       giveUpPolicyToProto(cfg.GiveUpPolicy),
		Ddl:                ddlPolicyToProto(cfg.DDL),
	}
}

func flowConfigFromProto(cfg *wallabypb.FlowConfig) flow.Config {
	if cfg == nil {
		return flow.Config{}
	}
	return flow.Config{
		AckPolicy:          ackPolicyFromProto(cfg.AckPolicy),
		PrimaryDestination: cfg.PrimaryDestination,
		FailureMode:        failureModeFromProto(cfg.FailureMode),
		GiveUpPolicy:       giveUpPolicyFromProto(cfg.GiveUpPolicy),
		DDL:                ddlPolicyFromProto(cfg.Ddl),
	}
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

func endpointToProto(spec connector.Spec) *wallabypb.Endpoint {
	return &wallabypb.Endpoint{
		Name:    spec.Name,
		Type:    endpointTypeToProto(spec.Type),
		Options: spec.Options,
	}
}

func endpointFromProto(endpoint *wallabypb.Endpoint) (connector.Spec, error) {
	if endpoint == nil {
		return connector.Spec{}, errors.New("endpoint is required")
	}
	if endpoint.Type == wallabypb.EndpointType_ENDPOINT_TYPE_UNSPECIFIED {
		return connector.Spec{}, errors.New("endpoint type is required")
	}
	return connector.Spec{
		Name:    endpoint.Name,
		Type:    endpointTypeFromProto(endpoint.Type),
		Options: endpoint.Options,
	}, nil
}

func endpointTypeToProto(t connector.EndpointType) wallabypb.EndpointType {
	switch t {
	case connector.EndpointPostgres:
		return wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES
	case connector.EndpointSnowflake:
		return wallabypb.EndpointType_ENDPOINT_TYPE_SNOWFLAKE
	case connector.EndpointS3:
		return wallabypb.EndpointType_ENDPOINT_TYPE_S3
	case connector.EndpointKafka:
		return wallabypb.EndpointType_ENDPOINT_TYPE_KAFKA
	case connector.EndpointHTTP:
		return wallabypb.EndpointType_ENDPOINT_TYPE_HTTP
	case connector.EndpointGRPC:
		return wallabypb.EndpointType_ENDPOINT_TYPE_GRPC
	case connector.EndpointProto:
		return wallabypb.EndpointType_ENDPOINT_TYPE_PROTO
	case connector.EndpointPGStream:
		return wallabypb.EndpointType_ENDPOINT_TYPE_PGSTREAM
	case connector.EndpointSnowpipe:
		return wallabypb.EndpointType_ENDPOINT_TYPE_SNOWPIPE
	case connector.EndpointParquet:
		return wallabypb.EndpointType_ENDPOINT_TYPE_PARQUET
	case connector.EndpointDuckDB:
		return wallabypb.EndpointType_ENDPOINT_TYPE_DUCKDB
	case connector.EndpointDuckLake:
		return wallabypb.EndpointType_ENDPOINT_TYPE_DUCKLAKE
	case connector.EndpointBufStream:
		return wallabypb.EndpointType_ENDPOINT_TYPE_BUFSTREAM
	case connector.EndpointClickHouse:
		return wallabypb.EndpointType_ENDPOINT_TYPE_CLICKHOUSE
	default:
		return wallabypb.EndpointType_ENDPOINT_TYPE_UNSPECIFIED
	}
}

func endpointTypeFromProto(t wallabypb.EndpointType) connector.EndpointType {
	switch t {
	case wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES:
		return connector.EndpointPostgres
	case wallabypb.EndpointType_ENDPOINT_TYPE_SNOWFLAKE:
		return connector.EndpointSnowflake
	case wallabypb.EndpointType_ENDPOINT_TYPE_S3:
		return connector.EndpointS3
	case wallabypb.EndpointType_ENDPOINT_TYPE_KAFKA:
		return connector.EndpointKafka
	case wallabypb.EndpointType_ENDPOINT_TYPE_HTTP:
		return connector.EndpointHTTP
	case wallabypb.EndpointType_ENDPOINT_TYPE_GRPC:
		return connector.EndpointGRPC
	case wallabypb.EndpointType_ENDPOINT_TYPE_PROTO:
		return connector.EndpointProto
	case wallabypb.EndpointType_ENDPOINT_TYPE_PGSTREAM:
		return connector.EndpointPGStream
	case wallabypb.EndpointType_ENDPOINT_TYPE_SNOWPIPE:
		return connector.EndpointSnowpipe
	case wallabypb.EndpointType_ENDPOINT_TYPE_PARQUET:
		return connector.EndpointParquet
	case wallabypb.EndpointType_ENDPOINT_TYPE_DUCKDB:
		return connector.EndpointDuckDB
	case wallabypb.EndpointType_ENDPOINT_TYPE_DUCKLAKE:
		return connector.EndpointDuckLake
	case wallabypb.EndpointType_ENDPOINT_TYPE_BUFSTREAM:
		return connector.EndpointBufStream
	case wallabypb.EndpointType_ENDPOINT_TYPE_CLICKHOUSE:
		return connector.EndpointClickHouse
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
