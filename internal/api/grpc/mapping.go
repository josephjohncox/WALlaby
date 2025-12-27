package grpc

import (
	"errors"

	ductstreampb "github.com/josephjohncox/ductstream/gen/go/ductstream/v1"
	"github.com/josephjohncox/ductstream/internal/flow"
	"github.com/josephjohncox/ductstream/pkg/connector"
)

func flowToProto(f flow.Flow) *ductstreampb.Flow {
	return &ductstreampb.Flow{
		Id:           f.ID,
		Name:         f.Name,
		Source:       endpointToProto(f.Source),
		Destinations: endpointsToProto(f.Destinations),
		State:        flowStateToProto(f.State),
		WireFormat:   wireFormatToProto(f.WireFormat),
		Parallelism:  int32(f.Parallelism),
	}
}

func endpointsToProto(specs []connector.Spec) []*ductstreampb.Endpoint {
	items := make([]*ductstreampb.Endpoint, 0, len(specs))
	for _, spec := range specs {
		items = append(items, endpointToProto(spec))
	}
	return items
}

func flowFromProto(pb *ductstreampb.Flow) (flow.Flow, error) {
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
	}, nil
}

func endpointToProto(spec connector.Spec) *ductstreampb.Endpoint {
	return &ductstreampb.Endpoint{
		Name:    spec.Name,
		Type:    endpointTypeToProto(spec.Type),
		Options: spec.Options,
	}
}

func endpointFromProto(endpoint *ductstreampb.Endpoint) (connector.Spec, error) {
	if endpoint == nil {
		return connector.Spec{}, errors.New("endpoint is required")
	}
	if endpoint.Type == ductstreampb.EndpointType_ENDPOINT_TYPE_UNSPECIFIED {
		return connector.Spec{}, errors.New("endpoint type is required")
	}
	return connector.Spec{
		Name:    endpoint.Name,
		Type:    endpointTypeFromProto(endpoint.Type),
		Options: endpoint.Options,
	}, nil
}

func endpointTypeToProto(t connector.EndpointType) ductstreampb.EndpointType {
	switch t {
	case connector.EndpointPostgres:
		return ductstreampb.EndpointType_ENDPOINT_TYPE_POSTGRES
	case connector.EndpointSnowflake:
		return ductstreampb.EndpointType_ENDPOINT_TYPE_SNOWFLAKE
	case connector.EndpointS3:
		return ductstreampb.EndpointType_ENDPOINT_TYPE_S3
	case connector.EndpointKafka:
		return ductstreampb.EndpointType_ENDPOINT_TYPE_KAFKA
	case connector.EndpointHTTP:
		return ductstreampb.EndpointType_ENDPOINT_TYPE_HTTP
	case connector.EndpointGRPC:
		return ductstreampb.EndpointType_ENDPOINT_TYPE_GRPC
	case connector.EndpointProto:
		return ductstreampb.EndpointType_ENDPOINT_TYPE_PROTO
	case connector.EndpointPGStream:
		return ductstreampb.EndpointType_ENDPOINT_TYPE_PGSTREAM
	case connector.EndpointSnowpipe:
		return ductstreampb.EndpointType_ENDPOINT_TYPE_SNOWPIPE
	case connector.EndpointParquet:
		return ductstreampb.EndpointType_ENDPOINT_TYPE_PARQUET
	case connector.EndpointDuckDB:
		return ductstreampb.EndpointType_ENDPOINT_TYPE_DUCKDB
	case connector.EndpointBufStream:
		return ductstreampb.EndpointType_ENDPOINT_TYPE_BUFSTREAM
	case connector.EndpointClickHouse:
		return ductstreampb.EndpointType_ENDPOINT_TYPE_CLICKHOUSE
	default:
		return ductstreampb.EndpointType_ENDPOINT_TYPE_UNSPECIFIED
	}
}

func endpointTypeFromProto(t ductstreampb.EndpointType) connector.EndpointType {
	switch t {
	case ductstreampb.EndpointType_ENDPOINT_TYPE_POSTGRES:
		return connector.EndpointPostgres
	case ductstreampb.EndpointType_ENDPOINT_TYPE_SNOWFLAKE:
		return connector.EndpointSnowflake
	case ductstreampb.EndpointType_ENDPOINT_TYPE_S3:
		return connector.EndpointS3
	case ductstreampb.EndpointType_ENDPOINT_TYPE_KAFKA:
		return connector.EndpointKafka
	case ductstreampb.EndpointType_ENDPOINT_TYPE_HTTP:
		return connector.EndpointHTTP
	case ductstreampb.EndpointType_ENDPOINT_TYPE_GRPC:
		return connector.EndpointGRPC
	case ductstreampb.EndpointType_ENDPOINT_TYPE_PROTO:
		return connector.EndpointProto
	case ductstreampb.EndpointType_ENDPOINT_TYPE_PGSTREAM:
		return connector.EndpointPGStream
	case ductstreampb.EndpointType_ENDPOINT_TYPE_SNOWPIPE:
		return connector.EndpointSnowpipe
	case ductstreampb.EndpointType_ENDPOINT_TYPE_PARQUET:
		return connector.EndpointParquet
	case ductstreampb.EndpointType_ENDPOINT_TYPE_DUCKDB:
		return connector.EndpointDuckDB
	case ductstreampb.EndpointType_ENDPOINT_TYPE_BUFSTREAM:
		return connector.EndpointBufStream
	case ductstreampb.EndpointType_ENDPOINT_TYPE_CLICKHOUSE:
		return connector.EndpointClickHouse
	default:
		return ""
	}
}

func flowStateToProto(state flow.State) ductstreampb.FlowState {
	switch state {
	case flow.StateCreated:
		return ductstreampb.FlowState_FLOW_STATE_CREATED
	case flow.StateRunning:
		return ductstreampb.FlowState_FLOW_STATE_RUNNING
	case flow.StatePaused:
		return ductstreampb.FlowState_FLOW_STATE_PAUSED
	case flow.StateStopping:
		return ductstreampb.FlowState_FLOW_STATE_STOPPING
	case flow.StateFailed:
		return ductstreampb.FlowState_FLOW_STATE_FAILED
	default:
		return ductstreampb.FlowState_FLOW_STATE_UNSPECIFIED
	}
}

func flowStateFromProto(state ductstreampb.FlowState) flow.State {
	switch state {
	case ductstreampb.FlowState_FLOW_STATE_CREATED:
		return flow.StateCreated
	case ductstreampb.FlowState_FLOW_STATE_RUNNING:
		return flow.StateRunning
	case ductstreampb.FlowState_FLOW_STATE_PAUSED:
		return flow.StatePaused
	case ductstreampb.FlowState_FLOW_STATE_STOPPING:
		return flow.StateStopping
	case ductstreampb.FlowState_FLOW_STATE_FAILED:
		return flow.StateFailed
	default:
		return ""
	}
}

func wireFormatToProto(format connector.WireFormat) ductstreampb.WireFormat {
	switch format {
	case connector.WireFormatArrow:
		return ductstreampb.WireFormat_WIRE_FORMAT_ARROW
	case connector.WireFormatParquet:
		return ductstreampb.WireFormat_WIRE_FORMAT_PARQUET
	case connector.WireFormatProto:
		return ductstreampb.WireFormat_WIRE_FORMAT_PROTO
	case connector.WireFormatAvro:
		return ductstreampb.WireFormat_WIRE_FORMAT_AVRO
	case connector.WireFormatJSON:
		return ductstreampb.WireFormat_WIRE_FORMAT_JSON
	default:
		return ductstreampb.WireFormat_WIRE_FORMAT_UNSPECIFIED
	}
}

func wireFormatFromProto(format ductstreampb.WireFormat) connector.WireFormat {
	switch format {
	case ductstreampb.WireFormat_WIRE_FORMAT_ARROW:
		return connector.WireFormatArrow
	case ductstreampb.WireFormat_WIRE_FORMAT_PARQUET:
		return connector.WireFormatParquet
	case ductstreampb.WireFormat_WIRE_FORMAT_PROTO:
		return connector.WireFormatProto
	case ductstreampb.WireFormat_WIRE_FORMAT_AVRO:
		return connector.WireFormatAvro
	case ductstreampb.WireFormat_WIRE_FORMAT_JSON:
		return connector.WireFormatJSON
	default:
		return ""
	}
}
