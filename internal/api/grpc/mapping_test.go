package grpc

import (
	"reflect"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestMaterializedAckPolicyAndProjectionRoundTrip(t *testing.T) {
	t.Parallel()

	model := flow.Config{
		AckPolicy:       stream.AckPolicyMaterialized,
		Materialization: flow.MaterializationPolicy{ProjectionID: "canonical_cdc_parquet_v1"},
	}
	wire := flowConfigToProto(model)
	if wire.GetAckPolicy() != wallabypb.AckPolicy_ACK_POLICY_MATERIALIZED {
		t.Fatalf("ack policy=%s, want materialized", wire.GetAckPolicy())
	}
	if wire.GetMaterialization().GetProjectionId() != "canonical_cdc_parquet_v1" {
		t.Fatalf("projection=%q", wire.GetMaterialization().GetProjectionId())
	}
	roundTrip, err := flowConfigFromProto(wire)
	if err != nil {
		t.Fatal(err)
	}
	if !roundTrip.Equal(model) {
		t.Fatalf("flow config round trip=%+v, want %+v", roundTrip, model)
	}
	if got := int32(wallabypb.AckPolicy_ACK_POLICY_MATERIALIZED); got != 3 {
		t.Fatalf("ACK_POLICY_MATERIALIZED=%d, want wire value 3", got)
	}
}

func TestTableMappingsRoundTrip(t *testing.T) {
	t.Parallel()
	model := flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: []flow.DestinationTableMappings{{
		Destination:  "warehouse",
		FutureTables: flow.FutureTableMapping{Action: flow.MappingActionInclude, TargetSchema: "{schema}", TargetTable: "raw_{table}", FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{column}"}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeAppend}},
		Tables:       []flow.TableMapping{{SourceSchema: "public", SourceTable: "customers", Action: flow.MappingActionInclude, TargetSchema: "analytics", TargetTable: "accounts", FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionExclude}, Columns: []flow.ColumnMapping{{SourceColumn: "id", Action: flow.MappingActionInclude, TargetColumn: "account_id"}}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeUpsert, KeyColumns: []string{"id"}, WatermarkColumn: "updated_at"}}},
	}}}
	wire := tableMappingsToProto(model)
	if wire.GetVersion() != 1 || len(wire.GetDestinations()) != 1 || wire.GetDestinations()[0].GetTables()[0].GetColumns()[0].GetTargetColumn() != "account_id" ||
		!reflect.DeepEqual(wire.GetDestinations()[0].GetTables()[0].GetWrite().GetKeyColumns(), []string{"id"}) {
		t.Fatalf("unexpected wire table mappings: %+v", wire)
	}
	roundTrip, err := tableMappingsFromProto(wire)
	if err != nil {
		t.Fatal(err)
	}
	if !roundTrip.Equal(model) {
		t.Fatalf("table mappings round trip=%+v, want %+v", roundTrip, model)
	}
}

func TestTableMappingsFromProtoRejectsNilListEntries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		mappings *wallabypb.TableMappings
	}{
		{name: "destination", mappings: &wallabypb.TableMappings{Version: 1, Destinations: []*wallabypb.DestinationTableMappings{nil}}},
		{name: "table", mappings: &wallabypb.TableMappings{Version: 1, Destinations: []*wallabypb.DestinationTableMappings{{Destination: "warehouse", Tables: []*wallabypb.TableMapping{nil}}}}},
		{name: "column", mappings: &wallabypb.TableMappings{Version: 1, Destinations: []*wallabypb.DestinationTableMappings{{Destination: "warehouse", Tables: []*wallabypb.TableMapping{{Columns: []*wallabypb.ColumnMapping{nil}}}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := tableMappingsFromProto(test.mappings); err == nil {
				t.Fatal("nil protobuf list entry was silently skipped")
			}
		})
	}
}

func TestFlowFromProtoPropagatesNilTableMappingEntry(t *testing.T) {
	t.Parallel()
	_, err := flowFromProto(&wallabypb.Flow{
		Source: &wallabypb.Endpoint{Type: wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES},
		Config: &wallabypb.FlowConfig{TableMappings: &wallabypb.TableMappings{
			Version: 1, Destinations: []*wallabypb.DestinationTableMappings{nil},
		}},
	})
	if err == nil {
		t.Fatal("flow conversion silently accepted nil table mapping entry")
	}
}

func TestIcebergEndpointWireValueAndRoundTrip(t *testing.T) {
	t.Parallel()
	wire := endpointTypeToProto(connector.EndpointIceberg)
	if wire != wallabypb.EndpointType_ENDPOINT_TYPE_ICEBERG || int32(wire) != 15 {
		t.Fatalf("Iceberg endpoint wire value=%d", wire)
	}
	if model := endpointTypeFromProto(wire); model != connector.EndpointIceberg {
		t.Fatalf("Iceberg endpoint round trip=%q", model)
	}
}

func TestFlowFromProtoRejectsUnknownAcknowledgementPolicy(t *testing.T) {
	t.Parallel()

	_, err := flowFromProto(&wallabypb.Flow{
		Source: &wallabypb.Endpoint{Type: wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES},
		Config: &wallabypb.FlowConfig{AckPolicy: wallabypb.AckPolicy(99)},
	})
	if err == nil {
		t.Fatal("unknown acknowledgement policy was silently mapped to unspecified")
	}
}

func TestFlowStateRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		model flow.State
		wire  wallabypb.FlowState
	}{
		{flow.StateCreated, wallabypb.FlowState_FLOW_STATE_CREATED},
		{flow.StateRunning, wallabypb.FlowState_FLOW_STATE_RUNNING},
		{flow.StatePaused, wallabypb.FlowState_FLOW_STATE_PAUSED},
		{flow.StateStopping, wallabypb.FlowState_FLOW_STATE_STOPPING},
		{flow.StateFailed, wallabypb.FlowState_FLOW_STATE_FAILED},
		{flow.StateStopped, wallabypb.FlowState_FLOW_STATE_STOPPED},
	}
	for _, tt := range tests {
		if got := flowStateToProto(tt.model); got != tt.wire {
			t.Errorf("flowStateToProto(%s) = %s, want %s", tt.model, got, tt.wire)
		}
		if got := flowStateFromProto(tt.wire); got != tt.model {
			t.Errorf("flowStateFromProto(%s) = %s, want %s", tt.wire, got, tt.model)
		}
	}
	if got := int32(wallabypb.FlowState_FLOW_STATE_STOPPED); got != 6 {
		t.Fatalf("FLOW_STATE_STOPPED = %d, want wire value 6", got)
	}
}
