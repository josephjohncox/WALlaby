package grpc

import (
	"reflect"
	"strings"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
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
		FutureTables: flow.FutureTableMapping{Action: flow.MappingActionInclude, TargetSchema: "{{ .Schema }}", TargetTable: "raw_{{ .Table }}", FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{{ .Column }}"}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeAppend}},
		Tables:       []flow.TableMapping{{SourceSchema: "public", SourceTable: "customers", Action: flow.MappingActionInclude, TargetSchema: "analytics", TargetTable: "accounts", FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionExclude}, Columns: []flow.ColumnMapping{{SourceColumn: "id", Action: flow.MappingActionInclude, TargetColumn: "account_id"}}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeUpsert, KeyColumns: []string{"id"}, WatermarkColumn: "updated_at"}}},
	}}}
	wire := tableMappingsToProto(model)
	if wire.GetVersion() != flow.TableMappingsVersion || len(wire.GetDestinations()) != 1 || wire.GetDestinations()[0].GetTables()[0].GetColumns()[0].GetTargetColumn() != "account_id" ||
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
		{name: "destination", mappings: &wallabypb.TableMappings{Version: flow.TableMappingsVersion, Destinations: []*wallabypb.DestinationTableMappings{nil}}},
		{name: "table", mappings: &wallabypb.TableMappings{Version: flow.TableMappingsVersion, Destinations: []*wallabypb.DestinationTableMappings{{Destination: "warehouse", Tables: []*wallabypb.TableMapping{nil}}}}},
		{name: "column", mappings: &wallabypb.TableMappings{Version: flow.TableMappingsVersion, Destinations: []*wallabypb.DestinationTableMappings{{Destination: "warehouse", Tables: []*wallabypb.TableMapping{{Columns: []*wallabypb.ColumnMapping{nil}}}}}}},
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
	_, err := flowFromProtoWithRegistry(&wallabypb.Flow{
		Source: &wallabypb.Endpoint{Config: &wallabypb.Endpoint_PostgresSource{PostgresSource: &wallabypb.PostgresSourceConfig{}}},
		Config: &wallabypb.FlowConfig{TableMappings: &wallabypb.TableMappings{
			Version: flow.TableMappingsVersion, Destinations: []*wallabypb.DestinationTableMappings{nil},
		}},
	}, connector.DefaultRegistry)
	if err == nil {
		t.Fatal("flow conversion silently accepted nil table mapping entry")
	}
}

func TestEndpointBranchesAndRoundTrips(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		model connector.EndpointType
	}{
		{name: "redpanda", model: connector.EndpointRedpanda},
		{name: "iceberg", model: connector.EndpointIceberg},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := map[string]string{}
			if test.model == connector.EndpointIceberg {
				options["destination_revision_id"] = "revision-1"
			}
			wire, err := endpointcodec.Encode(connector.RuntimeSpec{Name: test.name, Type: test.model, Options: options}, endpointcodec.RoleDestination)
			if err != nil {
				t.Fatal(err)
			}
			model, err := endpointcodec.Decode(wire, endpointcodec.RoleDestination)
			if err != nil || model.Type != test.model {
				t.Fatalf("endpoint round trip=%q, want %q (err=%v)", model.Type, test.model, err)
			}
		})
	}
}

func TestFlowFromProtoRejectsEveryUnknownEnumBeforeMappingToAbsence(t *testing.T) {
	t.Parallel()
	cases := []*wallabypb.Flow{
		{State: wallabypb.FlowState(99)},
		{WireFormat: wallabypb.WireFormat(99)},
		{Config: &wallabypb.FlowConfig{FailureMode: wallabypb.FailureMode(99)}},
		{Config: &wallabypb.FlowConfig{GiveUpPolicy: wallabypb.GiveUpPolicy(99)}},
	}
	for _, candidate := range cases {
		if _, err := flowFromProtoWithRegistry(candidate, connector.DefaultRegistry); err == nil || !strings.Contains(err.Error(), "unknown enum value") {
			t.Fatalf("unknown enum flow=%v error=%v", candidate, err)
		}
	}
}

func TestFlowFromProtoRejectsUnknownAcknowledgementPolicy(t *testing.T) {
	t.Parallel()

	_, err := flowFromProtoWithRegistry(&wallabypb.Flow{
		Source: &wallabypb.Endpoint{Config: &wallabypb.Endpoint_PostgresSource{PostgresSource: &wallabypb.PostgresSourceConfig{}}},
		Config: &wallabypb.FlowConfig{AckPolicy: wallabypb.AckPolicy(99)},
	}, connector.DefaultRegistry)
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
