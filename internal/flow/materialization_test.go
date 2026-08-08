package flow

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestShippedIcebergS3TablesExamplePassesAdmission(t *testing.T) {
	raw, err := os.ReadFile("../../examples/flows/postgres_to_iceberg_s3tables.json")
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Source       json.RawMessage   `json:"source"`
		Destinations []json.RawMessage `json:"destinations"`
		Config       Config            `json:"config"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	var source wallabypb.Endpoint
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(document.Source, &source); err != nil {
		t.Fatal(err)
	}
	candidate := Flow{Source: &source, Config: document.Config}
	for _, encoded := range document.Destinations {
		var endpoint wallabypb.Endpoint
		if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, &endpoint); err != nil {
			t.Fatal(err)
		}
		candidate.Destinations = append(candidate.Destinations, &endpoint)
	}
	if err := ValidateDefinition(candidate); err != nil {
		t.Fatalf("shipped Iceberg flow example: %v", err)
	}
}

func TestValidateDefinitionMaterializationContract(t *testing.T) {
	t.Parallel()

	sourceConfig := &wallabypb.PostgresSourceConfig{
		Mode:    wallabypb.PostgresSourceMode_POSTGRES_SOURCE_MODE_CDC,
		Managed: boolPointer(true), Bootstrap: wallabypb.BootstrapMode_BOOTSTRAP_MODE_NEVER,
		CreateSlot: boolPointer(false), EnsureState: boolPointer(false), EnsurePublication: boolPointer(false), SyncPublication: boolPointer(false),
	}
	destinationConfig := &wallabypb.IcebergDestinationConfig{DestinationRevisionId: "iceberg-v1"}
	valid := Flow{
		Source:       &wallabypb.Endpoint{Name: "source", Config: &wallabypb.Endpoint_PostgresSource{PostgresSource: sourceConfig}},
		Destinations: []*wallabypb.Endpoint{{Name: "consumer", Config: &wallabypb.Endpoint_Iceberg{Iceberg: destinationConfig}}},
		Config: Config{
			AckPolicy:       stream.AckPolicyMaterialized,
			Materialization: MaterializationPolicy{ProjectionID: "canonical_cdc_parquet_v2"},
		},
	}
	valid.Config.TableMappings = NewTableMappings([]connector.RuntimeSpec{{Name: "consumer", Type: connector.EndpointIceberg}})
	if err := ValidateDefinition(valid); err != nil {
		t.Fatalf("valid materialized definition: %v", err)
	}

	tests := []struct {
		name string
		edit func(*Flow)
		want string
	}{
		{name: "policy without materialization", edit: func(f *Flow) { f.Config.Materialization = MaterializationPolicy{} }, want: "materialization"},
		{name: "materialization silently ignored", edit: func(f *Flow) { f.Config.AckPolicy = stream.AckPolicyAll }, want: "ack_policy=materialized"},
		{name: "wrong projection", edit: func(f *Flow) { f.Config.Materialization.ProjectionID = "parquet" }, want: "canonical_cdc_parquet_v2"},
		{name: "primary is irrelevant", edit: func(f *Flow) { f.Config.PrimaryDestination = "consumer" }, want: "primary_destination"},
		{name: "non postgres source", edit: func(f *Flow) {
			f.Source.Config = &wallabypb.Endpoint_Custom{Custom: &wallabypb.CustomEndpointConfig{ConnectorType: "unregistered"}}
		}, want: "decode source"},
		{name: "unmanaged source", edit: func(f *Flow) { f.Source.GetPostgresSource().Managed = boolPointer(false) }, want: "managed PostgreSQL"},
		{name: "snapshot not admitted", edit: func(f *Flow) { f.Source.GetPostgresSource().Bootstrap = wallabypb.BootstrapMode_BOOTSTRAP_MODE_AUTO }, want: "bootstrap=never"},
		{name: "missing destination", edit: func(f *Flow) { f.Destinations = nil }, want: "unknown destination"},
		{name: "multiple destinations", edit: func(f *Flow) { f.Destinations = append(f.Destinations, cloneEndpoint(f.Destinations[0])) }, want: "duplicate destination name"},
		{name: "non Iceberg destination", edit: func(f *Flow) {
			f.Destinations[0].Config = &wallabypb.Endpoint_PostgresDestination{PostgresDestination: &wallabypb.PostgresDestinationConfig{}}
		}, want: "Iceberg destination"},
		{name: "missing revision", edit: func(f *Flow) { f.Destinations[0].GetIceberg().DestinationRevisionId = "" }, want: "destination_revision_id"},
		{name: "upsert mapping", edit: func(f *Flow) {
			mapping := &f.Config.TableMappings.Destinations[0]
			mapping.FutureTables = FutureTableMapping{Action: MappingActionExclude}
			mapping.Tables = []TableMapping{{SourceSchema: "public", SourceTable: "events", Action: MappingActionInclude, TargetSchema: "lake", TargetTable: "events", FutureColumns: FutureColumnMapping{Action: MappingActionInclude, TargetColumn: "{{ .Column }}"}, Write: TableWritePolicy{Mode: TableWriteModeUpsert, KeyColumns: []string{"id"}}}}
		}, want: "upsert"},
		{name: "watermark mapping", edit: func(f *Flow) {
			f.Config.TableMappings.Destinations[0].FutureTables.Write.WatermarkColumn = "updated_at"
		}, want: "watermark"},
		{name: "unknown acknowledgement policy", edit: func(f *Flow) { f.Config.AckPolicy = stream.AckPolicy("sometimes") }, want: "unsupported acknowledgement policy"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := Clone(valid)
			test.edit(&candidate)
			if err := ValidateDefinition(candidate); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateDefinition() error=%v, want %q", err, test.want)
			}
		})
	}
}

func boolPointer(value bool) *bool { return &value }
