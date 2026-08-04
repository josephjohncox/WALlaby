package flow

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestShippedIcebergS3TablesExamplePassesAdmission(t *testing.T) {
	raw, err := os.ReadFile("../../examples/flows/postgres_to_iceberg_s3tables.json")
	if err != nil {
		t.Fatal(err)
	}
	var candidate Flow
	if err := json.Unmarshal(raw, &candidate); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDefinition(candidate); err != nil {
		t.Fatalf("shipped Iceberg flow example: %v", err)
	}
}

func TestValidateDefinitionMaterializationContract(t *testing.T) {
	t.Parallel()

	valid := Flow{
		Source: connector.Spec{Type: connector.EndpointPostgres, Options: map[string]string{
			"managed": "true", "bootstrap": "never", "create_slot": "false", "ensure_state": "false", "ensure_publication": "false", "sync_publication": "false",
		}},
		Destinations: []connector.Spec{{Name: "consumer", Type: connector.EndpointIceberg, Options: map[string]string{"destination_revision_id": "iceberg-v1"}}},
		Config: Config{
			AckPolicy:       stream.AckPolicyMaterialized,
			Materialization: MaterializationPolicy{ProjectionID: "canonical_cdc_parquet_v2"},
		},
	}
	valid.Config.TableMappings = NewTableMappings(valid.Destinations)
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
		{name: "frozen v1 is not mapped Iceberg", edit: func(f *Flow) { f.Config.Materialization.ProjectionID = "canonical_cdc_parquet_v1" }, want: "canonical_cdc_parquet_v2"},
		{name: "primary is irrelevant", edit: func(f *Flow) { f.Config.PrimaryDestination = "consumer" }, want: "primary_destination"},
		{name: "non postgres source", edit: func(f *Flow) { f.Source.Type = connector.EndpointKafka }, want: "PostgreSQL source"},
		{name: "unmanaged source", edit: func(f *Flow) { f.Source.Options["managed"] = "false" }, want: "managed PostgreSQL"},
		{name: "snapshot not admitted", edit: func(f *Flow) { f.Source.Options["bootstrap"] = "auto" }, want: "bootstrap=never"},
		{name: "missing destination", edit: func(f *Flow) { f.Destinations = nil }, want: "unknown destination"},
		{name: "multiple destinations", edit: func(f *Flow) { f.Destinations = append(f.Destinations, f.Destinations[0]) }, want: "duplicate destination name"},
		{name: "non Iceberg destination", edit: func(f *Flow) { f.Destinations[0].Type = connector.EndpointPostgres }, want: "Iceberg destination"},
		{name: "missing revision", edit: func(f *Flow) { f.Destinations[0].Options = map[string]string{} }, want: "destination_revision_id"},
		{name: "persisted secret", edit: func(f *Flow) { f.Destinations[0].Options["aws_session_token"] = "secret" }, want: "unsupported persisted Iceberg option"},
		{name: "fixed table collapse", edit: func(f *Flow) { f.Destinations[0].Options["table"] = "shared" }, want: "fixed-table collapse"},
		{name: "logical namespace override", edit: func(f *Flow) { f.Destinations[0].Options["namespace"] = "lake" }, want: "unsupported persisted Iceberg option"},
		{name: "logical table prefix override", edit: func(f *Flow) { f.Destinations[0].Options["table_prefix"] = "cdc_" }, want: "unsupported persisted Iceberg option"},
		{name: "upsert mapping", edit: func(f *Flow) {
			mapping := &f.Config.TableMappings.Destinations[0]
			mapping.FutureTables = FutureTableMapping{Action: MappingActionExclude}
			mapping.Tables = []TableMapping{{SourceSchema: "public", SourceTable: "events", Action: MappingActionInclude, TargetSchema: "lake", TargetTable: "events", FutureColumns: FutureColumnMapping{Action: MappingActionInclude, TargetColumn: "{column}"}, Write: TableWritePolicy{Mode: TableWriteModeUpsert, KeyColumns: []string{"id"}}}}
		}, want: "upsert"},
		{name: "watermark mapping", edit: func(f *Flow) {
			f.Config.TableMappings.Destinations[0].FutureTables.Write.WatermarkColumn = "updated_at"
		}, want: "watermark"},
		{name: "unknown acknowledgement policy", edit: func(f *Flow) { f.Config.AckPolicy = stream.AckPolicy("sometimes") }, want: "unsupported acknowledgement policy"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			candidate.Source.Options = make(map[string]string, len(valid.Source.Options))
			for key, value := range valid.Source.Options {
				candidate.Source.Options[key] = value
			}
			candidate.Destinations = append([]connector.Spec(nil), valid.Destinations...)
			candidate.Config.TableMappings = valid.Config.TableMappings.Clone()
			candidate.Destinations[0].Options = map[string]string{"destination_revision_id": "iceberg-v1"}
			test.edit(&candidate)
			if err := ValidateDefinition(candidate); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateDefinition() error=%v, want %q", err, test.want)
			}
		})
	}
}
