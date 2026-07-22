package flow

import (
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestValidateDefinitionMaterializationContract(t *testing.T) {
	t.Parallel()

	valid := Flow{
		Source:       connector.Spec{Type: connector.EndpointPostgres, Options: map[string]string{"managed": "true"}},
		Destinations: []connector.Spec{{Name: "consumer", Type: connector.EndpointPostgres}},
		Config: Config{
			AckPolicy:       stream.AckPolicyMaterialized,
			Materialization: MaterializationPolicy{ProjectionID: "canonical_cdc_parquet_v1"},
		},
	}
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
		{name: "wrong projection", edit: func(f *Flow) { f.Config.Materialization.ProjectionID = "parquet" }, want: "canonical_cdc_parquet_v1"},
		{name: "primary is irrelevant", edit: func(f *Flow) { f.Config.PrimaryDestination = "consumer" }, want: "primary_destination"},
		{name: "non postgres source", edit: func(f *Flow) { f.Source.Type = connector.EndpointKafka }, want: "PostgreSQL source"},
		{name: "unmanaged source", edit: func(f *Flow) { f.Source.Options["managed"] = "false" }, want: "managed PostgreSQL"},
		{name: "unknown acknowledgement policy", edit: func(f *Flow) { f.Config.AckPolicy = stream.AckPolicy("sometimes") }, want: "unsupported acknowledgement policy"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			candidate.Source.Options = map[string]string{"managed": valid.Source.Options["managed"]}
			test.edit(&candidate)
			if err := ValidateDefinition(candidate); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateDefinition() error=%v, want %q", err, test.want)
			}
		})
	}
}
