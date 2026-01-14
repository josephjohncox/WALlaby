package flow

import (
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
)

func TestApplyRegistryDefaults(t *testing.T) {
	specs := []connector.Spec{
		{
			Name:    "kafka",
			Type:    connector.EndpointKafka,
			Options: map[string]string{},
		},
		{
			Name:    "http",
			Type:    connector.EndpointHTTP,
			Options: map[string]string{schemaregistry.OptRegistrySubject: "override.subject"},
		},
	}
	cfg := Config{
		SchemaRegistrySubject:          "flow.subject",
		SchemaRegistryProtoTypesSubject: "flow.proto",
		SchemaRegistrySubjectMode:      "topic_table",
	}

	applied := ApplyRegistryDefaults(specs, cfg)
	if applied[0].Options[schemaregistry.OptRegistrySubject] != "flow.subject" {
		t.Fatalf("expected flow subject applied, got %q", applied[0].Options[schemaregistry.OptRegistrySubject])
	}
	if applied[0].Options[schemaregistry.OptRegistryProtoTypes] != "flow.proto" {
		t.Fatalf("expected flow proto subject applied, got %q", applied[0].Options[schemaregistry.OptRegistryProtoTypes])
	}
	if applied[0].Options[schemaregistry.OptRegistrySubjectMode] != "topic_table" {
		t.Fatalf("expected flow subject mode applied, got %q", applied[0].Options[schemaregistry.OptRegistrySubjectMode])
	}
	if applied[1].Options[schemaregistry.OptRegistrySubject] != "override.subject" {
		t.Fatalf("expected endpoint subject override preserved, got %q", applied[1].Options[schemaregistry.OptRegistrySubject])
	}
}
