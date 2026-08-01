package flow

import (
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
)

func TestLifecycleTransitions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		from State
		to   State
		want bool
	}{
		{StateCreated, StateRunning, true},
		{StateRunning, StatePaused, true},
		{StatePaused, StateRunning, true},
		{StateRunning, StateStopping, true},
		{StatePaused, StateStopping, true},
		{StateStopping, StateStopped, true},
		{StateRunning, StateFailed, true},
		{StateStopping, StateFailed, true},
		{StateStopped, StateRunning, false},
		{StateFailed, StateRunning, false},
		{StateCreated, StatePaused, false},
	}
	for _, tt := range tests {
		if got := CanTransition(tt.from, tt.to); got != tt.want {
			t.Errorf("CanTransition(%s, %s) = %v, want %v", tt.from, tt.to, got, tt.want)
		}
	}
}

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
		SchemaRegistrySubject:           "flow.subject",
		SchemaRegistryProtoTypesSubject: "flow.proto",
		SchemaRegistrySubjectMode:       "topic_table",
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
