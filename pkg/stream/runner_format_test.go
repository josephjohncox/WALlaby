package stream

import (
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestRunnerNormalizeWireFormatMismatch(t *testing.T) {
	r := &Runner{
		WireFormat:   connector.WireFormatArrow,
		StrictFormat: true,
		SourceSpec: connector.Spec{
			Name:    "src",
			Type:    "postgres",
			Options: map[string]string{"format": "json"},
		},
		Destinations: []DestinationConfig{
			{Spec: connector.Spec{Name: "dest", Type: "kafka", Options: map[string]string{}}},
		},
	}

	if err := r.normalizeWireFormat(); err == nil {
		t.Fatalf("expected mismatch error")
	}
}

func TestRunnerNormalizeWireFormatPropagates(t *testing.T) {
	r := &Runner{
		WireFormat: connector.WireFormatAvro,
		SourceSpec: connector.Spec{
			Name:    "src",
			Type:    "postgres",
			Options: map[string]string{},
		},
		Destinations: []DestinationConfig{
			{Spec: connector.Spec{Name: "dest", Type: "kafka", Options: map[string]string{}}},
		},
	}

	if err := r.normalizeWireFormat(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if got := r.SourceSpec.Options["format"]; got != string(connector.WireFormatAvro) {
		t.Fatalf("expected source format %s, got %s", connector.WireFormatAvro, got)
	}
	if got := r.Destinations[0].Spec.Options["format"]; got != string(connector.WireFormatAvro) {
		t.Fatalf("expected destination format %s, got %s", connector.WireFormatAvro, got)
	}
}

func TestRunnerNormalizeWireFormatDestMismatch(t *testing.T) {
	r := &Runner{
		WireFormat:   connector.WireFormatProto,
		StrictFormat: true,
		SourceSpec: connector.Spec{
			Name:    "src",
			Type:    "postgres",
			Options: map[string]string{},
		},
		Destinations: []DestinationConfig{
			{Spec: connector.Spec{Name: "dest", Type: "kafka", Options: map[string]string{"format": "avro"}}},
		},
	}

	if err := r.normalizeWireFormat(); err == nil {
		t.Fatalf("expected destination mismatch error")
	}
}
