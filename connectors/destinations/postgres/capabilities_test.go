package postgres

import (
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestCapabilitiesForWriteMode(t *testing.T) {
	t.Parallel()

	destination := &Destination{}
	tests := []struct {
		name       string
		options    map[string]string
		replaySafe bool
	}{
		{name: "default target mode", replaySafe: true},
		{name: "explicit target mode", options: map[string]string{optWriteMode: writeModeTarget}, replaySafe: true},
		{name: "append mode", options: map[string]string{optWriteMode: writeModeAppend}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			capabilities := destination.CapabilitiesFor(connector.Spec{Options: tt.options})
			if capabilities.Delivery.ReplaySafe != tt.replaySafe ||
				capabilities.Delivery.IdempotentReplay != tt.replaySafe {
				t.Fatalf("delivery = %+v, replay safe = %v", capabilities.Delivery, tt.replaySafe)
			}
			if !capabilities.Delivery.TransactionalBatch || !capabilities.Delivery.ExecutesDDL {
				t.Fatalf("postgres delivery contract incomplete: %+v", capabilities.Delivery)
			}
		})
	}
}
