package runner

import (
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestIggyInvalidSpecRejectedBeforeAnySourceCanOpen(t *testing.T) {
	for _, test := range []map[string]string{
		{},
		{"address": "localhost:8090", "stream": "cdc", "topic": "events"},
		{"address": "localhost:8090", "stream": "cdc", "topic": "events", "token": "example", "format": "parquet"},
		{"address": "localhost:8090", "stream": "cdc", "topic": "events", "token": "example", "max_message_bytes": "0"},
	} {
		if _, err := (Factory{}).Destinations([]connector.RuntimeSpec{{Type: connector.EndpointIggy, Options: test}}); err == nil {
			t.Fatalf("invalid Iggy configuration admitted: %v", test)
		}
	}
}
