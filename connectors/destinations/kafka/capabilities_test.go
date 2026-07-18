package kafka

import (
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestCapabilitiesForDeliveryOptions(t *testing.T) {
	t.Parallel()

	destination := &Destination{}
	capabilities := destination.CapabilitiesFor(connector.Spec{Options: map[string]string{
		optTxnID:    "wallaby-flow",
		optOversize: "drop",
	}})
	if !capabilities.Delivery.TransactionalBatch {
		t.Fatal("transactional_id did not declare transactional batch writes")
	}
	if !capabilities.Delivery.Lossy {
		t.Fatal("oversize_policy=drop did not declare lossy delivery")
	}
	if capabilities.Delivery.IdempotentReplay || capabilities.Delivery.ReplaySafe {
		t.Fatalf("Kafka cross-process replay safety overstated: %+v", capabilities.Delivery)
	}
}
