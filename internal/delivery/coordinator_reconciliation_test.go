package delivery

import (
	"context"
	"testing"

	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type reconciliationResultProbe struct {
	connector.Destination
	disposition connector.DeliveryDisposition
	err         error
}

func (*reconciliationResultProbe) Apply(context.Context, connector.DeliveryIntent, connector.Batch) (connector.DeliveryEvidence, error) {
	panic("reconciliation must not apply a new side effect")
}

func (p *reconciliationResultProbe) Reconcile(context.Context, connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	return p.disposition, connector.DeliveryEvidence{ExternalID: "observed", ContentHash: "content"}, p.err
}

func TestReconciliationRejectsEveryUnknownWireValue(t *testing.T) {
	t.Parallel()
	// DeliveryDisposition has a uint8 representation. Check every representable
	// invalid value, not just a cast of one convenient integer. The zero value
	// remains the existing Indeterminate path, which records durable backoff.
	for value := 3; value <= 255; value++ {
		coordinator := &Coordinator{}
		probe := &reconciliationResultProbe{disposition: connector.DeliveryDisposition(value)}
		got, evidence, err := coordinator.reconcileUnfinished(context.Background(), authority.RunFence{}, connector.DeliveryIntent{LogicalBatchID: "reconciliation-probe"}, deliveryState{}, probe)
		if err == nil || got != connector.DeliveryIndeterminate || evidence != (connector.DeliveryEvidence{}) {
			t.Fatalf("R-ENUM: value=%d escaped fail-closed classification: got=%d evidence=%+v err=%v", value, got, evidence, err)
		}
	}
}

func TestReconciliationRecognizesOnlyDefiniteKnownResults(t *testing.T) {
	t.Parallel()
	for _, disposition := range []connector.DeliveryDisposition{connector.DeliveryNotApplied, connector.DeliveryApplied} {
		probe := &reconciliationResultProbe{disposition: disposition}
		got, evidence, err := (&Coordinator{}).reconcileUnfinished(context.Background(), authority.RunFence{}, connector.DeliveryIntent{LogicalBatchID: "reconciliation-probe"}, deliveryState{}, probe)
		if err != nil || got != disposition || evidence.ExternalID != "observed" {
			t.Fatalf("disposition=%d got=%d evidence=%+v err=%v", disposition, got, evidence, err)
		}
	}
}
