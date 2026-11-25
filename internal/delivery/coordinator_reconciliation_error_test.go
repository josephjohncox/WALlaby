package delivery

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestReconciliationErrorNeverAuthorizesRetryOrAdoption(t *testing.T) {
	t.Parallel()
	// A deterministic failed control-store connection observes that Wait reaches
	// durable retry accounting. No server, DNS lookup or socket is used. If the
	// caller accidentally passes failed=false, definite results escape as nil.
	controlUnavailable := errors.New("proof control store unavailable")
	config, err := pgxpool.ParseConfig("postgres://proof@localhost:1/proof?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.LookupFunc = func(context.Context, string) ([]string, error) {
		return []string{"127.0.0.1"}, nil
	}
	config.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		return nil, controlUnavailable
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	coordinator := &Coordinator{pool: pool}
	for _, disposition := range []connector.DeliveryDisposition{
		connector.DeliveryApplied, connector.DeliveryNotApplied,
		connector.DeliveryIndeterminate, connector.DeliveryDisposition(255),
	} {
		probe := &reconciliationResultProbe{disposition: disposition, err: errors.New("observation failed")}
		got, evidence, err := coordinator.reconcileUnfinished(context.Background(), authority.RunFence{}, connector.DeliveryIntent{LogicalBatchID: "reconciliation-error-probe"}, deliveryState{}, probe)
		if !errors.Is(err, controlUnavailable) || got != connector.DeliveryIndeterminate || evidence != (connector.DeliveryEvidence{}) {
			t.Fatalf("R-ERROR: error-bearing observation authorized a decision: disposition=%d result=%d evidence=%+v err=%v", disposition, got, evidence, err)
		}
	}
}
