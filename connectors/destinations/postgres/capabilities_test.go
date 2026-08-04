package postgres

import (
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestCapabilitiesDeclarePerTableWritePolicies(t *testing.T) {
	capabilities := (&Destination{}).Capabilities()
	if !capabilities.TableWrites.Declared || !capabilities.TableWrites.Append || !capabilities.TableWrites.Upsert || !capabilities.TableWrites.ExplicitKey || !capabilities.TableWrites.WatermarkGuard {
		t.Fatalf("postgres table write contract incomplete: %+v", capabilities.TableWrites)
	}
	if capabilities.Delivery.ReplaySafe || capabilities.Delivery.IdempotentReplay {
		t.Fatalf("mixed per-table append policy must not be globally advertised as replay safe: %+v", capabilities.Delivery)
	}
	if err := capabilities.SupportsTablePolicy(connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}, WatermarkColumn: "updated_at"}); err != nil {
		t.Fatal(err)
	}
}
