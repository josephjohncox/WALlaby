package duckdb

import (
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestCapabilitiesDoNotClaimUnprovenUpsert(t *testing.T) {
	capabilities := (&Destination{}).Capabilities()
	if err := capabilities.SupportsTablePolicy(connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend}); err != nil {
		t.Fatal(err)
	}
	if err := capabilities.SupportsTablePolicy(connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}}); err == nil {
		t.Fatal("DuckDB upsert capability was advertised without insert-on-conflict evidence")
	}
}
