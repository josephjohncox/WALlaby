package bootstrap

import (
	"strings"
	"testing"
)

func TestSnapshotLogicalBatchMigrationFailsClosedWithoutInference(t *testing.T) {
	raw, err := migrationFS.ReadFile("migrations/006_snapshot_logical_batch_identity.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)
	for _, required := range []string{"snapshot_delivery_attempts ADD COLUMN logical_batch_id", "snapshot_delivery_evidence ADD COLUMN logical_batch_id", "snapshot_delivery_receipts ADD COLUMN logical_batch_id", "legacy snapshot delivery rows lack logical batch identity", "ALTER COLUMN logical_batch_id SET NOT NULL"} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration missing %q", required)
		}
	}
	for _, forbidden := range []string{"UPDATE ", "COALESCE(", " DEFAULT "} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("migration contains identity inference token %q", forbidden)
		}
	}
}
