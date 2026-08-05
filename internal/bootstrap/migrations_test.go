package bootstrap

import (
	"strings"
	"testing"
)

func TestManagedSchemaBaselineMigrationHasNoBackfill(t *testing.T) {
	contents, err := migrationFS.ReadFile("migrations/008_managed_schema_baselines.sql")
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(string(contents))
	if strings.Contains(lower, "schema_versions") || strings.Contains(lower, "insert into public.managed_schema_baselines") {
		t.Fatal("managed schema-baseline migration imports pre-authority schema rows")
	}
}

func TestSnapshotDestinationContractMigrationFailsClosedWithoutInference(t *testing.T) {
	raw, err := migrationFS.ReadFile("migrations/007_snapshot_destination_contract.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)
	for _, required := range []string{"destination_schema_json", "write_policy_json", "projection_fingerprint", "projection_version", "legacy snapshot tasks lack an immutable destination delivery contract", "ALTER COLUMN destination_schema_json SET NOT NULL"} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration missing %q", required)
		}
	}
	for _, forbidden := range []string{"UPDATE ", "COALESCE(", " DEFAULT "} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("migration contains destination-contract inference token %q", forbidden)
		}
	}
}

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
