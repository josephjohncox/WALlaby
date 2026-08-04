package artifactlog

import (
	"strings"
	"testing"
)

func TestMappedProjectionMigrationFailsClosedWithoutLegacyInference(t *testing.T) {
	raw, err := migrationFS.ReadFile("migrations/006_mapped_projection_v2.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)
	for _, required := range []string{"artifact_streams ADD COLUMN mapping_fingerprint", "artifact_publications ADD COLUMN projection_id", "artifact_objects ADD COLUMN mapping_fingerprint", "legacy artifact rows lack explicit projection v2 identity", "ALTER COLUMN mapping_fingerprint SET NOT NULL", "canonical_cdc_parquet_v2"} {
		if !strings.Contains(sql, required) {
			t.Fatalf("mapped projection migration missing %q", required)
		}
	}
	for _, forbidden := range []string{"UPDATE ", "COALESCE(", " DEFAULT "} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("mapped projection migration contains legacy inference/backfill token %q", forbidden)
		}
	}
}
