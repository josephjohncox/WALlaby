package artifactlog

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"reflect"
	"strings"
	"testing"
)

func TestArtifactMigrationOrderAndChecksums(t *testing.T) {
	files, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"migrations/001_artifacts.sql", "migrations/002_consumers.sql", "migrations/003_authority_protocol_v2.sql", "migrations/004_materialized_publication.sql", "migrations/005_iceberg_consumer_receipts.sql", "migrations/006_mapped_projection_v2.sql", "migrations/007_current_catalog_attempt_identity.sql", "migrations/008_schema_baseline_publication.sql", "migrations/009_metadata_retention.sql"}
	if !reflect.DeepEqual(files, want) {
		t.Fatalf("artifact migrations=%v, want %v", files, want)
	}
	checksums := map[string]string{"migrations/001_artifacts.sql": "49bf2a1a7a615f756d6f29627164b400f424107b1c1b7e146bf3899df523419c", "migrations/002_consumers.sql": "e0d15946824cbc4f6038d55af2850a15659dbcef1c6aee3c2c022a1891ff4521", "migrations/003_authority_protocol_v2.sql": "05a86baecca1948cee91a2b388ebfad0682533d2ea80eae9f4c74bb1cf6a8f3f", "migrations/004_materialized_publication.sql": "0b5f3b5738c5c70201cc9a0e6c911f943948804ebecb6d0ca8f22173c8b50b06", "migrations/005_iceberg_consumer_receipts.sql": "1ef850e03210364c6478ff4521666690f4d8a34ab24ee3f1606f70b9bdc3342b", "migrations/006_mapped_projection_v2.sql": "2f2faf74dc81d86bc81aae9e5812ad8725843b66bef65a13bf0870090e4c90b7", "migrations/007_current_catalog_attempt_identity.sql": "85883a2201b1359fb69bf5358fe5bd77c6ccab6d26a27f682f2c8655915f87da", "migrations/008_schema_baseline_publication.sql": "a09256f9f3bb9b4fffa9b55d226421c034e76bee7f63e6231b121e6bb42ef09a", "migrations/009_metadata_retention.sql": "00179d42991a6777e7d0bcd01491211213691f7860c8f73ed1fb5fac80a62760"}
	for _, file := range files {
		raw, err := fs.ReadFile(migrationFS, file)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		if got := hex.EncodeToString(digest[:]); got != checksums[file] {
			t.Fatalf("artifact migration %s checksum=%s, want %s", file, got, checksums[file])
		}
	}
}

func TestSchemaBaselinePublicationMigrationRefusesInference(t *testing.T) {
	raw, err := migrationFS.ReadFile("migrations/008_schema_baseline_publication.sql")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	for _, required := range []string{"IF EXISTS (SELECT 1 FROM public.artifact_publications)", "existing baseline payloads are not inferred", "schema_baseline_payload JSONB NOT NULL", "schema_baseline_fingerprint TEXT NOT NULL"} {
		if !strings.Contains(source, required) {
			t.Fatalf("artifact baseline-binding migration missing %q", required)
		}
	}
}

func TestCurrentCatalogAttemptMigrationFailsClosedWithoutInference(t *testing.T) {
	raw, err := migrationFS.ReadFile("migrations/007_current_catalog_attempt_identity.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)
	for _, required := range []string{"refuses noncanonical attempt identities", "refuses conflicting receipt or checkpoint identities", "refuses ambiguous attempt identities", "pg_catalog.sha256", "wallaby.iceberg.commit.v1", "ALTER COLUMN commit_id DROP DEFAULT", "artifact_delivery_attempts_publication_unique", "artifact_delivery_attempts_commit_unique", "artifact_delivery_receipts_attempt_unique"} {
		if !strings.Contains(sql, required) {
			t.Fatalf("current catalog-attempt migration missing %q", required)
		}
	}
	for _, forbidden := range []string{"UPDATE ", "COALESCE(", "pgcrypto", "digest(", "SET commit_id="} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("current catalog-attempt migration contains inference token %q", forbidden)
		}
	}
}

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
