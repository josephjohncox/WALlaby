package delivery

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"reflect"
	"strings"
	"testing"
)

func TestDeliveryMigrationOrderAndChecksums(t *testing.T) {
	files, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"migrations/001_attempts_receipts.sql", "migrations/002_authority_protocol.sql", "migrations/003_authority_protocol_v2.sql", "migrations/004_logical_batches_retry_retention.sql", "migrations/005_reconciliation_backoff.sql", "migrations/006_rolling_logical_batch_compatibility.sql", "migrations/007_source_ack_retention_roots.sql", "migrations/008_current_logical_batch_identity.sql", "migrations/009_manifest_checkpoint_payload.sql", "migrations/010_schema_baseline_manifest.sql"}
	if !reflect.DeepEqual(files, want) {
		t.Fatalf("delivery migrations=%v, want %v", files, want)
	}
	checksums := map[string]string{"migrations/001_attempts_receipts.sql": "92100a57226d9d6b924c3a9f800411f2d8b433417e8ab16433b81c5eb171301b", "migrations/002_authority_protocol.sql": "dce4845f2e42c1e56deeb2de88b0b2f0784141524ff8a25d1fbce9a17de78802", "migrations/003_authority_protocol_v2.sql": "31b0cb102aaf1639f470d7b754bd6bc787a64079f0c43651053812831dc29125", "migrations/004_logical_batches_retry_retention.sql": "afb19b347f15ebbf8841824556059524eccdb6a5b642a3f0442e71dd911fe8d2", "migrations/005_reconciliation_backoff.sql": "9d557c5940fa4b1318490c4b9cf6e71542ec2714bf21ba4f2742bc83015901ff", "migrations/006_rolling_logical_batch_compatibility.sql": "683838f7594de5b845ec8a90643f502ccfe25de53a5dd1bb8489f7a58d4e3a56", "migrations/007_source_ack_retention_roots.sql": "125947995fe65c24e7b21a1a9c91a76c1eef49355dbab9761b9abb6f5e1c6c29", "migrations/008_current_logical_batch_identity.sql": "8ee760a7e82a8e1f8be01aa3603fddef9995d07d8c85cdf5c08b68a80f9a6ecf", "migrations/009_manifest_checkpoint_payload.sql": "480574c393ca3ca7bb1ac08e5b4807b109b866e5fd89ddc6d3a073ef0e164687", "migrations/010_schema_baseline_manifest.sql": "e44e58d6c23b0deee6212884691ea5a753167f11b847d7b8b56186b9c74455c8"}
	for _, file := range files {
		contents, err := fs.ReadFile(migrationFS, file)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(contents)
		if got := hex.EncodeToString(digest[:]); got != checksums[file] {
			t.Fatalf("migration %s checksum=%s, want %s", file, got, checksums[file])
		}
	}
}

func TestSchemaBaselineManifestMigrationRefusesInference(t *testing.T) {
	raw, err := migrationFS.ReadFile("migrations/010_schema_baseline_manifest.sql")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	for _, required := range []string{"IF EXISTS (SELECT 1 FROM public.delivery_manifests)", "existing baseline payloads are not inferred", "schema_baseline_payload JSONB NOT NULL", "schema_baseline_fingerprint TEXT NOT NULL"} {
		if !strings.Contains(source, required) {
			t.Fatalf("delivery baseline-binding migration missing %q", required)
		}
	}
}

func TestManifestCheckpointPayloadMigrationFailsClosedWithoutInference(t *testing.T) {
	raw, err := migrationFS.ReadFile("migrations/009_manifest_checkpoint_payload.sql")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	for _, required := range []string{"IF EXISTS (SELECT 1 FROM public.delivery_manifests)", "existing checkpoint payloads are not inferred", "checkpoint_metadata JSONB NOT NULL", "checkpoint_timestamp TIMESTAMPTZ NOT NULL"} {
		if !strings.Contains(source, required) {
			t.Fatalf("delivery checkpoint-payload migration missing %q", required)
		}
	}
}

func TestCurrentLogicalBatchMigrationFailsClosedWithoutInference(t *testing.T) {
	raw, err := migrationFS.ReadFile("migrations/008_current_logical_batch_identity.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)
	for _, required := range []string{"refuses noncanonical logical batch identities", "refuses ambiguous logical batch rows", "pg_catalog.sha256", "pg_catalog.decode('00','hex')", "delivery_manifests ALTER COLUMN logical_batch_id SET NOT NULL", "delivery_attempts ALTER COLUMN logical_batch_id SET NOT NULL", "delivery_receipts ALTER COLUMN logical_batch_id SET NOT NULL", "CREATE UNIQUE INDEX delivery_attempts_logical_batch_idx", "delivery_manifests_logical_batch_current", "delivery_receipts_logical_batch_current"} {
		if !strings.Contains(sql, required) {
			t.Fatalf("current identity migration missing %q", required)
		}
	}
	for _, forbidden := range []string{"UPDATE ", "SET logical_batch_id=", "COALESCE(", "WHERE logical_batch_id IS NOT NULL", "pgcrypto", "digest("} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("current identity migration contains inference/partial-identity token %q", forbidden)
		}
	}
}
