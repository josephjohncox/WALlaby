package iceberg

import (
	"context"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestConfigFingerprintPinsEffectiveCatalogWithoutCredentialValues(t *testing.T) {
	t.Parallel()

	base := Config{
		Profile: CatalogProfileS3Tables, URI: "https://glue.us-east-1.amazonaws.com/iceberg", Warehouse: "123456789012:s3tablescatalog/example",
		TargetNamespace: "wallaby", TablePrefix: "cdc_", ControlTable: "__wallaby_control", Region: "us-east-1", SigningName: "glue",
		ExpectedAWSRoleARN: "arn:aws:iam::123456789012:role/wallaby", SigV4: true,
		S3TablesTableBucketARN: "arn:aws:s3tables:us-east-1:123456789012:bucket/example", MaxCommitRetries: 4,
	}
	first, err := ConfigFingerprint(base)
	if err != nil {
		t.Fatal(err)
	}
	rotated := base
	rotated.S3AccessKeyID = "rotated-access-one"
	rotated.S3SecretAccessKey = "rotated-secret-one"
	second, err := ConfigFingerprint(rotated)
	if err != nil {
		t.Fatal(err)
	}
	rotated.S3AccessKeyID = "rotated-access-two"
	rotated.S3SecretAccessKey = "rotated-secret-two"
	third, err := ConfigFingerprint(rotated)
	if err != nil {
		t.Fatal(err)
	}
	if second != third {
		t.Fatal("credential value rotation changed the effective config fingerprint")
	}
	if first == second {
		t.Fatal("enabling a different catalog authentication mode did not change the effective config fingerprint")
	}
	changed := base
	changed.Warehouse = "123456789012:s3tablescatalog/other"
	changedFingerprint, err := ConfigFingerprint(changed)
	if err != nil {
		t.Fatal(err)
	}
	if changedFingerprint == first {
		t.Fatal("catalog warehouse change did not change the effective config fingerprint")
	}
	changed = base
	changed.ExpectedAWSRoleARN = "arn:aws:iam::123456789012:role/other-writer"
	changedFingerprint, err = ConfigFingerprint(changed)
	if err != nil {
		t.Fatal(err)
	}
	if changedFingerprint == first {
		t.Fatal("AWS writer identity change did not change the effective config fingerprint")
	}
}

func TestDestinationMarkerAcceptsDeploymentOwnedS3TablesConfiguration(t *testing.T) {
	t.Parallel()

	spec := connector.Spec{Type: connector.EndpointIceberg, Options: map[string]string{
		"catalog_profile":         CatalogProfileS3Tables,
		"destination_revision_id": "iceberg-s3tables-v1",
		"namespace":               "wallaby",
	}}
	if err := (&Destination{}).Open(context.Background(), spec); err != nil {
		t.Fatalf("marker open rejected deployment-owned S3 Tables configuration: %v", err)
	}
}

func TestParseSpecRejectsFlowOwnedCatalogEndpoints(t *testing.T) {
	t.Parallel()

	defaults := Config{Profile: CatalogProfileREST, URI: "https://catalog.example/iceberg", Warehouse: "warehouse", Region: "us-east-1", S3Endpoint: "https://s3.example", S3Region: "us-east-1"}
	for _, key := range []string{"uri", "warehouse", "prefix", "region", "s3tables_table_bucket_arn", "s3_endpoint", "s3_region"} {
		t.Run(key, func(t *testing.T) {
			t.Parallel()
			_, err := ParseSpec(connector.Spec{Type: connector.EndpointIceberg, Options: map[string]string{
				"destination_revision_id": "iceberg-v1", key: "flow-owned-value",
			}}, defaults)
			if err == nil || !strings.Contains(err.Error(), "unsupported persisted Iceberg option") {
				t.Fatalf("option %s error=%v", key, err)
			}
		})
	}
}

func TestValidateFlowSpecRejectsFixedTableCollapse(t *testing.T) {
	t.Parallel()

	err := ValidateFlowSpec(connector.Spec{Type: connector.EndpointIceberg, Options: map[string]string{
		"destination_revision_id": "iceberg-v1",
		"table":                   "all_changes",
	}})
	if err == nil || !strings.Contains(err.Error(), "fixed-table collapse") {
		t.Fatalf("error=%v, want fixed-table rejection", err)
	}
}

func TestParseSpecRejectsPersistedCatalogSecrets(t *testing.T) {
	t.Parallel()

	for _, key := range []string{
		"oauth_token", "oauth_credential", "client_key", "client_key_file",
		"s3_access_key_id", "s3_secret_access_key", "aws_access_key_id", "aws_secret_access_key",
	} {
		t.Run(key, func(t *testing.T) {
			t.Parallel()
			_, err := ParseSpec(connector.Spec{Type: connector.EndpointIceberg, Options: map[string]string{
				"destination_revision_id": "iceberg-v1",
				key:                       "secret",
			}}, Config{URI: "https://catalog.example/iceberg", Warehouse: "warehouse"})
			if err == nil || !strings.Contains(err.Error(), "unsupported persisted Iceberg option") {
				t.Fatalf("error=%v, want persisted-secret rejection", err)
			}
		})
	}
}

func TestParseSpecRejectsNonGlueS3TablesCatalogEndpoint(t *testing.T) {
	t.Parallel()

	_, err := ParseSpec(connector.Spec{Type: connector.EndpointIceberg, Options: map[string]string{
		"destination_revision_id": "iceberg-s3tables-v1",
	}}, Config{
		Profile: CatalogProfileS3Tables, URI: "https://attacker.example/iceberg",
		Warehouse: "123456789012:s3tablescatalog/example", Region: "us-east-1",
		S3TablesTableBucketARN: "arn:aws:s3tables:us-east-1:123456789012:bucket/example",
	})
	if err == nil || !strings.Contains(err.Error(), "regional AWS Glue Iceberg endpoint") {
		t.Fatalf("error=%v, want regional Glue endpoint rejection", err)
	}
}

func TestParseSpecUsesDeploymentS3TablesIdentity(t *testing.T) {
	t.Parallel()

	defaults := Config{
		Profile: CatalogProfileS3Tables, Warehouse: "123456789012:s3tablescatalog/example",
		Region: "us-east-1", ExpectedAWSRoleARN: "arn:aws:iam::123456789012:role/wallaby",
		S3TablesTableBucketARN: "arn:aws:s3tables:us-east-1:123456789012:bucket/example",
	}
	cfg, err := ParseSpec(connector.Spec{Type: connector.EndpointIceberg, Options: map[string]string{
		"destination_revision_id": "iceberg-s3tables-v1",
		"namespace":               "wallaby",
	}}, defaults)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Profile != CatalogProfileS3Tables || cfg.URI != "https://glue.us-east-1.amazonaws.com/iceberg" || cfg.Region != defaults.Region || cfg.S3TablesTableBucketARN != defaults.S3TablesTableBucketARN {
		t.Fatalf("parsed deployment identity=%+v", cfg)
	}
}
