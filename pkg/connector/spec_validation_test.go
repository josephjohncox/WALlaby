package connector

import (
	"strings"
	"testing"
)

func TestValidatePersistedIcebergSpecUsesAllowlist(t *testing.T) {
	t.Parallel()

	valid := Spec{Type: EndpointIceberg, Options: map[string]string{
		"catalog_profile": "s3tables", "destination_revision_id": "iceberg-v1",
		"control_table": "__wallaby_control",
	}}
	if err := ValidatePersistedSpec(valid); err != nil {
		t.Fatalf("valid persisted Iceberg spec: %v", err)
	}

	for _, key := range []string{
		"oauth_token", "OAuth-Token", "aws_session_token", "s3.session-token",
		"s3_secret_access_key", "client_key_file", "table", "namespace", "table_prefix", "max_commit_retries", "unknown_typo",
	} {
		t.Run(key, func(t *testing.T) {
			t.Parallel()
			candidate := Spec{Type: EndpointIceberg, Options: map[string]string{
				"destination_revision_id": "iceberg-v1", key: "secret-or-unsafe",
			}}
			if err := ValidatePersistedSpec(candidate); err == nil || !strings.Contains(err.Error(), "unsupported persisted Iceberg option") {
				t.Fatalf("option %q error=%v", key, err)
			}
		})
	}
}
