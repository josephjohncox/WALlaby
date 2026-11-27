package connector

import (
	"strings"
	"testing"
)

func TestValidatePersistedIcebergSpecUsesAllowlist(t *testing.T) {
	t.Parallel()

	valid := RuntimeSpec{Type: EndpointIceberg, Options: map[string]string{
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
			candidate := RuntimeSpec{Type: EndpointIceberg, Options: map[string]string{
				"destination_revision_id": "iceberg-v1", key: "secret-or-unsafe",
			}}
			if err := ValidatePersistedSpec(candidate); err == nil || !strings.Contains(err.Error(), "unsupported persisted Iceberg option") {
				t.Fatalf("option %q error=%v", key, err)
			}
		})
	}
}

func TestValidatePersistedPostgresSpecRejectsLocalFileSnapshotState(t *testing.T) {
	t.Parallel()

	for name, options := range map[string]map[string]string{
		"file backend":          {"snapshot_state_backend": "file"},
		"file backend mixed":    {" Snapshot_State_Backend ": " FILE "},
		"path only":             {"snapshot_state_path": "/var/lib/wallaby/state.json"},
		"file backend and path": {"snapshot_state_backend": "file", "snapshot_state_path": "state.json"},
	} {
		if err := ValidatePersistedSpec(RuntimeSpec{Type: EndpointPostgres, Options: options}); err == nil || !strings.Contains(err.Error(), "snapshot_state") {
			t.Errorf("%s: error=%v, want snapshot_state rejection", name, err)
		}
	}
	for name, options := range map[string]map[string]string{
		"none":     nil,
		"postgres": {"snapshot_state_backend": "postgres", "snapshot_state_dsn": "postgres://state.example/db"},
		"disabled": {"snapshot_state_backend": "none"},
		"blank":    {"snapshot_state_path": "  "},
	} {
		if err := ValidatePersistedSpec(RuntimeSpec{Type: EndpointPostgres, Options: options}); err != nil {
			t.Errorf("%s: unexpected rejection: %v", name, err)
		}
	}

}
