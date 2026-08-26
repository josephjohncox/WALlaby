package snowflake

import (
	"strings"
	"testing"
)

func TestManagedStagedProvisioningSQLIsCurrentSchemaOnly(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
	fingerprint := strings.Repeat("a", 64)
	statements, err := ManagedStagedProvisioningSQL(cfg, fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(statements, "\n")
	for _, required := range []string{"CREATE TABLE", "CREATE HYBRID TABLE", cfg.landingTable, cfg.authorityTable, cfg.targetManifestTable, "PROVISION_EPOCH", "CATALOG_FINGERPRINT", "ROW_HASHES_JSON", "PRIMARY KEY", "UNIQUE"} {
		if !strings.Contains(joined, required) {
			t.Fatalf("provisioning SQL omitted %q", required)
		}
	}
	for _, forbidden := range []string{"IF NOT EXISTS", "legacy", "fallback", "COPY_HISTORY"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("provisioning SQL contains compatibility/history token %q", forbidden)
		}
	}
	begin := ManagedStagedBeginProvisionSQL(cfg)
	finish := ManagedStagedFinishProvisionSQL(cfg, fingerprint)
	if !strings.Contains(begin, "STATE\"='PROVISIONING'") || !strings.Contains(begin, "NOT EXISTS") || !strings.Contains(begin, "EXPIRES_AT\">CURRENT_TIMESTAMP()") {
		t.Fatalf("exclusive provisioning guard is incomplete: %s", begin)
	}
	if !strings.Contains(finish, "STATE\"='CURRENT'") || !strings.Contains(finish, fingerprint) {
		t.Fatalf("provisioning completion guard is incomplete: %s", finish)
	}
}
