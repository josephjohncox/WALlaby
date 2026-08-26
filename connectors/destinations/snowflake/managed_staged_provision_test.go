package snowflake

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestManagedStagedProvisioningSQLIsCurrentSchemaOnly(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
	fingerprint := strings.Repeat("a", 64)
	statements, err := ManagedStagedProvisioningSQL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(statements, "\n")
	if strings.Contains(joined, fingerprint) || strings.Contains(joined, "'CATALOG'") {
		t.Fatal("bootstrap DDL installed a catalog fingerprint before live post-create inspection")
	}
	install, err := ManagedStagedInstallCatalogSQL(cfg, fingerprint)
	if err != nil || !strings.Contains(install, fingerprint) {
		t.Fatalf("post-create catalog installation=%q err=%v", install, err)
	}
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
	attempt := "11111111-1111-1111-1111-111111111111"
	begin, err := ManagedStagedBeginProvisionSQL(cfg, attempt, 1)
	if err != nil {
		t.Fatal(err)
	}
	finish, err := ManagedStagedFinishProvisionSQL(cfg, attempt, 2, fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(begin, "STATE\"='PROVISIONING'") || !strings.Contains(begin, "NOT EXISTS") || !strings.Contains(begin, "EXPIRES_AT\">CURRENT_TIMESTAMP()") || !strings.Contains(begin, "PROVISION_ATTEMPT_ID") {
		t.Fatalf("exclusive provisioning guard is incomplete: %s", begin)
	}
	if !strings.Contains(finish, "STATE\"='CURRENT'") || !strings.Contains(finish, fingerprint) || !strings.Contains(finish, attempt) {
		t.Fatalf("provisioning completion guard is incomplete: %s", finish)
	}
}

func TestExecStagedProvisionCASRequiresOneAffectedRow(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		rows int64
		want error
	}{{name: "winner", rows: 1}, {name: "lost", rows: 0, want: connector.ErrDeliveryIndeterminate}, {name: "ambiguous", rows: 2, want: connector.ErrDeliveryIndeterminate}} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectExec("UPDATE AUTHORITY").WillReturnResult(sqlmock.NewResult(0, test.rows))
			err = execStagedProvisionCAS(context.Background(), db, "UPDATE AUTHORITY", "test")
			if test.want == nil && err != nil || test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("CAS rows=%d error=%v want=%v", test.rows, err, test.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
