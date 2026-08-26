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
	statements, err := managedStagedProvisioningSQL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(statements, "\n")
	if strings.Contains(joined, fingerprint) || strings.Contains(joined, "'CATALOG'") {
		t.Fatal("bootstrap DDL installed a catalog fingerprint before live post-create inspection")
	}
	install, err := managedStagedInstallCatalogSQL(cfg, fingerprint)
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
	begin, err := managedStagedBeginProvisionSQL(cfg, attempt, 1)
	if err != nil {
		t.Fatal(err)
	}
	finish, err := managedStagedFinishProvisionSQL(cfg, attempt, 2, fingerprint)
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

func TestManagedStagedProvisioningPublicSpecAPI(t *testing.T) {
	t.Parallel()
	dsn, options := stagedValidOptions(t)
	delete(options, "managed_landing_created_on")
	delete(options, "managed_authority_created_on")
	delete(options, "managed_target_manifest_created_on")
	spec := ManagedStagedProvisionSpec{Endpoint: connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: options}}
	statements, err := ManagedStagedProvisioningSQLForSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(statements) == 0 || !strings.Contains(strings.Join(statements, "\n"), "CREATE HYBRID TABLE") {
		t.Fatalf("public provision SQL omitted current auxiliary DDL: %v", statements)
	}
	if spec.Endpoint.Options["dsn"] != dsn {
		t.Fatal("public provision API mutated the non-secret endpoint specification")
	}
	withPredictedIdentity := make(map[string]string, len(options)+1)
	for name, value := range options {
		withPredictedIdentity[name] = value
	}
	withPredictedIdentity["managed_landing_created_on"] = "2026-01-01T00:00:00.000000000+00:00"
	if _, err := ManagedStagedProvisioningSQLForSpec(ManagedStagedProvisionSpec{Endpoint: connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: withPredictedIdentity}}); err == nil {
		t.Fatal("bootstrap accepted a predicted auxiliary creation identity")
	}
}

func TestRestoreAbortedManagedStagedProvisionRequiresExactPreAttemptFingerprint(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
	fingerprint := strings.Repeat("a", 64)
	for _, test := range []struct {
		name   string
		status ManagedStagedProvisionStatus
		rows   int64
		want   error
	}{
		{name: "restores exact aborted state", status: ManagedStagedProvisionStatus{State: "ABORTED", ProvisionEpoch: 2, StoredFingerprint: fingerprint, LiveFingerprint: fingerprint}, rows: 1},
		{name: "rejects ABA drift", status: ManagedStagedProvisionStatus{State: "ABORTED", ProvisionEpoch: 2, StoredFingerprint: fingerprint, LiveFingerprint: strings.Repeat("b", 64)}, want: connector.ErrDeliveryConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if test.want == nil {
				mock.ExpectExec("UPDATE .*WALLABY_AUTHORITY.*STATE.*CURRENT").WillReturnResult(sqlmock.NewResult(0, test.rows))
			}
			err = restoreAbortedManagedStagedProvision(context.Background(), db, cfg, test.status, 2)
			if test.want == nil && err != nil || test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("restore error=%v want=%v", err, test.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
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
