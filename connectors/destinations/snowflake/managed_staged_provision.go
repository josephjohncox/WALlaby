package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

const ManagedStagedAuthoritySchemaVersion = 3

// ManagedStagedProvisionSpec is the strict, non-secret owner-operation input.
// Endpoint options pass the same closed staged-profile validator as runtime
// admission. The owner DSN is supplied only to the CLI process and is never
// stored in this specification.
type ManagedStagedProvisionSpec struct {
	Endpoint connector.RuntimeSpec `json:"endpoint"`
}

func (s ManagedStagedProvisionSpec) config() (stagedConfig, error) {
	if s.Endpoint.Type != connector.EndpointSnowflake {
		return stagedConfig{}, errors.New("managed staged Snowflake provision spec requires a Snowflake endpoint")
	}
	return stagedConfigFromSpec(strings.TrimSpace(s.Endpoint.Options["dsn"]), s.Endpoint)
}

// ManagedStagedProvisionStatus reports the durable owner operation and current
// live catalog fingerprint without exposing credentials.
type ManagedStagedProvisionStatus struct {
	DestinationRevision string `json:"destination_revision"`
	State               string `json:"state"`
	ProvisionEpoch      int64  `json:"provision_epoch"`
	ProvisionAttemptID  string `json:"provision_attempt_id,omitempty"`
	StoredFingerprint   string `json:"stored_fingerprint"`
	LiveFingerprint     string `json:"live_fingerprint"`
}

func InspectManagedStagedProvision(ctx context.Context, db *sql.DB, spec ManagedStagedProvisionSpec) (ManagedStagedProvisionStatus, error) {
	cfg, err := spec.config()
	if err != nil {
		return ManagedStagedProvisionStatus{}, err
	}
	if db == nil {
		return ManagedStagedProvisionStatus{}, errors.New("managed staged Snowflake provision requires an owner database")
	}
	status := ManagedStagedProvisionStatus{DestinationRevision: cfg.destinationRevision}
	authority := managedSnowflakeStagedQualifiedTable(cfg, cfg.authorityTable)
	var attempt sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT \"STATE\",\"PROVISION_EPOCH\",\"PROVISION_ATTEMPT_ID\",\"CATALOG_FINGERPRINT\" FROM "+authority+" WHERE \"AUTHORITY_KIND\"='CATALOG' AND \"DESTINATION_REVISION_ID\"=? AND \"AUTHORITY_ID\"='CURRENT'", cfg.destinationRevision).Scan(&status.State, &status.ProvisionEpoch, &attempt, &status.StoredFingerprint); err != nil {
		return ManagedStagedProvisionStatus{}, fmt.Errorf("inspect managed staged Snowflake provision state: %w", err)
	}
	status.ProvisionAttemptID = attempt.String
	catalog, err := (&Destination{stagedConfig: cfg}).loadManagedStagedCatalog(ctx, db)
	if err != nil {
		return ManagedStagedProvisionStatus{}, err
	}
	if err := validateManagedStagedCatalog(cfg, catalog); err != nil {
		return ManagedStagedProvisionStatus{}, err
	}
	status.LiveFingerprint, err = managedStagedCatalogFingerprint(catalog)
	return status, err
}

func BeginManagedStagedProvision(ctx context.Context, db *sql.DB, spec ManagedStagedProvisionSpec, attemptID string, expectedEpoch int64) error {
	cfg, err := spec.config()
	if err != nil {
		return err
	}
	statement, err := ManagedStagedBeginProvisionSQL(cfg, attemptID, expectedEpoch)
	if err != nil {
		return err
	}
	return execStagedProvisionCAS(ctx, db, statement, "begin")
}

func ResumeManagedStagedProvision(ctx context.Context, db *sql.DB, spec ManagedStagedProvisionSpec, attemptID string, epoch int64) (ManagedStagedProvisionStatus, error) {
	status, err := InspectManagedStagedProvision(ctx, db, spec)
	if err != nil {
		return ManagedStagedProvisionStatus{}, err
	}
	if status.State != "PROVISIONING" || status.ProvisionEpoch != epoch || status.ProvisionAttemptID != attemptID {
		return ManagedStagedProvisionStatus{}, fmt.Errorf("%w: staged Snowflake provision attempt is not the current owner", connector.ErrDeliveryConflict)
	}
	cfg, _ := spec.config()
	statement, err := ManagedStagedFinishProvisionSQL(cfg, attemptID, epoch, status.LiveFingerprint)
	if err != nil {
		return ManagedStagedProvisionStatus{}, err
	}
	if err := execStagedProvisionCAS(ctx, db, statement, "finish"); err != nil {
		return ManagedStagedProvisionStatus{}, err
	}
	return InspectManagedStagedProvision(ctx, db, spec)
}

func AbortManagedStagedProvision(ctx context.Context, db *sql.DB, spec ManagedStagedProvisionSpec, attemptID string, epoch int64) error {
	cfg, err := spec.config()
	if err != nil {
		return err
	}
	statement, err := ManagedStagedAbortProvisionSQL(cfg, attemptID, epoch)
	if err != nil {
		return err
	}
	return execStagedProvisionCAS(ctx, db, statement, "abort")
}

func execStagedProvisionCAS(ctx context.Context, db *sql.DB, statement, action string) error {
	if db == nil {
		return errors.New("managed staged Snowflake provision requires an owner database")
	}
	result, err := db.ExecContext(ctx, statement)
	if err != nil {
		return fmt.Errorf("%s managed staged Snowflake provision: %w", action, err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("%w: %s managed staged Snowflake provision CAS affected %d rows", connector.ErrDeliveryIndeterminate, action, affected)
	}
	return nil
}

// managedStagedProvisioningSQL returns the current-schema-only owner DDL. It
// does not contain compatibility branches or IF NOT EXISTS repair paths.
func ManagedStagedProvisioningSQL(cfg stagedConfig) ([]string, error) {
	landing := managedSnowflakeStagedQualifiedTable(cfg, cfg.landingTable)
	authority := managedSnowflakeStagedQualifiedTable(cfg, cfg.authorityTable)
	manifest := managedSnowflakeStagedQualifiedTable(cfg, cfg.targetManifestTable)
	target := managedSnowflakeStagedQualifiedTable(cfg, cfg.table)
	comments := func(kind string) string {
		return strings.ReplaceAll(managedStagedOwnershipComment(cfg, kind), "'", "''")
	}
	statements := []string{
		"CREATE TABLE " + landing + " LIKE " + target,
		"COMMENT ON TABLE " + landing + " IS '" + comments("landing") + "'",
		"CREATE HYBRID TABLE " + authority + ` (
  "AUTHORITY_KIND" VARCHAR NOT NULL,
  "DESTINATION_REVISION_ID" VARCHAR NOT NULL,
  "AUTHORITY_ID" VARCHAR NOT NULL,
  "OWNER_ID" VARCHAR NOT NULL,
  "FLOW_INCARNATION_ID" VARCHAR,
  "GENERATION" NUMBER(38,0),
  "ACQUISITION_ID" VARCHAR,
  "LEASE_EPOCH" NUMBER(38,0),
  "PROVISION_EPOCH" NUMBER(38,0) NOT NULL,
  "PROVISION_ATTEMPT_ID" VARCHAR,
  "CATALOG_FINGERPRINT" VARCHAR NOT NULL,
  "LOGICAL_BATCH_ID" VARCHAR,
  "MANIFEST_HASH" VARCHAR,
  "CONTENT_HASH" VARCHAR,
  "FILE_CONTENT_HASH" VARCHAR,
  "PLAN_HASH" VARCHAR,
  "EXPECTED_ROW_COUNT" NUMBER(38,0),
  "STATE" VARCHAR NOT NULL,
  "EXPIRES_AT" TIMESTAMP_LTZ(9) NOT NULL,
  "UPDATED_AT" TIMESTAMP_LTZ(9) NOT NULL,
  PRIMARY KEY ("AUTHORITY_KIND","DESTINATION_REVISION_ID","AUTHORITY_ID")
)`,
		"COMMENT ON TABLE " + authority + " IS '" + comments("authority") + "'",
		"CREATE HYBRID TABLE " + manifest + ` (
  "DESTINATION_REVISION_ID" VARCHAR NOT NULL,
  "LOGICAL_BATCH_ID" VARCHAR NOT NULL,
  "MANIFEST_HASH" VARCHAR NOT NULL,
  "CONTENT_HASH" VARCHAR NOT NULL,
  "FILE_CONTENT_HASH" VARCHAR NOT NULL,
  "PLAN_HASH" VARCHAR NOT NULL,
  "EXPECTED_ROW_COUNT" NUMBER(38,0) NOT NULL,
  "ROW_HASHES_JSON" VARCHAR NOT NULL,
  "PROVISION_EPOCH" NUMBER(38,0) NOT NULL,
  "CATALOG_FINGERPRINT" VARCHAR NOT NULL,
  "COMMITTED_AT" TIMESTAMP_LTZ(9) NOT NULL,
  PRIMARY KEY ("DESTINATION_REVISION_ID","LOGICAL_BATCH_ID"),
  UNIQUE ("MANIFEST_HASH")
)`,
		"COMMENT ON TABLE " + manifest + " IS '" + comments("target_manifest") + "'",
		"GRANT SELECT, INSERT, DELETE ON TABLE " + landing + " TO ROLE " + quoteIdent(cfg.executionRole, '"'),
		"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE " + authority + " TO ROLE " + quoteIdent(cfg.executionRole, '"'),
		"GRANT SELECT, INSERT ON TABLE " + manifest + " TO ROLE " + quoteIdent(cfg.executionRole, '"'),
	}
	return statements, nil
}

// ManagedStagedInstallCatalogSQL installs the first catalog authority only
// after the owner reloads and fingerprints the live post-create objects.
func ManagedStagedInstallCatalogSQL(cfg stagedConfig, catalogFingerprint string) (string, error) {
	if len(catalogFingerprint) != 64 || strings.Trim(catalogFingerprint, "0123456789abcdef") != "" {
		return "", errors.New("managed staged Snowflake catalog install requires a canonical live fingerprint")
	}
	authority := managedSnowflakeStagedQualifiedTable(cfg, cfg.authorityTable)
	return fmt.Sprintf("INSERT INTO %s (\"AUTHORITY_KIND\",\"DESTINATION_REVISION_ID\",\"AUTHORITY_ID\",\"OWNER_ID\",\"PROVISION_EPOCH\",\"PROVISION_ATTEMPT_ID\",\"CATALOG_FINGERPRINT\",\"STATE\",\"EXPIRES_AT\",\"UPDATED_AT\") VALUES ('CATALOG','%s','CURRENT','%s',1,NULL,'%s','CURRENT','9999-12-31 23:59:59 +00:00',CURRENT_TIMESTAMP())", authority, cfg.destinationRevision, cfg.ownerRole, catalogFingerprint), nil
}

// managedStagedBeginProvisionSQL fences runtime leases before owner DDL. DDL in
// Snowflake auto-commits, so STATE='PROVISIONING' is the durable crash marker.
func ManagedStagedBeginProvisionSQL(cfg stagedConfig, attemptID string, expectedEpoch int64) (string, error) {
	if err := validateStagedProvisionAttempt(attemptID, expectedEpoch); err != nil {
		return "", err
	}
	authority := managedSnowflakeStagedQualifiedTable(cfg, cfg.authorityTable)
	return "UPDATE " + authority + " AS C SET \"STATE\"='PROVISIONING',\"PROVISION_EPOCH\"=\"PROVISION_EPOCH\"+1,\"PROVISION_ATTEMPT_ID\"='" + attemptID + "',\"UPDATED_AT\"=CURRENT_TIMESTAMP() WHERE C.\"AUTHORITY_KIND\"='CATALOG' AND C.\"DESTINATION_REVISION_ID\"='" + cfg.destinationRevision + "' AND C.\"PROVISION_EPOCH\"=" + fmt.Sprint(expectedEpoch) + " AND C.\"PROVISION_ATTEMPT_ID\" IS NULL AND C.\"STATE\"='CURRENT' AND NOT EXISTS (SELECT 1 FROM " + authority + " AS L WHERE L.\"AUTHORITY_KIND\"='LEASE' AND L.\"DESTINATION_REVISION_ID\"=C.\"DESTINATION_REVISION_ID\" AND L.\"STATE\"='ACTIVE' AND L.\"EXPIRES_AT\">CURRENT_TIMESTAMP())", nil
}

func ManagedStagedFinishProvisionSQL(cfg stagedConfig, attemptID string, provisionEpoch int64, catalogFingerprint string) (string, error) {
	if err := validateStagedProvisionAttempt(attemptID, provisionEpoch); err != nil {
		return "", err
	}
	if len(catalogFingerprint) != 64 || strings.Trim(catalogFingerprint, "0123456789abcdef") != "" {
		return "", errors.New("managed staged Snowflake finish requires a canonical live catalog fingerprint")
	}
	authority := managedSnowflakeStagedQualifiedTable(cfg, cfg.authorityTable)
	return "UPDATE " + authority + " SET \"CATALOG_FINGERPRINT\"='" + catalogFingerprint + "',\"PROVISION_ATTEMPT_ID\"=NULL,\"STATE\"='CURRENT',\"UPDATED_AT\"=CURRENT_TIMESTAMP() WHERE \"AUTHORITY_KIND\"='CATALOG' AND \"DESTINATION_REVISION_ID\"='" + cfg.destinationRevision + "' AND \"PROVISION_EPOCH\"=" + fmt.Sprint(provisionEpoch) + " AND \"PROVISION_ATTEMPT_ID\"='" + attemptID + "' AND \"STATE\"='PROVISIONING'", nil
}

func ManagedStagedAbortProvisionSQL(cfg stagedConfig, attemptID string, provisionEpoch int64) (string, error) {
	if err := validateStagedProvisionAttempt(attemptID, provisionEpoch); err != nil {
		return "", err
	}
	authority := managedSnowflakeStagedQualifiedTable(cfg, cfg.authorityTable)
	return "UPDATE " + authority + " SET \"PROVISION_ATTEMPT_ID\"=NULL,\"STATE\"='ABORTED',\"UPDATED_AT\"=CURRENT_TIMESTAMP() WHERE \"AUTHORITY_KIND\"='CATALOG' AND \"DESTINATION_REVISION_ID\"='" + cfg.destinationRevision + "' AND \"PROVISION_EPOCH\"=" + fmt.Sprint(provisionEpoch) + " AND \"PROVISION_ATTEMPT_ID\"='" + attemptID + "' AND \"STATE\"='PROVISIONING'", nil
}

func validateStagedProvisionAttempt(attemptID string, epoch int64) error {
	if epoch <= 0 || len(attemptID) != 36 {
		return errors.New("managed staged Snowflake provision attempt requires a UUID and positive epoch")
	}
	for index, value := range attemptID {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if value != '-' {
				return errors.New("managed staged Snowflake provision attempt must be a UUID")
			}
			continue
		}
		if !strings.ContainsRune("0123456789abcdef", value) {
			return errors.New("managed staged Snowflake provision attempt must be a lowercase UUID")
		}
	}
	return nil
}
