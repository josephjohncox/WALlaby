package snowflake

import (
	"errors"
	"fmt"
	"strings"
)

const ManagedStagedAuthoritySchemaVersion = 2

// managedStagedProvisioningSQL returns the current-schema-only owner DDL. It
// does not contain compatibility branches or IF NOT EXISTS repair paths.
func ManagedStagedProvisioningSQL(cfg stagedConfig, catalogFingerprint string) ([]string, error) {
	if len(catalogFingerprint) != 64 || strings.Trim(catalogFingerprint, "0123456789abcdef") != "" {
		return nil, errors.New("managed staged Snowflake provisioning requires a canonical catalog fingerprint")
	}
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
		fmt.Sprintf("INSERT INTO %s (\"AUTHORITY_KIND\",\"DESTINATION_REVISION_ID\",\"AUTHORITY_ID\",\"OWNER_ID\",\"PROVISION_EPOCH\",\"CATALOG_FINGERPRINT\",\"STATE\",\"EXPIRES_AT\",\"UPDATED_AT\") VALUES ('CATALOG','%s','CURRENT','%s',1,'%s','CURRENT','9999-12-31 23:59:59 +00:00',CURRENT_TIMESTAMP())", authority, cfg.destinationRevision, cfg.ownerRole, catalogFingerprint),
		"GRANT SELECT, INSERT, DELETE ON TABLE " + landing + " TO ROLE " + quoteIdent(cfg.executionRole, '"'),
		"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE " + authority + " TO ROLE " + quoteIdent(cfg.executionRole, '"'),
		"GRANT SELECT, INSERT ON TABLE " + manifest + " TO ROLE " + quoteIdent(cfg.executionRole, '"'),
	}
	return statements, nil
}

// managedStagedBeginProvisionSQL fences runtime leases before owner DDL. DDL in
// Snowflake auto-commits, so STATE='PROVISIONING' is the durable crash marker.
func ManagedStagedBeginProvisionSQL(cfg stagedConfig) string {
	authority := managedSnowflakeStagedQualifiedTable(cfg, cfg.authorityTable)
	return "UPDATE " + authority + " AS C SET \"STATE\"='PROVISIONING',\"PROVISION_EPOCH\"=\"PROVISION_EPOCH\"+1,\"UPDATED_AT\"=CURRENT_TIMESTAMP() WHERE C.\"AUTHORITY_KIND\"='CATALOG' AND C.\"DESTINATION_REVISION_ID\"='" + cfg.destinationRevision + "' AND C.\"STATE\"='CURRENT' AND NOT EXISTS (SELECT 1 FROM " + authority + " AS L WHERE L.\"AUTHORITY_KIND\"='LEASE' AND L.\"DESTINATION_REVISION_ID\"=C.\"DESTINATION_REVISION_ID\" AND L.\"STATE\"='ACTIVE' AND L.\"EXPIRES_AT\">CURRENT_TIMESTAMP())"
}

func ManagedStagedFinishProvisionSQL(cfg stagedConfig, catalogFingerprint string) string {
	authority := managedSnowflakeStagedQualifiedTable(cfg, cfg.authorityTable)
	return "UPDATE " + authority + " SET \"CATALOG_FINGERPRINT\"='" + catalogFingerprint + "',\"STATE\"='CURRENT',\"UPDATED_AT\"=CURRENT_TIMESTAMP() WHERE \"AUTHORITY_KIND\"='CATALOG' AND \"DESTINATION_REVISION_ID\"='" + cfg.destinationRevision + "' AND \"STATE\"='PROVISIONING'"
}
