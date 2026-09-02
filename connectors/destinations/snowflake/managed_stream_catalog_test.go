package snowflake

import (
	"strings"
	"testing"
)

func managedStreamCatalogTestColumns(kind string, names []string) map[string]managedColumnSnapshot {
	contracts := managedStreamColumnContracts(kind, names)
	columns := make(map[string]managedColumnSnapshot, len(names))
	for _, name := range names {
		contract := contracts[name]
		columns[name] = managedColumnSnapshot{
			dataType: contract.dataType, characterMaximumLength: contract.characterMaximumLength,
			numericPrecision: contract.numericPrecision, numericScale: contract.numericScale,
			datetimePrecision: contract.datetimePrecision,
		}
	}
	return columns
}

func validManagedStreamCatalog(cfg streamConfig) managedStreamCatalogSnapshot {
	targetColumns := streamChangelogColumns()
	receiptColumns := append(append([]string(nil), streamReceiptColumns()...), "COMMITTED_AT")
	channelColumns := []string{"FLOW_INCARNATION_ID", "DESTINATION_REVISION_ID", "CHANNEL_NAME", "PIPE_NAME", "PIPE_REVISION", "CHANNEL_REVISION", "CONTINUATION_TOKEN", "COMMITTED_OFFSET_TOKEN", "LOGICAL_BATCH_ID", "ROWS_CONTENT_HASH", "REQUEST_ID", "STATE_VERSION", "UPDATED_AT"}
	requestColumns := []string{"REQUEST_ID", "FLOW_ID", "FLOW_INCARNATION_ID", "SOURCE_LINEAGE_ID", "DESTINATION_REVISION_ID", "LOGICAL_BATCH_ID", "POSITION_ID", "CONTENT_HASH", "MANIFEST_HASH", "ROWS_CONTENT_HASH", "ROW_COUNT", "CHANNEL_NAME", "PIPE_NAME", "CHANNEL_REVISION", "PIPE_REVISION", "INPUT_CONTINUATION_TOKEN", "EXPECTED_PREVIOUS_COMMITTED_OFFSET_TOKEN", "REQUESTED_OFFSET_TOKEN", "RESPONSE_CONTINUATION_TOKEN", "COMMITTED_OFFSET_TOKEN", "GENERATION", "ACQUISITION_ID", "LEASE_EPOCH", "ATTEMPT", "PHASE", "PHASE_VERSION", "RESPONSE_KIND", "RESPONSE_EVIDENCE", "CREATED_AT", "UPDATED_AT"}
	owner := map[string][]string{cfg.ownerRole: {"OWNERSHIP"}}
	return managedStreamCatalogSnapshot{
		pipeCount: 1,
		pipe: managedPipeSnapshot{
			present: true, definition: "COPY INTO " + managedSnowflakeStreamQualifiedTable(cfg, cfg.table) + " FROM TABLE(DATA_SOURCE(TYPE=>'STREAMING'))",
			ownerRole: cfg.ownerRole, createdOn: cfg.pipeCreatedOn, comment: managedStreamOwnershipComment(cfg, "pipe"),
			grants: map[string][]string{cfg.executionRole: {"MONITOR", "OPERATE"}, cfg.ownerRole: {"OWNERSHIP"}},
		},
		target: managedTableSnapshot{
			kind: "TABLE", ownerRole: cfg.ownerRole, createdOn: cfg.targetCreatedOn, comment: managedStreamOwnershipComment(cfg, "target"),
			columns: managedStreamCatalogTestColumns("target", targetColumns),
			grants:  map[string][]string{cfg.executionRole: {"SELECT"}, cfg.ownerRole: owner[cfg.ownerRole]},
		},
		receipts: managedTableSnapshot{
			kind: "HYBRID TABLE", ownerRole: cfg.ownerRole, createdOn: cfg.receiptsCreatedOn, comment: managedStreamOwnershipComment(cfg, "receipts"),
			columns:     managedStreamCatalogTestColumns("receipts", receiptColumns),
			constraints: []managedConstraintSnapshot{{name: "WALLABY_STREAM_RECEIPT_PK", constraintType: "PRIMARY KEY", enforced: true, columns: []string{"EXTERNAL_ID"}}},
			grants:      map[string][]string{cfg.executionRole: {"DELETE", "INSERT", "SELECT", "UPDATE"}, cfg.ownerRole: owner[cfg.ownerRole]},
		},
		channel: managedTableSnapshot{
			kind: "HYBRID TABLE", ownerRole: cfg.ownerRole, createdOn: cfg.channelStateCreatedOn, comment: managedStreamOwnershipComment(cfg, "channel_state"),
			columns:     managedStreamCatalogTestColumns("channel_state", channelColumns),
			constraints: []managedConstraintSnapshot{{name: "WALLABY_STREAM_CHANNEL_PK", constraintType: "PRIMARY KEY", enforced: true, columns: []string{"FLOW_INCARNATION_ID", "DESTINATION_REVISION_ID", "CHANNEL_NAME"}}},
			grants:      map[string][]string{cfg.executionRole: {"DELETE", "INSERT", "SELECT", "UPDATE"}, cfg.ownerRole: owner[cfg.ownerRole]},
		},
		requests: managedTableSnapshot{
			kind: "HYBRID TABLE", ownerRole: cfg.ownerRole, createdOn: cfg.requestJournalCreatedOn, comment: managedStreamOwnershipComment(cfg, "request_journal"),
			columns: managedStreamCatalogTestColumns("request_journal", requestColumns),
			constraints: []managedConstraintSnapshot{
				{name: "WALLABY_STREAM_REQUEST_ATTEMPT", constraintType: "UNIQUE", enforced: true, columns: []string{"FLOW_INCARNATION_ID", "DESTINATION_REVISION_ID", "LOGICAL_BATCH_ID", "ATTEMPT"}},
				{name: "WALLABY_STREAM_REQUEST_PK", constraintType: "PRIMARY KEY", enforced: true, columns: []string{"REQUEST_ID"}},
			},
			grants: map[string][]string{cfg.executionRole: {"DELETE", "INSERT", "SELECT", "UPDATE"}, cfg.ownerRole: owner[cfg.ownerRole]},
		},
	}
}

func TestValidateManagedStreamCatalogRejectsOneBitSchemaAndPipeDrift(t *testing.T) {
	cfg := streamTestConfig(t)
	baseline := validManagedStreamCatalog(cfg)
	if err := validateManagedStreamCatalogSnapshot(cfg, baseline); err != nil {
		t.Fatalf("valid catalog rejected: %v definition=%q normalized=%q", err, baseline.pipe.definition, normalizeStagedPipeSQL(baseline.pipe.definition))
	}
	mutations := map[string]func(*managedStreamCatalogSnapshot){
		"target kind": func(c *managedStreamCatalogSnapshot) { c.target.kind = "HYBRID TABLE" },
		"varchar width": func(c *managedStreamCatalogSnapshot) {
			v := c.target.columns["CONTENT_HASH"]
			v.characterMaximumLength--
			c.target.columns["CONTENT_HASH"] = v
		},
		"numeric precision": func(c *managedStreamCatalogSnapshot) {
			v := c.channel.columns["CHANNEL_REVISION"]
			v.numericPrecision--
			c.channel.columns["CHANNEL_REVISION"] = v
		},
		"numeric scale": func(c *managedStreamCatalogSnapshot) {
			v := c.channel.columns["CHANNEL_REVISION"]
			v.numericScale++
			c.channel.columns["CHANNEL_REVISION"] = v
		},
		"timestamp precision": func(c *managedStreamCatalogSnapshot) {
			v := c.requests.columns["UPDATED_AT"]
			v.datetimePrecision--
			c.requests.columns["UPDATED_AT"] = v
		},
		"target prefix": func(c *managedStreamCatalogSnapshot) {
			c.pipe.definition = strings.Replace(c.pipe.definition, `"WALLABY_CHANGELOG"`, `"WALLABY_CHANGELOG_EVIL"`, 1)
		},
		"comment":         func(c *managedStreamCatalogSnapshot) { c.pipe.definition += " -- unsafe" },
		"extra statement": func(c *managedStreamCatalogSnapshot) { c.pipe.definition += "; DROP TABLE X" },
		"transform": func(c *managedStreamCatalogSnapshot) {
			c.pipe.definition = "COPY INTO " + managedSnowflakeStreamQualifiedTable(cfg, cfg.table) + " FROM (SELECT * FROM TABLE(DATA_SOURCE(TYPE=>'STREAMING')))"
		},
		"wrong source": func(c *managedStreamCatalogSnapshot) {
			c.pipe.definition = strings.Replace(c.pipe.definition, "'STREAMING'", "'OTHER'", 1)
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			catalog := validManagedStreamCatalog(cfg)
			mutate(&catalog)
			if err := validateManagedStreamCatalogSnapshot(cfg, catalog); err == nil {
				t.Fatalf("catalog accepted %s drift", name)
			}
		})
	}
}

func TestManagedStreamCatalogFingerprintCoversValidatedSnapshot(t *testing.T) {
	cfg := streamTestConfig(t)
	catalog := validManagedStreamCatalog(cfg)
	baseline, err := managedStreamCatalogFingerprint(catalog)
	if err != nil {
		t.Fatal(err)
	}
	catalog.requests.comment += "x"
	drifted, err := managedStreamCatalogFingerprint(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if baseline == drifted {
		t.Fatal("catalog fingerprint ignored request-journal comment drift")
	}
}
