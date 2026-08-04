package snowflake

import (
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestManagedSnowflakeCatalogRequiresOwnedHybridTablesEnforcedIdentityAndCompatibleSchema(t *testing.T) {
	t.Parallel()
	cfg, catalog := managedCatalogFixture(t)
	if err := validateManagedSnowflakeCatalog(cfg, catalog); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*managedCatalogSnapshot)
		want   string
	}{
		{name: "schema task", mutate: func(snapshot *managedCatalogSnapshot) { snapshot.taskCount = 1 }, want: "requires none"},
		{name: "standard target", mutate: func(snapshot *managedCatalogSnapshot) { snapshot.target.kind = "TABLE" }, want: "HYBRID TABLE"},
		{name: "wrong target owner", mutate: func(snapshot *managedCatalogSnapshot) { snapshot.target.ownerRole = "OTHER" }, want: "owned by"},
		{name: "replaced target", mutate: func(snapshot *managedCatalogSnapshot) { snapshot.target.createdOn = "later" }, want: "creation identity"},
		{name: "execution role missing DML", mutate: func(snapshot *managedCatalogSnapshot) { delete(snapshot.target.grants, "ROLE") }, want: "privileges"},
		{name: "additional target writer", mutate: func(snapshot *managedCatalogSnapshot) { snapshot.target.grants["OTHER"] = []string{"INSERT"} }, want: "additional writer"},
		{name: "revision comment conflict", mutate: func(snapshot *managedCatalogSnapshot) { snapshot.target.comment = "wallaby:other" }, want: "ownership comment"},
		{name: "missing source column", mutate: func(snapshot *managedCatalogSnapshot) { delete(snapshot.target.columns, "VALUE") }, want: "missing source column"},
		{name: "wrong source type", mutate: func(snapshot *managedCatalogSnapshot) {
			column := snapshot.target.columns["ID"]
			column.dataType = "TEXT"
			snapshot.target.columns["ID"] = column
		}, want: "incompatible type"},
		{name: "narrow target text", mutate: func(snapshot *managedCatalogSnapshot) {
			column := snapshot.target.columns["VALUE"]
			column.characterMaximumLength = 10
			snapshot.target.columns["VALUE"] = column
		}, want: "VARCHAR width"},
		{name: "narrow target binary", mutate: func(snapshot *managedCatalogSnapshot) {
			column := snapshot.target.columns["PAYLOAD"]
			column.characterMaximumLength = 10
			snapshot.target.columns["PAYLOAD"] = column
		}, want: "BINARY width"},
		{name: "wrong nullability", mutate: func(snapshot *managedCatalogSnapshot) {
			column := snapshot.target.columns["VALUE"]
			column.nullable = false
			snapshot.target.columns["VALUE"] = column
		}, want: "nullability"},
		{name: "unenforced key", mutate: func(snapshot *managedCatalogSnapshot) { snapshot.target.constraints[0].enforced = false }, want: "enforced primary key"},
		{name: "wrong key order", mutate: func(snapshot *managedCatalogSnapshot) { snapshot.target.constraints[0].columns = []string{"VALUE"} }, want: "enforced primary key"},
		{name: "target foreign key", mutate: func(snapshot *managedCatalogSnapshot) { snapshot.target.otherConstraintCount = 1 }, want: "non-primary/unique constraints"},
		{name: "extra target unique key", mutate: func(snapshot *managedCatalogSnapshot) {
			snapshot.target.constraints = append(snapshot.target.constraints, managedConstraintSnapshot{constraintType: "UNIQUE", enforced: true, columns: []string{"VALUE"}})
		}, want: "exactly one enforced primary key"},
		{name: "receipt standard table", mutate: func(snapshot *managedCatalogSnapshot) { snapshot.receipts.kind = "TABLE" }, want: "receipt table must be HYBRID TABLE"},
		{name: "receipt wrong owner", mutate: func(snapshot *managedCatalogSnapshot) { snapshot.receipts.ownerRole = "OTHER" }, want: "receipt table must be owned"},
		{name: "replaced receipt table", mutate: func(snapshot *managedCatalogSnapshot) { snapshot.receipts.createdOn = "later" }, want: "creation identity"},
		{name: "receipt missing hash", mutate: func(snapshot *managedCatalogSnapshot) { delete(snapshot.receipts.columns, "MANIFEST_HASH") }, want: "receipt column MANIFEST_HASH"},
		{name: "receipt key not enforced", mutate: func(snapshot *managedCatalogSnapshot) { snapshot.receipts.constraints[0].enforced = false }, want: "receipt table requires enforced"},
		{name: "receipt foreign key", mutate: func(snapshot *managedCatalogSnapshot) { snapshot.receipts.otherConstraintCount = 1 }, want: "non-primary/unique constraints"},
		{name: "extra receipt unique key", mutate: func(snapshot *managedCatalogSnapshot) {
			snapshot.receipts.constraints = append(snapshot.receipts.constraints, managedConstraintSnapshot{constraintType: "UNIQUE", enforced: true, columns: []string{"FLOW_ID"}})
		}, want: "want exactly 3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copySnapshot := cloneManagedCatalogSnapshot(catalog)
			tt.mutate(&copySnapshot)
			if err := validateManagedSnowflakeCatalog(cfg, copySnapshot); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error=%v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestManagedSnowflakeCatalogRejectsCaseFoldedIdentifiers(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"id", "Id", " ID", "ID "} {
		if _, err := canonicalManagedSnowflakeCatalogIdentifier(name); err == nil {
			t.Errorf("noncanonical Snowflake catalog identifier %q was admitted", name)
		}
	}
	if got, err := canonicalManagedSnowflakeCatalogIdentifier("ID"); err != nil || got != "ID" {
		t.Fatalf("canonical Snowflake catalog identifier=%q/%v", got, err)
	}
	if hasManagedEnforcedConstraintType([]managedConstraintSnapshot{{
		constraintType: "PRIMARY KEY", enforced: true, columns: []string{"id"},
	}}, "PRIMARY KEY", []string{"ID"}) {
		t.Fatal("case-distinct quoted constraint column was folded onto managed target column")
	}
}

func TestManagedSnowflakeCatalogFingerprintIncludesObjectIdentitySchemaAndGrants(t *testing.T) {
	t.Parallel()
	_, catalog := managedCatalogFixture(t)
	baseline, err := managedSnowflakeCatalogFingerprint(catalog)
	if err != nil {
		t.Fatal(err)
	}
	clone := cloneManagedCatalogSnapshot(catalog)
	stable, err := managedSnowflakeCatalogFingerprint(clone)
	if err != nil {
		t.Fatal(err)
	}
	if stable != baseline {
		t.Fatalf("stable catalog fingerprint changed: %s != %s", stable, baseline)
	}
	mutations := []func(*managedCatalogSnapshot){
		func(snapshot *managedCatalogSnapshot) { snapshot.target.createdOn = "replacement" },
		func(snapshot *managedCatalogSnapshot) {
			snapshot.target.columns["VALUE"] = managedColumnSnapshot{dataType: "BINARY", nullable: true}
		},
		func(snapshot *managedCatalogSnapshot) { snapshot.target.grants["OTHER"] = []string{"INSERT"} },
		func(snapshot *managedCatalogSnapshot) { snapshot.target.constraints[0].name = "REPLACED_PK" },
	}
	for index, mutate := range mutations {
		changed := cloneManagedCatalogSnapshot(catalog)
		mutate(&changed)
		fingerprint, err := managedSnowflakeCatalogFingerprint(changed)
		if err != nil {
			t.Fatal(err)
		}
		if fingerprint == baseline {
			t.Errorf("catalog mutation %d did not change fingerprint", index)
		}
	}
}

func TestManagedSnowflakeCatalogPreservesPostgresDatetimePrecision(t *testing.T) {
	t.Parallel()
	cfg, catalog := managedCatalogFixture(t)
	cfg.schemaContract.Columns[1].Type = "timestamptz"
	catalog.target.columns["VALUE"] = managedColumnSnapshot{dataType: "TIMESTAMP_TZ(6)", datetimePrecision: 6, nullable: true}
	if err := validateManagedSnowflakeTargetSchema(cfg, catalog.target); err != nil {
		t.Fatal(err)
	}
	column := catalog.target.columns["VALUE"]
	column.datetimePrecision = 5
	catalog.target.columns["VALUE"] = column
	if err := validateManagedSnowflakeTargetSchema(cfg, catalog.target); err == nil || !strings.Contains(err.Error(), "microsecond precision") {
		t.Fatalf("narrow datetime error=%v", err)
	}
}

func cloneManagedCatalogSnapshot(snapshot managedCatalogSnapshot) managedCatalogSnapshot {
	cloneTable := func(table managedTableSnapshot) managedTableSnapshot {
		cloned := table
		cloned.columns = make(map[string]managedColumnSnapshot, len(table.columns))
		for name, column := range table.columns {
			cloned.columns[name] = column
		}
		cloned.constraints = make([]managedConstraintSnapshot, len(table.constraints))
		for index, constraint := range table.constraints {
			cloned.constraints[index] = constraint
			cloned.constraints[index].columns = append([]string(nil), constraint.columns...)
		}
		cloned.grants = make(map[string][]string, len(table.grants))
		for role, privileges := range table.grants {
			cloned.grants[role] = append([]string(nil), privileges...)
		}
		return cloned
	}
	return managedCatalogSnapshot{target: cloneTable(snapshot.target), receipts: cloneTable(snapshot.receipts)}
}

func managedCatalogFixture(t *testing.T) (managedConfig, managedCatalogSnapshot) {
	t.Helper()
	schema := managedTestSchema()
	cfg := managedConfig{
		profile: connector.ManagedProfilePostgresToSnowflakeSQLV1, flowID: "flow-1",
		account: "ACCOUNT", database: "DB", schema: "PUBLIC", table: "WIDGETS", receiptsTable: "WALLABY_RECEIPTS",
		ownerRole: "OWNER_ROLE", executionRole: "ROLE", warehouse: "WH", snowflakeVersion: "9.99.0",
		targetCreatedOn: "2026-01-01T00:00:00.000000000+00:00", receiptsCreatedOn: "2026-01-01T00:00:01.000000000+00:00",
		sourceSchema: "public", sourceTable: "widgets", schemaContract: schema,
		schemaContractHash: mustManagedSchemaHash(t, schema), destinationRevision: "snowflake-v1",
		maxTransactionRows: 10, maxTransactionBytes: 1 << 20, maxFragments: 4, maxOpenConnections: 2,
	}
	target := managedTableSnapshot{
		kind: "HYBRID TABLE", ownerRole: "OWNER_ROLE", createdOn: cfg.targetCreatedOn, comment: managedTableOwnershipComment(cfg, false),
		grants: map[string][]string{"OWNER_ROLE": {"OWNERSHIP"}, "ROLE": {"DELETE", "INSERT", "SELECT", "UPDATE"}},
		columns: map[string]managedColumnSnapshot{
			"ID":      {dataType: "NUMBER(38,0)", nullable: false},
			"VALUE":   {dataType: "VARCHAR", characterMaximumLength: 16 << 20, nullable: true},
			"PAYLOAD": {dataType: "BINARY", characterMaximumLength: 8 << 20, nullable: true},
		},
		constraints: []managedConstraintSnapshot{{name: "PK_WIDGETS", constraintType: "PRIMARY KEY", enforced: true, columns: []string{"ID"}}},
	}
	receipts := managedTableSnapshot{
		kind: "HYBRID TABLE", ownerRole: "OWNER_ROLE", createdOn: cfg.receiptsCreatedOn, comment: managedTableOwnershipComment(cfg, true),
		grants:  map[string][]string{"OWNER_ROLE": {"OWNERSHIP"}, "ROLE": {"INSERT", "SELECT"}},
		columns: managedExpectedReceiptColumns(),
		constraints: []managedConstraintSnapshot{
			{name: "PK_RECEIPTS", constraintType: "PRIMARY KEY", enforced: true, columns: []string{"FLOW_INCARNATION_ID", "DESTINATION_REVISION_ID", "SOURCE_LINEAGE_ID", "POSITION_ID"}},
			{name: "UQ_LOGICAL", constraintType: "UNIQUE", enforced: true, columns: []string{"FLOW_INCARNATION_ID", "DESTINATION_REVISION_ID", "LOGICAL_BATCH_ID"}},
			{name: "UQ_EXTERNAL", constraintType: "UNIQUE", enforced: true, columns: []string{"EXTERNAL_ID"}},
		},
	}
	return cfg, managedCatalogSnapshot{target: target, receipts: receipts}
}
