package snowflake

import (
	"context"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func validStagedCatalog(cfg stagedConfig) managedStagedCatalogSnapshot {
	return managedStagedCatalogSnapshot{
		stage: managedStageSnapshot{
			kind: "INTERNAL", ownerRole: cfg.ownerRole, createdOn: cfg.stageCreatedOn, comment: managedStagedOwnershipComment(cfg, "stage"),
			grants: map[string][]string{cfg.executionRole: {"READ", "WRITE"}, cfg.ownerRole: {"OWNERSHIP", "READ", "WRITE"}},
		},
		fileFormat: managedFileFormatSnapshot{
			formatType: "JSON", ownerRole: cfg.ownerRole, createdOn: cfg.fileFormatCreatedOn, comment: managedStagedOwnershipComment(cfg, "file_format"),
			definition: `CREATE FILE FORMAT "WALLABY_FORMAT" TYPE = JSON MULTI_LINE = FALSE`,
			properties: managedStagedJSONFileFormatProperties(),
			grants:     map[string][]string{cfg.executionRole: {"USAGE"}, cfg.ownerRole: {"OWNERSHIP", "USAGE"}},
		},
		target: managedTableSnapshot{
			kind: "TABLE", ownerRole: cfg.ownerRole, createdOn: cfg.targetCreatedOn, comment: managedStagedOwnershipComment(cfg, "target"),
			columns: stagedExpectedTargetColumns(),
			grants:  map[string][]string{cfg.executionRole: {"INSERT", "SELECT"}, cfg.ownerRole: {"OWNERSHIP"}},
		},
		landing: managedTableSnapshot{
			kind: "TABLE", ownerRole: cfg.ownerRole, createdOn: cfg.landingCreatedOn, comment: managedStagedOwnershipComment(cfg, "landing"),
			columns: stagedExpectedTargetColumns(),
			grants:  map[string][]string{cfg.executionRole: {"DELETE", "INSERT", "SELECT"}, cfg.ownerRole: {"OWNERSHIP"}},
		},
		authority: managedTableSnapshot{
			kind: "HYBRID TABLE", ownerRole: cfg.ownerRole, createdOn: cfg.authorityCreatedOn, comment: managedStagedOwnershipComment(cfg, "authority"),
			columns:     stagedExpectedAuthorityColumns(),
			grants:      map[string][]string{cfg.executionRole: {"DELETE", "INSERT", "SELECT", "UPDATE"}, cfg.ownerRole: {"OWNERSHIP"}},
			constraints: []managedConstraintSnapshot{{name: "PK_AUTHORITY", constraintType: "PRIMARY KEY", enforced: true, columns: []string{"AUTHORITY_KIND", "DESTINATION_REVISION_ID", "AUTHORITY_ID"}}},
		},
		targetManifest: managedTableSnapshot{
			kind: "HYBRID TABLE", ownerRole: cfg.ownerRole, createdOn: cfg.targetManifestCreatedOn, comment: managedStagedOwnershipComment(cfg, "target_manifest"),
			columns: stagedExpectedTargetManifestColumns(),
			grants:  map[string][]string{cfg.executionRole: {"INSERT", "SELECT"}, cfg.ownerRole: {"OWNERSHIP"}},
			constraints: []managedConstraintSnapshot{
				{name: "PK_MANIFEST", constraintType: "PRIMARY KEY", enforced: true, columns: []string{"DESTINATION_REVISION_ID", "LOGICAL_BATCH_ID"}},
				{name: "UQ_MANIFEST", constraintType: "UNIQUE", enforced: true, columns: []string{"MANIFEST_HASH"}},
			},
		},
		receipts: managedTableSnapshot{
			kind: "HYBRID TABLE", ownerRole: cfg.ownerRole, createdOn: cfg.receiptsCreatedOn, comment: managedStagedOwnershipComment(cfg, "receipts"),
			columns: stagedExpectedReceiptColumns(),
			grants:  map[string][]string{cfg.executionRole: {"INSERT", "SELECT"}, cfg.ownerRole: {"OWNERSHIP"}},
			constraints: []managedConstraintSnapshot{
				{name: "PK", constraintType: "PRIMARY KEY", enforced: true, columns: []string{"RECEIPT_KIND", "FLOW_INCARNATION_ID", "DESTINATION_REVISION_ID", "LOGICAL_BATCH_ID"}},
				{name: "UQ_EXTERNAL", constraintType: "UNIQUE", enforced: true, columns: []string{"EXTERNAL_ID"}},
			},
		},
	}
}

func TestLoadStagedFileFormatPropertiesCapturesCompleteEffectiveShape(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("new sql mock: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows := sqlmock.NewRows([]string{"property", "property_type", "property_value", "property_default"})
	for name, property := range managedStagedJSONFileFormatProperties() {
		rows.AddRow(name, property.propertyType, property.propertyValue, property.propertyDefault)
	}
	mock.ExpectQuery(regexp.QuoteMeta(`DESCRIBE FILE FORMAT "WALLABY_DB"."WALLABY_SCHEMA"."WALLABY_FORMAT"`)).WillReturnRows(rows)
	properties, err := loadStagedFileFormatProperties(context.Background(), db, `"WALLABY_DB"."WALLABY_SCHEMA"."WALLABY_FORMAT"`)
	if err != nil {
		t.Fatalf("load file format properties: %v", err)
	}
	if len(properties) != len(managedStagedJSONFileFormatProperties()) {
		t.Fatalf("loaded properties=%d", len(properties))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("file format property query: %v", err)
	}
}

func TestLoadStagedFileFormatPropertiesRejectsDuplicateAndWideOutput(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		rows *sqlmock.Rows
	}{
		{name: "duplicate", rows: sqlmock.NewRows([]string{"property", "property_type", "property_value", "property_default"}).
			AddRow("TYPE", "String", "JSON", "CSV").AddRow("type", "String", "JSON", "CSV")},
		{name: "wide", rows: sqlmock.NewRows([]string{"property", "property_type", "property_value", "property_default", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17"})},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			mock.ExpectQuery("DESCRIBE FILE FORMAT").WillReturnRows(test.rows)
			if _, err := loadStagedFileFormatProperties(context.Background(), db, `"DB"."SCHEMA"."FORMAT"`); err == nil {
				t.Fatalf("accepted %s DESCRIBE output", test.name)
			}
		})
	}
}

func TestValidateManagedStagedCatalogAcceptsProvisionedObjects(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
	catalog := validStagedCatalog(cfg)
	if err := validateManagedStagedCatalog(cfg, catalog); err != nil {
		t.Fatalf("valid staged catalog rejected: %v", err)
	}
	catalog.fileFormat.properties["MULTI_LINE"] = managedFileFormatPropertySnapshot{propertyType: "Boolean", propertyValue: "FALSE"}
	if err := validateManagedStagedCatalog(cfg, catalog); err != nil {
		t.Fatalf("valid staged catalog with described MULTI_LINE rejected: %v", err)
	}
}

func TestValidateManagedStagedCatalogRejectsUnsafeShapes(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
	cases := map[string]func(*managedStagedCatalogSnapshot){
		"task present":           func(c *managedStagedCatalogSnapshot) { c.taskCount = 1 },
		"external stage kind":    func(c *managedStagedCatalogSnapshot) { c.stage.kind = "EXTERNAL" },
		"stage wrong owner":      func(c *managedStagedCatalogSnapshot) { c.stage.ownerRole = "INTRUDER" },
		"stage extra writer":     func(c *managedStagedCatalogSnapshot) { c.stage.grants["OTHER"] = []string{"WRITE"} },
		"file format not json":   func(c *managedStagedCatalogSnapshot) { c.fileFormat.formatType = "CSV" },
		"multiline not explicit": func(c *managedStagedCatalogSnapshot) { c.fileFormat.definition = `CREATE FILE FORMAT X TYPE=JSON` },
		"multiline enabled": func(c *managedStagedCatalogSnapshot) {
			c.fileFormat.definition = `CREATE FILE FORMAT X TYPE=JSON MULTI_LINE=TRUE`
		},
		"described multiline enabled": func(c *managedStagedCatalogSnapshot) {
			c.fileFormat.properties["MULTI_LINE"] = managedFileFormatPropertySnapshot{propertyType: "Boolean", propertyValue: "TRUE"}
		},
		"target is hybrid":    func(c *managedStagedCatalogSnapshot) { c.target.kind = "HYBRID TABLE" },
		"target extra writer": func(c *managedStagedCatalogSnapshot) { c.target.grants["OTHER"] = []string{"INSERT"} },
		"target column drift": func(c *managedStagedCatalogSnapshot) { delete(c.target.columns, "RECORD_HASH") },
		"receipts not hybrid": func(c *managedStagedCatalogSnapshot) { c.receipts.kind = "TABLE" },
		"receipts missing pk": func(c *managedStagedCatalogSnapshot) { c.receipts.constraints = c.receipts.constraints[1:] },
		"pipe without ai":     func(c *managedStagedCatalogSnapshot) { c.pipe = managedPipeSnapshot{present: true} },
	}
	for name, mutate := range cases {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			catalog := validStagedCatalog(cfg)
			mutate(&catalog)
			if err := validateManagedStagedCatalog(cfg, catalog); err == nil {
				t.Fatalf("staged catalog validation accepted an unsafe shape (%s)", name)
			}
		})
	}
}

func TestStagedFileFormatDefinitionOption(t *testing.T) {
	t.Parallel()
	for _, definition := range []string{
		"CREATE FILE FORMAT X TYPE=JSON MULTI_LINE=FALSE;",
		"CREATE FILE FORMAT X TYPE = JSON, MULTI_LINE = FALSE, ALLOW_DUPLICATE = FALSE",
	} {
		if !stagedFileFormatDefinitionOption(definition, "MULTI_LINE", "FALSE") {
			t.Fatalf("did not find explicit false option in %q", definition)
		}
	}
	for _, definition := range []string{
		"CREATE FILE FORMAT X TYPE=JSON",
		"CREATE FILE FORMAT X TYPE=JSON MULTI_LINE=TRUE",
		"CREATE FILE FORMAT X TYPE=JSON NOT_MULTI_LINE=FALSE",
		"CREATE FILE FORMAT X TYPE=JSON COMMENT='MULTI_LINE=FALSE '",
		"CREATE FILE FORMAT X TYPE=JSON COMMENT='escaped '' MULTI_LINE=FALSE '",
		"CREATE FILE FORMAT X TYPE=JSON /* MULTI_LINE=FALSE */",
		"CREATE FILE FORMAT X TYPE=JSON -- MULTI_LINE=FALSE\n",
	} {
		if stagedFileFormatDefinitionOption(definition, "MULTI_LINE", "FALSE") {
			t.Fatalf("accepted absent or wrong option in %q", definition)
		}
	}
}

func TestValidateManagedStagedCatalogRejectsEveryJSONFileFormatOptionDrift(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
	for name := range managedStagedJSONFileFormatProperties() {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			catalog := validStagedCatalog(cfg)
			property := catalog.fileFormat.properties[name]
			property.propertyValue += "_DRIFT"
			catalog.fileFormat.properties[name] = property
			if err := validateManagedStagedCatalog(cfg, catalog); err == nil {
				t.Fatalf("accepted drift in JSON file format property %s", name)
			}
		})
	}
	missing := validStagedCatalog(cfg)
	delete(missing.fileFormat.properties, "ALLOW_DUPLICATE")
	if err := validateManagedStagedCatalog(cfg, missing); err == nil {
		t.Fatal("accepted a missing JSON file format property")
	}
	extra := validStagedCatalog(cfg)
	extra.fileFormat.properties["FUTURE_BEHAVIOR"] = managedFileFormatPropertySnapshot{propertyType: "Boolean", propertyValue: "FALSE"}
	if err := validateManagedStagedCatalog(cfg, extra); err == nil {
		t.Fatal("accepted an unadmitted JSON file format property")
	}
}

func TestValidateManagedStagedCatalogAutoIngestRequiresPipe(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
	cfg.autoIngest = true
	cfg.pipe = "WALLABY_PIPE"
	cfg.pipeCreatedOn = cfg.stageCreatedOn
	catalog := validStagedCatalog(cfg)
	if err := validateManagedStagedCatalog(cfg, catalog); err == nil {
		t.Fatal("auto-ingest catalog without a pipe was accepted")
	}
	catalog.pipe = validStagedAutoIngestPipe(t, cfg)
	catalog.unexpectedPipeCount = 1
	if err := validateManagedStagedCatalog(cfg, catalog); err != nil {
		t.Fatalf("valid auto-ingest catalog rejected: %v", err)
	}
}

// stagedInlinePipeDefinition renders a pipe DEFINITION that inlines exactly the
// JSON parsing options the synchronous COPY inlines, which is what admission
// requires so an ALTER FILE FORMAT cannot change auto-ingest parsing.
func stagedInlinePipeDefinition(t testing.TB, cfg stagedConfig) string {
	t.Helper()
	options, err := stagedInlineJSONFormatOptions()
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(options))
	for name := range options {
		names = append(names, name)
	}
	sort.Strings(names)
	rendered := make([]string, 0, len(names))
	for _, name := range names {
		rendered = append(rendered, name+" = "+options[name])
	}
	return "COPY INTO " + managedSnowflakeStagedQualifiedTable(cfg, cfg.landingTable) + " FROM @" + managedSnowflakeStagedQualified(cfg, cfg.stage) + "/wallaby_staged_append_v1/ FILE_FORMAT = (" + strings.Join(rendered, " ") +
		") MATCH_BY_COLUMN_NAME = CASE_SENSITIVE ON_ERROR = ABORT_STATEMENT FORCE = FALSE"
}

func validStagedAutoIngestPipe(t testing.TB, cfg stagedConfig) managedPipeSnapshot {
	t.Helper()
	return managedPipeSnapshot{
		present: true, autoIngest: true, ownerRole: cfg.ownerRole, createdOn: cfg.pipeCreatedOn,
		definition: stagedInlinePipeDefinition(t, cfg),
		onError:    "ABORT_STATEMENT", force: "FALSE", matchByColumnName: "CASE_SENSITIVE",
		comment: managedStagedOwnershipComment(cfg, "pipe"),
		grants:  map[string][]string{cfg.executionRole: {"MONITOR", "OPERATE"}, cfg.ownerRole: {"OWNERSHIP"}},
	}
}

func TestValidateManagedStagedPipeRejectsUnsafeCopyOptions(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
	cfg.autoIngest = true
	cfg.pipe = "WALLABY_PIPE"
	cfg.pipeCreatedOn = cfg.stageCreatedOn
	cases := map[string]func(*managedPipeSnapshot){
		"skip file on_error":   func(p *managedPipeSnapshot) { p.onError = "SKIP_FILE" },
		"continue on_error":    func(p *managedPipeSnapshot) { p.onError = "CONTINUE" },
		"absent on_error":      func(p *managedPipeSnapshot) { p.onError = "" },
		"force true":           func(p *managedPipeSnapshot) { p.force = "TRUE" },
		"loose column mapping": func(p *managedPipeSnapshot) { p.matchByColumnName = "CASE_INSENSITIVE" },
		"absent column match":  func(p *managedPipeSnapshot) { p.matchByColumnName = "" },
		"named file format": func(p *managedPipeSnapshot) {
			p.definition = "COPY INTO T FROM @S FILE_FORMAT = (FORMAT_NAME = DB.PUBLIC.FF) ON_ERROR = ABORT_STATEMENT"
		},
		"multiline parsing": func(p *managedPipeSnapshot) {
			p.definition = strings.ReplaceAll(p.definition, "MULTI_LINE = FALSE", "MULTI_LINE = TRUE")
		},
		"outer array stripping": func(p *managedPipeSnapshot) {
			p.definition = strings.ReplaceAll(p.definition, "STRIP_OUTER_ARRAY = FALSE", "STRIP_OUTER_ARRAY = TRUE")
		},
		"format name hidden in comment": func(p *managedPipeSnapshot) {
			p.definition = "COPY INTO T FROM @S FILE_FORMAT = (FORMAT_NAME = DB.PUBLIC.FF) /* " +
				stagedInlinePipeDefinition(t, cfg) + " */"
		},
	}
	for name, mutate := range cases {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pipe := validStagedAutoIngestPipe(t, cfg)
			mutate(&pipe)
			if err := validateManagedStagedPipe(cfg, pipe); err == nil {
				t.Fatalf("auto-ingest pipe validation accepted an unsafe COPY option (%s)", name)
			}
		})
	}
	if err := validateManagedStagedPipe(cfg, validStagedAutoIngestPipe(t, cfg)); err != nil {
		t.Fatalf("fail-closed auto-ingest pipe rejected: %v", err)
	}
}

func TestStagedPipeCopyOptionParsesDefinition(t *testing.T) {
	t.Parallel()
	definition := "COPY INTO db.public.tbl FROM @db.public.stg FILE_FORMAT = (FORMAT_NAME = db.public.ff) " +
		"MATCH_BY_COLUMN_NAME = CASE_SENSITIVE ON_ERROR = ABORT_STATEMENT FORCE = FALSE"
	cases := map[string]string{"ON_ERROR": "ABORT_STATEMENT", "FORCE": "FALSE", "MATCH_BY_COLUMN_NAME": "CASE_SENSITIVE", "PURGE": ""}
	for option, want := range cases {
		if got := stagedPipeCopyOption(definition, option); got != want {
			t.Fatalf("stagedPipeCopyOption(%s)=%q, want %q", option, got, want)
		}
	}
	if got := stagedPipeCopyOption("... ON_ERROR = 'ABORT_STATEMENT' ...", "ON_ERROR"); got != "ABORT_STATEMENT" {
		t.Fatalf("quoted ON_ERROR=%q, want ABORT_STATEMENT", got)
	}
}

func TestManagedStagedCatalogFingerprintIsDeterministicAndSensitive(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
	catalog := validStagedCatalog(cfg)
	first, err := managedStagedCatalogFingerprint(catalog)
	if err != nil {
		t.Fatal(err)
	}
	second, err := managedStagedCatalogFingerprint(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || len(first) != 64 {
		t.Fatalf("staged catalog fingerprint is not a stable 64-char digest: %q vs %q", first, second)
	}
	drifted := validStagedCatalog(cfg)
	drifted.target.comment = "changed"
	changed, err := managedStagedCatalogFingerprint(drifted)
	if err != nil {
		t.Fatal(err)
	}
	if changed == first {
		t.Fatal("staged catalog fingerprint did not change when the target comment drifted")
	}
	formatDrift := validStagedCatalog(cfg)
	property := formatDrift.fileFormat.properties["ALLOW_DUPLICATE"]
	property.propertyDefault = "TRUE"
	formatDrift.fileFormat.properties["ALLOW_DUPLICATE"] = property
	changed, err = managedStagedCatalogFingerprint(formatDrift)
	if err != nil {
		t.Fatal(err)
	}
	if changed == first {
		t.Fatal("staged catalog fingerprint did not change when a JSON file format default drifted")
	}
	cfg.autoIngest = true
	cfg.pipe = "WALLABY_PIPE"
	cfg.pipeCreatedOn = cfg.stageCreatedOn
	pipeCatalog := validStagedCatalog(cfg)
	pipeCatalog.pipe = validStagedAutoIngestPipe(t, cfg)
	pipeBaseline, err := managedStagedCatalogFingerprint(pipeCatalog)
	if err != nil {
		t.Fatal(err)
	}
	pipeCatalog.pipe.definition += " /* changed */"
	pipeChanged, err := managedStagedCatalogFingerprint(pipeCatalog)
	if err != nil {
		t.Fatal(err)
	}
	if pipeBaseline == pipeChanged {
		t.Fatal("staged catalog fingerprint did not bind the full pipe definition")
	}
}

func TestValidateManagedStagedCatalogProvesNoPipeForSynchronousCopy(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
	catalog := validStagedCatalog(cfg)
	catalog.unexpectedPipeCount = 1
	if err := validateManagedStagedCatalog(cfg, catalog); err == nil {
		t.Fatal("synchronous staged profile accepted an observed pipe")
	}
}
