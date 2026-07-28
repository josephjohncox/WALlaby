package snowflake

import (
	"testing"
)

func validStagedCatalog(cfg stagedConfig) managedStagedCatalogSnapshot {
	return managedStagedCatalogSnapshot{
		stage: managedStageSnapshot{
			kind: "INTERNAL", ownerRole: cfg.ownerRole, createdOn: cfg.stageCreatedOn, comment: managedStagedOwnershipComment(cfg, "stage"),
			grants: map[string][]string{cfg.executionRole: {"READ", "WRITE"}, cfg.ownerRole: {"OWNERSHIP", "READ", "WRITE"}},
		},
		fileFormat: managedFileFormatSnapshot{
			formatType: "JSON", ownerRole: cfg.ownerRole, createdOn: cfg.fileFormatCreatedOn, comment: managedStagedOwnershipComment(cfg, "file_format"),
			grants: map[string][]string{cfg.executionRole: {"USAGE"}, cfg.ownerRole: {"OWNERSHIP", "USAGE"}},
		},
		target: managedTableSnapshot{
			kind: "TABLE", ownerRole: cfg.ownerRole, createdOn: cfg.targetCreatedOn, comment: managedStagedOwnershipComment(cfg, "target"),
			columns: stagedExpectedTargetColumns(),
			grants:  map[string][]string{cfg.executionRole: {"INSERT", "SELECT"}, cfg.ownerRole: {"OWNERSHIP"}},
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

func TestValidateManagedStagedCatalogAcceptsProvisionedObjects(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
	if err := validateManagedStagedCatalog(cfg, validStagedCatalog(cfg)); err != nil {
		t.Fatalf("valid staged catalog rejected: %v", err)
	}
}

func TestValidateManagedStagedCatalogRejectsUnsafeShapes(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
	cases := map[string]func(*managedStagedCatalogSnapshot){
		"task present":         func(c *managedStagedCatalogSnapshot) { c.taskCount = 1 },
		"external stage kind":  func(c *managedStagedCatalogSnapshot) { c.stage.kind = "EXTERNAL" },
		"stage wrong owner":    func(c *managedStagedCatalogSnapshot) { c.stage.ownerRole = "INTRUDER" },
		"stage extra writer":   func(c *managedStagedCatalogSnapshot) { c.stage.grants["OTHER"] = []string{"WRITE"} },
		"file format not json": func(c *managedStagedCatalogSnapshot) { c.fileFormat.formatType = "CSV" },
		"target is hybrid":     func(c *managedStagedCatalogSnapshot) { c.target.kind = "HYBRID TABLE" },
		"target extra writer":  func(c *managedStagedCatalogSnapshot) { c.target.grants["OTHER"] = []string{"INSERT"} },
		"target column drift":  func(c *managedStagedCatalogSnapshot) { delete(c.target.columns, "RECORD_HASH") },
		"receipts not hybrid":  func(c *managedStagedCatalogSnapshot) { c.receipts.kind = "TABLE" },
		"receipts missing pk":  func(c *managedStagedCatalogSnapshot) { c.receipts.constraints = c.receipts.constraints[1:] },
		"pipe without ai":      func(c *managedStagedCatalogSnapshot) { c.pipe = managedPipeSnapshot{present: true} },
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
	catalog.pipe = validStagedAutoIngestPipe(cfg)
	if err := validateManagedStagedCatalog(cfg, catalog); err != nil {
		t.Fatalf("valid auto-ingest catalog rejected: %v", err)
	}
}

func validStagedAutoIngestPipe(cfg stagedConfig) managedPipeSnapshot {
	return managedPipeSnapshot{
		present: true, autoIngest: true, ownerRole: cfg.ownerRole, createdOn: cfg.pipeCreatedOn,
		onError: "ABORT_STATEMENT", force: "FALSE", matchByColumnName: "CASE_SENSITIVE",
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
	}
	for name, mutate := range cases {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pipe := validStagedAutoIngestPipe(cfg)
			mutate(&pipe)
			if err := validateManagedStagedPipe(cfg, pipe); err == nil {
				t.Fatalf("auto-ingest pipe validation accepted an unsafe COPY option (%s)", name)
			}
		})
	}
	if err := validateManagedStagedPipe(cfg, validStagedAutoIngestPipe(cfg)); err != nil {
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
}
