package snowflake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/snowflakedb/gosnowflake"
)

func stagedFixture(t *testing.T) (stagedConfig, connector.DeliveryIntent, connector.SourceTransaction) {
	t.Helper()
	cfg := stagedTestConfig(t)
	transaction := managedTestTransaction(cfg.schemaContract)
	intent := stagedTestIntent(t, cfg, transaction)
	return cfg, intent, transaction
}

func stagedPlanFor(t *testing.T, cfg stagedConfig, intent connector.DeliveryIntent, transaction connector.SourceTransaction) managedStagedPlan {
	t.Helper()
	plan, err := planManagedStagedTransaction(cfg, intent, transaction)
	if err != nil {
		t.Fatalf("plan staged transaction: %v", err)
	}
	return plan
}

func assertStagedApplied(t *testing.T, proto *fakeStageProtocol, intent connector.DeliveryIntent, evidence connector.DeliveryEvidence, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("staged apply error=%v", err)
	}
	if evidence.ContentHash != intent.ContentHash || evidence.ExternalID == "" {
		t.Fatalf("staged evidence=%+v, want content hash %q and a non-empty external id", evidence, intent.ContentHash)
	}
	loadReceipts := 0
	for _, receipt := range proto.receipts {
		if receipt.kind == stagedReceiptKindLoad {
			loadReceipts++
		}
	}
	if loadReceipts != 1 {
		t.Fatalf("staged apply left %d load receipts, want exactly 1", loadReceipts)
	}
}

func TestStagedDriverHappyPathApply(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	proto := newFakeStageProtocol()
	driver := newStagedTestDriver(cfg, proto)
	evidence, err := driver.apply(context.Background(), intent, transaction)
	assertStagedApplied(t, proto, intent, evidence, err)
	if proto.putCalls != 1 || proto.copyCalls != 1 {
		t.Fatalf("put/copy calls=%d/%d, want 1/1", proto.putCalls, proto.copyCalls)
	}
}

func TestStagedDriverReplayIsIdempotent(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	proto := newFakeStageProtocol()
	driver := newStagedTestDriver(cfg, proto)
	first, err := driver.apply(context.Background(), intent, transaction)
	assertStagedApplied(t, proto, intent, first, err)
	second, err := driver.apply(context.Background(), intent, transaction)
	assertStagedApplied(t, proto, intent, second, err)
	if first.ExternalID != second.ExternalID {
		t.Fatalf("replay external id changed: %q vs %q", first.ExternalID, second.ExternalID)
	}
}

func TestStagedDriverPutUncertaintyReconcilesInOnePass(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	proto := newFakeStageProtocol()
	// The PUT durably stages the object, then the response is lost.
	proto.putStagesThenError = errors.New("connection reset after upload")
	driver := newStagedTestDriver(cfg, proto)
	evidence, err := driver.apply(context.Background(), intent, transaction)
	assertStagedApplied(t, proto, intent, evidence, err)
}

func TestStagedDriverPutHardFailIsIndeterminateThenRecovers(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	proto := newFakeStageProtocol()
	proto.putHardFail = errors.New("network unreachable before upload")
	driver := newStagedTestDriver(cfg, proto)
	if _, err := driver.apply(context.Background(), intent, transaction); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("hard PUT failure error=%v, want indeterminate", err)
	}
	evidence, err := driver.apply(context.Background(), intent, transaction)
	assertStagedApplied(t, proto, intent, evidence, err)
}

func TestStagedDriverWrongByteCollisionFailsClosed(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	plan := stagedPlanFor(t, cfg, intent, transaction)
	proto := newFakeStageProtocol()
	proto.stageRaw(plan.copyPlan.stageRef, plan.identity.relativePath, []byte("not the planned bytes\n"))
	driver := newStagedTestDriver(cfg, proto)
	_, err := driver.apply(context.Background(), intent, transaction)
	if !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("wrong-byte collision error=%v, want conflict", err)
	}
	if proto.copyCalls != 0 {
		t.Fatalf("wrong-byte collision must never load: copyCalls=%d", proto.copyCalls)
	}
	if len(proto.receipts) != 0 {
		t.Fatalf("wrong-byte collision must never write a receipt")
	}
}

func TestValidateStagedObjectReferenceIsOneSharedAllowlist(t *testing.T) {
	t.Parallel()
	const stageRef = `"WALLABY_DB"."WALLABY_SCHEMA"."WALLABY_STAGE"`
	const path = "wallaby_staged_append_v1/inc_0011223344556677/rev_0011223344556677/batch_0011223344556677/0123456789abcdef-" +
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef.ndjson"
	if err := validateStagedObjectReference(stageRef, path); err != nil {
		t.Fatalf("rejected the production stage reference and path: %v", err)
	}
	if err := validateStagedObjectReference(`"DB$1"."SCHEMA$2"."STAGE$3"`, path); err != nil {
		t.Fatalf("rejected admitted dollar identifiers: %v", err)
	}
	for name, test := range map[string]struct{ stageRef, path string }{
		"unquoted stage":     {stageRef: "WALLABY_DB.WALLABY_SCHEMA.WALLABY_STAGE", path: path},
		"two part stage":     {stageRef: `"WALLABY_DB"."WALLABY_STAGE"`, path: path},
		"stage injection":    {stageRef: `"A"."B"."C"; DROP TABLE X`, path: path},
		"quoted injection":   {stageRef: `"A"."B"."C$"";DROP TABLE X"`, path: path},
		"comment injection":  {stageRef: `"A"."B"."C$1"--`, path: path},
		"leading dollar":     {stageRef: `"$A"."B"."C"`, path: path},
		"leading digit":      {stageRef: `"1A"."B"."C"`, path: path},
		"embedded space":     {stageRef: `"A"."B B"."C"`, path: path},
		"path traversal":     {stageRef: stageRef, path: "a/../../etc/passwd"},
		"path whitespace":    {stageRef: stageRef, path: "a b/c.ndjson"},
		"path quote":         {stageRef: stageRef, path: `a'/c.ndjson`},
		"path leading slash": {stageRef: stageRef, path: "/a/c.ndjson"},
		"path empty":         {stageRef: stageRef, path: ""},
	} {
		if err := validateStagedObjectReference(test.stageRef, test.path); err == nil {
			t.Fatalf("accepted an unsafe stage reference or path (%s)", name)
		}
	}
	protocol := &sqlStageProtocol{}
	if _, err := protocol.StatObject(context.Background(), "bad", path); err == nil {
		t.Fatal("StatObject accepted an unvalidated stage reference")
	}
	if err := protocol.PutObject(context.Background(), "bad", path, nil, ""); err == nil {
		t.Fatal("PutObject accepted an unvalidated stage reference")
	}
	if _, err := protocol.GetObject(context.Background(), "bad", path, 1); err == nil {
		t.Fatal("GetObject accepted an unvalidated stage reference")
	}
}

func TestStagedDollarIdentifiersOpenInitializeAndObjectProtocol(t *testing.T) {
	t.Parallel()
	dsn := managedSnowflakeTestDSN(t, func(config *gosnowflake.Config) {
		config.Database = "DB$1"
		config.Schema = "SCHEMA$2"
	})
	_, options := stagedValidOptions(t)
	options["dsn"] = dsn
	options["managed_database"] = "DB$1"
	options["managed_schema"] = "SCHEMA$2"
	options["managed_stage"] = "STAGE$3"
	options["managed_table"] = "TABLE$4"
	options["managed_receipts_table"] = "RECEIPTS$5"
	options["managed_file_format"] = "FORMAT$6"
	cfg, err := stagedConfigFromSpec(dsn, connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: options})
	if err != nil {
		t.Fatalf("Open admission rejected dollar identifiers: %v", err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	destination := &Destination{
		db: db, managedProfile: connector.ManagedProfilePostgresToSnowflakeStagedAppendV1,
		stagedConfig: cfg, stagedCatalogFingerprint: "catalog-fingerprint",
	}
	if err := destination.InitializeManagedDelivery(context.Background()); err != nil {
		t.Fatalf("Initialize after admitted Open state: %v", err)
	}
	transaction := managedTestTransaction(cfg.schemaContract)
	intent := stagedTestIntent(t, cfg, transaction)
	plan := stagedPlanFor(t, cfg, intent, transaction)
	stageRef := `"DB$1"."SCHEMA$2"."STAGE$3"`
	if plan.copyPlan.stageRef != stageRef {
		t.Fatalf("planned stage reference=%q, want %q", plan.copyPlan.stageRef, stageRef)
	}
	protocol := newSQLStageProtocol(db)
	mock.ExpectQuery("LIST @").WillReturnRows(sqlmock.NewRows([]string{"name", "size", "md5", "last_modified"}).
		AddRow("stage/"+plan.identity.relativePath, len(plan.fileBytes), plan.fileMD5, "now"))
	if stat, err := protocol.StatObject(context.Background(), stageRef, plan.identity.relativePath); err != nil || !stat.present {
		t.Fatalf("LIST dollar stage stat=%+v err=%v", stat, err)
	}
	mock.ExpectExec("GET @").WillReturnResult(sqlmock.NewResult(0, 1))
	if _, err := protocol.GetObject(context.Background(), stageRef, plan.identity.relativePath, len(plan.fileBytes)); err != nil {
		t.Fatalf("GET dollar stage: %v", err)
	}
	mock.ExpectQuery("LIST @").WillReturnRows(sqlmock.NewRows([]string{"name", "size", "md5", "last_modified"}))
	mock.ExpectExec("PUT file:///").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := protocol.PutObject(context.Background(), stageRef, plan.identity.relativePath, plan.fileBytes, plan.fileMD5); err != nil {
		t.Fatalf("PUT dollar stage: %v", err)
	}
	mock.ExpectQuery("COPY INTO").WillReturnRows(sqlmock.NewRows([]string{"status", "rows_loaded", "errors_seen", "first_error"}).AddRow("LOADED", plan.rowCount, 0, ""))
	if result, err := protocol.Copy(context.Background(), plan.copyPlan); err != nil || !result.present || result.rowsLoaded != plan.rowCount {
		t.Fatalf("COPY dollar stage result=%+v err=%v", result, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStatObjectRejectsPrefixSiblingBeforeAnyDownload(t *testing.T) {
	t.Parallel()
	const stageRef = `"WALLABY_DB"."WALLABY_SCHEMA"."WALLABY_STAGE"`
	const path = "wallaby_staged_append_v1/inc_00/rev_00/batch_00/0123456789abcdef-abc.ndjson"
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery("LIST @").WillReturnRows(sqlmock.NewRows([]string{"name", "size", "md5", "last_modified"}).
		AddRow("stage/"+path, 10, "abc", "now").
		AddRow("stage/"+path+".bak", 10, "def", "now"))
	protocol := &sqlStageProtocol{db: db}
	if _, err := protocol.StatObject(context.Background(), stageRef, path); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("prefix sibling error=%v, want conflict before any GET", err)
	}
}

func TestBoundedStageObjectWriterRejectsOversizePlaintext(t *testing.T) {
	t.Parallel()
	writer := &boundedStageObjectWriter{limit: 3}
	written, err := writer.Write([]byte("four"))
	if err == nil || written != 3 || writer.buffer.String() != "fou" {
		t.Fatalf("bounded writer=(written:%d bytes:%q err:%v), want 3/\"fou\"/error", written, writer.buffer.String(), err)
	}
}

func TestStagedDriverRequiresBoundedPlaintextByteEvidence(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	plan := stagedPlanFor(t, cfg, intent, transaction)

	t.Run("LIST checksum absent but exact GET proves bytes", func(t *testing.T) {
		proto := newFakeStageProtocol()
		proto.statOmitsMD5 = true
		evidence, err := newStagedTestDriver(cfg, proto).apply(context.Background(), intent, transaction)
		assertStagedApplied(t, proto, intent, evidence, err)
		if proto.getCalls != 1 {
			t.Fatalf("GET calls=%d, want 1", proto.getCalls)
		}
	})

	t.Run("LIST size exceeds bounded download", func(t *testing.T) {
		proto := newFakeStageProtocol()
		oversize := int64(len(plan.fileBytes) + maxStagedEncryptionOverheadBytes + 1)
		proto.statSizeOverride = &oversize
		_, err := newStagedTestDriver(cfg, proto).apply(context.Background(), intent, transaction)
		if !errors.Is(err, connector.ErrDeliveryConflict) {
			t.Fatalf("oversized LIST error=%v, want conflict", err)
		}
		if proto.getCalls != 0 || proto.copyCalls != 0 || proto.insertCalls != 0 {
			t.Fatalf("oversized object reached GET/COPY/receipt: %d/%d/%d", proto.getCalls, proto.copyCalls, proto.insertCalls)
		}
	})

	t.Run("LIST size missing", func(t *testing.T) {
		proto := newFakeStageProtocol()
		var missing int64
		proto.statSizeOverride = &missing
		_, err := newStagedTestDriver(cfg, proto).apply(context.Background(), intent, transaction)
		if !errors.Is(err, connector.ErrDeliveryIndeterminate) {
			t.Fatalf("missing LIST size error=%v, want indeterminate", err)
		}
		if proto.getCalls != 0 || proto.copyCalls != 0 || proto.insertCalls != 0 {
			t.Fatalf("unbounded object reached GET/COPY/receipt: %d/%d/%d", proto.getCalls, proto.copyCalls, proto.insertCalls)
		}
	})

	t.Run("GET unavailable", func(t *testing.T) {
		proto := newFakeStageProtocol()
		proto.getHardFail = errors.New("GET unavailable")
		_, err := newStagedTestDriver(cfg, proto).apply(context.Background(), intent, transaction)
		if !errors.Is(err, connector.ErrDeliveryIndeterminate) {
			t.Fatalf("GET unavailable error=%v, want indeterminate", err)
		}
		if proto.copyCalls != 0 || proto.insertCalls != 0 {
			t.Fatalf("unproved bytes reached copy/receipt: %d/%d", proto.copyCalls, proto.insertCalls)
		}
	})

	t.Run("GET plaintext is longer than the plan", func(t *testing.T) {
		proto := newFakeStageProtocol()
		proto.getOversize = true
		_, err := newStagedTestDriver(cfg, proto).apply(context.Background(), intent, transaction)
		if !errors.Is(err, connector.ErrDeliveryConflict) {
			t.Fatalf("oversize GET error=%v, want conflict rather than an unresolvable retry", err)
		}
		if proto.copyCalls != 0 || proto.insertCalls != 0 {
			t.Fatalf("oversize plaintext reached copy/receipt: %d/%d", proto.copyCalls, proto.insertCalls)
		}
	})

	t.Run("GET plaintext differs despite matching LIST", func(t *testing.T) {
		proto := newFakeStageProtocol()
		proto.getCorrupt = true
		_, err := newStagedTestDriver(cfg, proto).apply(context.Background(), intent, transaction)
		if !errors.Is(err, connector.ErrDeliveryConflict) {
			t.Fatalf("corrupt GET error=%v, want conflict", err)
		}
		if proto.copyCalls != 0 || proto.insertCalls != 0 {
			t.Fatalf("wrong GET bytes reached copy/receipt: %d/%d", proto.copyCalls, proto.insertCalls)
		}
	})
}

func TestStagedDriverEmptyTransactionStillProvesBytes(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
	transaction := connector.SourceTransaction{
		SourceLineageID: "lineage-1", TransactionID: 7,
		BeginLSN: "0/10", CommitLSN: "0/30", EndLSN: "0/38",
		Checkpoint: connector.Checkpoint{LSN: "0/38"},
	}
	intent := stagedTestIntent(t, cfg, transaction)
	proto := newFakeStageProtocol()
	evidence, err := newStagedTestDriver(cfg, proto).apply(context.Background(), intent, transaction)
	assertStagedApplied(t, proto, intent, evidence, err)
	if proto.getCalls != 1 || proto.copyCalls != 0 {
		t.Fatalf("zero-row transaction calls=(get:%d copy:%d), want 1/0 with durable manifest promotion", proto.getCalls, proto.copyCalls)
	}
}

func TestStagedDriverCopyResponseLossRecoversViaHistory(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	proto := newFakeStageProtocol()
	proto.copyLoadsThenError = errors.New("lost response after COPY committed")
	driver := newStagedTestDriver(cfg, proto)
	evidence, err := driver.apply(context.Background(), intent, transaction)
	assertStagedApplied(t, proto, intent, evidence, err)
}

func TestStagedDriverInconclusiveCopyReconcilesViaLandingProof(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	proto := newFakeStageProtocol()
	proto.copyReturnsInconclusive = true
	driver := newStagedTestDriver(cfg, proto)
	evidence, err := driver.apply(context.Background(), intent, transaction)
	assertStagedApplied(t, proto, intent, evidence, err)
	if proto.historyCalls != 0 {
		t.Fatal("inconclusive COPY must not use COPY_HISTORY as authority")
	}
}

func TestStagedDriverPartialLoadFailsClosed(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	proto := newFakeStageProtocol()
	proto.forcePartialLoad = true
	driver := newStagedTestDriver(cfg, proto)
	_, err := driver.apply(context.Background(), intent, transaction)
	if !errors.Is(err, connector.ErrDeliveryConflict) || !errors.Is(err, errStagedPartialLoad) {
		t.Fatalf("partial load error=%v, want conflict/partial", err)
	}
	for _, receipt := range proto.receipts {
		if receipt.kind == stagedReceiptKindLoad {
			t.Fatal("a partial load must never produce a load receipt")
		}
	}
}

func TestStagedDriverReceiptResponseLossAdopts(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	proto := newFakeStageProtocol()
	proto.insertCommitsThenDuplicate = true
	driver := newStagedTestDriver(cfg, proto)
	evidence, err := driver.apply(context.Background(), intent, transaction)
	assertStagedApplied(t, proto, intent, evidence, err)
	if proto.insertCalls != 1 {
		t.Fatalf("receipt insert calls=%d, want 1", proto.insertCalls)
	}
}

func TestStagedDriverCrashBetweenCopyAndReceiptConverges(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	proto := newFakeStageProtocol()
	crashing := newStagedDriver(proto, cfg, "catalog-fingerprint", stagedHooks{BeforeReceipt: func() error {
		return errors.New("process killed before receipt insert")
	}})
	if _, err := crashing.apply(context.Background(), intent, transaction); err == nil {
		t.Fatal("crash before receipt should surface an error")
	}
	for _, receipt := range proto.receipts {
		if receipt.kind == stagedReceiptKindLoad {
			t.Fatal("crash before receipt must not leave a load receipt")
		}
	}
	recovery := newStagedTestDriver(cfg, proto)
	evidence, err := recovery.apply(context.Background(), intent, transaction)
	assertStagedApplied(t, proto, intent, evidence, err)
}

func TestStagedDriverRejectsExistingReceiptWithoutTargetProof(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	plan := stagedPlanFor(t, cfg, intent, transaction)
	proto := newFakeStageProtocol()
	seeded := plan.receipt
	seeded.catalogFingerprint = "catalog-fingerprint"
	proto.seedReceipt(seeded)
	driver := newStagedTestDriver(cfg, proto)
	if _, err := driver.apply(context.Background(), intent, transaction); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("receipt without target proof error=%v, want conflict", err)
	}
}

func TestStagedDriverConcurrentGenerationAdopts(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	proto := newFakeStageProtocol()
	first := newStagedTestDriver(cfg, proto)
	if _, err := first.apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("first generation apply: %v", err)
	}
	secondIntent := intent
	secondIntent.Generation = 2
	secondIntent.AcquisitionID = "33333333-3333-3333-3333-333333333333"
	secondIntent.LeaseEpoch = 2
	second := newStagedTestDriver(cfg, proto)
	evidence, err := second.apply(context.Background(), secondIntent, transaction)
	assertStagedApplied(t, proto, secondIntent, evidence, err)
}

func TestStagedDriverReconcileDispositions(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	proto := newFakeStageProtocol()
	driver := newStagedTestDriver(cfg, proto)
	disposition, _, err := driver.reconcile(context.Background(), intent)
	if err != nil || disposition != connector.DeliveryNotApplied {
		t.Fatalf("pre-apply reconcile=%v/%v, want not-applied", disposition, err)
	}
	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatal(err)
	}
	disposition, evidence, err := driver.reconcile(context.Background(), intent)
	if err != nil || disposition != connector.DeliveryApplied || evidence.ContentHash != intent.ContentHash {
		t.Fatalf("post-apply reconcile=%v/%+v/%v", disposition, evidence, err)
	}
}

func TestStagedDriverReconcileRejectsIdentityConflict(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	proto := newFakeStageProtocol()
	driver := newStagedTestDriver(cfg, proto)
	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatal(err)
	}
	conflicting := intent
	conflicting.ContentHash = differentHex(intent.ContentHash)
	if _, _, err := driver.reconcile(context.Background(), conflicting); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("reconcile with a reused batch and different content=%v, want conflict", err)
	}
}

func TestStagedDriverAutoIngestWaitsForVerifiableCompletion(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	cfg.autoIngest = true
	cfg.pipe = "WALLABY_PIPE"
	cfg.loadVerifyAttempts = 5
	proto := newFakeStageProtocol()
	proto.autoIngestDelayCalls = 2
	driver := newStagedTestDriver(cfg, proto)
	evidence, err := driver.apply(context.Background(), intent, transaction)
	assertStagedApplied(t, proto, intent, evidence, err)
	if proto.refreshCalls == 0 {
		t.Fatal("auto-ingest must refresh the pipe")
	}
}

func TestStagedDriverAutoIngestDoesNotDependOnHistoryVisibility(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	cfg.autoIngest = true
	cfg.pipe = "WALLABY_PIPE"
	cfg.loadVerifyAttempts = 2
	proto := newFakeStageProtocol()
	proto.autoIngestDelayCalls = 10
	driver := newStagedTestDriver(cfg, proto)
	evidence, err := driver.apply(context.Background(), intent, transaction)
	assertStagedApplied(t, proto, intent, evidence, err)
	if proto.historyCalls != 0 {
		t.Fatal("auto-ingest target proof must not wait for COPY_HISTORY visibility")
	}
}

func TestNormalizeStagedLoadStatus(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"LOADED":            stagedHistoryLoaded,
		"Loaded":            stagedHistoryLoaded,
		"PARTIALLY_LOADED":  stagedHistoryPartiallyLoaded,
		"Partially loaded":  stagedHistoryPartiallyLoaded,
		"LOAD_FAILED":       stagedHistoryLoadFailed,
		"Load failed":       stagedHistoryLoadFailed,
		"Load in progress":  stagedHistoryLoadInProgress,
		"  load   failed  ": stagedHistoryLoadFailed,
	}
	for input, want := range cases {
		if got := normalizeStagedLoadStatus(input); got != want {
			t.Fatalf("normalizeStagedLoadStatus(%q)=%q, want %q", input, got, want)
		}
	}
}

func TestInterpretStagedCopyResultNormalizesStatusVocabulary(t *testing.T) {
	t.Parallel()
	if _, conclusive, err := interpretStagedCopyResult(stageCopyResult{present: true, status: "Loaded", rowsLoaded: 3}, 3); err != nil || !conclusive {
		t.Fatalf("space-form Loaded conclusive=%v err=%v, want true/nil", conclusive, err)
	}
	for _, status := range []string{"Partially loaded", "Load failed"} {
		_, _, err := interpretStagedCopyResult(stageCopyResult{present: true, status: status, firstError: "row rejected"}, 3)
		if !errors.Is(err, connector.ErrDeliveryConflict) || !errors.Is(err, errStagedPartialLoad) {
			t.Fatalf("space-form %q error=%v, want conflict/partial", status, err)
		}
	}
}

func TestStagedDriverAutoIngestSpaceFormPartialFailsClosed(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	cfg.autoIngest = true
	cfg.pipe = "WALLABY_PIPE"
	cfg.loadVerifyAttempts = 3
	proto := newFakeStageProtocol()
	proto.forcePartialLoad = true
	proto.historyEmitsSpaceStatus = true // COPY_HISTORY reports "Partially loaded".
	driver := newStagedTestDriver(cfg, proto)
	_, err := driver.apply(context.Background(), intent, transaction)
	if !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("partial landing proof error=%v, want conflict", err)
	}
	for _, receipt := range proto.receipts {
		if receipt.kind == stagedReceiptKindLoad {
			t.Fatal("a space-form partial pipe load must never produce a load receipt")
		}
	}
}

func TestStagedDriverAutoIngestPollsDelayedLandingProof(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	cfg.autoIngest = true
	cfg.pipe = "WALLABY_PIPE"
	cfg.loadVerifyAttempts = 4
	proto := newFakeStageProtocol()
	proto.landingDelayCalls = 3
	driver := newStagedTestDriver(cfg, proto)
	evidence, err := driver.apply(context.Background(), intent, transaction)
	assertStagedApplied(t, proto, intent, evidence, err)
	plan := stagedPlanFor(t, cfg, intent, transaction)
	if got := proto.landingObserves[plan.identity.relativePath]; got != 3 {
		t.Fatalf("landing observations=%d, want 3", got)
	}
}

func TestStagedDriverAutoIngestSpaceFormLoadedAcks(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	cfg.autoIngest = true
	cfg.pipe = "WALLABY_PIPE"
	cfg.loadVerifyAttempts = 3
	proto := newFakeStageProtocol()
	proto.historyEmitsSpaceStatus = true // COPY_HISTORY reports "Loaded".
	driver := newStagedTestDriver(cfg, proto)
	evidence, err := driver.apply(context.Background(), intent, transaction)
	assertStagedApplied(t, proto, intent, evidence, err)
}

func TestStagedDriverCleanupIsBoundedIdempotentAndSafe(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	plan := stagedPlanFor(t, cfg, intent, transaction)
	proto := newFakeStageProtocol()
	driver := newStagedTestDriver(cfg, proto)
	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatal(err)
	}
	// An orphan object without a load receipt must never be removed.
	orphanStage := managedSnowflakeStagedQualified(cfg, cfg.stage)
	proto.stageRaw(orphanStage, plan.identity.incarnationRoot+"/orphan.ndjson", []byte("orphan\n"))

	cleanup := ManagedStagedCleanupAuthority{FlowIncarnationID: intent.FlowIncarnationID, Generation: intent.Generation, AcquisitionID: intent.AcquisitionID, LeaseEpoch: intent.LeaseEpoch, DestinationRevisionID: intent.DestinationRevisionID}
	released, err := driver.cleanup(context.Background(), cleanup)
	if err != nil || released != 1 {
		t.Fatalf("first cleanup released=%d err=%v, want 1", released, err)
	}
	if proto.removeCalls != 1 {
		t.Fatalf("cleanup remove calls=%d, want 1", proto.removeCalls)
	}
	if _, present := proto.objects[fakeStageKey(orphanStage, plan.identity.incarnationRoot+"/orphan.ndjson")]; !present {
		t.Fatal("cleanup must never remove an object without a durable load receipt")
	}
	released, err = driver.cleanup(context.Background(), cleanup)
	if err != nil || released != 0 {
		t.Fatalf("idempotent cleanup released=%d err=%v, want 0", released, err)
	}
}

func TestStagedDriverCleanupSkipsOldProvisionEpochWithoutRemovingOrWedge(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	proto := newFakeStageProtocol()
	driver := newStagedTestDriver(cfg, proto)
	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatal(err)
	}
	proto.mu.Lock()
	proto.provisionEpoch++ // no-op owner provision bump; catalog fingerprint is unchanged
	proto.mu.Unlock()
	cleanup := ManagedStagedCleanupAuthority{FlowIncarnationID: intent.FlowIncarnationID, Generation: intent.Generation, AcquisitionID: intent.AcquisitionID, LeaseEpoch: intent.LeaseEpoch, DestinationRevisionID: intent.DestinationRevisionID}
	released, err := driver.cleanup(context.Background(), cleanup)
	if err != nil || released != 0 {
		t.Fatalf("old-epoch cleanup released=%d err=%v, want safe skip", released, err)
	}
	if proto.removeCalls != 0 {
		t.Fatalf("old-epoch cleanup removed %d objects", proto.removeCalls)
	}
}

func TestStagedDriverCleanupRejectsMaliciousPersistedPath(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	proto := newFakeStageProtocol()
	driver := newStagedTestDriver(cfg, proto)
	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatal(err)
	}
	proto.mu.Lock()
	for key, receipt := range proto.receipts {
		if receipt.kind == stagedReceiptKindLoad {
			receipt.stagePath = "../../other-flow/object.ndjson"
			proto.receipts[key] = receipt
		}
	}
	proto.mu.Unlock()
	cleanup := ManagedStagedCleanupAuthority{FlowIncarnationID: intent.FlowIncarnationID, Generation: intent.Generation, AcquisitionID: intent.AcquisitionID, LeaseEpoch: intent.LeaseEpoch, DestinationRevisionID: intent.DestinationRevisionID}
	if _, err := driver.cleanup(context.Background(), cleanup); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("malicious cleanup path error=%v, want conflict", err)
	}
	if proto.removeCalls != 0 {
		t.Fatalf("malicious path caused %d removals", proto.removeCalls)
	}
}

func TestStagedDriverCleanupCrashAfterRemoveResumesWithGuardedReceipt(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	proto := newFakeStageProtocol()
	base := newStagedTestDriver(cfg, proto)
	if _, err := base.apply(context.Background(), intent, transaction); err != nil {
		t.Fatal(err)
	}
	crash := true
	driver := newStagedDriver(proto, cfg, "catalog-fingerprint", stagedHooks{AfterCleanupRemove: func() error {
		if crash {
			crash = false
			return errors.New("crash after remove")
		}
		return nil
	}})
	driver.sleep = func(context.Context, time.Duration) error { return nil }
	cleanup := ManagedStagedCleanupAuthority{FlowIncarnationID: intent.FlowIncarnationID, Generation: intent.Generation, AcquisitionID: intent.AcquisitionID, LeaseEpoch: intent.LeaseEpoch, DestinationRevisionID: intent.DestinationRevisionID}
	if _, err := driver.cleanup(context.Background(), cleanup); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("cleanup crash error=%v, want indeterminate", err)
	}
	if released, err := driver.cleanup(context.Background(), cleanup); err != nil || released != 1 {
		t.Fatalf("cleanup resume released=%d err=%v", released, err)
	}
	if proto.removeCalls != 2 {
		t.Fatalf("idempotent remove calls=%d, want 2", proto.removeCalls)
	}
}

func TestStagedDriverCleanupRejectsStaleDestinationAuthority(t *testing.T) {
	t.Parallel()
	cfg, intent, _ := stagedFixture(t)
	proto := newFakeStageProtocol()
	driver := newStagedTestDriver(cfg, proto)
	cleanup := ManagedStagedCleanupAuthority{FlowIncarnationID: intent.FlowIncarnationID, Generation: intent.Generation, AcquisitionID: intent.AcquisitionID, LeaseEpoch: intent.LeaseEpoch, DestinationRevisionID: "other-revision"}
	if _, err := driver.cleanup(context.Background(), cleanup); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("stale cleanup authority error=%v, want conflict", err)
	}
}
