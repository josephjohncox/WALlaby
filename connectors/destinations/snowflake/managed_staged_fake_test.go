package snowflake

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// fakeStageObject is one immutable staged file in the in-memory protocol.
type fakeStageObject struct {
	content []byte
	md5     string
}

type fakeLoadEntry struct {
	status    string
	rowCount  int
	errors    int
	visibleAt int
}

// fakeStageProtocol is a deterministic, in-memory implementation of stageProtocol.
// It models Snowflake's staged-object, COPY, landing/target proof, and hybrid-receipt
// semantics precisely enough to drive every crash-window recovery test, and it
// exposes fault knobs to reproduce lost PUT/COPY/receipt responses. It proves
// protocol logic only and is never promotion evidence.
type fakeStageProtocol struct {
	mu                 sync.Mutex
	objects            map[string]fakeStageObject
	loaded             map[string]fakeLoadEntry
	receipts           map[string]managedStagedReceipt
	leases             map[string]stagedRuntimeLease
	claims             map[string]stagedLoadClaim
	landing            map[string]map[string]int
	target             map[string]map[string]int
	manifests          map[string]stagedLoadClaim
	landingObserves    map[string]int
	provisionEpoch     int64
	catalogFingerprint string
	now                time.Time
	removed            []string

	// Fault knobs consumed once per apply attempt.
	putStagesThenError         error // PUT durably stages the object, then reports a lost response.
	putHardFail                error // PUT fails without staging.
	copyLoadsThenError         error // COPY durably loads the file, then reports a lost response.
	copyReturnsInconclusive    bool  // COPY returns an empty (skipped) result, forcing history reconciliation.
	insertCommitsThenDuplicate bool  // InsertReceipt commits, then reports a duplicate (concurrent owner).
	forcePartialLoad           bool  // COPY/history reports a partial load.
	autoIngestDelayCalls       int   // Load history becomes visible only after this many LoadHistory calls.
	landingDelayCalls          int   // Auto-ingest landing rows become visible only after this many observations.
	historyEmitsSpaceStatus    bool  // LoadHistory reports Snowflake COPY_HISTORY space-form statuses (e.g. "Partially loaded").
	getHardFail                error
	getCorrupt                 bool
	getOversize                bool
	statOmitsMD5               bool
	statSizeOverride           *int64

	// Observability counters.
	statCalls, getCalls, putCalls, copyCalls, refreshCalls, historyCalls, insertCalls, removeCalls int
}

func newFakeStageProtocol() *fakeStageProtocol {
	return &fakeStageProtocol{
		objects:  make(map[string]fakeStageObject),
		loaded:   make(map[string]fakeLoadEntry),
		receipts: make(map[string]managedStagedReceipt),
		leases:   make(map[string]stagedRuntimeLease), claims: make(map[string]stagedLoadClaim),
		landing: make(map[string]map[string]int), target: make(map[string]map[string]int),
		manifests: make(map[string]stagedLoadClaim), landingObserves: make(map[string]int), provisionEpoch: 1,
		now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func fakeStageKey(stageRef, relativePath string) string { return stageRef + "\x00" + relativePath }

func fakeStagedRowHashes(content []byte) map[string]int {
	result := make(map[string]int)
	for _, line := range bytes.Split(content, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var row stagedChangelogRow
		if json.Unmarshal(line, &row) == nil {
			result[row.RecordHash]++
		}
	}
	return result
}

func fakeReceiptPK(r managedStagedReceipt) string {
	return strings.Join([]string{r.kind, r.flowIncarnationID, r.destinationRevisionID, r.logicalBatchID}, "\x00")
}

// stageRaw plants an object at a path with arbitrary bytes so tests can force a
// wrong-byte collision.
func (f *fakeStageProtocol) stageRaw(stageRef, relativePath string, content []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[fakeStageKey(stageRef, relativePath)] = fakeStageObject{content: content, md5: stagedFileMD5(content)}
}

func (f *fakeStageProtocol) seedReceipt(r managedStagedReceipt) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.receipts[fakeReceiptPK(r)] = r
}

func (f *fakeStageProtocol) StatObject(_ context.Context, stageRef, relativePath string) (stageObjectStat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statCalls++
	object, present := f.objects[fakeStageKey(stageRef, relativePath)]
	if !present {
		return stageObjectStat{}, nil
	}
	md5 := object.md5
	if f.statOmitsMD5 {
		md5 = ""
	}
	sizeBytes := int64(len(object.content))
	if f.statSizeOverride != nil {
		sizeBytes = *f.statSizeOverride
	}
	return stageObjectStat{present: true, md5: md5, sizeBytes: sizeBytes}, nil
}

func (f *fakeStageProtocol) GetObject(_ context.Context, stageRef, relativePath string, maxBytes int) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	if f.getHardFail != nil {
		return nil, f.getHardFail
	}
	object, present := f.objects[fakeStageKey(stageRef, relativePath)]
	if !present {
		return nil, errors.New("staged object is absent")
	}
	if len(object.content) > maxBytes {
		return nil, errors.New("staged object exceeds plaintext bound")
	}
	content := append([]byte(nil), object.content...)
	if f.getCorrupt && len(content) != 0 {
		content[0] ^= 0xff
	}
	if f.getOversize {
		writer := &boundedStageObjectWriter{limit: maxBytes}
		if _, err := writer.Write(append(content, 'x')); err != nil {
			return nil, err
		}
	}
	return content, nil
}

func (f *fakeStageProtocol) PutObject(_ context.Context, stageRef, relativePath string, content []byte, expectedMD5 string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putCalls++
	key := fakeStageKey(stageRef, relativePath)
	if existing, present := f.objects[key]; present {
		if existing.md5 != strings.ToLower(expectedMD5) {
			return errStagedWrongByteCollision
		}
		return nil
	}
	if err := f.putHardFail; err != nil {
		f.putHardFail = nil
		return err
	}
	f.objects[key] = fakeStageObject{content: append([]byte(nil), content...), md5: stagedFileMD5(content)}
	if err := f.putStagesThenError; err != nil {
		f.putStagesThenError = nil
		return err
	}
	return nil
}

func (f *fakeStageProtocol) recordLoadLocked(relativePath string, rowCount int) {
	if _, present := f.loaded[relativePath]; present {
		return
	}
	entry := fakeLoadEntry{status: stagedHistoryLoaded, rowCount: rowCount, visibleAt: f.historyCalls}
	if f.forcePartialLoad {
		entry.status = stagedHistoryPartiallyLoaded
		entry.errors = 1
	}
	if f.autoIngestDelayCalls > 0 {
		entry.visibleAt = f.historyCalls + f.autoIngestDelayCalls
	}
	f.loaded[relativePath] = entry
}

func (f *fakeStageProtocol) Copy(_ context.Context, plan stagedCopyPlan) (stageCopyResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.copyCalls++
	object, present := f.objects[fakeStageKey(plan.stageRef, plan.relativePath)]
	if !present {
		return stageCopyResult{}, connector.ErrDeliveryIndeterminate
	}
	rowCount := fakeCountRows(object.content)
	f.landing[plan.relativePath] = fakeStagedRowHashes(object.content)
	_, alreadyLoaded := f.loaded[plan.relativePath]
	f.recordLoadLocked(plan.relativePath, rowCount)
	if err := f.copyLoadsThenError; err != nil {
		f.copyLoadsThenError = nil
		return stageCopyResult{}, err
	}
	if f.copyReturnsInconclusive || alreadyLoaded {
		return stageCopyResult{present: true, status: "", rowsLoaded: 0}, nil
	}
	if f.forcePartialLoad {
		return stageCopyResult{present: true, status: stagedHistoryPartiallyLoaded, rowsLoaded: 0, errorsSeen: 1, firstError: "row rejected"}, nil
	}
	return stageCopyResult{present: true, status: stagedHistoryLoaded, rowsLoaded: rowCount}, nil
}

func (f *fakeStageProtocol) RefreshPipe(_ context.Context, _, relativePath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refreshCalls++
	// Auto-ingest ingests whatever is staged; visibility may lag by design.
	for key, object := range f.objects {
		if strings.HasSuffix(key, relativePath) {
			landing := fakeStagedRowHashes(object.content)
			if f.forcePartialLoad {
				for hash := range landing {
					delete(landing, hash)
					break
				}
			}
			f.landing[relativePath] = landing
			f.recordLoadLocked(relativePath, fakeCountRows(object.content))
			break
		}
	}
	return nil
}

func (f *fakeStageProtocol) LoadHistory(_ context.Context, _, relativePath string) (stageLoadEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.historyCalls++
	entry, present := f.loaded[relativePath]
	if !present || f.historyCalls < entry.visibleAt {
		return stageLoadEntry{}, nil
	}
	status := entry.status
	if f.historyEmitsSpaceStatus {
		status = stagedSpaceFormStatus(status)
	}
	return stageLoadEntry{present: true, status: status, rowCount: entry.rowCount, errorCount: entry.errors}, nil
}

// stagedSpaceFormStatus mirrors INFORMATION_SCHEMA.COPY_HISTORY.STATUS, which
// uses space-separated title-case forms rather than the underscore vocabulary of
// the COPY command result. It lets tests prove the driver classifies the async
// auto-ingest surface correctly.
func stagedSpaceFormStatus(underscore string) string {
	switch underscore {
	case stagedHistoryLoaded:
		return "Loaded"
	case stagedHistoryPartiallyLoaded:
		return "Partially loaded"
	case stagedHistoryLoadFailed:
		return "Load failed"
	case stagedHistoryLoadInProgress:
		return "Load in progress"
	default:
		return underscore
	}
}

func (f *fakeStageProtocol) LookupReceipt(_ context.Context, _ stagedConfig, key stagedReceiptKey) (managedStagedReceipt, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, receipt := range f.receipts {
		if receipt.kind == key.kind && receipt.flowIncarnationID == key.flowIncarnationID &&
			receipt.destinationRevisionID == key.destinationRevisionID && receipt.logicalBatchID == key.logicalBatchID {
			return receipt, true, nil
		}
		if receipt.kind == key.kind && receipt.flowIncarnationID == key.flowIncarnationID &&
			receipt.destinationRevisionID == key.destinationRevisionID && receipt.sourceLineageID == key.sourceLineageID &&
			receipt.positionID == key.positionID {
			return receipt, true, nil
		}
		if key.externalID != "" && receipt.externalID == key.externalID {
			return receipt, true, nil
		}
	}
	return managedStagedReceipt{}, false, nil
}

func (f *fakeStageProtocol) InsertReceipt(_ context.Context, _ stagedConfig, receipt managedStagedReceipt) (stageReceiptInsert, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.insertCalls++
	pk := fakeReceiptPK(receipt)
	if _, present := f.receipts[pk]; present {
		return stageReceiptInsert{inserted: false}, nil
	}
	f.receipts[pk] = receipt
	if f.insertCommitsThenDuplicate {
		f.insertCommitsThenDuplicate = false
		return stageReceiptInsert{inserted: false}, nil
	}
	return stageReceiptInsert{inserted: true}, nil
}

func (f *fakeStageProtocol) ListReleasableReceipts(_ context.Context, _ stagedConfig, flowIncarnationID string, _ time.Duration, limit int) ([]managedStagedReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	released := make(map[string]struct{})
	for _, receipt := range f.receipts {
		if receipt.kind == stagedReceiptKindRelease {
			released[receipt.externalID] = struct{}{}
		}
	}
	var candidates []managedStagedReceipt
	for _, receipt := range f.receipts {
		if receipt.kind != stagedReceiptKindLoad || receipt.flowIncarnationID != flowIncarnationID {
			continue
		}
		if _, isReleased := released[receipt.externalID+":release"]; isReleased {
			continue
		}
		candidates = append(candidates, receipt)
		if len(candidates) >= limit {
			break
		}
	}
	return candidates, nil
}

func (f *fakeStageProtocol) RemoveObject(_ context.Context, stageRef, relativePath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removeCalls++
	delete(f.objects, fakeStageKey(stageRef, relativePath))
	f.removed = append(f.removed, relativePath)
	return nil
}

func fakeCountRows(content []byte) int {
	count := 0
	for _, character := range content {
		if character == '\n' {
			count++
		}
	}
	return count
}

// stagedTestConfig returns an internally consistent stagedConfig backed by the
// shared managed test schema.
func TestStagedFakeTransportReceivesRawRenameSubsetImages(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
	cfg.schemaContract.Columns = append(cfg.schemaContract.Columns, connector.Column{Name: "secret", Type: "text", Nullable: true, TypeMetadata: map[string]string{"nullability_known": "true", "generated_known": "true"}})
	cfg.schemaContractHash = mustManagedSchemaHash(t, cfg.schemaContract)
	transaction := managedTestTransaction(cfg.schemaContract)
	transaction.Fragments[0].Batch.WritePolicy = connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend, ProjectionFingerprint: "rename-subset-v1"}
	transaction.Fragments[0].Batch.Records[0].After["secret"] = "raw-only"
	intent := stagedTestIntent(t, cfg, transaction)
	proto := newFakeStageProtocol()
	if _, err := newStagedTestDriver(cfg, proto).apply(context.Background(), intent, transaction); err != nil {
		t.Fatal(err)
	}
	if len(proto.objects) != 1 {
		t.Fatalf("staged fake objects=%d, want 1", len(proto.objects))
	}
	for _, object := range proto.objects {
		if !bytes.Contains(object.content, []byte(`"secret":"raw-only"`)) || !bytes.Contains(object.content, []byte(`"SOURCE_TABLE":"widgets"`)) || bytes.Contains(object.content, []byte(`"EVENT_ID"`)) {
			t.Fatalf("staged fake transport received double-mapped payload: %s", object.content)
		}
	}
}

func stagedTestConfig(t testing.TB) stagedConfig {
	t.Helper()
	schema := managedTestSchema()
	hash, err := ManagedSchemaContractHash(schema)
	if err != nil {
		t.Fatalf("hash staged test schema: %v", err)
	}
	return stagedConfig{
		profile: connector.ManagedProfilePostgresToSnowflakeStagedAppendV1, flowID: "flow-1",
		account: "ACME", database: "DB", schema: "PUBLIC", stage: "WALLABY_STAGE", table: "WALLABY_CHANGELOG",
		receiptsTable: "WALLABY_RECEIPTS", landingTable: "WALLABY_LANDING", authorityTable: "WALLABY_AUTHORITY",
		targetManifestTable: "WALLABY_TARGET_MANIFESTS", fileFormat: "WALLABY_JSON", ownerRole: "WALLABY_OWNER", executionRole: "WALLABY_EXEC",
		warehouse: "WALLABY_WH", snowflakeVersion: "8.0.0", stageCreatedOn: "2026-01-01T00:00:00.000000000+00:00",
		targetCreatedOn: "2026-01-01T00:00:00.000000000+00:00", receiptsCreatedOn: "2026-01-01T00:00:00.000000000+00:00",
		landingCreatedOn: "2026-01-01T00:00:00.000000000+00:00", authorityCreatedOn: "2026-01-01T00:00:00.000000000+00:00",
		targetManifestCreatedOn: "2026-01-01T00:00:00.000000000+00:00", fileFormatCreatedOn: "2026-01-01T00:00:00.000000000+00:00", sourceSchema: "public", sourceTable: "widgets",
		schemaContract: schema, schemaContractHash: hash, destinationRevision: "snowflake-staged-v1",
		maxTransactionRows: 1000, maxTransactionBytes: 8 << 20, maxFragments: 128, maxOpenConnections: 4,
		statementTimeoutSeconds: 600, loadVerifyAttempts: 5, cleanupMaxObjects: 100, validateEveryConnection: true,
		typeMappings: defaultSnowflakeTypeMappings(),
	}
}

func stagedTestIntent(t *testing.T, cfg stagedConfig, transaction connector.SourceTransaction) connector.DeliveryIntent {
	t.Helper()
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		t.Fatal(err)
	}
	position, err := connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	return connector.DeliveryIntent{
		FlowID: cfg.flowID, FlowIncarnationID: "11111111-1111-1111-1111-111111111111",
		SourceLineageID: transaction.SourceLineageID, Generation: 1,
		AcquisitionID: "22222222-2222-2222-2222-222222222222", LeaseEpoch: 1,
		DestinationRevisionID: cfg.destinationRevision, LogicalBatchID: logicalBatchID,
		PositionID: position, ContentHash: contentHash,
	}
}

func stagedClaimsSameIdentity(left, right stagedLoadClaim) bool {
	left.leaseID = ""
	right.leaseID = ""
	left.expiresAt = time.Time{}
	right.expiresAt = time.Time{}
	return left == right
}

func (f *fakeStageProtocol) AcquireRuntimeLease(_ context.Context, _ stagedConfig, request stagedLeaseRequest) (stagedRuntimeLease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.catalogFingerprint == "" {
		f.catalogFingerprint = request.catalogFingerprint
	}
	if f.catalogFingerprint != request.catalogFingerprint {
		return stagedRuntimeLease{}, connector.ErrDeliveryConflict
	}
	lease := stagedRuntimeLease{stagedLeaseRequest: request, provisionEpoch: f.provisionEpoch, expiresAt: f.now.Add(stagedRuntimeLeaseMinimum)}
	if existing, ok := f.leases[request.leaseID]; ok && existing.ownerID != request.ownerID && existing.expiresAt.After(f.now) {
		return stagedRuntimeLease{}, connector.ErrDeliveryIndeterminate
	}
	f.leases[request.leaseID] = lease
	return lease, nil
}

func (f *fakeStageProtocol) RevalidateRuntimeLease(_ context.Context, _ stagedConfig, lease stagedRuntimeLease) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	current, ok := f.leases[lease.leaseID]
	if !ok || current.ownerID != lease.ownerID || !current.expiresAt.After(f.now) || current.provisionEpoch != lease.provisionEpoch || current.provisionEpoch != f.provisionEpoch || f.catalogFingerprint != lease.catalogFingerprint {
		return connector.ErrDeliveryIndeterminate
	}
	current.expiresAt = f.now.Add(stagedRuntimeLeaseMinimum)
	f.leases[lease.leaseID] = current
	return nil
}

func (f *fakeStageProtocol) GuardCatalog(_ context.Context, _ stagedConfig, lease stagedRuntimeLease) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	current, ok := f.leases[lease.leaseID]
	if !ok || current.ownerID != lease.ownerID || !current.expiresAt.After(f.now) || current.provisionEpoch != f.provisionEpoch || current.catalogFingerprint != f.catalogFingerprint {
		return fmt.Errorf("%w: fake staged catalog guard changed", connector.ErrDeliveryIndeterminate)
	}
	return nil
}

func (f *fakeStageProtocol) ReleaseRuntimeLease(_ context.Context, _ stagedConfig, lease stagedRuntimeLease) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if current, ok := f.leases[lease.leaseID]; ok && current.ownerID == lease.ownerID {
		delete(f.leases, lease.leaseID)
	}
	return nil
}

func (f *fakeStageProtocol) AcquireLoadClaim(_ context.Context, _ stagedConfig, lease stagedRuntimeLease, claim stagedLoadClaim) (stagedLoadClaim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	current, ok := f.leases[lease.leaseID]
	if !ok || current.ownerID != lease.ownerID || current.provisionEpoch != f.provisionEpoch {
		return stagedLoadClaim{}, connector.ErrDeliveryIndeterminate
	}
	claim.expiresAt = f.now.Add(stagedRuntimeLeaseMinimum)
	if existing, ok := f.claims[claim.claimID]; ok {
		if existing.expiresAt.After(f.now) && !stagedClaimsSameIdentity(existing, claim) {
			return stagedLoadClaim{}, connector.ErrDeliveryConflict
		}
		if existing.expiresAt.After(f.now) {
			return existing, nil
		}
	}
	f.claims[claim.claimID] = claim
	return claim, nil
}

func fakeObserveRows(expected []string, manifest bool, manifestMatches bool, rows map[string]int) stagedTargetObservation {
	copyRows := make(map[string]int, len(rows))
	for hash, count := range rows {
		copyRows[hash] = count
	}
	return classifyStagedTarget(expected, manifest, manifestMatches, copyRows)
}

func (f *fakeStageProtocol) ObserveLanding(_ context.Context, _ stagedConfig, claim stagedLoadClaim, expected []string) (stagedTargetObservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.landingObserves[claim.stagePath]++
	if f.landingDelayCalls > 0 && f.landingObserves[claim.stagePath] < f.landingDelayCalls {
		return stagedTargetObservation{state: stagedTargetAbsent}, nil
	}
	rows := f.landing[claim.stagePath]
	observation := fakeObserveRows(expected, len(expected) == 0 || len(rows) > 0, true, rows)
	if len(expected) == 0 {
		observation.state = stagedTargetComplete
	}
	return observation, nil
}

func (f *fakeStageProtocol) ObserveTarget(_ context.Context, _ stagedConfig, claim stagedLoadClaim, expected []string) (stagedTargetObservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	manifest, present := f.manifests[claim.claimID]
	return fakeObserveRows(expected, present, !present || stagedClaimsSameIdentity(manifest, claim), f.target[claim.claimID]), nil
}

func (f *fakeStageProtocol) ValidateReceiptTargetProof(_ context.Context, _ stagedConfig, receipt managedStagedReceipt) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	claim, present := f.manifests[receipt.externalID]
	if !present || claim.manifestHash != receipt.manifestHash || claim.contentHash != receipt.contentHash || claim.fileContentHash != receipt.fileContentHash || claim.planHash != receipt.planHash || claim.provisionEpoch != receipt.provisionEpoch || receipt.provisionEpoch != f.provisionEpoch || claim.catalogFingerprint != receipt.catalogFingerprint || receipt.catalogFingerprint != f.catalogFingerprint {
		return connector.ErrDeliveryConflict
	}
	rows := f.target[receipt.externalID]
	count := 0
	for _, duplicates := range rows {
		if duplicates != 1 {
			return connector.ErrDeliveryConflict
		}
		count += duplicates
	}
	if count != receipt.recordCount {
		return connector.ErrDeliveryConflict
	}
	return nil
}

func (f *fakeStageProtocol) InsertLoadReceipt(_ context.Context, _ stagedConfig, lease stagedRuntimeLease, claim stagedLoadClaim, receipt managedStagedReceipt) (stageReceiptInsert, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	current, ok := f.leases[lease.leaseID]
	ownedClaim, claimOK := f.claims[claim.claimID]
	if !ok || current.ownerID != lease.ownerID || current.provisionEpoch != f.provisionEpoch || receipt.provisionEpoch != current.provisionEpoch || receipt.catalogFingerprint != current.catalogFingerprint || !claimOK || !stagedClaimsSameIdentity(ownedClaim, claim) {
		return stageReceiptInsert{}, connector.ErrDeliveryIndeterminate
	}
	pk := fakeReceiptPK(receipt)
	if _, present := f.receipts[pk]; present {
		return stageReceiptInsert{inserted: false}, nil
	}
	f.receipts[pk] = receipt
	f.insertCalls++
	if f.insertCommitsThenDuplicate {
		f.insertCommitsThenDuplicate = false
		return stageReceiptInsert{inserted: false}, nil
	}
	return stageReceiptInsert{inserted: true}, nil
}

func (f *fakeStageProtocol) InsertReleaseReceipt(ctx context.Context, cfg stagedConfig, lease stagedRuntimeLease, claim stagedLoadClaim, receipt managedStagedReceipt) (stageReceiptInsert, error) {
	return f.InsertLoadReceipt(ctx, cfg, lease, claim, receipt)
}

func (f *fakeStageProtocol) PromoteTarget(_ context.Context, _ stagedConfig, lease stagedRuntimeLease, claim stagedLoadClaim, rowHashes []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	current, ok := f.leases[lease.leaseID]
	if !ok || current.ownerID != lease.ownerID || current.provisionEpoch != f.provisionEpoch {
		return connector.ErrDeliveryIndeterminate
	}
	if existing, present := f.manifests[claim.claimID]; present {
		if !stagedClaimsSameIdentity(existing, claim) {
			return connector.ErrDeliveryConflict
		}
		return nil
	}
	landing := f.landing[claim.stagePath]
	expected := make(map[string]int, len(rowHashes))
	for _, hash := range rowHashes {
		expected[hash]++
	}
	if len(landing) != len(expected) {
		return connector.ErrDeliveryIndeterminate
	}
	for hash, count := range expected {
		if count != 1 || landing[hash] != 1 {
			return connector.ErrDeliveryConflict
		}
	}
	f.target[claim.claimID] = expected
	f.manifests[claim.claimID] = claim
	delete(f.landing, claim.stagePath)
	return nil
}

func newStagedTestDriver(cfg stagedConfig, proto stageProtocol) *stagedDriver {
	driver := newStagedDriver(proto, cfg, "catalog-fingerprint", stagedHooks{})
	driver.sleep = func(context.Context, time.Duration) error { return nil }
	return driver
}
