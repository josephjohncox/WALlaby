package snowflake

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// fakeStreamChannel is one durable channel in the in-memory protocol.
type fakeStreamChannel struct {
	revision             int64
	pipeRevision         string
	continuationToken    string
	committedOffsetToken string
}

// fakeCommittedRow tracks one durably committed row identity and the observe
// call after which it becomes visible (to model asynchronous commit latency).
type fakeCommittedRow struct {
	count     int
	visibleAt int
}

// fakeStreamProtocol is a deterministic, in-memory implementation of
// streamProtocol. It models Snowpipe Streaming channel opens, appends, committed
// offset tokens, SQL-observed row completeness, durable channel state, and
// receipt semantics precisely enough to drive every crash-window recovery test,
// and it exposes fault knobs to reproduce channel invalidation, auth expiry,
// throttling, lost responses, and rejected rows. It proves protocol logic only
// and is never promotion evidence.
type fakeStreamProtocol struct {
	mu               sync.Mutex
	channels         map[string]*fakeStreamChannel
	committed        map[string]*fakeCommittedRow
	channelState     map[string]managedStreamChannelState
	receipts         map[string]managedStreamReceipt
	appendedPayloads [][]byte

	// Fault knobs.
	openFailsOnce                error // OpenChannel fails once without opening.
	appendInvalidateThenReopen   bool  // First AppendRows reports a stale channel; reopen bumps the revision.
	appendAuthExpiresOnce        bool  // First AppendRows reports an expired ingest credential.
	appendThrottleTimes          int   // The next N AppendRows report backpressure.
	appendCommitsThenThrottle    bool  // AppendRows commits the rows, then reports a lost/throttled response.
	appendRejectsRows            bool  // AppendRows returns per-row rejections.
	insertCommitsThenDuplicate   bool  // InsertReceipt commits, then reports a duplicate (concurrent owner).
	statusSuppressCommittedToken bool  // ChannelStatus withholds the committed offset token.
	commitVisibilityDelay        int   // Committed rows become visible only after this many ObserveCommittedRows calls.

	// Observability counters.
	openCalls, appendCalls, statusCalls, observeCalls, upsertCalls, insertCalls, deleteCalls int
}

func newFakeStreamProtocol() *fakeStreamProtocol {
	return &fakeStreamProtocol{
		channels:     make(map[string]*fakeStreamChannel),
		committed:    make(map[string]*fakeCommittedRow),
		channelState: make(map[string]managedStreamChannelState),
		receipts:     make(map[string]managedStreamReceipt),
	}
}

func fakeStreamReceiptPK(r managedStreamReceipt) string {
	return strings.Join([]string{r.kind, r.flowIncarnationID, r.destinationRevisionID, r.logicalBatchID}, "\x00")
}

func fakeChannelStateKey(k streamChannelStateKey) string {
	return strings.Join([]string{k.flowIncarnationID, k.destinationRevisionID, k.channelName}, "\x00")
}

func (f *fakeStreamProtocol) seedReceipt(r managedStreamReceipt) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.receipts[fakeStreamReceiptPK(r)] = r
}

// seedCommittedRow plants a durably committed row identity so tests can model a
// duplicate-identity hazard or an already-committed replay.
func (f *fakeStreamProtocol) seedCommittedRow(rowHash string, count int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed[rowHash] = &fakeCommittedRow{count: count, visibleAt: 0}
}

func (f *fakeStreamProtocol) OpenChannel(_ context.Context, _ streamConfig, channelName string) (streamChannelStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.openCalls++
	if err := f.openFailsOnce; err != nil {
		f.openFailsOnce = nil
		return streamChannelStatus{}, err
	}
	channel, present := f.channels[channelName]
	if !present {
		channel = &fakeStreamChannel{revision: 1, pipeRevision: "pipe-rev-1", continuationToken: "cont-1"}
		f.channels[channelName] = channel
	} else {
		channel.revision++
		channel.continuationToken = fmt.Sprintf("cont-%d", channel.revision)
	}
	return streamChannelStatus{
		valid: true, channelName: channelName, channelRevision: channel.revision, pipeRevision: channel.pipeRevision,
		continuationToken: channel.continuationToken, committedOffsetToken: channel.committedOffsetToken,
	}, nil
}

func (f *fakeStreamProtocol) AppendRows(_ context.Context, req streamAppendRequest) (streamAppendResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.appendCalls++
	for _, row := range req.rows {
		f.appendedPayloads = append(f.appendedPayloads, append([]byte(nil), row.payload...))
	}
	channel, present := f.channels[req.channelName]
	if !present {
		return streamAppendResult{}, errStreamChannelInvalidated
	}
	if req.channelRevision != channel.revision {
		return streamAppendResult{}, errStreamChannelInvalidated
	}
	if f.appendInvalidateThenReopen {
		f.appendInvalidateThenReopen = false
		return streamAppendResult{}, errStreamChannelInvalidated
	}
	if f.appendAuthExpiresOnce {
		f.appendAuthExpiresOnce = false
		return streamAppendResult{}, errStreamAuthExpired
	}
	if f.appendThrottleTimes > 0 {
		f.appendThrottleTimes--
		return streamAppendResult{}, errStreamThrottled
	}
	if f.appendRejectsRows {
		f.appendRejectsRows = false
		rejections := make([]streamRowRejection, 0, len(req.rows))
		for _, row := range req.rows {
			rejections = append(rejections, streamRowRejection{rowHash: row.rowHash, ordinal: row.ordinal, reason: "invalid row"})
		}
		return streamAppendResult{rejections: rejections}, nil
	}
	// Commit the rows: they become observable after any configured latency.
	for _, row := range req.rows {
		entry, ok := f.committed[row.rowHash]
		if !ok {
			entry = &fakeCommittedRow{visibleAt: f.observeCalls + f.commitVisibilityDelay}
			f.committed[row.rowHash] = entry
		}
		entry.count++
	}
	channel.committedOffsetToken = req.offsetToken
	channel.continuationToken = fmt.Sprintf("cont-%d-%d", channel.revision, f.appendCalls)
	if f.appendCommitsThenThrottle {
		f.appendCommitsThenThrottle = false
		return streamAppendResult{}, errStreamThrottled
	}
	return streamAppendResult{continuationToken: channel.continuationToken}, nil
}

func (f *fakeStreamProtocol) ChannelStatus(_ context.Context, _ streamConfig, channelName string) (streamChannelStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statusCalls++
	channel, present := f.channels[channelName]
	if !present {
		return streamChannelStatus{}, nil
	}
	committed := channel.committedOffsetToken
	if f.statusSuppressCommittedToken {
		committed = ""
	}
	return streamChannelStatus{
		valid: true, channelName: channelName, channelRevision: channel.revision, pipeRevision: channel.pipeRevision,
		continuationToken: channel.continuationToken, committedOffsetToken: committed,
	}, nil
}

func (f *fakeStreamProtocol) ObserveCommittedRows(_ context.Context, _ streamConfig, _ string, rowHashes []string) (map[string]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeCalls++
	present := make(map[string]int, len(rowHashes))
	for _, hash := range rowHashes {
		entry, ok := f.committed[hash]
		if !ok || f.observeCalls < entry.visibleAt {
			continue
		}
		if entry.count > 0 {
			present[hash] = entry.count
		}
	}
	return present, nil
}

func (f *fakeStreamProtocol) UpsertChannelState(_ context.Context, _ streamConfig, state managedStreamChannelState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.upsertCalls++
	f.channelState[fakeChannelStateKey(streamChannelStateKey{
		flowIncarnationID: state.flowIncarnationID, destinationRevisionID: state.destinationRevisionID, channelName: state.channelName,
	})] = state
	return nil
}

func (f *fakeStreamProtocol) LookupChannelState(_ context.Context, _ streamConfig, key streamChannelStateKey) (managedStreamChannelState, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	state, ok := f.channelState[fakeChannelStateKey(key)]
	return state, ok, nil
}

func (f *fakeStreamProtocol) LookupReceipt(_ context.Context, _ streamConfig, key streamReceiptKey) (managedStreamReceipt, bool, error) {
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
	return managedStreamReceipt{}, false, nil
}

func (f *fakeStreamProtocol) InsertReceipt(_ context.Context, _ streamConfig, receipt managedStreamReceipt) (streamReceiptInsert, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.insertCalls++
	pk := fakeStreamReceiptPK(receipt)
	if _, present := f.receipts[pk]; present {
		return streamReceiptInsert{inserted: false}, nil
	}
	f.receipts[pk] = receipt
	if f.insertCommitsThenDuplicate {
		f.insertCommitsThenDuplicate = false
		return streamReceiptInsert{inserted: false}, nil
	}
	return streamReceiptInsert{inserted: true}, nil
}

func (f *fakeStreamProtocol) ListReleasableReceipts(_ context.Context, _ streamConfig, flowIncarnationID string, _ time.Duration, limit int) ([]managedStreamReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	released := make(map[string]struct{})
	for _, receipt := range f.receipts {
		if receipt.kind == streamReceiptKindRelease {
			released[receipt.externalID] = struct{}{}
		}
	}
	var candidates []managedStreamReceipt
	for _, receipt := range f.receipts {
		if receipt.kind != streamReceiptKindAppend || receipt.flowIncarnationID != flowIncarnationID {
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

func (f *fakeStreamProtocol) DeleteChannelState(_ context.Context, _ streamConfig, key streamChannelStateKey) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	delete(f.channelState, fakeChannelStateKey(key))
	return nil
}

// streamTestConfig returns an internally consistent streamConfig backed by the
// shared managed test schema.
func streamTestConfig(t testing.TB) streamConfig {
	t.Helper()
	schema := managedTestSchema()
	hash, err := ManagedSchemaContractHash(schema)
	if err != nil {
		t.Fatalf("hash streaming test schema: %v", err)
	}
	return streamConfig{
		profile: connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1, flowID: "flow-1",
		account: "ACME", database: "DB", schema: "PUBLIC", pipe: "WALLABY_PIPE", table: "WALLABY_CHANGELOG",
		receiptsTable: "WALLABY_RECEIPTS", channelStateTable: "WALLABY_CHANNELS", channelNamePrefix: "wallaby_stream",
		ownerRole: "WALLABY_OWNER", executionRole: "WALLABY_EXEC", warehouse: "WALLABY_WH", snowflakeVersion: "8.0.0",
		pipeCreatedOn: "2026-01-01T00:00:00.000000000+00:00", targetCreatedOn: "2026-01-01T00:00:00.000000000+00:00",
		receiptsCreatedOn: "2026-01-01T00:00:00.000000000+00:00", channelStateCreatedOn: "2026-01-01T00:00:00.000000000+00:00",
		sourceSchema: "public", sourceTable: "widgets", schemaContract: schema, schemaContractHash: hash,
		destinationRevision: "snowflake-streaming-v1", maxTransactionRows: 1000, maxTransactionBytes: 8 << 20,
		maxFragments: 128, maxRowBytes: 1 << 20, maxOpenConnections: 4, statementTimeoutSeconds: 600,
		observeAttempts: 8, observeInterval: time.Millisecond, appendAttempts: 8, appendBackoff: time.Millisecond,
		cleanupMaxObjects: 100, cleanupRetention: time.Hour, validateEveryConnection: true, typeMappings: defaultSnowflakeTypeMappings(),
	}
}

func TestStreamingFakeTransportReceivesRawRenameSubsetImages(t *testing.T) {
	t.Parallel()
	cfg := streamTestConfig(t)
	cfg.schemaContract.Columns = append(cfg.schemaContract.Columns, connector.Column{Name: "secret", Type: "text", Nullable: true, TypeMetadata: map[string]string{"nullability_known": "true", "generated_known": "true"}})
	cfg.schemaContractHash = mustManagedSchemaHash(t, cfg.schemaContract)
	transaction := managedTestTransaction(cfg.schemaContract)
	transaction.Fragments[0].Batch.WritePolicy = connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend, ProjectionFingerprint: "rename-subset-v1"}
	transaction.Fragments[0].Batch.Records[0].After["secret"] = "raw-only"
	intent := streamTestIntent(t, cfg, transaction)
	proto := newFakeStreamProtocol()
	if _, err := newStreamTestDriver(cfg, proto).apply(context.Background(), intent, transaction); err != nil {
		t.Fatal(err)
	}
	if len(proto.appendedPayloads) == 0 {
		t.Fatal("streaming fake transport received no append payloads")
	}
	if !bytes.Contains(proto.appendedPayloads[0], []byte(`"secret":"raw-only"`)) || !bytes.Contains(proto.appendedPayloads[0], []byte(`"SOURCE_TABLE":"widgets"`)) || bytes.Contains(proto.appendedPayloads[0], []byte(`"EVENT_ID"`)) {
		t.Fatalf("streaming fake transport received double-mapped payload: %s", proto.appendedPayloads[0])
	}
}

func streamTestIntent(t *testing.T, cfg streamConfig, transaction connector.SourceTransaction) connector.DeliveryIntent {
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

func newStreamTestDriver(cfg streamConfig, proto streamProtocol) *streamDriver {
	driver := newStreamDriver(proto, cfg, "catalog-fingerprint", streamingHooks{})
	driver.sleep = func(context.Context, time.Duration) error { return nil }
	return driver
}
