package snowflake

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

var errStreamChannelInvalidated = errors.New("streaming Snowflake channel is invalidated")

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
	logicalBatchID string
	count          int
	visibleAt      int
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
	requests         map[string]managedStreamRequest
	requestByBatch   map[string]string
	requestStatus    map[string]streamRequestDisposition
	receipts         map[string]managedStreamReceipt
	appendedPayloads [][]byte

	// Fault knobs.
	openFailsOnce                error // OpenChannel fails once without opening.
	appendInvalidateThenReopen   bool  // First AppendRows reports a stale channel; reopen bumps the revision.
	appendAuthExpiresOnce        bool  // First AppendRows reports an expired ingest credential.
	appendThrottleTimes          int   // The next N AppendRows report backpressure.
	appendUnknownOnce            error // AppendRows returns one ambiguous transport error without status.
	appendDefinitelyAbsentOnce   bool  // AppendRows proves that one request was not accepted.
	appendCommitsThenThrottle    bool  // AppendRows commits the rows, then reports a lost/throttled response.
	appendRejectsRows            bool  // AppendRows returns per-row rejections.
	insertCommitsThenDuplicate   bool  // InsertReceipt commits, then reports a duplicate (concurrent owner).
	statusSuppressCommittedToken bool  // ChannelStatus withholds the committed offset token.
	requestStatusUnknownTimes    int   // Authoritative request status remains unknown for N polls.
	requestStatusDivergent       bool  // Authoritative request status returns conflicting identity.
	commitVisibilityDelay        int   // Committed rows become visible only after this many ObserveCommittedRows calls.

	// Observability counters.
	openCalls, appendCalls, statusCalls, observeCalls, upsertCalls, insertCalls, deleteCalls int
}

func newFakeStreamProtocol() *fakeStreamProtocol {
	return &fakeStreamProtocol{
		channels:       make(map[string]*fakeStreamChannel),
		committed:      make(map[string]*fakeCommittedRow),
		channelState:   make(map[string]managedStreamChannelState),
		requests:       make(map[string]managedStreamRequest),
		requestByBatch: make(map[string]string),
		requestStatus:  make(map[string]streamRequestDisposition),
		receipts:       make(map[string]managedStreamReceipt),
	}
}

func fakeStreamReceiptPK(r managedStreamReceipt) string {
	return strings.Join([]string{r.kind, r.flowIncarnationID, r.destinationRevisionID, r.logicalBatchID}, "\x00")
}

func fakeChannelStateKey(k streamChannelStateKey) string {
	return strings.Join([]string{k.flowIncarnationID, k.destinationRevisionID, k.channelName}, "\x00")
}

func fakeRequestBatchKey(k streamRequestKey) string {
	return strings.Join([]string{k.flowIncarnationID, k.destinationRevisionID, k.logicalBatchID}, "\x00")
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
	request, requestPresent := f.requests[req.requestID]
	if !requestPresent || request.phase != streamRequestSendingUnknown || request.channelRevision != req.channelRevision || request.inputContinuation != req.continuationToken || request.requestedOffset != req.offsetToken || request.manifestHash != req.manifestHash || request.rowsContentHash != req.rowsContentHash || request.rowCount != req.rowCount {
		return streamAppendResult{}, fmt.Errorf("%w: append was not preceded by the exact durable SENDING_UNKNOWN request", connector.ErrDeliveryConflict)
	}
	channel, present := f.channels[req.channelName]
	if !present || req.channelRevision != channel.revision || req.pipeRevision != channel.pipeRevision {
		f.requestStatus[req.requestID] = streamRequestStatusProvenAbsent
		return streamAppendResult{}, errStreamChannelInvalidated
	}
	if f.appendUnknownOnce != nil {
		err := f.appendUnknownOnce
		f.appendUnknownOnce = nil
		return streamAppendResult{}, err
	}
	if f.appendDefinitelyAbsentOnce {
		f.appendDefinitelyAbsentOnce = false
		f.requestStatus[req.requestID] = streamRequestStatusProvenAbsent
		return streamAppendResult{disposition: streamAppendDefinitelyNotAccepted, requestID: req.requestID, continuationToken: channel.continuationToken, evidence: "not accepted"}, nil
	}
	if f.appendInvalidateThenReopen {
		f.appendInvalidateThenReopen = false
		f.requestStatus[req.requestID] = streamRequestStatusProvenAbsent
		return streamAppendResult{}, errStreamChannelInvalidated
	}
	if f.appendAuthExpiresOnce {
		f.appendAuthExpiresOnce = false
		f.requestStatus[req.requestID] = streamRequestStatusProvenAbsent
		return streamAppendResult{}, errStreamAuthExpired
	}
	if f.appendThrottleTimes > 0 {
		f.appendThrottleTimes--
		f.requestStatus[req.requestID] = streamRequestStatusProvenAbsent
		return streamAppendResult{}, errStreamThrottled
	}
	if f.appendRejectsRows {
		f.appendRejectsRows = false
		rejections := make([]streamRowRejection, 0, len(req.rows))
		for _, row := range req.rows {
			rejections = append(rejections, streamRowRejection{rowHash: row.rowHash, ordinal: row.ordinal, reason: "invalid row"})
		}
		f.requestStatus[req.requestID] = streamRequestStatusDivergent
		return streamAppendResult{disposition: streamAppendAccepted, requestID: req.requestID, rejections: rejections}, nil
	}
	for _, row := range req.rows {
		entry, ok := f.committed[row.rowHash]
		if !ok {
			entry = &fakeCommittedRow{logicalBatchID: request.logicalBatchID, visibleAt: f.observeCalls + f.commitVisibilityDelay}
			f.committed[row.rowHash] = entry
		}
		entry.count++
	}
	channel.committedOffsetToken = req.offsetToken
	channel.continuationToken = fmt.Sprintf("cont-%d-%d", channel.revision, f.appendCalls)
	f.requestStatus[req.requestID] = streamRequestStatusCommitted
	if f.appendCommitsThenThrottle {
		f.appendCommitsThenThrottle = false
		return streamAppendResult{}, errStreamThrottled
	}
	return streamAppendResult{disposition: streamAppendAccepted, requestID: req.requestID, continuationToken: channel.continuationToken, evidence: "accepted"}, nil
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

func (f *fakeStreamProtocol) RequestStatus(_ context.Context, _ streamConfig, request managedStreamRequest) (streamRequestStatusEvidence, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	status := f.requestStatus[request.requestID]
	if f.requestStatusUnknownTimes > 0 {
		f.requestStatusUnknownTimes--
		status = streamRequestUnknown
	}
	channel := f.channels[request.channelName]
	evidence := streamRequestStatusEvidence{
		disposition: status, requestID: request.requestID, channelName: request.channelName,
		channelRevision: request.channelRevision, pipeRevision: request.pipeRevision,
		inputContinuation: request.inputContinuation, requestedOffset: request.requestedOffset,
		manifestHash: request.manifestHash, rowsContentHash: request.rowsContentHash, rowCount: request.rowCount,
		detail: "fake authoritative request status",
	}
	if f.requestStatusDivergent {
		evidence.manifestHash = strings.Repeat("f", 64)
		evidence.disposition = streamRequestStatusDivergent
	}
	if channel != nil {
		evidence.responseContinuation = channel.continuationToken
		if status == streamRequestStatusCommitted && !f.statusSuppressCommittedToken {
			evidence.committedOffset = channel.committedOffsetToken
		}
	}
	return evidence, nil
}

func (f *fakeStreamProtocol) ObserveCommittedRows(_ context.Context, _ streamConfig, logicalBatchID string, _ []string) (map[string]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeCalls++
	present := make(map[string]int)
	for hash, entry := range f.committed {
		if entry.logicalBatchID != "" && entry.logicalBatchID != logicalBatchID || f.observeCalls < entry.visibleAt {
			continue
		}
		if entry.count > 0 {
			present[hash] = entry.count
		}
	}
	return present, nil
}

func (f *fakeStreamProtocol) CompareAndSwapChannelState(_ context.Context, _ streamConfig, expectedVersion int64, state managedStreamChannelState) (managedStreamChannelState, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.upsertCalls++
	key := fakeChannelStateKey(streamChannelStateKey{flowIncarnationID: state.flowIncarnationID, destinationRevisionID: state.destinationRevisionID, channelName: state.channelName})
	current, found := f.channelState[key]
	if !found {
		if expectedVersion != 0 || state.stateVersion != 1 {
			return managedStreamChannelState{}, false, nil
		}
		f.channelState[key] = state
		return state, true, nil
	}
	if current.stateVersion != expectedVersion || state.stateVersion != expectedVersion+1 || state.channelRevision < current.channelRevision {
		return current, false, nil
	}
	f.channelState[key] = state
	return state, true, nil
}

func (f *fakeStreamProtocol) LookupChannelState(_ context.Context, _ streamConfig, key streamChannelStateKey) (managedStreamChannelState, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	state, ok := f.channelState[fakeChannelStateKey(key)]
	return state, ok, nil
}

func (f *fakeStreamProtocol) InsertRequest(_ context.Context, _ streamConfig, request managedStreamRequest) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.requests[request.requestID]; exists {
		return false, nil
	}
	batchKey := fakeRequestBatchKey(streamRequestKey{flowIncarnationID: request.flowIncarnationID, destinationRevisionID: request.destinationRevisionID, logicalBatchID: request.logicalBatchID})
	if previousID := f.requestByBatch[batchKey]; previousID != "" {
		previous := f.requests[previousID]
		if previous.phase != streamRequestProvenAbsent && previous.phase != streamRequestReceipted {
			return false, fmt.Errorf("%w: unresolved request already owns the logical batch", connector.ErrDeliveryConflict)
		}
		if request.attempt <= previous.attempt {
			return false, fmt.Errorf("%w: request attempt did not increase", connector.ErrDeliveryConflict)
		}
	}
	f.requests[request.requestID] = request
	f.requestByBatch[batchKey] = request.requestID
	return true, nil
}

func (f *fakeStreamProtocol) LookupRequest(_ context.Context, _ streamConfig, key streamRequestKey) (managedStreamRequest, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.requestByBatch[fakeRequestBatchKey(key)]
	request, found := f.requests[id]
	return request, found, nil
}

func (f *fakeStreamProtocol) TransitionRequest(_ context.Context, _ streamConfig, transition streamRequestTransition) (managedStreamRequest, bool, error) {
	if !validStreamRequestTransition(transition.expectedPhase, transition.nextPhase) {
		return managedStreamRequest{}, false, errors.New("illegal request transition")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	request, found := f.requests[transition.requestID]
	if !found {
		return managedStreamRequest{}, false, errors.New("request not found")
	}
	if request.phase != transition.expectedPhase || request.phaseVersion != transition.expectedVersion {
		return request, false, nil
	}
	request.phase = transition.nextPhase
	request.phaseVersion++
	request.responseContinuation = transition.responseContinuation
	request.committedOffset = transition.committedOffset
	request.responseKind = transition.responseKind
	request.responseEvidence = transition.responseEvidence
	f.requests[request.requestID] = request
	return request, true, nil
}

func (f *fakeStreamProtocol) HasUnresolvedRequests(_ context.Context, _ streamConfig, key streamChannelStateKey) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, request := range f.requests {
		unresolved := request.phase == streamRequestPrepared || request.phase == streamRequestSendingUnknown || request.phase == streamRequestAccepted || request.phase == streamRequestCommitted
		if request.flowIncarnationID == key.flowIncarnationID && request.destinationRevisionID == key.destinationRevisionID && request.channelName == key.channelName && unresolved {
			return true, nil
		}
	}
	return false, nil
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
