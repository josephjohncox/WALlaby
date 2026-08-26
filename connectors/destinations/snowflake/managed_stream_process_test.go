package snowflake

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type processRequest struct {
	RequestID, FlowID, FlowIncarnationID, SourceLineageID, DestinationRevisionID string
	LogicalBatchID, PositionID, ContentHash, ManifestHash, RowsContentHash       string
	ChannelName, PipeName, PipeRevision, InputContinuation, RequestedOffset      string
	ResponseContinuation, CommittedOffset, AcquisitionID, ResponseKind, Evidence string
	RowCount, Attempt                                                            int
	ChannelRevision, Generation, LeaseEpoch, PhaseVersion                        int64
	Phase                                                                        string
}

func processRequestFrom(value managedStreamRequest) processRequest {
	return processRequest{
		RequestID: value.requestID, FlowID: value.flowID, FlowIncarnationID: value.flowIncarnationID,
		SourceLineageID: value.sourceLineageID, DestinationRevisionID: value.destinationRevisionID,
		LogicalBatchID: value.logicalBatchID, PositionID: value.positionID, ContentHash: value.contentHash,
		ManifestHash: value.manifestHash, RowsContentHash: value.rowsContentHash, RowCount: value.rowCount,
		ChannelName: value.channelName, PipeName: value.pipeName, ChannelRevision: value.channelRevision,
		PipeRevision: value.pipeRevision, InputContinuation: value.inputContinuation, RequestedOffset: value.requestedOffset,
		ResponseContinuation: value.responseContinuation, CommittedOffset: value.committedOffset,
		Generation: value.generation, AcquisitionID: value.acquisitionID, LeaseEpoch: value.leaseEpoch,
		Attempt: value.attempt, Phase: string(value.phase), PhaseVersion: value.phaseVersion,
		ResponseKind: value.responseKind, Evidence: value.responseEvidence,
	}
}

func (value processRequest) request() managedStreamRequest {
	return managedStreamRequest{
		requestID: value.RequestID, flowID: value.FlowID, flowIncarnationID: value.FlowIncarnationID,
		sourceLineageID: value.SourceLineageID, destinationRevisionID: value.DestinationRevisionID,
		logicalBatchID: value.LogicalBatchID, positionID: value.PositionID, contentHash: value.ContentHash,
		manifestHash: value.ManifestHash, rowsContentHash: value.RowsContentHash, rowCount: value.RowCount,
		channelName: value.ChannelName, pipeName: value.PipeName, channelRevision: value.ChannelRevision,
		pipeRevision: value.PipeRevision, inputContinuation: value.InputContinuation, requestedOffset: value.RequestedOffset,
		responseContinuation: value.ResponseContinuation, committedOffset: value.CommittedOffset,
		generation: value.Generation, acquisitionID: value.AcquisitionID, leaseEpoch: value.LeaseEpoch,
		attempt: value.Attempt, phase: streamRequestPhase(value.Phase), phaseVersion: value.PhaseVersion,
		responseKind: value.ResponseKind, responseEvidence: value.Evidence,
	}
}

type processChannelState struct {
	FlowIncarnationID, DestinationRevisionID, ChannelName, PipeName, PipeRevision string
	Continuation, CommittedOffset, LogicalBatchID, RowsContentHash, RequestID     string
	ChannelRevision, StateVersion                                                 int64
}

func processChannelStateFrom(value managedStreamChannelState) processChannelState {
	return processChannelState{
		FlowIncarnationID: value.flowIncarnationID, DestinationRevisionID: value.destinationRevisionID,
		ChannelName: value.channelName, PipeName: value.pipeName, PipeRevision: value.pipeRevision,
		Continuation: value.continuationToken, CommittedOffset: value.committedOffsetToken,
		LogicalBatchID: value.logicalBatchID, RowsContentHash: value.rowsContentHash, RequestID: value.requestID,
		ChannelRevision: value.channelRevision, StateVersion: value.stateVersion,
	}
}

func (value processChannelState) state() managedStreamChannelState {
	return managedStreamChannelState{
		flowIncarnationID: value.FlowIncarnationID, destinationRevisionID: value.DestinationRevisionID,
		channelName: value.ChannelName, pipeName: value.PipeName, pipeRevision: value.PipeRevision,
		continuationToken: value.Continuation, committedOffsetToken: value.CommittedOffset,
		logicalBatchID: value.LogicalBatchID, rowsContentHash: value.RowsContentHash, requestID: value.RequestID,
		channelRevision: value.ChannelRevision, stateVersion: value.StateVersion,
	}
}

type processReceipt struct {
	Kind, ProfileVersion, FlowID, FlowIncarnationID, SourceLineageID, DestinationRevisionID string
	LogicalBatchID, PositionID, ContentHash, SchemaContractHash, CatalogFingerprint         string
	ManifestHash, ExternalID, RequestID, AcquisitionID, ChannelName, OffsetToken            string
	PipeRevision, CommittedOffsetToken, RowsContentHash, ReceiptStatus                      string
	Generation, LeaseEpoch, ChannelRevision                                                 int64
	TransactionID                                                                           uint32
	FragmentCount, RecordCount                                                              int
}

func processReceiptFrom(value managedStreamReceipt) processReceipt {
	return processReceipt{
		Kind: value.kind, ProfileVersion: value.profileVersion, FlowID: value.flowID,
		FlowIncarnationID: value.flowIncarnationID, SourceLineageID: value.sourceLineageID,
		DestinationRevisionID: value.destinationRevisionID, LogicalBatchID: value.logicalBatchID,
		PositionID: value.positionID, ContentHash: value.contentHash, SchemaContractHash: value.schemaContractHash,
		CatalogFingerprint: value.catalogFingerprint, ManifestHash: value.manifestHash, ExternalID: value.externalID,
		RequestID: value.requestID, Generation: value.generation, AcquisitionID: value.acquisitionID,
		LeaseEpoch: value.leaseEpoch, TransactionID: value.transactionID, FragmentCount: value.fragmentCount,
		RecordCount: value.recordCount, ChannelName: value.channelName, OffsetToken: value.offsetToken,
		PipeRevision: value.pipeRevision, ChannelRevision: value.channelRevision,
		CommittedOffsetToken: value.committedOffsetToken, RowsContentHash: value.rowsContentHash,
		ReceiptStatus: value.receiptStatus,
	}
}

func (value processReceipt) receipt() managedStreamReceipt {
	return managedStreamReceipt{
		kind: value.Kind, profileVersion: value.ProfileVersion, flowID: value.FlowID,
		flowIncarnationID: value.FlowIncarnationID, sourceLineageID: value.SourceLineageID,
		destinationRevisionID: value.DestinationRevisionID, logicalBatchID: value.LogicalBatchID,
		positionID: value.PositionID, contentHash: value.ContentHash, schemaContractHash: value.SchemaContractHash,
		catalogFingerprint: value.CatalogFingerprint, manifestHash: value.ManifestHash, externalID: value.ExternalID,
		requestID: value.RequestID, generation: value.Generation, acquisitionID: value.AcquisitionID,
		leaseEpoch: value.LeaseEpoch, transactionID: value.TransactionID, fragmentCount: value.FragmentCount,
		recordCount: value.RecordCount, channelName: value.ChannelName, offsetToken: value.OffsetToken,
		pipeRevision: value.PipeRevision, channelRevision: value.ChannelRevision,
		committedOffsetToken: value.CommittedOffsetToken, rowsContentHash: value.RowsContentHash,
		receiptStatus: value.ReceiptStatus,
	}
}

type processStreamState struct {
	ChannelRevision, StateVersion               int64
	PipeRevision, Continuation, CommittedOffset string
	Request                                     *processRequest
	ChannelState                                *processChannelState
	RequestStatus                               int
	CommittedRows                               map[string]string
	Receipt                                     *processReceipt
	AppendCalls, InsertCalls                    int
}

type processStreamProtocol struct {
	mu   sync.Mutex
	path string
}

func newProcessStreamProtocol(path string) *processStreamProtocol {
	return &processStreamProtocol{path: path}
}

func (p *processStreamProtocol) load() (processStreamState, error) {
	encoded, err := os.ReadFile(p.path)
	if errors.Is(err, os.ErrNotExist) {
		return processStreamState{CommittedRows: map[string]string{}}, nil
	}
	if err != nil {
		return processStreamState{}, err
	}
	var state processStreamState
	if err := json.Unmarshal(encoded, &state); err != nil {
		return processStreamState{}, err
	}
	if state.CommittedRows == nil {
		state.CommittedRows = map[string]string{}
	}
	return state, nil
}

func (p *processStreamProtocol) save(state processStreamState) error {
	encoded, err := json.Marshal(state)
	if err != nil {
		return err
	}
	temporary := p.path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(encoded); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(temporary, p.path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(p.path))
	if err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return nil
}

func (p *processStreamProtocol) mutate(update func(*processStreamState) error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	state, err := p.load()
	if err != nil {
		return err
	}
	if err := update(&state); err != nil {
		return err
	}
	return p.save(state)
}

func (p *processStreamProtocol) OpenChannel(_ context.Context, cfg streamConfig, channelName string) (streamChannelStatus, error) {
	var result streamChannelStatus
	err := p.mutate(func(state *processStreamState) error {
		if state.ChannelRevision == 0 {
			state.ChannelRevision = 1
			state.PipeRevision = "pipe-rev-1"
			state.Continuation = "cont-1"
		}
		result = streamChannelStatus{valid: true, channelName: channelName, channelRevision: state.ChannelRevision, pipeRevision: state.PipeRevision, continuationToken: state.Continuation, committedOffsetToken: state.CommittedOffset}
		return nil
	})
	_ = cfg
	return result, err
}

func (p *processStreamProtocol) AppendRows(_ context.Context, request streamAppendRequest) (streamAppendResult, error) {
	var result streamAppendResult
	err := p.mutate(func(state *processStreamState) error {
		if state.Request == nil || state.Request.RequestID != request.requestID || state.Request.Phase != string(streamRequestSendingUnknown) {
			return errors.New("append lacks durable send claim")
		}
		state.AppendCalls++
		for _, row := range request.rows {
			state.CommittedRows[row.rowHash] = state.Request.LogicalBatchID
		}
		state.Continuation = "cont-committed"
		state.CommittedOffset = request.offsetToken
		state.RequestStatus = int(streamRequestStatusCommitted)
		result = streamAppendResult{disposition: streamAppendAccepted, requestID: request.requestID, continuationToken: state.Continuation, evidence: "accepted"}
		return nil
	})
	return result, err
}

func (p *processStreamProtocol) ChannelStatus(_ context.Context, _ streamConfig, channelName string) (streamChannelStatus, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state, err := p.load()
	return streamChannelStatus{valid: true, channelName: channelName, channelRevision: state.ChannelRevision, pipeRevision: state.PipeRevision, continuationToken: state.Continuation, committedOffsetToken: state.CommittedOffset}, err
}

func (p *processStreamProtocol) RequestStatus(_ context.Context, _ streamConfig, request managedStreamRequest) (streamRequestStatusEvidence, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state, err := p.load()
	if err != nil {
		return streamRequestStatusEvidence{}, err
	}
	disposition := streamRequestDisposition(state.RequestStatus)
	if state.AppendCalls == 0 {
		disposition = streamRequestStatusProvenAbsent
	}
	evidence := streamRequestStatusEvidence{disposition: disposition, requestID: request.requestID, channelName: request.channelName, pipeName: request.pipeName, channelRevision: request.channelRevision, pipeRevision: request.pipeRevision, inputContinuation: request.inputContinuation, requestedOffset: request.requestedOffset, manifestHash: request.manifestHash, rowsContentHash: request.rowsContentHash, rowCount: request.rowCount, detail: "durable process store"}
	if disposition == streamRequestStatusCommitted {
		evidence.responseContinuation = state.Continuation
		evidence.committedOffset = state.CommittedOffset
	}
	return evidence, nil
}

func (p *processStreamProtocol) ObserveCommittedRows(_ context.Context, _ streamConfig, logicalBatchID string, _ []string) (map[string]int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state, err := p.load()
	if err != nil {
		return nil, err
	}
	result := map[string]int{}
	for hash, batch := range state.CommittedRows {
		if batch == logicalBatchID {
			result[hash]++
		}
	}
	return result, nil
}

func (p *processStreamProtocol) CompareAndSwapChannelState(_ context.Context, _ streamConfig, expected, candidate managedStreamChannelState) (managedStreamChannelState, bool, error) {
	var current managedStreamChannelState
	applied := false
	err := p.mutate(func(state *processStreamState) error {
		if state.ChannelState == nil {
			if expected.stateVersion != 0 || candidate.stateVersion != 1 {
				return nil
			}
		} else {
			current = state.ChannelState.state()
			if current != expected {
				return nil
			}
		}
		copy := processChannelStateFrom(candidate)
		state.ChannelState = &copy
		current = candidate
		applied = true
		return nil
	})
	return current, applied, err
}

func (p *processStreamProtocol) LookupChannelState(_ context.Context, _ streamConfig, _ streamChannelStateKey) (managedStreamChannelState, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state, err := p.load()
	if err != nil || state.ChannelState == nil {
		return managedStreamChannelState{}, false, err
	}
	return state.ChannelState.state(), true, nil
}

func (p *processStreamProtocol) InsertRequest(_ context.Context, _ streamConfig, request managedStreamRequest) (bool, error) {
	inserted := false
	err := p.mutate(func(state *processStreamState) error {
		if state.Request != nil {
			current := state.Request.request()
			if current.requestID == request.requestID {
				return nil
			}
			if current.phase != streamRequestProvenAbsent || request.attempt <= current.attempt {
				return errors.New("unresolved durable request owns the logical batch")
			}
		}
		copy := processRequestFrom(request)
		state.Request = &copy
		inserted = true
		return nil
	})
	return inserted, err
}
func (p *processStreamProtocol) LookupRequest(_ context.Context, _ streamConfig, _ streamRequestKey) (managedStreamRequest, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state, err := p.load()
	if err != nil || state.Request == nil {
		return managedStreamRequest{}, false, err
	}
	return state.Request.request(), true, nil
}
func (p *processStreamProtocol) TransitionRequest(_ context.Context, _ streamConfig, transition streamRequestTransition) (managedStreamRequest, bool, error) {
	var current managedStreamRequest
	applied := false
	err := p.mutate(func(state *processStreamState) error {
		if state.Request == nil {
			return errors.New("request absent")
		}
		current = state.Request.request()
		if current.phase != transition.expectedPhase || current.phaseVersion != transition.expectedVersion {
			return nil
		}
		current.phase = transition.nextPhase
		current.phaseVersion++
		current.responseContinuation = transition.responseContinuation
		current.committedOffset = transition.committedOffset
		current.responseKind = transition.responseKind
		current.responseEvidence = transition.responseEvidence
		copy := processRequestFrom(current)
		state.Request = &copy
		applied = true
		return nil
	})
	return current, applied, err
}
func (p *processStreamProtocol) HasUnresolvedRequests(context.Context, streamConfig, streamChannelStateKey) (bool, error) {
	return false, nil
}
func (p *processStreamProtocol) LookupReceipt(_ context.Context, _ streamConfig, _ streamReceiptKey) (managedStreamReceipt, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state, err := p.load()
	if err != nil || state.Receipt == nil {
		return managedStreamReceipt{}, false, err
	}
	return state.Receipt.receipt(), true, nil
}
func (p *processStreamProtocol) InsertReceipt(_ context.Context, _ streamConfig, receipt managedStreamReceipt) (streamReceiptInsert, error) {
	inserted := false
	err := p.mutate(func(state *processStreamState) error {
		if state.Receipt != nil {
			return nil
		}
		copy := processReceiptFrom(receipt)
		state.Receipt = &copy
		state.InsertCalls++
		inserted = true
		return nil
	})
	return streamReceiptInsert{inserted: inserted}, err
}
func (*processStreamProtocol) ListReleasableReceipts(context.Context, streamConfig, string, time.Duration, int) ([]managedStreamReceipt, error) {
	return nil, nil
}
func (*processStreamProtocol) ReleaseChannelState(context.Context, streamConfig, managedStreamChannelState, managedStreamReceipt) (bool, error) {
	return false, nil
}

func TestStreamRequestProcessRestartUsesDurableStore(t *testing.T) {
	if os.Getenv("WALLABY_STREAM_DURABLE_CHILD") == "1" {
		cfg, transaction, intent, _ := streamTestFixture(t)
		protocol := newProcessStreamProtocol(os.Getenv("WALLABY_STREAM_DURABLE_STATE"))
		ready := os.Getenv("WALLABY_STREAM_DURABLE_READY")
		phase := os.Getenv("WALLABY_STREAM_DURABLE_PHASE")
		hook := func() error {
			if err := os.WriteFile(ready, []byte("ready"), 0o600); err != nil {
				return err
			}
			for {
				time.Sleep(time.Hour)
			}
		}
		hooks := streamingHooks{}
		if phase == "sending_unknown" {
			hooks.AfterSendClaim = hook
		} else {
			hooks.AfterAccepted = hook
		}
		_, _ = newStreamDriver(protocol, cfg, "catalog-fingerprint", hooks).apply(context.Background(), intent, transaction)
		os.Exit(2)
	}
	for _, phase := range []string{"sending_unknown", "accepted"} {
		t.Run(phase, func(t *testing.T) {
			directory := t.TempDir()
			statePath := directory + "/state.json"
			readyPath := directory + "/ready"
			cmd := exec.Command(os.Args[0], "-test.run=^TestStreamRequestProcessRestartUsesDurableStore$/"+phase+"$")
			cmd.Env = append(os.Environ(), "WALLABY_STREAM_DURABLE_CHILD=1", "WALLABY_STREAM_DURABLE_STATE="+statePath, "WALLABY_STREAM_DURABLE_READY="+readyPath, "WALLABY_STREAM_DURABLE_PHASE="+phase)
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				if _, err := os.Stat(readyPath); err == nil {
					break
				}
				if time.Now().After(deadline) {
					_ = cmd.Process.Kill()
					t.Fatal("durable helper did not reach real driver crash boundary")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err := cmd.Wait(); err == nil {
				t.Fatal("durable helper unexpectedly exited without SIGKILL")
			}
			cfg, transaction, intent, _ := streamTestFixture(t)
			protocol := newProcessStreamProtocol(statePath)
			if _, err := newStreamTestDriver(cfg, protocol).apply(context.Background(), intent, transaction); err != nil {
				t.Fatalf("fresh-process reconciliation: %v", err)
			}
			protocol.mu.Lock()
			state, err := protocol.load()
			protocol.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if state.AppendCalls != 1 || state.InsertCalls != 1 || state.Receipt == nil {
				t.Fatalf("durable append/receipt=%d/%d/%t, want 1/1/true", state.AppendCalls, state.InsertCalls, state.Receipt != nil)
			}
		})
	}
}
