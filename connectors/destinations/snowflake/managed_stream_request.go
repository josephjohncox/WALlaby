package snowflake

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

type streamRequestPhase string

const (
	streamRequestPrepared       streamRequestPhase = "PREPARED"
	streamRequestSendingUnknown streamRequestPhase = "SENDING_UNKNOWN"
	streamRequestAccepted       streamRequestPhase = "ACCEPTED"
	streamRequestCommitted      streamRequestPhase = "COMMITTED"
	streamRequestProvenAbsent   streamRequestPhase = "PROVEN_ABSENT"
	streamRequestRejected       streamRequestPhase = "REJECTED"
	streamRequestReceipted      streamRequestPhase = "RECEIPTED"
)

func validStreamRequestTransition(from, to streamRequestPhase) bool {
	switch from {
	case streamRequestPrepared:
		return to == streamRequestSendingUnknown || to == streamRequestProvenAbsent
	case streamRequestSendingUnknown:
		return to == streamRequestSendingUnknown || to == streamRequestAccepted || to == streamRequestCommitted || to == streamRequestProvenAbsent || to == streamRequestRejected
	case streamRequestAccepted:
		return to == streamRequestCommitted || to == streamRequestProvenAbsent || to == streamRequestRejected
	case streamRequestCommitted:
		return to == streamRequestCommitted || to == streamRequestReceipted
	case streamRequestProvenAbsent:
		return to == streamRequestProvenAbsent
	case streamRequestRejected:
		return to == streamRequestRejected
	case streamRequestReceipted:
		return to == streamRequestReceipted
	default:
		return false
	}
}

type streamAppendFailureOutcome uint8

const (
	streamAppendFailurePreSend streamAppendFailureOutcome = iota + 1
	streamAppendFailureDefinitelyNotAccepted
	streamAppendFailureAmbiguous
)

type streamAppendFailure struct {
	outcome streamAppendFailureOutcome
	cause   error
}

func (e *streamAppendFailure) Error() string {
	if e == nil || e.cause == nil {
		return "streaming Snowflake append failed"
	}
	return e.cause.Error()
}

func (e *streamAppendFailure) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func newStreamAppendFailure(outcome streamAppendFailureOutcome, cause error) error {
	if cause == nil {
		cause = errors.New("streaming Snowflake append failed")
	}
	return &streamAppendFailure{outcome: outcome, cause: cause}
}

func streamAppendFailureOutcomeOf(err error) streamAppendFailureOutcome {
	var failure *streamAppendFailure
	if errors.As(err, &failure) && failure.outcome != 0 {
		return failure.outcome
	}
	return streamAppendFailureAmbiguous
}

type streamAppendDisposition uint8

const (
	streamAppendAccepted streamAppendDisposition = iota
	streamAppendDefinitelyNotAccepted
	streamAppendUnknown
)

type streamRequestDisposition uint8

const (
	streamRequestUnknown streamRequestDisposition = iota
	streamRequestStatusCommitted
	streamRequestStatusProvenAbsent
	streamRequestStatusDivergent
)

type managedStreamRequest struct {
	requestID              string
	flowID                 string
	flowIncarnationID      string
	sourceLineageID        string
	destinationRevisionID  string
	logicalBatchID         string
	positionID             string
	contentHash            string
	manifestHash           string
	rowsContentHash        string
	rowCount               int
	channelName            string
	pipeName               string
	channelRevision        int64
	pipeRevision           string
	inputContinuation      string
	expectedPreviousOffset string
	requestedOffset        string
	responseContinuation   string
	committedOffset        string
	generation             int64
	acquisitionID          string
	leaseEpoch             int64
	attempt                int
	phase                  streamRequestPhase
	phaseVersion           int64
	responseKind           string
	responseEvidence       string
}

type streamRequestKey struct {
	flowIncarnationID     string
	destinationRevisionID string
	logicalBatchID        string
}

type streamRequestTransition struct {
	requestID            string
	expectedPhase        streamRequestPhase
	expectedVersion      int64
	nextPhase            streamRequestPhase
	responseContinuation string
	committedOffset      string
	responseKind         string
	responseEvidence     string
}

type streamRequestStatusEvidence struct {
	disposition            streamRequestDisposition
	requestID              string
	channelName            string
	pipeName               string
	channelRevision        int64
	pipeRevision           string
	inputContinuation      string
	expectedPreviousOffset string
	requestedOffset        string
	responseContinuation   string
	committedOffset        string
	manifestHash           string
	rowsContentHash        string
	rowCount               int
	detail                 string
}

func newManagedStreamRequest(plan managedStreamPlan, status streamChannelStatus, attempt int) (managedStreamRequest, error) {
	if attempt < 1 || status.channelRevision < 1 || status.channelName != plan.identity.channelName || strings.TrimSpace(status.pipeRevision) == "" || strings.TrimSpace(status.continuationToken) == "" {
		return managedStreamRequest{}, errors.New("streaming Snowflake request requires an exact channel revision, pipe revision, continuation, and positive attempt")
	}
	record := managedStreamRequest{
		flowID: plan.receipt.flowID, flowIncarnationID: plan.receipt.flowIncarnationID,
		sourceLineageID: plan.receipt.sourceLineageID, destinationRevisionID: plan.receipt.destinationRevisionID,
		logicalBatchID: plan.receipt.logicalBatchID, positionID: plan.receipt.positionID, contentHash: plan.receipt.contentHash,
		manifestHash: plan.identity.manifestHash, rowsContentHash: plan.rowsContentHash, rowCount: plan.rowCount,
		channelName: status.channelName, pipeName: plan.identity.pipeName, channelRevision: status.channelRevision, pipeRevision: status.pipeRevision,
		inputContinuation: status.continuationToken, expectedPreviousOffset: status.committedOffsetToken, requestedOffset: plan.identity.offsetToken,
		generation: plan.receipt.generation, acquisitionID: plan.receipt.acquisitionID, leaseEpoch: plan.receipt.leaseEpoch,
		attempt: attempt, phase: streamRequestPrepared, phaseVersion: 1,
	}
	identity := struct {
		Profile, FlowIncarnationID, DestinationRevisionID, LogicalBatchID, ChannelName, PipeName, PipeRevision string
		ChannelRevision                                                                                        int64
		InputContinuation, ExpectedPreviousOffset, RequestedOffset, ManifestHash, RowsContentHash              string
		RowCount, Attempt                                                                                      int
	}{
		Profile:           connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1,
		FlowIncarnationID: record.flowIncarnationID, DestinationRevisionID: record.destinationRevisionID,
		LogicalBatchID: record.logicalBatchID, ChannelName: record.channelName, PipeName: record.pipeName, PipeRevision: record.pipeRevision,
		ChannelRevision: record.channelRevision, InputContinuation: record.inputContinuation,
		ExpectedPreviousOffset: record.expectedPreviousOffset, RequestedOffset: record.requestedOffset, ManifestHash: record.manifestHash, RowsContentHash: record.rowsContentHash,
		RowCount: record.rowCount, Attempt: record.attempt,
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return managedStreamRequest{}, fmt.Errorf("encode streaming request identity: %w", err)
	}
	digest := sha256.Sum256(encoded)
	record.requestID = "wallaby-stream-request-" + hex.EncodeToString(digest[:])
	return record, record.validateIdentity()
}

func sameManagedStreamRequestIdentity(left, right managedStreamRequest) bool {
	return left.requestID == right.requestID && left.flowID == right.flowID && left.flowIncarnationID == right.flowIncarnationID &&
		left.sourceLineageID == right.sourceLineageID && left.destinationRevisionID == right.destinationRevisionID &&
		left.logicalBatchID == right.logicalBatchID && left.positionID == right.positionID && left.contentHash == right.contentHash &&
		left.manifestHash == right.manifestHash && left.rowsContentHash == right.rowsContentHash && left.rowCount == right.rowCount &&
		left.channelName == right.channelName && left.pipeName == right.pipeName && left.channelRevision == right.channelRevision && left.pipeRevision == right.pipeRevision &&
		left.inputContinuation == right.inputContinuation && left.expectedPreviousOffset == right.expectedPreviousOffset && left.requestedOffset == right.requestedOffset &&
		left.generation == right.generation && left.acquisitionID == right.acquisitionID && left.leaseEpoch == right.leaseEpoch && left.attempt == right.attempt
}

func (r managedStreamRequest) validateIdentity() error {
	if !strings.HasPrefix(r.requestID, "wallaby-stream-request-") || len(r.requestID) != len("wallaby-stream-request-")+64 ||
		r.flowID == "" || r.flowIncarnationID == "" || r.sourceLineageID == "" || r.destinationRevisionID == "" ||
		r.logicalBatchID == "" || r.positionID == "" || r.contentHash == "" || r.manifestHash == "" || r.rowsContentHash == "" ||
		r.rowCount <= 0 || r.channelName == "" || r.pipeName == "" || r.channelRevision <= 0 || r.pipeRevision == "" || r.inputContinuation == "" ||
		r.requestedOffset == "" || r.expectedPreviousOffset == r.requestedOffset || r.generation <= 0 || r.acquisitionID == "" || r.leaseEpoch <= 0 || r.attempt <= 0 || r.phaseVersion <= 0 {
		return errors.New("streaming Snowflake request identity is incomplete")
	}
	return nil
}

func validateStreamRequestEvidence(request managedStreamRequest, evidence streamRequestStatusEvidence) error {
	if evidence.requestID != request.requestID || evidence.channelName != request.channelName || evidence.pipeName != request.pipeName || evidence.channelRevision != request.channelRevision ||
		evidence.pipeRevision != request.pipeRevision || evidence.inputContinuation != request.inputContinuation || evidence.expectedPreviousOffset != request.expectedPreviousOffset || evidence.requestedOffset != request.requestedOffset ||
		evidence.manifestHash != request.manifestHash || evidence.rowsContentHash != request.rowsContentHash || evidence.rowCount != request.rowCount {
		return fmt.Errorf("%w: streaming Snowflake request status identity diverges", connector.ErrDeliveryConflict)
	}
	if evidence.disposition == streamRequestStatusCommitted && (strings.TrimSpace(evidence.responseContinuation) == "" || strings.TrimSpace(evidence.committedOffset) == "") {
		return fmt.Errorf("%w: committed streaming Snowflake request evidence is not yet complete", connector.ErrDeliveryIndeterminate)
	}
	if evidence.disposition == streamRequestStatusCommitted && evidence.committedOffset != request.requestedOffset {
		return fmt.Errorf("%w: committed streaming Snowflake offset differs from the exact request", connector.ErrDeliveryConflict)
	}
	if evidence.disposition == streamRequestStatusProvenAbsent && (evidence.responseContinuation != "" || evidence.committedOffset != "") {
		return fmt.Errorf("%w: proven-absent streaming Snowflake request carries commit evidence", connector.ErrDeliveryConflict)
	}
	// Offset tokens are opaque. An unknown status may report the exact prior
	// committed token or another value that cannot be ordered locally. Only the
	// exact requested token proves this request committed.
	return nil
}
