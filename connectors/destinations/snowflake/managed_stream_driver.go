package snowflake

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// streamingHooks exposes deterministic fault boundaries to same-package protocol tests.
type streamingHooks struct {
	AfterOpen      func() error
	AfterAppend    func() error
	AfterSendClaim func() error
	BeforeReceipt  func() error
	AfterReceipt   func() error
	RefreshAuth    func(context.Context) error
}

// streamDriver orchestrates the Snowpipe Streaming append protocol against a
// streamProtocol seam. All crash-window recovery lives here so it can be proven
// exhaustively with an in-memory protocol fake. It never adopts a delivery from
// a transport token alone: deterministic row identity plus SQL-observed
// completeness plus a durable receipt are the adoption authority.
type streamDriver struct {
	proto              streamProtocol
	cfg                streamConfig
	catalogFingerprint string
	hooks              streamingHooks
	sleep              func(context.Context, time.Duration) error
}

func newStreamDriver(proto streamProtocol, cfg streamConfig, catalogFingerprint string, hooks streamingHooks) *streamDriver {
	return &streamDriver{proto: proto, cfg: cfg, catalogFingerprint: catalogFingerprint, hooks: hooks}
}

func (p managedStreamPlan) appendReceiptKey() streamReceiptKey {
	return streamReceiptKey{
		flowIncarnationID: p.receipt.flowIncarnationID, destinationRevisionID: p.receipt.destinationRevisionID,
		logicalBatchID: p.receipt.logicalBatchID, sourceLineageID: p.receipt.sourceLineageID,
		positionID: p.receipt.positionID, externalID: p.receipt.externalID, kind: streamReceiptKindAppend,
	}
}

// apply materializes one committed transaction as an ordered set of
// deterministic-identity append rows, appends only the rows SQL observation
// proves are missing, verifies SQL-observed completeness, and records a durable
// receipt. Every step is idempotent so a replay after any crash window converges
// on exactly one append receipt.
func (d *streamDriver) apply(ctx context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (evidence connector.DeliveryEvidence, resultErr error) {
	plan, err := planManagedStreamTransaction(d.cfg, intent, transaction)
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	plan.catalogFingerprint = d.catalogFingerprint
	plan.receipt.catalogFingerprint = d.catalogFingerprint

	ctx, endSpan := telemetry.StartSnowflakeManagedSpan(ctx, "channel", plan.identity.externalID, intent.LogicalBatchID, int64(plan.rowCount), plan.encodedBytes)
	defer func() { endSpan(resultErr) }()
	if err := ctx.Err(); err != nil {
		return connector.DeliveryEvidence{}, err
	}

	if existing, found, lookupErr := d.proto.LookupReceipt(ctx, d.cfg, plan.appendReceiptKey()); lookupErr != nil {
		return connector.DeliveryEvidence{}, lookupErr
	} else if found {
		if err := validateStreamReceipt(plan.receipt, existing); err != nil {
			return connector.DeliveryEvidence{}, err
		}
		return connector.DeliveryEvidence{ExternalID: existing.externalID, ContentHash: existing.contentHash}, nil
	}

	requestKey := streamRequestKey{flowIncarnationID: plan.receipt.flowIncarnationID, destinationRevisionID: plan.receipt.destinationRevisionID, logicalBatchID: plan.receipt.logicalBatchID}
	request, requestFound, err := d.proto.LookupRequest(ctx, d.cfg, requestKey)
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	var status streamChannelStatus
	committed := false
	nextAttempt := 1
	if requestFound {
		if err := d.validateRequestPlan(plan, request); err != nil {
			return connector.DeliveryEvidence{}, err
		}
		nextAttempt = request.attempt + 1
		switch request.phase {
		case streamRequestReceipted:
			return connector.DeliveryEvidence{}, fmt.Errorf("%w: request is receipted but the durable receipt is absent", connector.ErrDeliveryConflict)
		case streamRequestRejected:
			return connector.DeliveryEvidence{}, fmt.Errorf("%w: durable streaming request was terminally rejected: %s", connector.ErrDeliveryConflict, request.responseEvidence)
		case streamRequestProvenAbsent:
		case streamRequestCommitted:
			if request.committedOffset == "" || request.responseContinuation == "" {
				return connector.DeliveryEvidence{}, fmt.Errorf("%w: committed request evidence is incomplete", connector.ErrDeliveryIndeterminate)
			}
			if request.committedOffset != request.requestedOffset {
				return connector.DeliveryEvidence{}, fmt.Errorf("%w: committed request offset diverges", connector.ErrDeliveryConflict)
			}
			status = streamChannelStatus{valid: true, channelName: request.channelName, channelRevision: request.channelRevision, pipeRevision: request.pipeRevision, continuationToken: request.responseContinuation, committedOffsetToken: request.committedOffset}
			committed = true
		case streamRequestSendingUnknown, streamRequestAccepted, streamRequestPrepared:
			status, request, committed, err = d.reconcileRequest(ctx, plan, request)
			if err != nil {
				return connector.DeliveryEvidence{}, err
			}
		default:
			return connector.DeliveryEvidence{}, fmt.Errorf("%w: unknown streaming Snowflake request phase %q", connector.ErrDeliveryConflict, request.phase)
		}
	}
	if !committed {
		status, err = d.openAndPersistChannel(ctx, plan, plan.rowsContentHash)
		if err != nil {
			return connector.DeliveryEvidence{}, err
		}
		if hook := d.hooks.AfterOpen; hook != nil {
			if err := hook(); err != nil {
				return connector.DeliveryEvidence{}, fmt.Errorf("%w: injected after streaming Snowflake channel open: %w", connector.ErrDeliveryIndeterminate, err)
			}
		}
		missing, err := d.provenMissingRows(ctx, plan, plan.rows)
		if err != nil {
			return connector.DeliveryEvidence{}, err
		}
		if len(missing) != len(plan.rows) {
			return connector.DeliveryEvidence{}, fmt.Errorf("%w: target rows exist without a committed durable request", connector.ErrDeliveryIndeterminate)
		}
		status, request, err = d.appendRequest(ctx, plan, status, missing, nextAttempt)
		if err != nil {
			return connector.DeliveryEvidence{}, err
		}
	}

	committedToken, err := d.verifyObservedCompleteness(ctx, plan, status, true)
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	plan.receipt.channelRevision = request.channelRevision
	plan.receipt.pipeRevision = request.pipeRevision
	plan.receipt.committedOffsetToken = committedToken
	plan.receipt.requestID = request.requestID

	if hook := d.hooks.BeforeReceipt; hook != nil {
		if err := hook(); err != nil {
			return connector.DeliveryEvidence{}, fmt.Errorf("before streaming Snowflake receipt: %w", err)
		}
	}
	receiptCtx, endReceipt := telemetry.StartSnowflakeManagedSpan(ctx, "receipt", plan.identity.externalID, intent.LogicalBatchID, 1, 0)
	insert, insertErr := d.proto.InsertReceipt(receiptCtx, d.cfg, plan.receipt)
	endReceipt(insertErr)
	if insertErr != nil {
		return connector.DeliveryEvidence{}, insertErr
	}
	if !insert.inserted {
		existing, found, lookupErr := d.proto.LookupReceipt(ctx, d.cfg, plan.appendReceiptKey())
		if lookupErr != nil {
			return connector.DeliveryEvidence{}, lookupErr
		}
		if !found {
			return connector.DeliveryEvidence{}, fmt.Errorf("%w: streaming Snowflake receipt insert reported a duplicate that is not visible", connector.ErrDeliveryIndeterminate)
		}
		if err := validateStreamReceipt(plan.receipt, existing); err != nil {
			return connector.DeliveryEvidence{}, err
		}
		if err := d.markRequestReceipted(ctx, request); err != nil {
			return connector.DeliveryEvidence{}, err
		}
		return connector.DeliveryEvidence{ExternalID: existing.externalID, ContentHash: existing.contentHash}, nil
	}
	if hook := d.hooks.AfterReceipt; hook != nil {
		if err := hook(); err != nil {
			return connector.DeliveryEvidence{}, fmt.Errorf("%w: injected after streaming Snowflake receipt: %w", connector.ErrDeliveryIndeterminate, err)
		}
	}
	if err := d.markRequestReceipted(ctx, request); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	return connector.DeliveryEvidence{ExternalID: plan.receipt.externalID, ContentHash: plan.receipt.contentHash}, nil
}

// openAndPersistChannel opens (or reopens) the deterministic channel and durably
// records its exact revision, continuation, and committed-token evidence.
func (d *streamDriver) openAndPersistChannel(ctx context.Context, plan managedStreamPlan, rowsContentHash string) (streamChannelStatus, error) {
	status, err := d.proto.OpenChannel(ctx, d.cfg, plan.identity.channelName)
	if err != nil {
		return streamChannelStatus{}, err
	}
	if !status.valid || status.channelName != plan.identity.channelName {
		return streamChannelStatus{}, fmt.Errorf("%w: streaming Snowflake channel %q did not open cleanly", connector.ErrDeliveryIndeterminate, plan.identity.channelName)
	}
	if err := d.persistChannelState(ctx, plan, status, rowsContentHash); err != nil {
		return streamChannelStatus{}, err
	}
	return status, nil
}

func (d *streamDriver) persistChannelState(ctx context.Context, plan managedStreamPlan, status streamChannelStatus, rowsContentHash string) error {
	if status.channelRevision <= 0 || status.channelName != plan.identity.channelName || status.pipeRevision == "" || status.continuationToken == "" {
		return fmt.Errorf("%w: incomplete streaming Snowflake channel evidence", connector.ErrDeliveryIndeterminate)
	}
	key := streamChannelStateKey{flowIncarnationID: plan.receipt.flowIncarnationID, destinationRevisionID: plan.receipt.destinationRevisionID, channelName: plan.identity.channelName}
	current, found, err := d.proto.LookupChannelState(ctx, d.cfg, key)
	if err != nil {
		return err
	}
	expected := managedStreamChannelState{}
	if found {
		expected = current
		if current.pipeName != d.cfg.pipe || current.pipeRevision != status.pipeRevision || current.channelRevision > status.channelRevision {
			return fmt.Errorf("%w: streaming Snowflake channel identity or revision regressed", connector.ErrDeliveryConflict)
		}
		if current.channelRevision == status.channelRevision && current.committedOffsetToken != "" && status.committedOffsetToken == "" {
			return fmt.Errorf("%w: streaming Snowflake committed offset evidence regressed", connector.ErrDeliveryConflict)
		}
		if current.channelRevision == status.channelRevision && current.logicalBatchID == plan.receipt.logicalBatchID && current.rowsContentHash != rowsContentHash {
			return fmt.Errorf("%w: same-revision streaming Snowflake request identity diverges", connector.ErrDeliveryConflict)
		}
	}
	if status.committedOffsetToken != "" && status.committedOffsetToken != plan.identity.offsetToken {
		return fmt.Errorf("%w: streaming Snowflake committed offset is not the exact request offset", connector.ErrDeliveryConflict)
	}
	state := managedStreamChannelState{
		flowIncarnationID: plan.receipt.flowIncarnationID, destinationRevisionID: plan.receipt.destinationRevisionID,
		channelName: plan.identity.channelName, pipeName: d.cfg.pipe, pipeRevision: status.pipeRevision,
		channelRevision: status.channelRevision, continuationToken: status.continuationToken,
		committedOffsetToken: status.committedOffsetToken, logicalBatchID: plan.receipt.logicalBatchID,
		rowsContentHash: rowsContentHash, stateVersion: expected.stateVersion + 1,
	}
	current, applied, err := d.proto.CompareAndSwapChannelState(ctx, d.cfg, expected, state)
	if err != nil {
		return err
	}
	if !applied {
		return fmt.Errorf("%w: streaming Snowflake channel state CAS lost to version %d", connector.ErrDeliveryIndeterminate, current.stateVersion)
	}
	return nil
}

// provenMissingRows returns the rows whose deterministic identity is not yet
// present in the target, and fails closed on any duplicate-identity observation.
func (d *streamDriver) provenMissingRows(ctx context.Context, plan managedStreamPlan, candidate []streamChangelogRow) ([]streamChangelogRow, error) {
	observeCtx, endObserve := telemetry.StartSnowflakeManagedSpan(ctx, "observe", plan.identity.externalID, plan.receipt.logicalBatchID, int64(len(candidate)), 0)
	observed, err := d.proto.ObserveCommittedRows(observeCtx, d.cfg, plan.receipt.logicalBatchID, rowHashesOf(candidate))
	endObserve(err)
	if err != nil {
		return nil, err
	}
	expected := make(map[string]struct{}, len(plan.rows))
	for index := range plan.rows {
		expected[plan.rows[index].RowHash] = struct{}{}
	}
	for hash, count := range observed {
		if _, ok := expected[hash]; !ok || count != 1 {
			return nil, fmt.Errorf("%w: unexpected or duplicate row hash %s observed %d times for logical batch %s", errStreamObservationInconsistent, hash, count, plan.receipt.logicalBatchID)
		}
	}
	missing := make([]streamChangelogRow, 0, len(candidate))
	for index := range candidate {
		count := observed[candidate[index].RowHash]
		if count > 1 {
			return nil, fmt.Errorf("%w: row hash %s observed %d times for logical batch %s", errStreamObservationInconsistent, candidate[index].RowHash, count, plan.receipt.logicalBatchID)
		}
		if count == 0 {
			missing = append(missing, candidate[index])
		}
	}
	return missing, nil
}

func (d *streamDriver) appendRequest(ctx context.Context, plan managedStreamPlan, status streamChannelStatus, missing []streamChangelogRow, firstAttempt int) (streamChannelStatus, managedStreamRequest, error) {
	if err := d.assertAppendSize(missing); err != nil {
		return streamChannelStatus{}, managedStreamRequest{}, err
	}
	attempts := d.cfg.appendAttempts
	if attempts < 1 {
		attempts = 1
	}
	for attempt := firstAttempt; attempt < firstAttempt+attempts; attempt++ {
		request, err := newManagedStreamRequest(plan, status, attempt)
		if err != nil {
			return streamChannelStatus{}, managedStreamRequest{}, err
		}
		inserted, err := d.proto.InsertRequest(ctx, d.cfg, request)
		if err != nil {
			return streamChannelStatus{}, managedStreamRequest{}, err
		}
		if !inserted {
			existing, found, err := d.proto.LookupRequest(ctx, d.cfg, streamRequestKey{flowIncarnationID: request.flowIncarnationID, destinationRevisionID: request.destinationRevisionID, logicalBatchID: request.logicalBatchID})
			if err != nil {
				return streamChannelStatus{}, managedStreamRequest{}, fmt.Errorf("%w: lookup duplicate streaming request: %w", connector.ErrDeliveryIndeterminate, err)
			}
			if !found {
				return streamChannelStatus{}, managedStreamRequest{}, fmt.Errorf("%w: duplicate request identity is not visible", connector.ErrDeliveryIndeterminate)
			}
			if !sameManagedStreamRequestIdentity(request, existing) {
				return streamChannelStatus{}, managedStreamRequest{}, fmt.Errorf("%w: duplicate streaming request identity diverges", connector.ErrDeliveryConflict)
			}
			request = existing
			if err := d.validateRequestPlan(plan, request); err != nil {
				return streamChannelStatus{}, managedStreamRequest{}, err
			}
		}
		request, sendOwner, err := d.claimRequestSend(ctx, request)
		if err != nil {
			return streamChannelStatus{}, managedStreamRequest{}, err
		}
		if !sendOwner {
			var committed bool
			status, request, committed, err = d.reconcileRequest(ctx, plan, request)
			if err != nil {
				return streamChannelStatus{}, managedStreamRequest{}, err
			}
			if committed {
				return status, request, nil
			}
			if request.phase != streamRequestProvenAbsent {
				return streamChannelStatus{}, managedStreamRequest{}, fmt.Errorf("%w: send claim is owned by another writer", connector.ErrDeliveryIndeterminate)
			}
			status, err = d.openAndPersistChannel(ctx, plan, plan.rowsContentHash)
			if err != nil {
				return streamChannelStatus{}, managedStreamRequest{}, err
			}
			continue
		}
		if hook := d.hooks.AfterSendClaim; hook != nil {
			if err := hook(); err != nil {
				return streamChannelStatus{}, managedStreamRequest{}, err
			}
		}
		if err := ctx.Err(); err != nil {
			return streamChannelStatus{}, managedStreamRequest{}, err
		}
		req := streamAppendRequest{
			cfg: d.cfg, requestID: request.requestID, channelName: request.channelName, channelRevision: request.channelRevision,
			pipeRevision: request.pipeRevision, continuationToken: request.inputContinuation, offsetToken: request.requestedOffset,
			manifestHash: request.manifestHash, rowsContentHash: request.rowsContentHash, rowCount: request.rowCount,
			rows: appendRowsOf(missing),
		}
		appendCtx, endAppend := telemetry.StartSnowflakeManagedSpan(ctx, "append", request.requestID, plan.receipt.logicalBatchID, int64(len(missing)), 0)
		result, appendErr := d.proto.AppendRows(appendCtx, req)
		endAppend(appendErr)
		if appendErr != nil {
			request, err = d.transitionRequestEvidence(context.WithoutCancel(ctx), request, streamRequestSendingUnknown, request.responseContinuation, "transport_error", appendErr.Error(), "")
			if err != nil {
				return streamChannelStatus{}, managedStreamRequest{}, err
			}
			if ctx.Err() != nil {
				return streamChannelStatus{}, managedStreamRequest{}, ctx.Err()
			}
		}
		if appendErr == nil && len(result.rejections) > 0 {
			summary := streamRejectionSummary(result.rejections)
			request, err = d.transitionRequestEvidence(context.WithoutCancel(ctx), request, streamRequestRejected, result.continuationToken, "rows_rejected", summary, "")
			if err != nil {
				return streamChannelStatus{}, managedStreamRequest{}, err
			}
			return streamChannelStatus{}, managedStreamRequest{}, fmt.Errorf("%w: %w: %s", connector.ErrDeliveryConflict, errStreamRowsRejected, summary)
		}
		if appendErr == nil {
			if result.requestID != request.requestID {
				return streamChannelStatus{}, managedStreamRequest{}, fmt.Errorf("%w: append response request identity differs", connector.ErrDeliveryConflict)
			}
			switch result.disposition {
			case streamAppendAccepted:
				request, err = d.transitionRequestEvidence(ctx, request, streamRequestAccepted, result.continuationToken, "accepted", result.evidence, "")
			case streamAppendDefinitelyNotAccepted:
				request, err = d.transitionRequestEvidence(ctx, request, streamRequestProvenAbsent, "", "definitely_not_accepted", result.evidence, "")
			default:
				request, err = d.transitionRequestEvidence(ctx, request, streamRequestSendingUnknown, result.continuationToken, "unknown", result.evidence, "")
			}
			if err != nil {
				return streamChannelStatus{}, managedStreamRequest{}, err
			}
		}
		var committed bool
		status, request, committed, err = d.reconcileRequest(ctx, plan, request)
		if err != nil {
			return streamChannelStatus{}, managedStreamRequest{}, err
		}
		if committed {
			if hook := d.hooks.AfterAppend; hook != nil {
				if err := hook(); err != nil {
					return streamChannelStatus{}, managedStreamRequest{}, fmt.Errorf("%w: injected after streaming Snowflake append: %w", connector.ErrDeliveryIndeterminate, err)
				}
			}
			return status, request, nil
		}
		if request.phase != streamRequestProvenAbsent {
			detail := "no transport error"
			if appendErr != nil {
				detail = appendErr.Error()
			}
			return streamChannelStatus{}, managedStreamRequest{}, fmt.Errorf("%w: request remains unresolved after append: %s", connector.ErrDeliveryIndeterminate, detail)
		}
		if errors.Is(appendErr, errStreamAuthExpired) && d.hooks.RefreshAuth != nil {
			if err := d.hooks.RefreshAuth(ctx); err != nil {
				return streamChannelStatus{}, managedStreamRequest{}, fmt.Errorf("%w: refresh streaming Snowflake credentials: %w", connector.ErrDeliveryIndeterminate, err)
			}
		}
		if errors.Is(appendErr, errStreamThrottled) {
			if err := d.sleepFor(ctx, d.cfg.appendBackoff); err != nil {
				return streamChannelStatus{}, managedStreamRequest{}, err
			}
		}
		status, err = d.openAndPersistChannel(ctx, plan, plan.rowsContentHash)
		if err != nil {
			return streamChannelStatus{}, managedStreamRequest{}, err
		}
	}
	return streamChannelStatus{}, managedStreamRequest{}, fmt.Errorf("%w: streaming Snowflake request did not converge within %d proven-absence retries", connector.ErrDeliveryRetryExhausted, attempts)
}

func (d *streamDriver) reconcileRequest(ctx context.Context, plan managedStreamPlan, request managedStreamRequest) (streamChannelStatus, managedStreamRequest, bool, error) {
	evidence, err := d.proto.RequestStatus(ctx, d.cfg, request)
	if err != nil {
		return streamChannelStatus{}, request, false, fmt.Errorf("%w: reconcile streaming Snowflake request: %w", connector.ErrDeliveryIndeterminate, err)
	}
	if err := validateStreamRequestEvidence(request, evidence); err != nil {
		return streamChannelStatus{}, request, false, err
	}
	switch evidence.disposition {
	case streamRequestStatusCommitted:
		request, err = d.transitionRequestEvidence(ctx, request, streamRequestCommitted, evidence.responseContinuation, "committed", evidence.detail, evidence.committedOffset)
		if err != nil {
			return streamChannelStatus{}, request, false, err
		}
		status := streamChannelStatus{valid: true, channelName: request.channelName, channelRevision: request.channelRevision, pipeRevision: request.pipeRevision, continuationToken: request.responseContinuation, committedOffsetToken: request.committedOffset}
		if err := d.persistChannelState(ctx, plan, status, plan.rowsContentHash); err != nil {
			return streamChannelStatus{}, request, false, err
		}
		return status, request, true, nil
	case streamRequestStatusProvenAbsent:
		request, err = d.transitionRequestEvidence(ctx, request, streamRequestProvenAbsent, evidence.responseContinuation, "proven_absent", evidence.detail, "")
		return streamChannelStatus{}, request, false, err
	case streamRequestStatusDivergent:
		return streamChannelStatus{}, request, false, fmt.Errorf("%w: divergent streaming Snowflake request status: %s", connector.ErrDeliveryConflict, evidence.detail)
	default:
		return streamChannelStatus{}, request, false, fmt.Errorf("%w: streaming Snowflake request status is unknown: %s", connector.ErrDeliveryIndeterminate, evidence.detail)
	}
}

func (d *streamDriver) claimRequestSend(ctx context.Context, request managedStreamRequest) (managedStreamRequest, bool, error) {
	if request.phase != streamRequestPrepared && request.phase != streamRequestSendingUnknown {
		return managedStreamRequest{}, false, fmt.Errorf("%w: request phase %q cannot claim the send boundary", connector.ErrDeliveryConflict, request.phase)
	}
	current, applied, err := d.proto.TransitionRequest(ctx, d.cfg, streamRequestTransition{
		requestID: request.requestID, expectedPhase: request.phase, expectedVersion: request.phaseVersion,
		nextPhase: streamRequestSendingUnknown, responseContinuation: request.responseContinuation,
		committedOffset: request.committedOffset, responseKind: "send_started", responseEvidence: request.responseEvidence,
	})
	if err != nil {
		return managedStreamRequest{}, false, err
	}
	if current.requestID != request.requestID || current.phaseVersion < request.phaseVersion || current.attempt != request.attempt {
		return managedStreamRequest{}, false, fmt.Errorf("%w: streaming Snowflake send claim lost to divergent request", connector.ErrDeliveryConflict)
	}
	return current, applied, nil
}

func (d *streamDriver) transitionRequest(ctx context.Context, request managedStreamRequest, phase streamRequestPhase, kind, evidence string) (managedStreamRequest, error) {
	return d.transitionRequestEvidence(ctx, request, phase, request.responseContinuation, kind, evidence, request.committedOffset)
}

func (d *streamDriver) transitionRequestEvidence(ctx context.Context, request managedStreamRequest, phase streamRequestPhase, continuation, kind, evidence, committedOffset string) (managedStreamRequest, error) {
	if !validStreamRequestTransition(request.phase, phase) {
		return managedStreamRequest{}, fmt.Errorf("%w: illegal streaming Snowflake request transition %s -> %s", connector.ErrDeliveryConflict, request.phase, phase)
	}
	if phase == streamRequestCommitted && (strings.TrimSpace(continuation) == "" || committedOffset != request.requestedOffset) {
		return managedStreamRequest{}, fmt.Errorf("%w: committed streaming request evidence is incomplete", connector.ErrDeliveryConflict)
	}
	if phase == streamRequestProvenAbsent && (continuation != "" || committedOffset != "") {
		return managedStreamRequest{}, fmt.Errorf("%w: proven-absent streaming request carries commit evidence", connector.ErrDeliveryConflict)
	}
	if phase == streamRequestRejected && committedOffset != "" {
		return managedStreamRequest{}, fmt.Errorf("%w: rejected streaming request carries committed-offset evidence", connector.ErrDeliveryConflict)
	}
	current, applied, err := d.proto.TransitionRequest(ctx, d.cfg, streamRequestTransition{requestID: request.requestID, expectedPhase: request.phase, expectedVersion: request.phaseVersion, nextPhase: phase, responseContinuation: continuation, committedOffset: committedOffset, responseKind: kind, responseEvidence: evidence})
	if err != nil {
		return managedStreamRequest{}, err
	}
	if !applied {
		if current.requestID != request.requestID || current.phaseVersion < request.phaseVersion || current.phase != phase {
			return managedStreamRequest{}, fmt.Errorf("%w: streaming Snowflake request CAS lost to divergent phase %q version %d", connector.ErrDeliveryConflict, current.phase, current.phaseVersion)
		}
	}
	return current, nil
}

func (d *streamDriver) validateRequestPlan(plan managedStreamPlan, request managedStreamRequest) error {
	if request.flowIncarnationID != plan.receipt.flowIncarnationID || request.destinationRevisionID != plan.receipt.destinationRevisionID || request.logicalBatchID != plan.receipt.logicalBatchID || request.positionID != plan.receipt.positionID || request.contentHash != plan.receipt.contentHash || request.manifestHash != plan.identity.manifestHash || request.rowsContentHash != plan.rowsContentHash || request.rowCount != plan.rowCount || request.requestedOffset != plan.identity.offsetToken || request.channelName != plan.identity.channelName {
		return fmt.Errorf("%w: durable streaming Snowflake request differs from the immutable delivery plan", connector.ErrDeliveryConflict)
	}
	return request.validateIdentity()
}

func (d *streamDriver) markRequestReceipted(ctx context.Context, request managedStreamRequest) error {
	if request.phase == streamRequestReceipted {
		return nil
	}
	if request.phase != streamRequestCommitted {
		return fmt.Errorf("%w: cannot receipt streaming Snowflake request in phase %q", connector.ErrDeliveryConflict, request.phase)
	}
	_, err := d.transitionRequest(ctx, request, streamRequestReceipted, "receipt_committed", request.responseEvidence)
	return err
}

func (d *streamDriver) assertAppendSize(rows []streamChangelogRow) error {
	var total int64
	for index := range rows {
		size := int64(len(rows[index].payloadBytes()))
		if size > d.cfg.maxRowBytes {
			return fmt.Errorf("%w: %w: row %d is %d bytes", connector.ErrDeliveryConflict, errStreamOversize, rows[index].AppendOrdinal, size)
		}
		total += size
	}
	if total > d.cfg.maxTransactionBytes {
		return fmt.Errorf("%w: %w: append request is %d bytes", connector.ErrDeliveryConflict, errStreamOversize, total)
	}
	return nil
}

// verifyObservedCompleteness polls SQL observation until every deterministic row
// identity is present, then reads the committed offset token as corroborating
// evidence. SQL observation is the completeness authority; the committed offset
// token is persisted evidence and must be non-empty before a receipt is written.
func (d *streamDriver) verifyObservedCompleteness(ctx context.Context, plan managedStreamPlan, status streamChannelStatus, appended bool) (string, error) {
	attempts := d.cfg.observeAttempts
	if attempts < 1 {
		attempts = 1
	}
	verifyCtx, endVerify := telemetry.StartSnowflakeManagedSpan(ctx, "verify", plan.identity.externalID, plan.receipt.logicalBatchID, int64(plan.rowCount), 0)
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			endVerify(err)
			return "", err
		}
		missing, err := d.provenMissingRows(verifyCtx, plan, plan.rows)
		if err != nil {
			endVerify(err)
			return "", err
		}
		if len(missing) == 0 {
			committedToken, tokenErr := d.observedCommittedToken(verifyCtx, plan, status, appended)
			if tokenErr != nil {
				endVerify(tokenErr)
				return "", tokenErr
			}
			endVerify(nil)
			return committedToken, nil
		}
		if attempt+1 < attempts {
			if err := d.sleepFor(ctx, d.cfg.observeInterval); err != nil {
				endVerify(err)
				return "", err
			}
		}
	}
	lastErr = fmt.Errorf("%w: streaming Snowflake rows are not yet SQL-observed complete", connector.ErrDeliveryIndeterminate)
	endVerify(lastErr)
	return "", lastErr
}

// observedCommittedToken reads the durable committed offset token and persists
// the final channel evidence. The token must exactly equal the immutable request
// offset. The driver never synthesizes committed evidence from local state.
func (d *streamDriver) observedCommittedToken(ctx context.Context, plan managedStreamPlan, status streamChannelStatus, _ bool) (string, error) {
	current, err := d.proto.ChannelStatus(ctx, d.cfg, plan.identity.channelName)
	if err != nil {
		return "", err
	}
	committedToken := current.committedOffsetToken
	if committedToken == "" {
		committedToken = status.committedOffsetToken
	}
	if strings.TrimSpace(committedToken) == "" {
		return "", fmt.Errorf("%w: streaming Snowflake rows are observed present without a durable committed offset token", connector.ErrDeliveryIndeterminate)
	}
	if committedToken != plan.identity.offsetToken {
		return "", fmt.Errorf("%w: streaming Snowflake committed offset is bound to a different request", connector.ErrDeliveryConflict)
	}
	status.committedOffsetToken = committedToken
	if current.channelRevision >= status.channelRevision {
		status.channelRevision = current.channelRevision
	}
	if current.pipeRevision != "" {
		status.pipeRevision = current.pipeRevision
	}
	if current.continuationToken != "" {
		status.continuationToken = current.continuationToken
	}
	if err := d.persistChannelState(ctx, plan, status, plan.rowsContentHash); err != nil {
		return "", err
	}
	return committedToken, nil
}

func (d *streamDriver) sleepFor(ctx context.Context, interval time.Duration) error {
	if d.sleep != nil {
		return d.sleep(ctx, interval)
	}
	if interval <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// reconcile treats only one fully matching durable append receipt as applied. It
// is read-only: an absent receipt is NotApplied so a replay can converge, even
// when SQL observation already shows the rows, because the durable receipt plus
// the observed completeness together are the completion proof.
func (d *streamDriver) reconcile(ctx context.Context, intent connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	if err := intent.Validate(); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if err := validateManagedSnowflakeIntentBounds(intent); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if intent.FlowID != d.cfg.flowID {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf("%w: delivery flow differs from admitted streaming Snowflake flow", connector.ErrDeliveryConflict)
	}
	if intent.DestinationRevisionID != d.cfg.destinationRevision {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf("%w: delivery destination revision differs from admitted streaming Snowflake revision", connector.ErrDeliveryConflict)
	}
	appendPlan := newStreamAppendPlan(d.cfg, intent)
	identity, err := newManagedStreamIdentity(d.cfg, intent, appendPlan, intent.ContentHash)
	if err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	expected := managedStreamReceipt{
		kind: streamReceiptKindAppend, profileVersion: d.cfg.profile, flowID: intent.FlowID, flowIncarnationID: intent.FlowIncarnationID,
		sourceLineageID: intent.SourceLineageID, destinationRevisionID: intent.DestinationRevisionID,
		logicalBatchID: intent.LogicalBatchID, positionID: intent.PositionID, contentHash: intent.ContentHash,
		schemaContractHash: d.cfg.schemaContractHash, catalogFingerprint: d.catalogFingerprint,
		manifestHash: identity.manifestHash, externalID: identity.externalID, channelName: identity.channelName,
		offsetToken: identity.offsetToken,
	}
	reconcileCtx, endReconcile := telemetry.StartSnowflakeManagedSpan(ctx, "reconcile", identity.externalID, intent.LogicalBatchID, 0, 0)
	receipt, found, err := d.proto.LookupReceipt(reconcileCtx, d.cfg, streamReceiptKey{
		flowIncarnationID: intent.FlowIncarnationID, destinationRevisionID: intent.DestinationRevisionID,
		logicalBatchID: intent.LogicalBatchID, sourceLineageID: intent.SourceLineageID,
		positionID: intent.PositionID, externalID: identity.externalID, kind: streamReceiptKindAppend,
	})
	if err != nil {
		endReconcile(err)
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if !found {
		endReconcile(nil)
		return connector.DeliveryNotApplied, connector.DeliveryEvidence{}, nil
	}
	if err := validateStreamReceiptIdentity(expected, receipt); err != nil {
		endReconcile(err)
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	endReconcile(nil)
	return connector.DeliveryApplied, connector.DeliveryEvidence{ExternalID: receipt.externalID, ContentHash: receipt.contentHash}, nil
}

// cleanup performs one bounded pass of channel-state retention for a flow
// incarnation. It writes an idempotent release receipt for each fully committed,
// durably recorded batch older than the retention window and removes the durable
// channel state so the removal is convergent. It never releases a batch whose
// delivery was not acknowledged.
func (d *streamDriver) cleanup(ctx context.Context, flowIncarnationID string) (released int, resultErr error) {
	ctx, endSpan := telemetry.StartSnowflakeManagedSpan(ctx, "cleanup", d.cfg.channelStateTable, flowIncarnationID, 0, 0)
	defer func() { endSpan(resultErr) }()
	if strings.TrimSpace(flowIncarnationID) == "" {
		return 0, errors.New("streaming Snowflake cleanup requires a flow incarnation")
	}
	candidates, err := d.proto.ListReleasableReceipts(ctx, d.cfg, flowIncarnationID, d.cfg.cleanupRetention, d.cfg.cleanupMaxObjects)
	if err != nil {
		return 0, err
	}
	for _, receipt := range candidates {
		if receipt.kind != streamReceiptKindAppend || receipt.receiptStatus != streamStatusCommitted {
			continue
		}
		channelKey := streamChannelStateKey{flowIncarnationID: receipt.flowIncarnationID, destinationRevisionID: receipt.destinationRevisionID, channelName: receipt.channelName}
		state, found, err := d.proto.LookupChannelState(ctx, d.cfg, channelKey)
		if err != nil {
			return released, err
		}
		if !found {
			return released, fmt.Errorf("%w: cleanup append receipt has no channel authority state", connector.ErrDeliveryConflict)
		}
		applied, err := d.proto.ReleaseChannelState(ctx, d.cfg, state, streamReleaseReceipt(receipt))
		if err != nil {
			return released, err
		}
		if !applied {
			return released, fmt.Errorf("%w: cleanup channel release CAS did not apply", connector.ErrDeliveryIndeterminate)
		}
		released++
	}
	return released, nil
}

func streamReleaseReceipt(applied managedStreamReceipt) managedStreamReceipt {
	release := applied
	release.kind = streamReceiptKindRelease
	release.externalID = applied.externalID + ":release"
	release.receiptStatus = streamStatusReleased
	return release
}

func validateStreamReceipt(expected, actual managedStreamReceipt) error {
	if err := validateStreamReceiptIdentity(expected, actual); err != nil {
		return err
	}
	if expected.rowsContentHash != actual.rowsContentHash {
		return fmt.Errorf("%w: streaming Snowflake receipt row-content identity differs", connector.ErrDeliveryConflict)
	}
	if expected.transactionID != actual.transactionID || expected.fragmentCount != actual.fragmentCount ||
		expected.recordCount != actual.recordCount {
		return fmt.Errorf("%w: streaming Snowflake receipt transaction manifest differs", connector.ErrDeliveryConflict)
	}
	return nil
}

// validateStreamReceiptIdentity compares only the immutable delivery and append
// identity. The channel/pipe revision and committed offset token are per-attempt
// evidence, not identity: a reopened channel legitimately bumps the revision on
// replay, so they are excluded from the identity equality.
func validateStreamReceiptIdentity(expected, actual managedStreamReceipt) error {
	if actual.kind != streamReceiptKindAppend {
		return fmt.Errorf("%w: streaming Snowflake receipt kind %q is not an append receipt", connector.ErrDeliveryConflict, actual.kind)
	}
	if expected.profileVersion != actual.profileVersion || expected.flowID != actual.flowID ||
		expected.flowIncarnationID != actual.flowIncarnationID || expected.sourceLineageID != actual.sourceLineageID ||
		expected.destinationRevisionID != actual.destinationRevisionID || expected.logicalBatchID != actual.logicalBatchID ||
		expected.positionID != actual.positionID || expected.contentHash != actual.contentHash ||
		expected.schemaContractHash != actual.schemaContractHash || expected.catalogFingerprint != actual.catalogFingerprint ||
		expected.manifestHash != actual.manifestHash || expected.externalID != actual.externalID || actual.requestID == "" ||
		expected.requestID != "" && expected.requestID != actual.requestID ||
		expected.channelName != actual.channelName || expected.offsetToken != actual.offsetToken {
		return fmt.Errorf("%w: streaming Snowflake receipt identity or hash differs", connector.ErrDeliveryConflict)
	}
	return nil
}

func rowHashesOf(rows []streamChangelogRow) []string {
	hashes := make([]string, 0, len(rows))
	for index := range rows {
		hashes = append(hashes, rows[index].RowHash)
	}
	return hashes
}

func appendRowsOf(rows []streamChangelogRow) []streamAppendRow {
	appendRows := make([]streamAppendRow, 0, len(rows))
	for index := range rows {
		appendRows = append(appendRows, streamAppendRow{
			rowHash: rows[index].RowHash, ordinal: rows[index].AppendOrdinal, payload: rows[index].payloadBytes(),
		})
	}
	return appendRows
}

func streamRejectionSummary(rejections []streamRowRejection) string {
	parts := make([]string, 0, len(rejections))
	for _, rejection := range rejections {
		parts = append(parts, fmt.Sprintf("row %d (%s): %s", rejection.ordinal, shortStreamHash(rejection.rowHash), rejection.reason))
	}
	return strings.Join(parts, "; ")
}

func shortStreamHash(hash string) string {
	if len(hash) <= 12 {
		return hash
	}
	return hash[:12]
}
