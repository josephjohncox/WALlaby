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

// StreamingHooks exposes deterministic fault boundaries around the ambiguous
// open/append/receipt transitions of the streaming append profile. Production
// callers leave every hook nil; the live recovery matrix injects response loss
// and credential refresh.
type StreamingHooks struct {
	AfterOpen     func() error
	AfterAppend   func() error
	BeforeReceipt func() error
	AfterReceipt  func() error
	RefreshAuth   func(context.Context) error
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
	hooks              StreamingHooks
	sleep              func(context.Context, time.Duration) error
}

func newStreamDriver(proto streamProtocol, cfg streamConfig, catalogFingerprint string, hooks StreamingHooks) *streamDriver {
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

	status, err := d.openAndPersistChannel(ctx, plan, plan.rowsContentHash)
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
	appended := len(missing) > 0
	if appended {
		status, err = d.appendMissing(ctx, plan, status, missing)
		if err != nil {
			return connector.DeliveryEvidence{}, err
		}
	}

	committedToken, err := d.verifyObservedCompleteness(ctx, plan, status, appended)
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}

	plan.receipt.channelRevision = status.channelRevision
	plan.receipt.pipeRevision = status.pipeRevision
	plan.receipt.committedOffsetToken = committedToken

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
		return connector.DeliveryEvidence{ExternalID: existing.externalID, ContentHash: existing.contentHash}, nil
	}
	if hook := d.hooks.AfterReceipt; hook != nil {
		if err := hook(); err != nil {
			return connector.DeliveryEvidence{}, fmt.Errorf("%w: injected after streaming Snowflake receipt: %w", connector.ErrDeliveryIndeterminate, err)
		}
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
	state := managedStreamChannelState{
		flowIncarnationID: plan.receipt.flowIncarnationID, destinationRevisionID: plan.receipt.destinationRevisionID,
		channelName: plan.identity.channelName, pipeName: d.cfg.pipe, pipeRevision: status.pipeRevision,
		channelRevision: status.channelRevision, continuationToken: status.continuationToken,
		committedOffsetToken: status.committedOffsetToken, logicalBatchID: plan.receipt.logicalBatchID,
		rowsContentHash: rowsContentHash,
	}
	return d.proto.UpsertChannelState(ctx, d.cfg, state)
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

// appendMissing appends the proven-missing rows, reconciling channel
// invalidation, auth expiry, and throttling within a bound. On invalidation it
// reopens the channel, re-observes committed rows, and appends only the rows
// still proven missing — never blindly re-appending already-committed rows.
func (d *streamDriver) appendMissing(ctx context.Context, plan managedStreamPlan, status streamChannelStatus, missing []streamChangelogRow) (streamChannelStatus, error) {
	if err := d.assertAppendSize(missing); err != nil {
		return streamChannelStatus{}, err
	}
	attempts := d.cfg.appendAttempts
	if attempts < 1 {
		attempts = 1
	}
	appendCtx, endAppend := telemetry.StartSnowflakeManagedSpan(ctx, "append", plan.identity.externalID, plan.receipt.logicalBatchID, int64(len(missing)), 0)
	defer func() { endAppend(nil) }()
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return streamChannelStatus{}, err
		}
		req := streamAppendRequest{
			cfg: d.cfg, channelName: plan.identity.channelName, channelRevision: status.channelRevision,
			continuationToken: status.continuationToken, offsetToken: plan.identity.offsetToken,
			rows: appendRowsOf(missing),
		}
		result, err := d.proto.AppendRows(appendCtx, req)
		if err != nil {
			var reopened bool
			status, missing, reopened, lastErr = d.reconcileAppendError(ctx, plan, status, err)
			if lastErr != nil {
				return streamChannelStatus{}, lastErr
			}
			if len(missing) == 0 {
				// Reopen + re-observation proved every row already committed.
				return status, nil
			}
			if reopened {
				if err := d.assertAppendSize(missing); err != nil {
					return streamChannelStatus{}, err
				}
			}
			continue
		}
		if len(result.rejections) > 0 {
			return streamChannelStatus{}, fmt.Errorf("%w: %w: %s", connector.ErrDeliveryConflict, errStreamRowsRejected, streamRejectionSummary(result.rejections))
		}
		// The append result echoes the requested offset and advances the
		// continuation token; it is NOT proof of commit. The committed offset token
		// is read only from the durable channel status during completeness
		// verification, so a lost or unacknowledged commit can never masquerade as
		// progress here.
		status.continuationToken = result.continuationToken
		if err := d.persistChannelState(ctx, plan, status, plan.rowsContentHash); err != nil {
			return streamChannelStatus{}, err
		}
		if hook := d.hooks.AfterAppend; hook != nil {
			if hookErr := hook(); hookErr != nil {
				return streamChannelStatus{}, fmt.Errorf("%w: injected after streaming Snowflake append: %w", connector.ErrDeliveryIndeterminate, hookErr)
			}
		}
		return status, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("%w: streaming Snowflake append did not converge within %d attempts", connector.ErrDeliveryRetryExhausted, attempts)
	}
	return streamChannelStatus{}, lastErr
}

// reconcileAppendError classifies one append transport error. It reopens the
// channel and recomputes proven-missing rows on invalidation, refreshes
// credentials on auth expiry, and backs off on throttling. Fatal errors
// (oversize, rejected rows) fail closed.
func (d *streamDriver) reconcileAppendError(ctx context.Context, plan managedStreamPlan, status streamChannelStatus, appendErr error) (streamChannelStatus, []streamChangelogRow, bool, error) {
	switch {
	case errors.Is(appendErr, errStreamChannelInvalidated):
		reopened, err := d.openAndPersistChannel(ctx, plan, plan.rowsContentHash)
		if err != nil {
			return streamChannelStatus{}, nil, false, err
		}
		missing, err := d.provenMissingRows(ctx, plan, plan.rows)
		if err != nil {
			return streamChannelStatus{}, nil, false, err
		}
		return reopened, missing, true, nil
	case errors.Is(appendErr, errStreamAuthExpired):
		if hook := d.hooks.RefreshAuth; hook != nil {
			if err := hook(ctx); err != nil {
				return streamChannelStatus{}, nil, false, fmt.Errorf("%w: refresh streaming Snowflake credentials: %w", connector.ErrDeliveryIndeterminate, err)
			}
		}
		missing, err := d.provenMissingRows(ctx, plan, plan.rows)
		if err != nil {
			return streamChannelStatus{}, nil, false, err
		}
		return status, missing, false, nil
	case errors.Is(appendErr, errStreamThrottled):
		if err := d.sleepFor(ctx, d.cfg.appendBackoff); err != nil {
			return streamChannelStatus{}, nil, false, err
		}
		missing, err := d.provenMissingRows(ctx, plan, plan.rows)
		if err != nil {
			return streamChannelStatus{}, nil, false, err
		}
		return status, missing, false, nil
	case errors.Is(appendErr, errStreamOversize):
		return streamChannelStatus{}, nil, false, fmt.Errorf("%w: %w", connector.ErrDeliveryConflict, appendErr)
	case errors.Is(appendErr, errStreamRowsRejected):
		return streamChannelStatus{}, nil, false, fmt.Errorf("%w: %w", connector.ErrDeliveryConflict, appendErr)
	default:
		return streamChannelStatus{}, nil, false, fmt.Errorf("%w: streaming Snowflake append: %w", connector.ErrDeliveryIndeterminate, appendErr)
	}
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
// the final channel evidence. SQL-observed completeness is the adoption
// authority; the committed offset token is corroborating evidence. When this
// incarnation performed the append, the token must be non-empty — the transport
// must corroborate the write it just made. In complete-unreceipted recovery
// (rows already present from a prior incarnation, nothing appended here) the
// channel may have been reopened with no committed token yet, so the batch's
// deterministic offset token is recorded as evidence and adoption proceeds on
// the SQL-observed completeness alone.
func (d *streamDriver) observedCommittedToken(ctx context.Context, plan managedStreamPlan, status streamChannelStatus, appended bool) (string, error) {
	current, err := d.proto.ChannelStatus(ctx, d.cfg, plan.identity.channelName)
	if err != nil {
		return "", err
	}
	committedToken := current.committedOffsetToken
	if committedToken == "" {
		committedToken = status.committedOffsetToken
	}
	if strings.TrimSpace(committedToken) == "" {
		if appended {
			return "", fmt.Errorf("%w: streaming Snowflake rows are observed present but the append incarnation has no durable committed offset token", connector.ErrDeliveryIndeterminate)
		}
		committedToken = plan.identity.offsetToken
	}
	status.committedOffsetToken = committedToken
	if current.channelRevision >= status.channelRevision {
		status.channelRevision = current.channelRevision
	}
	if current.pipeRevision != "" {
		status.pipeRevision = current.pipeRevision
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
		release := streamReleaseReceipt(receipt)
		if _, err := d.proto.InsertReceipt(ctx, d.cfg, release); err != nil {
			return released, err
		}
		if err := d.proto.DeleteChannelState(ctx, d.cfg, streamChannelStateKey{
			flowIncarnationID: receipt.flowIncarnationID, destinationRevisionID: receipt.destinationRevisionID, channelName: receipt.channelName,
		}); err != nil {
			return released, err
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
		expected.manifestHash != actual.manifestHash || expected.externalID != actual.externalID ||
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
