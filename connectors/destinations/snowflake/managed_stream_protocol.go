package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

var (
	// errStreamAuthExpired means the scoped ingest credential expired mid-append.
	// The driver refreshes and retries within its bound.
	errStreamAuthExpired = errors.New("streaming Snowflake append authentication expired")
	// errStreamThrottled means the server applied backpressure. The driver backs
	// off within its bound and retries; it is never converted into a receipt.
	errStreamThrottled = errors.New("streaming Snowflake append throttled")
	// errStreamOversize means a single row or the append request exceeds the
	// admitted size. It is fatal and fails closed; the driver never silently
	// splits or drops rows.
	errStreamOversize = errors.New("streaming Snowflake append exceeds admitted size")
	// errStreamRowsRejected means the terminal append response reported one or
	// more rejected rows. A terminal token with rejected rows can never be adopted
	// as a completed delivery.
	errStreamRowsRejected = errors.New("streaming Snowflake append rejected rows")
	// errStreamObservationInconsistent means SQL observation reported more rows for
	// one deterministic identity than the plan produced — a duplicate-identity
	// hazard that fails closed rather than acknowledging.
	errStreamObservationInconsistent = errors.New("streaming Snowflake observed row cardinality is inconsistent")
	// errStreamChannelInvalidated means Snowflake rejected the current client
	// sequencer. Recovery must reopen and reconcile before any append.
	errStreamChannelInvalidated = errors.New("streaming Snowflake channel is invalidated")
)

// streamAppendRow is one row handed to the append transport. The payload is the
// deterministic JSON body; rowHash is the SQL-observed identity.
type streamAppendRow struct {
	rowHash string
	ordinal uint64
	payload []byte
}

// streamAppendRequest is one bounded append call against an open channel.
type streamAppendRequest struct {
	cfg                    streamConfig
	requestID              string
	channelName            string
	channelRevision        int64
	pipeRevision           string
	continuationToken      string
	expectedPreviousOffset string
	offsetToken            string
	manifestHash           string
	rowsContentHash        string
	rowCount               int
	rows                   []streamAppendRow
}

// streamRowRejection is one server-rejected row.
type streamRowRejection struct {
	rowHash string
	ordinal uint64
	reason  string
}

// streamChannelStatus is the durable status of an open channel. continuationToken
// advances every append; committedOffsetToken names the last durably committed
// offset token; channelRevision is the monotonic client sequencer bumped by every
// reopen; pipeRevision binds the target pipe definition.
type streamChannelStatus struct {
	valid                bool
	channelName          string
	channelRevision      int64
	pipeRevision         string
	continuationToken    string
	committedOffsetToken string
}

// streamAppendResult is the outcome of one append call. It advances the
// continuation token and reports any per-row rejections. It deliberately does
// NOT surface a committed offset token: the append is an accept, not a commit,
// so completion is proven only by SQL observation and ChannelStatus.
type streamAppendResult struct {
	disposition       streamAppendDisposition
	requestID         string
	continuationToken string
	evidence          string
	rejections        []streamRowRejection
}

// managedStreamChannelState is the durable, SQL-persisted evidence of a channel:
// its exact pipe/channel revision, the last continuation token, and the last
// committed offset token. It is persisted so a restarted writer can prove which
// revision it must reopen against and never treats a stale token as progress.
type managedStreamChannelState struct {
	flowIncarnationID     string
	destinationRevisionID string
	channelName           string
	pipeName              string
	pipeRevision          string
	channelRevision       int64
	continuationToken     string
	committedOffsetToken  string
	logicalBatchID        string
	rowsContentHash       string
	requestID             string
	stateVersion          int64
}

type streamChannelStateKey struct {
	flowIncarnationID     string
	destinationRevisionID string
	channelName           string
}

// streamReceiptKey selects a durable receipt by its stable delivery identity and
// kind (append or release).
type streamReceiptKey struct {
	flowIncarnationID     string
	destinationRevisionID string
	logicalBatchID        string
	sourceLineageID       string
	positionID            string
	externalID            string
	kind                  string
}

type streamReceiptInsert struct {
	inserted bool
}

// streamProtocol is the deep seam between the streaming append driver and
// Snowflake. Every ambiguous transport boundary and every SQL-observed
// completeness read is a single method so the driver's crash-window recovery is
// exhaustively testable with an in-memory fake.
//
// The transport methods (OpenChannel, AppendRows, ChannelStatus) require a
// reviewed high-performance append client; the SQL-backed implementation refuses
// them fail-closed. The observation, channel-state, and receipt methods run
// against the ordinary query API and are the adoption authority.
type streamTransport interface {
	// OpenChannel opens (or reopens) the deterministic named channel and returns
	// its durable status, including the last committed offset token.
	OpenChannel(ctx context.Context, cfg streamConfig, channelName string) (streamChannelStatus, error)
	// AppendRows appends only the supplied rows to an open channel. It returns
	// per-row rejections; a rejection is never silently retried as success.
	AppendRows(ctx context.Context, req streamAppendRequest) (streamAppendResult, error)
	// ChannelStatus re-reads the durable channel status (committed offset token)
	// without appending.
	ChannelStatus(ctx context.Context, cfg streamConfig, channelName string) (streamChannelStatus, error)
	// RequestStatus authoritatively reconciles one exact request identity. Unknown
	// or divergent status never permits a resend.
	RequestStatus(ctx context.Context, cfg streamConfig, request managedStreamRequest) (streamRequestStatusEvidence, error)
}

type streamStateStore interface {
	// ObserveCommittedRows reports every ROW_HASH for one logical batch. The
	// driver rejects extra, duplicate, or missing identities.
	// supplied rows are durably present in the streaming target for this logical
	// batch. This SQL observation — not any transport token — proves completeness.
	ObserveCommittedRows(ctx context.Context, cfg streamConfig, logicalBatchID string, rowHashes []string) (map[string]int, error)
	// CompareAndSwapChannelState persists monotonic channel evidence.
	CompareAndSwapChannelState(ctx context.Context, cfg streamConfig, expected managedStreamChannelState, state managedStreamChannelState) (managedStreamChannelState, bool, error)
	// LookupChannelState returns the persisted channel evidence.
	LookupChannelState(ctx context.Context, cfg streamConfig, key streamChannelStateKey) (managedStreamChannelState, bool, error)
	// InsertRequest creates one immutable request identity before network I/O.
	InsertRequest(ctx context.Context, cfg streamConfig, request managedStreamRequest) (bool, error)
	LookupRequest(ctx context.Context, cfg streamConfig, key streamRequestKey) (managedStreamRequest, bool, error)
	TransitionRequest(ctx context.Context, cfg streamConfig, transition streamRequestTransition) (managedStreamRequest, bool, error)
	HasUnresolvedRequests(ctx context.Context, cfg streamConfig, key streamChannelStateKey) (bool, error)
	// LookupReceipt returns the durable destination receipt for one identity.
	LookupReceipt(ctx context.Context, cfg streamConfig, key streamReceiptKey) (managedStreamReceipt, bool, error)
	// InsertReceipt writes the receipt atomically; a duplicate primary key is a
	// non-error signal that a concurrent owner already committed it.
	InsertReceipt(ctx context.Context, cfg streamConfig, receipt managedStreamReceipt) (streamReceiptInsert, error)
	// ListReleasableReceipts returns append receipts for one flow incarnation
	// older than the retention window and not yet released, bounded by limit.
	ListReleasableReceipts(ctx context.Context, cfg streamConfig, flowIncarnationID string, retention time.Duration, limit int) ([]managedStreamReceipt, error)
	// ReleaseChannelState atomically writes the release receipt and conditionally
	// deletes the exact channel-state version only when no unresolved request exists.
	ReleaseChannelState(ctx context.Context, cfg streamConfig, expected managedStreamChannelState, release managedStreamReceipt) (bool, error)
}

type streamProtocol interface {
	streamTransport
	streamStateStore
}

// composedStreamProtocol binds HTTP channel operations to Snowflake SQL
// authority. Neither side can substitute for the other.
type composedStreamProtocol struct {
	streamTransport
	streamStateStore
}

// sqlStreamProtocol is the real gosnowflake-backed protocol. Its SQL-observation
// and receipt/channel-state methods query the ordinary Snowflake query API; its
// append-transport methods fail closed because no reviewed high-performance Go
// append client is linked. It is exercised only by the credential-gated live
// recovery matrix once a transport exists; unit and property coverage runs
// against the in-memory fake.
type sqlStreamProtocol struct {
	db *sql.DB
}

func newSQLStreamProtocol(db *sql.DB) *sqlStreamProtocol {
	return &sqlStreamProtocol{db: db}
}

func (p *sqlStreamProtocol) OpenChannel(context.Context, streamConfig, string) (streamChannelStatus, error) {
	return streamChannelStatus{}, fmt.Errorf("%w: open channel", errStreamingTransportUnavailable)
}

func (p *sqlStreamProtocol) AppendRows(context.Context, streamAppendRequest) (streamAppendResult, error) {
	return streamAppendResult{}, fmt.Errorf("%w: append rows", errStreamingTransportUnavailable)
}

func (p *sqlStreamProtocol) ChannelStatus(context.Context, streamConfig, string) (streamChannelStatus, error) {
	return streamChannelStatus{}, fmt.Errorf("%w: channel status", errStreamingTransportUnavailable)
}

func (p *sqlStreamProtocol) RequestStatus(context.Context, streamConfig, managedStreamRequest) (streamRequestStatusEvidence, error) {
	return streamRequestStatusEvidence{}, fmt.Errorf("%w: request status", errStreamingTransportUnavailable)
}

func (p *sqlStreamProtocol) ObserveCommittedRows(ctx context.Context, cfg streamConfig, logicalBatchID string, rowHashes []string) (map[string]int, error) {
	present := make(map[string]int, len(rowHashes))
	if len(rowHashes) == 0 {
		return present, nil
	}
	target := managedSnowflakeStreamQualified(cfg, cfg.table)
	// #nosec G202 -- the target identifier is composed only of validated unquoted
	// uppercase identifiers; the logical batch is a bound parameter.
	query := "SELECT \"ROW_HASH\", COUNT(*) FROM " + target +
		" WHERE \"LOGICAL_BATCH_ID\" = ? GROUP BY \"ROW_HASH\""
	rows, err := p.db.QueryContext(ctx, query, logicalBatchID)
	if err != nil {
		return nil, fmt.Errorf("observe streaming Snowflake committed rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var hash string
		var count int64
		if err := rows.Scan(&hash, &count); err != nil {
			return nil, fmt.Errorf("scan streaming Snowflake observed row: %w", err)
		}
		present[hash] = int(count)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate streaming Snowflake observed rows: %w", err)
	}
	return present, nil
}

func (p *sqlStreamProtocol) CompareAndSwapChannelState(ctx context.Context, cfg streamConfig, expected managedStreamChannelState, state managedStreamChannelState) (managedStreamChannelState, bool, error) {
	ctx, recordQueryID := managedSnowflakeQueryIDContext(ctx)
	values := append(streamChannelStateValues(state), expected.stateVersion, expected.pipeName, expected.pipeRevision, expected.channelRevision, expected.continuationToken, expected.committedOffsetToken, expected.logicalBatchID, expected.rowsContentHash, expected.requestID)
	result, err := p.db.ExecContext(ctx, streamChannelStateMergeSQL(cfg), values...)
	recordQueryID()
	if err != nil {
		return managedStreamChannelState{}, false, fmt.Errorf("%w: compare-and-swap streaming Snowflake channel state: %w", connector.ErrDeliveryIndeterminate, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return managedStreamChannelState{}, false, fmt.Errorf("read streaming Snowflake channel CAS cardinality: %w", err)
	}
	if affected < 0 || affected > 1 {
		return managedStreamChannelState{}, false, fmt.Errorf("%w: channel state CAS affected %d rows", connector.ErrDeliveryConflict, affected)
	}
	current, found, err := p.LookupChannelState(ctx, cfg, streamChannelStateKey{flowIncarnationID: state.flowIncarnationID, destinationRevisionID: state.destinationRevisionID, channelName: state.channelName})
	if err != nil {
		return managedStreamChannelState{}, false, err
	}
	if !found {
		return managedStreamChannelState{}, false, fmt.Errorf("%w: channel state CAS produced no visible row", connector.ErrDeliveryIndeterminate)
	}
	return current, affected == 1, nil
}

func (p *sqlStreamProtocol) LookupChannelState(ctx context.Context, cfg streamConfig, key streamChannelStateKey) (managedStreamChannelState, bool, error) {
	ctx, recordQueryID := managedSnowflakeQueryIDContext(ctx)
	row := p.db.QueryRowContext(ctx, streamChannelStateLookupSQL(cfg), key.flowIncarnationID, key.destinationRevisionID, key.channelName)
	recordQueryID()
	state, err := scanStreamChannelState(row)
	if errors.Is(err, sql.ErrNoRows) {
		return managedStreamChannelState{}, false, nil
	}
	if err != nil {
		return managedStreamChannelState{}, false, err
	}
	return state, true, nil
}

func (p *sqlStreamProtocol) InsertRequest(ctx context.Context, cfg streamConfig, request managedStreamRequest) (bool, error) {
	if err := request.validateIdentity(); err != nil {
		return false, err
	}
	ctx, recordQueryID := managedSnowflakeQueryIDContext(ctx)
	result, err := p.db.ExecContext(ctx, streamRequestInsertSQL(cfg), streamRequestValues(request)...)
	recordQueryID()
	if err != nil {
		if isStagedDuplicateKey(err) {
			return false, nil
		}
		return false, fmt.Errorf("%w: insert streaming Snowflake request: %w", connector.ErrDeliveryIndeterminate, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read streaming Snowflake request cardinality: %w", err)
	}
	return affected == 1, nil
}

func (p *sqlStreamProtocol) LookupRequest(ctx context.Context, cfg streamConfig, key streamRequestKey) (managedStreamRequest, bool, error) {
	ctx, recordQueryID := managedSnowflakeQueryIDContext(ctx)
	rows, err := p.db.QueryContext(ctx, streamRequestLookupSQL(cfg), key.flowIncarnationID, key.destinationRevisionID, key.logicalBatchID)
	recordQueryID()
	if err != nil {
		return managedStreamRequest{}, false, fmt.Errorf("query streaming Snowflake request: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return managedStreamRequest{}, false, err
		}
		return managedStreamRequest{}, false, nil
	}
	request, err := scanStreamRequest(rows)
	if err != nil {
		return managedStreamRequest{}, false, fmt.Errorf("scan streaming Snowflake request: %w", err)
	}
	if rows.Next() {
		previous, err := scanStreamRequest(rows)
		if err != nil {
			return managedStreamRequest{}, false, fmt.Errorf("scan prior streaming Snowflake request: %w", err)
		}
		if previous.attempt >= request.attempt || previous.phase != streamRequestProvenAbsent {
			return managedStreamRequest{}, false, fmt.Errorf("%w: durable streaming request attempt history is divergent", connector.ErrDeliveryConflict)
		}
	}
	if err := rows.Err(); err != nil {
		return managedStreamRequest{}, false, fmt.Errorf("iterate streaming Snowflake requests: %w", err)
	}
	return request, true, nil
}

func (p *sqlStreamProtocol) TransitionRequest(ctx context.Context, cfg streamConfig, transition streamRequestTransition) (managedStreamRequest, bool, error) {
	if !validStreamRequestTransition(transition.expectedPhase, transition.nextPhase) || transition.expectedVersion <= 0 {
		return managedStreamRequest{}, false, errors.New("illegal streaming Snowflake request transition")
	}
	ctx, recordQueryID := managedSnowflakeQueryIDContext(ctx)
	result, err := p.db.ExecContext(ctx, streamRequestTransitionSQL(cfg), string(transition.nextPhase), transition.responseContinuation, transition.committedOffset, transition.responseKind, transition.responseEvidence, transition.requestID, string(transition.expectedPhase), transition.expectedVersion)
	recordQueryID()
	if err != nil {
		return managedStreamRequest{}, false, fmt.Errorf("%w: transition streaming Snowflake request: %w", connector.ErrDeliveryIndeterminate, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return managedStreamRequest{}, false, err
	}
	if affected < 0 || affected > 1 {
		return managedStreamRequest{}, false, fmt.Errorf("%w: streaming Snowflake request transition affected %d rows", connector.ErrDeliveryConflict, affected)
	}
	row := p.db.QueryRowContext(ctx, streamRequestLookupByIDSQL(cfg), transition.requestID)
	request, scanErr := scanStreamRequest(row)
	if scanErr != nil {
		return managedStreamRequest{}, false, fmt.Errorf("read transitioned streaming Snowflake request: %w", scanErr)
	}
	return request, affected == 1, nil
}

func (p *sqlStreamProtocol) HasUnresolvedRequests(ctx context.Context, cfg streamConfig, key streamChannelStateKey) (bool, error) {
	var count int64
	if err := p.db.QueryRowContext(ctx, streamRequestUnresolvedSQL(cfg), key.flowIncarnationID, key.destinationRevisionID, key.channelName).Scan(&count); err != nil {
		return false, fmt.Errorf("query unresolved streaming Snowflake requests: %w", err)
	}
	return count > 0, nil
}

func (p *sqlStreamProtocol) LookupReceipt(ctx context.Context, cfg streamConfig, key streamReceiptKey) (managedStreamReceipt, bool, error) {
	queryCtx, recordQueryID := managedSnowflakeQueryIDContext(ctx)
	rows, err := p.db.QueryContext(queryCtx, streamReceiptLookupSQL(cfg),
		key.kind, key.flowIncarnationID, key.destinationRevisionID, key.logicalBatchID,
		key.kind, key.flowIncarnationID, key.destinationRevisionID, key.sourceLineageID, key.positionID,
		key.externalID,
	)
	recordQueryID()
	if err != nil {
		return managedStreamReceipt{}, false, fmt.Errorf("query streaming Snowflake receipt: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var receipts []managedStreamReceipt
	for rows.Next() {
		receipt, err := scanStreamReceipt(rows)
		if err != nil {
			return managedStreamReceipt{}, false, err
		}
		receipts = append(receipts, receipt)
		if len(receipts) > 1 {
			return managedStreamReceipt{}, false, fmt.Errorf("%w: multiple streaming Snowflake receipts match one delivery identity", connector.ErrDeliveryConflict)
		}
	}
	if err := rows.Err(); err != nil {
		return managedStreamReceipt{}, false, fmt.Errorf("iterate streaming Snowflake receipts: %w", err)
	}
	if len(receipts) == 0 {
		return managedStreamReceipt{}, false, nil
	}
	return receipts[0], true, nil
}

func (p *sqlStreamProtocol) InsertReceipt(ctx context.Context, cfg streamConfig, receipt managedStreamReceipt) (streamReceiptInsert, error) {
	ctx, recordQueryID := managedSnowflakeQueryIDContext(ctx)
	result, err := p.db.ExecContext(ctx, streamReceiptInsertSQL(cfg), streamReceiptValues(receipt)...)
	recordQueryID()
	if err != nil {
		if isStagedDuplicateKey(err) {
			return streamReceiptInsert{inserted: false}, nil
		}
		return streamReceiptInsert{}, fmt.Errorf("%w: insert streaming Snowflake receipt: %w", connector.ErrDeliveryIndeterminate, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return streamReceiptInsert{}, fmt.Errorf("read streaming Snowflake receipt cardinality: %w", err)
	}
	if affected != 1 {
		return streamReceiptInsert{}, fmt.Errorf("streaming Snowflake receipt insert affected %d rows, want exactly 1", affected)
	}
	return streamReceiptInsert{inserted: true}, nil
}

func (p *sqlStreamProtocol) ListReleasableReceipts(ctx context.Context, cfg streamConfig, flowIncarnationID string, retention time.Duration, limit int) ([]managedStreamReceipt, error) {
	table := managedSnowflakeStreamQualifiedTable(cfg, cfg.receiptsTable)
	channelTable := managedSnowflakeStreamQualifiedTable(cfg, cfg.channelStateTable)
	// #nosec G202 -- both table identifiers are composed only of validated unquoted uppercase identifiers; all values are bound parameters.
	query := "SELECT " + streamReceiptColumnsQualified("L") + " FROM " + table + " AS L" +
		" WHERE L.\"RECEIPT_KIND\" = ? AND L.\"FLOW_INCARNATION_ID\" = ? AND L.\"RECEIPT_STATUS\" = ?" +
		" AND L.\"COMMITTED_AT\" < DATEADD('second', ?, CURRENT_TIMESTAMP())" +
		" AND (NOT EXISTS (SELECT 1 FROM " + table + " AS R WHERE R.\"RECEIPT_KIND\" = ? AND R.\"EXTERNAL_ID\" = L.\"EXTERNAL_ID\" || ':release')" +
		" OR EXISTS (SELECT 1 FROM " + channelTable + " AS C WHERE C.\"FLOW_INCARNATION_ID\" = L.\"FLOW_INCARNATION_ID\" AND C.\"DESTINATION_REVISION_ID\" = L.\"DESTINATION_REVISION_ID\" AND C.\"CHANNEL_NAME\" = L.\"CHANNEL_NAME\"))" +
		" ORDER BY L.\"COMMITTED_AT\" LIMIT " + strconv.Itoa(limit)
	rows, err := p.db.QueryContext(ctx, query,
		streamReceiptKindAppend, flowIncarnationID, streamStatusCommitted,
		-int64(retention/time.Second), streamReceiptKindRelease,
	)
	if err != nil {
		return nil, fmt.Errorf("list releasable streaming receipts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var receipts []managedStreamReceipt
	for rows.Next() {
		receipt, err := scanStreamReceipt(rows)
		if err != nil {
			return nil, err
		}
		receipts = append(receipts, receipt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate releasable streaming receipts: %w", err)
	}
	return receipts, nil
}

func (p *sqlStreamProtocol) ReleaseChannelState(ctx context.Context, cfg streamConfig, expected managedStreamChannelState, release managedStreamReceipt) (bool, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin streaming Snowflake channel release: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	key := streamChannelStateKey{flowIncarnationID: expected.flowIncarnationID, destinationRevisionID: expected.destinationRevisionID, channelName: expected.channelName}
	current, err := scanStreamChannelState(tx.QueryRowContext(ctx, streamChannelStateLookupSQL(cfg), key.flowIncarnationID, key.destinationRevisionID, key.channelName))
	if errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("%w: cleanup channel state disappeared before release", connector.ErrDeliveryConflict)
	}
	if err != nil {
		return false, fmt.Errorf("read streaming Snowflake cleanup channel state: %w", err)
	}
	if current != expected {
		return false, fmt.Errorf("%w: cleanup channel state changed before release", connector.ErrDeliveryConflict)
	}
	var unresolved int64
	if err := tx.QueryRowContext(ctx, streamRequestUnresolvedSQL(cfg), key.flowIncarnationID, key.destinationRevisionID, key.channelName).Scan(&unresolved); err != nil {
		return false, fmt.Errorf("read unresolved streaming Snowflake cleanup requests: %w", err)
	}
	if unresolved != 0 {
		return false, fmt.Errorf("%w: cleanup channel has unresolved requests", connector.ErrDeliveryIndeterminate)
	}
	if _, err := tx.ExecContext(ctx, streamReceiptInsertSQL(cfg), streamReceiptValues(release)...); err != nil && !isStagedDuplicateKey(err) {
		return false, fmt.Errorf("insert streaming Snowflake release receipt: %w", err)
	}
	result, err := tx.ExecContext(ctx, streamChannelStateDeleteCASSQL(cfg), expected.flowIncarnationID, expected.destinationRevisionID, expected.channelName, expected.stateVersion, expected.pipeName, expected.pipeRevision, expected.channelRevision, expected.continuationToken, expected.committedOffsetToken, expected.logicalBatchID, expected.rowsContentHash, expected.requestID, expected.flowIncarnationID, expected.destinationRevisionID, expected.channelName)
	if err != nil {
		return false, fmt.Errorf("delete streaming Snowflake channel state: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		return false, fmt.Errorf("%w: cleanup channel-state CAS affected %d rows", connector.ErrDeliveryIndeterminate, affected)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("%w: commit streaming Snowflake channel release: %w", connector.ErrDeliveryIndeterminate, err)
	}
	return true, nil
}
