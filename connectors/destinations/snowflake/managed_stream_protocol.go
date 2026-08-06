package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

var (
	// errStreamChannelInvalidated means the server rejected an append because the
	// client sequencer / channel revision is stale (another writer reopened the
	// channel, or the channel was dropped and recreated). The driver must reopen,
	// re-read the committed offset, recompute proven-missing rows, and retry.
	errStreamChannelInvalidated = errors.New("streaming Snowflake channel is invalidated")
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
	cfg               streamConfig
	channelName       string
	channelRevision   int64
	continuationToken string
	offsetToken       string
	rows              []streamAppendRow
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
	continuationToken string
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
type streamProtocol interface {
	// OpenChannel opens (or reopens) the deterministic named channel and returns
	// its durable status, including the last committed offset token.
	OpenChannel(ctx context.Context, cfg streamConfig, channelName string) (streamChannelStatus, error)
	// AppendRows appends only the supplied rows to an open channel. It returns
	// per-row rejections; a rejection is never silently retried as success.
	AppendRows(ctx context.Context, req streamAppendRequest) (streamAppendResult, error)
	// ChannelStatus re-reads the durable channel status (committed offset token)
	// without appending.
	ChannelStatus(ctx context.Context, cfg streamConfig, channelName string) (streamChannelStatus, error)
	// ObserveCommittedRows reports, by deterministic ROW_HASH, which of the
	// supplied rows are durably present in the streaming target for this logical
	// batch. This SQL observation — not any transport token — proves completeness.
	ObserveCommittedRows(ctx context.Context, cfg streamConfig, logicalBatchID string, rowHashes []string) (map[string]int, error)
	// UpsertChannelState persists the exact channel/pipe revision, continuation,
	// and committed-token evidence for one channel.
	UpsertChannelState(ctx context.Context, cfg streamConfig, state managedStreamChannelState) error
	// LookupChannelState returns the persisted channel evidence.
	LookupChannelState(ctx context.Context, cfg streamConfig, key streamChannelStateKey) (managedStreamChannelState, bool, error)
	// LookupReceipt returns the durable destination receipt for one identity.
	LookupReceipt(ctx context.Context, cfg streamConfig, key streamReceiptKey) (managedStreamReceipt, bool, error)
	// InsertReceipt writes the receipt atomically; a duplicate primary key is a
	// non-error signal that a concurrent owner already committed it.
	InsertReceipt(ctx context.Context, cfg streamConfig, receipt managedStreamReceipt) (streamReceiptInsert, error)
	// ListReleasableReceipts returns append receipts for one flow incarnation
	// older than the retention window and not yet released, bounded by limit.
	ListReleasableReceipts(ctx context.Context, cfg streamConfig, flowIncarnationID string, retention time.Duration, limit int) ([]managedStreamReceipt, error)
	// DeleteChannelState removes one persisted channel-state row during cleanup.
	DeleteChannelState(ctx context.Context, cfg streamConfig, key streamChannelStateKey) error
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

func (p *sqlStreamProtocol) ObserveCommittedRows(ctx context.Context, cfg streamConfig, logicalBatchID string, rowHashes []string) (map[string]int, error) {
	present := make(map[string]int, len(rowHashes))
	if len(rowHashes) == 0 {
		return present, nil
	}
	target := managedSnowflakeStreamQualified(cfg, cfg.table)
	// #nosec G202 -- the target identifier is composed only of validated unquoted
	// uppercase identifiers; logical batch and row hashes are bound parameters.
	query := "SELECT \"ROW_HASH\", COUNT(*) FROM " + target +
		" WHERE \"LOGICAL_BATCH_ID\" = ? AND \"ROW_HASH\" IN (" + streamPlaceholders(len(rowHashes)) + ")" +
		" GROUP BY \"ROW_HASH\""
	args := make([]any, 0, len(rowHashes)+1)
	args = append(args, logicalBatchID)
	for _, hash := range rowHashes {
		args = append(args, hash)
	}
	rows, err := p.db.QueryContext(ctx, query, args...)
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

func (p *sqlStreamProtocol) UpsertChannelState(ctx context.Context, cfg streamConfig, state managedStreamChannelState) error {
	ctx, recordQueryID := managedSnowflakeQueryIDContext(ctx)
	_, err := p.db.ExecContext(ctx, streamChannelStateMergeSQL(cfg), streamChannelStateValues(state)...)
	recordQueryID()
	if err != nil {
		return fmt.Errorf("%w: upsert streaming Snowflake channel state: %w", connector.ErrDeliveryIndeterminate, err)
	}
	return nil
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
	// #nosec G202 -- the receipts table identifier is composed only of validated unquoted uppercase identifiers; all values are bound parameters.
	query := "SELECT " + streamReceiptColumnsQualified("L") + " FROM " + table + " AS L" +
		" WHERE L.\"RECEIPT_KIND\" = ? AND L.\"FLOW_INCARNATION_ID\" = ? AND L.\"RECEIPT_STATUS\" = ?" +
		" AND L.\"COMMITTED_AT\" < DATEADD('second', ?, CURRENT_TIMESTAMP())" +
		" AND NOT EXISTS (SELECT 1 FROM " + table + " AS R WHERE R.\"RECEIPT_KIND\" = ? AND R.\"EXTERNAL_ID\" = L.\"EXTERNAL_ID\" || ':release')" +
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

func (p *sqlStreamProtocol) DeleteChannelState(ctx context.Context, cfg streamConfig, key streamChannelStateKey) error {
	ctx, recordQueryID := managedSnowflakeQueryIDContext(ctx)
	_, err := p.db.ExecContext(ctx, streamChannelStateDeleteSQL(cfg), key.flowIncarnationID, key.destinationRevisionID, key.channelName)
	recordQueryID()
	if err != nil {
		return fmt.Errorf("delete streaming Snowflake channel state: %w", err)
	}
	return nil
}

func streamPlaceholders(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.TrimRight(strings.Repeat("?,", count), ",")
}
