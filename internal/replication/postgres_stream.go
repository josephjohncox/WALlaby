package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	postgrescodec "github.com/josephjohncox/wallaby/internal/postgres"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// PostgresStream implements logical replication using pgoutput.
type PostgresStream struct {
	dsn                    string
	outputPlugin           string
	statusInterval         time.Duration
	startLSN               pglogrepl.LSN
	pluginArgs             []string
	protocolVersion        int
	streamingTransactions  bool
	createSlot             bool
	requireAuthorizedStart bool
	expectedSystemID       string
	maxTransactionRecords  int
	maxTransactionBytes    int64
	typeMap                *pgtype.Map
	schemaHook             SchemaHook
	typeResolver           TypeResolver
	typeMu                 sync.Mutex
	typeNames              map[uint32]string
	connConfigFunc         func(context.Context, *pgconn.Config) error
	protocolError          func(context.Context, string)

	mu                 sync.Mutex
	conn               *pgconn.PgConn
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	changes            chan Change
	lastErr            error
	ackLSN             pglogrepl.LSN
	recvLSN            pglogrepl.LSN
	initialLSN         pglogrepl.LSN
	feedbackBarrier    pglogrepl.LSN
	feedbackWaiters    []*sourceFeedbackWaiter
	streamedStarts     uint64
	streamedSubaborts  uint64
	relations          map[uint32]*pglogrepl.RelationMessage
	schemas            map[uint32]connector.Schema
	versions           map[uint32]int64
	schemaBaselines    map[string]connector.Schema
	transaction        *pendingTransaction
	inStream           bool
	streamXID          uint32
	streamMessageXID   uint32
	streamTransactions map[uint32]*pendingTransaction

	emitPlanDDL      bool
	ddlMessagePrefix string
}

type pendingTransaction struct {
	xid                   uint32
	beginLSN              pglogrepl.LSN
	finalLSN              pglogrepl.LSN
	changes               []Change
	records               int
	bytes                 int64
	sequence              uint64
	subtransactionOffsets map[uint32]pendingTransactionOffset
	relationEvents        []pendingRelationEvent
	typeEvents            []pendingTypeEvent
	metadataEvents        []pendingMetadataEvent
	relationOverrides     map[uint32]*pglogrepl.RelationMessage
	schemaOverrides       map[uint32]connector.Schema
	typeOverrides         map[uint32]string
}

type pendingTransactionOffset struct {
	changeCount   int
	relationCount int
	typeCount     int
	metadataCount int
	records       int
	bytes         int64
	sequence      uint64
}

type pendingRelationEvent struct {
	relation *pglogrepl.RelationMessage
	schema   connector.Schema
	plan     internalschema.Plan
	lsn      pglogrepl.LSN
}

type pendingTypeEvent struct {
	oid  uint32
	name string
}

type pendingMetadataEvent struct {
	relation *pendingRelationEvent
	typeInfo *pendingTypeEvent
	ddl      string
	lsn      pglogrepl.LSN
}

func (t *pendingTransaction) noteXID(xid uint32) {
	if xid == 0 {
		xid = t.xid
	}
	if t.subtransactionOffsets == nil {
		t.subtransactionOffsets = make(map[uint32]pendingTransactionOffset)
	}
	if _, exists := t.subtransactionOffsets[xid]; !exists {
		t.subtransactionOffsets[xid] = pendingTransactionOffset{
			changeCount: len(t.changes), relationCount: len(t.relationEvents),
			typeCount: len(t.typeEvents), metadataCount: len(t.metadataEvents), records: t.records,
			bytes: t.bytes, sequence: t.sequence,
		}
	}
	t.sequence++
}

func (t *pendingTransaction) abortSubtransaction(xid uint32) error {
	offset, ok := t.subtransactionOffsets[xid]
	if !ok {
		// PostgreSQL may report an aborted subtransaction whose changes never
		// crossed logical_decoding_work_mem and therefore were never streamed.
		// There is no buffered suffix to discard in that case.
		return nil
	}
	for trackedXID, tracked := range t.subtransactionOffsets {
		if tracked.sequence >= offset.sequence && trackedXID != t.xid {
			delete(t.subtransactionOffsets, trackedXID)
		}
	}
	for index := offset.changeCount; index < len(t.changes); index++ {
		t.changes[index] = Change{}
	}
	t.changes = t.changes[:offset.changeCount]
	for index := offset.relationCount; index < len(t.relationEvents); index++ {
		t.relationEvents[index] = pendingRelationEvent{}
	}
	t.relationEvents = t.relationEvents[:offset.relationCount]
	for index := offset.typeCount; index < len(t.typeEvents); index++ {
		t.typeEvents[index] = pendingTypeEvent{}
	}
	t.typeEvents = t.typeEvents[:offset.typeCount]
	for index := offset.metadataCount; index < len(t.metadataEvents); index++ {
		t.metadataEvents[index] = pendingMetadataEvent{}
	}
	t.metadataEvents = t.metadataEvents[:offset.metadataCount]
	t.records = offset.records
	t.bytes = offset.bytes
	t.rebuildTransactionOverrides()
	return nil
}

func (t *pendingTransaction) recordRelation(xid uint32, event pendingRelationEvent) {
	t.noteXID(xid)
	t.relationEvents = append(t.relationEvents, event)
	eventCopy := event
	t.metadataEvents = append(t.metadataEvents, pendingMetadataEvent{relation: &eventCopy})
	if t.relationOverrides == nil {
		t.relationOverrides = make(map[uint32]*pglogrepl.RelationMessage)
		t.schemaOverrides = make(map[uint32]connector.Schema)
	}
	t.relationOverrides[event.relation.RelationID] = event.relation
	t.schemaOverrides[event.relation.RelationID] = event.schema
}

func (t *pendingTransaction) recordType(xid uint32, event pendingTypeEvent) {
	t.noteXID(xid)
	t.typeEvents = append(t.typeEvents, event)
	eventCopy := event
	t.metadataEvents = append(t.metadataEvents, pendingMetadataEvent{typeInfo: &eventCopy})
	if t.typeOverrides == nil {
		t.typeOverrides = make(map[uint32]string)
	}
	t.typeOverrides[event.oid] = event.name
}

func (t *pendingTransaction) recordDDL(xid uint32, ddl string, lsn pglogrepl.LSN) {
	t.noteXID(xid)
	t.metadataEvents = append(t.metadataEvents, pendingMetadataEvent{ddl: ddl, lsn: lsn})
}

func (t *pendingTransaction) rebuildTransactionOverrides() {
	relationOverrides := make(map[uint32]*pglogrepl.RelationMessage)
	schemaOverrides := make(map[uint32]connector.Schema)
	for _, event := range t.relationEvents {
		relationOverrides[event.relation.RelationID] = event.relation
		schemaOverrides[event.relation.RelationID] = event.schema
	}
	t.relationOverrides = relationOverrides
	t.schemaOverrides = schemaOverrides
	t.typeOverrides = make(map[uint32]string)
	for _, event := range t.typeEvents {
		t.typeOverrides[event.oid] = event.name
	}
}

type sourceFeedbackWaiter struct {
	lsn    pglogrepl.LSN
	result chan error
}

// PostgresStreamOption configures the stream.
type PostgresStreamOption func(*PostgresStream)

func WithOutputPlugin(plugin string) PostgresStreamOption {
	return func(s *PostgresStream) {
		s.outputPlugin = plugin
	}
}

func WithStatusInterval(interval time.Duration) PostgresStreamOption {
	return func(s *PostgresStream) {
		s.statusInterval = interval
	}
}

func WithStartLSN(lsn pglogrepl.LSN) PostgresStreamOption {
	return func(s *PostgresStream) {
		s.startLSN = lsn
	}
}

func WithPluginArgs(args []string) PostgresStreamOption {
	return func(s *PostgresStream) {
		s.pluginArgs = args
	}
}

// WithStreamingTransactions enables pgoutput protocol v2 and PostgreSQL's
// streamed in-progress transaction messages.
func WithStreamingTransactions(enabled bool) PostgresStreamOption {
	return func(s *PostgresStream) {
		s.streamingTransactions = enabled
		if enabled {
			s.protocolVersion = 2
		}
	}
}

func WithCreateSlot(enabled bool) PostgresStreamOption {
	return func(s *PostgresStream) {
		s.createSlot = enabled
	}
}

// WithRequireAuthorizedStart rejects an existing logical slot unless the
// caller supplied a PostgreSQL-authoritative durable checkpoint.
func WithRequireAuthorizedStart(enabled bool) PostgresStreamOption {
	return func(s *PostgresStream) {
		s.requireAuthorizedStart = enabled
	}
}

// WithExpectedSystemID binds a managed stream to PostgreSQL's immutable system
// identifier rather than relying on a reusable database name.
func WithExpectedSystemID(systemID string) PostgresStreamOption {
	return func(s *PostgresStream) {
		s.expectedSystemID = strings.TrimSpace(systemID)
	}
}

// WithTransactionLimits bounds decoded changes retained until COMMIT.
func WithTransactionLimits(maxRecords int, maxBytes int64) PostgresStreamOption {
	return func(s *PostgresStream) {
		if maxRecords > 0 {
			s.maxTransactionRecords = maxRecords
		}
		if maxBytes > 0 {
			s.maxTransactionBytes = maxBytes
		}
	}
}

func WithTypeMap(typeMap *pgtype.Map) PostgresStreamOption {
	return func(s *PostgresStream) {
		s.typeMap = typeMap
	}
}

func WithSchemaHook(hook SchemaHook) PostgresStreamOption {
	return func(s *PostgresStream) {
		s.schemaHook = hook
	}
}

// WithSchemaBaselines seeds the decoder from schemas stored on the last
// PostgreSQL-authoritative checkpoint. This makes the first Relation message
// after restart comparable with what was actually delivered before restart.
func WithSchemaBaselines(baselines []connector.Schema) PostgresStreamOption {
	return func(s *PostgresStream) {
		for _, schema := range baselines {
			s.schemaBaselines[relationSchemaKey(schema.Namespace, schema.Name)] = schema
		}
	}
}

func WithTypeResolver(resolver TypeResolver) PostgresStreamOption {
	return func(s *PostgresStream) {
		s.typeResolver = resolver
	}
}

func WithDDLMessagePrefix(prefix string) PostgresStreamOption {
	return func(s *PostgresStream) {
		s.ddlMessagePrefix = prefix
	}
}

func WithEmitPlanDDL(enabled bool) PostgresStreamOption {
	return func(s *PostgresStream) {
		s.emitPlanDDL = enabled
	}
}

func WithConnConfigFunc(fn func(context.Context, *pgconn.Config) error) PostgresStreamOption {
	return func(s *PostgresStream) {
		s.connConfigFunc = fn
	}
}

func WithProtocolErrorReporter(fn func(context.Context, string)) PostgresStreamOption {
	return func(s *PostgresStream) {
		s.protocolError = fn
	}
}

// SchemaHook receives schema evolution and DDL events.
type SchemaHook interface {
	OnSchema(ctx context.Context, schema connector.Schema) error
	OnSchemaChange(ctx context.Context, plan internalschema.Plan) error
	OnDDL(ctx context.Context, ddl string, lsn pglogrepl.LSN) error
}

// SchemaChangeLSNHook is a wire-compatible extension used by fenced registry
// hooks that require the exact WAL identity of a catalog transition.
type SchemaChangeLSNHook interface {
	OnSchemaChangeAtLSN(ctx context.Context, plan internalschema.Plan, lsn pglogrepl.LSN) error
}

// TypeResolver resolves Postgres type OIDs to names.
type TypeResolver interface {
	ResolveType(ctx context.Context, oid uint32) (string, bool, error)
}

// TypeInfo provides extended metadata for Postgres types.
type TypeInfo struct {
	OID       uint32
	Name      string
	Schema    string
	Extension string
}

// TypeInfoResolver optionally exposes richer type metadata.
type TypeInfoResolver interface {
	ResolveTypeInfo(ctx context.Context, oid uint32) (TypeInfo, bool, error)
}

// RelationColumnIdentityResolver returns PostgreSQL's stable pg_attribute
// number for a relation column.
type RelationColumnIdentityResolver interface {
	ResolveColumnIdentity(context.Context, uint32, string) (int16, bool, error)
}

// NewPostgresStream returns a Postgres logical replication stream.
func NewPostgresStream(dsn string, opts ...PostgresStreamOption) *PostgresStream {
	stream := &PostgresStream{
		dsn:                   dsn,
		outputPlugin:          "pgoutput",
		statusInterval:        10 * time.Second,
		protocolVersion:       1,
		createSlot:            true,
		maxTransactionRecords: 1_000_000,
		maxTransactionBytes:   256 << 20,
		relations:             make(map[uint32]*pglogrepl.RelationMessage),
		schemas:               make(map[uint32]connector.Schema),
		versions:              make(map[uint32]int64),
		schemaBaselines:       make(map[string]connector.Schema),
		streamTransactions:    make(map[uint32]*pendingTransaction),
		typeNames:             make(map[uint32]string),
		emitPlanDDL:           true,
		ddlMessagePrefix:      "wallaby_ddl",
	}

	for _, opt := range opts {
		opt(stream)
	}

	if stream.typeMap == nil {
		stream.typeMap = pgtype.NewMap()
	}
	postgrescodec.RegisterRawJSONCodecs(stream.typeMap)

	return stream
}

// Start begins streaming changes for the given replication slot and publication.
func (p *PostgresStream) Start(ctx context.Context, slot, publication string) (<-chan Change, error) {
	if p.dsn == "" {
		return nil, errors.New("postgres DSN is required")
	}
	if slot == "" {
		return nil, errors.New("replication slot is required")
	}
	if publication == "" {
		return nil, errors.New("publication is required")
	}

	cfg, err := pgconn.ParseConfig(p.dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.RuntimeParams["replication"] = "database"
	if p.connConfigFunc != nil {
		if err := p.connConfigFunc(ctx, cfg); err != nil {
			return nil, fmt.Errorf("configure replication connection: %w", err)
		}
	}

	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect replication: %w", err)
	}

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("identify system: %w", err)
	}
	if p.expectedSystemID != "" && sysident.SystemID != p.expectedSystemID {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("PostgreSQL system identifier %s does not match managed source lineage %s", sysident.SystemID, p.expectedSystemID)
	}
	if p.requireAuthorizedStart && p.expectedSystemID == "" {
		_ = conn.Close(ctx)
		return nil, errors.New("managed replication requires source_system_identifier")
	}

	slotState, err := loadReplicationSlotState(ctx, conn, slot)
	if errors.Is(err, errReplicationSlotNotFound) {
		slotState = nil
	} else if err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}

	startLSN := p.startLSN
	if slotState == nil {
		if !p.createSlot {
			_ = conn.Close(ctx)
			return nil, fmt.Errorf("replication slot %q does not exist", slot)
		}
		created, createErr := pglogrepl.CreateReplicationSlot(ctx, conn, slot, p.outputPlugin, pglogrepl.CreateReplicationSlotOptions{})
		if createErr != nil {
			if !isSlotExistsErr(createErr) {
				_ = conn.Close(ctx)
				return nil, fmt.Errorf("create replication slot: %w", createErr)
			}
			// A concurrent creator won. Inspect and validate the exact slot rather
			// than treating duplicate_object as proof of compatibility.
			slotState, err = loadReplicationSlotState(ctx, conn, slot)
			if errors.Is(err, errReplicationSlotNotFound) {
				_ = conn.Close(ctx)
				return nil, fmt.Errorf("replication slot %q disappeared after concurrent creation", slot)
			}
			if err != nil {
				_ = conn.Close(ctx)
				return nil, err
			}
			if slotState == nil {
				_ = conn.Close(ctx)
				return nil, fmt.Errorf("replication slot %q disappeared after concurrent creation", slot)
			}
		} else {
			consistentPoint, parseErr := pglogrepl.ParseLSN(created.ConsistentPoint)
			if parseErr != nil {
				_ = conn.Close(ctx)
				return nil, fmt.Errorf("parse new slot consistent point %q: %w", created.ConsistentPoint, parseErr)
			}
			startLSN, err = resolveNewSlotStart(p.requireAuthorizedStart, startLSN, consistentPoint)
			if err != nil {
				_ = conn.Close(ctx)
				return nil, err
			}
		}
	}
	if slotState != nil {
		if err := validateExistingSlotAuthorization(p.requireAuthorizedStart, startLSN); err != nil {
			_ = conn.Close(ctx)
			return nil, fmt.Errorf("validate replication slot %q: %w", slot, err)
		}
		startLSN, err = resolveSlotStart(*slotState, p.outputPlugin, sysident.DBName, startLSN, sysident.XLogPos)
		if err != nil {
			_ = conn.Close(ctx)
			return nil, fmt.Errorf("validate replication slot %q: %w", slot, err)
		}
	}

	pluginArgs := p.pluginArgs
	if len(pluginArgs) == 0 {
		protocolVersion := 1
		if p.streamingTransactions {
			protocolVersion = 2
		}
		pluginArgs = []string{
			fmt.Sprintf("proto_version '%d'", protocolVersion),
			fmt.Sprintf("publication_names '%s'", publication),
		}
		if p.streamingTransactions {
			pluginArgs = append(pluginArgs, "streaming 'on'")
		}
	}

	if err := pglogrepl.StartReplication(ctx, conn, slot, startLSN, pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("start replication: %w", err)
	}

	streamCtx, cancel := context.WithCancel(ctx)
	changes := make(chan Change, 256)

	p.mu.Lock()
	p.conn = conn
	p.cancel = cancel
	p.changes = changes
	p.initialLSN = startLSN
	if p.requireAuthorizedStart {
		p.feedbackBarrier = startLSN
	}
	p.mu.Unlock()

	p.wg.Add(1)
	go p.consume(streamCtx)

	return changes, nil
}

// Stop terminates replication streaming.
func (p *PostgresStream) Stop(ctx context.Context) error {
	p.mu.Lock()
	cancel := p.cancel
	conn := p.conn
	p.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	p.wg.Wait()

	if conn != nil {
		return conn.Close(ctx)
	}
	return nil
}

// Err returns the last error observed by the stream.
func (p *PostgresStream) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastErr
}

// Ack advances the acknowledged LSN for standby status updates.
func (p *PostgresStream) Ack(lsn pglogrepl.LSN) {
	p.mu.Lock()
	if lsn > p.ackLSN {
		p.ackLSN = lsn
	}
	p.mu.Unlock()
}

// AckWithEvidence sends the authorized position on the replication connection
// and returns only after the standby-status write succeeds.
func (p *PostgresStream) AckWithEvidence(ctx context.Context, lsn pglogrepl.LSN) error {
	waiter := &sourceFeedbackWaiter{lsn: lsn, result: make(chan error, 1)}
	p.mu.Lock()
	if p.conn == nil || p.cancel == nil {
		p.mu.Unlock()
		return errors.New("replication stream is not running")
	}
	p.feedbackWaiters = append(p.feedbackWaiters, waiter)
	p.mu.Unlock()
	select {
	case err := <-waiter.result:
		return err
	case <-ctx.Done():
		if p.cancelFeedbackWaiter(waiter) {
			return ctx.Err()
		}
		// Once the replication loop claims a request it also advances ackLSN.
		// That send is non-abandonable: the fenced coordinator must not release
		// takeover authority until the replication connection reports its result.
		return <-waiter.result
	}
}

// InitialLSN returns the validated start point used for this stream. For a
// newly-created slot this is PostgreSQL's returned consistent point.
func (p *PostgresStream) InitialLSN() pglogrepl.LSN {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.initialLSN
}

// LastReceivedLSN returns the most recent LSN observed from WAL data.
func (p *PostgresStream) LastReceivedLSN() pglogrepl.LSN {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.recvLSN
}

func (p *PostgresStream) consume(ctx context.Context) {
	defer p.wg.Done()
	defer func() {
		p.failFeedbackWaiters(errors.New("replication stream stopped before source feedback was sent"))
		p.mu.Lock()
		if p.changes != nil {
			close(p.changes)
		}
		p.mu.Unlock()
	}()

	p.mu.Lock()
	conn := p.conn
	p.mu.Unlock()
	if conn == nil {
		p.setErr(errors.New("replication connection not initialized"))
		return
	}

	nextStandbyMessageDeadline := time.Now().Add(p.statusInterval)

	for {
		if ctx.Err() != nil {
			return
		}

		if barrier := p.feedbackBarrierPosition(); barrier != 0 {
			if err := p.waitForFeedbackBarrier(ctx, conn, barrier); err != nil {
				p.setErr(err)
				return
			}
			nextStandbyMessageDeadline = time.Now().Add(p.statusInterval)
			continue
		}

		waiters, claimErr := p.claimFeedbackWaiters(0)
		if claimErr != nil {
			p.setErr(claimErr)
			return
		}
		if len(waiters) > 0 {
			err := p.sendStandbyStatus(ctx, conn)
			for _, waiter := range waiters {
				waiter.result <- err
			}
			if err != nil {
				p.setErr(fmt.Errorf("send authorized standby status: %w", err))
				return
			}
			nextStandbyMessageDeadline = time.Now().Add(p.statusInterval)
		}

		if time.Now().After(nextStandbyMessageDeadline) {
			if err := p.sendStandbyStatus(ctx, conn); err != nil {
				p.setErr(fmt.Errorf("send standby status: %w", err))
				return
			}
			nextStandbyMessageDeadline = time.Now().Add(p.statusInterval)
		}

		receiveDeadline := nextStandbyMessageDeadline
		feedbackPollDeadline := time.Now().Add(100 * time.Millisecond)
		if receiveDeadline.IsZero() || feedbackPollDeadline.Before(receiveDeadline) {
			receiveDeadline = feedbackPollDeadline
		}
		deadlineCtx, cancel := context.WithDeadline(ctx, receiveDeadline)
		rawMsg, err := conn.ReceiveMessage(deadlineCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			p.setErr(fmt.Errorf("receive message: %w", err))
			return
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			p.setErr(fmt.Errorf("postgres error: %s", errMsg.Message))
			return
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		if len(msg.Data) == 0 {
			p.recordProtocolError(ctx, "empty_message_payload")
			p.setErr(errors.New("replication message payload is empty"))
			return
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				p.recordProtocolError(ctx, "parse_keepalive")
				p.setErr(fmt.Errorf("parse keepalive: %w", err))
				return
			}
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				p.recordProtocolError(ctx, "parse_xlogdata")
				p.setErr(fmt.Errorf("parse xlogdata: %w", err))
				return
			}

			if err := p.handleWal(ctx, xld); err != nil {
				p.setErr(err)
				return
			}

			// WALStart is a protocol location. WALData length is not an LSN delta;
			// transaction-end progress is carried by CommitMessage and acknowledged
			// only after the durable source checkpoint is committed.
			p.setReceivedLSN(logicalReceivedPosition(xld))
		default:
			p.recordProtocolError(ctx, "unsupported_message_type")
			p.setErr(fmt.Errorf("unsupported replication message type: %d", msg.Data[0]))
			return
		}
	}
}

func (p *PostgresStream) feedbackBarrierPosition() pglogrepl.LSN {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.feedbackBarrier
}

func (p *PostgresStream) requireFeedbackAt(lsn pglogrepl.LSN) {
	if !p.requireAuthorizedStart || lsn == 0 {
		return
	}
	p.mu.Lock()
	p.feedbackBarrier = lsn
	p.mu.Unlock()
}

func (p *PostgresStream) waitForFeedbackBarrier(ctx context.Context, conn *pgconn.PgConn, expected pglogrepl.LSN) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	statusTicker := time.NewTicker(p.statusInterval)
	defer statusTicker.Stop()
	for {
		waiters, mismatch := p.claimFeedbackWaiters(expected)
		if len(waiters) > 0 {
			if mismatch != nil {
				for _, waiter := range waiters {
					waiter.result <- mismatch
				}
				return mismatch
			}
			err := p.sendStandbyStatus(ctx, conn)
			for _, waiter := range waiters {
				waiter.result <- err
			}
			if err != nil {
				return fmt.Errorf("send required source feedback: %w", err)
			}
			p.mu.Lock()
			if p.feedbackBarrier == expected {
				p.feedbackBarrier = 0
			}
			p.mu.Unlock()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-statusTicker.C:
			if err := p.sendStandbyStatus(ctx, conn); err != nil {
				return fmt.Errorf("keep replication sender alive while awaiting feedback: %w", err)
			}
		case <-ticker.C:
		}
	}
}

func (p *PostgresStream) claimFeedbackWaiters(expected pglogrepl.LSN) ([]*sourceFeedbackWaiter, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.feedbackWaiters) == 0 {
		return nil, nil
	}
	waiters := append([]*sourceFeedbackWaiter(nil), p.feedbackWaiters...)
	p.feedbackWaiters = nil
	if expected != 0 {
		for _, waiter := range waiters {
			if waiter.lsn != expected {
				return waiters, fmt.Errorf("authorized source feedback %s does not match required transaction boundary %s", waiter.lsn, expected)
			}
		}
	}
	for _, waiter := range waiters {
		if waiter.lsn > p.ackLSN {
			p.ackLSN = waiter.lsn
		}
	}
	return waiters, nil
}

func (p *PostgresStream) cancelFeedbackWaiter(target *sourceFeedbackWaiter) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for index, waiter := range p.feedbackWaiters {
		if waiter != target {
			continue
		}
		copy(p.feedbackWaiters[index:], p.feedbackWaiters[index+1:])
		p.feedbackWaiters[len(p.feedbackWaiters)-1] = nil
		p.feedbackWaiters = p.feedbackWaiters[:len(p.feedbackWaiters)-1]
		return true
	}
	return false
}

func (p *PostgresStream) failFeedbackWaiters(err error) {
	p.mu.Lock()
	waiters := append([]*sourceFeedbackWaiter(nil), p.feedbackWaiters...)
	p.feedbackWaiters = nil
	p.mu.Unlock()
	for _, waiter := range waiters {
		waiter.result <- err
	}
}

func (p *PostgresStream) sendStandbyStatus(ctx context.Context, conn *pgconn.PgConn) error {
	ackLSN := p.ackPosition()
	return pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: ackLSN,
		WALFlushPosition: ackLSN,
		WALApplyPosition: ackLSN,
	})
}

func (p *PostgresStream) handleWal(ctx context.Context, xld pglogrepl.XLogData) error {
	var logicalMsg pglogrepl.Message
	var err error
	if p.protocolVersion >= 2 {
		logicalMsg, err = pglogrepl.ParseV2(xld.WALData, p.inStream)
	} else {
		logicalMsg, err = pglogrepl.Parse(xld.WALData)
	}
	if err != nil {
		p.recordProtocolError(ctx, "logical_message_parse")
		return fmt.Errorf("parse logical message: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.StreamStartMessageV2:
		if p.inStream || p.transaction != nil {
			p.recordProtocolError(ctx, "nested_stream_start")
			return errors.New("received streamed transaction segment while another segment is active")
		}
		transaction := p.streamTransactions[msg.Xid]
		if msg.FirstSegment == 1 {
			if transaction != nil {
				p.recordProtocolError(ctx, "duplicate_stream_first_segment")
				return fmt.Errorf("received duplicate first stream segment for xid=%d", msg.Xid)
			}
			transaction = &pendingTransaction{xid: msg.Xid, beginLSN: xld.WALStart}
			p.streamTransactions[msg.Xid] = transaction
			p.mu.Lock()
			p.streamedStarts++
			p.mu.Unlock()
		} else if transaction == nil {
			p.recordProtocolError(ctx, "stream_segment_without_first")
			return fmt.Errorf("received continuation stream segment without first segment for xid=%d", msg.Xid)
		}
		p.inStream = true
		p.streamXID = msg.Xid
		p.streamMessageXID = msg.Xid
		p.transaction = transaction
		return nil
	case *pglogrepl.StreamStopMessageV2:
		if !p.inStream || p.transaction == nil {
			p.recordProtocolError(ctx, "stream_stop_without_start")
			return errors.New("received stream stop without an active segment")
		}
		p.inStream = false
		p.streamXID = 0
		p.streamMessageXID = 0
		p.transaction = nil
		return nil
	case *pglogrepl.StreamCommitMessageV2:
		if p.inStream {
			p.recordProtocolError(ctx, "stream_commit_during_segment")
			return errors.New("received stream commit before stream stop")
		}
		transaction := p.streamTransactions[msg.Xid]
		if transaction == nil {
			p.recordProtocolError(ctx, "stream_commit_without_transaction")
			return fmt.Errorf("received stream commit without buffered xid=%d", msg.Xid)
		}
		delete(p.streamTransactions, msg.Xid)
		p.streamMessageXID = 0
		return p.commitPendingTransaction(ctx, transaction, msg.CommitLSN, msg.TransactionEndLSN, msg.CommitTime)
	case *pglogrepl.StreamAbortMessageV2:
		transaction := p.streamTransactions[msg.Xid]
		if transaction == nil {
			p.recordProtocolError(ctx, "stream_abort_without_transaction")
			return fmt.Errorf("received stream abort without buffered xid=%d", msg.Xid)
		}
		if p.inStream && p.streamXID == msg.Xid {
			p.inStream = false
			p.streamXID = 0
			p.streamMessageXID = 0
			p.transaction = nil
		}
		if msg.SubXid != msg.Xid {
			if err := transaction.abortSubtransaction(msg.SubXid); err != nil {
				p.recordProtocolError(ctx, "stream_subtransaction_abort_without_offset")
				return err
			}
			p.mu.Lock()
			p.streamedSubaborts++
			p.mu.Unlock()
			return nil
		}
		delete(p.streamTransactions, msg.Xid)
		return nil
	case *pglogrepl.BeginMessage:
		if p.transaction != nil {
			p.recordProtocolError(ctx, "nested_begin")
			return errors.New("received BEGIN while another source transaction is active")
		}
		p.transaction = &pendingTransaction{
			xid:      msg.Xid,
			beginLSN: xld.WALStart,
			finalLSN: msg.FinalLSN,
		}
		p.streamMessageXID = msg.Xid
		return nil
	case *pglogrepl.CommitMessage:
		return p.commitTransaction(ctx, msg)
	case *pglogrepl.RelationMessageV2:
		if err := p.validateStreamXID(msg.Xid); err != nil {
			return err
		}
		return p.handleRelationMessage(ctx, xld, &msg.RelationMessage)
	case *pglogrepl.RelationMessage:
		return p.handleRelationMessage(ctx, xld, msg)
	case *pglogrepl.TypeMessageV2:
		if err := p.validateStreamXID(msg.Xid); err != nil {
			return err
		}
		return p.handleTypeMessage(&msg.TypeMessage)
	case *pglogrepl.TypeMessage:
		return p.handleTypeMessage(msg)
	case *pglogrepl.InsertMessageV2:
		if err := p.validateStreamXID(msg.Xid); err != nil {
			return err
		}
		record, schema, err := p.decodeInsert(&msg.InsertMessage, xld)
		if err != nil {
			p.recordProtocolError(ctx, "decode_insert_v2")
			return err
		}
		return p.emitChange(ctx, xld, schema, record)
	case *pglogrepl.InsertMessage:
		record, schema, err := p.decodeInsert(msg, xld)
		if err != nil {
			p.recordProtocolError(ctx, "decode_insert")
			return err
		}
		return p.emitChange(ctx, xld, schema, record)
	case *pglogrepl.UpdateMessageV2:
		if err := p.validateStreamXID(msg.Xid); err != nil {
			return err
		}
		record, schema, err := p.decodeUpdate(&msg.UpdateMessage, xld)
		if err != nil {
			p.recordProtocolError(ctx, "decode_update_v2")
			return err
		}
		return p.emitChange(ctx, xld, schema, record)
	case *pglogrepl.UpdateMessage:
		record, schema, err := p.decodeUpdate(msg, xld)
		if err != nil {
			p.recordProtocolError(ctx, "decode_update")
			return err
		}
		return p.emitChange(ctx, xld, schema, record)
	case *pglogrepl.DeleteMessageV2:
		if err := p.validateStreamXID(msg.Xid); err != nil {
			return err
		}
		record, schema, err := p.decodeDelete(&msg.DeleteMessage, xld)
		if err != nil {
			p.recordProtocolError(ctx, "decode_delete_v2")
			return err
		}
		return p.emitChange(ctx, xld, schema, record)
	case *pglogrepl.DeleteMessage:
		record, schema, err := p.decodeDelete(msg, xld)
		if err != nil {
			p.recordProtocolError(ctx, "decode_delete")
			return err
		}
		return p.emitChange(ctx, xld, schema, record)
	case *pglogrepl.TruncateMessageV2:
		if err := p.validateStreamXID(msg.Xid); err != nil {
			return err
		}
		return p.emitTruncate(ctx, xld, &msg.TruncateMessage)
	case *pglogrepl.TruncateMessage:
		return p.emitTruncate(ctx, xld, msg)
	case *pglogrepl.LogicalDecodingMessageV2:
		if err := p.validateStreamXID(msg.Xid); err != nil {
			return err
		}
		return p.handleLogicalMessage(ctx, xld, &msg.LogicalDecodingMessage)
	case *pglogrepl.LogicalDecodingMessage:
		return p.handleLogicalMessage(ctx, xld, msg)
	default:
		return nil
	}
}

func (p *PostgresStream) validateStreamXID(xid uint32) error {
	if !p.inStream {
		if xid != 0 {
			p.recordProtocolError(context.Background(), "stream_xid_outside_segment")
			return fmt.Errorf("received streamed message xid=%d outside a stream segment", xid)
		}
		if p.transaction != nil {
			p.streamMessageXID = p.transaction.xid
		}
		return nil
	}
	if p.transaction == nil || p.streamXID == 0 || xid == 0 || p.transaction.xid != p.streamXID {
		p.recordProtocolError(context.Background(), "stream_xid_mismatch")
		return fmt.Errorf("streamed message xid=%d is invalid for active top-level xid=%d", xid, p.streamXID)
	}
	// In protocol v2 this field may be the active subtransaction XID. Stream
	// Abort identifies the top-level and subtransaction separately, so requiring
	// every message to equal streamXID would reject valid savepoint traffic.
	p.streamMessageXID = xid
	return nil
}

func (p *PostgresStream) handleTypeMessage(msg *pglogrepl.TypeMessage) error {
	if msg == nil || msg.DataType == 0 || strings.TrimSpace(msg.Name) == "" {
		return errors.New("logical type message is incomplete")
	}
	name := strings.TrimSpace(msg.Name)
	namespace := strings.TrimSpace(msg.Namespace)
	if namespace != "" && namespace != "pg_catalog" && namespace != "pg_toast" {
		name = namespace + "." + name
	}
	event := pendingTypeEvent{oid: msg.DataType, name: name}
	if p.transaction != nil {
		p.transaction.noteXID(p.streamMessageXID)
		if err := p.reserveTransactionBytes(int64(32+len(name)), "type metadata"); err != nil {
			return err
		}
		p.transaction.recordType(p.streamMessageXID, event)
		return nil
	}
	p.typeMu.Lock()
	p.typeNames[event.oid] = event.name
	p.typeMu.Unlock()
	return nil
}

func (p *PostgresStream) handleRelationMessage(ctx context.Context, xld pglogrepl.XLogData, msg *pglogrepl.RelationMessage) error {
	prevSchema, hasPrev := p.schemas[msg.RelationID]
	if p.transaction != nil {
		if schema, ok := p.transaction.schemaOverrides[msg.RelationID]; ok {
			prevSchema, hasPrev = schema, true
		}
	}
	fromDurableBaseline := false
	if !hasPrev {
		if baseline, ok := p.schemaBaselines[relationSchemaKey(msg.Namespace, msg.RelationName)]; ok {
			prevSchema, hasPrev = baseline, true
			fromDurableBaseline = true
			p.versions[msg.RelationID] = baseline.Version
		}
	}
	schemaDef := p.schemaForRelation(ctx, msg)
	plan := internalschema.Plan{}
	if hasPrev {
		// A Relation message describes the published shape, not the relation, so an
		// absent column must never become a destination DROP COLUMN.
		plan = internalschema.DiffPublishedShape(prevSchema, schemaDef)
	}
	if fromDurableBaseline && !plan.HasChanges() {
		schemaDef.Version = prevSchema.Version
		p.versions[msg.RelationID] = prevSchema.Version
	}
	if p.transaction != nil {
		event := pendingRelationEvent{relation: msg, schema: schemaDef, plan: plan, lsn: xld.WALStart}
		p.transaction.noteXID(p.streamMessageXID)
		if err := p.reserveTransactionBytes(estimateRelationEventBytes(event), "relation metadata"); err != nil {
			return err
		}
		p.transaction.recordRelation(p.streamMessageXID, event)
		if plan.HasChanges() && p.emitPlanDDL {
			if err := p.emitSchemaChange(ctx, xld, schemaDef, plan); err != nil {
				return fmt.Errorf("schema change record: %w", err)
			}
		}
		return nil
	}
	return p.commitRelationEvent(ctx, pendingRelationEvent{
		relation: msg, schema: schemaDef, plan: plan, lsn: xld.WALStart,
	})
}

func (p *PostgresStream) commitMetadataEvents(ctx context.Context, transaction *pendingTransaction) error {
	for _, event := range transaction.metadataEvents {
		if event.typeInfo != nil {
			p.typeMu.Lock()
			p.typeNames[event.typeInfo.oid] = event.typeInfo.name
			p.typeMu.Unlock()
			continue
		}
		if event.relation != nil {
			if err := p.commitRelationEvent(ctx, *event.relation); err != nil {
				return err
			}
			continue
		}
		if p.schemaHook != nil {
			if err := p.schemaHook.OnDDL(ctx, event.ddl, event.lsn); err != nil {
				return fmt.Errorf("ddl hook: %w", err)
			}
		}
	}
	return nil
}

func (p *PostgresStream) commitRelationEvent(ctx context.Context, event pendingRelationEvent) error {
	if p.schemaHook != nil {
		if err := p.schemaHook.OnSchema(ctx, event.schema); err != nil {
			return fmt.Errorf("schema hook: %w", err)
		}
		if event.plan.HasChanges() {
			var hookErr error
			if lsnHook, ok := p.schemaHook.(SchemaChangeLSNHook); ok {
				hookErr = lsnHook.OnSchemaChangeAtLSN(ctx, event.plan, event.lsn)
			} else {
				hookErr = p.schemaHook.OnSchemaChange(ctx, event.plan)
			}
			if hookErr != nil {
				return fmt.Errorf("schema change hook: %w", hookErr)
			}
		}
	}
	p.relations[event.relation.RelationID] = event.relation
	p.schemas[event.relation.RelationID] = event.schema
	return nil
}

func (p *PostgresStream) emitTruncate(ctx context.Context, xld pglogrepl.XLogData, msg *pglogrepl.TruncateMessage) error {
	for _, relID := range msg.RelationIDs {
		schema := p.schemaForRelationID(relID)
		record := connector.Record{
			Table:         schema.Name,
			Operation:     connector.OpDDL,
			SchemaVersion: schema.Version,
			Timestamp:     xld.ServerTime,
		}
		if err := p.emitChange(ctx, xld, schema, &record); err != nil {
			p.recordProtocolError(ctx, "truncate_change")
			return err
		}
	}
	return nil
}

func (p *PostgresStream) handleLogicalMessage(ctx context.Context, xld pglogrepl.XLogData, msg *pglogrepl.LogicalDecodingMessage) error {
	if p.ddlMessagePrefix != "" && msg.Prefix != p.ddlMessagePrefix {
		return nil
	}
	ddl := string(msg.Content)
	if msg.Transactional {
		if p.transaction == nil {
			p.recordProtocolError(ctx, "transactional_message_without_transaction")
			return errors.New("received transactional logical message without an active source transaction")
		}
		p.transaction.noteXID(p.streamMessageXID)
		if err := p.reserveTransactionBytes(int64(32+len(ddl)), "DDL metadata"); err != nil {
			return err
		}
		p.transaction.recordDDL(p.streamMessageXID, ddl, xld.WALStart)
		return p.emitLogicalMessage(ctx, xld, ddl)
	}
	if p.schemaHook != nil {
		if err := p.schemaHook.OnDDL(ctx, ddl, xld.WALStart); err != nil {
			return fmt.Errorf("ddl hook: %w", err)
		}
	}
	return p.sendChange(ctx, logicalMessageChange(xld, ddl))
}

func (p *PostgresStream) recordProtocolError(ctx context.Context, errorType string) {
	if p.protocolError == nil || errorType == "" {
		return
	}
	p.protocolError(ctx, errorType)
}

func (p *PostgresStream) commitTransaction(ctx context.Context, msg *pglogrepl.CommitMessage) error {
	transaction := p.transaction
	if transaction == nil {
		p.recordProtocolError(ctx, "commit_without_begin")
		return errors.New("received COMMIT without an active source transaction")
	}
	p.transaction = nil
	p.streamMessageXID = 0
	return p.commitPendingTransaction(ctx, transaction, msg.CommitLSN, msg.TransactionEndLSN, msg.CommitTime)
}

func (p *PostgresStream) commitPendingTransaction(ctx context.Context, transaction *pendingTransaction, commitLSN, transactionEndLSN pglogrepl.LSN, commitTime time.Time) error {
	if transaction == nil {
		return errors.New("commit pending transaction requires buffered state")
	}
	if err := p.commitMetadataEvents(ctx, transaction); err != nil {
		return err
	}
	if len(transaction.changes) == 0 {
		if err := p.sendChange(ctx, Change{
			LSN:                  transactionEndLSN,
			TransactionID:        transaction.xid,
			TransactionBeginLSN:  transaction.beginLSN,
			TransactionCommitLSN: commitLSN,
			TransactionEndLSN:    transactionEndLSN,
			TransactionFinal:     true,
		}); err != nil {
			return err
		}
		p.requireFeedbackAt(transactionEndLSN)
		return nil
	}

	var ordinal uint64
	for index := range transaction.changes {
		change := transaction.changes[index]
		// Transfer ownership as the transaction is emitted so the decoder does
		// not retain every record map while Source.ReadTransaction assembles the
		// same committed transaction on the consumer side.
		transaction.changes[index] = Change{}
		change.LSN = transactionEndLSN
		change.TransactionID = transaction.xid
		change.TransactionBeginLSN = transaction.beginLSN
		change.TransactionCommitLSN = commitLSN
		change.TransactionEndLSN = transactionEndLSN
		change.TransactionOrdinal = ordinal
		ordinal++
		change.TransactionFinal = index == len(transaction.changes)-1
		if change.Record != nil {
			// XLogData.ServerTime is an observation timestamp and changes on
			// replay. The PostgreSQL commit timestamp is part of the logical
			// transaction and therefore keeps delivery hashes restart-stable.
			change.Record.Timestamp = commitTime
		}
		if err := p.sendChange(ctx, change); err != nil {
			return err
		}
	}
	p.requireFeedbackAt(transactionEndLSN)
	return nil
}

func (p *PostgresStream) reserveTransactionBytes(additional int64, kind string) error {
	if p.transaction == nil || additional <= 0 {
		return nil
	}
	nextBytes := p.transaction.bytes + additional
	if nextBytes > p.maxTransactionBytes {
		return fmt.Errorf(
			"postgres transaction xid=%d exceeds managed buffer limit while buffering %s: bytes=%d/%d",
			p.transaction.xid,
			kind,
			nextBytes,
			p.maxTransactionBytes,
		)
	}
	p.transaction.bytes = nextBytes
	return nil
}

func estimateRelationEventBytes(event pendingRelationEvent) int64 {
	size := int64(256)
	if event.relation != nil {
		size += int64(len(event.relation.Namespace) + len(event.relation.RelationName))
		for _, column := range event.relation.Columns {
			if column != nil {
				size += int64(48 + len(column.Name))
			}
		}
	}
	size += int64(len(event.schema.Namespace) + len(event.schema.Name))
	for _, column := range event.schema.Columns {
		size += int64(96 + len(column.Name) + len(column.Type) + len(column.Expression))
		for key, value := range column.TypeMetadata {
			size += int64(len(key) + len(value) + 16)
		}
	}
	for _, change := range event.plan.Changes {
		size += int64(128 + len(change.Namespace) + len(change.Table) + len(change.Column) + len(change.ToColumn) + len(change.FromType) + len(change.ToType) + len(change.Expression))
	}
	return size
}

func (p *PostgresStream) enqueueChange(ctx context.Context, change Change) error {
	if p.transaction == nil {
		return p.sendChange(ctx, change)
	}
	p.transaction.noteXID(p.streamMessageXID)
	encoded, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("measure transaction change: %w", err)
	}
	nextRecords := p.transaction.records
	if change.Record != nil {
		nextRecords++
	}
	nextBytes := p.transaction.bytes + int64(len(encoded))
	if nextRecords > p.maxTransactionRecords || nextBytes > p.maxTransactionBytes {
		return fmt.Errorf(
			"postgres transaction xid=%d exceeds managed buffer limit: records=%d/%d bytes=%d/%d",
			p.transaction.xid,
			nextRecords,
			p.maxTransactionRecords,
			nextBytes,
			p.maxTransactionBytes,
		)
	}
	p.transaction.records = nextRecords
	p.transaction.bytes = nextBytes
	p.transaction.changes = append(p.transaction.changes, change)
	return nil
}

func (p *PostgresStream) emitChange(ctx context.Context, xld pglogrepl.XLogData, schema connector.Schema, record *connector.Record) error {
	payload := make([]byte, len(xld.WALData))
	copy(payload, xld.WALData)
	if record != nil {
		record.Payload = payload
	}

	change := Change{
		LSN:       xld.WALStart,
		Schema:    schema.Namespace,
		Table:     schema.Name,
		Operation: string(record.Operation),
		Payload:   payload,
		DDL:       "",
		Record:    record,
		SchemaDef: &schema,
	}

	return p.enqueueChange(ctx, change)
}

func (p *PostgresStream) emitSchemaChange(ctx context.Context, xld pglogrepl.XLogData, schema connector.Schema, plan internalschema.Plan) error {
	if !plan.HasChanges() {
		return nil
	}

	planBytes, err := json.Marshal(plan)
	if err != nil {
		return fmt.Errorf("marshal schema plan: %w", err)
	}

	record := &connector.Record{
		Table:          schema.Name,
		Operation:      connector.OpDDL,
		SchemaVersion:  schema.Version,
		DDLPlan:        planBytes,
		Timestamp:      xld.ServerTime,
		SourcePosition: xld.WALStart.String(),
	}
	change := Change{
		LSN:       xld.WALStart,
		Schema:    schema.Namespace,
		Table:     schema.Name,
		Operation: string(record.Operation),
		Payload:   planBytes,
		DDL:       "",
		Record:    record,
		SchemaDef: &schema,
	}

	return p.enqueueChange(ctx, change)
}

func (p *PostgresStream) emitLogicalMessage(ctx context.Context, xld pglogrepl.XLogData, ddl string) error {
	return p.enqueueChange(ctx, logicalMessageChange(xld, ddl))
}

func logicalMessageChange(xld pglogrepl.XLogData, ddl string) Change {
	payload := make([]byte, len(xld.WALData))
	copy(payload, xld.WALData)
	return Change{
		LSN:       xld.WALStart,
		Operation: "message",
		Payload:   payload,
		DDL:       ddl,
		Record: &connector.Record{
			Operation:      connector.OpDDL,
			DDL:            ddl,
			Timestamp:      xld.ServerTime,
			Payload:        payload,
			SourcePosition: xld.WALStart.String(),
		},
	}
}

func (p *PostgresStream) sendChange(ctx context.Context, change Change) error {
	p.mu.Lock()
	ch := p.changes
	p.mu.Unlock()

	if ch == nil {
		return errors.New("change channel not initialized")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- change:
		return nil
	}
}

func (p *PostgresStream) setErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastErr = err
}

func isSlotExistsErr(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "42710"
	}
	return strings.Contains(strings.ToLower(err.Error()), "already exists")
}

func (p *PostgresStream) schemaForRelation(ctx context.Context, rel *pglogrepl.RelationMessage) connector.Schema {
	p.versions[rel.RelationID]++
	version := p.versions[rel.RelationID]

	var infoResolver TypeInfoResolver
	if resolver, ok := p.typeResolver.(TypeInfoResolver); ok {
		infoResolver = resolver
	}

	var columnIdentityResolver RelationColumnIdentityResolver
	if resolver, ok := p.typeResolver.(RelationColumnIdentityResolver); ok {
		columnIdentityResolver = resolver
	}

	columns := make([]connector.Column, 0, len(rel.Columns))
	for index, col := range rel.Columns {
		colType := p.resolveTypeName(ctx, col.DataType)
		column := connector.Column{
			Name:     col.Name,
			Type:     colType,
			Nullable: true,
			TypeMetadata: map[string]string{
				"oid":                fmt.Sprintf("%d", col.DataType),
				"source_relation_id": fmt.Sprintf("%d", rel.RelationID),
				"source_column_id":   fmt.Sprintf("%d", index+1),
				"nullability_known":  "false",
				"generated_known":    "false",
			},
		}
		if col.Flags == 1 {
			column.TypeMetadata["replica_identity"] = "true"
		}
		if columnIdentityResolver != nil {
			if identity, ok, err := columnIdentityResolver.ResolveColumnIdentity(ctx, rel.RelationID, col.Name); err == nil && ok {
				column.TypeMetadata["source_column_id"] = fmt.Sprintf("%d", identity)
			}
		}
		if infoResolver != nil {
			if info, ok, err := infoResolver.ResolveTypeInfo(ctx, col.DataType); err == nil && ok {
				column.TypeMetadata["oid"] = fmt.Sprintf("%d", info.OID)
				if info.Schema != "" {
					column.TypeMetadata["type_schema"] = info.Schema
				}
				if info.Extension != "" {
					column.TypeMetadata["extension"] = info.Extension
				}
			}
		}
		columns = append(columns, column)
	}

	return connector.Schema{
		Name:              rel.RelationName,
		Namespace:         rel.Namespace,
		Version:           version,
		Columns:           columns,
		QuotedIdentifiers: quotedIdentifiersForSchema(rel.Namespace, rel.RelationName, columns),
	}
}

func relationSchemaKey(namespace, name string) string {
	return strings.ToLower(strings.TrimSpace(namespace)) + "\x00" + strings.ToLower(strings.TrimSpace(name))
}

func (p *PostgresStream) schemaForRelationID(relationID uint32) connector.Schema {
	if p.transaction != nil {
		if schema, ok := p.transaction.schemaOverrides[relationID]; ok {
			return schema
		}
	}
	if schema, ok := p.schemas[relationID]; ok {
		return schema
	}
	if rel, ok := p.relations[relationID]; ok {
		return p.schemaForRelation(context.Background(), rel)
	}
	return connector.Schema{}
}

func (p *PostgresStream) decodeInsert(msg *pglogrepl.InsertMessage, xld pglogrepl.XLogData) (*connector.Record, connector.Schema, error) {
	rel, schema, err := p.loadRelation(msg.RelationID)
	if err != nil {
		return nil, connector.Schema{}, err
	}

	values, unchanged, err := p.decodeTuple(rel, msg.Tuple)
	if err != nil {
		return nil, connector.Schema{}, err
	}
	if err := connector.NormalizePostgresRecord(schema, values); err != nil {
		return nil, connector.Schema{}, err
	}

	key, err := encodeKey(p.keyColumns(rel, values))
	if err != nil {
		return nil, connector.Schema{}, err
	}

	record := &connector.Record{
		Table:         rel.RelationName,
		Operation:     connector.OpInsert,
		SchemaVersion: schema.Version,
		Key:           key,
		After:         values,
		Unchanged:     unchanged,
		Timestamp:     xld.ServerTime,
	}

	return record, schema, nil
}

func (p *PostgresStream) decodeUpdate(msg *pglogrepl.UpdateMessage, xld pglogrepl.XLogData) (*connector.Record, connector.Schema, error) {
	rel, schema, err := p.loadRelation(msg.RelationID)
	if err != nil {
		return nil, connector.Schema{}, err
	}

	var before map[string]any
	var beforeUnchanged []string
	if msg.OldTuple != nil {
		decoded, unchanged, err := p.decodeTuple(rel, msg.OldTuple)
		if err != nil {
			return nil, connector.Schema{}, err
		}
		if err := connector.NormalizePostgresRecord(schema, decoded); err != nil {
			return nil, connector.Schema{}, err
		}
		before = decoded
		beforeUnchanged = unchanged
	}

	after, afterUnchanged, err := p.decodeTuple(rel, msg.NewTuple)
	if err != nil {
		return nil, connector.Schema{}, err
	}
	if err := connector.NormalizePostgresRecord(schema, after); err != nil {
		return nil, connector.Schema{}, err
	}

	var keyFields map[string]any
	if msg.OldTupleType == pglogrepl.UpdateMessageTupleTypeKey && before != nil {
		keyFields = p.keyColumns(rel, before)
	} else {
		keyFields = p.keyColumns(rel, after)
	}

	key, err := encodeKey(keyFields)
	if err != nil {
		return nil, connector.Schema{}, err
	}

	beforeUnchanged = append(beforeUnchanged, afterUnchanged...)
	unchanged := beforeUnchanged
	record := &connector.Record{
		Table:         rel.RelationName,
		Operation:     connector.OpUpdate,
		SchemaVersion: schema.Version,
		Key:           key,
		Before:        before,
		After:         after,
		Unchanged:     unchanged,
		Timestamp:     xld.ServerTime,
	}

	return record, schema, nil
}

func (p *PostgresStream) decodeDelete(msg *pglogrepl.DeleteMessage, xld pglogrepl.XLogData) (*connector.Record, connector.Schema, error) {
	rel, schema, err := p.loadRelation(msg.RelationID)
	if err != nil {
		return nil, connector.Schema{}, err
	}

	before, unchanged, err := p.decodeTuple(rel, msg.OldTuple)
	if err != nil {
		return nil, connector.Schema{}, err
	}
	if err := connector.NormalizePostgresRecord(schema, before); err != nil {
		return nil, connector.Schema{}, err
	}

	keyFields := p.keyColumns(rel, before)
	key, err := encodeKey(keyFields)
	if err != nil {
		return nil, connector.Schema{}, err
	}

	record := &connector.Record{
		Table:         rel.RelationName,
		Operation:     connector.OpDelete,
		SchemaVersion: schema.Version,
		Key:           key,
		Before:        before,
		Unchanged:     unchanged,
		Timestamp:     xld.ServerTime,
	}

	return record, schema, nil
}

func (p *PostgresStream) loadRelation(relationID uint32) (*pglogrepl.RelationMessage, connector.Schema, error) {
	if p.transaction != nil {
		if rel, ok := p.transaction.relationOverrides[relationID]; ok {
			return rel, p.transaction.schemaOverrides[relationID], nil
		}
	}
	rel, ok := p.relations[relationID]
	if !ok {
		return nil, connector.Schema{}, fmt.Errorf("unknown relation id %d", relationID)
	}
	return rel, p.schemaForRelationID(relationID), nil
}

func (p *PostgresStream) decodeTuple(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) (map[string]any, []string, error) {
	if tuple == nil {
		return nil, nil, nil
	}

	values := make(map[string]any, len(tuple.Columns))
	unchanged := make([]string, 0)

	for idx, col := range tuple.Columns {
		if idx >= len(rel.Columns) {
			return nil, nil, fmt.Errorf("tuple column index %d out of range", idx)
		}
		colMeta := rel.Columns[idx]
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			values[colMeta.Name] = nil
		case pglogrepl.TupleDataTypeToast:
			unchanged = append(unchanged, colMeta.Name)
		case pglogrepl.TupleDataTypeText, pglogrepl.TupleDataTypeBinary:
			format := int16(pgtype.TextFormatCode)
			if col.DataType == pglogrepl.TupleDataTypeBinary {
				format = pgtype.BinaryFormatCode
			}
			if typ, ok := p.typeMap.TypeForOID(colMeta.DataType); ok {
				decoded, err := typ.Codec.DecodeValue(p.typeMap, colMeta.DataType, format, col.Data)
				if err != nil {
					return nil, nil, fmt.Errorf("decode column %s: %w", colMeta.Name, err)
				}
				values[colMeta.Name] = decoded
			} else {
				if col.DataType == pglogrepl.TupleDataTypeBinary {
					raw := make([]byte, len(col.Data))
					copy(raw, col.Data)
					values[colMeta.Name] = raw
				} else {
					values[colMeta.Name] = string(col.Data)
				}
			}
		default:
			return nil, nil, fmt.Errorf("unknown column data type %c", col.DataType)
		}
	}

	return values, unchanged, nil
}

func (p *PostgresStream) keyColumns(rel *pglogrepl.RelationMessage, values map[string]any) map[string]any {
	if values == nil {
		return map[string]any{}
	}
	keys := make(map[string]any)
	for _, col := range rel.Columns {
		if col.Flags&1 == 1 {
			keys[col.Name] = values[col.Name]
		}
	}
	if len(keys) == 0 {
		return values
	}
	return keys
}

func encodeKey(values map[string]any) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	buf.WriteByte('{')
	for idx, key := range keys {
		if idx > 0 {
			buf.WriteByte(',')
		}
		name, err := json.Marshal(key)
		if err != nil {
			return nil, err
		}
		value, err := json.Marshal(values[key])
		if err != nil {
			return nil, err
		}
		buf.Write(name)
		buf.WriteByte(':')
		buf.Write(value)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func logicalReceivedPosition(xld pglogrepl.XLogData) pglogrepl.LSN {
	// A pgoutput payload length is not a WAL byte range. CommitMessage carries
	// the transaction-end position separately.
	return xld.WALStart
}

func (p *PostgresStream) setReceivedLSN(lsn pglogrepl.LSN) {
	p.mu.Lock()
	if lsn > p.recvLSN {
		p.recvLSN = lsn
	}
	p.mu.Unlock()
}

func quotedIdentifiersForSchema(namespace, table string, columns []connector.Column) map[string]bool {
	quoted := make(map[string]bool, len(columns)+2)
	if namespace != "" {
		quoted[namespace] = true
	}
	if table != "" {
		quoted[table] = true
	}
	for _, col := range columns {
		if col.Name == "" {
			continue
		}
		quoted[col.Name] = true
	}
	return quoted
}

func (p *PostgresStream) ackPosition() pglogrepl.LSN {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ackLSN
}

func (p *PostgresStream) resolveTypeName(ctx context.Context, oid uint32) string {
	if oid == 0 {
		return ""
	}
	if p.transaction != nil {
		if name, ok := p.transaction.typeOverrides[oid]; ok {
			return name
		}
	}
	p.typeMu.Lock()
	if name, ok := p.typeNames[oid]; ok {
		p.typeMu.Unlock()
		return name
	}
	p.typeMu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}

	if p.typeResolver != nil {
		if name, ok, err := p.typeResolver.ResolveType(ctx, oid); err == nil && ok {
			p.typeMu.Lock()
			p.typeNames[oid] = name
			p.typeMu.Unlock()
			return name
		}
	}

	colType := fmt.Sprintf("oid:%d", oid)
	if typ, ok := p.typeMap.TypeForOID(oid); ok {
		colType = typ.Name
	}

	// When a catalog resolver is configured, an unresolved OID may belong to a
	// type created by the still-uncommitted streamed transaction. Do not poison
	// the process-wide cache; a pgoutput Type message or a post-commit retry can
	// still provide the authoritative name.
	if p.typeResolver == nil || !strings.HasPrefix(colType, "oid:") {
		p.typeMu.Lock()
		p.typeNames[oid] = colType
		p.typeMu.Unlock()
	}
	return colType
}
