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
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// PostgresStream implements logical replication using pgoutput.
type PostgresStream struct {
	dsn            string
	outputPlugin   string
	statusInterval time.Duration
	startLSN       pglogrepl.LSN
	pluginArgs     []string
	createSlot     bool
	typeMap        *pgtype.Map
	schemaHook     SchemaHook
	typeResolver   TypeResolver
	typeMu         sync.Mutex
	typeNames      map[uint32]string

	mu        sync.Mutex
	conn      *pgconn.PgConn
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	changes   chan Change
	lastErr   error
	ackLSN    pglogrepl.LSN
	recvLSN   pglogrepl.LSN
	relations map[uint32]*pglogrepl.RelationMessage
	schemas   map[uint32]connector.Schema
	versions  map[uint32]int64

	ddlMessagePrefix string
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

func WithCreateSlot(enabled bool) PostgresStreamOption {
	return func(s *PostgresStream) {
		s.createSlot = enabled
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

// SchemaHook receives schema evolution and DDL events.
type SchemaHook interface {
	OnSchema(ctx context.Context, schema connector.Schema) error
	OnSchemaChange(ctx context.Context, plan internalschema.Plan) error
	OnDDL(ctx context.Context, ddl string, lsn pglogrepl.LSN) error
}

// TypeResolver resolves Postgres type OIDs to names.
type TypeResolver interface {
	ResolveType(ctx context.Context, oid uint32) (string, bool, error)
}

// NewPostgresStream returns a Postgres logical replication stream.
func NewPostgresStream(dsn string, opts ...PostgresStreamOption) *PostgresStream {
	stream := &PostgresStream{
		dsn:              dsn,
		outputPlugin:     "pgoutput",
		statusInterval:   10 * time.Second,
		createSlot:       true,
		relations:        make(map[uint32]*pglogrepl.RelationMessage),
		schemas:          make(map[uint32]connector.Schema),
		versions:         make(map[uint32]int64),
		typeNames:        make(map[uint32]string),
		ddlMessagePrefix: "wallaby_ddl",
	}

	for _, opt := range opts {
		opt(stream)
	}

	if stream.typeMap == nil {
		stream.typeMap = pgtype.NewMap()
	}

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

	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect replication: %w", err)
	}

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("identify system: %w", err)
	}

	startLSN := p.startLSN
	if startLSN == 0 {
		startLSN = sysident.XLogPos
	}

	if p.createSlot {
		_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slot, p.outputPlugin, pglogrepl.CreateReplicationSlotOptions{})
		if err != nil && !isSlotExistsErr(err) {
			conn.Close(ctx)
			return nil, fmt.Errorf("create replication slot: %w", err)
		}
	}

	pluginArgs := p.pluginArgs
	if len(pluginArgs) == 0 {
		pluginArgs = []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", publication),
		}
	}

	if err := pglogrepl.StartReplication(ctx, conn, slot, startLSN, pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("start replication: %w", err)
	}

	streamCtx, cancel := context.WithCancel(ctx)
	changes := make(chan Change, 256)

	p.mu.Lock()
	p.conn = conn
	p.cancel = cancel
	p.changes = changes
	p.mu.Unlock()

	p.wg.Add(1)
	go p.consume(streamCtx, startLSN)

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

// LastReceivedLSN returns the most recent LSN observed from WAL data.
func (p *PostgresStream) LastReceivedLSN() pglogrepl.LSN {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.recvLSN
}

func (p *PostgresStream) consume(ctx context.Context, startLSN pglogrepl.LSN) {
	defer p.wg.Done()
	defer func() {
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

	clientXLogPos := startLSN
	nextStandbyMessageDeadline := time.Now().Add(p.statusInterval)

	for {
		if ctx.Err() != nil {
			return
		}

		if time.Now().After(nextStandbyMessageDeadline) {
			clientXLogPos = p.ackPosition()
			err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: clientXLogPos,
				WALFlushPosition: clientXLogPos,
				WALApplyPosition: clientXLogPos,
			})
			if err != nil {
				p.setErr(fmt.Errorf("send standby status: %w", err))
				return
			}
			nextStandbyMessageDeadline = time.Now().Add(p.statusInterval)
		}

		deadlineCtx, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
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

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				p.setErr(fmt.Errorf("parse keepalive: %w", err))
				return
			}
			if pkm.ServerWALEnd > clientXLogPos {
				clientXLogPos = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				p.setErr(fmt.Errorf("parse xlogdata: %w", err))
				return
			}

			if err := p.handleWal(ctx, xld); err != nil {
				p.setErr(err)
				return
			}

			p.setReceivedLSN(xld.WALStart + pglogrepl.LSN(len(xld.WALData)))
		}
	}
}

func (p *PostgresStream) handleWal(ctx context.Context, xld pglogrepl.XLogData) error {
	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return fmt.Errorf("parse logical message: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		p.relations[msg.RelationID] = msg
		prevSchema, hasPrev := p.schemas[msg.RelationID]
		schemaDef := p.schemaForRelation(ctx, msg)
		p.schemas[msg.RelationID] = schemaDef
		if p.schemaHook != nil {
			if err := p.schemaHook.OnSchema(ctx, schemaDef); err != nil {
				return fmt.Errorf("schema hook: %w", err)
			}
			if hasPrev {
				plan := internalschema.Diff(prevSchema, schemaDef)
				if plan.HasChanges() {
					if err := p.schemaHook.OnSchemaChange(ctx, plan); err != nil {
						return fmt.Errorf("schema change hook: %w", err)
					}
				}
			}
		}
		return nil
	case *pglogrepl.InsertMessage:
		record, schema, err := p.decodeInsert(msg, xld)
		if err != nil {
			return err
		}
		return p.emitChange(ctx, xld, schema, record, "")
	case *pglogrepl.UpdateMessage:
		record, schema, err := p.decodeUpdate(msg, xld)
		if err != nil {
			return err
		}
		return p.emitChange(ctx, xld, schema, record, "")
	case *pglogrepl.DeleteMessage:
		record, schema, err := p.decodeDelete(msg, xld)
		if err != nil {
			return err
		}
		return p.emitChange(ctx, xld, schema, record, "")
	case *pglogrepl.TruncateMessage:
		if len(msg.RelationIDs) == 0 {
			return nil
		}
		for _, relID := range msg.RelationIDs {
			schema := p.schemaForRelationID(relID)
			record := connector.Record{
				Table:         schema.Name,
				Operation:     connector.OpDDL,
				SchemaVersion: schema.Version,
				Timestamp:     xld.ServerTime,
			}
			if err := p.emitChange(ctx, xld, schema, &record, ""); err != nil {
				return err
			}
		}
		return nil
	case *pglogrepl.LogicalDecodingMessage:
		if p.ddlMessagePrefix != "" && msg.Prefix != p.ddlMessagePrefix {
			return nil
		}
		if p.schemaHook != nil {
			if err := p.schemaHook.OnDDL(ctx, string(msg.Content), xld.WALStart); err != nil {
				return fmt.Errorf("ddl hook: %w", err)
			}
		}
		return p.emitLogicalMessage(ctx, xld, string(msg.Content))
	default:
		return nil
	}
}

func (p *PostgresStream) emitChange(ctx context.Context, xld pglogrepl.XLogData, schema connector.Schema, record *connector.Record, ddl string) error {
	payload := make([]byte, len(xld.WALData))
	copy(payload, xld.WALData)

	change := Change{
		LSN:       xld.WALStart,
		Schema:    schema.Namespace,
		Table:     schema.Name,
		Operation: string(record.Operation),
		Payload:   payload,
		DDL:       ddl,
		Record:    record,
		SchemaDef: &schema,
	}

	return p.sendChange(ctx, change)
}

func (p *PostgresStream) emitLogicalMessage(ctx context.Context, xld pglogrepl.XLogData, ddl string) error {
	payload := make([]byte, len(xld.WALData))
	copy(payload, xld.WALData)

	record := &connector.Record{
		Operation: connector.OpDDL,
		DDL:       ddl,
		Timestamp: xld.ServerTime,
	}

	change := Change{
		LSN:       xld.WALStart,
		Operation: "message",
		Payload:   payload,
		DDL:       ddl,
		Record:    record,
	}

	return p.sendChange(ctx, change)
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

	columns := make([]connector.Column, 0, len(rel.Columns))
	for _, col := range rel.Columns {
		colType := p.resolveTypeName(ctx, col.DataType)
		columns = append(columns, connector.Column{
			Name:     col.Name,
			Type:     colType,
			Nullable: true,
		})
	}

	return connector.Schema{
		Name:              rel.RelationName,
		Namespace:         rel.Namespace,
		Version:           version,
		Columns:           columns,
		QuotedIdentifiers: quotedIdentifiersForSchema(rel.Namespace, rel.RelationName, columns),
	}
}

func (p *PostgresStream) schemaForRelationID(relationID uint32) connector.Schema {
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
		before = decoded
		beforeUnchanged = unchanged
	}

	after, afterUnchanged, err := p.decodeTuple(rel, msg.NewTuple)
	if err != nil {
		return nil, connector.Schema{}, err
	}

	keyFields := map[string]any{}
	if msg.OldTupleType == pglogrepl.UpdateMessageTupleTypeKey && before != nil {
		keyFields = before
	} else {
		keyFields = p.keyColumns(rel, after)
	}

	key, err := encodeKey(keyFields)
	if err != nil {
		return nil, connector.Schema{}, err
	}

	unchanged := append(beforeUnchanged, afterUnchanged...)
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

	key, err := encodeKey(before)
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
	rel, ok := p.relations[relationID]
	if !ok {
		return nil, connector.Schema{}, fmt.Errorf("unknown relation id %d", relationID)
	}
	schema := p.schemaForRelationID(relationID)
	return rel, schema, nil
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
				values[colMeta.Name] = string(col.Data)
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
	if p.ackLSN > 0 {
		return p.ackLSN
	}
	return p.recvLSN
}

func (p *PostgresStream) resolveTypeName(ctx context.Context, oid uint32) string {
	if oid == 0 {
		return ""
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

	p.typeMu.Lock()
	p.typeNames[oid] = colType
	p.typeMu.Unlock()
	return colType
}
