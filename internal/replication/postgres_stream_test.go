package replication

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestDecodeDelete_UsesReplicaIdentityKey(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		RelationID:   1,
		Namespace:    "public",
		RelationName: "events",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 20, Flags: 1},        // int8, key column
			{Name: "payload", DataType: 3802, Flags: 0}, // jsonb, non-key
		},
	}

	stream := NewPostgresStream("")
	stream.relations[rel.RelationID] = rel

	tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte("42")},
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte(`{"foo":"bar"}`)},
	}}

	record, _, err := stream.decodeDelete(&pglogrepl.DeleteMessage{
		RelationID: rel.RelationID,
		OldTuple:   tuple,
	}, pglogrepl.XLogData{ServerTime: time.Now()})
	if err != nil {
		t.Fatalf("decode delete: %v", err)
	}

	key := decodeKeyMap(t, record.Key)
	if len(key) != 1 {
		t.Fatalf("expected 1 key column, got %d: %v", len(key), key)
	}
	if got, ok := key["id"]; !ok || got.(float64) != 42 {
		t.Fatalf("expected id=42 in key, got %v", key)
	}
	if _, ok := key["payload"]; ok {
		t.Fatalf("did not expect payload in key: %v", key)
	}
}

func TestDecodeUpdate_UsesReplicaIdentityKey(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		RelationID:   2,
		Namespace:    "public",
		RelationName: "events",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 20, Flags: 1},        // int8, key column
			{Name: "payload", DataType: 3802, Flags: 0}, // jsonb, non-key
		},
	}

	stream := NewPostgresStream("")
	stream.relations[rel.RelationID] = rel

	oldTuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte("7")},
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte(`{"before":true}`)},
	}}
	newTuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte("7")},
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte(`{"after":true}`)},
	}}

	record, _, err := stream.decodeUpdate(&pglogrepl.UpdateMessage{
		RelationID:   rel.RelationID,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeKey,
		OldTuple:     oldTuple,
		NewTuple:     newTuple,
	}, pglogrepl.XLogData{ServerTime: time.Now()})
	if err != nil {
		t.Fatalf("decode update: %v", err)
	}

	key := decodeKeyMap(t, record.Key)
	if len(key) != 1 {
		t.Fatalf("expected 1 key column, got %d: %v", len(key), key)
	}
	if got, ok := key["id"]; !ok || got.(float64) != 7 {
		t.Fatalf("expected id=7 in key, got %v", key)
	}
	if _, ok := key["payload"]; ok {
		t.Fatalf("did not expect payload in key: %v", key)
	}
}

func TestPostgresStreamCanceledQueuedFeedbackCannotAdvanceFlush(t *testing.T) {
	stream := NewPostgresStream("")
	stream.conn = &pgconn.PgConn{}
	stream.cancel = func() {}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := stream.AckWithEvidence(ctx, pglogrepl.LSN(0x40))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("AckWithEvidence() error=%v, want context cancellation", err)
	}
	if got := stream.ackPosition(); got != 0 {
		t.Fatalf("ackPosition()=%s after canceled queued feedback, want zero", got)
	}
	if len(stream.feedbackWaiters) != 0 {
		t.Fatalf("canceled feedback waiters=%d, want zero", len(stream.feedbackWaiters))
	}
}

func TestPostgresStreamClaimedFeedbackWaitsForDefinitiveSendAfterCancellation(t *testing.T) {
	stream := NewPostgresStream("")
	stream.conn = &pgconn.PgConn{}
	stream.cancel = func() {}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- stream.AckWithEvidence(ctx, pglogrepl.LSN(0x40))
	}()

	deadline := time.Now().Add(time.Second)
	for {
		stream.mu.Lock()
		queued := len(stream.feedbackWaiters)
		stream.mu.Unlock()
		if queued == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("feedback request was not queued")
		}
		runtime.Gosched()
	}
	waiters, err := stream.claimFeedbackWaiters(0)
	if err != nil || len(waiters) != 1 {
		t.Fatalf("claim feedback waiters=(%d,%v), want 1/nil", len(waiters), err)
	}
	cancel()
	select {
	case err := <-result:
		t.Fatalf("claimed feedback returned before send result: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	waiters[0].result <- nil
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("claimed feedback result=%v, want send success", err)
		}
	case <-time.After(time.Second):
		t.Fatal("claimed feedback did not return after send result")
	}
}

func TestPostgresStreamDoesNotFeedbackBeforeDurableAck(t *testing.T) {
	stream := NewPostgresStream("")
	stream.setReceivedLSN(pglogrepl.LSN(0x80))

	if got := stream.ackPosition(); got != 0 {
		t.Fatalf("ackPosition() = %s, want 0 before durable Ack", got)
	}

	stream.Ack(pglogrepl.LSN(0x40))
	stream.setReceivedLSN(pglogrepl.LSN(0x90))
	if got := stream.ackPosition(); got != pglogrepl.LSN(0x40) {
		t.Fatalf("ackPosition() = %s, want durable Ack 0/40", got)
	}
}

func TestLogicalReceivedPositionDoesNotUsePayloadLength(t *testing.T) {
	xld := pglogrepl.XLogData{WALStart: pglogrepl.LSN(0x100), WALData: make([]byte, 4096)}
	if got := logicalReceivedPosition(xld); got != xld.WALStart {
		t.Fatalf("logical received position=%s, want WALStart %s", got, xld.WALStart)
	}
}

func TestManagedPostgresStreamRequiresFeedbackBeforeReadingPastCommit(t *testing.T) {
	stream := NewPostgresStream("", WithRequireAuthorizedStart(true))
	stream.changes = make(chan Change, 1)
	stream.transaction = &pendingTransaction{xid: 42, beginLSN: 0x10, changes: []Change{{Record: &connector.Record{Operation: connector.OpInsert}}}}
	if err := stream.commitTransaction(context.Background(), &pglogrepl.CommitMessage{CommitLSN: 0x30, TransactionEndLSN: 0x38}); err != nil {
		t.Fatal(err)
	}
	if got := stream.feedbackBarrierPosition(); got != 0x38 {
		t.Fatalf("feedback barrier=%s, want transaction end 0/38", got)
	}
}

func TestPostgresStreamUsesTransactionEndLSN(t *testing.T) {
	stream := NewPostgresStream("")
	stream.changes = make(chan Change, 2)

	beginLSN := pglogrepl.LSN(0x10)
	commitLSN := pglogrepl.LSN(0x30)
	transactionEndLSN := pglogrepl.LSN(0x38)
	xid := uint32(42)
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{
		WALStart: beginLSN,
		WALData:  beginMessage(beginLSN, xid),
	}); err != nil {
		t.Fatalf("begin transaction: %v", err)
	}

	schema := connector.Schema{Namespace: "public", Name: "events", Version: 1}
	record := &connector.Record{Table: "events", Operation: connector.OpInsert, SchemaVersion: 1}
	observationTime := time.Now()
	if err := stream.emitChange(context.Background(), pglogrepl.XLogData{
		WALStart:   pglogrepl.LSN(0x20),
		WALData:    []byte{'I'},
		ServerTime: observationTime,
	}, schema, record); err != nil {
		t.Fatalf("buffer change: %v", err)
	}
	select {
	case change := <-stream.changes:
		t.Fatalf("change emitted before commit: %+v", change)
	default:
	}

	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{
		WALStart: commitLSN,
		WALData:  commitMessage(commitLSN, transactionEndLSN),
	}); err != nil {
		t.Fatalf("commit transaction: %v", err)
	}

	change := <-stream.changes
	if change.LSN != transactionEndLSN || change.TransactionEndLSN != transactionEndLSN {
		t.Fatalf("change LSNs = (%s, %s), want transaction end %s", change.LSN, change.TransactionEndLSN, transactionEndLSN)
	}
	if change.TransactionID != xid || change.TransactionOrdinal != 0 || !change.TransactionFinal {
		t.Fatalf("transaction metadata = xid:%d ordinal:%d final:%t, want 42/0/true", change.TransactionID, change.TransactionOrdinal, change.TransactionFinal)
	}
	if change.Record == nil || change.Record.Timestamp.Equal(observationTime) || change.Record.Timestamp.UTC().Year() != 2000 {
		t.Fatalf("record timestamp=%v, want replay-stable PostgreSQL commit time", change.Record)
	}
}

func TestLogicalMessagesOptionLeavesPluginValueConstructionToStart(t *testing.T) {
	stream := NewPostgresStream("", WithLogicalMessages(true))
	if !stream.logicalMessages {
		t.Fatal("logical messages option was not retained")
	}
	if len(stream.pluginArgs) != 0 {
		t.Fatalf("logical messages option installed raw plugin arguments: %v", stream.pluginArgs)
	}
}

func TestPostgresStreamV2PreservesStreamedTransactionFragmentsAndBarrier(t *testing.T) {
	stream := NewPostgresStream("", WithStreamingTransactions(true))
	stream.changes = make(chan Change, 3)

	const xid = uint32(77)
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0x10, WALData: streamStartMessage(xid, true)}); err != nil {
		t.Fatalf("start first stream segment: %v", err)
	}
	widgets := connector.Schema{Namespace: "public", Name: "widgets", Version: 1}
	if err := stream.emitChange(context.Background(), pglogrepl.XLogData{WALStart: 0x18, WALData: []byte{'I'}}, widgets, &connector.Record{Table: "widgets", Operation: connector.OpInsert}); err != nil {
		t.Fatal(err)
	}
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0x20, WALData: streamStopMessage()}); err != nil {
		t.Fatalf("stop first stream segment: %v", err)
	}
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0x28, WALData: streamStartMessage(xid, false)}); err != nil {
		t.Fatalf("start continuation stream segment: %v", err)
	}
	if err := stream.emitLogicalMessage(context.Background(), pglogrepl.XLogData{WALStart: 0x30, WALData: []byte{'M'}}, "ALTER TABLE public.widgets ADD COLUMN note text"); err != nil {
		t.Fatal(err)
	}
	audit := connector.Schema{Namespace: "audit", Name: "events", Version: 1}
	if err := stream.emitChange(context.Background(), pglogrepl.XLogData{WALStart: 0x38, WALData: []byte{'I'}}, audit, &connector.Record{Table: "events", Operation: connector.OpInsert}); err != nil {
		t.Fatal(err)
	}
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0x40, WALData: streamStopMessage()}); err != nil {
		t.Fatalf("stop continuation stream segment: %v", err)
	}
	select {
	case change := <-stream.changes:
		t.Fatalf("streamed change emitted before commit: %+v", change)
	default:
	}
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0x48, WALData: streamCommitMessage(xid, 0x48, 0x50)}); err != nil {
		t.Fatalf("commit streamed transaction: %v", err)
	}

	for index, wantOperation := range []connector.Operation{connector.OpInsert, connector.OpDDL, connector.OpInsert} {
		change := <-stream.changes
		if change.TransactionID != xid || change.TransactionOrdinal != uint64(index) || change.LSN != 0x50 {
			t.Fatalf("change %d transaction metadata=%+v", index, change)
		}
		if change.Record == nil || change.Record.Operation != wantOperation {
			t.Fatalf("change %d operation=%v, want %s", index, change.Record, wantOperation)
		}
		if change.TransactionFinal != (index == 2) {
			t.Fatalf("change %d final=%t", index, change.TransactionFinal)
		}
	}
	if len(stream.streamTransactions) != 0 || stream.inStream || stream.transaction != nil {
		t.Fatalf("streamed transaction state leaked after commit: %+v", stream.streamTransactions)
	}
}

func TestPostgresStreamV2SubtransactionAbortPreservesParent(t *testing.T) {
	stream := NewPostgresStream("", WithStreamingTransactions(true))
	stream.changes = make(chan Change, 3)

	const (
		xid    = uint32(88)
		subXID = uint32(89)
	)
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0x10, WALData: streamStartMessage(xid, true)}); err != nil {
		t.Fatal(err)
	}
	stream.streamMessageXID = xid
	parent := connector.Schema{Namespace: "public", Name: "parent", Version: 1}
	if err := stream.emitChange(context.Background(), pglogrepl.XLogData{WALStart: 0x18, WALData: []byte{'I'}}, parent, &connector.Record{Table: "parent", Operation: connector.OpInsert}); err != nil {
		t.Fatal(err)
	}
	stream.streamMessageXID = subXID
	aborted := connector.Schema{Namespace: "public", Name: "aborted", Version: 1}
	if err := stream.emitChange(context.Background(), pglogrepl.XLogData{WALStart: 0x20, WALData: []byte{'I'}}, aborted, &connector.Record{Table: "aborted", Operation: connector.OpInsert}); err != nil {
		t.Fatal(err)
	}
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0x28, WALData: streamStopMessage()}); err != nil {
		t.Fatal(err)
	}
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0x30, WALData: streamAbortMessage(xid, subXID)}); err != nil {
		t.Fatal(err)
	}
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0x38, WALData: streamStartMessage(xid, false)}); err != nil {
		t.Fatal(err)
	}
	stream.streamMessageXID = xid
	if err := stream.emitChange(context.Background(), pglogrepl.XLogData{WALStart: 0x40, WALData: []byte{'I'}}, parent, &connector.Record{Table: "parent", Operation: connector.OpUpdate}); err != nil {
		t.Fatal(err)
	}
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0x48, WALData: streamStopMessage()}); err != nil {
		t.Fatal(err)
	}
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0x50, WALData: streamCommitMessage(xid, 0x50, 0x58)}); err != nil {
		t.Fatalf("commit parent after subtransaction abort: %v", err)
	}

	for index, want := range []connector.Operation{connector.OpInsert, connector.OpUpdate} {
		change := <-stream.changes
		if change.Record == nil || change.Record.Table != "parent" || change.Record.Operation != want {
			t.Fatalf("change %d=%+v, want parent %s", index, change.Record, want)
		}
		if change.TransactionOrdinal != uint64(index) || change.TransactionFinal != (index == 1) {
			t.Fatalf("change %d transaction metadata=%+v", index, change)
		}
	}
	select {
	case extra := <-stream.changes:
		t.Fatalf("aborted subtransaction change leaked: %+v", extra)
	default:
	}
}

func TestPostgresStreamV2UsesTransactionalTypeMessageAtCommit(t *testing.T) {
	stream := NewPostgresStream("", WithStreamingTransactions(true))
	stream.changes = make(chan Change, 1)
	const (
		xid       = uint32(92)
		customOID = uint32(99001)
	)
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0x90, WALData: streamStartMessage(xid, true)}); err != nil {
		t.Fatal(err)
	}
	stream.streamMessageXID = xid
	if err := stream.handleTypeMessage(&pglogrepl.TypeMessage{DataType: customOID, Namespace: "app", Name: "status"}); err != nil {
		t.Fatal(err)
	}
	relation := &pglogrepl.RelationMessage{
		RelationID: 12, Namespace: "public", RelationName: "typed_events",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 20, Flags: 1},
			{Name: "state", DataType: customOID},
		},
	}
	if err := stream.handleRelationMessage(context.Background(), pglogrepl.XLogData{WALStart: 0x98}, relation); err != nil {
		t.Fatal(err)
	}
	schema := stream.schemaForRelationID(relation.RelationID)
	if got := schema.Columns[1].Type; got != "app.status" {
		t.Fatalf("transactional type=%q before commit, want app.status", got)
	}
	if err := stream.emitChange(context.Background(), pglogrepl.XLogData{WALStart: 0xA0, WALData: []byte{'I'}}, schema, &connector.Record{Table: schema.Name, Operation: connector.OpInsert}); err != nil {
		t.Fatal(err)
	}
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0xA8, WALData: streamStopMessage()}); err != nil {
		t.Fatal(err)
	}
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0xB0, WALData: streamCommitMessage(xid, 0xB0, 0xB8)}); err != nil {
		t.Fatal(err)
	}
	change := <-stream.changes
	if change.SchemaDef == nil || change.SchemaDef.Columns[1].Type != "app.status" || change.SchemaDef.Columns[1].TypeMetadata["oid"] != "99001" {
		t.Fatalf("committed transactional type schema=%+v", change.SchemaDef)
	}
	stream.typeMu.Lock()
	cached := stream.typeNames[customOID]
	stream.typeMu.Unlock()
	if cached != "app.status" {
		t.Fatalf("committed type cache=%q, want app.status", cached)
	}
}

func TestPostgresStreamV2AbortedSubtransactionDoesNotPublishSchema(t *testing.T) {
	hook := &recordingSchemaHook{}
	stream := NewPostgresStream("", WithStreamingTransactions(true), WithSchemaHook(hook))
	stream.changes = make(chan Change, 1)
	oldRelation := &pglogrepl.RelationMessage{
		RelationID: 11, Namespace: "public", RelationName: "widgets",
		Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: 20, Flags: 1}},
	}
	oldSchema := connector.Schema{Namespace: "public", Name: "widgets", Version: 1, Columns: []connector.Column{{Name: "id", Type: "bigint"}}}
	stream.relations[11] = oldRelation
	stream.schemas[11] = oldSchema
	stream.versions[11] = 1

	const (
		xid       = uint32(90)
		subXID    = uint32(91)
		customOID = uint32(99002)
	)
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0x60, WALData: streamStartMessage(xid, true)}); err != nil {
		t.Fatal(err)
	}
	stream.streamMessageXID = subXID
	if err := stream.handleTypeMessage(&pglogrepl.TypeMessage{DataType: customOID, Namespace: "app", Name: "aborted_status"}); err != nil {
		t.Fatal(err)
	}
	changedRelation := &pglogrepl.RelationMessage{
		RelationID: 11, Namespace: "public", RelationName: "widgets",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 20, Flags: 1},
			{Name: "note", DataType: customOID},
		},
	}
	if err := stream.handleRelationMessage(context.Background(), pglogrepl.XLogData{WALStart: 0x68}, changedRelation); err != nil {
		t.Fatal(err)
	}
	if err := stream.handleLogicalMessage(context.Background(), pglogrepl.XLogData{WALStart: 0x6C}, &pglogrepl.LogicalDecodingMessage{
		Transactional: true, Prefix: stream.ddlMessagePrefix, Content: []byte("ALTER TABLE public.widgets ADD COLUMN note text"),
	}); err != nil {
		t.Fatal(err)
	}
	if stream.relations[11] != oldRelation || len(stream.schemas[11].Columns) != 1 || hook.schemas != 0 || hook.changes != 0 || hook.ddls != 0 {
		t.Fatalf("uncommitted schema leaked before abort: relation=%+v schema=%+v hook=%+v", stream.relations[11], stream.schemas[11], hook)
	}
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0x70, WALData: streamStopMessage()}); err != nil {
		t.Fatal(err)
	}
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0x78, WALData: streamAbortMessage(xid, subXID)}); err != nil {
		t.Fatal(err)
	}
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{WALStart: 0x80, WALData: streamCommitMessage(xid, 0x80, 0x88)}); err != nil {
		t.Fatal(err)
	}
	if stream.relations[11] != oldRelation || len(stream.schemas[11].Columns) != 1 || hook.schemas != 0 || hook.changes != 0 || hook.ddls != 0 {
		t.Fatalf("aborted schema leaked after parent commit: relation=%+v schema=%+v hook=%+v", stream.relations[11], stream.schemas[11], hook)
	}
	stream.typeMu.Lock()
	_, typeLeaked := stream.typeNames[customOID]
	stream.typeMu.Unlock()
	if typeLeaked {
		t.Fatal("aborted streamed type metadata leaked into the committed cache")
	}
}

type recordingSchemaHook struct {
	schemas int
	changes int
	ddls    int
}

func (h *recordingSchemaHook) OnSchema(context.Context, connector.Schema) error {
	h.schemas++
	return nil
}
func (h *recordingSchemaHook) OnSchemaChange(context.Context, internalschema.Plan) error {
	h.changes++
	return nil
}
func (h *recordingSchemaHook) OnDDL(context.Context, string, pglogrepl.LSN) error {
	h.ddls++
	return nil
}

func TestPostgresStreamTransactionLimitIncludesRelationMetadata(t *testing.T) {
	stream := NewPostgresStream("", WithTransactionLimits(10, 128))
	stream.transaction = &pendingTransaction{xid: 42}
	stream.streamMessageXID = 42
	err := stream.handleRelationMessage(context.Background(), pglogrepl.XLogData{WALStart: 0x40}, &pglogrepl.RelationMessage{
		RelationID: 11, Namespace: "public", RelationName: "wide_table",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "first_very_wide_column", DataType: 25},
			{Name: "second_very_wide_column", DataType: 25},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "relation metadata") {
		t.Fatalf("relation metadata limit error=%v, want bounded rejection", err)
	}
}

type testSchemaBaselineHook struct{ calls int }

func (*testSchemaBaselineHook) OnSchema(context.Context, connector.Schema) error          { return nil }
func (*testSchemaBaselineHook) OnSchemaChange(context.Context, internalschema.Plan) error { return nil }
func (*testSchemaBaselineHook) OnDDL(context.Context, string, pglogrepl.LSN) error        { return nil }
func (h *testSchemaBaselineHook) SchemaBaseline(context.Context, string, string) (connector.Schema, bool, error) {
	h.calls++
	return connector.Schema{Namespace: "public", Name: "widgets", Version: 99, Columns: []connector.Column{{Name: "hostile", Type: "text"}}}, true, nil
}

type durablePlanResolverHook struct {
	resolved internalschema.Plan
	observed internalschema.Plan
	lsn      pglogrepl.LSN
}

func (*durablePlanResolverHook) OnSchema(context.Context, connector.Schema) error { return nil }
func (*durablePlanResolverHook) OnSchemaChange(context.Context, internalschema.Plan) error {
	return nil
}
func (*durablePlanResolverHook) OnDDL(context.Context, string, pglogrepl.LSN) error {
	return nil
}
func (h *durablePlanResolverHook) ResolveSchemaChangeAtLSN(_ context.Context, observed internalschema.Plan, lsn pglogrepl.LSN) (internalschema.Plan, error) {
	h.observed = observed
	h.lsn = lsn
	return h.resolved, nil
}

func TestManagedPostgresStreamNeverFallsBackToGlobalSchemaBaseline(t *testing.T) {
	hook := &testSchemaBaselineHook{}
	stream := NewPostgresStream("", WithSchemaHook(hook), WithSchemaBaselines(nil))
	if err := stream.handleRelationMessage(context.Background(), pglogrepl.XLogData{WALStart: 0x40}, &pglogrepl.RelationMessage{
		RelationID: 11, Namespace: "public", RelationName: "widgets",
		Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: 20, Flags: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	if hook.calls != 0 || stream.schemas[11].Version != 1 {
		t.Fatalf("managed decoder global baseline calls=%d schema=%+v", hook.calls, stream.schemas[11])
	}
}

func TestPostgresStreamRestartDiffsFirstRelationFromFencedAuthorityBaseline(t *testing.T) {
	baseline := connector.Schema{
		Namespace: "public", Name: "widgets", Version: 7,
		Columns: []connector.Column{{Name: "id", Type: "int8", Nullable: true}},
	}
	stream := NewPostgresStream("", WithSchemaBaselines([]connector.Schema{baseline}), WithEmitPlanDDL(true))
	stream.transaction = &pendingTransaction{xid: 42}
	stream.streamMessageXID = 42
	message := &pglogrepl.RelationMessage{
		RelationID: 11, Namespace: "public", RelationName: "widgets",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 20, Flags: 1},
			{Name: "note", DataType: 25},
		},
	}
	if err := stream.handleRelationMessage(context.Background(), pglogrepl.XLogData{WALStart: 0x40}, message); err != nil {
		t.Fatal(err)
	}
	if len(stream.transaction.relationEvents) != 1 || len(stream.transaction.changes) != 1 {
		t.Fatalf("relation events/DDL changes=%d/%d, want 1/1", len(stream.transaction.relationEvents), len(stream.transaction.changes))
	}
	event := stream.transaction.relationEvents[0]
	if event.schema.Version != 8 || len(event.plan.Changes) != 1 || event.plan.Changes[0].Type != internalschema.ChangeAddColumn || event.plan.Changes[0].Column != "note" {
		t.Fatalf("restart relation event=%+v, want version 8 add note", event)
	}
}

func TestPostgresStreamRestartMatchesBaselinesByExactIdentifierBytes(t *testing.T) {
	baselines := []connector.Schema{
		{Namespace: "public", Name: "Events", Version: 7, Columns: []connector.Column{{Name: "id", Type: "int8", Nullable: true}}},
		{Namespace: "public", Name: "events", Version: 8, Columns: []connector.Column{{Name: "id", Type: "int8", Nullable: true}}},
		{Namespace: "public", Name: " events ", Version: 9, Columns: []connector.Column{{Name: "id", Type: "int8", Nullable: true}}},
	}
	stream := NewPostgresStream("", WithSchemaBaselines(baselines), WithEmitPlanDDL(true))
	stream.transaction = &pendingTransaction{xid: 42}
	stream.streamMessageXID = 42
	messages := []*pglogrepl.RelationMessage{
		{RelationID: 11, Namespace: "public", RelationName: "Events", Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: 20, Flags: 1}, {Name: "note", DataType: 25}}},
		{RelationID: 12, Namespace: "public", RelationName: "events", Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: 20, Flags: 1}}},
		{RelationID: 13, Namespace: "public", RelationName: " events ", Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: 20, Flags: 1}}},
	}
	for _, message := range messages {
		if err := stream.handleRelationMessage(context.Background(), pglogrepl.XLogData{WALStart: 0x40}, message); err != nil {
			t.Fatal(err)
		}
	}
	if len(stream.transaction.relationEvents) != 3 || len(stream.transaction.changes) != 1 {
		t.Fatalf("exact identifier relation observations/DDL changes=%d/%d, want three observations and one DDL", len(stream.transaction.relationEvents), len(stream.transaction.changes))
	}
	observedVersions := make(map[string]int64, len(stream.transaction.relationEvents))
	for _, event := range stream.transaction.relationEvents {
		observedVersions[event.schema.Name] = event.schema.Version
		if event.schema.Name == "Events" {
			if event.schema.Namespace != "public" || len(event.plan.Changes) != 1 || event.plan.Changes[0].Column != "note" {
				t.Fatalf("exact identifier restart event=%+v, want public.Events add note", event)
			}
		} else if event.plan.HasChanges() {
			t.Fatalf("unchanged exact relation %q emitted DDL: %+v", event.schema.Name, event.plan)
		}
	}
	if observedVersions["events"] != 8 || observedVersions[" events "] != 9 {
		t.Fatalf("unchanged case/whitespace baseline versions were not retained: %v", observedVersions)
	}
}

func TestPostgresStreamReplaysApprovedDurablePlanWhenObservedDiffIsEmpty(t *testing.T) {
	baseline := connector.Schema{
		Namespace: "public",
		Name:      "events",
		Version:   2,
		Columns: []connector.Column{
			{Name: "id", Type: "int8", Nullable: true},
			{Name: "extra", Type: "text", Nullable: true},
		},
	}
	durablePlan := internalschema.Plan{Changes: []internalschema.Change{{
		Type: internalschema.ChangeAddColumn, Namespace: "public", Table: "events", Column: "extra", ToType: "text",
	}}}
	hook := &durablePlanResolverHook{resolved: durablePlan}
	stream := NewPostgresStream("", WithSchemaBaselines([]connector.Schema{baseline}), WithSchemaHook(hook), WithEmitPlanDDL(true))
	stream.transaction = &pendingTransaction{xid: 42}
	stream.streamMessageXID = 42
	lsn := pglogrepl.LSN(0x80)
	relation := &pglogrepl.RelationMessage{
		RelationID: 77, Namespace: "public", RelationName: "events",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 20, Flags: 1},
			{Name: "extra", DataType: 25},
		},
	}
	if err := stream.handleRelationMessage(context.Background(), pglogrepl.XLogData{WALStart: lsn}, relation); err != nil {
		t.Fatal(err)
	}
	if hook.observed.HasChanges() {
		t.Fatalf("observed diff=%+v, want empty against already-new durable schema", hook.observed)
	}
	if hook.lsn != lsn {
		t.Fatalf("resolved LSN=%s, want %s", hook.lsn, lsn)
	}
	if len(stream.transaction.relationEvents) != 1 || !stream.transaction.relationEvents[0].plan.HasChanges() {
		t.Fatalf("relation events=%+v, want replayed durable plan", stream.transaction.relationEvents)
	}
	if len(stream.transaction.changes) != 1 || stream.transaction.changes[0].Record == nil || stream.transaction.changes[0].Record.Operation != connector.OpDDL {
		t.Fatalf("transaction changes=%+v, want one structured DDL record", stream.transaction.changes)
	}
}

func TestPostgresStreamRestartKeepsVersionWhenFirstRelationMatchesBaseline(t *testing.T) {
	baseline := connector.Schema{
		Namespace: "public", Name: "widgets", Version: 7,
		Columns: []connector.Column{{Name: "id", Type: "int8", Nullable: true}},
	}
	stream := NewPostgresStream("", WithSchemaBaselines([]connector.Schema{baseline}))
	if err := stream.handleRelationMessage(context.Background(), pglogrepl.XLogData{WALStart: 0x40}, &pglogrepl.RelationMessage{
		RelationID: 11, Namespace: "public", RelationName: "widgets",
		Columns: []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: 20, Flags: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	if got := stream.schemas[11].Version; got != baseline.Version {
		t.Fatalf("unchanged restart schema version=%d, want %d", got, baseline.Version)
	}
}

func TestPostgresStreamPreservesDDLPositionAtTransactionEnd(t *testing.T) {
	stream := NewPostgresStream("")
	stream.changes = make(chan Change, 1)

	beginLSN := pglogrepl.LSN(0x10)
	messageLSN := pglogrepl.LSN(0x20)
	commitLSN := pglogrepl.LSN(0x30)
	transactionEndLSN := pglogrepl.LSN(0x38)
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{
		WALStart: beginLSN,
		WALData:  beginMessage(beginLSN, 42),
	}); err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	if err := stream.emitLogicalMessage(context.Background(), pglogrepl.XLogData{
		WALStart: messageLSN,
		WALData:  []byte{'M'},
	}, "ALTER TABLE widgets ADD COLUMN note text"); err != nil {
		t.Fatalf("buffer DDL message: %v", err)
	}
	if err := stream.handleWal(context.Background(), pglogrepl.XLogData{
		WALStart: commitLSN,
		WALData:  commitMessage(commitLSN, transactionEndLSN),
	}); err != nil {
		t.Fatalf("commit transaction: %v", err)
	}

	change := <-stream.changes
	if change.LSN != transactionEndLSN {
		t.Fatalf("change LSN=%s, want transaction end %s", change.LSN, transactionEndLSN)
	}
	if change.Record == nil || change.Record.SourcePosition != messageLSN.String() {
		t.Fatalf("DDL source position=%v, want message position %s", change.Record, messageLSN)
	}
}

func beginMessage(finalLSN pglogrepl.LSN, xid uint32) []byte {
	message := make([]byte, 1+8+8+4)
	message[0] = 'B'
	binary.BigEndian.PutUint64(message[1:], uint64(finalLSN))
	binary.BigEndian.PutUint64(message[9:], 0)
	binary.BigEndian.PutUint32(message[17:], xid)
	return message
}

func streamStartMessage(xid uint32, first bool) []byte {
	message := make([]byte, 1+4+1)
	message[0] = 'S'
	binary.BigEndian.PutUint32(message[1:], xid)
	if first {
		message[5] = 1
	}
	return message
}

func streamStopMessage() []byte {
	return []byte{'E'}
}

func streamAbortMessage(xid, subXID uint32) []byte {
	message := make([]byte, 1+4+4)
	message[0] = 'A'
	binary.BigEndian.PutUint32(message[1:], xid)
	binary.BigEndian.PutUint32(message[5:], subXID)
	return message
}

func streamCommitMessage(xid uint32, commitLSN, transactionEndLSN pglogrepl.LSN) []byte {
	message := make([]byte, 1+4+1+8+8+8)
	message[0] = 'c'
	binary.BigEndian.PutUint32(message[1:], xid)
	binary.BigEndian.PutUint64(message[6:], uint64(commitLSN))
	binary.BigEndian.PutUint64(message[14:], uint64(transactionEndLSN))
	return message
}

func commitMessage(commitLSN, transactionEndLSN pglogrepl.LSN) []byte {
	message := make([]byte, 1+1+8+8+8)
	message[0] = 'C'
	binary.BigEndian.PutUint64(message[2:], uint64(commitLSN))
	binary.BigEndian.PutUint64(message[10:], uint64(transactionEndLSN))
	binary.BigEndian.PutUint64(message[18:], 0)
	return message
}

func decodeKeyMap(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode key: %v", err)
	}
	return out
}
