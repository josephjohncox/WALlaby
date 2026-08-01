package replication

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
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
