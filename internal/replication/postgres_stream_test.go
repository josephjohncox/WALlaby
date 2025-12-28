package replication

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
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

func decodeKeyMap(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode key: %v", err)
	}
	return out
}
