package wire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/hamba/avro/v2/ocf"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

var testColumnTypes = []string{"int8", "float8", "bool", "text"}

func TestProtoCodecRoundTripRapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		batch := rapidBatch(t)
		batch.WireFormat = connector.WireFormatProto

		codec := &ProtoCodec{}
		payload, err := codec.Encode(batch)
		if err != nil {
			t.Fatalf("encode proto: %v", err)
		}
		var decoded wallabypb.Batch
		if err := proto.Unmarshal(payload, &decoded); err != nil {
			t.Fatalf("decode proto: %v", err)
		}
		expected, err := batchToProto(batch)
		if err != nil {
			t.Fatalf("batch to proto: %v", err)
		}
		if !proto.Equal(&decoded, expected) {
			t.Fatalf("proto round-trip mismatch")
		}
	})
}

func TestAvroCodecRoundTripRapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		batch := rapidBatch(t)
		batch.WireFormat = connector.WireFormatAvro

		codec := &AvroCodec{}
		payload, err := codec.Encode(batch)
		if err != nil {
			t.Fatalf("encode avro: %v", err)
		}
		if len(payload) == 0 {
			t.Fatalf("avro payload empty")
		}

		decoder, err := ocf.NewDecoder(bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("avro decoder: %v", err)
		}
		rows := make([]map[string]any, 0, len(batch.Records))
		for decoder.HasNext() {
			row := map[string]any{}
			if err := decoder.Decode(&row); err != nil {
				t.Fatalf("decode avro row: %v", err)
			}
			rows = append(rows, row)
		}
		if len(rows) != len(batch.Records) {
			t.Fatalf("expected %d avro rows, got %d", len(batch.Records), len(rows))
		}
		for i, record := range batch.Records {
			row := rows[i]
			assertAvroRow(t, row, batch.Schema, record)
		}
	})
}

func TestArrowCodecRoundTripRapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		batch := rapidBatch(t)
		batch.WireFormat = connector.WireFormatArrow

		codec := &ArrowIPCCodec{}
		payload, err := codec.Encode(batch)
		if err != nil {
			t.Fatalf("encode arrow: %v", err)
		}
		if len(payload) == 0 {
			t.Fatalf("arrow payload empty")
		}

		reader, err := ipc.NewReader(bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("arrow reader: %v", err)
		}
		defer reader.Release()

		var records []arrow.Record
		for reader.Next() {
			rec := reader.Record()
			rec.Retain()
			records = append(records, rec)
		}
		if reader.Err() != nil {
			t.Fatalf("arrow read: %v", reader.Err())
		}
		rowCount := 0
		for _, rec := range records {
			rowCount += int(rec.NumRows())
		}
		if rowCount != len(batch.Records) {
			t.Fatalf("expected %d arrow rows, got %d", len(batch.Records), rowCount)
		}

		rowIndex := 0
		for _, rec := range records {
			fieldIndex := map[string]int{}
			for idx, field := range rec.Schema().Fields() {
				fieldIndex[field.Name] = idx
			}
			for i := 0; i < int(rec.NumRows()); i++ {
				record := batch.Records[rowIndex]
				assertArrowRow(t, rec, fieldIndex, batch.Schema, record, i)
				rowIndex++
			}
			rec.Release()
		}
	})
}

func rapidBatch(t *rapid.T) connector.Batch {
	columnCount := rapid.IntRange(1, 3).Draw(t, "columns")
	columns := make([]connector.Column, 0, columnCount)
	for i := 0; i < columnCount; i++ {
		colType := rapid.SampledFrom(testColumnTypes).Draw(t, fmt.Sprintf("type-%d", i))
		nullable := rapid.Bool().Draw(t, fmt.Sprintf("nullable-%d", i))
		columns = append(columns, connector.Column{
			Name:     fmt.Sprintf("col_%d", i),
			Type:     colType,
			Nullable: nullable,
		})
	}

	recordCount := rapid.IntRange(1, 4).Draw(t, "records")
	records := make([]connector.Record, 0, recordCount)
	hasDelete := false
	for i := 0; i < recordCount; i++ {
		op := rapid.SampledFrom([]connector.Operation{connector.OpInsert, connector.OpUpdate, connector.OpDelete}).Draw(t, fmt.Sprintf("op-%d", i))
		if op == connector.OpDelete {
			hasDelete = true
		}
		after := map[string]any{}
		for _, col := range columns {
			after[col.Name] = rapidValue(t, col)
		}
		records = append(records, connector.Record{
			Table:         "events",
			Operation:     op,
			SchemaVersion: 1,
			Key:           rapid.SliceOfN(rapid.Byte(), 0, 8).Draw(t, fmt.Sprintf("key-%d", i)),
			After:         after,
			Timestamp:     time.UnixMilli(int64(rapid.IntRange(0, 1_000_000).Draw(t, fmt.Sprintf("ts-%d", i)))),
		})
	}
	if hasDelete {
		for i := range columns {
			columns[i].Nullable = true
		}
	}

	return connector.Batch{
		Schema: connector.Schema{
			Name:      "events",
			Namespace: "public",
			Version:   1,
			Columns:   columns,
		},
		Records: records,
		Checkpoint: connector.Checkpoint{
			LSN:       fmt.Sprintf("%d", rapid.IntRange(1, 10_000).Draw(t, "lsn")),
			Timestamp: time.UnixMilli(int64(rapid.IntRange(0, 1_000_000).Draw(t, "cp-ts"))),
			Metadata:  map[string]string{"seq": "1"},
		},
	}
}

func rapidValue(t *rapid.T, col connector.Column) any {
	if col.Nullable && rapid.Bool().Draw(t, "nil") {
		return nil
	}
	switch col.Type {
	case "int8":
		return int64(rapid.IntRange(-1024, 1024).Draw(t, "int"))
	case "float8":
		return float64(rapid.IntRange(-1024, 1024).Draw(t, "float")) / 10
	case "bool":
		return rapid.Bool().Draw(t, "bool")
	default:
		return rapid.StringMatching(`[a-z]{1,8}`).Draw(t, "text")
	}
}

func assertAvroRow(t *rapid.T, row map[string]any, schema connector.Schema, record connector.Record) {
	if row["__op"] != string(record.Operation) {
		t.Fatalf("avro op mismatch")
	}
	if !equalInt64(row["__schema_version"], record.SchemaVersion) {
		t.Fatalf("avro schema version mismatch")
	}
	if row["__table"] != record.Table {
		t.Fatalf("avro table mismatch")
	}
	if row["__namespace"] != schema.Namespace {
		t.Fatalf("avro namespace mismatch")
	}
	if !equalBytes(row["__key"], record.Key) {
		t.Fatalf("avro key mismatch")
	}

	beforeJSON := ""
	if record.Before != nil {
		payload, _ := json.Marshal(record.Before)
		beforeJSON = string(payload)
	}
	if row["__before_json"] != beforeJSON {
		t.Fatalf("avro before json mismatch")
	}

	for _, col := range schema.Columns {
		val := record.After[col.Name]
		if record.Operation == connector.OpDelete {
			val = nil
		}
		expected := normalizeAvroValue(col.Type, val)
		if !equalAvroValue(row[col.Name], expected) {
			t.Fatalf("avro value mismatch for %s", col.Name)
		}
	}
}

func assertArrowRow(t *rapid.T, rec arrow.Record, fieldIndex map[string]int, schema connector.Schema, record connector.Record, row int) {
	get := func(name string) any {
		idx := fieldIndex[name]
		arr := rec.Column(idx)
		if arr.IsNull(row) {
			return nil
		}
		switch col := arr.(type) {
		case *array.String:
			return col.Value(row)
		case *array.Int64:
			return col.Value(row)
		case *array.Float64:
			return col.Value(row)
		case *array.Boolean:
			return col.Value(row)
		case *array.Binary:
			return col.Value(row)
		case *array.Timestamp:
			return int64(col.Value(row))
		default:
			return nil
		}
	}

	if get("__op") != string(record.Operation) {
		t.Fatalf("arrow op mismatch")
	}
	if get("__table") != record.Table {
		t.Fatalf("arrow table mismatch")
	}
	if get("__namespace") != schema.Namespace {
		t.Fatalf("arrow namespace mismatch")
	}
	if !equalBytes(get("__key"), record.Key) {
		t.Fatalf("arrow key mismatch")
	}
	beforeJSON := []byte(nil)
	if record.Before != nil {
		beforeJSON, _ = json.Marshal(record.Before)
	}
	if !equalBytes(get("__before_json"), beforeJSON) {
		t.Fatalf("arrow before json mismatch")
	}
	if tsVal := get("__ts"); tsVal != nil {
		if !equalInt64(tsVal, record.Timestamp.UnixMilli()) {
			t.Fatalf("arrow timestamp mismatch")
		}
	}
	if !equalInt64(get("__schema_version"), record.SchemaVersion) {
		t.Fatalf("arrow schema version mismatch")
	}

	for _, col := range schema.Columns {
		val := record.After[col.Name]
		if record.Operation == connector.OpDelete {
			val = nil
		}
		if val == nil {
			if get(col.Name) != nil {
				t.Fatalf("arrow expected null for %s", col.Name)
			}
			continue
		}
		var expected any
		switch col.Type {
		case "int8":
			iv, err := asInt64(val)
			if err != nil {
				t.Fatalf("arrow int conversion failed for %s: %v", col.Name, err)
			}
			expected = iv
		case "float8":
			expected = asFloat64(val)
		case "bool":
			expected = asBool(val)
		default:
			expected = fmt.Sprint(val)
		}
		if !equalArrowValue(get(col.Name), expected) {
			t.Fatalf("arrow value mismatch for %s", col.Name)
		}
	}
}

func equalBytes(got any, want []byte) bool {
	switch v := got.(type) {
	case nil:
		return len(want) == 0
	case []byte:
		return bytes.Equal(v, want)
	case string:
		return bytes.Equal([]byte(v), want)
	default:
		return false
	}
}

func equalInt64(got any, want int64) bool {
	switch v := got.(type) {
	case int64:
		return v == want
	case int32:
		return int64(v) == want
	case int:
		return int64(v) == want
	case uint64:
		return int64(v) == want
	default:
		return false
	}
}

func equalArrowValue(got any, want any) bool {
	switch w := want.(type) {
	case int64:
		return equalInt64(got, w)
	case float64:
		gv, ok := got.(float64)
		return ok && gv == w
	case bool:
		gv, ok := got.(bool)
		return ok && gv == w
	case string:
		gv, ok := got.(string)
		return ok && gv == w
	default:
		return false
	}
}

func equalAvroValue(got any, want any) bool {
	if want == nil {
		return got == nil
	}
	switch w := want.(type) {
	case int64:
		return equalInt64(got, w)
	case float64:
		gv, ok := got.(float64)
		return ok && gv == w
	case bool:
		gv, ok := got.(bool)
		return ok && gv == w
	case string:
		gv, ok := got.(string)
		return ok && gv == w
	case []byte:
		return equalBytes(got, w)
	default:
		return false
	}
}
