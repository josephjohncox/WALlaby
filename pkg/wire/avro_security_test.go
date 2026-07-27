package wire

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	// Imported through the hamba/avro/v2 path on purpose: the module is redirected
	// by a local replace to the patched github.com/iskorotkov/avro/v2 (v2.33.0+).
	// Exercising these APIs here proves the shim wires our compiled code to the
	// fixed decoder that closes GO-2026-5046/5047/5048.
	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// avroLong encodes an int64 using Avro's zig-zag + variable-length long encoding.
// It lets tests hand-craft hostile block headers directly on the wire.
func avroLong(n int64) []byte {
	u := uint64((n << 1) ^ (n >> 63))
	var b []byte
	for u >= 0x80 {
		b = append(b, byte(u)|0x80)
		u >>= 7
	}
	return append(b, byte(u))
}

// runWithDeadline runs fn and fails if it does not return within d. It guards the
// liveness property behind GO-2026-5046: a hostile block count must not pin a CPU
// core indefinitely before the decoder reports an error.
func runWithDeadline(t *testing.T, d time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("decode did not terminate within %s (possible unbounded loop/alloc regression)", d)
	}
}

// TestAvroShimReportsPatchedProvenance is a compile-time and runtime assertion
// that the hamba/avro/v2 import path resolves to the patched implementation that
// carries the GO-2026-5048 remediation surface (Config.MaxMapAllocSize) and its
// GO-2026-5047 siblings (MaxSliceAllocSize/MaxByteSliceSize).
func TestAvroShimReportsPatchedProvenance(t *testing.T) {
	// If the shim ever regressed to the archived hamba implementation (which had
	// no MaxMapAllocSize field), this would fail to compile.
	cfg := avro.Config{
		MaxMapAllocSize:   1 << 10,
		MaxSliceAllocSize: 1 << 10,
		MaxByteSliceSize:  1 << 10,
	}
	if cfg.Freeze() == nil {
		t.Fatal("frozen config must be non-nil")
	}
}

// TestAvroDecoderRejectsMaliciousArrayBlockCount covers GO-2026-5046/5047: an
// attacker-declared array block count must be bounded and must never trigger the
// MinInt negation panic.
func TestAvroDecoderRejectsMaliciousArrayBlockCount(t *testing.T) {
	schema := avro.MustParse(`{"type":"array","items":"long"}`)
	api := avro.Config{MaxSliceAllocSize: 1 << 10}.Freeze()

	cases := []struct {
		name    string
		payload []byte
		wantErr string
	}{
		{
			name:    "count_over_alloc_bound",
			payload: avroLong(1_000_000_000),
			wantErr: "MaxSliceAllocSize",
		},
		{
			name:    "count_max_int64_truncated",
			payload: avroLong(math.MaxInt64),
			wantErr: "", // any bounded error; must not hang or panic
		},
		{
			name:    "count_min_int64_negation",
			payload: avroLong(math.MinInt64),
			wantErr: "block length is too small",
		},
		{
			name:    "negative_block_size_too_small",
			payload: append(avroLong(-4), avroLong(math.MinInt64)...),
			wantErr: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out []int64
			var err error
			runWithDeadline(t, 5*time.Second, func() {
				err = api.Unmarshal(schema, tc.payload, &out)
			})
			if err == nil {
				t.Fatalf("expected a bounded decode error, got nil (decoded %d elems)", len(out))
			}
			if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// TestAvroDecoderRejectsMaliciousMapBlockCount covers GO-2026-5048: the map
// decoder must enforce Config.MaxMapAllocSize cumulatively across blocks rather
// than growing the destination map without bound.
func TestAvroDecoderRejectsMaliciousMapBlockCount(t *testing.T) {
	schema := avro.MustParse(`{"type":"map","values":"long"}`)
	api := avro.Config{MaxMapAllocSize: 1 << 10}.Freeze()

	// Single hostile block declaring a billion entries, then EOF.
	single := avroLong(1_000_000_000)

	// Many sub-limit blocks that individually pass but must be rejected once the
	// cumulative running total crosses the bound.
	var chunked []byte
	for i := 0; i < 8; i++ {
		chunked = append(chunked, avroLong(1_000)...)
		// 1000 (key,value) pairs would need real data; truncation forces the
		// decoder to fail on the cumulative bound or on the missing payload,
		// never to loop unbounded.
	}

	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{"single_huge_block", single},
		{"chunked_blocks", chunked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out map[string]int64
			var err error
			runWithDeadline(t, 5*time.Second, func() {
				err = api.Unmarshal(schema, tc.payload, &out)
			})
			if err == nil {
				t.Fatalf("expected a bounded decode error, got nil (decoded %d entries)", len(out))
			}
		})
	}
}

// TestAvroDecoderRejectsOversizedByteSlice covers the GO-2026-5047 byte/string
// length narrowing path: a declared length beyond MaxByteSliceSize must error.
func TestAvroDecoderRejectsOversizedByteSlice(t *testing.T) {
	schema := avro.MustParse(`{"type":"bytes"}`)
	api := avro.Config{MaxByteSliceSize: 1 << 10}.Freeze()

	payload := avroLong(1_000_000_000) // declares ~1GB of bytes, then EOF
	var out []byte
	var err error
	runWithDeadline(t, 5*time.Second, func() {
		err = api.Unmarshal(schema, payload, &out)
	})
	if err == nil {
		t.Fatalf("expected an error for oversized bytes length, decoded %d bytes", len(out))
	}
}

// TestAvroLong64BitConversionRoundTrip verifies the 32/64-bit conversion
// properties: full-range int64 values survive the long codec without truncation
// or overflow. This is the safe counterpart to the GO-2026-5047 narrowing bugs.
func TestAvroLong64BitConversionRoundTrip(t *testing.T) {
	schema := avro.MustParse(`{"type":"long"}`)
	values := []int64{
		0, 1, -1, 2, -2,
		math.MaxInt32, math.MinInt32,
		int64(math.MaxInt32) + 1, int64(math.MinInt32) - 1,
		1 << 40, -(1 << 40),
		math.MaxInt64, math.MinInt64,
	}
	for _, v := range values {
		data, err := avro.Marshal(schema, v)
		if err != nil {
			t.Fatalf("marshal %d: %v", v, err)
		}
		var got int64
		if err := avro.Unmarshal(schema, data, &got); err != nil {
			t.Fatalf("unmarshal %d: %v", v, err)
		}
		if got != v {
			t.Fatalf("long round-trip mismatch: want %d got %d", v, got)
		}
	}
}

// TestAvroCodecOCFCompatibility verifies the OCF container format produced by the
// codec is well-formed (magic header) and round-trips through the patched decoder.
func TestAvroCodecOCFCompatibility(t *testing.T) {
	batch := connector.Batch{
		Schema: connector.Schema{
			Name:      "events",
			Namespace: "public",
			Version:   1,
			Columns: []connector.Column{
				{Name: "id", Type: "int8"},
				{Name: "amount", Type: "float8", Nullable: true},
				{Name: "label", Type: "text"},
			},
		},
		Records: []connector.Record{
			{
				Table:         "events",
				Operation:     connector.OpInsert,
				SchemaVersion: 1,
				Key:           []byte("k1"),
				After:         map[string]any{"id": int64(7), "amount": 1.5, "label": "hello"},
				Timestamp:     time.UnixMilli(1_700_000_000_000),
			},
		},
		WireFormat: connector.WireFormatAvro,
	}

	codec := &AvroCodec{}
	payload, err := codec.Encode(batch)
	if err != nil {
		t.Fatalf("encode avro: %v", err)
	}
	if len(payload) < 4 || !bytes.Equal(payload[:4], []byte("Obj\x01")) {
		t.Fatalf("payload is not a valid OCF container (bad magic): %x", payload[:min(4, len(payload))])
	}

	decoder, err := ocf.NewDecoder(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("ocf decoder: %v", err)
	}
	rows := 0
	for decoder.HasNext() {
		row := map[string]any{}
		if err := decoder.Decode(&row); err != nil {
			t.Fatalf("decode row: %v", err)
		}
		if row["label"] != "hello" {
			t.Fatalf("unexpected label: %v", row["label"])
		}
		rows++
	}
	if err := decoder.Error(); err != nil {
		t.Fatalf("decoder error: %v", err)
	}
	if rows != 1 {
		t.Fatalf("expected 1 row, got %d", rows)
	}
}

// TestAvroSchemaGenerationDeterministic verifies the canonical Avro schema our
// codec emits is byte-stable across repeated generation, including map-valued
// column metadata. Determinism here underpins reproducible artifact hashing.
func TestAvroSchemaGenerationDeterministic(t *testing.T) {
	schema := connector.Schema{
		Name:      "events",
		Namespace: "public",
		Columns: []connector.Column{
			{Name: "id", Type: "int8"},
			{Name: "vec", Type: "vector", TypeMetadata: map[string]string{
				"dimensions": "3", "algorithm": "hnsw", "source": "pgvector",
			}},
			{Name: "doc", Type: "jsonb", Nullable: true},
		},
	}
	first := avroSchemaFor(schema)
	for i := 0; i < 64; i++ {
		if got := avroSchemaFor(schema); got != first {
			t.Fatalf("schema generation not deterministic:\n first=%s\n got  =%s", first, got)
		}
	}
	// The generated schema must still parse under the patched parser.
	if _, err := avro.Parse(first); err != nil {
		t.Fatalf("generated schema failed to parse: %v", err)
	}
}
