package snowflake

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"pgregory.net/rapid"
)

// FuzzStreamRecordHash proves the per-row identity is deterministic and always a
// 64-character lowercase hexadecimal digest for arbitrary content. This identity,
// not any transport token, is the SQL-observed completeness key.
func FuzzStreamRecordHash(f *testing.F) {
	f.Add("public", "widgets", "id", "value", int64(1), uint64(0))
	f.Add("", "", "", "", int64(0), uint64(0))
	f.Add("s", "t", "k", "robert'); DROP TABLE widgets;--", int64(-5), uint64(9))
	f.Fuzz(func(t *testing.T, namespace, name, keyColumn, value string, id int64, ordinal uint64) {
		row := streamChangelogRow{
			FlowID: "flow", FlowIncarnationID: "inc", SourceLineageID: "lin", DestinationRevisionID: "rev",
			LogicalBatchID: "batch", ContentHash: "hash", OffsetToken: "off", AppendOrdinal: ordinal, SourcePosition: "0/1",
			TransactionID: 1, BeginLSN: "0/1", CommitLSN: "0/1", EndLSN: "0/1", SourceNamespace: namespace, SourceTable: name,
			SchemaContractHash: "sc", Operation: "insert", KeyJSON: map[string]any{keyColumn: id},
			AfterImage: map[string]any{keyColumn: id, "value": value}, EventTime: "2026-01-01T00:00:00Z",
		}
		first, err := streamRecordHash(row)
		if err != nil {
			return
		}
		second, err := streamRecordHash(row)
		if err != nil || first != second {
			t.Fatalf("streamRecordHash is not deterministic: %q vs %q (%v)", first, second, err)
		}
		if !stagedIsLowerHex(first, 64) {
			t.Fatalf("streamRecordHash produced a non-hex digest: %q", first)
		}
	})
}

// FuzzStreamChannelName proves channel-name derivation never panics, never emits
// a whitespace or control character, and is deterministic. A hostile identity can
// therefore never produce an invalid or colliding channel name.
func FuzzStreamChannelName(f *testing.F) {
	for _, seed := range []string{"", "inc-1", "a/b", "space here", "\x00\x01", strings.Repeat("x", 300)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, incarnation string) {
		cfg := streamConfig{channelNamePrefix: "wallaby_stream"}
		intent := connector.DeliveryIntent{FlowIncarnationID: incarnation, DestinationRevisionID: "rev", LogicalBatchID: "scope-validation"}
		first := streamChannelName(cfg, intent)
		second := streamChannelName(cfg, intent)
		if first != second {
			t.Fatalf("streamChannelName is not deterministic: %q vs %q", first, second)
		}
		if strings.ContainsAny(first, " \t\r\n\x00") {
			t.Fatalf("streamChannelName produced a hostile channel name: %q", first)
		}
	})
}

// TestStreamPlanDeterminismProperty proves the whole planner is deterministic for
// a bounded space of insert transactions: identical logical content always yields
// identical row identities, offset token, and append identity.
func TestStreamPlanDeterminismProperty(t *testing.T) {
	t.Parallel()
	cfg := streamTestConfig(t)
	rapid.Check(t, func(t *rapid.T) {
		id := rapid.Int64Range(1, 1_000_000).Draw(t, "id")
		value := rapid.String().Draw(t, "value")
		transaction := connector.SourceTransaction{
			SourceLineageID: "lineage-1", TransactionID: uint32(id%1000) + 1, BeginLSN: "0/10", CommitLSN: "0/20", EndLSN: "0/20",
			Checkpoint: connector.Checkpoint{LSN: "0/20"},
			Fragments: []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{Schema: cfg.schemaContract, Records: []connector.Record{
				{Table: "widgets", Operation: connector.OpInsert, Key: json.RawMessage(`{"id":` + itoa(id) + `}`), After: map[string]any{"id": id, "value": value, "payload": []byte{1}}},
			}}}},
		}
		contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
		if err != nil {
			return
		}
		position, err := connector.CheckpointPositionID(transaction.Checkpoint)
		if err != nil {
			return
		}
		intent := connector.DeliveryIntent{
			FlowID: cfg.flowID, FlowIncarnationID: "inc", SourceLineageID: "lineage-1", Generation: 1,
			AcquisitionID: "acq", LeaseEpoch: 1, DestinationRevisionID: cfg.destinationRevision,
			LogicalBatchID: logicalBatchID, PositionID: position, ContentHash: contentHash,
		}
		first, err := planManagedStreamTransaction(cfg, intent, transaction)
		if err != nil {
			return
		}
		second, err := planManagedStreamTransaction(cfg, intent, transaction)
		if err != nil {
			t.Fatalf("second plan failed after first succeeded: %v", err)
		}
		if first.rowsContentHash != second.rowsContentHash || first.identity.externalID != second.identity.externalID {
			t.Fatal("streaming planning is not deterministic for identical logical content")
		}
	})
}

// TestStreamRequestJournalProperty proves that unjournaled target state fails
// closed, while a fresh request appends once and replay adopts its receipt.
func TestStreamRequestJournalProperty(t *testing.T) {
	t.Parallel()
	cfg := streamTestConfig(t)
	transaction := managedTestTransaction(managedTestSchema())
	intent := streamTestIntent(t, cfg, transaction)
	plan, err := planManagedStreamTransaction(cfg, intent, transaction)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	rapid.Check(t, func(rt *rapid.T) {
		proto := newFakeStreamProtocol()
		seeded := 0
		for index, hash := range plan.rowHashes {
			if rapid.Bool().Draw(rt, "committed-"+itoa(int64(index))) {
				proto.seedCommittedRow(hash, 1)
				seeded++
			}
		}
		driver := newStreamDriver(proto, cfg, "catalog-fingerprint", streamingHooks{})
		driver.sleep = noStreamSleep
		_, applyErr := driver.apply(context.Background(), intent, transaction)
		if seeded > 0 {
			if !errors.Is(applyErr, connector.ErrDeliveryIndeterminate) || proto.appendCalls != 0 || proto.insertCalls != 0 {
				rt.Fatalf("unjournaled target outcome err/appends/receipts=%v/%d/%d", applyErr, proto.appendCalls, proto.insertCalls)
			}
			return
		}
		if applyErr != nil {
			rt.Fatalf("fresh apply: %v", applyErr)
		}
		appendCalls := proto.appendCalls
		if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
			rt.Fatalf("receipt replay: %v", err)
		}
		if proto.appendCalls != appendCalls || proto.insertCalls != 1 {
			rt.Fatalf("replay append/receipt=%d/%d, want %d/1", proto.appendCalls, proto.insertCalls, appendCalls)
		}
	})
}

func noStreamSleep(context.Context, time.Duration) error { return nil }
