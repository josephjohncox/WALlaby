package snowflake

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestStreamPlanIsDeterministic(t *testing.T) {
	t.Parallel()
	cfg := streamTestConfig(t)
	transaction := managedTestTransaction(managedTestSchema())
	intent := streamTestIntent(t, cfg, transaction)
	first, err := planManagedStreamTransaction(cfg, intent, transaction)
	if err != nil {
		t.Fatalf("first plan: %v", err)
	}
	second, err := planManagedStreamTransaction(cfg, intent, transaction)
	if err != nil {
		t.Fatalf("second plan: %v", err)
	}
	if first.rowsContentHash != second.rowsContentHash || first.identity.externalID != second.identity.externalID {
		t.Fatal("streaming planning is not deterministic for identical logical content")
	}
	if first.identity.offsetToken != second.identity.offsetToken || first.identity.channelName != second.identity.channelName {
		t.Fatal("streaming offset token or channel name is not deterministic")
	}
	// Every row identity must be a 64-char lowercase hex hash and unique.
	seen := make(map[string]struct{}, len(first.rowHashes))
	for _, hash := range first.rowHashes {
		if !stagedIsLowerHex(hash, 64) {
			t.Fatalf("row hash %q is not a 64-char lowercase hex digest", hash)
		}
		if _, duplicate := seen[hash]; duplicate {
			t.Fatalf("row identity %q is not unique within the batch", hash)
		}
		seen[hash] = struct{}{}
	}
}

func TestStreamPlanRejectsOversizeRow(t *testing.T) {
	t.Parallel()
	cfg := streamTestConfig(t)
	cfg.maxRowBytes = 64 // far below one serialized row
	transaction := managedTestTransaction(managedTestSchema())
	intent := streamTestIntent(t, cfg, transaction)
	_, err := planManagedStreamTransaction(cfg, intent, transaction)
	if !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("oversize row must fail closed, got %v", err)
	}
}

func TestStreamPlanAcceptsUnchangedToastUpdate(t *testing.T) {
	t.Parallel()
	cfg := streamTestConfig(t)
	schema := managedTestSchema()
	// An update whose "payload" TOAST column is unchanged: absent from the after
	// image and declared via Unchanged. Under toast_fetch=off a replay yields the
	// identical partial after image and the identical row identity.
	transaction := connector.SourceTransaction{
		SourceLineageID: "lineage-1", TransactionID: 7, BeginLSN: "0/10", CommitLSN: "0/20", EndLSN: "0/20",
		Checkpoint: connector.Checkpoint{LSN: "0/20", Timestamp: time.Unix(100, 0).UTC()},
		Fragments: []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{Schema: schema, Records: []connector.Record{
			{Table: "widgets", Operation: connector.OpUpdate, Key: json.RawMessage(`{"id":1}`),
				After: map[string]any{"id": int64(1), "value": "changed"}, Unchanged: []string{"payload"}},
		}}}},
	}
	intent := streamTestIntent(t, cfg, transaction)
	first, err := planManagedStreamTransaction(cfg, intent, transaction)
	if err != nil {
		t.Fatalf("unchanged-TOAST update rejected: %v", err)
	}
	if len(first.rows) != 1 || len(first.rows[0].UnchangedToast) != 1 || first.rows[0].UnchangedToast[0] != "payload" {
		t.Fatalf("unchanged-TOAST columns not recorded: %+v", first.rows[0].UnchangedToast)
	}
	second, err := planManagedStreamTransaction(cfg, intent, transaction)
	if err != nil {
		t.Fatalf("replay plan: %v", err)
	}
	if first.rows[0].RowHash != second.rows[0].RowHash {
		t.Fatal("unchanged-TOAST row identity is not stable across replay")
	}
}

func TestStreamPlanRejectsToastValueInAfterImage(t *testing.T) {
	t.Parallel()
	cfg := streamTestConfig(t)
	schema := managedTestSchema()
	transaction := connector.SourceTransaction{
		SourceLineageID: "lineage-1", TransactionID: 7, BeginLSN: "0/10", CommitLSN: "0/20", EndLSN: "0/20",
		Checkpoint: connector.Checkpoint{LSN: "0/20", Timestamp: time.Unix(100, 0).UTC()},
		Fragments: []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{Schema: schema, Records: []connector.Record{
			// "payload" is both declared unchanged AND present in the after image: an
			// ambiguous placeholder that must fail closed.
			{Table: "widgets", Operation: connector.OpUpdate, Key: json.RawMessage(`{"id":1}`),
				After: map[string]any{"id": int64(1), "value": "changed", "payload": []byte{9}}, Unchanged: []string{"payload"}},
		}}}},
	}
	intent := streamTestIntent(t, cfg, transaction)
	if _, err := planManagedStreamTransaction(cfg, intent, transaction); err == nil {
		t.Fatal("ambiguous unchanged-TOAST value in the after image must fail closed")
	}
}

func TestStreamPlanRejectsForeignRelation(t *testing.T) {
	t.Parallel()
	cfg := streamTestConfig(t)
	// A self-consistent transaction for a relation the contract does not admit.
	schema := managedTestSchema()
	schema.Name = "gadgets"
	transaction := connector.SourceTransaction{
		SourceLineageID: "lineage-1", TransactionID: 42, BeginLSN: "0/10", CommitLSN: "0/20", EndLSN: "0/20",
		Checkpoint: connector.Checkpoint{LSN: "0/20", Timestamp: time.Unix(100, 0).UTC()},
		Fragments: []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{Schema: schema, Records: []connector.Record{
			{Table: "gadgets", Operation: connector.OpInsert, Key: json.RawMessage(`{"id":1}`), After: map[string]any{"id": int64(1), "value": "first", "payload": []byte{1}}},
		}}}},
	}
	intent := streamTestIntent(t, cfg, transaction)
	if _, err := planManagedStreamTransaction(cfg, intent, transaction); err == nil {
		t.Fatal("a relation outside the admitted contract must be rejected")
	}
}
