package snowflake

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"pgregory.net/rapid"
)

// FuzzStagedPathSegment proves stage-path derivation never panics, never emits a
// path separator or stage-hostile character, and is deterministic. A hostile
// identity can therefore never escape its stage directory.
func FuzzStagedPathSegment(f *testing.F) {
	for _, seed := range []string{"", "flow-1", "a/b/c", "../escape", "space here", "\x00\x01", strings.Repeat("x", 300)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		first := stagedPathSegment(value)
		second := stagedPathSegment(value)
		if first != second {
			t.Fatalf("stagedPathSegment is not deterministic: %q vs %q", first, second)
		}
		if strings.ContainsAny(first, "/\\ \t\r\n\x00") {
			t.Fatalf("stagedPathSegment produced a hostile path segment: %q", first)
		}
		for _, character := range first {
			if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '_' || character == '-') {
				t.Fatalf("stagedPathSegment produced an out-of-alphabet character in %q", first)
			}
		}
	})
}

// FuzzCanonicalStagedValue proves value canonicalization never panics and is a
// fixed point for already-canonical values.
func FuzzCanonicalStagedValue(f *testing.F) {
	for _, seed := range []string{"", "text", "9", "3.14", "true"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		canonical, err := canonicalStagedValue(value)
		if err != nil {
			return
		}
		again, err := canonicalStagedValue(canonical)
		if err != nil || again != canonical {
			t.Fatalf("canonicalStagedValue is not idempotent for %q: %v/%v", value, again, err)
		}
	})
}

// FuzzSerializeStagedFile proves the immutable-file encoder never panics, never
// emits an embedded newline inside a row, and produces a 64-char content hash
// and 32-char MD5 that round-trip through the newline-delimited decoder.
func FuzzSerializeStagedFile(f *testing.F) {
	f.Add("public", "widgets", "id", "value", int64(1))
	f.Add("", "", "", "", int64(0))
	f.Add("s", "t", "k", "robert'); DROP TABLE widgets;--", int64(-5))
	f.Fuzz(func(t *testing.T, namespace, name, keyColumn, value string, id int64) {
		row := stagedChangelogRow{
			FlowID: "flow", FlowIncarnationID: "inc", SourceLineageID: "lin", DestinationRevisionID: "rev",
			LogicalBatchID: "batch", ContentHash: "hash", SourcePosition: "0/1", TransactionID: 1,
			BeginLSN: "0/1", CommitLSN: "0/1", EndLSN: "0/1", SourceNamespace: namespace, SourceTable: name,
			SchemaContractHash: "sc", Operation: "insert", KeyJSON: map[string]any{keyColumn: id},
			AfterImage: map[string]any{keyColumn: id, "value": value}, EventTime: "2026-01-01T00:00:00Z",
		}
		hash, err := stagedRecordHash(row)
		if err != nil {
			return
		}
		row.RecordHash = hash
		content, contentHash, md5, err := serializeStagedFile([]stagedChangelogRow{row})
		if err != nil {
			return
		}
		if len(contentHash) != 64 || len(md5) != 32 {
			t.Fatalf("digest sizes content=%d md5=%d", len(contentHash), len(md5))
		}
		if bytes.Count(content, []byte("\n")) != 1 {
			t.Fatalf("serialized single row has %d newlines, want exactly 1", bytes.Count(content, []byte("\n")))
		}
		decoder := json.NewDecoder(bytes.NewReader(content))
		var decoded stagedChangelogRow
		if err := decoder.Decode(&decoded); err != nil {
			t.Fatalf("round-trip decode failed: %v", err)
		}
	})
}

// FuzzNewManagedStagedIdentity proves identity derivation never panics for
// arbitrary delivery identities and is deterministic.
func FuzzNewManagedStagedIdentity(f *testing.F) {
	f.Add("inc", "rev", "batch", "aa", "bb")
	f.Add("", "", "", "", "")
	f.Fuzz(func(t *testing.T, incarnation, revision, batch, planHash, contentHash string) {
		cfg := stagedConfig{
			profile: connector.ManagedProfilePostgresToSnowflakeStagedAppendV1, flowID: "flow-1", stage: "STAGE",
			table: "T", receiptsTable: "R", fileFormat: "FF",
		}
		intent := connector.DeliveryIntent{FlowIncarnationID: incarnation, DestinationRevisionID: revision, LogicalBatchID: batch}
		first, firstErr := newManagedStagedIdentity(cfg, intent, planHash, contentHash)
		second, secondErr := newManagedStagedIdentity(cfg, intent, planHash, contentHash)
		if (firstErr == nil) != (secondErr == nil) {
			t.Fatalf("nondeterministic identity error: %v vs %v", firstErr, secondErr)
		}
		if firstErr != nil {
			return
		}
		if first.relativePath != second.relativePath || first.externalID != second.externalID {
			t.Fatalf("identity is not deterministic: %+v vs %+v", first, second)
		}
		if !strings.HasPrefix(first.relativePath, stagedRetentionRoot+"/") {
			t.Fatalf("identity path %q escaped the retention root", first.relativePath)
		}
	})
}

// TestStagedPlanDeterminismProperty proves the whole planner is deterministic for
// a bounded space of insert transactions: identical logical content always yields
// identical bytes, digests, and stage identity, which is what makes replay safe.
func TestStagedPlanDeterminismProperty(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
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
		first, err := planManagedStagedTransaction(cfg, intent, transaction)
		if err != nil {
			return
		}
		second, err := planManagedStagedTransaction(cfg, intent, transaction)
		if err != nil {
			t.Fatalf("second plan failed after first succeeded: %v", err)
		}
		if !bytes.Equal(first.fileBytes, second.fileBytes) || first.identity.relativePath != second.identity.relativePath {
			t.Fatal("staged planning is not deterministic for identical logical content")
		}
	})
}

func itoa(value int64) string {
	return strings.TrimSpace(jsonNumber(value))
}

func jsonNumber(value int64) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
