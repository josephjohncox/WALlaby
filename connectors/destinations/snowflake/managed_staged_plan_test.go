package snowflake

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestStagedPlanIsDeterministic(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	first := stagedPlanFor(t, cfg, intent, transaction)
	second := stagedPlanFor(t, cfg, intent, transaction)
	if !bytes.Equal(first.fileBytes, second.fileBytes) {
		t.Fatal("staged file bytes are not deterministic")
	}
	if first.fileContentHash != second.fileContentHash || first.fileMD5 != second.fileMD5 {
		t.Fatalf("staged file digests differ: %s/%s vs %s/%s", first.fileContentHash, first.fileMD5, second.fileContentHash, second.fileMD5)
	}
	if first.identity.relativePath != second.identity.relativePath {
		t.Fatalf("staged path is not deterministic: %q vs %q", first.identity.relativePath, second.identity.relativePath)
	}
	if first.identity.externalID != second.identity.externalID {
		t.Fatalf("staged external id is not deterministic: %q vs %q", first.identity.externalID, second.identity.externalID)
	}
}

func TestStagedPlanPathBindsIdentity(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	plan := stagedPlanFor(t, cfg, intent, transaction)
	if !strings.HasPrefix(plan.identity.relativePath, stagedRetentionRoot+"/") {
		t.Fatalf("staged path %q does not begin at the retention root", plan.identity.relativePath)
	}
	if !strings.HasSuffix(plan.identity.relativePath, stagedFileExtension) {
		t.Fatalf("staged path %q does not carry the immutable extension", plan.identity.relativePath)
	}
	if !strings.Contains(plan.identity.relativePath, intent.ContentHash) {
		t.Fatalf("staged path %q must embed the logical content hash", plan.identity.relativePath)
	}
	// A different content hash for the same batch must land at a different path.
	other := intent
	other.ContentHash = differentHex(intent.ContentHash)
	otherIdentity, err := newManagedStagedIdentity(cfg, other, plan.identity.planHash, other.ContentHash)
	if err != nil {
		t.Fatal(err)
	}
	if otherIdentity.relativePath == plan.identity.relativePath {
		t.Fatal("distinct content hashes must not collide on one stage path")
	}
}

func TestStagedPlanCopyOptionsAreFailClosed(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	plan := stagedPlanFor(t, cfg, intent, transaction)
	statement := stagedCopyStatement(plan.copyPlan)
	if !strings.Contains(statement, "ON_ERROR = ABORT_STATEMENT") {
		t.Fatalf("staged COPY is not fail-closed: %q", statement)
	}
	if strings.Contains(statement, "CONTINUE") || strings.Contains(statement, "SKIP_FILE") {
		t.Fatalf("staged COPY admits a lossy ON_ERROR continuation: %q", statement)
	}
	if !strings.Contains(statement, "FORCE = FALSE") {
		t.Fatalf("staged COPY must not force re-loads: %q", statement)
	}
	if !strings.Contains(statement, "PURGE = FALSE") {
		t.Fatalf("staged COPY must not purge before receipt: %q", statement)
	}
}

func TestStagedPlanSerializedFileRoundTrips(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	plan := stagedPlanFor(t, cfg, intent, transaction)
	decoder := json.NewDecoder(bytes.NewReader(plan.fileBytes))
	decoder.UseNumber()
	rows := 0
	for {
		var row stagedChangelogRow
		if err := decoder.Decode(&row); err != nil {
			if err.Error() == "EOF" {
				break
			}
			t.Fatalf("decode staged row: %v", err)
		}
		if row.FlowIncarnationID != intent.FlowIncarnationID || row.LogicalBatchID != intent.LogicalBatchID {
			t.Fatalf("staged row identity=%+v does not carry the delivery identity", row)
		}
		if row.RecordHash == "" {
			t.Fatal("staged row is missing a record hash")
		}
		rows++
	}
	if rows != plan.rowCount || rows != 3 {
		t.Fatalf("staged file has %d rows, want %d", rows, plan.rowCount)
	}
}

func TestStagedPlanRejectsDDL(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
	schema := cfg.schemaContract
	transaction := connector.SourceTransaction{
		SourceLineageID: "lineage-1", TransactionID: 9, BeginLSN: "0/10", CommitLSN: "0/20", EndLSN: "0/20",
		Checkpoint: connector.Checkpoint{LSN: "0/20"},
		Fragments: []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{Schema: schema, Records: []connector.Record{
			{Table: "widgets", Operation: connector.OpDDL, DDL: "ALTER TABLE widgets ADD COLUMN c text"},
		}}}},
	}
	intent := stagedTestIntent(t, cfg, transaction)
	if _, err := planManagedStagedTransaction(cfg, intent, transaction); err == nil || !strings.Contains(err.Error(), "rejects all DDL") {
		t.Fatalf("staged plan DDL error=%v, want DDL rejection", err)
	}
}

func TestStagedPlanRejectsForeignRelation(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
	schema := cfg.schemaContract
	schema.Namespace = "public"
	schema.Name = "other"
	transaction := connector.SourceTransaction{
		SourceLineageID: "lineage-1", TransactionID: 9, BeginLSN: "0/10", CommitLSN: "0/20", EndLSN: "0/20",
		Checkpoint: connector.Checkpoint{LSN: "0/20"},
		Fragments: []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{Schema: schema, Records: []connector.Record{
			{Table: "other", Operation: connector.OpInsert, Key: json.RawMessage(`{"id":1}`), After: map[string]any{"id": int64(1), "value": "x", "payload": []byte{1}}},
		}}}},
	}
	intent := stagedTestIntent(t, cfg, transaction)
	if _, err := planManagedStagedTransaction(cfg, intent, transaction); err == nil {
		t.Fatal("staged plan admitted a relation outside the configured source")
	}
}

func TestStagedPlanRejectsIntentMismatch(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	mismatched := intent
	mismatched.ContentHash = differentHex(intent.ContentHash)
	if _, err := planManagedStagedTransaction(cfg, mismatched, transaction); err == nil {
		t.Fatal("staged plan admitted a content hash that differs from the transaction identity")
	}
}

func differentHex(hash string) string {
	if hash == strings.Repeat("a", len(hash)) {
		return strings.Repeat("b", len(hash))
	}
	return strings.Repeat("a", len(hash))
}

func TestStagedPlanEnforcesRowBound(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	cfg.maxTransactionRows = 1
	if _, err := planManagedStagedTransaction(cfg, intent, transaction); err == nil || !strings.Contains(err.Error(), "more than") {
		t.Fatalf("staged plan row-bound error=%v, want a bound rejection", err)
	}
}
