package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type policyState struct {
	typ, value, position, content string
	deleted                       bool
}

type capturePolicyTx struct {
	pgx.Tx
	mu                  sync.Mutex
	queries             []string
	state               map[string]policyState
	forceSchemaMismatch bool
	copiedRows          int64
}

type policyRow struct {
	values []any
	err    error
}

func (r policyRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for index := range dest {
		switch pointer := dest[index].(type) {
		case *int:
			*pointer = r.values[index].(int)
		case *string:
			*pointer = r.values[index].(string)
		case *bool:
			*pointer = r.values[index].(bool)
		default:
			return fmt.Errorf("unsupported scan destination %T", dest[index])
		}
	}
	return nil
}

func policyStateKey(args []any) string {
	return fmt.Sprintf("%v|%v|%v|%v|%v|%v", args[0], args[1], args[2], args[3], args[4], args[5])
}

func (t *capturePolicyTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.queries = append(t.queries, sql)
	if t.state == nil {
		t.state = map[string]policyState{}
	}
	switch {
	case strings.Contains(sql, "array_agg(a.attname||'|'") || strings.Contains(sql, "array_agg(c.conname||'|'"):
		return policyRow{values: []any{!t.forceSchemaMismatch}}
	case strings.HasPrefix(sql, "SELECT count(*) FROM pg_catalog.pg_index"):
		return policyRow{values: []any{1}}
	case strings.Contains(sql, "am.amname='btree'"):
		return policyRow{values: []any{!t.forceSchemaMismatch}}
	case strings.HasPrefix(sql, "INSERT INTO wallaby.watermark_state"):
		key := policyStateKey(args)
		if _, exists := t.state[key]; exists {
			return policyRow{err: pgx.ErrNoRows}
		}
		t.state[key] = policyState{typ: args[6].(string), value: args[7].(string), position: args[8].(string), content: args[9].(string), deleted: args[10].(bool)}
		return policyRow{values: []any{1}}
	case strings.HasPrefix(sql, "SELECT watermark_type,watermark_value"):
		state, ok := t.state[policyStateKey(args)]
		if !ok {
			return policyRow{err: pgx.ErrNoRows}
		}
		return policyRow{values: []any{state.typ, state.value, state.position, state.content}}
	case strings.HasPrefix(sql, "SELECT $1::") && len(args) == 1:
		return policyRow{values: []any{fmt.Sprint(args[0])}}
	case strings.HasPrefix(sql, "SELECT CASE WHEN"):
		incoming, err := strconv.ParseInt(args[0].(string), 10, 64)
		if err != nil {
			return policyRow{err: err}
		}
		stored, err := strconv.ParseInt(args[1].(string), 10, 64)
		if err != nil {
			return policyRow{err: err}
		}
		wmCmp := 0
		if incoming > stored {
			wmCmp = 1
		} else if incoming < stored {
			wmCmp = -1
		}
		positionCmp, err := connector.CompareCheckpointLSN(args[2].(string), args[3].(string))
		if err != nil {
			return policyRow{err: err}
		}
		return policyRow{values: []any{wmCmp, positionCmp}}
	default:
		return policyRow{err: errors.New("unexpected QueryRow: " + sql)}
	}
}

func (t *capturePolicyTx) CopyFrom(_ context.Context, _ pgx.Identifier, _ []string, source pgx.CopyFromSource) (int64, error) {
	var rows int64
	for source.Next() {
		rows++
	}
	if err := source.Err(); err != nil {
		return 0, err
	}
	t.mu.Lock()
	t.queries = append(t.queries, "COPY TARGET ROWS")
	t.copiedRows += rows
	t.mu.Unlock()
	return rows, nil
}

func (t *capturePolicyTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.queries = append(t.queries, sql)
	if strings.HasPrefix(sql, "UPDATE wallaby.watermark_state") {
		key := policyStateKey(args)
		state := t.state[key]
		state.value, state.position, state.content, state.deleted = args[6].(string), args[7].(string), args[8].(string), args[9].(bool)
		t.state[key] = state
	}
	return pgconn.NewCommandTag("OK"), nil
}

func watermarkSchema() connector.Schema {
	return connector.Schema{Namespace: "public", Name: "widgets", Columns: []connector.Column{
		{Name: "id", Type: "int8", TypeMetadata: map[string]string{"primary_key": "true", "replica_identity": "true"}}, {Name: "name", Type: "text"}, {Name: "updated_at", Type: "int8", Nullable: false, TypeMetadata: map[string]string{"replica_identity": "true"}},
	}}
}

func watermarkPolicy() connector.TableWritePolicy {
	return connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}, WatermarkColumn: "updated_at", ProjectionFingerprint: "projection-v1"}
}

func applyTestWatermarkBatch(destination *Destination, tx pgx.Tx, target string, schema connector.Schema, records []connector.Record, policy connector.TableWritePolicy) error {
	for index := range records {
		if records[index].SourcePosition == "" {
			records[index].SourcePosition = fmt.Sprintf("0/%X", 16+index)
		}
	}
	return destination.applyWatermarkBatch(context.Background(), tx, target, schema, records, policy)
}

func TestWatermarkStateCatalogVerificationCoversCanonicalColumnConstraintAndIndexContracts(t *testing.T) {
	tx := &capturePolicyTx{}
	if err := ensureWatermarkStateTable(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"a.attgenerated::text", "a.attidentity::text", "pg_get_constraintdef", "am.amname='btree'", "i.indisvalid", "i.indpred IS NULL", "i.indexprs IS NULL"} {
		if !containsQuery(tx.queries, required) {
			t.Fatalf("catalog verification missing %q: %v", required, tx.queries)
		}
	}
}

func TestWatermarkStateSchemaMismatchFailsClosed(t *testing.T) {
	tx := &capturePolicyTx{forceSchemaMismatch: true}
	if err := ensureWatermarkStateTable(context.Background(), tx); err == nil || !strings.Contains(err.Error(), "contract mismatch") {
		t.Fatalf("schema mismatch error=%v", err)
	}
}

func TestMappedTargetTableDotIsLiteralIdentifier(t *testing.T) {
	destination := &Destination{}
	schema, table := destination.targetParts(connector.Schema{Namespace: "mapped_schema"}, "literal.table")
	if schema != "mapped_schema" || table != "literal.table" {
		t.Fatalf("target parts=%q/%q", schema, table)
	}
	got := destination.targetTable(connector.Schema{Namespace: "mapped_schema"}, connector.Record{Table: "literal.table"})
	if got != `"mapped_schema"."literal.table"` {
		t.Fatalf("target=%s", got)
	}
}

func TestManagedRuntimeOldImageAdmissionUsesPolicyNotPrimaryMetadata(t *testing.T) {
	fullSchema := connector.Schema{Name: "events", Columns: []connector.Column{
		{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"primary_key": "true", "replica_identity": "true"}},
		{Name: "email", Type: "text", TypeMetadata: map[string]string{"replica_identity": "true"}},
		{Name: "watermark", Type: "bigint", TypeMetadata: map[string]string{"replica_identity": "true"}},
	}}
	policy := connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"email"}, WatermarkColumn: "watermark"}
	if err := validateProjectedOldImagePolicy(fullSchema, policy); err != nil {
		t.Fatalf("full identity natural key rejected: %v", err)
	}
	missing := fullSchema
	missing.Columns = append([]connector.Column(nil), fullSchema.Columns...)
	missing.Columns[1].TypeMetadata = map[string]string{}
	if err := validateProjectedOldImagePolicy(missing, policy); err == nil || !strings.Contains(err.Error(), "upsert key") {
		t.Fatalf("missing natural-key old image error=%v", err)
	}
}

func TestUpsertConflictTargetUsesProjectedPolicyKeyOrder(t *testing.T) {
	destination := &Destination{}
	tx := &capturePolicyTx{}
	schema := connector.Schema{Name: "events", Columns: []connector.Column{{Name: "id", Type: "bigint"}, {Name: "tenant", Type: "bigint"}, {Name: "value", Type: "text"}}}
	record := connector.Record{Table: "events", Operation: connector.OpInsert, Key: []byte(`{"id":1,"tenant":2}`), After: map[string]any{"id": int64(1), "tenant": int64(2), "value": "x"}}
	if err := destination.upsertRows(context.Background(), tx, `"events"`, schema, []connector.Record{record}, []string{"tenant", "id"}); err != nil {
		t.Fatal(err)
	}
	if !containsQuery(tx.queries, `ON CONFLICT ("tenant", "id")`) {
		t.Fatalf("upsert queries=%v", tx.queries)
	}
}

func TestManagedAppendAcceptsRepeatedSourceKeys(t *testing.T) {
	destination := &Destination{}
	tx := &capturePolicyTx{}
	schema := connector.Schema{Namespace: "logs", Name: "events", Columns: []connector.Column{{Name: "id", Type: "bigint"}, {Name: connector.AppendOperationColumn, Type: "text"}, {Name: connector.AppendDeletedColumn, Type: "boolean"}, {Name: connector.AppendSourcePositionColumn, Type: "text"}}}
	records := []connector.Record{
		{Table: "events", Operation: connector.OpInsert, After: map[string]any{"id": int64(1), connector.AppendOperationColumn: "insert", connector.AppendDeletedColumn: false, connector.AppendSourcePositionColumn: "0/10"}},
		{Table: "events", Operation: connector.OpInsert, After: map[string]any{"id": int64(1), connector.AppendOperationColumn: "update", connector.AppendDeletedColumn: false, connector.AppendSourcePositionColumn: "0/20"}},
	}
	target, err := newPostgresTarget("logs", "events")
	if err != nil {
		t.Fatal(err)
	}
	if err := destination.applyBatch(context.Background(), tx, target, schema, records, writeModeAppend, connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend}); err != nil {
		t.Fatal(err)
	}
	if tx.copiedRows != 2 {
		t.Fatalf("copied rows=%d, want both repeated-key events", tx.copiedRows)
	}
}

func TestBootstrapSeedsDurableWatermarkStateFromPublishedTarget(t *testing.T) {
	destination := &Destination{flowID: "flow"}
	tx := &capturePolicyTx{}
	table := connector.BootstrapTable{Schema: watermarkSchema(), WritePolicy: watermarkPolicy()}
	if err := destination.seedBootstrapWatermarkState(context.Background(), tx, connector.BootstrapIntent{ManifestHash: "manifest-v1"}, "mapped", "literal.table", connector.BootstrapTable{Schema: table.Schema, WritePolicy: table.WritePolicy, SourcePosition: "0/10"}); err != nil {
		t.Fatal(err)
	}
	deleteIndex, insertIndex := -1, -1
	for index, query := range tx.queries {
		if strings.HasPrefix(query, "DELETE FROM wallaby.watermark_state") {
			deleteIndex = index
		}
		if strings.HasPrefix(query, "INSERT INTO wallaby.watermark_state") {
			insertIndex = index
		}
	}
	if !containsQuery(tx.queries, `ARRAY["id"::bigint::text]`) || !containsQuery(tx.queries, `"mapped"."literal.table"`) || containsQuery(tx.queries, `ON CONFLICT`) || deleteIndex < 0 || insertIndex <= deleteIndex {
		t.Fatalf("bootstrap must replace the exact state scope before seeding: %v", tx.queries)
	}
}

func TestWatermarkDeleteTombstoneBlocksStaleAndEqualResurrection(t *testing.T) {
	destination := &Destination{flowID: "flow"}
	tx := &capturePolicyTx{}
	deleteRecord := connector.Record{Table: "widgets", Operation: connector.OpDelete, Key: []byte(`{"id":1}`), Before: map[string]any{"id": int64(1), "updated_at": int64(20)}, SourcePosition: "0/20"}
	if err := applyTestWatermarkBatch(destination, tx, `"public"."widgets"`, watermarkSchema(), []connector.Record{deleteRecord}, watermarkPolicy()); err != nil {
		t.Fatal(err)
	}
	stale := connector.Record{Table: "widgets", Operation: connector.OpInsert, Key: []byte(`{"id":1}`), After: map[string]any{"id": int64(1), "name": "stale", "updated_at": int64(10)}, SourcePosition: "0/30"}
	equal := connector.Record{Table: "widgets", Operation: connector.OpInsert, Key: []byte(`{"id":1}`), After: map[string]any{"id": int64(1), "name": "equal", "updated_at": int64(20)}, SourcePosition: "0/18"}
	before := len(tx.queries)
	if err := applyTestWatermarkBatch(destination, tx, `"public"."widgets"`, watermarkSchema(), []connector.Record{stale, equal}, watermarkPolicy()); err != nil {
		t.Fatal(err)
	}
	for _, query := range tx.queries[before:] {
		if strings.HasPrefix(query, "INSERT INTO \"public\".\"widgets\"") {
			t.Fatalf("stale/equal mutation reached target: %s", query)
		}
	}
	if !containsQuery(tx.queries, "SELECT $1::bigint::text") {
		t.Fatal("key/watermark values were not canonicalized through PostgreSQL casts")
	}
	for _, state := range tx.state {
		if !state.deleted || state.value != "20" {
			t.Fatalf("state=%+v, want durable watermark-20 tombstone", state)
		}
	}
}

func TestWatermarkStaleDeleteAndEqualReplayAreNoops(t *testing.T) {
	destination := &Destination{flowID: "flow"}
	tx := &capturePolicyTx{}
	insert := connector.Record{Table: "widgets", Operation: connector.OpInsert, Key: []byte(`{"id":1}`), After: map[string]any{"id": int64(1), "name": "current", "updated_at": int64(20)}, SourcePosition: "0/20"}
	if err := applyTestWatermarkBatch(destination, tx, `"public"."widgets"`, watermarkSchema(), []connector.Record{insert}, watermarkPolicy()); err != nil {
		t.Fatal(err)
	}
	staleDelete := connector.Record{Table: "widgets", Operation: connector.OpDelete, Key: []byte(`{"id":1}`), Before: map[string]any{"id": int64(1), "updated_at": int64(10)}, SourcePosition: "0/30"}
	before := len(tx.queries)
	if err := applyTestWatermarkBatch(destination, tx, `"public"."widgets"`, watermarkSchema(), []connector.Record{staleDelete, insert}, watermarkPolicy()); err != nil {
		t.Fatal(err)
	}
	for _, query := range tx.queries[before:] {
		if strings.HasPrefix(query, "DELETE FROM") || strings.HasPrefix(query, "INSERT INTO \"public\".\"widgets\"") {
			t.Fatalf("stale/equal replay reached target: %s", query)
		}
	}
	if !containsQuery(tx.queries, "FOR UPDATE") {
		t.Fatal("existing watermark state was not locked for concurrent serialization")
	}
}

func TestWatermarkEqualPositionConflictAndGreaterPositionAdvance(t *testing.T) {
	destination := &Destination{flowID: "flow"}
	tx := &capturePolicyTx{}
	initial := connector.Record{Table: "widgets", Operation: connector.OpInsert, Key: []byte(`{"id":1}`), After: map[string]any{"id": int64(1), "name": "initial", "updated_at": int64(20)}, SourcePosition: "0/20"}
	if err := applyTestWatermarkBatch(destination, tx, `"public"."widgets"`, watermarkSchema(), []connector.Record{initial}, watermarkPolicy()); err != nil {
		t.Fatal(err)
	}
	conflict := initial
	conflict.After = map[string]any{"id": int64(1), "name": "different", "updated_at": int64(20)}
	if err := applyTestWatermarkBatch(destination, tx, `"public"."widgets"`, watermarkSchema(), []connector.Record{conflict}, watermarkPolicy()); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("equal identity conflict error=%v", err)
	}
	newerDelete := connector.Record{Table: "widgets", Operation: connector.OpDelete, Key: []byte(`{"id":1}`), Before: map[string]any{"id": int64(1), "updated_at": int64(20)}, SourcePosition: "0/28"}
	if err := applyTestWatermarkBatch(destination, tx, `"public"."widgets"`, watermarkSchema(), []connector.Record{newerDelete}, watermarkPolicy()); err != nil {
		t.Fatal(err)
	}
	for _, state := range tx.state {
		if !state.deleted || state.position != "0/28" {
			t.Fatalf("state=%+v, want newer-position tombstone", state)
		}
	}
}

func TestWatermarkStateIdentityIncludesProjectionAndOrderedKeyNames(t *testing.T) {
	base := watermarkStateIdentity{flowID: "flow", targetSchema: "public", targetTable: "widgets", projectionFingerprint: "p1", keyColumns: []string{"a", "b"}, keyValues: []string{"1", "2"}}
	tx := &capturePolicyTx{}
	if _, err := advanceWatermarkState(context.Background(), tx, base, "bigint", "1", "0/10", "one", false); err != nil {
		t.Fatal(err)
	}
	other := base
	other.projectionFingerprint = "p2"
	if _, err := advanceWatermarkState(context.Background(), tx, other, "bigint", "1", "0/10", "two", false); err != nil {
		t.Fatal(err)
	}
	reordered := base
	reordered.keyColumns = []string{"b", "a"}
	if _, err := advanceWatermarkState(context.Background(), tx, reordered, "bigint", "1", "0/10", "three", false); err != nil {
		t.Fatal(err)
	}
	if len(tx.state) != 3 {
		t.Fatalf("state identities=%d, want projection and ordered-key scopes", len(tx.state))
	}
}

func TestApplyWatermarkBatchRejectsInvalidInputBeforeStateOrTargetMutation(t *testing.T) {
	for name, testCase := range map[string]struct {
		schema connector.Schema
		record connector.Record
	}{
		"missing":               {watermarkSchema(), connector.Record{Operation: connector.OpInsert, Key: []byte(`{"id":1}`), After: map[string]any{"id": int64(1)}}},
		"nullable":              {connector.Schema{Name: "widgets", Columns: []connector.Column{{Name: "id", Type: "int8"}, {Name: "updated_at", Type: "int8", Nullable: true}}}, connector.Record{Operation: connector.OpInsert, Key: []byte(`{"id":1}`), After: map[string]any{"id": int64(1), "updated_at": int64(1)}}},
		"unsupported_watermark": {connector.Schema{Name: "widgets", Columns: []connector.Column{{Name: "id", Type: "int8"}, {Name: "updated_at", Type: "jsonb", TypeMetadata: map[string]string{"replica_identity": "true"}}}}, connector.Record{Operation: connector.OpInsert, Key: []byte(`{"id":1}`), After: map[string]any{"id": int64(1), "updated_at": `{}`}}},
		"unsupported_key":       {connector.Schema{Name: "widgets", Columns: []connector.Column{{Name: "id", Type: "jsonb"}, {Name: "updated_at", Type: "int8", TypeMetadata: map[string]string{"replica_identity": "true"}}}}, connector.Record{Operation: connector.OpInsert, Key: []byte(`{"id":{"x":1}}`), After: map[string]any{"id": map[string]any{"x": 1}, "updated_at": int64(1)}}},
	} {
		t.Run(name, func(t *testing.T) {
			tx := &capturePolicyTx{}
			err := (&Destination{flowID: "flow"}).applyWatermarkBatch(context.Background(), tx, `"widgets"`, testCase.schema, []connector.Record{testCase.record}, watermarkPolicy())
			if err == nil {
				t.Fatal("invalid watermark input was admitted")
			}
			if len(tx.queries) != 0 {
				t.Fatalf("queries executed before validation: %v", tx.queries)
			}
		})
	}
}

func containsQuery(queries []string, fragment string) bool {
	for _, query := range queries {
		if strings.Contains(query, fragment) {
			return true
		}
	}
	return false
}
