package tablemap

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestProjectBatchRenamesFiltersKeysAndCarriesWritePolicy(t *testing.T) {
	t.Parallel()
	projector := testProjector(t, upsertMappings())
	batch := connector.Batch{
		Schema: connector.Schema{Namespace: "public", Name: "widgets", Version: 2, Columns: []connector.Column{
			{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"source_column_id": "1", "replica_identity": "true"}},
			{Name: "value", Type: "text"},
			{Name: "secret", Type: "text"},
			{Name: "updated_at", Type: "timestamptz", TypeMetadata: map[string]string{"replica_identity": "true"}},
		}},
		Records: []connector.Record{{
			Table: "widgets", Operation: connector.OpUpdate, Key: []byte(`{"id":1}`), Payload: []byte("opaque"),
			Before:    map[string]any{"id": float64(1), "updated_at": "2026-01-01", "secret": "old"},
			After:     map[string]any{"id": float64(1), "value": "new", "updated_at": "2026-01-02", "secret": "hidden"},
			Unchanged: []string{"secret"}, SourcePosition: "0/20",
		}},
		Checkpoint: connector.Checkpoint{LSN: "0/20"},
	}
	got, decision, err := projector.ProjectBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if decision != stream.ProjectionIncluded {
		t.Fatalf("decision=%v", decision)
	}
	if got.Schema.Namespace != "analytics" || got.Schema.Name != "events" {
		t.Fatalf("schema=%s.%s", got.Schema.Namespace, got.Schema.Name)
	}
	wantColumns := []string{"event_id", "dst_value", "dst_updated_at"}
	var columns []string
	for _, column := range got.Schema.Columns {
		columns = append(columns, column.Name)
	}
	if !reflect.DeepEqual(columns, wantColumns) {
		t.Fatalf("columns=%v, want %v", columns, wantColumns)
	}
	if got.Schema.Columns[0].TypeMetadata["source_column_id"] != "1" {
		t.Fatalf("source column identity was not preserved: %+v", got.Schema.Columns[0].TypeMetadata)
	}
	record := got.Records[0]
	if record.Table != "events" || len(record.Payload) != 0 || len(record.Unchanged) != 0 {
		t.Fatalf("record table/payload/unchanged=%q/%q/%v", record.Table, record.Payload, record.Unchanged)
	}
	if _, leaked := record.After["secret"]; leaked || record.After["dst_value"] != "new" {
		t.Fatalf("projected after=%v", record.After)
	}
	var key map[string]any
	if err := json.Unmarshal(record.Key, &key); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(key, map[string]any{"event_id": float64(1)}) {
		t.Fatalf("key=%v", key)
	}
	if got.WritePolicy.Mode != connector.ResolvedWriteUpsert || !reflect.DeepEqual(got.WritePolicy.KeyColumns, []string{"event_id"}) || got.WritePolicy.WatermarkColumn != "dst_updated_at" || got.WritePolicy.ProjectionFingerprint == "" {
		t.Fatalf("write policy=%+v", got.WritePolicy)
	}
	firstHash, err := connector.BatchContentHash(got)
	if err != nil {
		t.Fatal(err)
	}
	again, _, err := projector.ProjectBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := connector.BatchContentHash(again)
	if err != nil || firstHash != secondHash {
		t.Fatalf("projected hashes %q/%q err=%v", firstHash, secondHash, err)
	}
}

func TestAppendProjectionStripsSourceIdentityAndPreservesRepeatedKeys(t *testing.T) {
	mappings := flow.NewTableMappings([]connector.Spec{{Name: "sink", Type: connector.EndpointKafka}})
	projector := testProjector(t, mappings)
	batch := connector.Batch{Schema: connector.Schema{Namespace: "public", Name: "events", Columns: []connector.Column{{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"primary_key": "true", "primary_key_ordinal": "1", "replica_identity": "true"}}}}, Checkpoint: connector.Checkpoint{LSN: "0/20"}, Records: []connector.Record{
		{Table: "events", Operation: connector.OpInsert, Key: []byte(`{"id":1}`), After: map[string]any{"id": 1}, SourcePosition: "0/18"},
		{Table: "events", Operation: connector.OpUpdate, Key: []byte(`{"id":1}`), After: map[string]any{"id": 1}, SourcePosition: "0/20"},
	}}
	got, _, err := projector.ProjectBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Records) != 2 {
		t.Fatalf("repeated-key append records=%d", len(got.Records))
	}
	metadata := got.Schema.Columns[0].TypeMetadata
	for _, key := range []string{"primary_key", "primary_key_ordinal", "replica_identity"} {
		if metadata[key] != "" {
			t.Fatalf("append schema retained %s metadata: %v", key, metadata)
		}
	}
}

func TestAppendProjectionUsesStableSourcePositionAndDeleteImage(t *testing.T) {
	t.Parallel()
	mappings := flow.NewTableMappings([]connector.Spec{{Name: "sink", Type: connector.EndpointKafka}})
	projector := testProjector(t, mappings)
	batch := connector.Batch{
		Schema:     connector.Schema{Namespace: "public", Name: "logs", Columns: []connector.Column{{Name: "message", Type: "text"}}},
		Records:    []connector.Record{{Table: "logs", Operation: connector.OpDelete, Before: map[string]any{"message": "gone"}}},
		Checkpoint: connector.Checkpoint{LSN: "0/44"},
	}
	got, decision, err := projector.ProjectBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if decision != stream.ProjectionIncluded || got.Records[0].Operation != connector.OpInsert {
		t.Fatalf("decision/operation=%v/%s", decision, got.Records[0].Operation)
	}
	after := got.Records[0].After
	if after["message"] != "gone" || after[connector.AppendOperationColumn] != "delete" || after[connector.AppendDeletedColumn] != true || after[connector.AppendSourcePositionColumn] != "0/44" {
		t.Fatalf("append image=%v", after)
	}
	batch.Checkpoint.LSN = ""
	batch.Checkpoint.Metadata = map[string]string{"mode": "backfill", "table": "public.logs", "cursor": "42"}
	snapshot, _, err := projector.ProjectBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	snapshotPosition, ok := snapshot.Records[0].After[connector.AppendSourcePositionColumn].(string)
	if !ok || snapshotPosition == "" || strings.Contains(snapshotPosition, "/") {
		t.Fatalf("snapshot position=%v", snapshot.Records[0].After[connector.AppendSourcePositionColumn])
	}
	again, _, err := projector.ProjectBatch(batch)
	if err != nil || again.Records[0].After[connector.AppendSourcePositionColumn] != snapshotPosition {
		t.Fatalf("unstable snapshot position=%v error=%v", again.Records[0].After[connector.AppendSourcePositionColumn], err)
	}
	batch.Checkpoint.Metadata = nil
	if _, _, err := projector.ProjectBatch(batch); err == nil || !strings.Contains(err.Error(), "stable source position") {
		t.Fatalf("missing position error=%v", err)
	}
}

func TestFutureTemplatesRejectCrossVariablesInBothComponents(t *testing.T) {
	t.Parallel()
	mappings := flow.NewTableMappings([]connector.Spec{{Name: "sink", Type: connector.EndpointPostgres}})
	mappings.Destinations[0].FutureTables.TargetSchema = "{schema}_{table}"
	mappings.Destinations[0].FutureTables.TargetTable = "{schema}_{table}"
	if err := mappings.Validate([]connector.Spec{{Name: "sink", Type: connector.EndpointPostgres}}); err == nil || !strings.Contains(err.Error(), "placeholders other than {schema}") {
		t.Fatalf("cross-variable future templates error=%v", err)
	}
}

func TestFutureTemplatesKeepSchemaAndTableComponentsDistinct(t *testing.T) {
	t.Parallel()
	mappings := flow.NewTableMappings([]connector.Spec{{Name: "sink", Type: connector.EndpointPostgres}})
	mappings.Destinations[0].FutureTables.TargetSchema = "raw_{schema}"
	mappings.Destinations[0].FutureTables.TargetTable = "tbl_{table}"
	mappings.Destinations[0].FutureTables.FutureColumns.TargetColumn = "dst_{column}"
	projector := testProjector(t, mappings)
	project := func(namespace, table string) connector.Batch {
		batch, _, err := projector.ProjectBatch(connector.Batch{
			Schema:     connector.Schema{Namespace: namespace, Name: table, Columns: []connector.Column{{Name: "id", Type: "bigint"}}},
			Records:    []connector.Record{{Table: table, Operation: connector.OpInsert, After: map[string]any{"id": 1}}},
			Checkpoint: connector.Checkpoint{LSN: "0/20"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return batch
	}
	first := project("a", "b_c")
	second := project("a_b", "c")
	if first.Schema.Namespace != "raw_a" || first.Schema.Name != "tbl_b_c" || second.Schema.Namespace != "raw_a_b" || second.Schema.Name != "tbl_c" {
		t.Fatalf("future targets a.b_c=%s.%s a_b.c=%s.%s", first.Schema.Namespace, first.Schema.Name, second.Schema.Namespace, second.Schema.Name)
	}
	if first.Schema.Columns[0].Name != "dst_id" || second.Schema.Columns[0].Name != "dst_id" {
		t.Fatalf("future target columns first=%s second=%s", first.Schema.Columns[0].Name, second.Schema.Columns[0].Name)
	}
	if first.Schema.Namespace == second.Schema.Namespace && first.Schema.Name == second.Schema.Name {
		t.Fatal("a.b_c and a_b.c collapsed onto one future target")
	}
}

func TestProjectTransactionRenumbersFilteredFragmentsContiguously(t *testing.T) {
	t.Parallel()
	mappings := upsertMappings()
	mappings.Destinations[0].Tables = append(mappings.Destinations[0].Tables, flow.TableMapping{SourceSchema: "public", SourceTable: "ignored", Action: flow.MappingActionExclude})
	projector := testProjector(t, mappings)
	transaction := connector.SourceTransaction{
		SourceLineageID: "lineage", TransactionID: 7, BeginLSN: "0/10", CommitLSN: "0/30", EndLSN: "0/38", Checkpoint: connector.Checkpoint{LSN: "0/38"},
		Fragments: []connector.TransactionFragment{
			{Ordinal: 0, Batch: connector.Batch{Schema: connector.Schema{Namespace: "public", Name: "ignored", Columns: []connector.Column{{Name: "id", Type: "bigint"}}}, Records: []connector.Record{{Table: "ignored", Operation: connector.OpInsert, After: map[string]any{"id": 1}}}}},
			{Ordinal: 1, Batch: connector.Batch{Schema: connector.Schema{Namespace: "public", Name: "widgets", Columns: []connector.Column{{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"replica_identity": "true"}}, {Name: "updated_at", Type: "text", TypeMetadata: map[string]string{"replica_identity": "true"}}}}, Records: []connector.Record{{Table: "widgets", Operation: connector.OpInsert, After: map[string]any{"id": 1, "updated_at": "new"}}}}},
		},
	}
	got, decision, err := projector.ProjectTransaction(transaction)
	if err != nil {
		t.Fatal(err)
	}
	if decision != stream.ProjectionIncluded || len(got.Fragments) != 1 || got.Fragments[0].Ordinal != 0 {
		t.Fatalf("projected fragments=%+v decision=%v", got.Fragments, decision)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("projected transaction invalid: %v", err)
	}
	if _, _, err := connector.SourceTransactionIdentity(got); err != nil {
		t.Fatalf("projected identity: %v", err)
	}
}

func TestUpsertKeyChangeEmitsDeleteThenInsert(t *testing.T) {
	t.Parallel()
	projector := testProjector(t, upsertMappings())
	batch := connector.Batch{
		Schema:     connector.Schema{Namespace: "public", Name: "widgets", Columns: []connector.Column{{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"replica_identity": "true"}}, {Name: "updated_at", Type: "text", TypeMetadata: map[string]string{"replica_identity": "true"}}}},
		Records:    []connector.Record{{Table: "widgets", Operation: connector.OpUpdate, Key: []byte(`{"id":1}`), Before: map[string]any{"id": float64(1), "updated_at": "old"}, After: map[string]any{"id": float64(2), "updated_at": "new"}}},
		Checkpoint: connector.Checkpoint{LSN: "0/20"},
	}
	got, _, err := projector.ProjectBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Records) != 2 || got.Records[0].Operation != connector.OpDelete || got.Records[1].Operation != connector.OpInsert {
		t.Fatalf("key-change operations=%+v", got.Records)
	}
	var oldKey, newKey map[string]any
	_ = json.Unmarshal(got.Records[0].Key, &oldKey)
	_ = json.Unmarshal(got.Records[1].Key, &newKey)
	if oldKey["event_id"] != float64(1) || newKey["event_id"] != float64(2) {
		t.Fatalf("key-change keys old=%v new=%v", oldKey, newKey)
	}
}

func TestConfiguredKeyUpdateRejectsMissingOldMatchKey(t *testing.T) {
	t.Parallel()
	projector := testProjector(t, upsertMappings())
	batch := connector.Batch{
		Schema:     connector.Schema{Namespace: "public", Name: "widgets", Columns: []connector.Column{{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"replica_identity": "true"}}, {Name: "updated_at", Type: "text", TypeMetadata: map[string]string{"replica_identity": "true"}}}},
		Records:    []connector.Record{{Table: "widgets", Operation: connector.OpUpdate, Before: map[string]any{"updated_at": "old"}, After: map[string]any{"id": 2, "updated_at": "new"}}},
		Checkpoint: connector.Checkpoint{LSN: "0/20"},
	}
	if _, _, err := projector.ProjectBatch(batch); err == nil || !strings.Contains(err.Error(), "cannot reconstruct old match column") {
		t.Fatalf("missing old configured key error=%v", err)
	}
}

func TestConfiguredKeyColumnsAreAuthoritativeAndOrdered(t *testing.T) {
	t.Parallel()
	mappings := upsertMappings()
	mappings.Destinations[0].Tables[0].Write.KeyColumns = []string{"second", "id"}
	mappings.Destinations[0].Tables[0].Columns = append(mappings.Destinations[0].Tables[0].Columns,
		flow.ColumnMapping{SourceColumn: "second", Action: flow.MappingActionInclude, TargetColumn: "second_key"})
	schema := connector.Schema{Namespace: "public", Name: "widgets", Columns: []connector.Column{
		{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"replica_identity": "true"}},
		{Name: "second", Type: "bigint", TypeMetadata: map[string]string{"replica_identity": "true"}},
		{Name: "updated_at", Type: "text", TypeMetadata: map[string]string{"replica_identity": "true"}},
	}}
	batch := connector.Batch{Schema: schema, Records: []connector.Record{{Table: "widgets", Operation: connector.OpInsert, After: map[string]any{"id": 1, "second": 2, "updated_at": "now"}}}, Checkpoint: connector.Checkpoint{LSN: "0/20"}}
	got, _, err := testProjector(t, mappings).ProjectBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.WritePolicy.KeyColumns, []string{"second_key", "event_id"}) {
		t.Fatalf("resolved configured key order=%v", got.WritePolicy.KeyColumns)
	}
	reordered := mappings.Clone()
	reordered.Destinations[0].Tables[0].Write.KeyColumns = []string{"id", "second"}
	got, _, err = testProjector(t, reordered).ProjectBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.WritePolicy.KeyColumns, []string{"event_id", "second_key"}) {
		t.Fatalf("reordered configured keys=%v", got.WritePolicy.KeyColumns)
	}
}

func TestExplicitNaturalKeyAndFullReplicaIdentityDoNotInferTargetUniqueness(t *testing.T) {
	mappings := upsertMappings()
	mappings.Destinations[0].Tables[0].Write.KeyColumns = []string{"email"}
	mappings.Destinations[0].Tables[0].Columns = append(mappings.Destinations[0].Tables[0].Columns, flow.ColumnMapping{SourceColumn: "email", Action: flow.MappingActionInclude, TargetColumn: "natural_email"})
	schema := connector.Schema{Namespace: "public", Name: "widgets", Columns: []connector.Column{
		{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"primary_key": "true", "replica_identity": "true"}},
		{Name: "email", Type: "text", TypeMetadata: map[string]string{"replica_identity": "true"}},
		{Name: "updated_at", Type: "text", TypeMetadata: map[string]string{"replica_identity": "true"}},
	}}
	batch := connector.Batch{Schema: schema, Records: []connector.Record{{Table: "widgets", Operation: connector.OpInsert, After: map[string]any{"id": 1, "email": "a@example.com", "updated_at": "now"}}}, Checkpoint: connector.Checkpoint{LSN: "0/20"}}
	got, _, err := testProjector(t, mappings).ProjectBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.WritePolicy.KeyColumns, []string{"natural_email"}) {
		t.Fatalf("full identity changed authoritative natural key policy: %v", got.WritePolicy.KeyColumns)
	}
}

func TestProjectionRejectsUpsertKeyOutsideReplicaIdentity(t *testing.T) {
	projector := testProjector(t, upsertMappings())
	batch := connector.Batch{Schema: connector.Schema{Namespace: "public", Name: "widgets", Columns: []connector.Column{{Name: "id", Type: "bigint"}, {Name: "updated_at", Type: "text", TypeMetadata: map[string]string{"replica_identity": "true"}}}}, Checkpoint: connector.Checkpoint{LSN: "0/20"}, Records: []connector.Record{{Table: "widgets", Operation: connector.OpDelete, Before: map[string]any{"id": 1, "updated_at": "now"}}}}
	if _, _, err := projector.ProjectBatch(batch); err == nil || !strings.Contains(err.Error(), "upsert key column") {
		t.Fatalf("upsert key old-image error=%v", err)
	}
}

func TestProjectionRejectsWatermarkOutsideReplicaIdentity(t *testing.T) {
	projector := testProjector(t, upsertMappings())
	batch := connector.Batch{Schema: connector.Schema{Namespace: "public", Name: "widgets", Columns: []connector.Column{{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"replica_identity": "true"}}, {Name: "updated_at", Type: "text"}}}, Checkpoint: connector.Checkpoint{LSN: "0/20"}, Records: []connector.Record{{Table: "widgets", Operation: connector.OpDelete, Before: map[string]any{"id": 1, "updated_at": "now"}}}}
	if _, _, err := projector.ProjectBatch(batch); err == nil || !strings.Contains(err.Error(), "replica identity") {
		t.Fatalf("replica identity error=%v", err)
	}
}

func TestProjectionRejectsMissingWatermarkSchemaColumn(t *testing.T) {
	t.Parallel()
	projector := testProjector(t, upsertMappings())
	batch := connector.Batch{
		Schema:     connector.Schema{Namespace: "public", Name: "widgets", Columns: []connector.Column{{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"replica_identity": "true"}}}},
		Records:    []connector.Record{{Table: "widgets", Operation: connector.OpInsert, After: map[string]any{"id": 1}}},
		Checkpoint: connector.Checkpoint{LSN: "0/20"},
	}
	if _, _, err := projector.ProjectBatch(batch); err == nil || !strings.Contains(err.Error(), "watermark column") {
		t.Fatalf("missing watermark schema error=%v", err)
	}
}

func TestProjectorOwnsImmutableMappingCopy(t *testing.T) {
	t.Parallel()
	mappings := upsertMappings()
	projector := testProjector(t, mappings)
	mappings.Destinations[0].Tables[0].TargetTable = "mutated"
	mappings.Destinations[0].Tables[0].Columns[0].TargetColumn = "mutated_id"
	batch := connector.Batch{
		Schema:     connector.Schema{Namespace: "public", Name: "widgets", Columns: []connector.Column{{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"replica_identity": "true"}}, {Name: "updated_at", Type: "text", TypeMetadata: map[string]string{"replica_identity": "true"}}}},
		Records:    []connector.Record{{Table: "widgets", Operation: connector.OpInsert, After: map[string]any{"id": 1, "updated_at": "now"}}},
		Checkpoint: connector.Checkpoint{LSN: "0/20"},
	}
	got, _, err := projector.ProjectBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if got.Schema.Name != "events" || got.Schema.Columns[0].Name != "event_id" {
		t.Fatalf("projector mapping was mutated: schema=%s column=%s", got.Schema.Name, got.Schema.Columns[0].Name)
	}
}

func TestProjectionRejectsGeneratedExpressionRewrite(t *testing.T) {
	t.Parallel()
	projector := testProjector(t, upsertMappings())
	batch := connector.Batch{Schema: connector.Schema{Namespace: "public", Name: "widgets", Columns: []connector.Column{
		{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"replica_identity": "true"}}, {Name: "computed", Type: "text", Generated: true, Expression: "id::text"}, {Name: "updated_at", Type: "text", TypeMetadata: map[string]string{"replica_identity": "true"}},
	}}, Records: []connector.Record{{Table: "widgets", Operation: connector.OpInsert, After: map[string]any{"id": 1, "updated_at": "now"}}}, Checkpoint: connector.Checkpoint{LSN: "0/20"}}
	if _, _, err := projector.ProjectBatch(batch); err == nil || !strings.Contains(err.Error(), "generated column") {
		t.Fatalf("generated expression error=%v", err)
	}
}

func TestProjectionMatchesCaseAndWhitespaceDistinctSourceIdentifiersExactly(t *testing.T) {
	t.Parallel()
	mappings := flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: []flow.DestinationTableMappings{{
		Destination: "sink", FutureTables: flow.FutureTableMapping{Action: flow.MappingActionExclude},
		Tables: []flow.TableMapping{
			{SourceSchema: "Exact Schema", SourceTable: "Events", Action: flow.MappingActionInclude, TargetSchema: "public", TargetTable: "upper_events", FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{column}"}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeAppend}},
			{SourceSchema: "Exact Schema", SourceTable: "events", Action: flow.MappingActionInclude, TargetSchema: "public", TargetTable: "lower_events", FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{column}"}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeAppend}},
			{SourceSchema: "Exact Schema", SourceTable: " ", Action: flow.MappingActionInclude, TargetSchema: "public", TargetTable: " ", FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{column}"}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeAppend}},
		},
	}}}
	projector := testProjector(t, mappings)
	for source, target := range map[string]string{"Events": "upper_events", "events": "lower_events", " ": " "} {
		column := "id"
		if source == " " {
			column = " "
		}
		batch := connector.Batch{
			Schema:     connector.Schema{Namespace: "Exact Schema", Name: source, Columns: []connector.Column{{Name: column, Type: "bigint"}}},
			Records:    []connector.Record{{Table: source, Operation: connector.OpInsert, After: map[string]any{column: 1}}},
			Checkpoint: connector.Checkpoint{LSN: "0/20"},
		}
		projected, decision, err := projector.ProjectBatch(batch)
		if err != nil {
			t.Fatalf("project exact source %q: %v", source, err)
		}
		projectedColumn := false
		for _, candidate := range projected.Schema.Columns {
			if candidate.Name == column {
				projectedColumn = true
			}
		}
		if decision != stream.ProjectionIncluded || projected.Schema.Name != target || !projectedColumn || len(projected.Records) != 1 || projected.Records[0].Table != target {
			t.Fatalf("exact source %q projected to decision/schema/records=%v/%+v/%+v, want table %q with column %q", source, decision, projected.Schema, projected.Records, target, column)
		}
	}
}

func testProjector(t *testing.T, mappings flow.TableMappings) *Projector {
	t.Helper()
	destination := connector.Spec{Name: "sink", Type: connector.EndpointPostgres}
	if err := mappings.Validate([]connector.Spec{destination}); err != nil {
		t.Fatalf("validate mappings: %v", err)
	}
	projector, err := New(mappings, "sink")
	if err != nil {
		t.Fatal(err)
	}
	return projector
}

func upsertMappings() flow.TableMappings {
	return flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: []flow.DestinationTableMappings{{
		Destination:  "sink",
		FutureTables: flow.FutureTableMapping{Action: flow.MappingActionInclude, TargetSchema: "{schema}", TargetTable: "{table}", FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{column}"}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeAppend}},
		Tables: []flow.TableMapping{{
			SourceSchema: "public", SourceTable: "widgets", Action: flow.MappingActionInclude, TargetSchema: "analytics", TargetTable: "events",
			FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "dst_{column}"},
			Columns:       []flow.ColumnMapping{{SourceColumn: "id", Action: flow.MappingActionInclude, TargetColumn: "event_id"}, {SourceColumn: "secret", Action: flow.MappingActionExclude}},
			Write:         flow.TableWritePolicy{Mode: flow.TableWriteModeUpsert, KeyColumns: []string{"id"}, WatermarkColumn: "updated_at"},
		}},
	}}}
}
