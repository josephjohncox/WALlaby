package tablemap

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestProjectStructuredDDLRenamesAndFiltersColumns(t *testing.T) {
	t.Parallel()
	projector := testProjector(t, upsertMappings())
	plan := schema.Plan{Changes: []schema.Change{
		{Type: schema.ChangeAddColumn, Namespace: "public", Table: "widgets", Column: "extra", ToType: "text"},
		{Type: schema.ChangeDropColumn, Namespace: "public", Table: "widgets", Column: "secret"},
		{Type: schema.ChangeRenameColumn, Namespace: "public", Table: "widgets", Column: "id", ToColumn: "customer_id"},
	}}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	batch := connector.Batch{
		Schema:     connector.Schema{Namespace: "public", Name: "widgets", Columns: []connector.Column{{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"replica_identity": "true"}}, {Name: "updated_at", Type: "text", TypeMetadata: map[string]string{"replica_identity": "true"}}}},
		Records:    []connector.Record{{Table: "widgets", Operation: connector.OpDDL, DDL: "ALTER TABLE widgets ...", DDLPlan: encoded, SourcePosition: "0/20"}},
		Checkpoint: connector.Checkpoint{LSN: "0/20"},
	}
	got, decision, err := projector.ProjectBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if decision != stream.ProjectionIncluded || len(got.Records) != 1 || got.Records[0].DDL != "" {
		t.Fatalf("projected DDL decision/record=%v/%+v", decision, got.Records)
	}
	var mapped schema.Plan
	if err := json.Unmarshal(got.Records[0].DDLPlan, &mapped); err != nil {
		t.Fatal(err)
	}
	if len(mapped.Changes) != 2 {
		t.Fatalf("mapped changes=%+v, want excluded secret change removed", mapped.Changes)
	}
	if mapped.Changes[0].Namespace != "analytics" || mapped.Changes[0].Table != "events" || mapped.Changes[0].Column != "dst_extra" {
		t.Fatalf("mapped add=%+v", mapped.Changes[0])
	}
	if mapped.Changes[1].Column != "event_id" || mapped.Changes[1].ToColumn != "dst_customer_id" {
		t.Fatalf("mapped rename=%+v", mapped.Changes[1])
	}
}

func TestProjectTablelessStructuredDDLByPlanRelation(t *testing.T) {
	projector := testProjector(t, upsertMappings())
	first, _ := json.Marshal(schema.Plan{Changes: []schema.Change{{Type: schema.ChangeAddColumn, Namespace: "public", Table: "widgets", Column: "extra", ToType: "text"}}})
	second, _ := json.Marshal(schema.Plan{Changes: []schema.Change{{Type: schema.ChangeAddColumn, Namespace: "other", Table: "new_table", Column: "note", ToType: "text"}}})
	batch := connector.Batch{Checkpoint: connector.Checkpoint{LSN: "0/20"}, Records: []connector.Record{
		{Operation: connector.OpDDL, DDLPlan: first, SourcePosition: "0/10"},
		{Operation: connector.OpDDL, DDLPlan: second, SourcePosition: "0/20"},
	}}
	got, decision, err := projector.ProjectBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if decision != stream.ProjectionIncluded || len(got.Records) != 2 {
		t.Fatalf("decision/records=%v/%d", decision, len(got.Records))
	}
	if err := connector.ValidateBatch(got); err != nil {
		t.Fatalf("projected tableless batch is invalid: %v", err)
	}
	var firstMapped, secondMapped schema.Plan
	_ = json.Unmarshal(got.Records[0].DDLPlan, &firstMapped)
	_ = json.Unmarshal(got.Records[1].DDLPlan, &secondMapped)
	if firstMapped.Changes[0].Namespace != "analytics" || firstMapped.Changes[0].Table != "events" {
		t.Fatalf("first mapping=%+v", firstMapped)
	}
	if secondMapped.Changes[0].Namespace != "other" || secondMapped.Changes[0].Table != "new_table" {
		t.Fatalf("second mapping=%+v", secondMapped)
	}
}

func TestProjectTablelessDDLRejectsAmbiguity(t *testing.T) {
	projector := testProjector(t, upsertMappings())
	multi, _ := json.Marshal(schema.Plan{Changes: []schema.Change{
		{Type: schema.ChangeAddColumn, Namespace: "public", Table: "widgets", Column: "a"},
		{Type: schema.ChangeAddColumn, Namespace: "public", Table: "other", Column: "b"},
	}})
	for name, record := range map[string]connector.Record{
		"raw":            {Operation: connector.OpDDL, DDL: "ALTER TABLE widgets ADD COLUMN a text"},
		"multi_relation": {Operation: connector.OpDDL, DDLPlan: multi},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := projector.ProjectBatch(connector.Batch{Records: []connector.Record{record}, Checkpoint: connector.Checkpoint{LSN: "0/10"}})
			if err == nil {
				t.Fatal("ambiguous tableless DDL was admitted")
			}
		})
	}
}

func TestAppendProjectionRemovesSourcePrimaryKeyDDL(t *testing.T) {
	mappings := flow.NewTableMappings([]connector.Spec{{Name: "sink", Type: connector.EndpointKafka}})
	projector := testProjector(t, mappings)
	plan, _ := json.Marshal(schema.Plan{Changes: []schema.Change{
		{Type: schema.ChangeCreateTable, Namespace: "public", Table: "events", PrimaryKeys: []string{"id"}},
		{Type: schema.ChangeAlterPrimaryKey, Namespace: "public", Table: "events", PrimaryKeys: []string{"id"}},
	}})
	batch := connector.Batch{Schema: connector.Schema{Namespace: "public", Name: "events", Columns: []connector.Column{{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"primary_key": "true"}}}}, Checkpoint: connector.Checkpoint{LSN: "0/20"}, Records: []connector.Record{{Table: "events", Operation: connector.OpDDL, DDLPlan: plan, SourcePosition: "0/20"}}}
	got, _, err := projector.ProjectBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	var projected schema.Plan
	if err := json.Unmarshal(got.Records[0].DDLPlan, &projected); err != nil {
		t.Fatal(err)
	}
	if len(projected.Changes) != 1 || len(projected.Changes[0].PrimaryKeys) != 0 {
		t.Fatalf("append DDL retained source uniqueness: %+v", projected)
	}
}

func TestProjectDDLRejectsRawSQLForNonidentityProjection(t *testing.T) {
	t.Parallel()
	projector := testProjector(t, upsertMappings())
	batch := connector.Batch{
		Schema:     connector.Schema{Namespace: "public", Name: "widgets", Columns: []connector.Column{{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"replica_identity": "true"}}, {Name: "updated_at", Type: "text", TypeMetadata: map[string]string{"replica_identity": "true"}}}},
		Records:    []connector.Record{{Table: "widgets", Operation: connector.OpDDL, DDL: "ALTER TABLE widgets ADD COLUMN extra text", SourcePosition: "0/20"}},
		Checkpoint: connector.Checkpoint{LSN: "0/20"},
	}
	if _, _, err := projector.ProjectBatch(batch); err == nil || !strings.Contains(err.Error(), "raw SQL DDL") {
		t.Fatalf("raw DDL error=%v", err)
	}
}
