package tablemap

import (
	"encoding/json"
	"strings"
	"testing"

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
		Schema:     connector.Schema{Namespace: "public", Name: "widgets", Columns: []connector.Column{{Name: "id", Type: "bigint"}, {Name: "updated_at", Type: "text"}}},
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

func TestProjectDDLRejectsRawSQLForNonidentityProjection(t *testing.T) {
	t.Parallel()
	projector := testProjector(t, upsertMappings())
	batch := connector.Batch{
		Schema:     connector.Schema{Namespace: "public", Name: "widgets", Columns: []connector.Column{{Name: "id", Type: "bigint"}, {Name: "updated_at", Type: "text"}}},
		Records:    []connector.Record{{Table: "widgets", Operation: connector.OpDDL, DDL: "ALTER TABLE widgets ADD COLUMN extra text", SourcePosition: "0/20"}},
		Checkpoint: connector.Checkpoint{LSN: "0/20"},
	}
	if _, _, err := projector.ProjectBatch(batch); err == nil || !strings.Contains(err.Error(), "raw SQL DDL") {
		t.Fatalf("raw DDL error=%v", err)
	}
}
