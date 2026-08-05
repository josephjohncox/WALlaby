package duckdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestAppendWriteAndDDLBehavior(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE main.events (event_id BIGINT)`); err != nil {
		t.Fatal(err)
	}
	destination := &Destination{db: db, metaEnabled: false, spec: connector.Spec{Type: connector.EndpointDuckDB}}
	plan, err := json.Marshal(internalschema.Plan{Changes: []internalschema.Change{{Type: internalschema.ChangeAddColumn, Namespace: "main", Table: "events", Column: "status", ToType: "text", Nullable: true}}})
	if err != nil {
		t.Fatal(err)
	}
	schema := connector.Schema{Namespace: "main", Name: "events", Columns: []connector.Column{{Name: "event_id", Type: "int8"}, {Name: "status", Type: "text", Nullable: true}}}
	if err := destination.ApplyDDL(context.Background(), schema, connector.Record{Operation: connector.OpDDL, DDLPlan: plan}); err != nil {
		t.Fatal(err)
	}
	batch := connector.Batch{Schema: schema, WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend}, Records: []connector.Record{{Table: "events", Operation: connector.OpInsert, After: map[string]any{"event_id": int64(7), "status": "ready"}}}}
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	var id int64
	var status string
	if err := db.QueryRow(`SELECT event_id,status FROM main.events`).Scan(&id, &status); err != nil {
		t.Fatal(err)
	}
	if id != 7 || status != "ready" {
		t.Fatalf("row=(%d,%q)", id, status)
	}
}
