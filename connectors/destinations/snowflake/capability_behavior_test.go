package snowflake

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestGenericAppendPlanAndDDLBehavior(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	destination := &Destination{db: db, disableTx: true, metaEnabled: false, spec: connector.Spec{Type: connector.EndpointSnowflake}}
	mock.ExpectExec(`INSERT INTO "MAPPED"\."EVENTS" \("EVENT_ID"\) VALUES \(\?\)`).WithArgs(int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
	batch := connector.Batch{
		Schema:      connector.Schema{Namespace: "MAPPED", Name: "EVENTS", Columns: []connector.Column{{Name: "EVENT_ID", Type: "int8"}}},
		WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend},
		Records:     []connector.Record{{Table: "EVENTS", Operation: connector.OpInsert, After: map[string]any{"EVENT_ID": int64(7)}}},
	}
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	plan, err := json.Marshal(internalschema.Plan{Changes: []internalschema.Change{{Type: internalschema.ChangeAddColumn, Namespace: "MAPPED", Table: "EVENTS", Column: "STATUS", ToType: "text", Nullable: false}}})
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec(`ALTER TABLE "MAPPED"\."EVENTS" ADD COLUMN "STATUS" STRING NOT NULL`).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := destination.ApplyDDL(context.Background(), batch.Schema, connector.Record{Operation: connector.OpDDL, DDLPlan: plan}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
