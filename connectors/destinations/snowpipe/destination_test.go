package snowpipe

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/wire"
)

func TestCopyOptionsClause(t *testing.T) {
	purge := true
	dest := &Destination{
		fileFormat:  "MY_FORMAT",
		copyPattern: ".*",
		copyOnError: "continue",
		copyMatch:   "case_insensitive",
		copyPurge:   &purge,
	}

	got := dest.copyOptionsClause()
	expectContains(t, got, "FILE_FORMAT = (FORMAT_NAME = 'MY_FORMAT')")
	expectContains(t, got, "PATTERN = '.*'")
	expectContains(t, got, "ON_ERROR = 'continue'")
	expectContains(t, got, "MATCH_BY_COLUMN_NAME = 'case_insensitive'")
	expectContains(t, got, "PURGE = TRUE")
}

func TestMappedSchemaAndTableDotsRemainLiteral(t *testing.T) {
	destination := &Destination{spec: connector.Spec{Options: map[string]string{"schema": "legacy", "table": "legacy"}}}
	batch := connector.Batch{Schema: connector.Schema{Namespace: "sch.ma"}, Records: []connector.Record{{Table: "ta.ble"}}}
	if got := destination.targetTable(batch.Schema, batch.Records[0]); got != `"sch.ma"."ta.ble"` {
		t.Fatalf("target=%s", got)
	}
	if got := destination.resolveStage(batch); got != `@%"sch.ma"."ta.ble"` {
		t.Fatalf("stage=%s", got)
	}
}

func TestWriteIssuesPutAndCopy(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	purge := true
	dest := &Destination{
		db:          db,
		codec:       &wire.JSONCodec{},
		stage:       "@stage",
		copyOnWrite: true,
		copyPattern: ".*",
		copyOnError: "continue",
		copyMatch:   "case_insensitive",
		copyPurge:   &purge,
		fileFormat:  "MY_FORMAT",
	}

	mock.ExpectExec(regexp.QuoteMeta("PUT file://")).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("COPY INTO").WillReturnResult(sqlmock.NewResult(1, 1))

	batch := connector.Batch{
		Schema: connector.Schema{
			Name:      "orders",
			Namespace: "public",
			Version:   1,
			Columns: []connector.Column{
				{Name: "id", Type: "int8"},
			},
		},
		Checkpoint:  connector.Checkpoint{LSN: "1"},
		WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend},
		Records: []connector.Record{
			{Table: "public.orders", Operation: connector.OpInsert, After: map[string]any{"id": 1}},
		},
	}

	if err := dest.Write(context.Background(), batch); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestApplyDDLExecutesTranslatedStatement(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	plan, err := json.Marshal(internalschema.Plan{Changes: []internalschema.Change{{Type: internalschema.ChangeAddColumn, Namespace: "ANALYTICS", Table: "EVENTS", Column: "STATUS", ToType: "text", Nullable: false}}})
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec(`ALTER TABLE "ANALYTICS"\."EVENTS" ADD COLUMN "STATUS" STRING NOT NULL`).WillReturnResult(sqlmock.NewResult(0, 1))
	destination := &Destination{db: db, spec: connector.Spec{Type: connector.EndpointSnowpipe}}
	if err := destination.ApplyDDL(context.Background(), connector.Schema{Namespace: "ANALYTICS", Name: "EVENTS"}, connector.Record{Operation: connector.OpDDL, DDLPlan: plan}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteSkipsCopyWhenDisabled(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	dest := &Destination{
		db:          db,
		codec:       &wire.JSONCodec{},
		stage:       "@stage",
		copyOnWrite: false,
	}

	mock.ExpectExec(regexp.QuoteMeta("PUT file://")).WillReturnResult(sqlmock.NewResult(1, 1))

	batch := connector.Batch{
		Schema:      connector.Schema{Name: "orders", Namespace: "public"},
		Checkpoint:  connector.Checkpoint{LSN: "1"},
		WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend},
		Records: []connector.Record{
			{Table: "public.orders", Operation: connector.OpInsert, After: map[string]any{"id": 1}},
		},
	}

	if err := dest.Write(context.Background(), batch); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func expectContains(t *testing.T, value, substr string) {
	t.Helper()
	if value == "" || substr == "" {
		t.Fatalf("invalid input: value=%q substr=%q", value, substr)
	}
	if !strings.Contains(value, substr) {
		t.Fatalf("expected %q to contain %q", value, substr)
	}
}
