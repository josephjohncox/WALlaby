package snowpipe

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/wire"
)

type fakeStagedTransport struct {
	statements []string
	failAt     int
	err        error
}

func (f *fakeStagedTransport) ExecContext(_ context.Context, statement string, _ ...any) (sql.Result, error) {
	f.statements = append(f.statements, statement)
	if f.failAt > 0 && len(f.statements) == f.failAt {
		return nil, f.err
	}
	return sqlmock.NewResult(0, 1), nil
}

func snowpipeAppendBatch() connector.Batch {
	return connector.Batch{
		Schema: connector.Schema{
			Name: "orders", Namespace: "public", Version: 1,
			Columns: []connector.Column{{Name: "id", Type: "int8"}},
		},
		Checkpoint:  connector.Checkpoint{LSN: "1"},
		WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend},
		Records: []connector.Record{{
			Table: "orders", Operation: connector.OpInsert,
			Key: []byte(`{"id":1}`), After: map[string]any{"id": int64(1)},
		}},
	}
}

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
	destination := &Destination{spec: connector.RuntimeSpec{Options: map[string]string{"schema": "legacy", "table": "legacy"}}}
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

func TestStagedTransportRunsPutCopyAndMetadataReceipt(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	transport := &fakeStagedTransport{}
	destination := &Destination{
		db: db, stagedTransport: transport, codec: &wire.JSONCodec{}, stage: "@stage", copyOnWrite: true,
		copyPattern: ".*", copyOnError: "continue", copyMatch: "case_insensitive",
		metaEnabled: true, metaSchema: "WALLABY_META", metaTable: "__METADATA", metaPKPrefix: "pk_", flowID: "flow-1",
		metaColumns: map[string]struct{}{
			"pk_id": {}, "registry_subject": {}, "registry_id": {}, "registry_version": {},
		},
	}
	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := destination.Write(context.Background(), snowpipeAppendBatch()); err != nil {
		t.Fatal(err)
	}
	if len(transport.statements) != 2 {
		t.Fatalf("staged statements=%q", transport.statements)
	}
	if !strings.HasPrefix(transport.statements[0], "PUT file://") || !strings.Contains(transport.statements[0], " @stage AUTO_COMPRESS=FALSE") {
		t.Fatalf("PUT statement=%q", transport.statements[0])
	}
	putFile := strings.TrimPrefix(strings.SplitN(transport.statements[0], " @stage", 2)[0], "PUT file://")
	uploadedName := filepath.Base(putFile)
	copyStatement := transport.statements[1]
	for _, fragment := range []string{
		`COPY INTO "public"."orders" FROM @stage FILES = ('`,
		"FILES = ('" + uploadedName + "')",
		"FILE_FORMAT = (TYPE = JSON)",
		"PATTERN = '.*'",
		"ON_ERROR = 'continue'",
		"MATCH_BY_COLUMN_NAME = 'case_insensitive'",
	} {
		if !strings.Contains(copyStatement, fragment) {
			t.Fatalf("COPY statement %q lacks %q", copyStatement, fragment)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStagedDeliveryErrorsRemainStagedErrors(t *testing.T) {
	tests := []struct {
		name       string
		failAt     int
		failure    string
		wantPrefix string
	}{
		{name: "put", failAt: 1, failure: "stage upload failed", wantPrefix: "put to stage: stage upload failed"},
		{name: "copy_after_put", failAt: 2, failure: "copy execution failed", wantPrefix: "copy into: copy execution failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := &fakeStagedTransport{failAt: test.failAt, err: errors.New(test.failure)}
			destination := &Destination{
				stagedTransport: transport, codec: &wire.JSONCodec{}, stage: "@stage", copyOnWrite: true,
			}
			err := destination.Write(context.Background(), snowpipeAppendBatch())
			if err == nil || err.Error() != test.wantPrefix {
				t.Fatalf("Write error=%v want=%q", err, test.wantPrefix)
			}
			if len(transport.statements) != test.failAt {
				t.Fatalf("statements=%q want count=%d", transport.statements, test.failAt)
			}
			assertTemporaryUploadRemoved(t, transport.statements[0])
			for _, statement := range transport.statements {
				if strings.Contains(strings.ToUpper(statement), "INSERT INTO") {
					t.Fatalf("unexpected row statement after staged failure: %q", statement)
				}
			}
		})
	}
}

func TestReceiptFailureAfterCopyReturnsReceiptError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	transport := &fakeStagedTransport{}
	destination := &Destination{
		db: db, stagedTransport: transport, codec: &wire.JSONCodec{}, stage: "@stage", copyOnWrite: true,
		metaEnabled: true, metaSchema: "WALLABY_META", metaTable: "__METADATA", metaPKPrefix: "pk_", flowID: "flow-1",
		metaColumns: map[string]struct{}{
			"pk_id": {}, "registry_subject": {}, "registry_id": {}, "registry_version": {},
		},
	}
	mock.ExpectBegin().WillReturnError(errors.New("receipt store unavailable"))

	err = destination.Write(context.Background(), snowpipeAppendBatch())
	if err == nil || err.Error() != "begin metadata transaction: receipt store unavailable" {
		t.Fatalf("Write error=%v", err)
	}
	if len(transport.statements) != 2 || !strings.HasPrefix(transport.statements[0], "PUT ") || !strings.HasPrefix(transport.statements[1], "COPY INTO ") {
		t.Fatalf("staged statements=%q", transport.statements)
	}
	assertTemporaryUploadRemoved(t, transport.statements[0])
	for _, statement := range transport.statements {
		if strings.Contains(strings.ToUpper(statement), "INSERT INTO") {
			t.Fatalf("unexpected row statement after receipt failure: %q", statement)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func assertTemporaryUploadRemoved(t *testing.T, putStatement string) {
	t.Helper()
	const prefix = "PUT file://"
	if !strings.HasPrefix(putStatement, prefix) {
		t.Fatalf("not a PUT statement: %q", putStatement)
	}
	localPath := strings.TrimPrefix(strings.SplitN(putStatement, " @", 2)[0], prefix)
	if _, err := os.Stat(localPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary upload %q was not removed after failure: %v", localPath, err)
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
	destination := &Destination{db: db, spec: connector.RuntimeSpec{Type: connector.EndpointSnowpipe}}
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
