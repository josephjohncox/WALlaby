package snowpipe

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
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
		writeMode:   writeModeAppend,
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
		Checkpoint: connector.Checkpoint{LSN: "1"},
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
		writeMode:   writeModeAppend,
	}

	mock.ExpectExec(regexp.QuoteMeta("PUT file://")).WillReturnResult(sqlmock.NewResult(1, 1))

	batch := connector.Batch{
		Schema:     connector.Schema{Name: "orders", Namespace: "public"},
		Checkpoint: connector.Checkpoint{LSN: "1"},
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
