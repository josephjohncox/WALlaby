package snowflake

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSQLStageProtocolRefreshPipeUsesBatchPrefix(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	statement := `ALTER PIPE "DB"."PUBLIC"."PIPE" REFRESH PREFIX = 'wallaby_staged_append_v1/incarnation/batch/'`
	mock.ExpectExec(regexp.QuoteMeta(statement)).WillReturnResult(sqlmock.NewResult(0, 1))
	proto := newSQLStageProtocol(db)
	if err := proto.RefreshPipe(context.Background(), `"DB"."PUBLIC"."PIPE"`, "wallaby_staged_append_v1/incarnation/batch/file.ndjson"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
