package snowflake

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/snowflakedb/gosnowflake/v2"
)

type stagedStreamTestListing struct {
	md5  string
	read bool
}

func (*stagedStreamTestListing) Columns() []string { return []string{"name", "size", "md5"} }
func (*stagedStreamTestListing) Close() error      { return nil }
func (r *stagedStreamTestListing) Next(values []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	values[0], values[1], values[2] = "stage/batch/file.ndjson", int64(4), r.md5
	return nil
}

func TestStagedSkippedUploadDistinguishesUnknownFromConflict(t *testing.T) {
	for _, test := range []struct {
		name, md5 string
		want      error
	}{
		{name: "visible same bytes", md5: "expected"},
		{name: "visible different bytes", md5: "different", want: errStagedWrongByteCollision},
		{name: "not observable", want: connector.ErrDeliveryIndeterminate},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection := &stagedStreamTestConnection{listedMD5: test.md5, uploadErr: &gosnowflake.SnowflakeError{Number: gosnowflake.ErrUploadStreamSkipped}}
			db := sql.OpenDB(stagedStreamTestConnector{connection: connection})
			t.Cleanup(func() { _ = db.Close() })
			err := newSQLStageProtocol(db).PutObject(context.Background(), `"DB"."SCHEMA"."STAGE"`, "batch/file.ndjson", []byte("data"), "expected")
			if (test.want == nil && err != nil) || (test.want != nil && !errors.Is(err, test.want)) {
				t.Fatalf("err=%v want=%v", err, test.want)
			}
		})
	}
}

func TestStagedLargeUploadDoesNotRequestPlaintextSpill(t *testing.T) {
	connection := &stagedStreamTestConnection{}
	db := sql.OpenDB(stagedStreamTestConnector{connection: connection})
	t.Cleanup(func() { _ = db.Close() })
	content := bytes.Repeat([]byte{'x'}, (8<<20)+1)
	if err := newSQLStageProtocol(db).PutObject(context.Background(), `"DB"."SCHEMA"."STAGE"`, "batch/file.ndjson", content, "md5"); err != nil {
		t.Fatal(err)
	}
	if connection.upload.MaxBuffer != gosnowflake.MaxBufferUnbounded || len(connection.content) != len(content) {
		t.Fatal("bounded plan was redirected to a plaintext spill")
	}
}
