package snowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/snowflakedb/gosnowflake/v2"
)

// Only the native methods under test are implemented. Embedding the remaining
// interface keeps this fake independent of unrelated Snowflake monitoring APIs.
type stagedStreamTestConnection struct {
	gosnowflake.SnowflakeConnection
	content   string
	stage     string
	path      string
	upload    gosnowflake.UploadStreamConfig
	closed    bool
	downloads int
	uploadErr error
	listedMD5 string
	listCalls int
}

func (c *stagedStreamTestConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepared statement")
}
func (c *stagedStreamTestConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction")
}
func (*stagedStreamTestConnection) Close() error { return nil }
func (c *stagedStreamTestConnection) DownloadStream(_ context.Context, stage, path string) (io.ReadCloser, error) {
	c.stage, c.path = stage, path
	c.downloads++
	c.closed = false
	return &stagedStreamTestReader{Reader: strings.NewReader(c.content), connection: c}, nil
}
func (c *stagedStreamTestConnection) UploadStreamWithConfig(_ context.Context, stage, file string, source io.Reader, cfg gosnowflake.UploadStreamConfig) error {
	c.stage, c.path, c.upload = stage, file, cfg
	content, err := io.ReadAll(source)
	if err != nil {
		return err
	}
	c.content = string(content)
	return c.uploadErr
}
func (c *stagedStreamTestConnection) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.HasPrefix(query, "LIST @") {
		return nil, errors.New("unexpected query: " + query)
	}
	c.listCalls++
	if c.listCalls > 1 && c.listedMD5 != "" {
		return &stagedStreamTestListing{md5: c.listedMD5}, nil
	}
	return stagedStreamEmptyListing{}, nil
}

type stagedStreamTestReader struct {
	io.Reader
	connection *stagedStreamTestConnection
}

func (r *stagedStreamTestReader) Close() error { r.connection.closed = true; return nil }

type stagedStreamEmptyListing struct{}

func (stagedStreamEmptyListing) Columns() []string         { return []string{"name", "size", "md5"} }
func (stagedStreamEmptyListing) Close() error              { return nil }
func (stagedStreamEmptyListing) Next([]driver.Value) error { return io.EOF }

type stagedStreamTestConnector struct{ connection *stagedStreamTestConnection }

func (c stagedStreamTestConnector) Connect(context.Context) (driver.Conn, error) {
	return c.connection, nil
}
func (c stagedStreamTestConnector) Driver() driver.Driver { return stagedStreamTestDriver(c) }

type stagedStreamTestDriver stagedStreamTestConnector

func (d stagedStreamTestDriver) Open(string) (driver.Conn, error) { return d.connection, nil }

func TestStagedNativeStreamingTransferPreservesBoundsAndImmutableNames(t *testing.T) {
	t.Parallel()
	const stage = `"DB$1"."SCHEMA$2"."STAGE$3"`
	const path = "wallaby_staged_append_v1/batch/content.ndjson"
	connection := &stagedStreamTestConnection{content: "test"}
	db := sql.OpenDB(stagedStreamTestConnector{connection: connection})
	t.Cleanup(func() { _ = db.Close() })
	protocol := newSQLStageProtocol(db)

	content, err := protocol.GetObject(context.Background(), stage, path, 4)
	if err != nil || string(content) != "test" || !connection.closed || connection.stage != "@"+stage || connection.path != path {
		t.Fatalf("download content=%q stage=%q path=%q closed=%v err=%v", content, connection.stage, connection.path, connection.closed, err)
	}
	if _, err := protocol.GetObject(context.Background(), stage, path, 3); !errors.Is(err, errStagedPlaintextOversize) || !connection.closed {
		t.Fatalf("oversize streaming download err=%v closed=%v", err, connection.closed)
	}
	if err := protocol.PutObject(context.Background(), stage, path, []byte("new content"), "md5"); err != nil {
		t.Fatal(err)
	}
	cfg := connection.upload
	if connection.stage != "@"+stage || connection.path != "content.ndjson" || cfg.DestPrefix != "wallaby_staged_append_v1/batch" || cfg.Compression != gosnowflake.CompressionNone || cfg.Overwrite != gosnowflake.ConfigBoolFalse || cfg.MaxBuffer != gosnowflake.MaxBufferUnbounded || connection.content != "new content" {
		t.Fatalf("upload changed immutable object name, bytes or configuration: stage=%q file=%q cfg=%+v", connection.stage, connection.path, cfg)
	}
}

func TestStagedNativeStreamingUploadReturnsErrors(t *testing.T) {
	t.Parallel()
	failure := errors.New("upload failed")
	connection := &stagedStreamTestConnection{uploadErr: failure}
	db := sql.OpenDB(stagedStreamTestConnector{connection: connection})
	t.Cleanup(func() { _ = db.Close() })
	err := newSQLStageProtocol(db).PutObject(context.Background(), `"DB"."SCHEMA"."STAGE"`, "batch/file.ndjson", []byte("data"), "md5")
	if !errors.Is(err, failure) {
		t.Fatalf("upload error=%v", err)
	}
}
