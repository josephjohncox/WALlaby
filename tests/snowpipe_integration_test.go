package tests

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/connectors/destinations/snowpipe"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestSnowpipeAutoIngestUpload(t *testing.T) {
	dsn := os.Getenv("WALLABY_TEST_SNOWPIPE_DSN")
	stage := os.Getenv("WALLABY_TEST_SNOWPIPE_STAGE")
	if dsn == "" {
		if usingFakesnow() {
			derived, _, ok := snowflakeTestDSN(t)
			if !ok {
				t.Skip("snowpipe DSN not configured")
			}
			dsn = derived
		}
	}
	if stage == "" && usingFakesnow() {
		stage = "@~"
	}
	if dsn == "" || stage == "" {
		t.Skip("WALLABY_TEST_SNOWPIPE_DSN or WALLABY_TEST_SNOWPIPE_STAGE not set")
	}
	if usingFakesnow() && !allowFakesnowSnowflake() {
		t.Skip("fakesnow enabled; set WALLABY_TEST_RUN_FAKESNOW=1 to run Snowpipe integration")
	}
	if usingFakesnow() {
		t.Skip("fakesnow does not support snowpipe PUT/COPY operations")
	}

	ctx := context.Background()
	dest := &snowpipe.Destination{}
	spec := connector.Spec{
		Name: "snowpipe-test",
		Type: connector.EndpointSnowpipe,
		Options: map[string]string{
			"dsn":                dsn,
			"stage":              stage,
			"format":             "json",
			"auto_ingest":        "true",
			"copy_on_write":      "false",
			"meta_table_enabled": "false",
		},
	}

	if err := dest.Open(ctx, spec); err != nil {
		if usingFakesnow() {
			t.Skipf("fakesnow open failed: %v", err)
		}
		t.Fatalf("open destination: %v", err)
	}
	defer dest.Close(ctx)

	schema := connector.Schema{Name: "snowpipe_events"}
	ddlRecord := connector.Record{
		Table:     "snowpipe_events",
		Operation: connector.OpDDL,
		DDL:       "CREATE TABLE snowpipe_events (id int, name text)",
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schema, ddlRecord); err != nil {
		t.Fatalf("apply ddl: %v", err)
	}
	record := connector.Record{
		Table:     "snowpipe_events",
		Operation: connector.OpInsert,
		After:     map[string]any{"id": 1, "name": "alpha"},
	}
	batch := connector.Batch{Records: []connector.Record{record}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "1"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("write batch: %v", err)
	}
}
