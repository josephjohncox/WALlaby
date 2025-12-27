package tests

import (
	"context"
	"os"
	"testing"

	"github.com/josephjohncox/wallaby/connectors/destinations/snowpipe"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestSnowpipeAutoIngestUpload(t *testing.T) {
	dsn := os.Getenv("WALLABY_TEST_SNOWPIPE_DSN")
	stage := os.Getenv("WALLABY_TEST_SNOWPIPE_STAGE")
	if dsn == "" || stage == "" {
		t.Skip("WALLABY_TEST_SNOWPIPE_DSN or WALLABY_TEST_SNOWPIPE_STAGE not set")
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
		t.Fatalf("open destination: %v", err)
	}
	defer dest.Close(ctx)

	schema := connector.Schema{Name: "snowpipe_events"}
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
