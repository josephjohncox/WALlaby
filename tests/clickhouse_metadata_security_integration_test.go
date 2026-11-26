package tests

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	chgo "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	chdest "github.com/josephjohncox/wallaby/connectors/destinations/clickhouse"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"google.golang.org/protobuf/proto"
)

func clickHouseTypedMetadataSpec(t *testing.T, dsn, database, table, engine, orderBy string) connector.RuntimeSpec {
	t.Helper()
	endpoint := &wallabypb.Endpoint{Name: "typed-metadata", Config: &wallabypb.Endpoint_Clickhouse{
		Clickhouse: &wallabypb.ClickHouseDestinationConfig{Dsn: dsn, Metadata: &wallabypb.ClickHouseMetadataConfig{
			Enabled: proto.Bool(true), Schema: database, Table: table, Engine: engine, OrderBy: orderBy,
		}},
	}}
	spec, err := endpointcodec.Decode(endpoint, endpointcodec.RoleDestination)
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

func TestClickHouseTypedMetadataRejectsRawDDLAtOpen(t *testing.T) {
	for _, tc := range []struct{ field, engine, orderBy string }{
		{"meta_engine", "MergeTree; DROP TABLE victim", "flow_id"},
		{"meta_order_by", "MergeTree", "flow_id) SETTINGS allow_experimental_analyzer=1 --"},
	} {
		t.Run(tc.field, func(t *testing.T) {
			spec := clickHouseTypedMetadataSpec(t, "clickhouse://127.0.0.1:1/default", "default", "metadata", tc.engine, tc.orderBy)
			if spec.Options["meta_engine"] != tc.engine || spec.Options["meta_order_by"] != tc.orderBy {
				t.Fatal("typed remote fields did not reach the runtime options under test")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			destination := &chdest.Destination{}
			err := destination.Open(ctx, spec)
			defer destination.Close(ctx)
			if err == nil || !strings.Contains(err.Error(), "clickhouse "+tc.field) {
				t.Fatalf("typed raw DDL was not rejected by its pre-I/O compiler: %v", err)
			}
		})
	}
}

func TestClickHouseTypedMetadataCanonicalLayout(t *testing.T) {
	dsn := os.Getenv("WALLABY_TEST_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("WALLABY_TEST_CLICKHOUSE_DSN required")
	}
	database := os.Getenv("WALLABY_TEST_CLICKHOUSE_DB")
	if database == "" {
		database = "default"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, tc := range []struct{ name, engine, orderBy, expectedKey string }{
		{"defaults", "", "", "flow_id,source_schema,source_table,key_json"},
		{"explicit", "mErGeTrEe", "lsn, key_json", "lsn,key_json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := "wb_metadata_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			params := []any{chgo.Named("database", database), chgo.Named("table", table)}
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				if _, err := db.ExecContext(cleanup, `DROP TABLE IF EXISTS {database:Identifier}.{table:Identifier}`, params...); err != nil {
					t.Errorf("drop owned metadata table: %v", err)
				}
			})
			destination := &chdest.Destination{}
			spec := clickHouseTypedMetadataSpec(t, dsn, database, table, tc.engine, tc.orderBy)
			if err := destination.Open(ctx, spec); err != nil {
				t.Fatal(err)
			}
			defer destination.Close(ctx)
			var engine, sortingKey string
			if err := db.QueryRowContext(ctx, `SELECT engine,sorting_key FROM system.tables WHERE database={database:String} AND name={table:String}`, params...).Scan(&engine, &sortingKey); err != nil {
				t.Fatal(err)
			}
			key := strings.ReplaceAll(strings.ReplaceAll(sortingKey, " ", ""), "`", "")
			key = strings.TrimSuffix(strings.TrimPrefix(key, "tuple("), ")")
			if engine != "MergeTree" || key != tc.expectedKey {
				t.Fatalf("server metadata layout engine=%q sorting_key=%q, want MergeTree/%q", engine, sortingKey, tc.expectedKey)
			}
			var columns uint64
			if err := db.QueryRowContext(ctx, `SELECT count() FROM system.columns WHERE database={database:String} AND table={table:String}`, params...).Scan(&columns); err != nil || columns != 8 {
				t.Fatalf("metadata fixed column count=%d err=%v, want 8", columns, err)
			}
		})
	}
}
