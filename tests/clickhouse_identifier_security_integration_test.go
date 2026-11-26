package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	chgo "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	chdest "github.com/josephjohncox/wallaby/connectors/destinations/clickhouse"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestClickHouseBackslashIdentifiersRoundTripWithoutSQLInjection(t *testing.T) {
	dsn := os.Getenv("WALLABY_TEST_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("WALLABY_TEST_CLICKHOUSE_DSN required")
	}
	database := os.Getenv("WALLABY_TEST_CLICKHOUSE_DB")
	if database == "" {
		database = "default"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for index := range 2 {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			table := "wb_identifier_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			column := "trailing\\"
			if index == 1 {
				column = "payload\\`; DROP TABLE " + database + "." + table + "_victim; --"
			}
			params := func(name string) []any {
				return []any{chgo.Named("database", database), chgo.Named("table", name), chgo.Named("column", column)}
			}
			for _, name := range []string{table, table + "_staging", table + "_victim"} {
				// ClickHouse cannot parameterize a column declaration. Use Go's
				// double-quoted literal encoding for this ASCII fixture, independent
				// of the adapter's backtick quoting; bind table/database identifiers.
				if _, err := db.ExecContext(ctx, `CREATE TABLE {database:Identifier}.{table:Identifier}
(id UInt64, `+strconv.Quote(column)+` String) ENGINE=MergeTree ORDER BY id`, params(name)...); err != nil {
					t.Fatal(err)
				}
				name := name
				t.Cleanup(func() {
					cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
					defer stop()
					if _, err := db.ExecContext(cleanup, `DROP TABLE IF EXISTS {database:Identifier}.{table:Identifier}`, params(name)...); err != nil {
						t.Errorf("drop owned table: %v", err)
					}
				})
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO {database:Identifier}.{table:Identifier} SELECT 7,'previous'`, params(table)...); err != nil {
				t.Fatal(err)
			}
			destination := &chdest.Destination{}
			if err := destination.Open(ctx, connector.RuntimeSpec{Type: connector.EndpointClickHouse, Options: map[string]string{
				"dsn": dsn, "batch_mode": "staging", "batch_resolution": "replace", "meta_table_enabled": "false",
			}}); err != nil {
				t.Fatal(err)
			}
			defer destination.Close(ctx)
			batch := connector.Batch{
				Schema: connector.Schema{Name: table, Namespace: database, Columns: []connector.Column{
					{Name: "id", Type: "UInt64"}, {Name: column, Type: "String"},
				}},
				Records:    []connector.Record{{Table: table, Operation: connector.OpLoad, After: map[string]any{"id": uint64(42), column: "exact-value"}}},
				Checkpoint: connector.Checkpoint{LSN: "42"}, WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend},
			}
			if err := destination.Write(ctx, batch); err != nil {
				t.Fatalf("production staging write rejected a valid escaped identifier: %v", err)
			}
			if err := destination.ResolveStaging(ctx); err != nil {
				t.Fatalf("production INSERT SELECT rejected a valid escaped identifier: %v", err)
			}
			var count uint64
			if err := db.QueryRowContext(ctx, `SELECT count() FROM {database:Identifier}.{table:Identifier}`, params(table)...).Scan(&count); err != nil || count != 1 {
				t.Fatalf("replace resolution row count=%d err=%v, want one new row", count, err)
			}
			var id uint64
			var value string
			if err := db.QueryRowContext(ctx, `SELECT * FROM {database:Identifier}.{table:Identifier}`, params(table)...).Scan(&id, &value); err != nil {
				t.Fatal(err)
			}
			if id != 42 || value != "exact-value" {
				t.Fatalf("identifier/value roundtrip=%d/%q", id, value)
			}
			rows, err := db.QueryContext(ctx, `SELECT name FROM system.columns WHERE database={database:String} AND table={table:String}`, params(table)...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			names := map[string]bool{}
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err != nil {
					t.Fatal(err)
				}
				names[name] = true
			}
			if err := rows.Err(); err != nil || len(names) != 2 || !names["id"] || !names[column] {
				t.Fatalf("server did not preserve exact column identity: names=%v err=%v", names, err)
			}
			var victims uint64
			if err := db.QueryRowContext(ctx, `SELECT count() FROM system.tables WHERE database={database:String} AND name={table:String}`, params(table+"_victim")...).Scan(&victims); err != nil || victims != 1 {
				t.Fatalf("sentinel table was modified: count=%d err=%v", victims, err)
			}
		})
	}
}
