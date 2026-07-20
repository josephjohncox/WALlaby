package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestBackfillCompositeCursorBoundsReplayWithDuplicatePartitionValues(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	table := fmt.Sprintf("wallaby_backfill_cursor_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{table}.Sanitize()
	if _, err := conn.Exec(ctx, "CREATE TEMP TABLE "+identifier+" (id integer PRIMARY KEY, tenant_id integer)"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO "+identifier+" (id, tenant_id) VALUES (1, 1), (2, 1), (3, 1), (4, 2), (5, NULL), (6, NULL)"); err != nil {
		t.Fatal(err)
	}

	columns := []string{"tenant_id", "id"}
	cursor, err := encodeBackfillCursor(map[string]any{"tenant_id": 1, "id": 2}, columns)
	if err != nil {
		t.Fatal(err)
	}
	query, args, err := buildBackfillQueryWithCursor(identifier, "tenant_id", columns, backfillTask{cursor: cursor})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		t.Fatal(err)
	}
	var got []int
	for rows.Next() {
		var id int
		var tenant sql.NullInt64
		if err := rows.Scan(&id, &tenant); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		got = append(got, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	want := []int{2, 3, 4, 5, 6}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("resumed ids=%v, want bounded inclusive replay %v", got, want)
	}

	nullCursor, err := encodeBackfillCursor(map[string]any{"tenant_id": nil, "id": 5}, columns)
	if err != nil {
		t.Fatal(err)
	}
	query, args, err = buildBackfillQueryWithCursor(identifier, "tenant_id", columns, backfillTask{cursor: nullCursor})
	if err != nil {
		t.Fatal(err)
	}
	rows, err = conn.Query(ctx, query, args...)
	if err != nil {
		t.Fatal(err)
	}
	got = got[:0]
	for rows.Next() {
		var id int
		var tenant sql.NullInt64
		if err := rows.Scan(&id, &tenant); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		got = append(got, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	want = []int{5, 6}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("null-partition resumed ids=%v, want %v", got, want)
	}
}

func TestBackfillHashPartitionsCoverEveryRow(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	table := fmt.Sprintf("wallaby_backfill_partition_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{table}.Sanitize()
	if _, err := conn.Exec(ctx, "CREATE TEMP TABLE "+identifier+" (tenant_id integer)"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "INSERT INTO "+identifier+" SELECT generate_series(-1000, 1000) UNION ALL SELECT NULL"); err != nil {
		t.Fatal(err)
	}

	seen := make(map[string]int, 2002)
	for partition := range 8 {
		query, args, err := buildBackfillQueryWithCursor(identifier, "tenant_id", []string{"tenant_id"}, backfillTask{
			partitionCount: 8,
			partitionIndex: partition,
		})
		if err != nil {
			t.Fatalf("partition %d query: %v", partition, err)
		}
		rows, err := conn.Query(ctx, query, args...)
		if err != nil {
			t.Fatalf("partition %d: %v", partition, err)
		}
		for rows.Next() {
			var id sql.NullInt64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			key := "<null>"
			if id.Valid {
				key = fmt.Sprint(id.Int64)
			}
			seen[key]++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
	}

	for id := -1000; id <= 1000; id++ {
		key := fmt.Sprint(id)
		if seen[key] != 1 {
			t.Fatalf("row %d appeared in %d partitions, want exactly one", id, seen[key])
		}
	}
	if seen["<null>"] != 1 {
		t.Fatalf("NULL row appeared in %d partitions, want exactly one", seen["<null>"])
	}
}
