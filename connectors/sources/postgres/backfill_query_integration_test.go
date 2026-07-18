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
		query, args := buildBackfillQuery(identifier, "tenant_id", backfillTask{
			partitionCount: 8,
			partitionIndex: partition,
		})
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
