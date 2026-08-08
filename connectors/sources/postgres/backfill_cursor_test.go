package postgres

import (
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"pgregory.net/rapid"
)

func BenchmarkBuildBackfillCompositeCursorQuery(b *testing.B) {
	columns := []string{"tenant_id", "account_id", "event_id"}
	cursor, err := encodeBackfillCursor(map[string]any{
		"tenant_id": 42, "account_id": 100, "event_id": 1000,
	}, columns)
	if err != nil {
		b.Fatal(err)
	}
	task := backfillTask{partitionCount: 16, partitionIndex: 7, cursor: cursor}
	identifier := pgx.Identifier{"public", "events"}.Sanitize()
	b.ReportAllocs()
	for range b.N {
		if _, _, err := buildBackfillQueryWithCursor(identifier, "tenant_id", columns, task); err != nil {
			b.Fatal(err)
		}
	}
}

func TestBackfillCursorRoundTripRapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		partitionNull := rapid.Bool().Draw(t, "partition_null")
		partition := rapid.StringMatching(`[A-Za-z0-9_-]{0,24}`).Draw(t, "partition")
		firstKey := rapid.StringMatching(`[A-Za-z0-9_-]{1,24}`).Draw(t, "first_key")
		secondKey := rapid.StringMatching(`[A-Za-z0-9_-]{1,24}`).Draw(t, "second_key")
		row := map[string]any{
			"tenant_id":  partition,
			"account_id": firstKey,
			"event_id":   secondKey,
		}
		if partitionNull {
			row["tenant_id"] = nil
		}
		columns := []string{"tenant_id", "account_id", "event_id"}
		cursor, err := encodeBackfillCursor(row, columns)
		if err != nil {
			t.Fatal(err)
		}
		values, encoded, err := decodeBackfillCursor(cursor, len(columns))
		if err != nil || !encoded {
			t.Fatalf("decode encoded=%v error=%v", encoded, err)
		}
		if values[0].Null != partitionNull || values[1].Text != firstKey || values[2].Text != secondKey {
			t.Fatalf("round trip values=%+v", values)
		}
		if !partitionNull && values[0].Text != partition {
			t.Fatalf("partition=%q, want %q", values[0].Text, partition)
		}
	})
}

func TestBuildBackfillQueryUsesCompositeCursor(t *testing.T) {
	t.Parallel()

	columns := []string{"tenant_id", "id"}
	cursor, err := encodeBackfillCursor(map[string]any{"tenant_id": 42, "id": 7}, columns)
	if err != nil {
		t.Fatal(err)
	}
	query, args, err := buildBackfillQueryWithCursor(
		pgx.Identifier{"public", "events"}.Sanitize(),
		"tenant_id",
		columns,
		backfillTask{cursor: cursor},
	)
	if err != nil {
		t.Fatal(err)
	}
	wantClause := `("tenant_id" > $1 OR "tenant_id" IS NULL OR ("tenant_id" = $1 AND ROW("id") >= ROW($2)))`
	if !strings.Contains(query, wantClause) {
		t.Fatalf("query=%q, want composite resume clause %q", query, wantClause)
	}
	if want := []any{"42", "7"}; !reflect.DeepEqual(args, want) {
		t.Fatalf("args=%#v, want %#v", args, want)
	}
	if !strings.HasSuffix(query, `ORDER BY "tenant_id" NULLS LAST, "id"`) {
		t.Fatalf("query=%q, want deterministic composite ordering", query)
	}
}

func TestBuildBackfillQueryResumesWithinNullPartition(t *testing.T) {
	t.Parallel()

	columns := []string{"tenant_id", "id"}
	cursor, err := encodeBackfillCursor(map[string]any{"tenant_id": nil, "id": 7}, columns)
	if err != nil {
		t.Fatal(err)
	}
	query, args, err := buildBackfillQueryWithCursor(
		pgx.Identifier{"public", "events"}.Sanitize(),
		"tenant_id",
		columns,
		backfillTask{cursor: cursor},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query, `("tenant_id" IS NULL AND ROW("id") >= ROW($1))`) {
		t.Fatalf("query=%q, want null-partition resume clause", query)
	}
	if want := []any{"7"}; !reflect.DeepEqual(args, want) {
		t.Fatalf("args=%#v, want %#v", args, want)
	}
}

func TestBackfillCursorUsesPostgresTextForBinaryKeys(t *testing.T) {
	t.Parallel()

	cursor, err := encodeBackfillCursor(map[string]any{
		"tenant_id": 1,
		"key":       []byte{0x00, 0xab, 0xff},
	}, []string{"tenant_id", "key"})
	if err != nil {
		t.Fatal(err)
	}
	values, encoded, err := decodeBackfillCursor(cursor, 2)
	if err != nil || !encoded {
		t.Fatalf("decode encoded=%v error=%v", encoded, err)
	}
	if values[1].Text != `\\x00abff` {
		t.Fatalf("binary key=%q, want PostgreSQL bytea text", values[1].Text)
	}
}

func TestDecodeBackfillCursorRejectsMalformedEnvelope(t *testing.T) {
	t.Parallel()

	if _, encoded, err := decodeBackfillCursor(backfillCursorPrefix+"not-base64", 2); !encoded || err == nil {
		t.Fatalf("encoded=%v error=%v, want malformed encoded cursor failure", encoded, err)
	}
}

func TestBackfillCursorColumnsUsesPrimaryKeyTieBreakers(t *testing.T) {
	t.Parallel()

	got := backfillCursorColumns("tenant_id", []string{"account_id", "TENANT_ID", "tenant_id", "event_id"})
	want := []string{"tenant_id", "account_id", "TENANT_ID", "event_id"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("columns=%v, want %v", got, want)
	}
}
