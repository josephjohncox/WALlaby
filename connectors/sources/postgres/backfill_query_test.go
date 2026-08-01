package postgres

import (
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestBuildBackfillQueryNormalizesSignedHashesAndNulls(t *testing.T) {
	t.Parallel()

	query, args, err := buildBackfillQueryWithCursor(
		pgx.Identifier{"public", "events"}.Sanitize(),
		"tenant_id",
		[]string{"tenant_id"},
		backfillTask{partitionCount: 4, partitionIndex: 3},
	)
	if err != nil {
		t.Fatal(err)
	}

	wantClause := `mod(mod(hashtext(COALESCE("tenant_id"::text, '<wallaby:null>')), $1) + $1, $1) = $2`
	if !strings.Contains(query, wantClause) {
		t.Fatalf("query = %q, want normalized hash clause %q", query, wantClause)
	}
	if want := []any{4, 3}; !reflect.DeepEqual(args, want) {
		t.Fatalf("args = %#v, want %#v", args, want)
	}
}

func TestBuildBackfillQueryResumesInclusively(t *testing.T) {
	t.Parallel()

	query, args, err := buildBackfillQueryWithCursor(
		pgx.Identifier{"public", "events"}.Sanitize(),
		"tenant_id",
		[]string{"tenant_id"},
		backfillTask{partitionCount: 4, partitionIndex: 2, cursor: "42"},
	)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(query, `("tenant_id" >= $3 OR "tenant_id" IS NULL)`) {
		t.Fatalf("query = %q, want inclusive null-safe cursor predicate", query)
	}
	if want := []any{4, 2, "42"}; !reflect.DeepEqual(args, want) {
		t.Fatalf("args = %#v, want %#v", args, want)
	}
	if !strings.HasSuffix(query, `ORDER BY "tenant_id" NULLS LAST`) {
		t.Fatalf("query = %q, want stable partition-column ordering", query)
	}
}
