package postgres

import (
	"strings"
	"testing"
)

func TestCheckedCatalogOIDBounds(t *testing.T) {
	t.Parallel()

	for _, value := range []int64{1, int64(^uint32(0))} {
		got, err := checkedCatalogOID(value, "test relation")
		if err != nil {
			t.Fatalf("checkedCatalogOID(%d): %v", value, err)
		}
		if uint64(got) != uint64(value) {
			t.Fatalf("checkedCatalogOID(%d)=%d", value, got)
		}
	}
	for _, value := range []int64{-1, 0, int64(^uint32(0)) + 1} {
		if _, err := checkedCatalogOID(value, "test relation"); err == nil || !strings.Contains(err.Error(), "outside PostgreSQL oid bounds") {
			t.Fatalf("checkedCatalogOID(%d) error=%v", value, err)
		}
	}
}
