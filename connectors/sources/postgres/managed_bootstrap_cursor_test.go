package postgres

import (
	"bytes"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestManagedSnapshotCursorIsVersionedLosslessAndTypeStable(t *testing.T) {
	t.Parallel()
	instant := time.Date(2026, 2, 17, 12, 34, 56, 789012345, time.FixedZone("offset", -7*60*60))
	id := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	task := bootstrap.SnapshotTask{
		Namespace:  "public",
		Table:      "cursor_types",
		KeyColumns: []string{"big", "amount", "payload", "id", "created_at"},
		Schema: connector.Schema{Namespace: "public", Name: "cursor_types", Columns: []connector.Column{
			{Name: "big", Type: "bigint"},
			{Name: "amount", Type: "numeric(40,20)"},
			{Name: "payload", Type: "bytea"},
			{Name: "id", Type: "uuid"},
			{Name: "created_at", Type: "timestamp with time zone"},
		}},
	}
	task.Delivery = identitySnapshotDelivery(task.Schema)
	bigint := int64(9007199254740993)
	numeric, ok := new(big.Rat).SetString("12345678901234567890.12345678901234567890")
	if !ok {
		t.Fatal("parse numeric fixture")
	}
	cursor, err := encodeManagedSnapshotCursor(task, map[string]any{
		"big": bigint, "amount": numeric, "payload": []byte{0, 1, 2, 0xff}, "id": id, "created_at": instant,
	})
	if err != nil {
		t.Fatal(err)
	}
	values, err := decodeManagedSnapshotCursor(task, cursor)
	if err != nil {
		t.Fatal(err)
	}
	if got := values[0]; got != "9007199254740993" {
		t.Fatalf("bigint cursor=%#v", got)
	}
	if got := values[1]; got != "12345678901234567890.12345678901234567890" {
		t.Fatalf("numeric cursor=%#v", got)
	}
	if got, ok := values[2].([]byte); !ok || !bytes.Equal(got, []byte{0, 1, 2, 0xff}) {
		t.Fatalf("bytea cursor=%#v", values[2])
	}
	if got := values[3]; got != id.String() {
		t.Fatalf("uuid cursor=%#v", got)
	}
	if got := values[4]; got != instant.Format(time.RFC3339Nano) {
		t.Fatalf("timestamptz cursor=%#v", got)
	}

	changed := task
	changed.Schema.Columns = append([]connector.Column(nil), task.Schema.Columns...)
	changed.Schema.Columns[1].Type = "text"
	if _, err := decodeManagedSnapshotCursor(changed, cursor); err == nil {
		t.Fatal("cursor schema/type mismatch was accepted")
	}
	changed = task
	changed.KeyColumns = []string{"big"}
	if _, err := decodeManagedSnapshotCursor(changed, cursor); err == nil {
		t.Fatal("cursor arity mismatch was accepted")
	}
}

func TestManagedSnapshotCursorMixedCompositeRestartIsStable(t *testing.T) {
	t.Parallel()
	task := bootstrap.SnapshotTask{
		Namespace:  "public",
		Table:      "cursor_composite",
		KeyColumns: []string{"tenant", "sequence", "token"},
		Schema: connector.Schema{Namespace: "public", Name: "cursor_composite", Columns: []connector.Column{
			{Name: "tenant", Type: "text"},
			{Name: "sequence", Type: "bigint"},
			{Name: "token", Type: "bytea"},
		}},
	}
	task.Delivery = identitySnapshotDelivery(task.Schema)
	row := map[string]any{"tenant": "acme", "sequence": int64(9007199254740997), "token": []byte{0xde, 0xad}}
	first, err := encodeManagedSnapshotCursor(task, row)
	if err != nil {
		t.Fatal(err)
	}
	values, err := decodeManagedSnapshotCursor(task, first)
	if err != nil {
		t.Fatal(err)
	}
	restarted := map[string]any{"tenant": values[0], "sequence": values[1], "token": values[2]}
	second, err := encodeManagedSnapshotCursor(task, restarted)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("cursor changed across restart:\n%s\n%s", first, second)
	}
}
