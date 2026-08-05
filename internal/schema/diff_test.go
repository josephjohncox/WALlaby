package schema

import (
	"reflect"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestDiffPublishedShapeNeverDropsUnpublishedColumns(t *testing.T) {
	t.Parallel()
	// PostgreSQL logical replication does not publish STORED generated columns
	// before 18.0, and a table column list can exclude any column, so a Relation
	// message that omits a column is not evidence the column was dropped.
	full := connector.Schema{
		Namespace: "public", Name: "widgets",
		Columns: []connector.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "value", Type: "text", Nullable: true},
			{Name: "rendered", Type: "text", Nullable: true, Generated: true, Expression: "(value || '-generated')"},
		},
	}
	published := connector.Schema{
		Namespace: "public", Name: "widgets",
		Columns: []connector.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "value", Type: "text", Nullable: true},
		},
	}

	if plan := DiffPublishedShape(full, published); plan.HasChanges() {
		t.Fatalf("published-shape diff planned %+v; an unpublished column must never be dropped at the destination", plan.Changes)
	}

	// The authoritative pg_catalog scanner still reports real drops.
	plan := Diff(full, published)
	if len(plan.Changes) != 1 || plan.Changes[0].Type != ChangeDropColumn || plan.Changes[0].Column != "rendered" {
		t.Fatalf("catalog diff changes=%+v, want exactly one drop of rendered", plan.Changes)
	}

	// Additive evolution still propagates through the published-shape diff.
	evolved := published
	evolved.Columns = append(append([]connector.Column(nil), published.Columns...), connector.Column{Name: "note", Type: "text", Nullable: true})
	addPlan := DiffPublishedShape(published, evolved)
	if len(addPlan.Changes) != 1 || addPlan.Changes[0].Type != ChangeAddColumn || addPlan.Changes[0].Column != "note" {
		t.Fatalf("published-shape diff changes=%+v, want exactly one add of note", addPlan.Changes)
	}
}

func TestDiffPreservesCaseAndWhitespaceDistinctColumnIdentifiers(t *testing.T) {
	t.Parallel()
	oldSchema := connector.Schema{Namespace: "Exact Schema", Name: "Events", Columns: []connector.Column{
		{Name: "ID", Type: "bigint"},
		{Name: "id", Type: "text"},
		{Name: " id ", Type: "boolean"},
	}}
	newSchema := connector.Schema{Namespace: "Exact Schema", Name: "Events", Columns: []connector.Column{
		{Name: "ID", Type: "uuid"},
		{Name: "id", Type: "text"},
		{Name: " ID ", Type: "numeric"},
	}}
	plan := Diff(oldSchema, newSchema)
	if len(plan.Changes) != 3 {
		t.Fatalf("exact identifier diff changes=%+v, want alter ID, add whitespace-ID, and drop whitespace-id", plan.Changes)
	}
	want := []Change{
		{Type: ChangeAlterColumn, Namespace: "Exact Schema", Table: "Events", Column: "ID", FromType: "bigint", ToType: "uuid"},
		{Type: ChangeAddColumn, Namespace: "Exact Schema", Table: "Events", Column: " ID ", ToType: "numeric"},
		{Type: ChangeDropColumn, Namespace: "Exact Schema", Table: "Events", Column: " id "},
	}
	for index := range want {
		if plan.Changes[index].Type != want[index].Type || plan.Changes[index].Namespace != want[index].Namespace || plan.Changes[index].Table != want[index].Table || plan.Changes[index].Column != want[index].Column || plan.Changes[index].FromType != want[index].FromType || plan.Changes[index].ToType != want[index].ToType {
			t.Fatalf("exact identifier change[%d]=%+v, want %+v", index, plan.Changes[index], want[index])
		}
	}
}

func TestDiffDetectsGeneratedFlagChanges(t *testing.T) {
	oldSchema := connector.Schema{
		Namespace: "public",
		Name:      "widgets",
		Columns: []connector.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "value", Type: "text", Nullable: true},
		},
	}

	newSchema := connector.Schema{
		Namespace: "public",
		Name:      "widgets",
		Columns: []connector.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "value", Type: "text", Nullable: true, Generated: true, Expression: "COALESCE(value, 'x')"},
		},
	}

	plan := Diff(oldSchema, newSchema)
	if len(plan.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(plan.Changes))
	}
	got := plan.Changes[0]
	if got.Type != ChangeSetGenerated {
		t.Fatalf("expected set_generated change, got %s", got.Type)
	}
	if got.Column != "value" {
		t.Fatalf("expected generated change for value, got %s", got.Column)
	}
	if got.Expression != "COALESCE(value, 'x')" {
		t.Fatalf("expected generated expression, got %q", got.Expression)
	}
}

func TestDiffDetectsDropGeneratedChange(t *testing.T) {
	oldSchema := connector.Schema{
		Namespace: "public",
		Name:      "widgets",
		Columns: []connector.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "value", Type: "text", Nullable: true, Generated: true, Expression: "COALESCE(value, 'x')"},
		},
	}

	newSchema := connector.Schema{
		Namespace: "public",
		Name:      "widgets",
		Columns: []connector.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "value", Type: "text", Nullable: true},
		},
	}

	plan := Diff(oldSchema, newSchema)
	if len(plan.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(plan.Changes))
	}
	if plan.Changes[0].Type != ChangeDropGenerated {
		t.Fatalf("expected drop_generated change, got %s", plan.Changes[0].Type)
	}
}

func TestDiffIgnoresUnknownPgoutputNullabilityAndGeneratedFlags(t *testing.T) {
	oldSchema := connector.Schema{
		Namespace: "public", Name: "widgets",
		Columns: []connector.Column{{Name: "id", Type: "int8", Nullable: false, Generated: true, Expression: "id + 1"}},
	}
	newSchema := connector.Schema{
		Namespace: "public", Name: "widgets",
		Columns: []connector.Column{{
			Name: "id", Type: "int8", Nullable: true,
			TypeMetadata: map[string]string{"nullability_known": "false", "generated_known": "false"},
		}},
	}
	if plan := Diff(oldSchema, newSchema); plan.HasChanges() {
		t.Fatalf("unknown pgoutput catalog flags produced destructive DDL: %+v", plan.Changes)
	}
}

func TestDiffPreservesColumnOrderForAddAlterAndGeneratedChanges(t *testing.T) {
	oldSchema := connector.Schema{
		Namespace: "public",
		Name:      "widgets",
		Columns: []connector.Column{
			{Name: "z", Type: "int4", Nullable: false},
			{Name: "a", Type: "text", Nullable: true},
		},
	}
	newSchema := connector.Schema{
		Namespace: "public",
		Name:      "widgets",
		Columns: []connector.Column{
			{Name: "a", Type: "text", Nullable: true, Generated: true, Expression: "UPPER(name)"},
			{Name: "z", Type: "int8", Nullable: false},
			{Name: "newcol", Type: "jsonb", Nullable: true},
		},
	}

	plan := Diff(oldSchema, newSchema)
	expected := []ChangeType{ChangeSetGenerated, ChangeAlterColumn, ChangeAddColumn}
	if len(plan.Changes) != len(expected) {
		t.Fatalf("expected %d changes, got %d", len(expected), len(plan.Changes))
	}
	got := make([]ChangeType, 0, len(plan.Changes))
	for _, c := range plan.Changes {
		got = append(got, c.Type)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("unexpected change ordering: got=%v expected=%v", got, expected)
	}
}
