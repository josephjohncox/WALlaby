package schema

import (
	"reflect"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

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
