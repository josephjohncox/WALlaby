package postgres

import (
	"testing"

	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"pgregory.net/rapid"
)

func TestReconcilePostgresDDLPlanRejectsPartialApplication(t *testing.T) {
	t.Parallel()

	plan := internalschema.Plan{Changes: []internalschema.Change{
		{Type: internalschema.ChangeAddColumn, Namespace: "public", Table: "widgets", Column: "first", ToType: "text", Nullable: true},
		{Type: internalschema.ChangeAddColumn, Namespace: "public", Table: "widgets", Column: "second", ToType: "text", Nullable: true},
	}}
	catalog := ddlCatalogState{ddlCatalogKey("public", "widgets"): {
		exists: true,
		columns: map[string]ddlColumnState{
			"first": {typeName: "text", nullable: true},
		},
	}}
	if got := reconcilePostgresDDLPlan(connector.Schema{}, plan, catalog); got != connector.DDLReconcileIndeterminate {
		t.Fatalf("partial plan result=%v, want indeterminate", got)
	}
}

func TestReconcilePostgresPlanFollowsLaterRename(t *testing.T) {
	t.Parallel()

	plan := internalschema.Plan{Changes: []internalschema.Change{
		{Type: internalschema.ChangeAlterColumn, Namespace: "public", Table: "widgets", Column: "display_name", FromType: "text", ToType: "varchar(64)", FromNullable: true, Nullable: false},
		{Type: internalschema.ChangeRenameColumn, Namespace: "public", Table: "widgets", Column: "display_name", ToColumn: "title"},
	}}
	before := ddlCatalogState{ddlCatalogKey("public", "widgets"): {
		exists: true,
		columns: map[string]ddlColumnState{
			"display_name": {typeName: "text", nullable: true},
		},
	}}
	if got := reconcilePostgresDDLPlan(connector.Schema{}, plan, before); got != connector.DDLReconcileNotApplied {
		t.Fatalf("before result=%v, want not applied", got)
	}
	after := ddlCatalogState{ddlCatalogKey("public", "widgets"): {
		exists: true,
		columns: map[string]ddlColumnState{
			"title": {typeName: "varchar(64)", nullable: false},
		},
	}}
	if got := reconcilePostgresDDLPlan(connector.Schema{}, plan, after); got != connector.DDLReconcileApplied {
		t.Fatalf("after result=%v, want applied", got)
	}
}

func TestReconcilePostgresRenameStates(t *testing.T) {
	t.Parallel()

	change := internalschema.Change{Type: internalschema.ChangeRenameColumn, Column: "old", ToColumn: "new"}
	tests := []struct {
		name string
		cols map[string]ddlColumnState
		want connector.DDLReconcileResult
	}{
		{name: "not applied", cols: map[string]ddlColumnState{"old": {}}, want: connector.DDLReconcileNotApplied},
		{name: "applied", cols: map[string]ddlColumnState{"new": {}}, want: connector.DDLReconcileApplied},
		{name: "both names", cols: map[string]ddlColumnState{"old": {}, "new": {}}, want: connector.DDLReconcileIndeterminate},
		{name: "neither name", cols: map[string]ddlColumnState{}, want: connector.DDLReconcileIndeterminate},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := reconcilePostgresDDLChange(change, ddlTableState{exists: true, columns: tt.cols}); got != tt.want {
				t.Fatalf("result=%v, want %v", got, tt.want)
			}
		})
	}
}

func TestReconcilePostgresColumnChangesRapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		changeType := rapid.SampledFrom([]internalschema.ChangeType{
			internalschema.ChangeAddColumn,
			internalschema.ChangeDropColumn,
			internalschema.ChangeAlterColumn,
		}).Draw(t, "change_type")
		exists := rapid.Bool().Draw(t, "exists")
		actualType := rapid.SampledFrom([]string{"text", "int8", "int4", "bool"}).Draw(t, "actual_type")
		expectedType := rapid.SampledFrom([]string{"text", "bigint", "integer", "boolean"}).Draw(t, "expected_type")
		actualNullable := rapid.Bool().Draw(t, "actual_nullable")
		expectedNullable := rapid.Bool().Draw(t, "expected_nullable")

		columns := make(map[string]ddlColumnState)
		if exists {
			columns["value"] = ddlColumnState{typeName: actualType, nullable: actualNullable}
		}
		change := internalschema.Change{
			Type:         changeType,
			Column:       "value",
			FromType:     actualType,
			ToType:       expectedType,
			FromNullable: actualNullable,
			Nullable:     expectedNullable,
		}
		got := reconcilePostgresDDLChange(change, ddlTableState{exists: true, columns: columns})

		switch changeType {
		case internalschema.ChangeDropColumn:
			want := connector.DDLReconcileNotApplied
			if !exists {
				want = connector.DDLReconcileApplied
			}
			if got != want {
				t.Fatalf("drop exists=%v result=%v want=%v", exists, got, want)
			}
		case internalschema.ChangeAlterColumn:
			if !exists {
				if got != connector.DDLReconcileIndeterminate {
					t.Fatalf("alter missing result=%v", got)
				}
				return
			}
			typeChanged := !postgresDDLTypesEquivalent(actualType, expectedType)
			nullabilityChanged := actualNullable != expectedNullable
			matches := (!typeChanged || postgresDDLTypesEquivalent(actualType, expectedType)) &&
				(!nullabilityChanged || actualNullable == expectedNullable)
			want := connector.DDLReconcileNotApplied
			if matches {
				want = connector.DDLReconcileApplied
			}
			if got != want {
				t.Fatalf("alter result=%v want=%v", got, want)
			}
		case internalschema.ChangeAddColumn:
			switch {
			case !exists:
				if got != connector.DDLReconcileNotApplied {
					t.Fatalf("add missing result=%v", got)
				}
			case postgresDDLTypesEquivalent(actualType, expectedType) && actualNullable == expectedNullable:
				if got != connector.DDLReconcileApplied {
					t.Fatalf("add matching result=%v", got)
				}
			default:
				if got != connector.DDLReconcileIndeterminate {
					t.Fatalf("add conflicting result=%v", got)
				}
			}
		}
	})
}

func TestNormalizePostgresDDLTypeAliases(t *testing.T) {
	t.Parallel()

	for _, pair := range [][2]string{
		{"bigint", "int8"},
		{"integer", "int4"},
		{"boolean", "bool"},
		{"timestamp", "timestamp without time zone"},
		{"character varying(64)", "varchar(64)"},
		{"numeric(10,2)", "decimal(10, 2)"},
		{"integer[]", "int4[]"},
		{"timestamp(6) with time zone", "timestamptz(6)"},
	} {
		if !postgresDDLTypesEquivalent(pair[0], pair[1]) {
			t.Fatalf("types %q and %q should be equivalent", pair[0], pair[1])
		}
	}
	if postgresDDLTypesEquivalent("character varying(32)", "varchar(64)") {
		t.Fatal("different varchar bounds must not reconcile as the same type")
	}
}
