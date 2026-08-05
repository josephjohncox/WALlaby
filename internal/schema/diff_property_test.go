package schema

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"pgregory.net/rapid"
)

func TestDiffSchemaEvolutionRapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		columnCount := rapid.IntRange(1, 12).Draw(t, "column_count")
		oldSchema := connector.Schema{Name: "widgets", Namespace: "public"}
		newSchema := connector.Schema{Name: "widgets", Namespace: "public"}
		for index := range columnCount {
			name := fmt.Sprintf("column_%d", index)
			inOld := rapid.Bool().Draw(t, fmt.Sprintf("old_%d", index))
			inNew := rapid.Bool().Draw(t, fmt.Sprintf("new_%d", index))
			if !inOld && !inNew {
				inNew = true
			}
			oldColumn := rapidSchemaColumn(t, name, fmt.Sprintf("old_column_%d", index))
			newColumn := rapidSchemaColumn(t, name, fmt.Sprintf("new_column_%d", index))
			if inOld {
				oldSchema.Columns = append(oldSchema.Columns, oldColumn)
			}
			if inNew {
				newSchema.Columns = append(newSchema.Columns, newColumn)
			}
		}

		got := schemaChangeSignatures(Diff(oldSchema, newSchema).Changes)
		want := expectedSchemaChangeSignatures(oldSchema, newSchema)
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("diff mismatch\ngot:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
	})
}

func TestDiffIdenticalSchemaHasNoChangesRapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		columnCount := rapid.IntRange(0, 20).Draw(t, "column_count")
		schema := connector.Schema{Name: "widgets", Namespace: "public"}
		for index := range columnCount {
			schema.Columns = append(schema.Columns, rapidSchemaColumn(t, fmt.Sprintf("column_%d", index), fmt.Sprintf("column_%d", index)))
		}
		if plan := Diff(schema, schema); plan.HasChanges() {
			t.Fatalf("identical schema produced changes: %+v", plan.Changes)
		}
	})
}

func rapidSchemaColumn(t *rapid.T, name, label string) connector.Column {
	generated := rapid.Bool().Draw(t, label+"_generated")
	expression := ""
	if generated {
		expression = rapid.SampledFrom([]string{"lower(value)", "id + 1", "now()"}).Draw(t, label+"_expression")
	}
	return connector.Column{
		Name:       name,
		Type:       rapid.SampledFrom([]string{"text", "bigint", "boolean", "numeric(10,2)"}).Draw(t, label+"_type"),
		Nullable:   rapid.Bool().Draw(t, label+"_nullable"),
		Generated:  generated,
		Expression: expression,
	}
}

func expectedSchemaChangeSignatures(oldSchema, newSchema connector.Schema) []string {
	oldColumns := make(map[string]connector.Column, len(oldSchema.Columns))
	newColumns := make(map[string]connector.Column, len(newSchema.Columns))
	for _, column := range oldSchema.Columns {
		oldColumns[column.Name] = column
	}
	for _, column := range newSchema.Columns {
		newColumns[column.Name] = column
	}
	var signatures []string
	for name, newColumn := range newColumns {
		oldColumn, ok := oldColumns[name]
		if !ok {
			signatures = append(signatures, fmt.Sprintf("%s:%s:%s:%t", ChangeAddColumn, name, newColumn.Type, newColumn.Nullable))
			continue
		}
		if oldColumn.Type != newColumn.Type || oldColumn.Nullable != newColumn.Nullable {
			signatures = append(signatures, fmt.Sprintf("%s:%s:%s:%t", ChangeAlterColumn, name, newColumn.Type, newColumn.Nullable))
		}
		if oldColumn.Generated != newColumn.Generated || oldColumn.Expression != newColumn.Expression {
			changeType := ChangeSetGenerated
			if !newColumn.Generated {
				changeType = ChangeDropGenerated
			}
			signatures = append(signatures, fmt.Sprintf("%s:%s:%s", changeType, name, newColumn.Expression))
		}
	}
	for name := range oldColumns {
		if _, ok := newColumns[name]; !ok {
			signatures = append(signatures, fmt.Sprintf("%s:%s", ChangeDropColumn, name))
		}
	}
	sort.Strings(signatures)
	return signatures
}

func schemaChangeSignatures(changes []Change) []string {
	signatures := make([]string, 0, len(changes))
	for _, change := range changes {
		name := change.Column
		switch change.Type {
		case ChangeAddColumn, ChangeAlterColumn:
			signatures = append(signatures, fmt.Sprintf("%s:%s:%s:%t", change.Type, name, change.ToType, change.Nullable))
		case ChangeSetGenerated, ChangeDropGenerated:
			signatures = append(signatures, fmt.Sprintf("%s:%s:%s", change.Type, name, change.Expression))
		case ChangeDropColumn:
			signatures = append(signatures, fmt.Sprintf("%s:%s", change.Type, name))
		}
	}
	sort.Strings(signatures)
	return signatures
}
