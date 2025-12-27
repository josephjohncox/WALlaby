package schema

import (
	"strings"

	"github.com/josephjohncox/ductstream/pkg/connector"
)

// Diff compares two schemas and returns a change plan.
func Diff(oldSchema, newSchema connector.Schema) Plan {
	changes := make([]Change, 0)
	oldColumns := make(map[string]connector.Column)
	for _, col := range oldSchema.Columns {
		oldColumns[strings.ToLower(col.Name)] = col
	}
	newColumns := make(map[string]connector.Column)
	for _, col := range newSchema.Columns {
		newColumns[strings.ToLower(col.Name)] = col
	}

	for name, newCol := range newColumns {
		oldCol, ok := oldColumns[name]
		if !ok {
			changes = append(changes, Change{
				Type:      ChangeAddColumn,
				Namespace: newSchema.Namespace,
				Table:     newSchema.Name,
				Column:    newCol.Name,
				ToType:    newCol.Type,
				Nullable:  newCol.Nullable,
			})
			continue
		}

		if oldCol.Type != newCol.Type || oldCol.Nullable != newCol.Nullable {
			changes = append(changes, Change{
				Type:      ChangeAlterColumn,
				Namespace: newSchema.Namespace,
				Table:     newSchema.Name,
				Column:    newCol.Name,
				FromType:  oldCol.Type,
				ToType:    newCol.Type,
				Nullable:  newCol.Nullable,
			})
		}

		if oldCol.Generated != newCol.Generated || oldCol.Expression != newCol.Expression {
			changeType := ChangeSetGenerated
			if !newCol.Generated {
				changeType = ChangeDropGenerated
			}
			changes = append(changes, Change{
				Type:       changeType,
				Namespace:  newSchema.Namespace,
				Table:      newSchema.Name,
				Column:     newCol.Name,
				Expression: newCol.Expression,
			})
		}
	}

	for name, oldCol := range oldColumns {
		if _, ok := newColumns[name]; !ok {
			changes = append(changes, Change{
				Type:      ChangeDropColumn,
				Namespace: oldSchema.Namespace,
				Table:     oldSchema.Name,
				Column:    oldCol.Name,
			})
		}
	}

	return Plan{Changes: changes}
}

// HasChanges returns true when the plan includes at least one change.
func (p Plan) HasChanges() bool {
	return len(p.Changes) > 0
}
