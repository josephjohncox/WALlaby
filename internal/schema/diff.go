package schema

import (
	"strings"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// Diff compares two schemas and returns a change plan. It treats newSchema as a
// complete description of the relation, so a column that disappears is a drop.
func Diff(oldSchema, newSchema connector.Schema) Plan {
	return diff(oldSchema, newSchema, true)
}

// DiffPublishedShape compares two schemas observed through a replication stream,
// where newSchema describes only what the publication actually publishes rather
// than the whole relation. Absence is therefore not evidence of a drop, so no
// ChangeDropColumn is produced.
//
// This distinction is load-bearing, not cosmetic. PostgreSQL logical replication
// does not publish STORED generated columns at all before 18.0, and a table
// column list can exclude any column. Treating a Relation message as complete
// makes the destination drop a column that still exists at the source, which is
// destination data loss caused purely by publication scope. Real drops are
// carried by the pg_catalog DDL scanner, which reads pg_attribute and does see
// generated and unpublished columns.
func DiffPublishedShape(oldSchema, newSchema connector.Schema) Plan {
	return diff(oldSchema, newSchema, false)
}

func diff(oldSchema, newSchema connector.Schema, allowDrops bool) Plan {
	changes := make([]Change, 0)
	oldColumns := make(map[string]connector.Column)
	for _, col := range oldSchema.Columns {
		name := strings.ToLower(strings.TrimSpace(col.Name))
		if name == "" {
			continue
		}
		oldColumns[name] = col
	}
	newColumns := make(map[string]connector.Column)
	for _, col := range newSchema.Columns {
		name := strings.ToLower(strings.TrimSpace(col.Name))
		if name == "" {
			continue
		}
		newColumns[name] = col
	}

	for _, newCol := range newSchema.Columns {
		name := strings.ToLower(strings.TrimSpace(newCol.Name))
		if name == "" {
			continue
		}
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

		typeChanged := oldCol.Type != newCol.Type
		nullabilityKnown := newCol.TypeMetadata["nullability_known"] != "false"
		nullabilityChanged := nullabilityKnown && oldCol.Nullable != newCol.Nullable
		if typeChanged || nullabilityChanged {
			targetNullable := newCol.Nullable
			if !nullabilityKnown {
				targetNullable = oldCol.Nullable
			}
			changes = append(changes, Change{
				Type:         ChangeAlterColumn,
				Namespace:    newSchema.Namespace,
				Table:        newSchema.Name,
				Column:       newCol.Name,
				FromType:     oldCol.Type,
				ToType:       newCol.Type,
				FromNullable: oldCol.Nullable,
				Nullable:     targetNullable,
			})
		}

		generatedKnown := newCol.TypeMetadata["generated_known"] != "false"
		if generatedKnown && (oldCol.Generated != newCol.Generated || oldCol.Expression != newCol.Expression) {
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

	for _, oldCol := range oldSchema.Columns {
		if !allowDrops {
			break
		}
		name := strings.ToLower(strings.TrimSpace(oldCol.Name))
		if name == "" {
			continue
		}
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
