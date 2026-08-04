package tablemap

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/josephjohncox/wallaby/internal/flow"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func projectDDLRecord(sourceSchema, targetSchema connector.Schema, resolved resolvedTable, record connector.Record) (connector.Record, bool, error) {
	out := record
	out.Table = resolved.targetTable
	if len(record.DDLPlan) == 0 {
		if strings.TrimSpace(record.DDL) == "" {
			return connector.Record{}, false, errors.New("DDL record has neither raw SQL nor a structured plan")
		}
		if resolved.nonidentity {
			return connector.Record{}, false, errors.New("raw SQL DDL cannot be used with a nonidentity table projection")
		}
		return out, true, nil
	}
	var plan internalschema.Plan
	if err := json.Unmarshal(record.DDLPlan, &plan); err != nil {
		return connector.Record{}, false, fmt.Errorf("decode structured DDL plan: %w", err)
	}
	projected := internalschema.Plan{Changes: make([]internalschema.Change, 0, len(plan.Changes))}
	for _, change := range plan.Changes {
		if resolved.write.Mode == flow.TableWriteModeAppend && change.Type == internalschema.ChangeAlterPrimaryKey {
			continue
		}
		if change.Namespace != "" && change.Namespace != sourceSchema.Namespace {
			return connector.Record{}, false, fmt.Errorf("DDL change namespace %q differs from batch source namespace %q", change.Namespace, sourceSchema.Namespace)
		}
		if change.Table != "" && change.Table != sourceSchema.Name {
			return connector.Record{}, false, fmt.Errorf("DDL change table %q differs from batch source table %q", change.Table, sourceSchema.Name)
		}
		mapped := change
		mapped.Namespace = targetSchema.Namespace
		mapped.Table = targetSchema.Name
		if strings.TrimSpace(change.Expression) != "" && resolved.nonidentity {
			return connector.Record{}, false, fmt.Errorf("DDL expression for %s cannot be rewritten by a nonidentity projection", change.Type)
		}
		if change.Column != "" {
			target, included := resolveDDLColumn(resolved, change.Column)
			if !included {
				if change.Type == internalschema.ChangeRenameColumn {
					return connector.Record{}, false, fmt.Errorf("rename source column %q is excluded", change.Column)
				}
				continue
			}
			mapped.Column = target
		}
		if change.Type == internalschema.ChangeRenameColumn {
			if strings.TrimSpace(change.ToColumn) == "" {
				return connector.Record{}, false, errors.New("rename column DDL requires to_column")
			}
			target, included := resolveDDLColumn(resolved, change.ToColumn)
			if !included {
				return connector.Record{}, false, fmt.Errorf("rename target column %q is excluded", change.ToColumn)
			}
			mapped.ToColumn = target
		}
		if len(change.PrimaryKeys) > 0 && resolved.write.Mode == flow.TableWriteModeAppend {
			mapped.PrimaryKeys = nil
		}
		if len(change.PrimaryKeys) > 0 && resolved.write.Mode != flow.TableWriteModeAppend {
			mapped.PrimaryKeys = make([]string, 0, len(change.PrimaryKeys))
			for _, key := range change.PrimaryKeys {
				target, included := resolveDDLColumn(resolved, key)
				if !included {
					return connector.Record{}, false, fmt.Errorf("primary key DDL references excluded column %q", key)
				}
				mapped.PrimaryKeys = append(mapped.PrimaryKeys, target)
			}
		}
		projected.Changes = append(projected.Changes, mapped)
	}
	if len(projected.Changes) == 0 {
		return connector.Record{}, false, nil
	}
	encoded, err := json.Marshal(projected)
	if err != nil {
		return connector.Record{}, false, fmt.Errorf("encode projected DDL plan: %w", err)
	}
	out.DDLPlan = encoded
	if resolved.nonidentity {
		out.DDL = ""
		out.Payload = nil
	}
	return out, true, nil
}

func resolveDDLColumn(resolved resolvedTable, source string) (string, bool) {
	if column, ok := resolved.exactColumns[source]; ok {
		if column.Action == flow.MappingActionExclude {
			return "", false
		}
		return column.TargetColumn, true
	}
	if resolved.futureColumns.Action == flow.MappingActionExclude {
		return "", false
	}
	return expand(resolved.futureColumns.TargetColumn, resolved.sourceSchema, resolved.sourceTable, source), true
}
