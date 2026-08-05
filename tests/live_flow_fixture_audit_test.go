package tests

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	internalflow "github.com/josephjohncox/wallaby/internal/flow"
)

func TestAllLiveFlowFixturesDeclareDurableTableMappings(t *testing.T) {
	t.Parallel()
	expectedFlows := map[string]int{
		"artifactlog_failure_integration_test.go":                1,
		"artifactlog_integration_test.go":                        5,
		"artifactlog_worker_integration_test.go":                 1,
		"authority_integration_test.go":                          8,
		"clickhouse_managed_e2e_integration_test.go":             1,
		"delivery_integration_test.go":                           3,
		"delivery_retry_retention_integration_test.go":           1,
		"integration/dbos_integration_test.go":                   4,
		"integration/dbos_managed_bootstrap_integration_test.go": 1,
		"integration/postgres_to_postgres_integration_test.go":   1,
		"managed_admission_ownership_integration_test.go":        2,
		"managed_bootstrap_boundaries_integration_test.go":       1,
		"managed_bootstrap_wiring_integration_test.go":           2,
		"postgres_bootstrap_integration_test.go":                 1,
		"postgres_managed_e2e_integration_test.go":               1,
		"postgres_managed_transaction_integration_test.go":       2,
		"registry_fencing_integration_test.go":                   1,
		"registry_schema_fencing_integration_test.go":            1,
		"snowflake_managed_profile_integration_test.go":          1,
		"snowflake_managed_source_cut_integration_test.go":       1,
		"source_feedback_fencing_integration_test.go":            1,
		"wallaby_worker_recovery_integration_test.go":            1,
	}
	expectedPayloads := map[string]int{"integration/cli_integration_test.go": 15}
	foundFlows := map[string]int{}
	foundPayloads := map[string]int{}
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_integration_test.go") {
			return nil
		}
		path = filepath.ToSlash(strings.TrimPrefix(filepath.Clean(path), "./"))
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		for _, serializer := range mappingRepairSerializers(parsed) {
			t.Errorf("%s serializer %s mutates/defaults TableMappings", path, serializer)
		}
		definitions := expressionDefinitions(parsed)
		flowCount, payloadCount := 0, 0
		ast.Inspect(parsed, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			kind, number := "", 0
			switch {
			case isFlowComposite(literal.Type):
				flowCount++
				kind, number = "flow.Flow", flowCount
			case isFlowConfigPayloadComposite(literal.Type):
				payloadCount++
				kind, number = "flowConfigPayload", payloadCount
			default:
				return true
			}
			if err := validateFixtureMappings(literal, definitions); err != nil {
				t.Errorf("%s %s fixture %d: %v", path, kind, number, err)
			}
			for _, option := range removedFlowOptions(literal, definitions) {
				t.Errorf("%s %s fixture %d declares removed endpoint option %q", path, kind, number, option)
			}
			return true
		})
		if flowCount > 0 {
			foundFlows[path] = flowCount
		}
		if payloadCount > 0 {
			foundPayloads[path] = payloadCount
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertFixtureManifest(t, "live flow.Flow", foundFlows, expectedFlows)
	assertFixtureManifest(t, "CLI flowConfigPayload", foundPayloads, expectedPayloads)
}

func TestLiveFlowFixtureAuditInspectsUntranslatedDefinitions(t *testing.T) {
	t.Parallel()
	source := `package fixture
import "github.com/josephjohncox/wallaby/internal/flow"
var missing=flow.Flow{Destinations:[]connector.Spec{{Name:"target"}},Config:flow.Config{AckPolicy:"all"}}
var legacy=map[string]string{"schema":"target","write_mode":"upsert"}
var empty=flow.Flow{Destinations:[]connector.Spec{{Name:"target",Options:legacy}},Config:flow.Config{TableMappings:flow.TableMappings{}}}
var wrong=flowConfigPayload{Destinations:[]endpointConfigPayload{{Name:"target"}},Config:flow.Config{TableMappings:flow.TableMappings{Version:flow.TableMappingsVersion,Destinations:[]flow.DestinationTableMappings{{Destination:"other"}}}}}
var valid=flowConfigPayload{Destinations:[]endpointConfigPayload{{Name:"target"}},Config:flow.Config{TableMappings:flow.TableMappings{Version:flow.TableMappingsVersion,Destinations:[]flow.DestinationTableMappings{{Destination:"target"}}}}}
func writeFlowConfig(cfg flowConfigPayload){cfg.Config.TableMappings=flow.TableMappings{}}`
	parsed, err := parser.ParseFile(token.NewFileSet(), "fixture.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	definitions := expressionDefinitions(parsed)
	var literals []*ast.CompositeLit
	ast.Inspect(parsed, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if ok && (isFlowComposite(literal.Type) || isFlowConfigPayloadComposite(literal.Type)) {
			literals = append(literals, literal)
		}
		return true
	})
	if len(literals) != 4 {
		t.Fatalf("fixture literals=%d, want 4", len(literals))
	}
	for index := 0; index < 2; index++ {
		if err := validateFixtureMappings(literals[index], definitions); err == nil {
			t.Fatalf("invalid fixture %d was accepted", index)
		}
	}
	if err := validateFixtureMappings(literals[2], definitions); err == nil || !strings.Contains(err.Error(), "do not match") {
		t.Fatalf("mismatched destinations error=%v", err)
	}
	if err := validateFixtureMappings(literals[3], definitions); err != nil {
		t.Fatalf("valid fixture rejected: %v", err)
	}
	if got := removedFlowOptions(literals[1], definitions); !reflect.DeepEqual(got, []string{"schema", "write_mode"}) {
		t.Fatalf("removed options=%v", got)
	}
	if got := mappingRepairSerializers(parsed); !reflect.DeepEqual(got, []string{"writeFlowConfig"}) {
		t.Fatalf("repairing serializers=%v", got)
	}
}

func mappingRepairSerializers(file *ast.File) []string {
	var found []string
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		name := strings.ToLower(function.Name.Name)
		if !strings.Contains(name, "write") || !strings.Contains(name, "flow") || (!strings.Contains(name, "config") && !strings.Contains(name, "file")) {
			continue
		}
		repairs := false
		ast.Inspect(function.Body, func(node ast.Node) bool {
			assignment, ok := node.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for _, left := range assignment.Lhs {
				for _, part := range selectorPath(left) {
					if part == "TableMappings" {
						repairs = true
					}
				}
			}
			return true
		})
		if repairs {
			found = append(found, function.Name.Name)
		}
	}
	sort.Strings(found)
	return found
}

func selectorPath(expression ast.Expr) []string {
	switch value := expression.(type) {
	case *ast.Ident:
		return []string{value.Name}
	case *ast.SelectorExpr:
		return append(selectorPath(value.X), value.Sel.Name)
	default:
		return nil
	}
}

func validateFixtureMappings(literal *ast.CompositeLit, definitions map[string]ast.Expr) error {
	destinationNames, err := fixtureDestinationNames(fieldExpression(literal, "Destinations"), definitions)
	if err != nil {
		return err
	}
	if len(destinationNames) == 0 {
		return fmt.Errorf("destinations must be declared directly and nonempty")
	}
	config, ok := resolveComposite(fieldExpression(literal, "Config"), definitions)
	if !ok {
		return fmt.Errorf("Config must be declared directly")
	}
	mappingExpression := fieldExpression(config, "TableMappings")
	if mappingExpression == nil {
		return fmt.Errorf("Config.TableMappings must be declared directly")
	}
	mappingNames, current, err := fixtureMappingNames(mappingExpression, definitions)
	if err != nil {
		return err
	}
	if !current {
		return fmt.Errorf("Config.TableMappings must use current nonzero version")
	}
	if len(mappingNames) == 0 {
		return fmt.Errorf("Config.TableMappings destination mappings must be nonempty")
	}
	if !sameNames(destinationNames, mappingNames) {
		return fmt.Errorf("mapping destinations %v do not match endpoint destinations %v", mappingNames, destinationNames)
	}
	return nil
}

func fixtureMappingNames(expression ast.Expr, definitions map[string]ast.Expr) ([]string, bool, error) {
	expression = resolveExpression(expression, definitions)
	if call, ok := expression.(*ast.CallExpr); ok && selectorName(call.Fun) == "NewTableMappings" {
		if len(call.Args) != 1 {
			return nil, false, fmt.Errorf("NewTableMappings must receive the fixture destinations")
		}
		names, err := fixtureDestinationNames(call.Args[0], definitions)
		return names, true, err
	}
	mapping, ok := expression.(*ast.CompositeLit)
	if !ok {
		return nil, false, fmt.Errorf("Config.TableMappings must be a current literal or NewTableMappings call")
	}
	current := currentMappingVersion(fieldExpression(mapping, "Version"))
	destinations := fieldExpression(mapping, "Destinations")
	names, err := fixtureMappingDestinationNames(destinations, definitions)
	return names, current, err
}

func fixtureDestinationNames(expression ast.Expr, definitions map[string]ast.Expr) ([]string, error) {
	list, ok := resolveComposite(expression, definitions)
	if !ok {
		return nil, fmt.Errorf("destinations must be an explicit collection")
	}
	names := make([]string, 0, len(list.Elts))
	for _, element := range list.Elts {
		entry, ok := resolveComposite(element, definitions)
		if !ok {
			return nil, fmt.Errorf("destination entry must be explicit")
		}
		name, ok := stringLiteral(fieldExpression(entry, "Name"), definitions)
		if !ok || strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("destination name must be a nonempty literal")
		}
		names = append(names, name)
	}
	return names, nil
}

func fixtureMappingDestinationNames(expression ast.Expr, definitions map[string]ast.Expr) ([]string, error) {
	list, ok := resolveComposite(expression, definitions)
	if !ok {
		return nil, fmt.Errorf("mapping destinations must be an explicit collection")
	}
	names := make([]string, 0, len(list.Elts))
	for _, element := range list.Elts {
		entry, ok := resolveComposite(element, definitions)
		if !ok {
			return nil, fmt.Errorf("mapping destination entry must be explicit")
		}
		name, ok := stringLiteral(fieldExpression(entry, "Destination"), definitions)
		if !ok || strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("mapping destination name must be a nonempty literal")
		}
		names = append(names, name)
	}
	return names, nil
}

func currentMappingVersion(expression ast.Expr) bool {
	switch value := expression.(type) {
	case *ast.SelectorExpr:
		return value.Sel.Name == "TableMappingsVersion"
	case *ast.BasicLit:
		version, err := strconv.Atoi(value.Value)
		return err == nil && uint32(version) == internalflow.TableMappingsVersion
	default:
		return false
	}
}

func fieldExpression(literal *ast.CompositeLit, name string) ast.Expr {
	if literal == nil {
		return nil
	}
	for _, element := range literal.Elts {
		field, ok := element.(*ast.KeyValueExpr)
		if ok && identifierName(field.Key) == name {
			return field.Value
		}
	}
	return nil
}

func resolveComposite(expression ast.Expr, definitions map[string]ast.Expr) (*ast.CompositeLit, bool) {
	resolved := resolveExpression(expression, definitions)
	literal, ok := resolved.(*ast.CompositeLit)
	return literal, ok
}

func resolveExpression(expression ast.Expr, definitions map[string]ast.Expr) ast.Expr {
	visited := map[string]bool{}
	for {
		identifier, ok := expression.(*ast.Ident)
		if !ok || visited[identifier.Name] {
			return expression
		}
		replacement, exists := definitions[identifier.Name]
		if !exists {
			return expression
		}
		visited[identifier.Name] = true
		expression = replacement
	}
}

func stringLiteral(expression ast.Expr, definitions map[string]ast.Expr) (string, bool) {
	expression = resolveExpression(expression, definitions)
	if selector, ok := expression.(*ast.SelectorExpr); ok {
		if owner, valid := resolveComposite(selector.X, definitions); valid {
			return stringLiteral(fieldExpression(owner, selector.Sel.Name), definitions)
		}
	}
	value, ok := expression.(*ast.BasicLit)
	if !ok || value.Kind != token.STRING {
		return "", false
	}
	decoded, err := strconv.Unquote(value.Value)
	return decoded, err == nil
}

func sameNames(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	counts := map[string]int{}
	for _, name := range left {
		counts[name]++
	}
	for _, name := range right {
		counts[name]--
	}
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}
	return true
}

func assertFixtureManifest(t *testing.T, name string, found, expected map[string]int) {
	t.Helper()
	if reflect.DeepEqual(found, expected) {
		return
	}
	paths := make([]string, 0, len(found))
	for path := range found {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	t.Fatalf("%s manifest mismatch\nfound=%v\nwant=%v", name, found, expected)
}

func expressionDefinitions(root ast.Node) map[string]ast.Expr {
	definitions := map[string]ast.Expr{}
	ast.Inspect(root, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.AssignStmt:
			for index, left := range value.Lhs {
				if index >= len(value.Rhs) {
					break
				}
				if identifier, ok := left.(*ast.Ident); ok {
					definitions[identifier.Name] = value.Rhs[index]
				}
			}
		case *ast.ValueSpec:
			for index, name := range value.Names {
				if index < len(value.Values) {
					definitions[name.Name] = value.Values[index]
				}
			}
		}
		return true
	})
	return definitions
}

func removedFlowOptions(literal *ast.CompositeLit, definitions map[string]ast.Expr) []string {
	removed := map[string]struct{}{
		"schema": {}, "table": {}, "database": {}, "write_mode": {}, "append_mode": {},
		"soft_delete": {}, "meta_enabled": {}, "meta_synced_at": {}, "meta_deleted": {},
		"meta_watermark": {}, "meta_op": {}, "watermark_source": {}, "namespace": {},
		"table_prefix": {}, "fixed_table": {}, "target_namespace": {}, "target_table": {},
	}
	found := map[string]struct{}{}
	visited := map[string]bool{}
	var inspect func(ast.Node)
	inspect = func(root ast.Node) {
		ast.Inspect(root, func(node ast.Node) bool {
			if identifier, ok := node.(*ast.Ident); ok && !visited[identifier.Name] {
				if definition, exists := definitions[identifier.Name]; exists {
					visited[identifier.Name] = true
					inspect(definition)
				}
			}
			field, ok := node.(*ast.KeyValueExpr)
			if !ok {
				return true
			}
			key, ok := field.Key.(*ast.BasicLit)
			if !ok || key.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(key.Value)
			if err == nil {
				if _, obsolete := removed[value]; obsolete {
					found[value] = struct{}{}
				}
			}
			return true
		})
	}
	inspect(literal)
	options := make([]string, 0, len(found))
	for option := range found {
		options = append(options, option)
	}
	sort.Strings(options)
	return options
}

func identifierName(expression ast.Expr) string {
	identifier, _ := expression.(*ast.Ident)
	if identifier == nil {
		return ""
	}
	return identifier.Name
}

func selectorName(expression ast.Expr) string {
	selector, _ := expression.(*ast.SelectorExpr)
	if selector == nil {
		return ""
	}
	return selector.Sel.Name
}

func isFlowComposite(expr ast.Expr) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Flow" {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == "flow"
}

func isFlowConfigPayloadComposite(expr ast.Expr) bool {
	identifier, ok := expr.(*ast.Ident)
	return ok && identifier.Name == "flowConfigPayload"
}
