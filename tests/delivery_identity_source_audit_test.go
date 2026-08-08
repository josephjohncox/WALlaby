package tests

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestEveryDeliveryIntentConstructionDeclaresLogicalBatchID(t *testing.T) {
	root := deliveryAuditRoot(t)
	files := token.NewFileSet()
	var constructors int
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			name := entry.Name()
			if path != root && (strings.HasPrefix(name, ".") || name == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		parsed, err := parser.ParseFile(files, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok || deliveryIntentTypeName(literal.Type) != "DeliveryIntent" {
				return true
			}
			constructors++
			hasLogical := false
			for _, element := range literal.Elts {
				field, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if identifier, ok := field.Key.(*ast.Ident); ok && identifier.Name == "LogicalBatchID" {
					hasLogical = true
					if value, ok := field.Value.(*ast.BasicLit); ok && value.Value == `""` {
						t.Errorf("%s constructs DeliveryIntent with empty LogicalBatchID", files.Position(value.Pos()))
					}
				}
			}
			if !hasLogical {
				t.Errorf("%s constructs DeliveryIntent without LogicalBatchID", files.Position(literal.Pos()))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if constructors < 10 {
		t.Fatalf("audited only %d DeliveryIntent constructors", constructors)
	}
}

func TestDeliveryRuntimeContainsNoLegacyIdentityFabrication(t *testing.T) {
	root := deliveryAuditRoot(t)
	paths := []string{"pkg/connector", "internal/delivery", "internal/bootstrap", "pkg/stream", "connectors/destinations/postgres", "connectors/destinations/clickhouse", "connectors/destinations/snowflake"}
	for _, directory := range paths {
		err := filepath.WalkDir(filepath.Join(root, directory), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			contents, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			source := string(contents)
			for _, forbidden := range []string{`return "legacy:`, `deliveryLogicalBatchID(`} {
				if strings.Contains(source, forbidden) {
					t.Errorf("runtime identity fabrication token %q remains in %s", forbidden, path)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestManagedPostgresReceiptsAreNotHeuristicallyPrunedAndUseBothKeys(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join(deliveryAuditRoot(t), "connectors", "destinations", "postgres", "managed_driver.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(contents)
	if strings.Contains(source, "DELETE FROM wallaby_meta.__delivery_receipts") {
		t.Fatal("managed PostgreSQL runtime still prunes immutable delivery receipts")
	}
	for _, required := range []string{"logical_batch_id=$3 OR position_id=$4", "ROLLBACK TO SAVEPOINT wallaby_managed_receipt_insert"} {
		if !strings.Contains(source, required) {
			t.Fatalf("managed PostgreSQL receipt protocol missing %q", required)
		}
	}
}

func deliveryIntentTypeName(expression ast.Expr) string {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		return value.Sel.Name
	default:
		return ""
	}
}
func deliveryAuditRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate repository root")
	}
	return filepath.Dir(filepath.Dir(filename))
}
