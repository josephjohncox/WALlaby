package main

import (
	"bytes"
	"errors"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"golang.org/x/tools/go/packages"
)

func TestRenderIsDeterministicAndComplete(t *testing.T) {
	_, version, err := moduleRootAndVersion()
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []target{
		{importPath: upstreamModulePath, packageName: "avro"},
		{importPath: upstreamModulePath + "/ocf", packageName: "ocf"},
	} {
		t.Run(target.packageName, func(t *testing.T) {
			first, err := render(target, version)
			if err != nil {
				t.Fatal(err)
			}
			second, err := render(target, version)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(first, second) {
				t.Fatal("successive renders differ")
			}
			assertGeneratedSurface(t, first, target.importPath)
		})
	}
}

func TestCheckOutputRejectsMissingAndStaleFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "avro_shim.go")
	if err := checkOutput(path, []byte("generated")); err == nil {
		t.Fatal("missing generated output passed check")
	}
	if err := os.WriteFile(path, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := checkOutput(path, []byte("generated")); err == nil {
		t.Fatal("stale generated output passed check")
	}
	if err := os.WriteFile(path, []byte("generated"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := checkOutput(path, []byte("generated")); err != nil {
		t.Fatalf("byte-identical output failed check: %v", err)
	}
}

func TestFingerprintTracksAPIInsteadOfImplementationNoise(t *testing.T) {
	base := fingerprintSource(t, `package sample
		type Record struct{ Value string }
		func Parse(value string) Record { return Record{Value: value} }
	`)
	bodyOnly := fingerprintSource(t, `package sample
		// Record is intentionally documented differently.
		type Record struct{ Value string }
		func Parse(value string) Record { panic("body does not affect API") }
	`)
	changed := fingerprintSource(t, `package sample
		type Record struct{ Value string }
		func Parse(value []byte) Record { return Record{} }
	`)
	if base != bodyOnly {
		t.Fatalf("implementation-only change altered API fingerprint: %s != %s", base, bodyOnly)
	}
	if base == changed {
		t.Fatal("signature change did not alter API fingerprint")
	}
}

func TestInspectPackageRejectsUnsupportedGenerics(t *testing.T) {
	for _, source := range []string{
		`package sample; func Parse[T any](value T) T { return value }`,
		`package sample; type Record[T any] struct{ Value T }`,
	} {
		files := token.NewFileSet()
		file, err := parser.ParseFile(files, "generic.go", source, 0)
		if err != nil {
			t.Fatal(err)
		}
		pkg, err := (&types.Config{Importer: importer.Default()}).Check("example.test/sample", files, []*ast.File{file}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := inspectPackage(pkg); err == nil {
			t.Fatalf("unsupported generic API was silently accepted: %s", source)
		}
	}
}

func fingerprintSource(t *testing.T, source string) string {
	t.Helper()
	files := token.NewFileSet()
	file, err := parser.ParseFile(files, "sample.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	pkg, err := (&types.Config{Importer: importer.Default()}).Check("example.test/sample", files, []*ast.File{file}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, fingerprint, err := inspectPackage(pkg)
	if err != nil {
		t.Fatal(err)
	}
	return fingerprint
}

func assertGeneratedSurface(t *testing.T, generated []byte, importPath string) {
	t.Helper()
	files := token.NewFileSet()
	file, err := parser.ParseFile(files, "shim.go", generated, 0)
	if err != nil {
		t.Fatal(err)
	}
	var generatedNames []string
	for _, declaration := range file.Decls {
		switch typed := declaration.(type) {
		case *ast.FuncDecl:
			t.Fatalf("generated shim copied a function body for %s", typed.Name.Name)
		case *ast.GenDecl:
			if typed.Tok == token.IMPORT {
				continue
			}
			for _, specification := range typed.Specs {
				switch spec := specification.(type) {
				case *ast.TypeSpec:
					if !spec.Assign.IsValid() {
						t.Fatalf("generated type %s is not an alias", spec.Name.Name)
					}
					generatedNames = append(generatedNames, spec.Name.Name)
				case *ast.ValueSpec:
					for index, name := range spec.Names {
						selector, ok := spec.Values[index].(*ast.SelectorExpr)
						identifier, identifierOK := selector.X.(*ast.Ident)
						if !ok || !identifierOK || identifier.Name != "upstream" || selector.Sel.Name != name.Name {
							t.Fatalf("generated value %s is not a direct same-name upstream reference", name.Name)
						}
						generatedNames = append(generatedNames, name.Name)
					}
				}
			}
		}
	}

	loaded, err := loadOne(importPath)
	if err != nil {
		t.Fatal(err)
	}
	var upstreamNames []string
	for _, name := range loaded.Scope().Names() {
		if token.IsExported(name) {
			upstreamNames = append(upstreamNames, name)
		}
	}
	sort.Strings(generatedNames)
	sort.Strings(upstreamNames)
	if len(generatedNames) != len(upstreamNames) {
		t.Fatalf("generated names=%d, upstream names=%d", len(generatedNames), len(upstreamNames))
	}
	for index := range upstreamNames {
		if generatedNames[index] != upstreamNames[index] {
			t.Fatalf("generated surface differs at %d: %q != %q", index, generatedNames[index], upstreamNames[index])
		}
	}
}

func loadOne(importPath string) (*types.Package, error) {
	loaded, err := packages.Load(&packages.Config{Mode: packages.NeedTypes | packages.NeedImports | packages.NeedDeps}, importPath)
	if err != nil {
		return nil, err
	}
	if packages.PrintErrors(loaded) != 0 || len(loaded) != 1 || loaded[0].Types == nil {
		return nil, errors.New("package load failed")
	}
	return loaded[0].Types, nil
}
