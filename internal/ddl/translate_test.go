package ddl

import (
	"strings"
	"testing"
)

func TestTranslatePostgresDDL_PreservesQuotedCase(t *testing.T) {
	ddl := `CREATE TABLE "CaseSchema"."CaseTable" ("MyCol" TEXT, unquoted INT)`
	stmts, err := TranslatePostgresDDL(ddl, DialectConfigFor(DialectSnowflake), nil)
	if err != nil {
		t.Fatalf("translate ddl: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt := stmts[0]
	if !strings.Contains(stmt, `"CaseSchema"."CaseTable"`) {
		t.Fatalf("expected quoted schema/table preserved: %s", stmt)
	}
	if !strings.Contains(stmt, `"MyCol"`) {
		t.Fatalf("expected quoted column preserved: %s", stmt)
	}
}

func TestTranslatePostgresDDL_UnquotedFolded(t *testing.T) {
	ddl := `CREATE TABLE FooBar (Bar INT)`
	stmts, err := TranslatePostgresDDL(ddl, DialectConfigFor(DialectSnowflake), nil)
	if err != nil {
		t.Fatalf("translate ddl: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt := stmts[0]
	if !strings.Contains(stmt, `"foobar"`) {
		t.Fatalf("expected unquoted table folded to lower-case: %s", stmt)
	}
	if !strings.Contains(stmt, `"bar"`) {
		t.Fatalf("expected unquoted column folded to lower-case: %s", stmt)
	}
}

func TestTranslatePostgresDDL_TypeMappings(t *testing.T) {
	ddl := `CREATE TABLE foo (id bigint, payload jsonb, tags text[], amount numeric(12,2))`
	mappings := map[string]string{
		"bigint":  "NUMBER",
		"jsonb":   "VARIANT",
		"text":    "STRING",
		"numeric": "NUMBER",
	}
	stmts, err := TranslatePostgresDDL(ddl, DialectConfigFor(DialectSnowflake), mappings)
	if err != nil {
		t.Fatalf("translate ddl: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt := stmts[0]
	if !strings.Contains(stmt, `"id" NUMBER`) {
		t.Fatalf("expected id mapped to NUMBER: %s", stmt)
	}
	if !strings.Contains(stmt, `"payload" VARIANT`) {
		t.Fatalf("expected payload mapped to VARIANT: %s", stmt)
	}
	if !strings.Contains(stmt, `"tags" ARRAY`) {
		t.Fatalf("expected tags mapped to ARRAY: %s", stmt)
	}
	if !strings.Contains(stmt, `"amount" NUMBER(12,2)`) {
		t.Fatalf("expected numeric suffix preserved: %s", stmt)
	}
}
