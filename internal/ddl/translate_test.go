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
