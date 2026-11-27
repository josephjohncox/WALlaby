package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// clickHouseAdversarialNames are legal source names that are hostile to
// ClickHouse's backtick identifier syntax, where a backslash is an escape.
var clickHouseAdversarialNames = []string{
	"plain",
	`trailing\`,
	`\`,
	`\\`,
	"\\`; DROP TABLE victim; --",
	"\\`) SELECT secret FROM other --",
	"a`b",
	"`",
	"``",
	`a\nb`,
	"a\nb",
	"?",
	"$1",
	"@name",
	"-- comment",
	"/* block */",
	"quote'single",
	`quote"double`,
	"dotted.name",
	"名前",
}

// lexClickHouseStatement tokenizes a statement with ClickHouse's quoted
// identifier rules (a backslash escapes the next byte and a doubled delimiter
// is a literal delimiter). It returns the statement with each identifier
// replaced by "<id>" and the decoded identifiers in order.
func lexClickHouseStatement(statement string) (string, []string, error) {
	var skeleton strings.Builder
	var identifiers []string
	for i := 0; i < len(statement); i++ {
		if statement[i] != '`' {
			skeleton.WriteByte(statement[i])
			continue
		}
		var identifier strings.Builder
		closed := false
		for i++; i < len(statement); i++ {
			switch {
			case statement[i] == '\\' && i+1 < len(statement):
				i++
				identifier.WriteByte(statement[i])
			case statement[i] == '`' && i+1 < len(statement) && statement[i+1] == '`':
				i++
				identifier.WriteByte('`')
			case statement[i] == '`':
				closed = true
			default:
				identifier.WriteByte(statement[i])
			}
			if closed {
				break
			}
		}
		if !closed {
			return "", nil, fmt.Errorf("unterminated identifier in %q", statement)
		}
		skeleton.WriteString("<id>")
		identifiers = append(identifiers, identifier.String())
	}
	return skeleton.String(), identifiers, nil
}

func TestQuoteIdentEscapesClickHouseBackslashAndDelimiter(t *testing.T) {
	for _, tc := range []struct{ name, input, want string }{
		{"plain", "events", "`events`"},
		{"delimiter", "a`b", "`a``b`"},
		{"trailing backslash", `a\`, "`a\\\\`"},
		{"backslash then delimiter", "\\`", "`\\\\``" + "`"},
		{"empty", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := quoteIdent(tc.input, '`'); got != tc.want {
				t.Fatalf("quoteIdent(%q)=%q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestQuoteIdentRoundTripsThroughClickHouseLexer(t *testing.T) {
	for _, name := range clickHouseAdversarialNames {
		t.Run(name, func(t *testing.T) {
			skeleton, identifiers, err := lexClickHouseStatement("SELECT " + quoteIdent(name, '`') + " FROM t")
			if err != nil {
				t.Fatal(err)
			}
			if skeleton != "SELECT <id> FROM t" || !reflect.DeepEqual(identifiers, []string{name}) {
				t.Fatalf("skeleton=%q identifiers=%q, want one identifier %q", skeleton, identifiers, name)
			}
		})
	}
}

// legacyQuoteIdent is the delimiter-doubling-only quoting that ClickHouse
// mis-parses; it keeps the lexer assertions above non-vacuous.
func legacyQuoteIdent(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func TestLegacyDelimiterOnlyQuotingEscapesClickHouseIdentifier(t *testing.T) {
	for _, name := range []string{`trailing\`, "\\`; DROP TABLE victim; --"} {
		skeleton, identifiers, err := lexClickHouseStatement("SELECT " + legacyQuoteIdent(name) + " FROM t")
		if err == nil && skeleton == "SELECT <id> FROM t" && reflect.DeepEqual(identifiers, []string{name}) {
			t.Fatalf("legacy quoting of %q unexpectedly round-trips", name)
		}
	}
}

func TestResolveStagingTableQuotesAdversarialIdentifiers(t *testing.T) {
	for _, resolve := range []string{batchResolveAppend, batchResolveReplace} {
		t.Run(resolve, func(t *testing.T) {
			var statements []string
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(func(_, actual string) error {
				statements = append(statements, actual)
				return nil
			})))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			namespace, table := "\\`; DROP DATABASE victim; --", `tbl\`
			columns := []connector.Column{{Name: `c1\`}, {Name: "\\`) SELECT secret FROM other --"}, {Name: "?"}}
			expectedStatements := 1
			if resolve == batchResolveReplace {
				expectedStatements = 2
				mock.ExpectExec("TRUNCATE").WillReturnResult(sqlmock.NewResult(0, 0))
			}
			mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(0, 1))
			destination := &Destination{db: db, batchResolve: resolve, stagingSuffix: "_staging"}
			info := tableInfo{schema: connector.Schema{Namespace: namespace, Columns: columns}, table: table}
			if err := destination.resolveStagingTable(context.Background(), info); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
			if len(statements) != expectedStatements {
				t.Fatalf("statements=%q", statements)
			}
			insert := statements[len(statements)-1]
			skeleton, identifiers, err := lexClickHouseStatement(insert)
			if err != nil {
				t.Fatal(err)
			}
			wantSkeleton := "INSERT INTO <id>.<id> (<id>, <id>, <id>) SELECT <id>, <id>, <id> FROM <id>.<id>"
			columnNames := []string{columns[0].Name, columns[1].Name, columns[2].Name}
			wantIdentifiers := append([]string{namespace, table}, columnNames...)
			wantIdentifiers = append(wantIdentifiers, columnNames...)
			wantIdentifiers = append(wantIdentifiers, namespace, table+"_staging")
			if skeleton != wantSkeleton || !reflect.DeepEqual(identifiers, wantIdentifiers) {
				t.Fatalf("insert=%q\nskeleton=%q\nidentifiers=%q", insert, skeleton, identifiers)
			}
			if resolve == batchResolveReplace {
				skeleton, identifiers, err := lexClickHouseStatement(statements[0])
				if err != nil {
					t.Fatal(err)
				}
				if skeleton != "TRUNCATE TABLE <id>.<id>" || !reflect.DeepEqual(identifiers, []string{namespace, table}) {
					t.Fatalf("truncate=%q skeleton=%q identifiers=%q", statements[0], skeleton, identifiers)
				}
			}
		})
	}
}

func TestResolveStagingTablePropagatesExecError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	execErr := errors.New("insert failed")
	mock.ExpectExec("INSERT").WillReturnError(execErr)
	destination := &Destination{db: db, batchResolve: batchResolveAppend, stagingSuffix: "_staging"}
	info := tableInfo{schema: connector.Schema{Namespace: "ns", Columns: []connector.Column{{Name: `c\`}}}, table: "t"}
	err = destination.resolveStagingTable(context.Background(), info)
	if !errors.Is(err, execErr) || !strings.Contains(err.Error(), "resolve staging") {
		t.Fatalf("resolveStagingTable() error = %v", err)
	}
}

func TestInsertRowQuotesAdversarialColumnsAndBindsValues(t *testing.T) {
	var statements []string
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(func(_, actual string) error {
		statements = append(statements, actual)
		return nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	column := "\\`) VALUES (1); DROP TABLE victim; --"
	payload := "'); DROP TABLE victim; --"
	mock.ExpectExec("INSERT").WithArgs(payload).WillReturnResult(sqlmock.NewResult(0, 1))
	destination := &Destination{db: db}
	schema := connector.Schema{Namespace: "ns", Name: "t", Columns: []connector.Column{{Name: column, Type: "text"}}}
	record := connector.Record{Table: "t", Operation: connector.OpInsert, After: map[string]any{column: payload}}
	if err := destination.insertRow(context.Background(), destination.targetTable(schema, record), schema, record); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	skeleton, identifiers, err := lexClickHouseStatement(statements[0])
	if err != nil {
		t.Fatal(err)
	}
	if skeleton != "INSERT INTO <id>.<id> (<id>) VALUES (?)" || !reflect.DeepEqual(identifiers, []string{"ns", "t", column}) {
		t.Fatalf("statement=%q skeleton=%q identifiers=%q", statements[0], skeleton, identifiers)
	}
}

func TestEnsureMetaTableQuotesAdversarialMetaIdentifiers(t *testing.T) {
	var statements []string
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(func(_, actual string) error {
		statements = append(statements, actual)
		return nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema, table := "\\`; DROP DATABASE victim; --", `meta\`
	mock.ExpectExec("CREATE DATABASE").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT").WithArgs(schema, table).WillReturnRows(sqlmock.NewRows([]string{"name"}))
	destination := &Destination{db: db, metaSchema: schema, metaTable: table, metaLayout: mustCompileMetaLayout(t, "", "")}
	if err := destination.ensureMetaTable(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	skeleton, identifiers, err := lexClickHouseStatement(statements[0])
	if err != nil || skeleton != "CREATE DATABASE IF NOT EXISTS <id>" || !reflect.DeepEqual(identifiers, []string{schema}) {
		t.Fatalf("create database=%q skeleton=%q identifiers=%q err=%v", statements[0], skeleton, identifiers, err)
	}
	skeleton, identifiers, err = lexClickHouseStatement(statements[1])
	wantIdentifiers := []string{schema, table, "flow_id", "source_schema", "source_table", "key_json"}
	if err != nil || !strings.HasPrefix(skeleton, "CREATE TABLE IF NOT EXISTS <id>.<id> (") || !strings.HasSuffix(skeleton, ") ENGINE = MergeTree ORDER BY (<id>, <id>, <id>, <id>)") || !reflect.DeepEqual(identifiers, wantIdentifiers) {
		t.Fatalf("create table=%q skeleton=%q identifiers=%q err=%v", statements[1], skeleton, identifiers, err)
	}
}
