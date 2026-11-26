package snowflake

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

var snowflakeAdversarialNames = []string{
	"PLAIN",
	`x"; DROP TABLE victim; --`,
	`"`,
	`""`,
	`trailing\`,
	`\"; DROP TABLE victim; --`,
	"?",
	"$1",
	`'quoted'`,
	"new\nline",
	"-- comment",
	"/* block */",
	"semi;colon",
	"名前",
}

// lexSnowflakeStatement tokenizes a statement with Snowflake's delimited
// identifier rules: only a doubled double quote is an escape and a backslash
// is an ordinary character. It returns the statement with each identifier
// replaced by "<id>" and the decoded identifiers in order.
func lexSnowflakeStatement(statement string) (string, []string, error) {
	var skeleton strings.Builder
	var identifiers []string
	for i := 0; i < len(statement); i++ {
		if statement[i] != '"' {
			skeleton.WriteByte(statement[i])
			continue
		}
		var identifier strings.Builder
		closed := false
		for i++; i < len(statement) && !closed; i++ {
			switch {
			case statement[i] == '"' && i+1 < len(statement) && statement[i+1] == '"':
				identifier.WriteByte('"')
				i++
			case statement[i] == '"':
				closed = true
				i--
			default:
				identifier.WriteByte(statement[i])
			}
		}
		if !closed {
			return "", nil, errors.New("unterminated identifier in " + statement)
		}
		skeleton.WriteString("<id>")
		identifiers = append(identifiers, identifier.String())
	}
	return skeleton.String(), identifiers, nil
}

func TestQuoteIdentRoundTripsThroughSnowflakeLexer(t *testing.T) {
	for _, name := range snowflakeAdversarialNames {
		t.Run(name, func(t *testing.T) {
			skeleton, identifiers, err := lexSnowflakeStatement("SELECT " + quoteIdent(name, '"') + " FROM t")
			if err != nil {
				t.Fatal(err)
			}
			if skeleton != "SELECT <id> FROM t" || !reflect.DeepEqual(identifiers, []string{name}) {
				t.Fatalf("skeleton=%q identifiers=%q, want one identifier %q", skeleton, identifiers, name)
			}
		})
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
			namespace, table := `S"; DROP SCHEMA victim; --`, `T\`
			columns := []string{`C1\`, `C"); DROP TABLE victim; --`, "?"}
			schema := connector.Schema{Namespace: namespace}
			for _, column := range columns {
				schema.Columns = append(schema.Columns, connector.Column{Name: column, Type: "text"})
			}
			if resolve == batchResolveReplace {
				mock.ExpectExec("TRUNCATE").WillReturnResult(sqlmock.NewResult(0, 0))
			}
			mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(0, 1))
			destination := &Destination{db: db, batchResolve: resolve, stagingSuffix: "_STAGING"}
			if err := destination.resolveStagingTable(context.Background(), tableInfo{schema: schema, table: table}); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
			wantStatements := 1
			if resolve == batchResolveReplace {
				wantStatements = 2
			}
			if len(statements) != wantStatements {
				t.Fatalf("statements=%q", statements)
			}
			skeleton, identifiers, err := lexSnowflakeStatement(statements[len(statements)-1])
			if err != nil {
				t.Fatal(err)
			}
			wantIdentifiers := append([]string{namespace, table}, columns...)
			wantIdentifiers = append(wantIdentifiers, columns...)
			wantIdentifiers = append(wantIdentifiers, namespace, table+"_STAGING")
			wantSkeleton := "INSERT INTO <id>.<id> (<id>, <id>, <id>) SELECT <id>, <id>, <id> FROM <id>.<id>"
			if skeleton != wantSkeleton || !reflect.DeepEqual(identifiers, wantIdentifiers) {
				t.Fatalf("insert=%q\nskeleton=%q\nidentifiers=%q", statements[len(statements)-1], skeleton, identifiers)
			}
			if resolve == batchResolveReplace {
				skeleton, identifiers, err := lexSnowflakeStatement(statements[0])
				if err != nil || skeleton != "TRUNCATE TABLE <id>.<id>" || !reflect.DeepEqual(identifiers, []string{namespace, table}) {
					t.Fatalf("truncate=%q skeleton=%q identifiers=%q err=%v", statements[0], skeleton, identifiers, err)
				}
			}
		})
	}
}

func TestResolveStagingTableQuotesAdversarialStagingOption(t *testing.T) {
	var statements []string
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(func(_, actual string) error {
		statements = append(statements, actual)
		return nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(0, 1))
	stagingSchema, stagingTable := `STG"; DROP TABLE victim; --`, `ROWS"; DROP TABLE victim; --`
	destination := &Destination{db: db, batchResolve: batchResolveAppend, stagingTableName: stagingSchema + "." + stagingTable}
	schema := connector.Schema{Columns: []connector.Column{{Name: "ID", Type: "int4"}}}
	if err := destination.resolveStagingTable(context.Background(), tableInfo{schema: schema, table: "EVENTS"}); err != nil {
		t.Fatal(err)
	}
	skeleton, identifiers, err := lexSnowflakeStatement(statements[0])
	wantIdentifiers := []string{"EVENTS", "ID", "ID", stagingSchema, stagingTable}
	if err != nil || skeleton != "INSERT INTO <id> (<id>) SELECT <id> FROM <id>.<id>" || !reflect.DeepEqual(identifiers, wantIdentifiers) {
		t.Fatalf("insert=%q skeleton=%q identifiers=%q err=%v", statements[0], skeleton, identifiers, err)
	}
}

func TestResolveStagingTablePropagatesExecFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	execErr := errors.New("insert failed")
	mock.ExpectExec("INSERT").WillReturnError(execErr)
	destination := &Destination{db: db, batchResolve: batchResolveAppend, stagingSuffix: "_STAGING"}
	schema := connector.Schema{Namespace: "S", Columns: []connector.Column{{Name: "ID", Type: "int4"}}}
	err = destination.resolveStagingTable(context.Background(), tableInfo{schema: schema, table: "T"})
	if !errors.Is(err, execErr) || !strings.Contains(err.Error(), "resolve staging") {
		t.Fatalf("resolveStagingTable() error = %v", err)
	}
}
