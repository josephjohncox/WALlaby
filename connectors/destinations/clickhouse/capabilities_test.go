package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type recordingDDLExecutor struct {
	statements []string
}

func (e *recordingDDLExecutor) ExecContext(_ context.Context, statement string, _ ...any) (sql.Result, error) {
	e.statements = append(e.statements, statement)
	return ddlResult(1), nil
}

type ddlResult int64

func (r ddlResult) LastInsertId() (int64, error) { return 0, errors.New("unsupported") }
func (r ddlResult) RowsAffected() (int64, error) { return int64(r), nil }

func clickHouseAddColumnRecord(t *testing.T) (connector.Schema, connector.Record) {
	t.Helper()
	plan, err := json.Marshal(internalschema.Plan{Changes: []internalschema.Change{{Type: internalschema.ChangeAddColumn, Namespace: "analytics", Table: "events", Column: "status", ToType: "text", Nullable: false}}})
	if err != nil {
		t.Fatal(err)
	}
	return connector.Schema{Namespace: "analytics", Name: "events"}, connector.Record{Operation: connector.OpDDL, Table: "events", DDLPlan: plan}
}

func TestGenericAppendWriteExecutesInsert(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	destination := &Destination{db: db, metaEnabled: false}
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `mapped`.`events` (`event_id`) VALUES (?)")).WithArgs(int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
	batch := connector.Batch{
		Schema:      connector.Schema{Namespace: "mapped", Name: "events", Columns: []connector.Column{{Name: "event_id", Type: "int8"}}},
		WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend},
		Records:     []connector.Record{{Table: "events", Operation: connector.OpInsert, After: map[string]any{"event_id": int64(7)}}},
	}
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGenericApplyDDLExecutesTranslatedStatement(t *testing.T) {
	executor := &recordingDDLExecutor{}
	destination := &Destination{ddlExecutor: executor, spec: connector.Spec{Type: connector.EndpointClickHouse}}
	spec := connector.Spec{Type: connector.EndpointClickHouse}
	capabilities, err := destination.CapabilitiesFor(spec)
	if err != nil {
		t.Fatal(err)
	}
	if !capabilities.Delivery.ExecutesDDL {
		t.Fatal("generic ClickHouse must advertise executed DDL")
	}
	schema, record := clickHouseAddColumnRecord(t)
	if err := destination.ApplyDDL(context.Background(), schema, record); err != nil {
		t.Fatal(err)
	}
	if len(executor.statements) != 1 || executor.statements[0] != "ALTER TABLE `analytics`.`events` ADD COLUMN `status` String NOT NULL" {
		t.Fatalf("executed DDL=%q", executor.statements)
	}
}

func TestManagedApplyDDLRejectsBeforeExecutor(t *testing.T) {
	executor := &recordingDDLExecutor{}
	spec := connector.Spec{Type: connector.EndpointClickHouse, Options: map[string]string{"managed_profile": connector.ManagedProfilePostgresToClickHouseAppendV1}}
	destination := &Destination{ddlExecutor: executor, managedProfile: connector.ManagedProfilePostgresToClickHouseAppendV1}
	capabilities, err := destination.CapabilitiesFor(spec)
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.Delivery.ExecutesDDL {
		t.Fatal("managed ClickHouse must not advertise executed DDL")
	}
	schema, record := clickHouseAddColumnRecord(t)
	err = destination.ApplyDDL(context.Background(), schema, record)
	if err == nil || !strings.Contains(err.Error(), "never executes target DDL") {
		t.Fatalf("managed ApplyDDL error=%v", err)
	}
	if len(executor.statements) != 0 {
		t.Fatalf("managed ApplyDDL invoked executor: %q", executor.statements)
	}
}
