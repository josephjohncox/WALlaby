package postgres

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type recordingDDLExecer struct {
	statements []string
}

func (e *recordingDDLExecer) Exec(_ context.Context, statement string, _ ...any) (pgconn.CommandTag, error) {
	e.statements = append(e.statements, statement)
	return pgconn.NewCommandTag("ALTER TABLE"), nil
}

func TestApplyDDLExecutesTranslatedStatement(t *testing.T) {
	executor := &recordingDDLExecer{}
	destination := &Destination{ddlExecutor: executor, spec: connector.RuntimeSpec{Type: connector.EndpointPostgres}}
	plan, err := json.Marshal(internalschema.Plan{Changes: []internalschema.Change{{Type: internalschema.ChangeAddColumn, Namespace: "mapped", Table: "events", Column: "status", ToType: "text", Nullable: false}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := destination.ApplyDDL(context.Background(), connector.Schema{Namespace: "mapped", Name: "events"}, connector.Record{Operation: connector.OpDDL, DDLPlan: plan}); err != nil {
		t.Fatal(err)
	}
	if len(executor.statements) != 1 || executor.statements[0] != `ALTER TABLE "mapped"."events" ADD COLUMN "status" text NOT NULL` {
		t.Fatalf("executed statements=%q", executor.statements)
	}
}

func TestCapabilitiesDeclarePerTableWritePolicies(t *testing.T) {
	capabilities := (&Destination{}).Capabilities()
	if !capabilities.TableWrites.Append || !capabilities.TableWrites.Upsert || !capabilities.TableWrites.ExplicitKey || !capabilities.TableWrites.WatermarkGuard {
		t.Fatalf("postgres table write contract incomplete: %+v", capabilities.TableWrites)
	}
	if capabilities.Delivery.ReplaySafe || capabilities.Delivery.IdempotentReplay {
		t.Fatalf("mixed per-table append policy must not be globally advertised as replay safe: %+v", capabilities.Delivery)
	}
	if err := capabilities.SupportsTablePolicy(connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}, WatermarkColumn: "updated_at"}); err != nil {
		t.Fatal(err)
	}
}
