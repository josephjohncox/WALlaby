package tests

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/schemabaseline"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func noAutomaticDDLDefaults() *flow.DDLPolicyDefaults {
	return &flow.DDLPolicyDefaults{}
}

func noAutomaticDDLPolicy() flow.DDLPolicy {
	autoApply := false
	return flow.DDLPolicy{AutoApply: &autoApply}
}

func newManagedSchemaBaselines(pool *pgxpool.Pool) (connector.ManagedSchemaBaselineStore, error) {
	return schemabaseline.NewStore(pool)
}

func managedBaselinePayload(t *testing.T, transaction connector.SourceTransaction) connector.ManagedSchemaBaselinePayload {
	t.Helper()
	payload, err := connector.NewManagedSchemaBaselinePayload(transaction.SourceLineageID, connector.SourceTransactionSchemas(transaction))
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func emptyManagedBaselinePayload(t *testing.T, lineage string) connector.ManagedSchemaBaselinePayload {
	t.Helper()
	payload, err := connector.NewManagedSchemaBaselinePayload(lineage, nil)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func mustManagedSchemaBaselines(t *testing.T, pool *pgxpool.Pool) connector.ManagedSchemaBaselineStore {
	t.Helper()
	store, err := newManagedSchemaBaselines(pool)
	if err != nil {
		t.Fatal(err)
	}
	return store
}
