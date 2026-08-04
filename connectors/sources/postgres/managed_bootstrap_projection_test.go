package postgres

import (
	"testing"

	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type bootstrapProjectionStub struct{ fingerprint string }

func (p bootstrapProjectionStub) Fingerprint() string { return p.fingerprint }
func (p bootstrapProjectionStub) IncludeBootstrapRelation(_ string, table string) (bool, error) {
	return table != "excluded", nil
}
func (p bootstrapProjectionStub) ProjectBootstrapSchema(schema connector.Schema) (connector.Schema, connector.TableWritePolicy, bool, error) {
	if schema.Name == "excluded" {
		return connector.Schema{}, connector.TableWritePolicy{}, false, nil
	}
	schema.Namespace, schema.Name = "mapped", "dst_"+schema.Name
	return schema, connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend, ProjectionFingerprint: p.fingerprint}, true, nil
}
func (p bootstrapProjectionStub) ProjectBootstrapBatch(batch connector.Batch) (connector.Batch, bool, error) {
	mapped, policy, included, err := p.ProjectBootstrapSchema(batch.Schema)
	if err != nil || !included {
		return connector.Batch{}, included, err
	}
	batch.Schema, batch.WritePolicy = mapped, policy
	for index := range batch.Records {
		batch.Records[index].Table = mapped.Name
	}
	return batch, true, nil
}

func TestManagedBootstrapFiltersNamesBeforeEligibilityAndMaxAccounting(t *testing.T) {
	projector := bootstrapProjectionStub{fingerprint: "mapping-v1"}
	excludedPartition := bootstrap.PublicationRelation{OID: 1, Namespace: "public", Table: "excluded", RelationKind: "p", IsPartition: true}
	included, err := admitManagedSnapshotRelation(projector, excludedPartition, 1, 1)
	if err != nil || included {
		t.Fatalf("excluded partition admission=%t err=%v", included, err)
	}
	includedPartition := excludedPartition
	includedPartition.Table = "keep"
	if _, err := admitManagedSnapshotRelation(projector, includedPartition, 0, 1); err == nil {
		t.Fatal("included partition bypassed eligibility")
	}
	ordinary := bootstrap.PublicationRelation{OID: 2, Namespace: "public", Table: "keep", RelationKind: "r"}
	if _, err := admitManagedSnapshotRelation(projector, ordinary, 1, 1); err == nil {
		t.Fatal("included relation bypassed max-table accounting")
	}
}

func TestManagedBootstrapFiltersPublicationTasksAndPreservesSourceSchemas(t *testing.T) {
	tasks := []bootstrap.SnapshotTask{
		{RelationID: 1, Namespace: "public", Table: "keep", Schema: connector.Schema{Namespace: "public", Name: "keep", Columns: []connector.Column{{Name: "id", Type: "int8"}}}},
		{RelationID: 2, Namespace: "public", Table: "excluded", Schema: connector.Schema{Namespace: "public", Name: "excluded", Columns: []connector.Column{{Name: "id", Type: "int8"}}}},
	}
	relations := []bootstrap.PublicationRelation{{OID: 1, Namespace: "public", Table: "keep"}, {OID: 2, Namespace: "public", Table: "excluded"}}
	filteredTasks, filteredRelations, tables, err := filterManagedSnapshotTasks(tasks, relations, bootstrapProjectionStub{fingerprint: "mapping-v1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(filteredTasks) != 1 || len(filteredRelations) != 1 || len(tables) != 1 {
		t.Fatalf("filtered sizes=%d/%d/%d", len(filteredTasks), len(filteredRelations), len(tables))
	}
	if filteredTasks[0].Schema.Name != "keep" || filteredTasks[0].Schema.Namespace != "public" {
		t.Fatalf("source query schema was projected: %+v", filteredTasks[0].Schema)
	}
	if tables[0].Schema.Name != "dst_keep" || tables[0].Schema.Namespace != "mapped" || tables[0].WritePolicy.Mode != connector.ResolvedWriteAppend {
		t.Fatalf("destination bootstrap table=%+v", tables[0])
	}
}

func TestManagedBootstrapManifestBindsProjectionFingerprint(t *testing.T) {
	tasks := []bootstrap.SnapshotTask{{RelationID: 1, Namespace: "public", Table: "keep", Schema: connector.Schema{Namespace: "public", Name: "keep"}}}
	first := managedManifestHash(tasks, "mapping-v1")
	second := managedManifestHash(tasks, "mapping-v2")
	if first.err != nil || second.err != nil {
		t.Fatalf("manifest errors=%v/%v", first.err, second.err)
	}
	if first.value == second.value {
		t.Fatal("projection fingerprint did not change bootstrap manifest identity")
	}
}
