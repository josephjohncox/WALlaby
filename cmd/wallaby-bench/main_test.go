package main

import (
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestBenchmarkProjectorCarriesDestinationWritePolicy(t *testing.T) {
	t.Parallel()
	specs := []tableSpec{{Name: "bench_src.narrow"}}
	batch := connector.Batch{
		Schema: connector.Schema{Namespace: "bench_src", Name: "narrow", Columns: []connector.Column{{
			Name: "id", Type: "bigint", TypeMetadata: map[string]string{"primary_key": "true", "replica_identity": "true"},
		}}},
		Records:    []connector.Record{{Table: "narrow", Operation: connector.OpInsert, After: map[string]any{"id": int64(1)}}},
		Checkpoint: connector.Checkpoint{LSN: "0/10"},
	}

	postgresSpec, _, err := buildDestination("postgres", "postgres://bench", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, obsolete := range []string{"schema", "table", "database", "write_mode"} {
		if _, exists := postgresSpec.Options[obsolete]; exists {
			t.Fatalf("benchmark PostgreSQL destination retained obsolete option %q", obsolete)
		}
	}
	postgresProjector, err := benchmarkProjector("postgres", postgresSpec, specs)
	if err != nil {
		t.Fatal(err)
	}
	postgresBatch, included, err := postgresProjector.ProjectBatch(batch)
	if err != nil || included != stream.ProjectionIncluded {
		t.Fatalf("project PostgreSQL benchmark batch decision=%v err=%v", included, err)
	}
	if postgresBatch.Schema.Namespace != "bench_sink" || postgresBatch.Schema.Name != "narrow" || postgresBatch.WritePolicy.Mode != connector.ResolvedWriteUpsert || len(postgresBatch.WritePolicy.KeyColumns) != 1 || postgresBatch.WritePolicy.KeyColumns[0] != "id" {
		t.Fatalf("PostgreSQL benchmark projection=%+v policy=%+v", postgresBatch.Schema, postgresBatch.WritePolicy)
	}

	kafkaSpec, _, err := buildDestination("kafka", "", "", "localhost:9092", "test")
	if err != nil {
		t.Fatal(err)
	}
	kafkaProjector, err := benchmarkProjector("kafka", kafkaSpec, specs)
	if err != nil {
		t.Fatal(err)
	}
	kafkaBatch, included, err := kafkaProjector.ProjectBatch(batch)
	if err != nil || included != stream.ProjectionIncluded {
		t.Fatalf("project Kafka benchmark batch decision=%v err=%v", included, err)
	}
	if kafkaBatch.WritePolicy.Mode != connector.ResolvedWriteAppend || kafkaBatch.Records[0].After[connector.AppendOperationColumn] == nil || kafkaBatch.Records[0].After[connector.AppendSourcePositionColumn] == nil {
		t.Fatalf("Kafka benchmark append projection record=%+v policy=%+v", kafkaBatch.Records[0], kafkaBatch.WritePolicy)
	}
}

func TestResolveProfileCI(t *testing.T) {
	got, err := resolveProfile(" CI ")
	if err != nil {
		t.Fatalf("resolveProfile(ci): %v", err)
	}
	if got.Name != "ci" || got.InitialRows != 250 || got.Operations != 1000 || got.Writers != 2 {
		t.Fatalf("resolveProfile(ci) = %+v", got)
	}
	if got.BatchSize <= 0 || got.EmptyReads <= 0 {
		t.Fatalf("resolveProfile(ci) has invalid execution bounds: %+v", got)
	}
}
