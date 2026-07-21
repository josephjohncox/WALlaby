package artifactlog

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestCanonicalParquetSeparateProcess(t *testing.T) {
	if os.Getenv("WALLABY_ARTIFACT_HELPER") == "1" {
		artifact := encodeDeterministicArtifact(t)
		fmt.Printf("ARTIFACT=%s:%s:%s\n", artifact.ID, artifact.SchemaID, artifact.EncodedByteHash)
		return
	}

	outputs := make([]string, 2)
	for index := range outputs {
		command := exec.Command(os.Args[0], "-test.run=^TestCanonicalParquetSeparateProcess$")
		command.Env = append(os.Environ(), "WALLABY_ARTIFACT_HELPER=1")
		raw, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("helper process %d: %v\n%s", index, err, raw)
		}
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "ARTIFACT=") {
				outputs[index] = line
			}
		}
		if outputs[index] == "" {
			t.Fatalf("helper process %d omitted artifact digest: %s", index, raw)
		}
	}
	if outputs[0] != outputs[1] {
		t.Fatalf("canonical Parquet differs across processes:\n%s\n%s", outputs[0], outputs[1])
	}
}

func TestCanonicalArtifactSeparatesLogicalAndEncodedHashes(t *testing.T) {
	artifact := encodeDeterministicArtifact(t)
	if artifact.LogicalContentHash == "" || artifact.EncodedByteHash == "" {
		t.Fatalf("artifact hashes are incomplete: %+v", artifact)
	}
	if artifact.LogicalContentHash == artifact.EncodedByteHash {
		t.Fatal("logical-content and encoded-byte hashes unexpectedly share one identity")
	}
	if len(artifact.Encoded) == 0 || len(artifact.Encoded) > MaxEncodedObject {
		t.Fatalf("encoded bytes=%d outside canonical bounds", len(artifact.Encoded))
	}
}

func encodeDeterministicArtifact(t *testing.T) Artifact {
	t.Helper()
	transaction := connector.SourceTransaction{
		SourceLineageID: "postgres-system-1/publication-v1",
		TransactionID:   9,
		BeginLSN:        "0/10",
		CommitLSN:       "0/20",
		EndLSN:          "0/28",
		Checkpoint:      connector.Checkpoint{LSN: "0/28", Timestamp: time.Unix(100, 0).UTC(), Metadata: map[string]string{"b": "2", "a": "1"}},
		Fragments: []connector.TransactionFragment{{
			Ordinal: 0,
			Batch: connector.Batch{
				Schema: connector.Schema{Namespace: "public", Name: "events", Version: 1, Columns: []connector.Column{
					{Name: "id", Type: "int8", TypeMetadata: map[string]string{"source_relation_id": "42", "source_column_id": "1"}},
					{Name: "payload", Type: "jsonb", Nullable: true, TypeMetadata: map[string]string{"source_relation_id": "42", "source_column_id": "2"}},
				}},
				Records: []connector.Record{{
					Table: "events", Operation: connector.OpInsert, SchemaVersion: 1,
					Key: []byte(`{"id":1}`), After: map[string]any{"payload": map[string]any{"z": float64(2), "a": nil}, "id": int64(1)},
					Timestamp: time.Unix(99, 123).UTC(),
				}},
			},
		}},
	}
	artifacts, err := NewEncoder().EncodeTransaction(context.Background(), uuid.MustParse("11111111-1111-1111-1111-111111111111"), transaction)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("artifacts=%d, want 1", len(artifacts))
	}
	return artifacts[0]
}
