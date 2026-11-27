package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// batch_size sizes make([]connector.Record, 0, batchSize) and partition_count
// the task list. Hostile values must fail before any connection is attempted:
// the DSN below is unreachable, so reaching the dial would produce a different
// error (or a panic for a negative capacity).
func TestBackfillSourceOpenRejectsHostileSizesBeforeConnecting(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		options map[string]string
		want    string
	}{
		{"huge batch", map[string]string{optBatchSize: "9223372036854775807"}, "batch_size"},
		{"above limit", map[string]string{optBatchSize: "10001"}, "batch_size"},
		{"negative batch", map[string]string{optBatchSize: "-1"}, "batch_size"},
		{"zero batch", map[string]string{optBatchSize: "0"}, "batch_size"},
		{"huge partitions", map[string]string{optPartitionCount: "9223372036854775807"}, "partition_count"},
		{"partitions above limit", map[string]string{optPartitionCount: "4097"}, "partition_count"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			options := map[string]string{optDSN: "postgres://127.0.0.1:1/unreachable"}
			for key, value := range tt.options {
				options[key] = value
			}
			source := &BackfillSource{}
			err := source.Open(context.Background(), connector.RuntimeSpec{Options: options})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
			if source.pool != nil {
				t.Fatal("connection opened before validation")
			}
		})
	}
}
