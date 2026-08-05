package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/internal/artifactlog"
)

type retryingArtifactCommitter struct {
	err            error
	commitCalls    int
	reconcileCalls int
}

func (c *retryingArtifactCommitter) Commit(context.Context, artifactlog.CommitRequest) (artifactlog.CommitResult, error) {
	c.commitCalls++
	return artifactlog.CommitResult{}, c.err
}

func (c *retryingArtifactCommitter) Reconcile(context.Context, artifactlog.CommitRequest) (artifactlog.ReconcileResult, error) {
	c.reconcileCalls++
	return artifactlog.ReconcileResult{}, c.err
}

func TestArtifactConsumerRetryDoesNotBlockReadsBelowBacklogWatermark(t *testing.T) {
	deps := newArtifactIntegrationDeps(t)
	fence := deps.newFence(t, "consumer-retry-admission")
	retryCause := errors.New("catalog service temporarily unavailable")
	committer := &retryingArtifactCommitter{err: errors.Join(artifactlog.ErrConsumerRetryable, retryCause)}
	runtime, err := artifactlog.NewRuntime(deps.ctx, deps.pool, deps.objects, artifactlog.RuntimeConfig{
		Stream: artifactlog.StreamConfig{
			HardRetainedBytes:        128 << 20,
			BacklogCountHigh:         2,
			BacklogBytesHigh:         128 << 20,
			BacklogAgeHigh:           time.Hour,
			BackpressurePollInterval: 10 * time.Millisecond,
		},
		OrphanGrace: time.Minute,
		Retention:   time.Hour,
		GCInterval:  time.Hour,
		Consumers: []artifactlog.CatalogConsumerConfig{{
			RevisionID: "iceberg-retry-v1",
			Committer:  committer,
		}},
		DestinationFingerprint: "retryable-catalog-profile",
	})
	if err != nil {
		t.Fatal(err)
	}
	first := artifactTransactionAt(9101, "0/910", "0/918", "0/920", "first")
	if _, err := runtime.Append(deps.ctx, fence, first, managedBaselinePayload(t, first)); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Recover(deps.ctx, fence); err != nil {
		t.Fatalf("retryable consumer blocked runtime recovery below watermark: %v", err)
	}
	if err := runtime.WaitForReadAdmission(deps.ctx, fence); err != nil {
		t.Fatalf("retryable consumer below watermark blocked source read: %v", err)
	}
	if committer.commitCalls != 1 {
		t.Fatalf("consumer commit attempts=%d, want 1", committer.commitCalls)
	}

	second := artifactTransactionAt(9102, "0/930", "0/938", "0/940", "second")
	if _, err := runtime.Append(deps.ctx, fence, second, managedBaselinePayload(t, second)); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(deps.ctx, 100*time.Millisecond)
	defer cancel()
	if err := runtime.WaitForReadAdmission(waitCtx, fence); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retryable consumer at watermark error=%v, want deadline while reads remain blocked", err)
	}
	if committer.reconcileCalls == 0 {
		t.Fatal("consumer was not retried while backlog remained at the watermark")
	}
}
