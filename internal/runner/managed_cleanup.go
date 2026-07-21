package runner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

const defaultManagedCleanupLease = 30 * time.Second

// ManagedSourceCleanup is the lifecycle-to-connector bridge for terminal
// managed source cleanup. It is intentionally owned by the control plane, not
// FlowRunner, so a replacement lifecycle process can finish a hard-killed
// worker's stop.
type ManagedSourceCleanup struct {
	Factory   Factory
	Authority authority.CleanupStore
	Lease     time.Duration
}

// CleanupSourceResources acquires purpose-built cleanup authority only for a
// managed flow whose stop generation has already quiesced.
func (c ManagedSourceCleanup) CleanupSourceResources(ctx context.Context, f flow.Flow, generation int64) (retErr error) {
	if !managedSourceSpec(f.Source) {
		return nil
	}
	if c.Authority == nil {
		return errors.New("managed stop requires terminal cleanup authority")
	}
	lease := c.Lease
	if lease <= 0 {
		lease = defaultManagedCleanupLease
	}
	source, err := c.Factory.SourceForFlow(f)
	if err != nil {
		return fmt.Errorf("build managed source for terminal cleanup: %w", err)
	}
	cleaner, ok := source.(connector.ManagedSourceResourceCleaner)
	if !ok {
		return errors.New("managed source does not implement terminal resource cleanup")
	}
	fence, err := c.Authority.AcquireCleanupFence(ctx, f.ID, generation, lease)
	if err != nil {
		return fmt.Errorf("acquire managed cleanup authority: %w", err)
	}
	defer func() {
		reason := "cleanup_completed"
		if retErr != nil {
			reason = "cleanup_retryable_error"
		}
		if finishErr := c.Authority.FinishCleanup(context.WithoutCancel(ctx), fence, reason); finishErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("finish managed cleanup authority: %w", finishErr))
		}
	}()
	if err := cleaner.CleanupManagedResources(ctx, fence, f.Source); err != nil {
		return fmt.Errorf("clean managed source resources: %w", err)
	}
	return nil
}
