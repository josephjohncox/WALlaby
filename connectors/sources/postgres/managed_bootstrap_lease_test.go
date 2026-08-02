package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/internal/authority"
)

type failingClaimRenewalStore struct {
	authority.Store
	renewed chan struct{}
}

func (s failingClaimRenewalStore) RenewClaim(context.Context, authority.ClaimFence, time.Duration) error {
	close(s.renewed)
	return errors.New("injected renewal failure")
}

func TestSnapshotClaimRenewalFailureCancelsBlockedDestinationWork(t *testing.T) {
	t.Parallel()
	store := failingClaimRenewalStore{renewed: make(chan struct{})}
	workCanceled := make(chan struct{})
	err := runWithRenewedSnapshotClaim(context.Background(), store, authority.ClaimFence{}, 30*time.Millisecond, func(ctx context.Context) error {
		<-ctx.Done()
		close(workCanceled)
		return ctx.Err()
	})
	if err == nil || !strings.Contains(err.Error(), "injected renewal failure") {
		t.Fatalf("renewal error=%v", err)
	}
	select {
	case <-store.renewed:
	case <-time.After(time.Second):
		t.Fatal("claim was not renewed")
	}
	select {
	case <-workCanceled:
	case <-time.After(time.Second):
		t.Fatal("blocked work was not canceled")
	}
}
