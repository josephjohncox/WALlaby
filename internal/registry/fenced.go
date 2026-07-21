package registry

import (
	"context"
	"errors"

	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type runFenceContextKey struct{}

func contextWithRunFence(ctx context.Context, fence authority.RunFence) context.Context {
	return context.WithValue(ctx, runFenceContextKey{}, fence)
}

func runFenceFromContext(ctx context.Context) (authority.RunFence, bool) {
	fence, ok := ctx.Value(runFenceContextKey{}).(connector.RunFence)
	return fence, ok
}

// FencedDDLExecutionStore binds every attempt/receipt mutation to one immutable
// producer acquisition. The underlying advisory lock remains only external-I/O
// serialization; SQL publication still validates the RunFence transactionally.
type FencedDDLExecutionStore struct {
	store *PostgresStore
	fence authority.RunFence
}

// ForRunFence returns the DDL mutation surface for one producer acquisition.
func (p *PostgresStore) ForRunFence(fence authority.RunFence) (*FencedDDLExecutionStore, error) {
	if p == nil {
		return nil, errors.New("registry store is required")
	}
	if err := fence.Validate(); err != nil {
		return nil, err
	}
	return &FencedDDLExecutionStore{store: p, fence: fence}, nil
}

func (s *FencedDDLExecutionStore) WithDDLExecutionLock(ctx context.Context, flowID, destination string, fn func() error) error {
	return s.store.WithDDLExecutionLock(contextWithRunFence(ctx, s.fence), flowID, destination, fn)
}

func (s *FencedDDLExecutionStore) PrepareDDLExecution(ctx context.Context, flowID, position, destination string, expected []string) (connector.DDLExecutionState, error) {
	return s.store.PrepareDDLExecution(contextWithRunFence(ctx, s.fence), flowID, position, destination, expected)
}

func (s *FencedDDLExecutionStore) RecordDDLExecution(ctx context.Context, flowID, position, ddl, destination string, expected []string) error {
	return s.store.RecordDDLExecution(contextWithRunFence(ctx, s.fence), flowID, position, ddl, destination, expected)
}

// BindRunFence attaches the exact producer authority to a per-flow hook before
// the source opens. Rebinding to another fence is rejected.
func (h *Hook) BindRunFence(fence connector.RunFence) error {
	if err := fence.Validate(); err != nil {
		return err
	}
	if h.RunFence != nil && *h.RunFence != fence {
		return errors.New("registry hook run fence is immutable")
	}
	copyFence := fence
	h.RunFence = &copyFence
	return nil
}

func (h *Hook) fencedContext(ctx context.Context) context.Context {
	if h.RunFence == nil {
		return ctx
	}
	return contextWithRunFence(ctx, *h.RunFence)
}
