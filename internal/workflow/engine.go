package workflow

import (
	"context"

	"github.com/josephjohncox/ductstream/internal/flow"
)

// Engine coordinates durable flow execution.
type Engine interface {
	Create(ctx context.Context, flow flow.Flow) (flow.Flow, error)
	Update(ctx context.Context, flow flow.Flow) (flow.Flow, error)
	Start(ctx context.Context, flowID string) (flow.Flow, error)
	Stop(ctx context.Context, flowID string) (flow.Flow, error)
	Resume(ctx context.Context, flowID string) (flow.Flow, error)
	Delete(ctx context.Context, flowID string) error
	Get(ctx context.Context, flowID string) (flow.Flow, error)
	List(ctx context.Context) ([]flow.Flow, error)
}

// NoopEngine is a placeholder until the durable Postgres-backed engine lands.
type NoopEngine struct{}

func NewNoopEngine() *NoopEngine {
	return &NoopEngine{}
}

func (n *NoopEngine) Create(_ context.Context, f flow.Flow) (flow.Flow, error) {
	if f.State == "" {
		f.State = flow.StateCreated
	}
	return f, nil
}
func (n *NoopEngine) Update(_ context.Context, f flow.Flow) (flow.Flow, error) { return f, nil }
func (n *NoopEngine) Start(_ context.Context, _ string) (flow.Flow, error)     { return flow.Flow{}, nil }
func (n *NoopEngine) Stop(_ context.Context, _ string) (flow.Flow, error)      { return flow.Flow{}, nil }
func (n *NoopEngine) Resume(_ context.Context, _ string) (flow.Flow, error)    { return flow.Flow{}, nil }
func (n *NoopEngine) Delete(_ context.Context, _ string) error                 { return nil }
func (n *NoopEngine) Get(_ context.Context, _ string) (flow.Flow, error) {
	return flow.Flow{}, nil
}
func (n *NoopEngine) List(_ context.Context) ([]flow.Flow, error) { return nil, nil }
