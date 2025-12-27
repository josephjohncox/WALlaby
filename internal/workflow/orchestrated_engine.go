package workflow

import (
	"context"
	"errors"

	"github.com/josephjohncox/ductstream/internal/flow"
)

// FlowDispatcher enqueues flow executions.
type FlowDispatcher interface {
	EnqueueFlow(ctx context.Context, flowID string) error
}

// OrchestratedEngine wraps an Engine and dispatches flow runs on start/resume.
type OrchestratedEngine struct {
	base       Engine
	dispatcher FlowDispatcher
}

func NewOrchestratedEngine(base Engine, dispatcher FlowDispatcher) *OrchestratedEngine {
	return &OrchestratedEngine{base: base, dispatcher: dispatcher}
}

func (o *OrchestratedEngine) Create(ctx context.Context, f flow.Flow) (flow.Flow, error) {
	return o.base.Create(ctx, f)
}

func (o *OrchestratedEngine) Start(ctx context.Context, flowID string) (flow.Flow, error) {
	updated, err := o.base.Start(ctx, flowID)
	if err != nil {
		return flow.Flow{}, err
	}
	if o.dispatcher == nil {
		return updated, nil
	}
	if err := o.dispatcher.EnqueueFlow(ctx, updated.ID); err != nil {
		_, _ = o.base.Stop(ctx, updated.ID)
		return flow.Flow{}, err
	}
	return updated, nil
}

func (o *OrchestratedEngine) Stop(ctx context.Context, flowID string) (flow.Flow, error) {
	return o.base.Stop(ctx, flowID)
}

func (o *OrchestratedEngine) Resume(ctx context.Context, flowID string) (flow.Flow, error) {
	updated, err := o.base.Resume(ctx, flowID)
	if err != nil {
		return flow.Flow{}, err
	}
	if o.dispatcher == nil {
		return updated, nil
	}
	if err := o.dispatcher.EnqueueFlow(ctx, updated.ID); err != nil {
		_, _ = o.base.Stop(ctx, updated.ID)
		return flow.Flow{}, err
	}
	return updated, nil
}

func (o *OrchestratedEngine) Delete(ctx context.Context, flowID string) error {
	if o.base == nil {
		return errors.New("workflow engine is required")
	}
	return o.base.Delete(ctx, flowID)
}

func (o *OrchestratedEngine) Get(ctx context.Context, flowID string) (flow.Flow, error) {
	return o.base.Get(ctx, flowID)
}

func (o *OrchestratedEngine) List(ctx context.Context) ([]flow.Flow, error) {
	return o.base.List(ctx)
}
