package workflow

import (
	"context"
	"errors"

	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/telemetry"
)

// FlowDispatcher enqueues flow executions.
type FlowDispatcher interface {
	EnqueueFlow(ctx context.Context, flowID string) error
}

// OrchestratedEngine wraps an Engine and dispatches flow runs on start/resume.
type OrchestratedEngine struct {
	base       Engine
	dispatcher FlowDispatcher
	meters     *telemetry.Meters
}

func NewOrchestratedEngine(base Engine, dispatcher FlowDispatcher, meters *telemetry.Meters) *OrchestratedEngine {
	return &OrchestratedEngine{base: base, dispatcher: dispatcher, meters: meters}
}

func (o *OrchestratedEngine) Create(ctx context.Context, f flow.Flow) (flow.Flow, error) {
	created, err := o.base.Create(ctx, f)
	if err == nil && o.meters != nil {
		o.meters.RecordFlowCreate(ctx)
	}
	return created, err
}

func (o *OrchestratedEngine) Update(ctx context.Context, f flow.Flow) (flow.Flow, error) {
	return o.base.Update(ctx, f)
}

func (o *OrchestratedEngine) Start(ctx context.Context, flowID string) (flow.Flow, error) {
	fromState := o.getState(ctx, flowID)
	updated, err := o.base.Start(ctx, flowID)
	if err != nil {
		return flow.Flow{}, err
	}
	o.recordTransition(ctx, fromState, string(updated.State))
	if o.meters != nil {
		o.meters.RecordFlowActive(ctx, 1)
	}
	if o.dispatcher == nil {
		return updated, nil
	}
	if err := o.dispatcher.EnqueueFlow(ctx, updated.ID); err != nil {
		_, _ = o.base.Stop(ctx, updated.ID)
		if o.meters != nil {
			o.meters.RecordFlowActive(ctx, -1)
		}
		return flow.Flow{}, err
	}
	return updated, nil
}

func (o *OrchestratedEngine) Stop(ctx context.Context, flowID string) (flow.Flow, error) {
	fromState := o.getState(ctx, flowID)
	stopped, err := o.base.Stop(ctx, flowID)
	if err == nil {
		o.recordTransition(ctx, fromState, string(stopped.State))
		if o.meters != nil {
			o.meters.RecordFlowActive(ctx, -1)
		}
	}
	return stopped, err
}

func (o *OrchestratedEngine) Resume(ctx context.Context, flowID string) (flow.Flow, error) {
	fromState := o.getState(ctx, flowID)
	updated, err := o.base.Resume(ctx, flowID)
	if err != nil {
		return flow.Flow{}, err
	}
	o.recordTransition(ctx, fromState, string(updated.State))
	if o.meters != nil {
		o.meters.RecordFlowActive(ctx, 1)
	}
	if o.dispatcher == nil {
		return updated, nil
	}
	if err := o.dispatcher.EnqueueFlow(ctx, updated.ID); err != nil {
		_, _ = o.base.Stop(ctx, updated.ID)
		if o.meters != nil {
			o.meters.RecordFlowActive(ctx, -1)
		}
		return flow.Flow{}, err
	}
	return updated, nil
}

func (o *OrchestratedEngine) Delete(ctx context.Context, flowID string) error {
	if o.base == nil {
		return errors.New("workflow engine is required")
	}
	fromState := o.getState(ctx, flowID)
	err := o.base.Delete(ctx, flowID)
	if err == nil {
		o.recordTransition(ctx, fromState, "deleted")
	}
	return err
}

func (o *OrchestratedEngine) Get(ctx context.Context, flowID string) (flow.Flow, error) {
	return o.base.Get(ctx, flowID)
}

func (o *OrchestratedEngine) List(ctx context.Context) ([]flow.Flow, error) {
	return o.base.List(ctx)
}

func (o *OrchestratedEngine) recordTransition(ctx context.Context, fromState, toState string) {
	if o.meters == nil || toState == "" {
		return
	}
	if fromState == "" {
		fromState = "unknown"
	}
	o.meters.RecordFlowStateTransition(ctx, fromState, toState)
}

func (o *OrchestratedEngine) getState(ctx context.Context, flowID string) string {
	if o.meters == nil {
		return ""
	}
	current, err := o.base.Get(ctx, flowID)
	if err != nil {
		return ""
	}
	return string(current.State)
}
