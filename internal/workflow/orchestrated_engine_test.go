package workflow

import (
	"context"
	"errors"
	"testing"

	"github.com/josephjohncox/wallaby/internal/flow"
)

type fakeEngine struct {
	startFlow  flow.Flow
	startErr   error
	stopCalled bool
}

func (f *fakeEngine) Create(_ context.Context, fl flow.Flow) (flow.Flow, error) { return fl, nil }
func (f *fakeEngine) Update(_ context.Context, fl flow.Flow) (flow.Flow, error) { return fl, nil }
func (f *fakeEngine) Start(_ context.Context, _ string) (flow.Flow, error) {
	return f.startFlow, f.startErr
}
func (f *fakeEngine) Stop(_ context.Context, _ string) (flow.Flow, error) {
	f.stopCalled = true
	return flow.Flow{}, nil
}
func (f *fakeEngine) Resume(_ context.Context, _ string) (flow.Flow, error) {
	return f.startFlow, f.startErr
}
func (f *fakeEngine) Delete(_ context.Context, _ string) error           { return nil }
func (f *fakeEngine) Get(_ context.Context, _ string) (flow.Flow, error) { return f.startFlow, nil }
func (f *fakeEngine) List(_ context.Context) ([]flow.Flow, error)        { return nil, nil }

type fakeDispatcher struct {
	err      error
	enqueued []string
}

func (d *fakeDispatcher) EnqueueFlow(_ context.Context, flowID string) error {
	if d.err != nil {
		return d.err
	}
	d.enqueued = append(d.enqueued, flowID)
	return nil
}

func TestOrchestratedEngine_StartDispatches(t *testing.T) {
	base := &fakeEngine{startFlow: flow.Flow{ID: "flow-1", State: flow.StateRunning}}
	dispatcher := &fakeDispatcher{}
	engine := NewOrchestratedEngine(base, dispatcher, nil)

	if _, err := engine.Start(context.Background(), "flow-1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if len(dispatcher.enqueued) != 1 || dispatcher.enqueued[0] != "flow-1" {
		t.Fatalf("expected flow to be enqueued, got %v", dispatcher.enqueued)
	}
	if base.stopCalled {
		t.Fatalf("did not expect stop on success")
	}
}

func TestOrchestratedEngine_StartDispatchErrorStopsFlow(t *testing.T) {
	base := &fakeEngine{startFlow: flow.Flow{ID: "flow-2", State: flow.StateRunning}}
	dispatcher := &fakeDispatcher{err: errors.New("dispatch failed")}
	engine := NewOrchestratedEngine(base, dispatcher, nil)

	if _, err := engine.Start(context.Background(), "flow-2"); err == nil {
		t.Fatalf("expected error")
	}
	if !base.stopCalled {
		t.Fatalf("expected stop to be called on dispatch error")
	}
}

func TestOrchestratedEngine_ResumeDispatches(t *testing.T) {
	base := &fakeEngine{startFlow: flow.Flow{ID: "flow-3", State: flow.StateRunning}}
	dispatcher := &fakeDispatcher{}
	engine := NewOrchestratedEngine(base, dispatcher, nil)

	if _, err := engine.Resume(context.Background(), "flow-3"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(dispatcher.enqueued) != 1 || dispatcher.enqueued[0] != "flow-3" {
		t.Fatalf("expected flow to be enqueued, got %v", dispatcher.enqueued)
	}
}
