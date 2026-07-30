package integrationharness

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakePortForwardSession struct {
	done    chan error
	stopped chan struct{}
}

func TestStartPortForwardSupervisorRestartsTerminatedSession(t *testing.T) {
	t.Parallel()

	sessions := make(chan *fakePortForwardSession, 3)
	starter := func() (context.CancelFunc, <-chan error, error) {
		session := &fakePortForwardSession{
			done:    make(chan error, 1),
			stopped: make(chan struct{}),
		}
		var stopOnce sync.Once
		sessions <- session
		return func() { stopOnce.Do(func() { close(session.stopped) }) }, session.done, nil
	}

	stop, err := startPortForwardSupervisor(starter, time.Millisecond, func(error) {})
	if err != nil {
		t.Fatalf("start supervisor: %v", err)
	}
	first := receivePortForwardSession(t, sessions)
	first.done <- errors.New("lost SPDY stream")
	second := receivePortForwardSession(t, sessions)
	select {
	case <-first.stopped:
	case <-time.After(time.Second):
		t.Fatal("terminated port-forward was not stopped before restart")
	}

	stop()
	select {
	case <-second.stopped:
	case <-time.After(time.Second):
		t.Fatal("active port-forward was not stopped with supervisor")
	}
}

func receivePortForwardSession(t *testing.T, sessions <-chan *fakePortForwardSession) *fakePortForwardSession {
	t.Helper()
	select {
	case session := <-sessions:
		return session
	case <-time.After(time.Second):
		t.Fatal("port-forward session did not start")
		return nil
	}
}
