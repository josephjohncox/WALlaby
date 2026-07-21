package workflow

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/flow"
)

// Engine coordinates durable flow execution.
type Engine interface {
	Create(ctx context.Context, flow flow.Flow) (flow.Flow, error)
	Update(ctx context.Context, flow flow.Flow) (flow.Flow, error)
	Start(ctx context.Context, flowID string) (flow.Flow, error)
	Pause(ctx context.Context, flowID string) (flow.Flow, error)
	Stop(ctx context.Context, flowID string) (flow.Flow, error)
	Resume(ctx context.Context, flowID string) (flow.Flow, error)
	Delete(ctx context.Context, flowID string) error
	Get(ctx context.Context, flowID string) (flow.Flow, error)
	List(ctx context.Context) ([]flow.Flow, error)
}

// ControlReader exposes the current lifecycle fence without granting mutation.
type ControlReader interface {
	Control(ctx context.Context, flowID string) (LifecycleControl, error)
}

// ControlEngine is the control-plane interface used by the gRPC service.
type ControlEngine interface {
	Engine
	ControlReader
}

// LifecycleTarget is durable internal intent. It deliberately does not add a
// public lifecycle state: pause remains publicly running until quiescence.
type LifecycleTarget string

const (
	TargetCreated LifecycleTarget = "created"
	TargetRunning LifecycleTarget = "running"
	TargetPaused  LifecycleTarget = "paused"
	TargetStopped LifecycleTarget = "stopped"
	TargetFailed  LifecycleTarget = "failed"
)

// LifecycleControl is the durable fencing state for one flow.
type LifecycleControl struct {
	FlowID          string
	State           flow.State
	Target          LifecycleTarget
	Generation      int64
	DispatchPending bool
}

// CancellationReceipt is proof supplied by a dispatcher. Terminal is true
// only when the backend has authoritatively observed all work through the
// requested generation as terminal. TerminalExecutionIDs names only the exact
// backend executions covered by that observation; it is never a backend-wide
// permission to finish execution rows.
type CancellationReceipt struct {
	ThroughGeneration    int64
	Terminal             bool
	Backend              string
	TerminalExecutionIDs []string
}

// Dispatcher is the generation-aware lifecycle dispatcher contract.
type Dispatcher interface {
	EnqueueGeneration(ctx context.Context, flowID string, generation int64) error
	CancelThroughGeneration(ctx context.Context, flowID string, generation int64) (CancellationReceipt, error)
}

// PassiveDispatcher is explicit standalone/development behavior. It does not
// claim authority over manually launched worker processes.
type PassiveDispatcher struct{}

func (PassiveDispatcher) EnqueueGeneration(context.Context, string, int64) error { return nil }
func (PassiveDispatcher) CancelThroughGeneration(_ context.Context, _ string, generation int64) (CancellationReceipt, error) {
	return CancellationReceipt{ThroughGeneration: generation, Terminal: false, Backend: "standalone"}, nil
}

// LifecycleStore is the compile-time control-plane storage contract. The
// callback is executed while a per-flow operation lock is held. try=true must
// return acquired=false instead of waiting when another replica owns the lock.
type LifecycleStore interface {
	ControlEngine
	WithFlowLock(ctx context.Context, flowID string, try bool, fn func() error) (acquired bool, err error)
	PlanStart(ctx context.Context, flowID string, resume bool) (flow.Flow, LifecycleControl, error)
	MarkDispatched(ctx context.Context, flowID string, generation int64) error
	RequestPause(ctx context.Context, flowID string) (flow.Flow, LifecycleControl, error)
	CompletePause(ctx context.Context, flowID string, generation int64) (flow.Flow, error)
	RequestStop(ctx context.Context, flowID string) (flow.Flow, LifecycleControl, error)
	CompleteStopGeneration(ctx context.Context, flowID string, generation int64) (flow.Flow, error)
	Fail(ctx context.Context, flowID string) (flow.Flow, error)
	PendingControls(ctx context.Context) ([]LifecycleControl, error)
	RegisterExecutionGeneration(ctx context.Context, flowID, executionID, backend string, generation int64, lease time.Duration) error
	RegisterExecutionFence(ctx context.Context, flowID, executionID, backend string, generation int64, lease time.Duration) (ExecutionFence, error)
	RenewExecutionFence(ctx context.Context, fence ExecutionFence, lease time.Duration) error
	FinishExecutionFence(ctx context.Context, fence ExecutionFence, reason string) error
	FailExecutionFence(ctx context.Context, fence ExecutionFence, reason string) error
	RenewExecution(ctx context.Context, flowID, executionID string, generation int64, lease time.Duration) error
	FinishExecutionReason(ctx context.Context, flowID, executionID, reason string) error
	ActiveExecutionsThrough(ctx context.Context, flowID string, generation int64) (int, error)
	ReconcileTerminatedExecutions(ctx context.Context, flowID string, generation int64, backend string, executionIDs []string, reason string) error
}

// ExecutionFence is the immutable compatibility-worker capability returned by
// registration. Every worker-originated lifecycle mutation must present all
// fields; flow IDs and execution IDs alone are not authority.
type ExecutionFence struct {
	FlowID        string
	IncarnationID uuid.UUID
	Generation    int64
	ExecutionID   string
	Backend       string
}

// ExecutionEngine is the narrow data-plane seam used by FlowRunner. It omits
// unrestricted control-plane failure and legacy execution mutation methods.
type ExecutionEngine interface {
	ControlReader
	Get(ctx context.Context, flowID string) (flow.Flow, error)
	WithFlowLock(ctx context.Context, flowID string, try bool, fn func() error) (acquired bool, err error)
	RequestPause(ctx context.Context, flowID string) (flow.Flow, LifecycleControl, error)
	RegisterExecutionFence(ctx context.Context, flowID, executionID, backend string, generation int64, lease time.Duration) (ExecutionFence, error)
	RenewExecutionFence(ctx context.Context, fence ExecutionFence, lease time.Duration) error
	FinishExecutionFence(ctx context.Context, fence ExecutionFence, reason string) error
	FailExecutionFence(ctx context.Context, fence ExecutionFence, reason string) error
}

type memoryExecution struct {
	backend    string
	generation int64
	heartbeat  time.Time
	leaseUntil time.Time
}

// MemoryEngine is an explicit development/test workflow store. It is not
// durable and must not be selected implicitly in production.
type MemoryEngine struct {
	mu           sync.RWMutex
	flows        map[string]flow.Flow
	controls     map[string]LifecycleControl
	executions   map[string]map[string]memoryExecution
	incarnations map[string]uuid.UUID
	opMu         sync.Mutex
	opLocks      map[string]*sync.Mutex
}

func NewMemoryEngine() *MemoryEngine {
	return &MemoryEngine{
		flows:        make(map[string]flow.Flow),
		controls:     make(map[string]LifecycleControl),
		executions:   make(map[string]map[string]memoryExecution),
		incarnations: make(map[string]uuid.UUID),
		opLocks:      make(map[string]*sync.Mutex),
	}
}

func NewNoopEngine() *MemoryEngine { return NewMemoryEngine() }

func (m *MemoryEngine) operationLock(flowID string) *sync.Mutex {
	m.opMu.Lock()
	defer m.opMu.Unlock()
	lock := m.opLocks[flowID]
	if lock == nil {
		lock = &sync.Mutex{}
		m.opLocks[flowID] = lock
	}
	return lock
}

func (m *MemoryEngine) WithFlowLock(ctx context.Context, flowID string, try bool, fn func() error) (bool, error) {
	if flowID == "" {
		return false, errors.New("flow id is required")
	}
	lock := m.operationLock(flowID)
	if try {
		if !lock.TryLock() {
			return false, nil
		}
	} else {
		locked := make(chan struct{})
		go func() { lock.Lock(); close(locked) }()
		select {
		case <-ctx.Done():
			// The goroutine must eventually release the lock it acquires.
			go func() { <-locked; lock.Unlock() }()
			return false, ctx.Err()
		case <-locked:
		}
	}
	defer lock.Unlock()
	return true, fn()
}

func (m *MemoryEngine) Create(_ context.Context, f flow.Flow) (flow.Flow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if f.ID == "" {
		return flow.Flow{}, errors.New("flow id is required")
	}
	if _, ok := m.flows[f.ID]; ok {
		return flow.Flow{}, ErrAlreadyExists
	}
	if f.State == "" {
		f.State = flow.StateCreated
	}
	if f.State != flow.StateCreated {
		return flow.Flow{}, fmt.Errorf("%w: flows must be created in state %s", ErrInvalidState, flow.StateCreated)
	}
	if f.Parallelism <= 0 {
		f.Parallelism = 1
	}
	m.flows[f.ID] = f
	m.incarnations[f.ID] = uuid.New()
	m.controls[f.ID] = LifecycleControl{FlowID: f.ID, State: f.State, Target: TargetCreated}
	return f, nil
}

func (m *MemoryEngine) Update(_ context.Context, f flow.Flow) (flow.Flow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.flows[f.ID]
	if !ok {
		return flow.Flow{}, ErrNotFound
	}
	f.State = current.State
	if f.Parallelism <= 0 {
		f.Parallelism = 1
	}
	m.flows[f.ID] = f
	return f, nil
}

func (m *MemoryEngine) PlanStart(_ context.Context, flowID string, resume bool) (flow.Flow, LifecycleControl, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.flows[flowID]
	if !ok {
		return flow.Flow{}, LifecycleControl{}, ErrNotFound
	}
	control := m.controls[flowID]
	if current.State == flow.StateRunning && control.Target == TargetRunning {
		return current, control, nil
	}
	expected := flow.StateCreated
	if resume {
		expected = flow.StatePaused
	}
	if current.State != expected || LifecycleTarget(current.State) != control.Target {
		return flow.Flow{}, control, fmt.Errorf("%w: cannot start flow in state %s with target %s", ErrInvalidState, current.State, control.Target)
	}
	current.State = flow.StateRunning
	control.State = current.State
	control.Target = TargetRunning
	control.Generation++
	control.DispatchPending = true
	m.flows[flowID] = current
	m.controls[flowID] = control
	return current, control, nil
}

func (m *MemoryEngine) MarkDispatched(_ context.Context, flowID string, generation int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	control, ok := m.controls[flowID]
	if !ok {
		return ErrNotFound
	}
	if control.Generation != generation || control.Target != TargetRunning {
		return fmt.Errorf("%w: dispatch generation %d is fenced by generation %d target %s", ErrInvalidState, generation, control.Generation, control.Target)
	}
	control.DispatchPending = false
	m.controls[flowID] = control
	return nil
}

func (m *MemoryEngine) RequestPause(_ context.Context, flowID string) (flow.Flow, LifecycleControl, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.flows[flowID]
	if !ok {
		return flow.Flow{}, LifecycleControl{}, ErrNotFound
	}
	control := m.controls[flowID]
	if current.State == flow.StatePaused && control.Target == TargetPaused {
		return current, control, nil
	}
	if current.State != flow.StateRunning || (control.Target != TargetRunning && control.Target != TargetPaused) {
		return flow.Flow{}, control, fmt.Errorf("%w: cannot pause flow in state %s with target %s", ErrInvalidState, current.State, control.Target)
	}
	control.Target = TargetPaused
	control.DispatchPending = false
	m.controls[flowID] = control
	return current, control, nil
}

func (m *MemoryEngine) CompletePause(_ context.Context, flowID string, generation int64) (flow.Flow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.flows[flowID]
	if !ok {
		return flow.Flow{}, ErrNotFound
	}
	control := m.controls[flowID]
	if control.Generation != generation || control.Target != TargetPaused {
		return flow.Flow{}, fmt.Errorf("%w: pause generation is fenced", ErrInvalidState)
	}
	if activeMemoryExecutions(m.executions[flowID], generation) != 0 {
		return flow.Flow{}, fmt.Errorf("%w: active executions prevent pause", ErrInvalidState)
	}
	if current.State != flow.StateRunning && current.State != flow.StatePaused {
		return flow.Flow{}, fmt.Errorf("%w: cannot complete pause from %s", ErrInvalidState, current.State)
	}
	current.State = flow.StatePaused
	control.State = current.State
	m.flows[flowID] = current
	m.controls[flowID] = control
	return current, nil
}

func (m *MemoryEngine) RequestStop(_ context.Context, flowID string) (flow.Flow, LifecycleControl, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.flows[flowID]
	if !ok {
		return flow.Flow{}, LifecycleControl{}, ErrNotFound
	}
	control := m.controls[flowID]
	if current.State == flow.StateStopped && control.Target == TargetStopped {
		return current, control, nil
	}
	if current.State == flow.StateStopping && control.Target == TargetStopped {
		return current, control, nil
	}
	if current.State != flow.StateRunning && current.State != flow.StatePaused {
		return flow.Flow{}, control, fmt.Errorf("%w: cannot stop flow in state %s", ErrInvalidState, current.State)
	}
	current.State = flow.StateStopping
	control.State = current.State
	control.Target = TargetStopped
	control.DispatchPending = false
	m.flows[flowID] = current
	m.controls[flowID] = control
	return current, control, nil
}

func (m *MemoryEngine) CompleteStopGeneration(_ context.Context, flowID string, generation int64) (flow.Flow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.flows[flowID]
	if !ok {
		return flow.Flow{}, ErrNotFound
	}
	control := m.controls[flowID]
	if current.State == flow.StateStopped && control.Target == TargetStopped && control.Generation == generation {
		return current, nil
	}
	if control.Generation != generation || control.Target != TargetStopped || current.State != flow.StateStopping {
		return flow.Flow{}, fmt.Errorf("%w: stop generation is fenced", ErrInvalidState)
	}
	if activeMemoryExecutions(m.executions[flowID], generation) != 0 {
		return flow.Flow{}, fmt.Errorf("%w: active executions prevent stop", ErrInvalidState)
	}
	current.State = flow.StateStopped
	control.State = current.State
	m.flows[flowID] = current
	m.controls[flowID] = control
	return current, nil
}

func (m *MemoryEngine) Start(ctx context.Context, flowID string) (flow.Flow, error) {
	f, _, err := m.PlanStart(ctx, flowID, false)
	return f, err
}
func (m *MemoryEngine) Resume(ctx context.Context, flowID string) (flow.Flow, error) {
	f, _, err := m.PlanStart(ctx, flowID, true)
	return f, err
}
func (m *MemoryEngine) Pause(ctx context.Context, flowID string) (flow.Flow, error) {
	_, control, err := m.RequestPause(ctx, flowID)
	if err != nil {
		return flow.Flow{}, err
	}
	return m.CompletePause(ctx, flowID, control.Generation)
}
func (m *MemoryEngine) Stop(ctx context.Context, flowID string) (flow.Flow, error) {
	f, _, err := m.RequestStop(ctx, flowID)
	return f, err
}
func (m *MemoryEngine) Fail(_ context.Context, flowID string) (flow.Flow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.flows[flowID]
	if !ok {
		return flow.Flow{}, ErrNotFound
	}
	if current.State == flow.StateStopped {
		return flow.Flow{}, fmt.Errorf("%w: stopped flow is terminal", ErrInvalidState)
	}
	current.State = flow.StateFailed
	control := m.controls[flowID]
	control.State, control.Target, control.DispatchPending = current.State, TargetFailed, false
	m.flows[flowID], m.controls[flowID] = current, control
	return current, nil
}

func (m *MemoryEngine) Delete(_ context.Context, flowID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.flows[flowID]
	if !ok {
		return ErrNotFound
	}
	control := m.controls[flowID]
	stable := LifecycleTarget(current.State) == control.Target || (current.State == flow.StateStopped && control.Target == TargetStopped)
	if !stable || control.DispatchPending || activeMemoryExecutions(m.executions[flowID], control.Generation) != 0 || current.State == flow.StateRunning || current.State == flow.StateStopping {
		return fmt.Errorf("%w: flow has incomplete lifecycle work", ErrInvalidState)
	}
	delete(m.flows, flowID)
	delete(m.controls, flowID)
	delete(m.executions, flowID)
	delete(m.incarnations, flowID)
	return nil
}

func (m *MemoryEngine) Get(_ context.Context, flowID string) (flow.Flow, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	f, ok := m.flows[flowID]
	if !ok {
		return flow.Flow{}, ErrNotFound
	}
	return f, nil
}
func (m *MemoryEngine) List(_ context.Context) ([]flow.Flow, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]string, 0, len(m.flows))
	for id := range m.flows {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]flow.Flow, 0, len(ids))
	for _, id := range ids {
		out = append(out, m.flows[id])
	}
	return out, nil
}
func (m *MemoryEngine) Control(_ context.Context, flowID string) (LifecycleControl, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	control, ok := m.controls[flowID]
	if !ok {
		return LifecycleControl{}, ErrNotFound
	}
	return control, nil
}
func (m *MemoryEngine) PendingControls(_ context.Context) ([]LifecycleControl, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]LifecycleControl, 0)
	for _, control := range m.controls {
		stable := LifecycleTarget(control.State) == control.Target || (control.State == flow.StateStopped && control.Target == TargetStopped)
		if control.DispatchPending || !stable {
			out = append(out, control)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FlowID < out[j].FlowID })
	return out, nil
}

func (m *MemoryEngine) RegisterExecutionGeneration(ctx context.Context, flowID, executionID, backend string, generation int64, lease time.Duration) error {
	_, err := m.registerExecutionFence(ctx, flowID, executionID, backend, generation, lease)
	return err
}

func (m *MemoryEngine) RegisterExecutionFence(ctx context.Context, flowID, executionID, backend string, generation int64, lease time.Duration) (ExecutionFence, error) {
	return m.registerExecutionFence(ctx, flowID, executionID, backend, generation, lease)
}

func (m *MemoryEngine) registerExecutionFence(ctx context.Context, flowID, executionID, backend string, generation int64, lease time.Duration) (ExecutionFence, error) {
	if executionID == "" {
		return ExecutionFence{}, errors.New("execution id is required")
	}
	var registered ExecutionFence
	_, err := m.WithFlowLock(ctx, flowID, false, func() error {
		m.mu.Lock()
		defer m.mu.Unlock()
		f, ok := m.flows[flowID]
		if !ok {
			return ErrNotFound
		}
		control := m.controls[flowID]
		if f.State != flow.StateRunning || control.Target != TargetRunning || generation != control.Generation {
			return fmt.Errorf("%w: execution generation %d is fenced by generation %d target %s", ErrInvalidState, generation, control.Generation, control.Target)
		}
		if lease <= 0 {
			lease = 15 * time.Second
		}
		if m.executions[flowID] == nil {
			m.executions[flowID] = make(map[string]memoryExecution)
		}
		if existing, exists := m.executions[flowID][executionID]; exists && (existing.backend != backend || existing.generation != generation) {
			return fmt.Errorf("%w: execution identity is already owned by backend %s generation %d", ErrInvalidState, existing.backend, existing.generation)
		}
		now := time.Now()
		m.executions[flowID][executionID] = memoryExecution{backend: backend, generation: generation, heartbeat: now, leaseUntil: now.Add(lease)}
		registered = ExecutionFence{FlowID: flowID, IncarnationID: m.incarnations[flowID], Generation: generation, ExecutionID: executionID, Backend: backend}
		return nil
	})
	if err != nil {
		return ExecutionFence{}, err
	}
	return registered, nil
}

func (m *MemoryEngine) RenewExecutionFence(_ context.Context, fence ExecutionFence, lease time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	control, ok := m.controls[fence.FlowID]
	record, executionOK := m.executions[fence.FlowID][fence.ExecutionID]
	if !ok || !executionOK || m.incarnations[fence.FlowID] != fence.IncarnationID || record.generation != fence.Generation || record.backend != fence.Backend || control.Generation != fence.Generation || control.Target != TargetRunning || control.State != flow.StateRunning {
		return fmt.Errorf("%w: execution fence is stale", ErrInvalidState)
	}
	if lease <= 0 {
		lease = 15 * time.Second
	}
	record.heartbeat = time.Now()
	record.leaseUntil = record.heartbeat.Add(lease)
	m.executions[fence.FlowID][fence.ExecutionID] = record
	return nil
}

func (m *MemoryEngine) FinishExecutionFence(_ context.Context, fence ExecutionFence, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.executions[fence.FlowID][fence.ExecutionID]
	control, flowOK := m.controls[fence.FlowID]
	if !ok || !flowOK || m.incarnations[fence.FlowID] != fence.IncarnationID || control.Generation != fence.Generation || record.generation != fence.Generation || record.backend != fence.Backend {
		return fmt.Errorf("%w: execution fence is stale", ErrInvalidState)
	}
	delete(m.executions[fence.FlowID], fence.ExecutionID)
	return nil
}

func (m *MemoryEngine) FailExecutionFence(_ context.Context, fence ExecutionFence, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.executions[fence.FlowID][fence.ExecutionID]
	current, flowOK := m.flows[fence.FlowID]
	control := m.controls[fence.FlowID]
	if !ok || !flowOK || m.incarnations[fence.FlowID] != fence.IncarnationID || record.generation != fence.Generation || record.backend != fence.Backend || control.Generation != fence.Generation || control.State != flow.StateRunning || control.Target != TargetRunning {
		return fmt.Errorf("%w: execution fence is stale", ErrInvalidState)
	}
	current.State = flow.StateFailed
	control.State, control.Target, control.DispatchPending = flow.StateFailed, TargetFailed, false
	m.flows[fence.FlowID], m.controls[fence.FlowID] = current, control
	return nil
}

func (m *MemoryEngine) RenewExecution(_ context.Context, flowID, executionID string, generation int64, lease time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	control, ok := m.controls[flowID]
	if !ok {
		return ErrNotFound
	}
	record, ok := m.executions[flowID][executionID]
	if !ok || record.generation != generation || control.Generation != generation || control.Target != TargetRunning || control.State != flow.StateRunning {
		return fmt.Errorf("%w: execution lease is fenced", ErrInvalidState)
	}
	if lease <= 0 {
		lease = 15 * time.Second
	}
	record.heartbeat = time.Now()
	record.leaseUntil = record.heartbeat.Add(lease)
	m.executions[flowID][executionID] = record
	return nil
}
func (m *MemoryEngine) FinishExecutionReason(_ context.Context, flowID, executionID, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if executions := m.executions[flowID]; executions != nil {
		delete(executions, executionID)
	}
	return nil
}
func (m *MemoryEngine) ActiveExecutionsThrough(_ context.Context, flowID string, generation int64) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.flows[flowID]; !ok {
		return 0, ErrNotFound
	}
	return activeMemoryExecutions(m.executions[flowID], generation), nil
}
func activeMemoryExecutions(executions map[string]memoryExecution, generation int64) int {
	count := 0
	for _, execution := range executions {
		if execution.generation <= generation {
			count++
		}
	}
	return count
}
func (m *MemoryEngine) ReconcileTerminatedExecutions(_ context.Context, flowID string, generation int64, backend string, executionIDs []string, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.flows[flowID]; !ok {
		return ErrNotFound
	}
	terminal := make(map[string]struct{}, len(executionIDs))
	for _, executionID := range executionIDs {
		if executionID != "" {
			terminal[executionID] = struct{}{}
		}
	}
	now := time.Now()
	for id, execution := range m.executions[flowID] {
		_, exact := terminal[id]
		if exact && execution.generation <= generation && execution.backend == backend && !execution.leaseUntil.IsZero() && !execution.leaseUntil.After(now) {
			delete(m.executions[flowID], id)
		}
	}
	return nil
}

var _ LifecycleStore = (*MemoryEngine)(nil)
