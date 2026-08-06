package failmatrix

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ProcessWorkerMode identifies the role of a child OS process.
type ProcessWorkerMode string

const (
	ProcessWorkerInitial        ProcessWorkerMode = "initial"
	ProcessWorkerInitialRestart ProcessWorkerMode = "initial_restart"
	ProcessWorkerRecover        ProcessWorkerMode = "recover"
	ProcessWorkerTakeover       ProcessWorkerMode = "takeover"
)

// ProcessWorkerConfig is the wire contract used by the prebuilt child executable.
type ProcessWorkerConfig struct {
	Mode       ProcessWorkerMode
	StatePath  string
	Profile    string
	Boundary   Boundary
	Generation int64
	Schedule   string
}

// PlannedRestartExitCode is emitted only after the initial child durably reaches
// the requested boundary.
const PlannedRestartExitCode = 75

// ErrPlannedRestart is returned by an initial child after it durably reaches the
// requested boundary. The command maps it to PlannedRestartExitCode.
var ErrPlannedRestart = errors.New("planned durable-boundary restart")

// RunProcessWorker executes one child role against fsync-backed state.
func RunProcessWorker(cfg ProcessWorkerConfig) error {
	switch cfg.Mode {
	case ProcessWorkerInitial, ProcessWorkerInitialRestart:
		return runInitialProcessWorker(cfg)
	case ProcessWorkerRecover:
		return runRecoveryProcessWorker(cfg, false)
	case ProcessWorkerTakeover:
		return runRecoveryProcessWorker(cfg, true)
	default:
		return fmt.Errorf("unknown process worker mode %q", cfg.Mode)
	}
}

func runInitialProcessWorker(cfg ProcessWorkerConfig) error {
	profile, ok := supportedProfile(cfg.Profile)
	if !ok {
		return fmt.Errorf("unsupported profile %q", cfg.Profile)
	}
	if boundaryStage(cfg.Boundary) == stageDone {
		return fmt.Errorf("unsupported boundary %q", cfg.Boundary)
	}
	e := newEngine(profile)
	injected, failClosed := e.runToBoundary(cfg.Generation, boundaryStage(cfg.Boundary))
	phase := processPhaseReady
	state := stateFromEngine(e, cfg.Boundary, injected, failClosed, phase, 0)
	if err := storeProcessState(cfg.StatePath, state, 0); err != nil {
		return err
	}
	if cfg.Mode == ProcessWorkerInitialRestart {
		return ErrPlannedRestart
	}
	if err := waitForFile(filepath.Join(filepath.Dir(cfg.StatePath), "resume-old")); err != nil {
		return err
	}
	return recordStaleAttempt(cfg.StatePath, cfg.Generation)
}

func runRecoveryProcessWorker(cfg ProcessWorkerConfig, takeover bool) error {
	state, err := loadProcessState(cfg.StatePath)
	if err != nil {
		return err
	}
	if takeover {
		if cfg.Generation <= state.Generation {
			return fmt.Errorf("takeover generation %d must exceed durable generation %d", cfg.Generation, state.Generation)
		}
		state.Generation = cfg.Generation
		state.Authority.LeaseEpoch = cfg.Generation
		state.Phase = processPhaseTakeoverReady
		if err := runDurableTransitionHandshake(cfg, "takeover", func() error {
			return storeProcessState(cfg.StatePath, state, state.Generation-1)
		}); err != nil {
			return err
		}
		if err := waitForFile(filepath.Join(filepath.Dir(cfg.StatePath), "continue-new")); err != nil {
			return err
		}
	} else if state.Generation != cfg.Generation {
		return fmt.Errorf("recovery generation %d does not match durable generation %d", cfg.Generation, state.Generation)
	}

	state, err = loadProcessState(cfg.StatePath)
	if err != nil {
		return err
	}
	e, err := state.engine()
	if err != nil {
		return err
	}
	completed, failClosed := e.recover(cfg.Generation)
	phase := processPhaseComplete
	if failClosed {
		phase = processPhaseFailClosed
	}
	if !completed && !failClosed {
		return fmt.Errorf("process recovery did not converge")
	}
	finalState := stateFromEngine(e, Boundary(state.RequestedBoundary), state.BoundaryReached, state.FailClosed || failClosed, phase, state.DurableWrites)
	if takeover {
		return storeProcessState(cfg.StatePath, finalState, cfg.Generation)
	}
	return runDurableTransitionHandshake(cfg, "recovery", func() error {
		return storeProcessState(cfg.StatePath, finalState, cfg.Generation)
	})
}

func runDurableTransitionHandshake(cfg ProcessWorkerConfig, transition string, durableTransition func() error) error {
	if cfg.Schedule == "" {
		return durableTransition()
	}
	dir := filepath.Dir(cfg.StatePath)
	pre := filepath.Join(dir, transition+"-pre")
	release := filepath.Join(dir, transition+"-release")
	post := filepath.Join(dir, transition+"-post")
	ack := filepath.Join(dir, transition+"-ack")
	if err := touchDurable(pre); err != nil {
		return err
	}
	if err := waitForFile(release); err != nil {
		return err
	}
	if err := durableTransition(); err != nil {
		return err
	}
	if err := touchDurable(post); err != nil {
		return err
	}
	return waitForFile(ack)
}

type staleAttemptResult struct {
	PID               int    `json:"pid"`
	AttemptGeneration int64  `json:"attempt_generation"`
	DurableGeneration int64  `json:"durable_generation"`
	MutationAttempted bool   `json:"mutation_attempted"`
	ErrorType         string `json:"error_type"`
	Rejected          bool   `json:"rejected"`
	StateUnchanged    bool   `json:"state_unchanged"`
	RevisionBefore    int64  `json:"revision_before"`
	RevisionAfter     int64  `json:"revision_after"`
	StateSHA256Before string `json:"state_sha256_before"`
	StateSHA256After  string `json:"state_sha256_after"`
}

func recordStaleAttempt(statePath string, generation int64) error {
	state, err := loadProcessState(statePath)
	if err != nil {
		return err
	}
	before, err := snapshotProcessState(statePath)
	if err != nil {
		return err
	}
	// Attempt a real corrupting durable mutation through the same locked state
	// store API used by takeover and recovery. CAS must reject it before rename.
	candidate := state
	candidate.Authority.Checkpoint = positionLSN + 1
	candidate.Phase = processPhaseComplete
	mutationErr := storeProcessState(statePath, candidate, generation)
	after, snapshotErr := snapshotProcessState(statePath)
	if snapshotErr != nil {
		return snapshotErr
	}
	var staleErr *StaleGenerationError
	rejected := errors.As(mutationErr, &staleErr)
	result := staleAttemptResult{
		PID: os.Getpid(), AttemptGeneration: generation,
		DurableGeneration: state.Generation, MutationAttempted: true,
		Rejected: rejected, StateUnchanged: before == after,
		RevisionBefore: before.Revision, RevisionAfter: after.Revision,
		StateSHA256Before: before.SHA256, StateSHA256After: after.SHA256,
	}
	if rejected {
		result.ErrorType = "stale_generation"
	}
	if !result.Rejected || !result.StateUnchanged {
		if mutationErr == nil {
			return fmt.Errorf("stale process generation %d mutation was accepted; unchanged=%t", generation, result.StateUnchanged)
		}
		return fmt.Errorf("stale process generation %d mutation rejection=%t unchanged=%t: %w", generation, result.Rejected, result.StateUnchanged, mutationErr)
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	path := filepath.Join(filepath.Dir(statePath), "stale-result.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		return err
	}
	// #nosec G304 -- path is the runner-owned stale-attempt evidence file.
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return file.Sync()
}

func waitForFile(path string) error {
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		time.Sleep(2 * time.Millisecond)
	}
}
