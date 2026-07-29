package failmatrix

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const processStateVersion = 1

const (
	processPhaseReady         = "ready_for_fault"
	processPhaseTakeoverReady = "takeover_ready"
	processPhaseComplete      = "complete"
	processPhaseFailClosed    = "fail_closed"
)

type durableAuthority struct {
	LeaseEpoch         int64 `json:"lease_epoch"`
	AttemptPrepared    bool  `json:"attempt_prepared"`
	ReceiptAdopted     bool  `json:"receipt_adopted"`
	Checkpoint         int64 `json:"checkpoint"`
	AckIntent          bool  `json:"ack_intent"`
	SourceFlushLSN     int64 `json:"source_flush_lsn"`
	FlushReceipt       bool  `json:"flush_receipt"`
	Publication        bool  `json:"publication"`
	ObjectVersion      int64 `json:"object_version"`
	ConsumerReceipt    bool  `json:"consumer_receipt"`
	RetentionReleased  bool  `json:"retention_released"`
	GCFinalized        bool  `json:"gc_finalized"`
	GCMarked           bool  `json:"gc_marked"`
	ExternalApplyCount int   `json:"external_apply_count"`
	AdoptionCount      int   `json:"adoption_count"`
}

type durableDestination struct {
	Committed      bool  `json:"committed"`
	Reveal         int   `json:"reveal"`
	ReceiptVisible bool  `json:"receipt_visible"`
	Version        int64 `json:"version"`
	ApplyAttempts  int   `json:"apply_attempts"`
}

// processState is the fsync-backed protocol state exchanged by independent
// worker processes. It deliberately describes protocol-model state, not a real
// destination implementation or PostgreSQL transaction log.
type processState struct {
	Version           int                `json:"version"`
	Revision          int64              `json:"revision"`
	Profile           string             `json:"profile"`
	Generation        int64              `json:"generation"`
	RequestedBoundary string             `json:"requested_boundary"`
	BoundaryReached   bool               `json:"boundary_reached"`
	Phase             string             `json:"phase"`
	FailClosed        bool               `json:"fail_closed"`
	DurableWrites     int                `json:"durable_writes"`
	Authority         durableAuthority   `json:"authority"`
	Destination       durableDestination `json:"destination"`
	ObjectSeq         int64              `json:"object_seq"`
	ConfirmLSN        int64              `json:"confirm_lsn"`
}

func stateFromEngine(e *engine, boundary Boundary, reached, failClosed bool, phase string, writes int) processState {
	a := e.auth
	d := e.dest
	return processState{
		Version: processStateVersion, Profile: e.profile.Name, Generation: a.leaseEpoch,
		RequestedBoundary: string(boundary), BoundaryReached: reached, Phase: phase,
		FailClosed: failClosed, DurableWrites: writes,
		Authority: durableAuthority{
			LeaseEpoch: a.leaseEpoch, AttemptPrepared: a.attemptPrepared,
			ReceiptAdopted: a.receiptAdopted, Checkpoint: a.checkpoint,
			AckIntent: a.ackIntent, SourceFlushLSN: a.sourceFlushLSN,
			FlushReceipt: a.flushReceipt, Publication: a.publication,
			ObjectVersion: a.objectVersion, ConsumerReceipt: a.consumerReceipt,
			RetentionReleased: a.retentionReleased, GCFinalized: a.gcFinalized,
			GCMarked: a.gcMarked, ExternalApplyCount: a.externalApplyCount,
			AdoptionCount: a.adoptionCount,
		},
		Destination: durableDestination{
			Committed: d.committed, Reveal: d.reveal, ReceiptVisible: d.receiptVisible,
			Version: d.version, ApplyAttempts: d.applyAttempts,
		},
		ObjectSeq: e.objectSeq, ConfirmLSN: e.confirmLSN,
	}
}

func (s processState) engine() (*engine, error) {
	profile, ok := supportedProfile(s.Profile)
	if !ok {
		return nil, fmt.Errorf("unsupported persisted profile %q", s.Profile)
	}
	if s.Version != processStateVersion {
		return nil, fmt.Errorf("unsupported process state version %d", s.Version)
	}
	a := s.Authority
	d := s.Destination
	return &engine{
		profile: profile,
		auth: authority{
			leaseEpoch: a.LeaseEpoch, attemptPrepared: a.AttemptPrepared,
			receiptAdopted: a.ReceiptAdopted, checkpoint: a.Checkpoint,
			ackIntent: a.AckIntent, sourceFlushLSN: a.SourceFlushLSN,
			flushReceipt: a.FlushReceipt, publication: a.Publication,
			objectVersion: a.ObjectVersion, consumerReceipt: a.ConsumerReceipt,
			retentionReleased: a.RetentionReleased, gcFinalized: a.GCFinalized,
			gcMarked: a.GCMarked, externalApplyCount: a.ExternalApplyCount,
			adoptionCount: a.AdoptionCount,
		},
		dest: destination{
			committed: d.Committed, reveal: d.Reveal, receiptVisible: d.ReceiptVisible,
			version: d.Version, applyAttempts: d.ApplyAttempts,
		},
		objectSeq: s.ObjectSeq, confirmLSN: s.ConfirmLSN,
	}, nil
}

func supportedProfile(name string) (Profile, bool) {
	for _, profile := range SupportedProfiles() {
		if profile.Name == name {
			return profile, true
		}
	}
	return Profile{}, false
}

func loadProcessState(path string) (processState, error) {
	// #nosec G304 -- path is supplied by the parent runner under its private work root.
	payload, err := os.ReadFile(path)
	if err != nil {
		return processState{}, err
	}
	var state processState
	if err := json.Unmarshal(payload, &state); err != nil {
		return processState{}, fmt.Errorf("decode durable process state: %w", err)
	}
	return state, nil
}

// ErrStaleGeneration identifies a durable state compare-and-swap rejection.
var ErrStaleGeneration = errors.New("failmatrix: stale durable generation")

// StaleGenerationError reports the expected and authoritative generations from
// a rejected durable mutation. It is intentionally typed so the old child PID
// proves that the state-store API, rather than an in-memory model helper,
// performed the rejection.
type StaleGenerationError struct {
	Expected int64
	Actual   int64
}

func (e *StaleGenerationError) Error() string {
	return fmt.Sprintf("%v: expected %d, authoritative %d", ErrStaleGeneration, e.Expected, e.Actual)
}

func (e *StaleGenerationError) Unwrap() error { return ErrStaleGeneration }

// storeProcessState atomically replaces state only when expectedGeneration
// matches the durable generation. Generation validation and replacement occur
// while holding the same inter-process file lock. expectedGeneration=0 creates
// the initial state and rejects an existing state file.
func storeProcessState(path string, state processState, expectedGeneration int64) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create process state directory: %w", err)
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600) // #nosec G304 -- runner-owned path.
	if err != nil {
		return fmt.Errorf("open process state lock: %w", err)
	}
	defer func() { _ = lock.Close() }()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil { // #nosec G115 -- file descriptors fit int on supported Unix targets.
		return fmt.Errorf("lock process state: %w", err)
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }() // #nosec G115 -- see lock acquisition.

	var current processState
	current, err = loadProcessState(path)
	switch {
	case err == nil:
		if expectedGeneration == 0 || current.Generation != expectedGeneration {
			return &StaleGenerationError{Expected: expectedGeneration, Actual: current.Generation}
		}
		state.Revision = current.Revision + 1
		state.DurableWrites = current.DurableWrites + 1
	case errors.Is(err, os.ErrNotExist):
		if expectedGeneration != 0 {
			return &StaleGenerationError{Expected: expectedGeneration, Actual: 0}
		}
		state.Revision = 1
		state.DurableWrites = 1
	default:
		return err
	}

	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode durable process state: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("create process state temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write process state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fsync process state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close process state: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("replace process state: %w", err)
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open process state directory: %w", err)
	}
	defer func() { _ = dir.Close() }()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("fsync process state directory: %w", err)
	}
	return nil
}

type durableStateSnapshot struct {
	Generation int64
	Revision   int64
	SHA256     string
}

func snapshotProcessState(path string) (durableStateSnapshot, error) {
	// #nosec G304 -- path is under the runner-owned cycle directory.
	payload, err := os.ReadFile(path)
	if err != nil {
		return durableStateSnapshot{}, err
	}
	var state processState
	if err := json.Unmarshal(payload, &state); err != nil {
		return durableStateSnapshot{}, fmt.Errorf("decode durable process state snapshot: %w", err)
	}
	digest := sha256.Sum256(payload)
	return durableStateSnapshot{
		Generation: state.Generation,
		Revision:   state.Revision,
		SHA256:     hex.EncodeToString(digest[:]),
	}, nil
}

func touchDurable(path string) error {
	if err := os.WriteFile(path, []byte("continue\n"), 0o600); err != nil {
		return err
	}
	// #nosec G304 -- path is the runner-owned synchronization marker.
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return file.Sync()
}
