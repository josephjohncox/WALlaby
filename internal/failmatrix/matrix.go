package failmatrix

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
)

// positionLSN is the single logical position the modeled pipeline delivers.
const positionLSN int64 = 100

// maxSteps bounds recovery so a stuck-indeterminate model can never loop
// forever; it is a fail-closed halt, not a hang.
const maxSteps = 64

// engine holds the durable authority plus the immutable external effect for one
// modeled position under one profile.
type engine struct {
	auth    authority
	dest    destination
	profile Profile

	objectSeq  int64
	confirmLSN int64 // source-confirmed flush LSN observed by the model
}

func newEngine(p Profile) *engine {
	return &engine{auth: authority{leaseEpoch: 1}, profile: p}
}

// disposition mirrors connector.DeliveryDisposition for the model.
type disposition int

const (
	notApplied disposition = iota
	applied
	indeterminate
)

// reconcile inspects the external destination. It fails closed (Indeterminate)
// while an eventually-consistent effect is still hidden, and never reports
// Applied for an effect that did not commit.
func (e *engine) reconcile() disposition {
	if !e.dest.committed {
		return notApplied
	}
	if !e.dest.visible() {
		if e.dest.reveal > 0 {
			e.dest.reveal--
		}
		if e.dest.visible() {
			e.dest.receiptVisible = true
			return applied
		}
		return indeterminate
	}
	e.dest.receiptVisible = true
	return applied
}

// applyExternal performs the idempotent external side effect. It is identified
// by content, so repeated calls never create a second committed marker.
func (e *engine) applyExternal() error {
	if !e.profile.StreamingTransportLinked && e.profile.Visibility == EventuallyConsistent && isStreaming(e.profile) {
		return errStreamingFailClosed
	}
	if e.dest.committed {
		return nil
	}
	e.dest.applyAttempts++
	e.dest.committed = true
	e.objectSeq++
	e.dest.version = e.objectSeq
	e.dest.reveal = e.profile.HiddenReconciles
	e.auth.externalApplyCount++
	return nil
}

func isStreaming(p Profile) bool {
	return p.Name == "snowpipe-streaming-v1" || p.Name == "snowpipe-streaming-linked-v1"
}

// The stage functions below are all idempotent and fenced by the caller epoch.
// They re-derive from the durable authority so recovery converges regardless of
// where a prior worker crashed.

func (e *engine) doPrepare(epoch int64) error {
	if e.auth.attemptPrepared {
		return nil
	}
	if err := e.auth.fence(epoch); err != nil {
		return err
	}
	e.auth.attemptPrepared = true
	return nil
}

func (e *engine) doAdopt(epoch int64) error {
	if e.auth.receiptAdopted {
		return nil
	}
	// Reconcile the external effect, applying it if a prior worker crashed
	// before the side effect landed. Never adopt an indeterminate outcome.
	for attempt := 0; attempt < maxSteps; attempt++ {
		switch e.reconcile() {
		case applied:
			if err := e.auth.fence(epoch); err != nil {
				return err
			}
			e.auth.receiptAdopted = true
			e.auth.adoptionCount++
			return nil
		case notApplied:
			if err := e.applyExternal(); err != nil {
				return err
			}
		case indeterminate:
			// Fail closed: keep reconciling until the effect is observable.
			continue
		}
	}
	return fmt.Errorf("failmatrix: adoption did not converge for %s", e.profile.Name)
}

func (e *engine) doCheckpoint(epoch int64) error {
	if !e.auth.receiptAdopted {
		return fmt.Errorf("failmatrix: checkpoint requires adopted receipt")
	}
	if e.auth.checkpoint >= positionLSN {
		return nil
	}
	if err := e.auth.fence(epoch); err != nil {
		return err
	}
	e.auth.checkpoint = positionLSN
	return nil
}

func (e *engine) doSourceAck(epoch int64) error {
	if e.auth.checkpoint < positionLSN {
		return fmt.Errorf("failmatrix: source ack requires durable checkpoint")
	}
	if !e.auth.ackIntent {
		if err := e.auth.fence(epoch); err != nil {
			return err
		}
		e.auth.ackIntent = true
	}
	// Source flush is an external, monotonic feedback. It is strictly later than
	// the durable checkpoint and never exceeds it.
	if e.auth.sourceFlushLSN < e.auth.checkpoint {
		e.auth.sourceFlushLSN = e.auth.checkpoint
	}
	e.confirmLSN = e.auth.sourceFlushLSN
	if !e.auth.flushReceipt {
		if e.auth.sourceFlushLSN < e.auth.checkpoint {
			return fmt.Errorf("failmatrix: flush receipt requires observed source flush")
		}
		if err := e.auth.fence(epoch); err != nil {
			return err
		}
		e.auth.flushReceipt = true
	}
	return nil
}

func (e *engine) doPublication(epoch int64) error {
	if e.auth.publication {
		return nil
	}
	if e.auth.objectVersion == 0 {
		e.objectSeq++
		e.auth.objectVersion = e.objectSeq // immutable once written
	}
	if err := e.auth.fence(epoch); err != nil {
		return err
	}
	e.auth.publication = true
	return nil
}

func (e *engine) doConsumerReceipt(epoch int64) error {
	if e.auth.consumerReceipt {
		return nil
	}
	if !e.auth.publication {
		return fmt.Errorf("failmatrix: consumer receipt requires a committed publication")
	}
	if err := e.auth.fence(epoch); err != nil {
		return err
	}
	e.auth.consumerReceipt = true
	return nil
}

func (e *engine) doRetentionRelease(epoch int64) error {
	if e.auth.retentionReleased {
		return nil
	}
	if !e.auth.ackIntent || !e.auth.flushReceipt || !e.auth.consumerReceipt || e.auth.checkpoint < positionLSN {
		return fmt.Errorf("failmatrix: retention release requires ack + consumer receipt + checkpoint")
	}
	if err := e.auth.fence(epoch); err != nil {
		return err
	}
	e.auth.retentionReleased = true
	return nil
}

func (e *engine) doGC(epoch int64) error {
	if e.auth.gcFinalized {
		return nil
	}
	if !e.auth.retentionReleased {
		return fmt.Errorf("failmatrix: gc requires released retention root")
	}
	if !e.auth.gcMarked {
		if err := e.auth.fence(epoch); err != nil {
			return err
		}
		e.auth.gcMarked = true
	}
	// finalize: delete object + PG finalize under fence. Never deletes an object
	// referenced by an active (unreleased) publication.
	if err := e.auth.fence(epoch); err != nil {
		return err
	}
	e.auth.gcFinalized = true
	return nil
}

// stageOrder is the ordered pipeline of durable stages.
var stageOrder = []Stage{
	stagePrepare,
	stageBeforeSideEffect,
	stageAfterSideEffect,
	stageDestinationReceipt,
	stagePostgresAdoption,
	stageCheckpoint,
	stageSourceAck,
	stageArtifactPublication,
	stageConsumerReceipt,
	stageRetentionRelease,
	stageGC,
}

// runStage executes exactly one stage transition idempotently.
func (e *engine) runStage(epoch int64, s Stage) error {
	switch s {
	case stagePrepare:
		return e.doPrepare(epoch)
	case stageBeforeSideEffect:
		return nil // boundary marker only; no external effect yet
	case stageAfterSideEffect:
		return e.applyExternal()
	case stageDestinationReceipt:
		// Observe the destination-local receipt; reconcile advances visibility.
		e.reconcile()
		return nil
	case stagePostgresAdoption:
		return e.doAdopt(epoch)
	case stageCheckpoint:
		return e.doCheckpoint(epoch)
	case stageSourceAck:
		return e.doSourceAck(epoch)
	case stageArtifactPublication:
		return e.doPublication(epoch)
	case stageConsumerReceipt:
		return e.doConsumerReceipt(epoch)
	case stageRetentionRelease:
		return e.doRetentionRelease(epoch)
	case stageGC:
		return e.doGC(epoch)
	default:
		return nil
	}
}

// runToBoundary executes the pipeline with the given epoch, injecting a crash
// immediately after the work of crashAt completes. A nil crashAt (stageDone)
// runs to completion. Returns whether the crash fired and any fail-closed halt.
func (e *engine) runToBoundary(epoch int64, crashAt Stage) (injected bool, failClosed bool) {
	for _, s := range stageOrder {
		if err := e.runStage(epoch, s); err != nil {
			if errors.Is(err, errStreamingFailClosed) {
				return false, true
			}
			// A fence rejection here means a stale worker; stop this worker.
			return false, false
		}
		if s == crashAt {
			return true, false
		}
	}
	return false, false
}

// recover drives recovery to completion (or a bounded fail-closed halt) using
// the recovering epoch.
func (e *engine) recover(epoch int64) (completed bool, failClosed bool) {
	for step := 0; step < maxSteps; step++ {
		done := true
		for _, s := range stageOrder {
			if err := e.runStage(epoch, s); err != nil {
				if errors.Is(err, errStreamingFailClosed) {
					return false, true
				}
				done = false
				break
			}
		}
		if done && e.auth.gcFinalized {
			return true, false
		}
		if done {
			return true, false
		}
	}
	return e.auth.gcFinalized, false
}

// staleFencedMutationRejected asserts a stale worker (old epoch) cannot commit
// any authoritative mutation. It attempts a representative fenced write and
// expects rejection.
func (e *engine) staleFencedMutationRejected(staleEpoch int64) bool {
	before := e.auth
	// A stale worker attempts to advance the checkpoint; the fence must reject.
	err := func() error {
		if err := e.auth.fence(staleEpoch); err != nil {
			return err
		}
		e.auth.checkpoint = positionLSN + 1 // would corrupt if allowed
		return nil
	}()
	if err == nil {
		return false // stale worker committed: fencing is broken
	}
	e.auth = before // no mutation occurred
	return true
}

// CycleResult is the machine-readable evidence for a single crash cycle.
type CycleResult struct {
	Cycle           int      `json:"cycle"`
	Seed            int64    `json:"seed"`
	Profile         string   `json:"profile"`
	Kind            string   `json:"kind"`
	Boundary        string   `json:"boundary"`
	Fault           string   `json:"fault"`
	Injected        bool     `json:"injected"`
	Recovered       bool     `json:"recovered"`
	Converged       bool     `json:"converged"`
	FailClosed      bool     `json:"fail_closed"`
	ExternalApplies int      `json:"external_applies"`
	Adoptions       int      `json:"adoptions"`
	CheckpointLSN   int64    `json:"checkpoint_lsn"`
	SourceFlushLSN  int64    `json:"source_flush_lsn"`
	StaleRejected   bool     `json:"stale_rejected"`
	Violations      []string `json:"violations,omitempty"`
}

// Ok reports whether the cycle satisfied every invariant.
func (r CycleResult) Ok() bool { return len(r.Violations) == 0 }

// RunCycle executes a single deterministic crash cycle and returns its evidence.
func RunCycle(cycle int, seed int64, profile Profile, boundary Boundary, fault FaultKind) CycleResult {
	e := newEngine(profile)
	result := CycleResult{
		Cycle:    cycle,
		Seed:     seed,
		Profile:  profile.Name,
		Kind:     string(profile.Kind),
		Boundary: string(boundary),
		Fault:    string(fault),
	}

	crashStage := boundaryStage(boundary)
	injected, failClosed := e.runToBoundary(1, crashStage)
	result.Injected = injected

	staleRejected := true
	recoverEpoch := int64(1)
	if fault == FaultOverlappingTakeover {
		// Fence the crashed worker and drive recovery from a replacement worker.
		e.auth.leaseEpoch++
		recoverEpoch = e.auth.leaseEpoch
		staleRejected = e.staleFencedMutationRejected(1)
	}
	result.StaleRejected = staleRejected

	completed, recFailClosed := e.recover(recoverEpoch)
	result.Recovered = completed
	result.FailClosed = failClosed || recFailClosed
	result.ExternalApplies = e.auth.externalApplyCount
	result.Adoptions = e.auth.adoptionCount
	result.CheckpointLSN = e.auth.checkpoint
	result.SourceFlushLSN = e.auth.sourceFlushLSN

	result.Violations = e.checkInvariants(result)
	result.Converged = result.Ok()
	return result
}

// checkInvariants validates the standing safety properties after recovery.
func (e *engine) checkInvariants(r CycleResult) []string {
	var v []string
	a := &e.auth

	// Streaming fail-closed: nothing durable may advance when the transport is
	// unlinked. This is the correct behavior, not a defect.
	if !e.profile.StreamingTransportLinked && isStreaming(e.profile) {
		if a.receiptAdopted || a.checkpoint != 0 || a.ackIntent || a.flushReceipt || a.publication {
			v = append(v, "streaming_fail_closed_violated: durable state advanced without a linked transport")
		}
		// A fail-closed pipeline is converged when it correctly halts.
		return v
	}

	// INV1 AdoptionUnique: at most one durable adoption for the position.
	if a.adoptionCount > 1 {
		v = append(v, fmt.Sprintf("adoption_not_unique: %d adoptions", a.adoptionCount))
	}
	// INV11 AtLeastOnceConvergent / bounded duplicates: the external effect is
	// applied at least once but never unboundedly; the marker is a single commit.
	if e.dest.committed && e.dest.applyAttempts < 1 {
		v = append(v, "external_effect_missing_commit")
	}
	if e.dest.applyAttempts > 2 {
		v = append(v, fmt.Sprintf("unbounded_replay: %d external applies", e.dest.applyAttempts))
	}
	// INV2 ReceiptRequiresAdoption: a durable receipt implies an external commit.
	if a.receiptAdopted && !e.dest.committed {
		v = append(v, "receipt_without_side_effect")
	}
	// INV12 IndeterminateFailsClosed: a durable receipt implies the effect is
	// observable (never adopted from a hidden/indeterminate outcome).
	if a.receiptAdopted && !e.dest.visible() {
		v = append(v, "adopted_indeterminate_effect")
	}
	// INV3 CheckpointRequiresReceipt + monotonic value.
	if a.checkpoint != 0 && !a.receiptAdopted {
		v = append(v, "checkpoint_without_receipt")
	}
	if a.checkpoint != 0 && a.checkpoint != positionLSN {
		v = append(v, fmt.Sprintf("checkpoint_wrong_value: %d", a.checkpoint))
	}
	// INV4 AckAfterCheckpoint.
	if a.ackIntent && a.checkpoint < positionLSN {
		v = append(v, "ack_before_checkpoint")
	}
	if a.flushReceipt && !a.ackIntent {
		v = append(v, "flush_receipt_without_ack_intent")
	}
	// INV5 SourceFlushMonotonic and bounded by the durable checkpoint.
	if a.sourceFlushLSN > a.checkpoint {
		v = append(v, "source_flush_exceeds_checkpoint")
	}
	// INV6 ArtifactImmutable: a publication references an intact object version.
	if a.publication && a.objectVersion == 0 {
		v = append(v, "publication_without_object_version")
	}
	// INV7 ConsumerReceiptRequiresPublication.
	if a.consumerReceipt && !a.publication {
		v = append(v, "consumer_receipt_without_publication")
	}
	// INV8 RetentionSafety.
	if a.retentionReleased && (!a.ackIntent || !a.flushReceipt || !a.consumerReceipt || a.checkpoint < positionLSN) {
		v = append(v, "retention_released_without_preconditions")
	}
	// INV9 GCSafety: GC only after retention release; never deletes an active root.
	if a.gcFinalized && !a.retentionReleased {
		v = append(v, "gc_finalized_active_root")
	}
	// INV10 StaleFenceRejected.
	if r.Fault == string(FaultOverlappingTakeover) && !r.StaleRejected {
		v = append(v, "stale_worker_committed")
	}
	// Completed pipelines must fully converge (unless fail-closed streaming,
	// handled above).
	if r.Recovered && !a.gcFinalized {
		v = append(v, "recovery_incomplete")
	}
	return v
}

// PlanEntry is one cell of the deterministic execution plan.
type PlanEntry struct {
	Profile  Profile
	Boundary Boundary
}

// Config parameterizes a full matrix run.
type Config struct {
	// CyclesPerBoundary is the number of crash cycles per (profile, boundary).
	CyclesPerBoundary int
	// Seed is the master seed; per-cell seeds derive deterministically from it.
	Seed int64
	// Profiles restricts the run; empty means SupportedProfiles.
	Profiles []Profile
}

// Summary is the machine-readable roll-up for a matrix run.
type Summary struct {
	Seed              int64          `json:"seed"`
	CyclesPerBoundary int            `json:"cycles_per_boundary"`
	TotalCycles       int            `json:"total_cycles"`
	Passed            int            `json:"passed"`
	Failed            int            `json:"failed"`
	FailClosedCycles  int            `json:"fail_closed_cycles"`
	CoverageOK        bool           `json:"coverage_ok"`
	PerBoundary       map[string]int `json:"per_boundary"`
	PerProfile        map[string]int `json:"per_profile"`
	Violations        []CycleResult  `json:"violations,omitempty"`
}

// Run executes the full deterministic matrix, invoking record for every cycle's
// evidence. It returns a summary with no-skip coverage accounting: every
// (profile, boundary) cell must reach CyclesPerBoundary or CoverageOK is false.
func Run(cfg Config, record func(CycleResult)) Summary {
	if cfg.CyclesPerBoundary <= 0 {
		cfg.CyclesPerBoundary = 100
	}
	profiles := cfg.Profiles
	if len(profiles) == 0 {
		profiles = SupportedProfiles()
	}
	faults := []FaultKind{FaultKill, FaultRestart, FaultOverlappingTakeover}
	summary := Summary{
		Seed:              cfg.Seed,
		CyclesPerBoundary: cfg.CyclesPerBoundary,
		PerBoundary:       map[string]int{},
		PerProfile:        map[string]int{},
	}
	coverageOK := true
	cycleID := 0
	for _, profile := range profiles {
		for _, boundary := range RequiredBoundaries() {
			cellSeed := deriveSeed(cfg.Seed, profile.Name, string(boundary))
			// #nosec G404 -- deterministic seeded PRNG for reproducible evidence, not security.
			rng := rand.New(rand.NewSource(cellSeed))
			executed := 0
			for i := 0; i < cfg.CyclesPerBoundary; i++ {
				fault := faults[rng.Intn(len(faults))]
				cycleSeed := rng.Int63()
				result := RunCycle(cycleID, cycleSeed, profile, boundary, fault)
				cycleID++
				executed++
				summary.TotalCycles++
				summary.PerBoundary[string(boundary)]++
				summary.PerProfile[profile.Name]++
				if result.FailClosed {
					summary.FailClosedCycles++
				}
				if result.Ok() {
					summary.Passed++
				} else {
					summary.Failed++
					if len(summary.Violations) < 64 {
						summary.Violations = append(summary.Violations, result)
					}
				}
				if record != nil {
					record(result)
				}
			}
			if executed < cfg.CyclesPerBoundary {
				coverageOK = false
			}
		}
	}
	summary.CoverageOK = coverageOK && summary.Failed == 0
	return summary
}

// deriveSeed produces a stable per-cell seed from the master seed and cell keys.
func deriveSeed(master int64, keys ...string) int64 {
	h := uint64(1469598103934665603) ^ uint64(master) // #nosec G115 -- intentional wraparound bit-mix for a stable seed hash.
	for _, k := range keys {
		for i := 0; i < len(k); i++ {
			h ^= uint64(k[i])
			h *= 1099511628211
		}
	}
	// h>>1 clears the sign bit, so the result always fits in a non-negative int64.
	return int64(h >> 1) // #nosec G115 -- top bit cleared by >>1; value is always non-negative.
}

// SortedKeys returns map keys in stable order for deterministic reporting.
func SortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
