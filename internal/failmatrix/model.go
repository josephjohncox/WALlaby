// Package failmatrix implements a deterministic, in-process protocol-fake
// process-failure matrix for WALlaby's durable delivery core.
//
// It is an executable specification of the boundary chain that the real
// PostgreSQL-authoritative delivery/artifact code implements: fence acquisition,
// durable attempt, external destination side effect, destination receipt,
// PostgreSQL adoption, authoritative checkpoint, source ACK (intent + flush +
// flush receipt), artifact publication, consumer receipt, retention release,
// and garbage collection. The model injects a kill / restart / overlapping
// takeover at each boundary and asserts the standing safety invariants after
// recovery.
//
// The model is faithful to the durability contract, not to any single wire
// topology: PostgreSQL is the sole authority for fences, attempts, receipts,
// checkpoints, source feedback, publications, retention roots and GC state; the
// external destination and object store hold immutable side effects that are
// adopted only after fenced reconciliation. The model never claims exactly-once:
// replays converge by deterministic identity (at-least-once with idempotent
// dedupe), duplicates are bounded, and gaps are impossible.
//
// This package requires no live services and no credentials, so it can run
// hundreds of randomized crash cycles per boundary deterministically and
// cheaply. It exists alongside — not instead of — the real local-service
// integration harnesses, which remain the promotion evidence for the exact
// maintained profiles.
package failmatrix

import (
	"errors"
	"fmt"
)

// Kind classifies a profile as a maintained (exact, real-service) profile or an
// experimental profile. Experimental live cells are always credential-gated and
// are never counted as promotion evidence; the protocol-fake here exercises the
// experimental *protocol* only.
type Kind string

const (
	// Maintained profiles are the exact maintained real-service profiles.
	Maintained Kind = "maintained"
	// Experimental profiles model experimental destination protocols.
	Experimental Kind = "experimental"
)

// Visibility models how an external side effect becomes reconcilable after it
// durably commits at the destination.
type Visibility string

const (
	// Synchronous side effects are reconcilable immediately after commit
	// (managed PostgreSQL target, ClickHouse append, Snowflake SQL hybrid table).
	Synchronous Visibility = "synchronous"
	// EventuallyConsistent side effects land durably but are not observable for
	// a bounded number of reconcile attempts (Snowpipe staged COPY / streaming).
	// Reconciliation of a hidden effect must fail closed as Indeterminate.
	EventuallyConsistent Visibility = "eventually_consistent"
)

// Profile parameterizes one supported protocol boundary under test.
type Profile struct {
	Name string
	Kind Kind
	// Visibility of the external destination side effect.
	Visibility Visibility
	// HiddenReconciles is the number of reconcile attempts an eventually
	// consistent effect stays hidden before it becomes observable.
	HiddenReconciles int
	// StreamingTransportLinked reports whether a streaming transport is wired.
	// When false the side effect fails closed and no checkpoint/ack advances.
	StreamingTransportLinked bool
}

// SupportedProfiles is the required matrix of supported protocol boundaries.
// Maintained cells mirror the exact maintained profiles; experimental cells
// exercise the experimental protocols with protocol fakes only.
func SupportedProfiles() []Profile {
	return []Profile{
		{Name: "postgres-to-postgres-v1", Kind: Maintained, Visibility: Synchronous, StreamingTransportLinked: true},
		{Name: "clickhouse-append-v1", Kind: Maintained, Visibility: Synchronous, StreamingTransportLinked: true},
		{Name: "snowflake-sql-v1", Kind: Experimental, Visibility: Synchronous, StreamingTransportLinked: true},
		{Name: "snowpipe-copy-v1", Kind: Experimental, Visibility: EventuallyConsistent, HiddenReconciles: 2, StreamingTransportLinked: true},
		{Name: "snowpipe-streaming-linked-v1", Kind: Experimental, Visibility: EventuallyConsistent, HiddenReconciles: 1, StreamingTransportLinked: true},
		// The default streaming transport is not linked; the side effect must
		// fail closed and the pipeline must never advance a checkpoint or ACK.
		{Name: "snowpipe-streaming-v1", Kind: Experimental, Visibility: EventuallyConsistent, HiddenReconciles: 1, StreamingTransportLinked: false},
	}
}

// Stage is a durable boundary in the delivery pipeline. Stages are ordered; a
// crash is injected immediately after the named work of a stage completes (or,
// for beforeSideEffect, immediately before the external effect).
type Stage int

const (
	stageAcquire Stage = iota
	stagePrepare
	stageBeforeSideEffect
	stageAfterSideEffect
	stageDestinationReceipt
	stagePostgresAdoption
	stageCheckpoint
	stageSourceAck
	stageArtifactPublication
	stageConsumerReceipt
	stageRetentionRelease
	stageGC
	stageDone
)

// Boundary is the externally injectable crash point name. These map one-to-one
// to the boundaries required by the failure matrix.
type Boundary string

const (
	BoundaryBeforeSideEffect    Boundary = "before_side_effect"
	BoundaryAfterSideEffect     Boundary = "after_side_effect"
	BoundaryDestinationReceipt  Boundary = "destination_receipt"
	BoundaryPostgresAdoption    Boundary = "postgres_adoption"
	BoundaryCheckpoint          Boundary = "checkpoint"
	BoundarySourceAck           Boundary = "source_ack"
	BoundaryArtifactPublication Boundary = "artifact_publication"
	BoundaryConsumerReceipt     Boundary = "consumer_receipt"
	BoundaryRetentionRelease    Boundary = "retention_release"
	BoundaryGC                  Boundary = "gc"
)

// RequiredBoundaries is the full set of boundaries that must be exercised at
// least the configured minimum number of cycles for every supported profile.
func RequiredBoundaries() []Boundary {
	return []Boundary{
		BoundaryBeforeSideEffect,
		BoundaryAfterSideEffect,
		BoundaryDestinationReceipt,
		BoundaryPostgresAdoption,
		BoundaryCheckpoint,
		BoundarySourceAck,
		BoundaryArtifactPublication,
		BoundaryConsumerReceipt,
		BoundaryRetentionRelease,
		BoundaryGC,
	}
}

func boundaryStage(b Boundary) Stage {
	switch b {
	case BoundaryBeforeSideEffect:
		return stageBeforeSideEffect
	case BoundaryAfterSideEffect:
		return stageAfterSideEffect
	case BoundaryDestinationReceipt:
		return stageDestinationReceipt
	case BoundaryPostgresAdoption:
		return stagePostgresAdoption
	case BoundaryCheckpoint:
		return stageCheckpoint
	case BoundarySourceAck:
		return stageSourceAck
	case BoundaryArtifactPublication:
		return stageArtifactPublication
	case BoundaryConsumerReceipt:
		return stageConsumerReceipt
	case BoundaryRetentionRelease:
		return stageRetentionRelease
	case BoundaryGC:
		return stageGC
	default:
		return stageDone
	}
}

// FaultKind selects how the worker is interrupted at the chosen boundary.
type FaultKind string

const (
	// FaultKill stops the worker mid-flight; the same identity restarts recovery.
	FaultKill FaultKind = "kill"
	// FaultRestart is a graceful process restart at the boundary.
	FaultRestart FaultKind = "restart"
	// FaultOverlappingTakeover fences the crashed worker with a new lease epoch
	// and drives recovery from a concurrent replacement worker. The stale worker
	// must never be able to commit afterward.
	FaultOverlappingTakeover FaultKind = "overlapping_takeover"
)

// errStreamingFailClosed is returned when a streaming transport is not linked.
var errStreamingFailClosed = errors.New("failmatrix: streaming transport not linked; fail closed")

// authority is the PostgreSQL-authoritative durable state. Every mutation is
// fenced by the current lease epoch; a stale epoch is rejected. This mirrors
// pg_advisory_xact_lock + ValidateRunFence in the same transaction.
type authority struct {
	leaseEpoch int64

	// durable attempt exists before any external side effect.
	attemptPrepared bool
	// adopted external evidence -> durable destination receipt.
	receiptAdopted bool
	// authoritative checkpoint position (0 = none).
	checkpoint int64
	// source ACK intent recorded atomically with the checkpoint.
	ackIntent bool
	// source confirmed flush LSN (monotonic) and the recorded flush receipt.
	sourceFlushLSN int64
	flushReceipt   bool
	// artifact publication committed (references object version).
	publication bool
	// object version referenced by the publication (immutable once written).
	objectVersion int64
	// consumer catalog commit + consumer receipt.
	consumerReceipt bool
	// retention root released.
	retentionReleased bool
	// GC finalized (object swept).
	gcFinalized bool
	// gcMarked records the mark/finalize crash boundary.
	gcMarked bool

	// counters used for at-least-once accounting.
	externalApplyCount int
	adoptionCount      int
}

// fence validates the caller epoch against the authoritative lease epoch.
func (a *authority) fence(epoch int64) error {
	if epoch != a.leaseEpoch {
		return fmt.Errorf("failmatrix: stale fence epoch %d != authoritative %d", epoch, a.leaseEpoch)
	}
	return nil
}

// destination is the immutable external side effect (managed target / object
// store). It is idempotent by content identity: applying the same position
// twice never produces two distinct committed markers.
type destination struct {
	// committed marks the durable external commit for the single modeled
	// position. Once true it stays true (immutable, versioned).
	committed bool
	// reveal counts down the hidden window for eventually consistent effects.
	reveal int
	// receipt marks the destination-local receipt visibility.
	receiptVisible bool
	// version is a monotonic object/commit version assigned on first commit.
	version int64
	// applyAttempts counts external commit attempts (must stay bounded).
	applyAttempts int
}

// visible reports whether the committed effect is currently observable to a
// reconcile. Synchronous effects are visible immediately; eventually consistent
// effects stay hidden until the reveal counter reaches zero.
func (d *destination) visible() bool {
	return d.committed && d.reveal <= 0
}
