package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrDeliveryConflict means a stable delivery identity was reused with
	// different logical content.
	ErrDeliveryConflict = errors.New("delivery identity conflict")
	// ErrDeliveryIndeterminate means the external outcome cannot be proven and
	// must not be converted into a replay or receipt without reconciliation.
	ErrDeliveryIndeterminate = errors.New("delivery outcome indeterminate")
	// ErrDeliveryRetryExhausted means bounded delivery or reconciliation work
	// cannot make further automatic progress and requires operator recovery.
	ErrDeliveryRetryExhausted = errors.New("delivery retry budget exhausted")
)

// BindProjectionFingerprint binds a deployment-effective destination identity
// to the immutable logical projection revision.
func BindProjectionFingerprint(destinationFingerprint, projectionFingerprint string) (string, error) {
	if strings.TrimSpace(destinationFingerprint) == "" || strings.TrimSpace(projectionFingerprint) == "" {
		return "", errors.New("destination and projection fingerprints are required")
	}
	payload, err := json.Marshal(struct {
		Destination string `json:"destination_fingerprint"`
		Projection  string `json:"projection_fingerprint"`
	}{destinationFingerprint, projectionFingerprint})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

// DeliveryIntent is the immutable identity supplied to a reconcilable external
// destination attempt. PostgreSQL remains authoritative for adopting evidence
// as a durable receipt; destination evidence alone never advances a checkpoint.
type DeliveryIntent struct {
	FlowID                string
	FlowIncarnationID     string
	SourceLineageID       string
	Generation            int64
	AcquisitionID         string
	LeaseEpoch            int64
	DestinationRevisionID string
	LogicalBatchID        string
	PositionID            string
	ContentHash           string
}

// Validate rejects incomplete delivery identities before external I/O.
func (i DeliveryIntent) Validate() error {
	for name, value := range map[string]string{
		"flow_incarnation_id":     i.FlowIncarnationID,
		"source_lineage_id":       i.SourceLineageID,
		"acquisition_id":          i.AcquisitionID,
		"destination_revision_id": i.DestinationRevisionID,
		"logical_batch_id":        i.LogicalBatchID,
		"position_id":             i.PositionID,
		"content_hash":            i.ContentHash,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("delivery intent %s is required", name)
		}
	}
	expectedLogicalBatchID, err := DeliveryLogicalBatchID(i.SourceLineageID, i.PositionID, i.ContentHash)
	if err != nil {
		return fmt.Errorf("validate delivery intent logical_batch_id: %w", err)
	}
	if i.LogicalBatchID != strings.TrimSpace(i.LogicalBatchID) || strings.HasPrefix(i.LogicalBatchID, "legacy:") || i.LogicalBatchID != expectedLogicalBatchID {
		return fmt.Errorf("%w: delivery intent logical_batch_id must equal the canonical source-lineage/position/content identity", ErrDeliveryConflict)
	}
	if i.Generation <= 0 {
		return errors.New("delivery intent generation must be positive")
	}
	if i.LeaseEpoch <= 0 {
		return errors.New("delivery intent lease epoch must be positive")
	}
	return nil
}

// DeliveryEvidence is untrusted external proof returned by a destination.
type DeliveryEvidence struct {
	ExternalID  string
	ContentHash string
}

// DeliveryDisposition is the result of destination reconciliation.
type DeliveryDisposition uint8

const (
	DeliveryIndeterminate DeliveryDisposition = iota
	DeliveryNotApplied
	DeliveryApplied
)

// CanonicalArtifactDestination marks a destination specification that is
// consumed asynchronously from the PostgreSQL-authoritative artifact log. Its
// ordinary Destination methods are never a data-delivery path.
type CanonicalArtifactDestination interface {
	Destination
	CanonicalArtifactConsumer()
}

// ManagedDestination applies an immutable delivery intent and reconciles an
// ambiguous prior attempt. Implementations must fail closed when evidence is
// insufficient.
type ManagedDestination interface {
	Destination
	Apply(context.Context, DeliveryIntent, Batch) (DeliveryEvidence, error)
	Reconcile(context.Context, DeliveryIntent) (DeliveryDisposition, DeliveryEvidence, error)
}

// ManagedTransactionDestination is the full-transaction extension used by
// managed execution. Initialization establishes and verifies destination receipt
// authority before bootstrap or CDC I/O. Validation runs immediately before a
// new control-plane attempt is prepared, but never blocks adoption of an already
// committed target marker. Transactional targets commit all fragments with the
// marker; append-only targets insert ordered, replay-convergent fragments and
// write the marker last.
type ManagedTransactionDestination interface {
	ManagedDestination
	InitializeManagedDelivery(context.Context) error
	ValidateTransaction(context.Context, SourceTransaction) error
	ApplyTransaction(context.Context, DeliveryIntent, SourceTransaction) (DeliveryEvidence, error)
}

// PreparedManagedTransaction is a bounded, validated destination operation.
// The implementation hides destination-specific planning behind one Apply
// method so the coordinator does not ask an adapter to materialize a full
// transaction twice around the durable attempt boundary.
type PreparedManagedTransaction interface {
	Apply(context.Context) (DeliveryEvidence, error)
}

const ManagedPartResourceClickHouseActivePartsV1 = "clickhouse_active_parts_v1"

// ManagedPartIdentity is one immutable external part planned by a managed
// transaction. Identity is stable across retries and endpoint failover.
type ManagedPartIdentity struct {
	Kind    string
	Ordinal uint64
	QueryID string
}

// ManagedPartReservationRequest carries a destination observation and the
// complete immutable part plan to PostgreSQL. PostgreSQL serializes admission
// by destination revision; destination-side observations alone never reserve
// capacity.
type ManagedPartReservationRequest struct {
	Resource              string
	DestinationRevisionID string
	LogicalBatchID        string
	ContentHash           string
	ServerActiveParts     uint64
	Capacity              uint64
	Parts                 []ManagedPartIdentity
}

// Validate rejects incomplete, duplicated, or out-of-budget reservation plans.
func (r ManagedPartReservationRequest) Validate() error {
	if r.Resource != ManagedPartResourceClickHouseActivePartsV1 {
		return errors.New("managed part reservation resource is not supported")
	}
	if strings.TrimSpace(r.DestinationRevisionID) == "" || strings.TrimSpace(r.LogicalBatchID) == "" || strings.TrimSpace(r.ContentHash) == "" {
		return errors.New("managed part reservation identity is incomplete")
	}
	if r.Capacity == 0 || len(r.Parts) == 0 {
		return errors.New("managed part reservation capacity and parts must be positive")
	}
	seen := make(map[string]struct{}, len(r.Parts))
	for _, part := range r.Parts {
		if part.Kind != "changelog" && part.Kind != "receipt" {
			return fmt.Errorf("managed part reservation kind %q is invalid", part.Kind)
		}
		if strings.TrimSpace(part.QueryID) == "" {
			return errors.New("managed part reservation query ID is required")
		}
		key := fmt.Sprintf("%s:%d:%s", part.Kind, part.Ordinal, part.QueryID)
		if _, duplicate := seen[key]; duplicate {
			return errors.New("managed part reservation contains duplicate part identity")
		}
		seen[key] = struct{}{}
	}
	return nil
}

// ManagedPartReservation is the narrow PostgreSQL-backed capability bound to
// one admitted plan. A destination must not perform managed part writes until
// this capability has been supplied by the coordinator.
type ManagedPartReservation interface {
	ReservationID() string
	MarkPartDurable(context.Context, ManagedPartIdentity) error
}

// ManagedPartReservationPrepared is implemented only by managed append plans
// whose external operations consume a shared part budget.
type ManagedPartReservationPrepared interface {
	PreparedManagedTransaction
	PartReservationRequest() (ManagedPartReservationRequest, error)
	BindPartReservation(ManagedPartReservation) error
}

// ManagedPartReservationReconciler proves that neither endpoint contains any
// fragment or receipt for an immutable logical batch before PostgreSQL releases
// an abandoned reservation.
type ManagedPartReservationReconciler interface {
	ProvePartReservationAbsent(context.Context, DeliveryIntent) error
}

// ManagedTransactionPreparer is an optional deep interface implemented by
// managed destinations that can validate and retain one bounded transaction
// plan before PostgreSQL persists the external attempt.
type ManagedTransactionPreparer interface {
	PrepareTransaction(context.Context, DeliveryIntent, SourceTransaction) (PreparedManagedTransaction, error)
}
