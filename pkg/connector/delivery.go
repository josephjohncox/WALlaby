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

// DeliveryConfigFingerprint returns a deterministic identity for the behavior
// of one destination revision. The revision ID itself is excluded so callers
// can compare two independently named revisions with identical configuration.
func DeliveryConfigFingerprint(spec Spec) (string, error) {
	options := make(map[string]string, len(spec.Options))
	for key, value := range spec.Options {
		if key == "destination_revision_id" {
			continue
		}
		options[key] = value
	}
	payload, err := json.Marshal(struct {
		Name    string            `json:"name"`
		Type    EndpointType      `json:"type"`
		Options map[string]string `json:"options"`
	}{Name: spec.Name, Type: spec.Type, Options: options})
	if err != nil {
		return "", fmt.Errorf("encode delivery config fingerprint: %w", err)
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
		"position_id":             i.PositionID,
		"content_hash":            i.ContentHash,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("delivery intent %s is required", name)
		}
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
// named managed profiles. Validation runs immediately before a new control-plane
// attempt is prepared, but never blocks adoption of an already committed target
// marker. Transactional targets commit all fragments with the marker; append-only
// targets insert ordered, replay-convergent fragments and write the marker last.
type ManagedTransactionDestination interface {
	ManagedDestination
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

// ManagedTransactionPreparer is an optional deep interface implemented by
// managed destinations that can validate and retain one bounded transaction
// plan before PostgreSQL persists the external attempt.
type ManagedTransactionPreparer interface {
	PrepareTransaction(context.Context, DeliveryIntent, SourceTransaction) (PreparedManagedTransaction, error)
}
