package connector

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// BootstrapIntent identifies one immutable managed snapshot generation. It is
// independent of the public lifecycle state, which remains running while the
// private bootstrap phases advance.
type BootstrapIntent struct {
	FlowID                string
	FlowIncarnationID     string
	SourceLineageID       string
	BootstrapID           string
	BootstrapGeneration   int64
	Generation            int64
	AcquisitionID         string
	LeaseEpoch            int64
	DestinationRevisionID string
	ManifestHash          string
}

// Validate rejects incomplete bootstrap identities before any destination
// table or source resource can be changed.
func (i BootstrapIntent) Validate() error {
	for name, value := range map[string]string{
		"flow_id":                 i.FlowID,
		"flow_incarnation_id":     i.FlowIncarnationID,
		"source_lineage_id":       i.SourceLineageID,
		"bootstrap_id":            i.BootstrapID,
		"acquisition_id":          i.AcquisitionID,
		"destination_revision_id": i.DestinationRevisionID,
		"manifest_hash":           i.ManifestHash,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("bootstrap intent %s is required", name)
		}
	}
	if i.BootstrapGeneration <= 0 || i.Generation <= 0 || i.LeaseEpoch <= 0 {
		return errors.New("bootstrap intent generations and lease epoch must be positive")
	}
	return nil
}

// ManagedBootstrapResult is the durable source cut installed by a managed
// bootstrap. SourceOptions are runtime-only overrides derived from
// PostgreSQL-authoritative resource rows; they are never written back to the
// public flow definition.
type ManagedBootstrapResult struct {
	SourceOptions   map[string]string
	Checkpoint      Checkpoint
	CheckpointValid bool
}

// ManagedBootstrapSource owns the source-specific exported-snapshot protocol.
// The destination is already open when this method is called.
type ManagedBootstrapSource interface {
	PrepareManagedBootstrap(context.Context, RunFence, Spec, string, ManagedBootstrapDestination) (ManagedBootstrapResult, error)
}

// ManagedSourceResourceCleaner retires source resources owned by a managed
// flow after its stopping generation has quiesced. Implementations must never
// drop adopted resources and must prove external absence before returning.
type ManagedSourceResourceCleaner interface {
	CleanupManagedResources(context.Context, CleanupFence, Spec) error
}

// ManagedBootstrapDestination stages one immutable snapshot generation and
// atomically publishes every table in its frozen manifest. External evidence
// is untrusted until the source bootstrap coordinator records it under the
// current RunFence in PostgreSQL.
type ManagedBootstrapDestination interface {
	ManagedDestination
	PrepareBootstrap(context.Context, BootstrapIntent, []Schema) error
	ApplyBootstrap(context.Context, BootstrapIntent, DeliveryIntent, Batch) (DeliveryEvidence, error)
	ReconcileBootstrap(context.Context, BootstrapIntent, DeliveryIntent) (DeliveryDisposition, DeliveryEvidence, error)
	PublishBootstrap(context.Context, BootstrapIntent, []Schema) (DeliveryEvidence, error)
	AbandonBootstrap(context.Context, BootstrapIntent, []Schema) error
}

// ManagedBootstrapPublicationReconciler is an optional recovery extension. It
// preserves the original destination contract while allowing implementations
// with an atomic publication marker to prove a publish-before-control-receipt
// crash without replaying publication.
type ManagedBootstrapPublicationReconciler interface {
	ReconcileBootstrapPublication(context.Context, BootstrapIntent) (DeliveryDisposition, DeliveryEvidence, error)
}
