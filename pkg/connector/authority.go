package connector

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// RunFence is an immutable capability identifying one live flow producer.
type RunFence struct {
	FlowIncarnationID uuid.UUID
	FlowID            string
	Generation        int64
	AcquisitionID     uuid.UUID
	ExecutionID       string
	LeaseEpoch        int64
}

// Validate rejects incomplete authority before it can reach SQL. In
// particular, a default-zero fence is never a compatibility mode.
func (f RunFence) Validate() error {
	for name, value := range map[string]string{
		"flow_id":      f.FlowID,
		"execution_id": f.ExecutionID,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("run fence %s is required", name)
		}
	}
	if f.FlowIncarnationID == uuid.Nil {
		return errors.New("run fence flow_incarnation_id is required")
	}
	if f.AcquisitionID == uuid.Nil {
		return errors.New("run fence acquisition_id is required")
	}
	if f.Generation <= 0 {
		return errors.New("run fence generation must be positive")
	}
	if f.LeaseEpoch <= 0 {
		return errors.New("run fence lease_epoch must be positive")
	}
	return nil
}

// CleanupFence is an immutable terminal source-resource capability. It is
// distinct from RunFence so ordinary managed data-plane methods cannot accept
// cleanup authority accidentally.
type CleanupFence struct {
	RunFence
}

// Validate rejects incomplete terminal cleanup authority.
func (f CleanupFence) Validate() error {
	return f.RunFence.Validate()
}

// RunFenceBinder receives producer authority before a managed connector opens.
// Binding is immutable for one connector instance.
type RunFenceBinder interface {
	BindRunFence(RunFence) error
}

// ClaimKind scopes a resource claim without extending public lifecycle state.
type ClaimKind string

const (
	ClaimSnapshot ClaimKind = "snapshot"
	ClaimDelivery ClaimKind = "delivery"
	ClaimConsumer ClaimKind = "consumer"
	ClaimGC       ClaimKind = "gc"
)

// ClaimFence adds exact work ownership to a producer fence.
type ClaimFence struct {
	RunFence
	Kind       ClaimKind
	WorkID     string
	ClaimEpoch int64
}

// Validate rejects a claim that is not tied to a complete producer fence and
// one positive claim epoch.
func (f ClaimFence) Validate() error {
	if err := f.RunFence.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(string(f.Kind)) == "" {
		return errors.New("claim fence kind is required")
	}
	if strings.TrimSpace(f.WorkID) == "" {
		return errors.New("claim fence work_id is required")
	}
	if f.ClaimEpoch <= 0 {
		return errors.New("claim fence claim_epoch must be positive")
	}
	return nil
}

// AckGrant is a PostgreSQL-authorized source feedback position.
type AckGrant struct {
	Checkpoint Checkpoint
	PositionID string
}
