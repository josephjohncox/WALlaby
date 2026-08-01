package connector

import "github.com/google/uuid"

// RunFence is an immutable capability identifying one live flow producer.
type RunFence struct {
	FlowIncarnationID uuid.UUID
	FlowID            string
	Generation        int64
	AcquisitionID     uuid.UUID
	ExecutionID       string
	LeaseEpoch        int64
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

// AckGrant is a PostgreSQL-authorized source feedback position.
type AckGrant struct {
	Checkpoint Checkpoint
	PositionID string
}
