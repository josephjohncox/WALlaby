package checkpoint

import (
	"context"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// Store persists checkpoints for durable recovery.
type Store = connector.CheckpointStore

// ExternalStore is the typed administrative mutation seam. PostgreSQL
// implementations must guard authority and write in one transaction.
type ExternalStore interface {
	PutExternal(context.Context, string, connector.Checkpoint) error
}
