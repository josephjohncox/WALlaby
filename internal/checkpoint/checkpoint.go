package checkpoint

import (
	"context"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// ExternalStore is the typed administrative mutation seam. PostgreSQL
// implementations must guard authority and write in one transaction.
type ExternalStore interface {
	PutExternal(context.Context, string, connector.Checkpoint) error
}
