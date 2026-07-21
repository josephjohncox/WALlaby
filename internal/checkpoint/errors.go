package checkpoint

import (
	"errors"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// ErrNotFound is retained for compatibility with callers of internal/checkpoint.
var ErrNotFound = connector.ErrCheckpointNotFound

// ErrManagedProducerActive prevents an administrative checkpoint override from
// racing a live or dispatched producer.
var ErrManagedProducerActive = errors.New("checkpoint override rejected while producer or dispatch is active")
