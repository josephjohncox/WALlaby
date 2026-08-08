package checkpoint

import "errors"

// ErrManagedProducerActive prevents an administrative checkpoint override from
// racing a live or dispatched producer.
var ErrManagedProducerActive = errors.New("checkpoint override rejected while producer or dispatch is active")
