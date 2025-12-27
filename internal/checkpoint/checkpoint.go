package checkpoint

import "github.com/josephjohncox/ductstream/pkg/connector"

// Store persists checkpoints for durable recovery.
type Store = connector.CheckpointStore
