package checkpoint

import "github.com/josephjohncox/wallaby/pkg/connector"

// Store persists checkpoints for durable recovery.
type Store = connector.CheckpointStore
