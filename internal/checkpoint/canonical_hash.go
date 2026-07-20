package checkpoint

import "github.com/josephjohncox/wallaby/pkg/connector"

// hashOutboxBatch preserves the stored outbox hash contract while sharing the
// canonical logical-batch identity with replay-safe destinations.
func hashOutboxBatch(batch connector.Batch) (string, error) {
	return connector.BatchContentHash(batch)
}
