package stream

import "context"

// DDLExecutionStore establishes immutable destination manifests before DDL
// side effects and persists per-destination execution receipts afterward.
type DDLExecutionStore interface {
	PrepareDDLExecution(
		ctx context.Context,
		flowID, position, destination string,
		expectedDestinations []string,
	) (alreadyExecuted bool, err error)
	RecordDDLExecution(
		ctx context.Context,
		flowID, position, ddl, destination string,
		expectedDestinations []string,
	) error
}
