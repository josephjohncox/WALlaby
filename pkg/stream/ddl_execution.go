package stream

import (
	"context"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// DDLExecutionStore establishes immutable destination manifests before DDL
// side effects and persists per-destination execution receipts afterward.
type DDLExecutionStore interface {
	// WithDDLExecutionLock serializes a flow's destination DDL stream across
	// attempt preparation, external side effects, batch write, and receipts.
	WithDDLExecutionLock(
		ctx context.Context,
		flowID, destination string,
		fn func() error,
	) error
	PrepareDDLExecution(
		ctx context.Context,
		flowID, position, destination string,
		expectedDestinations []string,
	) (connector.DDLExecutionState, error)
	RecordVacuousDDLExecution(ctx context.Context, flowID, position, ddl string) error
	RecordDDLExecution(
		ctx context.Context,
		flowID, position, ddl, destination string,
		expectedDestinations []string,
	) error
}
