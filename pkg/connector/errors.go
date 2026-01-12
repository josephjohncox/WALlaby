package connector

import (
	"errors"
	"strconv"
)

// ErrDDLApprovalRequired signals that a DDL event is awaiting approval.
var ErrDDLApprovalRequired = errors.New("ddl approval required")

// DDLGateError captures details for a blocked DDL approval gate.
type DDLGateError struct {
	FlowID   string
	LSN      string
	DDL      string
	PlanJSON string
	Status   string
	EventID  int64
}

func (e *DDLGateError) Error() string {
	if e == nil {
		return "ddl approval required"
	}
	msg := "ddl approval required"
	if e.Status != "" {
		msg += " status=" + e.Status
	}
	if e.FlowID != "" {
		msg += " flow_id=" + e.FlowID
	}
	if e.LSN != "" {
		msg += " lsn=" + e.LSN
	}
	if e.EventID != 0 {
		msg += " event_id=" + strconv.FormatInt(e.EventID, 10)
	}
	return msg
}

func (e *DDLGateError) Unwrap() error {
	return ErrDDLApprovalRequired
}

// AsDDLGate extracts a DDLGateError from an error chain.
func AsDDLGate(err error) (*DDLGateError, bool) {
	var gate *DDLGateError
	if errors.As(err, &gate) {
		return gate, true
	}
	return nil, false
}
