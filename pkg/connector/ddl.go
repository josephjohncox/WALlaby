package connector

import (
	"context"
	"errors"
)

// DDLExecutionState is the durable state returned before a destination DDL
// side effect. A retry requires destination reconciliation before execution.
type DDLExecutionState uint8

const (
	DDLExecutionUnknown DDLExecutionState = iota
	DDLExecutionNew
	DDLExecutionRetry
	DDLExecutionComplete
)

// Valid reports whether the state is a runnable protocol state.
func (s DDLExecutionState) Valid() bool {
	return s == DDLExecutionNew || s == DDLExecutionRetry || s == DDLExecutionComplete
}

// DDLReconcileResult describes whether the intended DDL effect is already
// visible at a destination after an ambiguous prior attempt.
type DDLReconcileResult uint8

const (
	DDLReconcileIndeterminate DDLReconcileResult = iota
	DDLReconcileNotApplied
	DDLReconcileApplied
)

// Valid reports whether the result is a recognized reconciliation outcome.
func (r DDLReconcileResult) Valid() bool {
	return r == DDLReconcileNotApplied || r == DDLReconcileApplied
}

// DDLReconciler inspects destination state after an ambiguous DDL attempt.
// Implementations must not mutate the destination.
type DDLReconciler interface {
	ReconcileDDL(ctx context.Context, schema Schema, record Record) (DDLReconcileResult, error)
}

var (
	ErrDDLReconciliationRequired      = errors.New("destination DDL reconciliation is required after an ambiguous attempt")
	ErrDDLReconciliationIndeterminate = errors.New("destination DDL reconciliation was indeterminate")
)
