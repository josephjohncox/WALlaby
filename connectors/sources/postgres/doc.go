// Package postgres implements PostgreSQL change-data capture sources.
//
// It supports pgoutput logical replication, transaction assembly, managed
// publication and replication-slot setup, exported-snapshot bootstrap, source
// feedback, and named-profile admission checks. Generic configurations remain
// subject to their declared support level; package availability alone does not
// promote a managed profile.
package postgres
