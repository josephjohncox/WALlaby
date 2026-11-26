package connector

import (
	"errors"
	"fmt"
	"strings"
)

var persistedIcebergOptions = map[string]struct{}{
	"catalog_profile": {}, "control_table": {}, "destination_revision_id": {},
}

// ValidatePersistedSpec rejects endpoint options that cannot safely become
// durable flow state. Deployment-only credentials and behavior controls must
// never be smuggled through a connector's arbitrary option map.
func ValidatePersistedSpec(spec RuntimeSpec) error {
	if IsSnowflakeEndpoint(spec.Type) {
		return ValidatePersistedSnowflakeSpec(spec)
	}
	if spec.Type == EndpointPostgres {
		return validatePersistedPostgresSnapshotState(spec)
	}
	if spec.Type != EndpointIceberg {
		return nil
	}
	for rawKey := range spec.Options {
		key := strings.ToLower(strings.TrimSpace(rawKey))
		if _, ok := persistedIcebergOptions[key]; !ok {
			return fmt.Errorf("unsupported persisted Iceberg option %q; catalog credentials, endpoints not explicitly admitted, fixed-table collapse, and behavior controls belong to deployment configuration", rawKey)
		}
	}
	if strings.TrimSpace(spec.Options["destination_revision_id"]) == "" {
		return errors.New("iceberg destination_revision_id is required")
	}
	profile := strings.ToLower(strings.TrimSpace(spec.Options["catalog_profile"]))
	if profile != "" && profile != "rest" && profile != "s3tables" {
		return fmt.Errorf("unsupported Iceberg catalog_profile %q", profile)
	}
	return nil
}

// validatePersistedPostgresSnapshotState rejects the worker-local file snapshot
// state backend. A persisted flow is replayed on whichever worker leases it, so
// a database-stored file path would direct that worker's filesystem writes. The
// typed snapshot_state.file_path field and local RuntimeSpec options remain
// valid for direct, operator-run backfill, which never passes through here.
func validatePersistedPostgresSnapshotState(spec RuntimeSpec) error {
	for rawKey, value := range spec.Options {
		switch strings.ToLower(strings.TrimSpace(rawKey)) {
		case "snapshot_state_backend":
			if strings.EqualFold(strings.TrimSpace(value), "file") {
				return errors.New("persisted PostgreSQL snapshot_state file backend is not allowed; use the postgres backend or run a local backfill")
			}
		case "snapshot_state_path":
			if strings.TrimSpace(value) != "" {
				return errors.New("persisted PostgreSQL snapshot_state_path is not allowed; file snapshot state is local-only")
			}
		}
	}
	return nil
}
