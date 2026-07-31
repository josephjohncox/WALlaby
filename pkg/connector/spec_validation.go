package connector

import (
	"errors"
	"fmt"
	"strings"
)

var persistedIcebergOptions = map[string]struct{}{
	"catalog_profile": {}, "namespace": {}, "table_prefix": {},
	"control_table": {}, "destination_revision_id": {},
}

// ValidatePersistedSpec rejects endpoint options that cannot safely become
// durable flow state. Deployment-only credentials and behavior controls must
// never be smuggled through a connector's arbitrary option map.
func ValidatePersistedSpec(spec Spec) error {
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
