package connector

import (
	"fmt"
	"strings"
)

// Source modes supported by PostgreSQL source options.
const (
	SourceModeCDC      = "cdc"
	SourceModeBackfill = "backfill"
)

// IsManagedSourceSpec reports whether a source requests either the legacy
// managed protocol or a named managed profile. Control-plane and runtime gates
// must use this single predicate so profile-only flows cannot bypass fencing.
func IsManagedSourceSpec(spec Spec) bool {
	if (spec.Type != "" && spec.Type != EndpointPostgres) || spec.Options == nil {
		return false
	}
	if strings.TrimSpace(spec.Options["managed_profile"]) != "" {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(spec.Options["managed"])) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// NormalizeSourceMode normalizes and validates source modes for worker flow sources.
//
// It is case-insensitive, trims whitespace, and defaults empty values to cdc.
func NormalizeSourceMode(raw string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(raw))
	if mode == "" {
		return SourceModeCDC, nil
	}
	switch mode {
	case SourceModeCDC, SourceModeBackfill:
		return mode, nil
	default:
		return "", fmt.Errorf("unsupported source mode %q (expected %s or %s)", mode, SourceModeCDC, SourceModeBackfill)
	}
}
