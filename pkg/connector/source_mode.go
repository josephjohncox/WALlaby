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
