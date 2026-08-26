package snowflake

import (
	"strings"
	"testing"
)

func TestManagedStreamCurrentSchemaContainsRequestAuthority(t *testing.T) {
	cfg := streamTestConfig(t)
	ddl := strings.Join(managedStreamCurrentSchemaDDL(cfg), "\n")
	for _, required := range []string{
		"STATE_VERSION", "REQUEST_ID", "INPUT_CONTINUATION_TOKEN", "REQUESTED_OFFSET_TOKEN",
		"MANIFEST_HASH", "ROWS_CONTENT_HASH", "GENERATION", "ACQUISITION_ID", "LEASE_EPOCH",
		"SENDING_UNKNOWN", "PROVEN_ABSENT", "WALLABY_STREAM_REQUEST_ATTEMPT",
	} {
		if !strings.Contains(ddl, required) {
			t.Fatalf("current streaming schema missing %q", required)
		}
	}
	for _, forbidden := range []string{"IF NOT EXISTS", "legacy", "fallback"} {
		if strings.Contains(ddl, forbidden) {
			t.Fatalf("current streaming schema contains compatibility token %q", forbidden)
		}
	}
}

func TestStreamRequestSQLUsesCASPredicates(t *testing.T) {
	cfg := streamTestConfig(t)
	for name, contract := range map[string]struct {
		query    string
		required []string
	}{
		"channel": {query: streamChannelStateMergeSQL(cfg), required: []string{"STATE_VERSION", "EXPECTED_VERSION"}},
		"request": {query: streamRequestTransitionSQL(cfg), required: []string{"PHASE_VERSION", "PHASE"}},
	} {
		for _, required := range contract.required {
			if !strings.Contains(contract.query, required) {
				t.Fatalf("%s CAS SQL missing %q: %s", name, required, contract.query)
			}
		}
	}
	if !strings.Contains(streamRequestTransitionSQL(cfg), `"PHASE_VERSION" = ?`) || !strings.Contains(streamRequestTransitionSQL(cfg), `"PHASE" = ?`) {
		t.Fatal("request transition is not phase/version compare-and-swap")
	}
}
