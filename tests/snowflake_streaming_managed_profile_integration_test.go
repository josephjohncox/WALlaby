package tests

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/connectors/destinations/snowflake"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/snowflakedb/gosnowflake"
)

// These functions are the same-SHA live-service gates named by the constrained
// Snowpipe Streaming REST append profile contract. Deterministic channel /
// append / SQL-observed-completeness / receipt recovery is exercised separately
// against the in-memory protocol fake in the snowflake connector package; the
// fake exercises logic only and never promotes.
//
// Because no reviewed high-performance Snowpipe Streaming append transport is
// linked into this build, the profile fails closed at admission. Each live gate
// below therefore asserts the fail-closed refusal against a real Snowflake
// account rather than proving delivery from local continuation/offset tokens.
// The gates skip closed without WALLABY_TEST_SNOWFLAKE_MANAGED=1 and real
// credentials. They are the executable
// promotion barrier: promotion requires linking a transport and turning these
// refusals into positive recovery evidence on one reviewed SHA.

type snowflakeStreamingManagedFixture struct {
	spec    connector.RuntimeSpec
	version string
}

func newSnowflakeStreamingManagedFixture(t *testing.T) *snowflakeStreamingManagedFixture {
	t.Helper()
	if os.Getenv("WALLABY_TEST_SNOWFLAKE_MANAGED") != "1" {
		t.Skip("set WALLABY_TEST_SNOWFLAKE_MANAGED=1 with a real Snowflake account")
	}
	dsn := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_DSN"))
	expectedVersion := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_VERSION"))
	for name, value := range map[string]string{
		"WALLABY_TEST_SNOWFLAKE_DSN": dsn, "WALLABY_TEST_SNOWFLAKE_VERSION": expectedVersion,
	} {
		if value == "" {
			t.Fatalf("%s is required when WALLABY_TEST_SNOWFLAKE_MANAGED=1", name)
		}
	}
	parsed, err := gosnowflake.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Authenticator != gosnowflake.AuthTypeJwt || parsed.PrivateKey != nil {
		t.Fatal("managed streaming execution DSN must use JWT without inline private-key material")
	}
	db, err := connector.OpenSnowflakeDB(dsn, snowflakeDeploymentPolicyForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	var account, database, schemaName, role, warehouse, version string
	if err := db.QueryRowContext(ctx, `SELECT CURRENT_ACCOUNT_NAME(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_VERSION()`).Scan(&account, &database, &schemaName, &role, &warehouse, &version); err != nil {
		t.Fatal(err)
	}
	if version != expectedVersion {
		t.Fatalf("live CURRENT_VERSION()=%q, exact reviewed pin=%q", version, expectedVersion)
	}
	suffix := strings.ToUpper(strconv.FormatInt(time.Now().UnixNano(), 36))
	flowID := "snowflake-streaming-flow"
	revision := "snowflake-streaming-" + strings.ToLower(suffix)
	ownerRole := strings.ToUpper(strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_OWNER_ROLE")))
	if ownerRole == "" || ownerRole == strings.ToUpper(role) {
		ownerRole = "WALLABY_OWNER"
	}
	schema := snowflakeManagedSchema()
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	schemaHash, err := snowflake.ManagedSchemaContractHash(schema)
	if err != nil {
		t.Fatal(err)
	}
	created := "2026-01-01T00:00:00.000000000+00:00"
	spec := connector.RuntimeSpec{Name: "snowflake-streaming", Type: connector.EndpointSnowflake, Options: map[string]string{
		"dsn": dsn, "flow_id": flowID, "managed_profile": connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1,
		"destination_revision_id": revision, "batch_mode": "target", "batch_resolution": "none",
		"meta_table_enabled": "false", "disable_transactions": "false", "session_keep_alive": "false",
		"managed_account": strings.ToUpper(account), "managed_database": database, "managed_schema": schemaName,
		"managed_pipe": "WALLABY_SF_PIPE_" + suffix, "managed_table": "WALLABY_SF_STREAM_" + suffix,
		"managed_receipts_table": "WALLABY_SF_STREAM_RECEIPTS_" + suffix, "managed_channel_state_table": "WALLABY_SF_STREAM_CHANNELS_" + suffix,
		"managed_channel_name_prefix": "wallaby_stream", "managed_owner_role": ownerRole, "managed_execution_role": role, "managed_warehouse": warehouse,
		"managed_snowflake_version": version, "managed_pipe_created_on": created, "managed_target_created_on": created,
		"managed_receipts_created_on": created, "managed_channel_state_created_on": created, "managed_request_journal_created_on": created,
		"managed_source_schema": "public", "managed_source_table": "widgets",
		"managed_schema_contract": string(schemaJSON), "managed_schema_contract_hash": schemaHash,
		"managed_max_transaction_rows": "1000", "managed_max_transaction_bytes": "4194304",
		"managed_max_transaction_fragments": "128", "managed_max_row_bytes": "1048576", "managed_max_open_conns": "4",
		"managed_statement_timeout_seconds": "600", "managed_observe_attempts": "60", "managed_observe_interval_ms": "1000",
		"managed_append_attempts": "16", "managed_append_backoff_ms": "250",
		"managed_cleanup_max_objects": "1000", "managed_cleanup_retention_seconds": "2592000",
	}}
	return &snowflakeStreamingManagedFixture{spec: spec, version: version}
}

// assertStreamingFailsClosed opens the managed streaming destination against a
// real Snowflake account and requires the fail-closed transport refusal. This is
// the honest live-gate behavior until a reviewed append transport is linked.
func assertStreamingFailsClosed(t *testing.T, capability string) {
	t.Helper()
	fixture := newSnowflakeStreamingManagedFixture(t)
	destination := snowflake.NewDestination(snowflakeDeploymentPolicyForTest(t))
	err := destination.Open(context.Background(), fixture.spec)
	if err == nil {
		_ = destination.Close(context.Background())
		t.Fatalf("streaming %s gate: Open must fail closed until a reviewed append transport is linked", capability)
	}
	if !errors.Is(err, snowflake.ErrManagedStreamingTransportUnavailable) {
		t.Fatalf("streaming %s gate: Open error=%v, want the transport-unavailable refusal", capability, err)
	}
}

// TestSnowflakeStreamingManagedProfileReviewedTransport is the promotion barrier
// and runs without credentials: no reviewed high-performance append transport is
// linked, so the profile must stay experimental and admission must fail closed.
func TestSnowflakeStreamingManagedProfileReviewedTransport(t *testing.T) {
	if snowflake.ManagedStreamingTransportAvailable() {
		t.Fatal("a reviewed high-performance append transport is linked; promote the profile and turn the live gates into positive recovery evidence")
	}
	profile := connector.PostgresToSnowflakeStreamingRestAppendV1Profile()
	if profile.Support != connector.SupportExperimental {
		t.Fatalf("streaming profile support=%v, want experimental until a transport is linked and its live matrix passes", profile.Support)
	}
	if len(profile.SnowflakeVersions) != 0 || len(profile.SnowflakeDeploymentCells) != 0 {
		t.Fatalf("streaming profile already records reviewed versions/cells %v/%v", profile.SnowflakeVersions, profile.SnowflakeDeploymentCells)
	}
}

// TestSnowflakeStreamingManagedProfileTelemetry is non-live: it asserts the
// bounded telemetry contract that the connector-package fake exercises.
func TestSnowflakeStreamingManagedProfileTelemetry(t *testing.T) {
	profile := connector.PostgresToSnowflakeStreamingRestAppendV1Profile()
	for _, gate := range profile.Gates {
		if gate.Capability == "telemetry" && gate.Live {
			t.Fatal("telemetry is exercised with the in-memory fake and must not be a live-only gate")
		}
	}
}

func TestSnowflakeStreamingManagedProfileReviewedDeploymentCell(t *testing.T) {
	fixture := newSnowflakeStreamingManagedFixture(t)
	profile := connector.PostgresToSnowflakeStreamingRestAppendV1Profile()
	if profile.Support != connector.SupportExperimental {
		t.Fatalf("streaming profile support=%v, want experimental", profile.Support)
	}
	if len(profile.SnowflakeVersions) != 0 || len(profile.SnowflakeDeploymentCells) != 0 {
		t.Fatalf("streaming profile records reviewed versions/cells %v/%v", profile.SnowflakeVersions, profile.SnowflakeDeploymentCells)
	}
	_ = fixture
	assertStreamingFailsClosed(t, "reviewed deployment cell")
}

func TestSnowflakeStreamingManagedProfileLiveAdmission(t *testing.T) {
	assertStreamingFailsClosed(t, "live admission")
}

func TestPostgresToSnowflakeStreamingManagedProfileRecoveryContract(t *testing.T) {
	assertStreamingFailsClosed(t, "PostgreSQL recovery contract")
}

func TestSnowflakeStreamingManagedProfileRoleIsolation(t *testing.T) {
	assertStreamingFailsClosed(t, "role hierarchy and alternate writers")
}

func TestSnowflakeStreamingManagedProfileChannelRevisionEvidence(t *testing.T) {
	assertStreamingFailsClosed(t, "channel and pipe revision evidence")
}

func TestSnowflakeStreamingManagedProfileDeterministicRowObservation(t *testing.T) {
	assertStreamingFailsClosed(t, "deterministic row identity and SQL-observed completeness")
}

func TestSnowflakeStreamingManagedProfileReopenAppendsProvenMissing(t *testing.T) {
	assertStreamingFailsClosed(t, "reopen after uncommitted rows and append proven-missing")
}

func TestSnowflakeStreamingManagedProfileRejectedRowsFailClosed(t *testing.T) {
	assertStreamingFailsClosed(t, "terminal token with rejected rows fails closed")
}

func TestSnowflakeStreamingManagedProfileCompleteUnreceiptedRecovery(t *testing.T) {
	assertStreamingFailsClosed(t, "complete-unreceipted recovery and receipt adoption")
}

func TestSnowflakeStreamingManagedProfileReceiptConflictAndChannelInvalidation(t *testing.T) {
	assertStreamingFailsClosed(t, "receipt conflicts and channel invalidation")
}

func TestSnowflakeStreamingManagedProfileSchemaEvolutionAndToast(t *testing.T) {
	assertStreamingFailsClosed(t, "schema evolution and TOAST unchanged fields")
}

func TestSnowflakeStreamingManagedProfileAuthExpiryRefresh(t *testing.T) {
	assertStreamingFailsClosed(t, "auth expiry refresh")
}

func TestSnowflakeStreamingManagedProfileThrottlingBackpressure(t *testing.T) {
	assertStreamingFailsClosed(t, "throttling and backpressure")
}

func TestSnowflakeStreamingManagedProfileOversizeRejection(t *testing.T) {
	assertStreamingFailsClosed(t, "oversize rejection")
}

func TestSnowflakeStreamingManagedProfileProcessKillRecovery(t *testing.T) {
	assertStreamingFailsClosed(t, "adapter process kill")
}

func TestSnowflakeStreamingManagedProfileWorkerSIGKILLRecovery(t *testing.T) {
	assertStreamingFailsClosed(t, "full worker SIGKILL")
}

func TestSnowflakeStreamingManagedProfileCancellationAndPoolSafety(t *testing.T) {
	assertStreamingFailsClosed(t, "cancellation and pool safety")
}

func TestSnowflakeStreamingManagedProfileCleanup(t *testing.T) {
	assertStreamingFailsClosed(t, "cleanup release receipts and channel state")
}

func TestSnowflakeStreamingManagedProfileSecretRedaction(t *testing.T) {
	assertStreamingFailsClosed(t, "secret redaction")
}

// These same-SHA commercial gates stay negative until a reviewed transport is
// linked. They name the exact live boundaries required before promotion.
func TestSnowflakeStreamingManagedProfileAmbiguousRequestRecovery(t *testing.T) {
	assertStreamingFailsClosed(t, "ambiguous request recovery")
}

func TestSnowflakeStreamingManagedProfileVisibilityLagWithoutResend(t *testing.T) {
	assertStreamingFailsClosed(t, "visibility lag without resend")
}

func TestSnowflakeStreamingManagedProfileProvenAbsenceRetry(t *testing.T) {
	assertStreamingFailsClosed(t, "proven absence retry")
}

func TestSnowflakeStreamingManagedProfileRequestProcessRestart(t *testing.T) {
	assertStreamingFailsClosed(t, "request process restart")
}
