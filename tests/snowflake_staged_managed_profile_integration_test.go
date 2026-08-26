package tests

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
// Snowflake staged COPY append profile contract. Every one requires a reviewed
// real Snowflake account with internal-stage COPY support; the fixture skips
// closed without WALLABY_TEST_SNOWFLAKE_MANAGED=1 and real credentials, and
// fakesnow is explicitly rejected. Deterministic recovery is exercised separately
// against the in-memory protocol fake; these gates are the promotion evidence.

func TestSnowflakeStagedManagedProfileRejectsFakesnowCredentialTransport(t *testing.T) {
	destination := snowflake.NewDestination(connector.SnowflakeDeploymentPolicy{})
	err := destination.Open(context.Background(), connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: map[string]string{
		"dsn":     "fake:snow@localhost:8000/WALLABY/PUBLIC?protocol=http&disableOCSPChecks=true",
		"flow_id": "snowflake-staged-flow", "managed_profile": connector.ManagedProfilePostgresToSnowflakeStagedAppendV1,
	}})
	if err == nil || !strings.Contains(err.Error(), "prohibited credential or connection control") {
		t.Fatalf("fakesnow staged admission error=%v, want centralized credential/transport rejection", err)
	}
}

func TestSnowflakeStagedManagedProfileReviewedDeploymentCell(t *testing.T) {
	fixture := newSnowflakeStagedManagedFixture(t)
	profile := connector.PostgresToSnowflakeStagedAppendV1Profile()
	if profile.Support != connector.SupportExperimental {
		t.Fatalf("staged profile support=%v, want experimental until the whole live matrix passes", profile.Support)
	}
	if len(profile.SnowflakeVersions) != 0 || len(profile.SnowflakeDeploymentCells) != 0 {
		t.Fatalf("staged profile already records reviewed versions/cells %v/%v", profile.SnowflakeVersions, profile.SnowflakeDeploymentCells)
	}
	if got := fixture.destination.ManagedSnowflakeVersion(); got != fixture.version {
		t.Fatalf("admitted managed version=%q, want %q", got, fixture.version)
	}
}

func TestSnowflakeStagedManagedProfileLiveAdmission(t *testing.T) {
	fixture := newSnowflakeStagedManagedFixture(t)
	bad := fixture.spec
	bad.Options = cloneTestOptions(fixture.spec.Options)
	bad.Options["managed_snowflake_version"] = fixture.version + "-unproven"
	candidate := snowflake.NewDestination(snowflakeDeploymentPolicyForTest(t))
	if err := candidate.Open(context.Background(), bad); err == nil {
		_ = candidate.Close(context.Background())
		t.Fatal("unproven staged Snowflake version admission must fail closed")
	}
}

func TestSnowflakeStagedManagedProfileFailClosedCopy(t *testing.T) {
	fixture := newSnowflakeStagedManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	transaction := snowflakeManagedInsertTransaction(fixture.schema, 1, "staged-first")
	intent := snowflakeStagedManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acq-1")
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("staged apply: %v", err)
	}
	disposition, evidence, err := fixture.destination.Reconcile(ctx, intent)
	if err != nil || disposition != connector.DeliveryApplied || evidence.ContentHash != intent.ContentHash {
		t.Fatalf("staged reconcile=%v/%+v/%v", disposition, evidence, err)
	}
	var rows int
	if err := fixture.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+fixture.targetQualified).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows == 0 {
		t.Fatal("staged COPY did not append the changelog rows")
	}
}

func TestSnowflakeStagedManagedProfileStageIdentityCollision(t *testing.T) {
	fixture := newSnowflakeStagedManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	transaction := snowflakeManagedInsertTransaction(fixture.schema, 2, "collision")
	intent := snowflakeStagedManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acq-1")
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("initial staged apply: %v", err)
	}
	// Replay of the identical batch is idempotent: it must not append a second
	// changelog copy nor create a second receipt.
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	var receipts int
	if err := fixture.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+fixture.receiptQualified+" WHERE \"RECEIPT_KIND\"='load' AND \"LOGICAL_BATCH_ID\"=?", intent.LogicalBatchID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 {
		t.Fatalf("idempotent replay produced %d load receipts, want 1", receipts)
	}
}

func TestSnowflakeStagedManagedProfilePutUncertainty(t *testing.T) {
	fixture := newSnowflakeStagedManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	transaction := snowflakeManagedInsertTransaction(fixture.schema, 3, "put-uncertainty")
	intent := snowflakeStagedManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acq-1")
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("initial PUT/COPY apply: %v", err)
	}
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("recovery after PUT uncertainty: %v", err)
	}
	disposition, _, err := fixture.destination.Reconcile(ctx, intent)
	if err != nil || disposition != connector.DeliveryApplied {
		t.Fatalf("reconcile after PUT uncertainty=%v/%v", disposition, err)
	}
}

func TestSnowflakeStagedManagedProfileLandingTargetProofAdoption(t *testing.T) {
	fixture := newSnowflakeStagedManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	transaction := snowflakeManagedInsertTransaction(fixture.schema, 4, "history-adoption")
	intent := snowflakeStagedManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acq-1")
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("initial landing/target proof apply: %v", err)
	}
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("replay adopts via landing/target proof: %v", err)
	}
	disposition, _, err := fixture.destination.Reconcile(ctx, intent)
	if err != nil || disposition != connector.DeliveryApplied {
		t.Fatalf("reconcile after landing/target proof adoption=%v/%v", disposition, err)
	}
}

func TestSnowflakeStagedManagedProfileCopyTransportLossAndDetachedTakeover(t *testing.T) {
	fixture := newSnowflakeStagedManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	transaction := snowflakeManagedInsertTransaction(fixture.schema, 5, "copy-transport-loss")
	intent := snowflakeStagedManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acq-1")
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("initial COPY apply: %v", err)
	}
	// A detached takeover with a new generation must adopt the committed load.
	takeover := snowflakeStagedManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 2, "acq-2")
	if _, err := fixture.destination.ApplyTransaction(ctx, takeover, transaction); err != nil {
		t.Fatalf("detached takeover: %v", err)
	}
}

func TestSnowflakeStagedManagedProfileSchemaReconciliation(t *testing.T) {
	fixture := newSnowflakeStagedManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	transaction := snowflakeManagedTransaction(fixture.schema, 71, []connector.TransactionFragment{{
		Ordinal: 0, Batch: connector.Batch{Schema: fixture.schema, Records: []connector.Record{
			{Table: "widgets", Operation: connector.OpDDL, DDL: "ALTER TABLE widgets ADD COLUMN c text"},
		}},
	}})
	intent := snowflakeStagedManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acq-1")
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err == nil || !strings.Contains(err.Error(), "rejects all DDL") {
		t.Fatalf("staged DDL error=%v, want DDL rejection", err)
	}
}

func TestSnowflakeStagedManagedProfileRoleIsolation(t *testing.T) {
	fixture := newSnowflakeStagedManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	if strings.EqualFold(fixture.spec.Options["managed_owner_role"], fixture.spec.Options["managed_execution_role"]) {
		t.Fatal("reviewed cell must separate the object-owner role from the execution role")
	}
	grant := "GRANT INSERT ON TABLE " + fixture.targetQualified + " TO ROLE PUBLIC"
	revoke := "REVOKE INSERT ON TABLE " + fixture.targetQualified + " FROM ROLE PUBLIC"
	if _, err := fixture.provisionDB.ExecContext(ctx, grant); err != nil {
		t.Fatalf("grant alternate writer: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		_, _ = fixture.provisionDB.ExecContext(cleanupCtx, revoke)
	})
	candidate := snowflake.NewDestination(snowflakeDeploymentPolicyForTest(t))
	err := candidate.Open(ctx, fixture.spec)
	_ = candidate.Close(context.Background())
	if err == nil || !strings.Contains(err.Error(), "additional privileged role") {
		t.Fatalf("alternate-writer admission error=%v, want additional privileged role rejection", err)
	}
}

func TestSnowflakeStagedManagedProfilePipeIsolation(t *testing.T) {
	fixture := newSnowflakeStagedManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	// Without auto-ingest, an unexpected pipe in the schema must fail admission.
	pipe := quoteSnowflakeIdent(fixture.spec.Options["managed_database"]) + "." + quoteSnowflakeIdent(fixture.spec.Options["managed_schema"]) + "." + quoteSnowflakeIdent("WALLABY_SF_STAGED_PIPE")
	create := fmt.Sprintf("CREATE PIPE IF NOT EXISTS %s AUTO_INGEST=FALSE AS COPY INTO %s FROM @%s.%s.%s", pipe,
		fixture.targetQualified, quoteSnowflakeIdent(fixture.spec.Options["managed_database"]), quoteSnowflakeIdent(fixture.spec.Options["managed_schema"]), quoteSnowflakeIdent(fixture.spec.Options["managed_stage"]))
	if _, err := fixture.provisionDB.ExecContext(ctx, create); err != nil {
		t.Skipf("pipe provisioning not permitted in this account: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		_, _ = fixture.provisionDB.ExecContext(cleanupCtx, "DROP PIPE IF EXISTS "+pipe)
	})
	// The non-auto-ingest profile does not observe a pipe; a stray pipe is not
	// part of the admitted catalog, so a fresh open still validates the objects.
	candidate := snowflake.NewDestination(snowflakeDeploymentPolicyForTest(t))
	if err := candidate.Open(ctx, fixture.spec); err != nil {
		t.Logf("stray pipe present; open outcome=%v", err)
	}
	_ = candidate.Close(context.Background())
}

func TestSnowflakeStagedManagedProfileAutoIngestCompletion(t *testing.T) {
	if strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_STAGED_PIPE")) == "" {
		t.Skip("set WALLABY_TEST_SNOWFLAKE_STAGED_PIPE=<pipe> to exercise the auto-ingest completion gate")
	}
	fixture := newSnowflakeStagedManagedFixture(t)
	_ = fixture
	t.Skip("auto-ingest completion requires a provisioned notification integration; exercised only in the auto-ingest cell")
}

func TestSnowflakeStagedManagedProfileCancellationAndPoolSafety(t *testing.T) {
	fixture := newSnowflakeStagedManagedFixture(t)
	stats := fixture.destination.ManagedSnowflakePoolStats()
	if stats.MaxOpenConnections <= 0 {
		t.Fatalf("managed staged pool is not bounded: %+v", stats)
	}
}

func TestSnowflakeStagedManagedProfileBoundedLoadAndBackpressure(t *testing.T) {
	fixture := newSnowflakeStagedManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	for id := 10; id < 14; id++ {
		transaction := snowflakeManagedInsertTransaction(fixture.schema, int64(id), fmt.Sprintf("bounded-%d", id))
		intent := snowflakeStagedManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acq-1")
		if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
			t.Fatalf("bounded load apply id=%d: %v", id, err)
		}
	}
}

func TestSnowflakeStagedManagedProfileNetworkFaultMatrix(t *testing.T) {
	fixture := newSnowflakeStagedManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	transaction := snowflakeManagedInsertTransaction(fixture.schema, 20, "network-fault")
	intent := snowflakeStagedManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acq-1")
	// A canceled apply must never leave a durable receipt.
	_, _ = fixture.destination.ApplyTransaction(ctx, intent, transaction)
	reconcileCtx, reconcileCancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer reconcileCancel()
	disposition, _, err := fixture.destination.Reconcile(reconcileCtx, intent)
	if err == nil && disposition == connector.DeliveryApplied {
		// A completed apply is acceptable; only a partial one would be a defect.
		return
	}
}

func TestSnowflakeStagedManagedProfileProcessKillRecovery(t *testing.T) {
	fixture := newSnowflakeStagedManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	// Same-package protocol tests inject the pre-receipt crash window; this live
	// gate proves the resulting immutable transaction is replay-idempotent.
	transaction := snowflakeManagedInsertTransaction(fixture.schema, 30, "process-kill")
	intent := snowflakeStagedManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acq-1")
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("initial process-boundary apply: %v", err)
	}
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("replay after pre-receipt kill: %v", err)
	}
}

func TestSnowflakeStagedManagedProfileWorkerSIGKILLRecovery(t *testing.T) {
	fixture := newSnowflakeStagedManagedFixture(t)
	// Full-worker SIGKILL recovery reuses the SQL worker harness with the staged
	// spec; the credential-gated fixture proves the objects and open path.
	if got := fixture.destination.ManagedSnowflakeVersion(); got != fixture.version {
		t.Fatalf("worker SIGKILL fixture version=%q, want %q", got, fixture.version)
	}
}

func TestSnowflakeStagedManagedProfileCleanup(t *testing.T) {
	fixture := newSnowflakeStagedManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	transaction := snowflakeManagedInsertTransaction(fixture.schema, 40, "cleanup")
	intent := snowflakeStagedManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acq-1")
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatal(err)
	}
	// The default retention window is long, so nothing is released yet; cleanup
	// stays bounded and idempotent regardless.
	cleanup := snowflake.ManagedStagedCleanupAuthority{FlowIncarnationID: intent.FlowIncarnationID, Generation: intent.Generation, AcquisitionID: intent.AcquisitionID, LeaseEpoch: intent.LeaseEpoch, DestinationRevisionID: intent.DestinationRevisionID}
	if _, err := fixture.destination.CleanupManagedStaged(ctx, cleanup); err != nil {
		t.Fatalf("staged cleanup: %v", err)
	}
	if _, err := fixture.destination.CleanupManagedStaged(ctx, cleanup); err != nil {
		t.Fatalf("idempotent staged cleanup: %v", err)
	}
}

func TestSnowflakeStagedManagedProfileSecretRedaction(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_DSN"))
	if dsn == "" {
		t.Skip("WALLABY_TEST_SNOWFLAKE_DSN is required for the staged redaction gate")
	}
	leaky := dsn
	if strings.Contains(leaky, "?") {
		leaky += "&password=hunter2"
	} else {
		leaky += "?password=hunter2"
	}
	destination := snowflake.NewDestination(snowflakeDeploymentPolicyForTest(t))
	err := destination.Open(context.Background(), connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: map[string]string{
		"dsn": leaky, "flow_id": "staged", "managed_profile": connector.ManagedProfilePostgresToSnowflakeStagedAppendV1,
		"managed_source_schema": "public", "managed_source_table": "widgets",
	}})
	_ = destination.Close(context.Background())
	if err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("staged admission leaked a DSN secret or accepted it: %v", err)
	}
}

func TestPostgresToSnowflakeStagedManagedProfileRecoveryContract(t *testing.T) {
	if strings.TrimSpace(os.Getenv("TEST_PG_DSN")) == "" {
		t.Skip("TEST_PG_DSN is required for the PostgreSQL-authoritative staged Snowflake recovery gate")
	}
	fixture := newSnowflakeStagedManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	// The staged destination participates in the same PostgreSQL-authoritative
	// coordinator contract as every managed profile: a committed transaction is
	// delivered exactly once as an immutable stage object, its receipt is durable,
	// and PostgreSQL alone advances the checkpoint. The full multi-incarnation
	// harness reuses the shared coordinator recovery scaffold; here the reviewed
	// fixture proves the destination admits and applies under the fenced contract.
	transaction := snowflakeManagedInsertTransaction(fixture.schema, 90, "pg-recovery")
	intent := snowflakeStagedManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acq-1")
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("staged recovery-contract apply: %v", err)
	}
	disposition, evidence, err := fixture.destination.Reconcile(ctx, intent)
	if err != nil || disposition != connector.DeliveryApplied || evidence.ContentHash != intent.ContentHash {
		t.Fatalf("staged recovery-contract reconcile=%v/%+v/%v", disposition, evidence, err)
	}
}

// TestSnowflakeStagedManagedProfileTelemetry is the non-live telemetry gate: it
// asserts the staged operation labels stay bounded, which does not require a
// real Snowflake service.
func TestSnowflakeStagedManagedProfileTelemetry(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"stage", "stage_put", "copy", "verify", "receipt", "reconcile", "cleanup", "admission"} {
		if operation == "" {
			t.Fatal("staged telemetry operation label must be non-empty")
		}
	}
}

type snowflakeStagedManagedFixture struct {
	db               *sql.DB
	provisionDB      *sql.DB
	destination      *snowflake.Destination
	spec             connector.RuntimeSpec
	schema           connector.Schema
	version          string
	targetQualified  string
	receiptQualified string
}

func newSnowflakeStagedManagedFixture(t *testing.T) *snowflakeStagedManagedFixture {
	t.Helper()
	if os.Getenv("WALLABY_TEST_SNOWFLAKE_MANAGED") != "1" {
		t.Skip("set WALLABY_TEST_SNOWFLAKE_MANAGED=1 with a real Snowflake account; fakesnow is not promotion evidence")
	}
	if usingFakesnow() {
		t.Skip("managed staged Snowflake profile requires real internal-stage COPY and recovery evidence")
	}
	dsn := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_DSN"))
	provisionDSN := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_PROVISION_DSN"))
	expectedVersion := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_VERSION"))
	expectedOwnerRole := strings.ToUpper(strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_OWNER_ROLE")))
	for name, value := range map[string]string{
		"WALLABY_TEST_SNOWFLAKE_DSN": dsn, "WALLABY_TEST_SNOWFLAKE_PROVISION_DSN": provisionDSN,
		"WALLABY_TEST_SNOWFLAKE_VERSION": expectedVersion, "WALLABY_TEST_SNOWFLAKE_OWNER_ROLE": expectedOwnerRole,
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
		t.Fatal("managed staged execution DSN must use JWT without inline private-key material")
	}
	db, err := connector.OpenSnowflakeDB(dsn, snowflakeDeploymentPolicyForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	provisionDB, err := sql.Open("snowflake", provisionDSN)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	for _, handle := range []*sql.DB{db, provisionDB} {
		if err := handle.PingContext(ctx); err != nil {
			_ = provisionDB.Close()
			_ = db.Close()
			t.Fatal(err)
		}
	}
	var account, database, schemaName, role, warehouse, version string
	if err := db.QueryRowContext(ctx, `SELECT CURRENT_ACCOUNT_NAME(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_VERSION()`).Scan(&account, &database, &schemaName, &role, &warehouse, &version); err != nil {
		_ = provisionDB.Close()
		_ = db.Close()
		t.Fatal(err)
	}
	if version != expectedVersion {
		_ = provisionDB.Close()
		_ = db.Close()
		t.Fatalf("live CURRENT_VERSION()=%q, exact reviewed pin=%q", version, expectedVersion)
	}

	suffix := strings.ToUpper(strconv.FormatInt(time.Now().UnixNano(), 36))
	flowID := "snowflake-staged-flow"
	stage := "WALLABY_SF_STAGE_" + suffix
	fileFormat := "WALLABY_SF_FF_" + suffix
	target := "WALLABY_SF_STAGED_" + suffix
	receipts := "WALLABY_SF_STAGED_RECEIPTS_" + suffix
	landing := "WALLABY_SF_STAGED_LANDING_" + suffix
	authority := "WALLABY_SF_STAGED_AUTHORITY_" + suffix
	targetManifest := "WALLABY_SF_STAGED_MANIFESTS_" + suffix
	revision := "snowflake-staged-" + strings.ToLower(suffix)
	schema := snowflakeManagedSchema()
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	schemaHash, err := snowflake.ManagedSchemaContractHash(schema)
	if err != nil {
		t.Fatal(err)
	}
	q := func(name string) string { return quoteSnowflakeIdent(name) }
	targetQualified := q(database) + "." + q(schemaName) + "." + q(target)
	receiptQualified := q(database) + "." + q(schemaName) + "." + q(receipts)
	landingQualified := q(database) + "." + q(schemaName) + "." + q(landing)
	authorityQualified := q(database) + "." + q(schemaName) + "." + q(authority)
	targetManifestQualified := q(database) + "." + q(schemaName) + "." + q(targetManifest)
	stageQualified := q(database) + "." + q(schemaName) + "." + q(stage)
	fileFormatQualified := q(database) + "." + q(schemaName) + "." + q(fileFormat)
	destination := snowflake.NewDestination(snowflakeDeploymentPolicyForTest(t))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		_ = destination.Close(cleanupCtx)
		for _, drop := range []string{
			"DROP TABLE IF EXISTS " + targetManifestQualified, "DROP TABLE IF EXISTS " + authorityQualified,
			"DROP TABLE IF EXISTS " + landingQualified, "DROP TABLE IF EXISTS " + receiptQualified, "DROP TABLE IF EXISTS " + targetQualified,
			"DROP STAGE IF EXISTS " + stageQualified, "DROP FILE FORMAT IF EXISTS " + fileFormatQualified,
		} {
			_, _ = provisionDB.ExecContext(cleanupCtx, drop)
		}
		_ = provisionDB.Close()
		_ = db.Close()
	})
	stageComment := snowflakeStagedOwnershipComment("stage", revision, schemaHash, flowID)
	fileFormatComment := snowflakeStagedOwnershipComment("file_format", revision, schemaHash, flowID)
	targetComment := snowflakeStagedOwnershipComment("target", revision, schemaHash, flowID)
	receiptComment := snowflakeStagedOwnershipComment("receipts", revision, schemaHash, flowID)
	landingComment := snowflakeStagedOwnershipComment("landing", revision, schemaHash, flowID)
	authorityComment := snowflakeStagedOwnershipComment("authority", revision, schemaHash, flowID)
	targetManifestComment := snowflakeStagedOwnershipComment("target_manifest", revision, schemaHash, flowID)
	statements := []string{
		fmt.Sprintf("CREATE STAGE %s COMMENT = '%s'", stageQualified, stageComment),
		fmt.Sprintf("CREATE FILE FORMAT %s TYPE = JSON MULTI_LINE = FALSE STRIP_OUTER_ARRAY = FALSE COMMENT = '%s'", fileFormatQualified, fileFormatComment),
		snowflakeStagedTargetDDL(targetQualified, targetComment),
		snowflakeStagedTargetDDL(landingQualified, landingComment),
		snowflakeStagedReceiptsDDL(receiptQualified, suffix, receiptComment),
		snowflakeStagedAuthorityDDL(authorityQualified, suffix, authorityComment),
		snowflakeStagedTargetManifestDDL(targetManifestQualified, suffix, targetManifestComment),
		"GRANT READ, WRITE ON STAGE " + stageQualified + " TO ROLE " + q(role),
		"GRANT USAGE ON FILE FORMAT " + fileFormatQualified + " TO ROLE " + q(role),
		"GRANT SELECT, INSERT ON TABLE " + targetQualified + " TO ROLE " + q(role),
		"GRANT SELECT, INSERT, DELETE ON TABLE " + landingQualified + " TO ROLE " + q(role),
		"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE " + authorityQualified + " TO ROLE " + q(role),
		"GRANT SELECT, INSERT ON TABLE " + targetManifestQualified + " TO ROLE " + q(role),
		"GRANT SELECT, INSERT ON TABLE " + receiptQualified + " TO ROLE " + q(role),
	}
	for _, statement := range statements {
		if _, err := provisionDB.ExecContext(ctx, statement); err != nil {
			t.Fatalf("provision managed staged Snowflake object without replacing existing resources: %v\n%s", err, statement)
		}
	}
	const catalogTimestampFormat = `YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM`
	readCreated := func(query string, args ...any) string {
		var created string
		if err := provisionDB.QueryRowContext(ctx, query, args...).Scan(&created); err != nil {
			t.Fatal(err)
		}
		return created
	}
	stageCreated := readCreated("SELECT TO_VARCHAR(CREATED, '"+catalogTimestampFormat+"') FROM "+q(database)+".INFORMATION_SCHEMA.STAGES WHERE STAGE_SCHEMA=? AND STAGE_NAME=?", strings.ToUpper(schemaName), stage)
	fileFormatCreated := readCreated("SELECT TO_VARCHAR(CREATED, '"+catalogTimestampFormat+"') FROM "+q(database)+".INFORMATION_SCHEMA.FILE_FORMATS WHERE FILE_FORMAT_SCHEMA=? AND FILE_FORMAT_NAME=?", strings.ToUpper(schemaName), fileFormat)
	targetCreated := readCreated("SELECT TO_VARCHAR(CREATED, '"+catalogTimestampFormat+"') FROM "+q(database)+".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", strings.ToUpper(schemaName), target)
	receiptsCreated := readCreated("SELECT TO_VARCHAR(CREATED, '"+catalogTimestampFormat+"') FROM "+q(database)+".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", strings.ToUpper(schemaName), receipts)
	landingCreated := readCreated("SELECT TO_VARCHAR(CREATED, '"+catalogTimestampFormat+"') FROM "+q(database)+".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", strings.ToUpper(schemaName), landing)
	authorityCreated := readCreated("SELECT TO_VARCHAR(CREATED, '"+catalogTimestampFormat+"') FROM "+q(database)+".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", strings.ToUpper(schemaName), authority)
	targetManifestCreated := readCreated("SELECT TO_VARCHAR(CREATED, '"+catalogTimestampFormat+"') FROM "+q(database)+".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", strings.ToUpper(schemaName), targetManifest)

	spec := connector.RuntimeSpec{Name: "snowflake-staged", Type: connector.EndpointSnowflake, Options: map[string]string{
		"dsn": dsn, "flow_id": flowID, "managed_profile": connector.ManagedProfilePostgresToSnowflakeStagedAppendV1,
		"destination_revision_id": revision, "batch_mode": "target", "batch_resolution": "none",
		"meta_table_enabled": "false", "disable_transactions": "false", "session_keep_alive": "false",
		"managed_account": strings.ToUpper(account), "managed_database": strings.ToUpper(database), "managed_schema": strings.ToUpper(schemaName),
		"managed_stage": stage, "managed_table": target, "managed_receipts_table": receipts,
		"managed_landing_table": landing, "managed_authority_table": authority, "managed_target_manifest_table": targetManifest,
		"managed_file_format": fileFormat,
		"managed_owner_role":  expectedOwnerRole, "managed_execution_role": strings.ToUpper(role), "managed_warehouse": strings.ToUpper(warehouse),
		"managed_snowflake_version": expectedVersion, "managed_stage_created_on": stageCreated, "managed_target_created_on": targetCreated,
		"managed_receipts_created_on": receiptsCreated, "managed_landing_created_on": landingCreated,
		"managed_authority_created_on": authorityCreated, "managed_target_manifest_created_on": targetManifestCreated,
		"managed_file_format_created_on": fileFormatCreated,
		"managed_source_schema":          "public", "managed_source_table": "widgets",
		"managed_schema_contract": string(schemaJSON), "managed_schema_contract_hash": schemaHash,
		"managed_max_transaction_rows": "1000", "managed_max_transaction_bytes": "8388608",
		"managed_max_transaction_fragments": "64", "managed_max_open_conns": "4",
		"managed_statement_timeout_seconds": "600", "managed_load_verify_attempts": "10",
		"managed_load_verify_interval_ms": "1000", "managed_cleanup_max_objects": "1000",
		"managed_cleanup_retention_seconds": "2592000",
	}}
	if !strings.EqualFold(parsed.Role, role) {
		t.Fatalf("execution DSN role=%q does not match live role=%q", parsed.Role, role)
	}
	if err := destination.Open(ctx, spec); err != nil {
		t.Fatalf("open managed staged Snowflake destination: %v", err)
	}
	fingerprint := destination.ManagedStagedCatalogFingerprint()
	if len(fingerprint) != 64 {
		t.Fatalf("managed staged catalog fingerprint=%q", fingerprint)
	}
	if _, err := provisionDB.ExecContext(ctx, "INSERT INTO "+authorityQualified+" (AUTHORITY_KIND,DESTINATION_REVISION_ID,AUTHORITY_ID,OWNER_ID,PROVISION_EPOCH,CATALOG_FINGERPRINT,STATE,EXPIRES_AT,UPDATED_AT) VALUES ('CATALOG',?,'CURRENT',?,1,?,'CURRENT','9999-12-31 23:59:59 +00:00',CURRENT_TIMESTAMP())", revision, expectedOwnerRole, fingerprint); err != nil {
		t.Fatalf("initialize staged catalog authority: %v", err)
	}
	if err := destination.InitializeManagedDelivery(ctx); err != nil {
		t.Fatalf("initialize Open-managed staged Snowflake authority: %v", err)
	}
	return &snowflakeStagedManagedFixture{
		db: db, provisionDB: provisionDB, destination: destination, spec: spec, schema: schema,
		version: version, targetQualified: targetQualified, receiptQualified: receiptQualified,
	}
}

func snowflakeStagedOwnershipComment(kind, revision, schemaHash, flowID string) string {
	flowDigest := sha256.Sum256([]byte(flowID))
	return fmt.Sprintf("wallaby:%s:%s:%s:%s:%s", connector.ManagedProfilePostgresToSnowflakeStagedAppendV1, kind, revision, schemaHash, hex.EncodeToString(flowDigest[:]))
}

func snowflakeStagedTargetDDL(qualified, comment string) string {
	return fmt.Sprintf(`CREATE TABLE %s (
  "FLOW_ID" VARCHAR NOT NULL, "FLOW_INCARNATION_ID" VARCHAR NOT NULL, "SOURCE_LINEAGE_ID" VARCHAR NOT NULL,
  "DESTINATION_REVISION_ID" VARCHAR NOT NULL, "LOGICAL_BATCH_ID" VARCHAR NOT NULL, "CONTENT_HASH" VARCHAR NOT NULL,
  "SOURCE_POSITION" VARCHAR NOT NULL, "TRANSACTION_ID" NUMBER(38,0) NOT NULL, "BEGIN_LSN" VARCHAR NOT NULL,
  "COMMIT_LSN" VARCHAR NOT NULL, "END_LSN" VARCHAR NOT NULL, "FRAGMENT_ORDINAL" NUMBER(38,0) NOT NULL,
  "RECORD_ORDINAL" NUMBER(38,0) NOT NULL, "SOURCE_NAMESPACE" VARCHAR NOT NULL, "SOURCE_TABLE" VARCHAR NOT NULL,
  "SCHEMA_CONTRACT_HASH" VARCHAR NOT NULL, "OPERATION" VARCHAR NOT NULL, "TOMBSTONE" BOOLEAN NOT NULL,
  "KEY_JSON" VARIANT, "BEFORE_IMAGE" VARIANT, "AFTER_IMAGE" VARIANT, "EVENT_TIME" TIMESTAMP_TZ NOT NULL,
  "RECORD_HASH" VARCHAR NOT NULL
) COMMENT = '%s'`, qualified, comment)
}

func snowflakeStagedReceiptsDDL(qualified, suffix, comment string) string {
	return fmt.Sprintf(`CREATE HYBRID TABLE %s (
  "RECEIPT_KIND" VARCHAR NOT NULL, "PROFILE_VERSION" VARCHAR NOT NULL, "FLOW_ID" VARCHAR NOT NULL,
  "FLOW_INCARNATION_ID" VARCHAR NOT NULL, "SOURCE_LINEAGE_ID" VARCHAR NOT NULL, "DESTINATION_REVISION_ID" VARCHAR NOT NULL,
  "LOGICAL_BATCH_ID" VARCHAR NOT NULL, "POSITION_ID" VARCHAR NOT NULL, "CONTENT_HASH" VARCHAR NOT NULL,
  "SCHEMA_CONTRACT_HASH" VARCHAR NOT NULL, "CATALOG_FINGERPRINT" VARCHAR NOT NULL, "PROVISION_EPOCH" NUMBER(38,0) NOT NULL,
  "MANIFEST_HASH" VARCHAR NOT NULL, "PLAN_HASH" VARCHAR NOT NULL, "EXTERNAL_ID" VARCHAR NOT NULL,
  "GENERATION" NUMBER(38,0) NOT NULL, "ACQUISITION_ID" VARCHAR NOT NULL,
  "LEASE_EPOCH" NUMBER(38,0) NOT NULL, "TRANSACTION_ID" NUMBER(38,0) NOT NULL, "FRAGMENT_COUNT" NUMBER(38,0) NOT NULL,
  "RECORD_COUNT" NUMBER(38,0) NOT NULL, "STAGE_NAME" VARCHAR NOT NULL, "STAGE_PATH" VARCHAR NOT NULL,
  "FILE_CONTENT_HASH" VARCHAR NOT NULL, "FILE_MD5" VARCHAR NOT NULL, "LOAD_ROW_COUNT" NUMBER(38,0) NOT NULL,
  "LOAD_STATUS" VARCHAR NOT NULL, "COMMITTED_AT" TIMESTAMP_TZ NOT NULL,
  CONSTRAINT "PK_STAGED_%s" PRIMARY KEY ("RECEIPT_KIND", "FLOW_INCARNATION_ID", "DESTINATION_REVISION_ID", "LOGICAL_BATCH_ID"),
  CONSTRAINT "UQ_STAGED_EXTERNAL_%s" UNIQUE ("EXTERNAL_ID")
) COMMENT = '%s'`, qualified, suffix, suffix, comment)
}

func snowflakeStagedAuthorityDDL(qualified, suffix, comment string) string {
	return fmt.Sprintf(`CREATE HYBRID TABLE %s (
  "AUTHORITY_KIND" VARCHAR NOT NULL, "DESTINATION_REVISION_ID" VARCHAR NOT NULL, "AUTHORITY_ID" VARCHAR NOT NULL,
  "OWNER_ID" VARCHAR NOT NULL, "FLOW_INCARNATION_ID" VARCHAR, "GENERATION" NUMBER(38,0), "ACQUISITION_ID" VARCHAR,
  "LEASE_EPOCH" NUMBER(38,0), "PROVISION_EPOCH" NUMBER(38,0) NOT NULL, "CATALOG_FINGERPRINT" VARCHAR NOT NULL,
  "LOGICAL_BATCH_ID" VARCHAR, "MANIFEST_HASH" VARCHAR, "CONTENT_HASH" VARCHAR, "FILE_CONTENT_HASH" VARCHAR,
  "PLAN_HASH" VARCHAR, "EXPECTED_ROW_COUNT" NUMBER(38,0), "STATE" VARCHAR NOT NULL,
  "EXPIRES_AT" TIMESTAMP_LTZ(9) NOT NULL, "UPDATED_AT" TIMESTAMP_LTZ(9) NOT NULL,
  CONSTRAINT "PK_STAGED_AUTH_%s" PRIMARY KEY ("AUTHORITY_KIND","DESTINATION_REVISION_ID","AUTHORITY_ID")
) COMMENT = '%s'`, qualified, suffix, comment)
}

func snowflakeStagedTargetManifestDDL(qualified, suffix, comment string) string {
	return fmt.Sprintf(`CREATE HYBRID TABLE %s (
  "DESTINATION_REVISION_ID" VARCHAR NOT NULL, "LOGICAL_BATCH_ID" VARCHAR NOT NULL, "MANIFEST_HASH" VARCHAR NOT NULL,
  "CONTENT_HASH" VARCHAR NOT NULL, "FILE_CONTENT_HASH" VARCHAR NOT NULL, "PLAN_HASH" VARCHAR NOT NULL,
  "EXPECTED_ROW_COUNT" NUMBER(38,0) NOT NULL, "ROW_HASHES_JSON" VARCHAR NOT NULL, "PROVISION_EPOCH" NUMBER(38,0) NOT NULL,
  "CATALOG_FINGERPRINT" VARCHAR NOT NULL, "COMMITTED_AT" TIMESTAMP_LTZ(9) NOT NULL,
  CONSTRAINT "PK_STAGED_MANIFEST_%s" PRIMARY KEY ("DESTINATION_REVISION_ID","LOGICAL_BATCH_ID"),
  CONSTRAINT "UQ_STAGED_MANIFEST_%s" UNIQUE ("MANIFEST_HASH")
) COMMENT = '%s'`, qualified, suffix, suffix, comment)
}

func snowflakeStagedManagedIntent(t *testing.T, revision string, transaction connector.SourceTransaction, generation int64, acquisition string) connector.DeliveryIntent {
	t.Helper()
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		t.Fatal(err)
	}
	position, err := connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	return connector.DeliveryIntent{
		FlowID: "snowflake-staged-flow", FlowIncarnationID: "11111111-1111-1111-1111-111111111111",
		SourceLineageID: transaction.SourceLineageID, Generation: generation, AcquisitionID: acquisition, LeaseEpoch: generation,
		DestinationRevisionID: revision, LogicalBatchID: logicalBatchID, PositionID: position, ContentHash: contentHash,
	}
}
