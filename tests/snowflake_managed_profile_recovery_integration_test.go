package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/connectors/destinations/snowflake"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// These functions are the same-SHA live-service gates named by the constrained
// Snowflake SQL profile contract. Every one requires a reviewed real Snowflake
// account with hybrid-table support; newSnowflakeManagedFixture skips closed
// without WALLABY_TEST_SNOWFLAKE_MANAGED=1 and real credentials, and fakesnow is
// explicitly rejected. They exist and stay credential-gated so the profile
// remains experimental until this whole matrix passes without skips on one
// reviewed service version and deployment cell.

// TestSnowflakeManagedProfileReviewedDeploymentCell proves the live runtime is
// the exact reviewed cell: commercial AWS region, the configured CURRENT_VERSION
// pin, and a profile that is still, deliberately, unpromoted.
func TestSnowflakeManagedProfileReviewedDeploymentCell(t *testing.T) {
	fixture := newSnowflakeManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	var version, region, edition string
	if err := fixture.db.QueryRowContext(ctx, `SELECT CURRENT_VERSION(), CURRENT_REGION(), SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO()`).Scan(&version, &region, &edition); err != nil {
		// Not every account exposes the platform-info function; the region and
		// version pins below remain the load-bearing deployment-cell evidence.
		if err := fixture.db.QueryRowContext(ctx, `SELECT CURRENT_VERSION(), CURRENT_REGION()`).Scan(&version, &region); err != nil {
			t.Fatal(err)
		}
	}
	if version != fixture.version {
		t.Fatalf("live CURRENT_VERSION()=%q, exact reviewed pin=%q", version, fixture.version)
	}
	if !strings.HasPrefix(strings.ToUpper(region), "AWS_") {
		t.Fatalf("managed Snowflake reviewed cell requires a commercial AWS region, got %q", region)
	}
	if got := fixture.destination.ManagedSnowflakeVersion(); got != fixture.version {
		t.Fatalf("admitted managed version=%q, want %q", got, fixture.version)
	}

	// The reviewed-cell evidence must not silently promote the profile: with no
	// recorded service version or deployment cell it stays experimental and
	// cannot pass ValidatePromotion under maintained support.
	profile := connector.PostgresToSnowflakeSQLV1Profile()
	if profile.Support != connector.SupportExperimental {
		t.Fatalf("Snowflake SQL profile support=%v, want experimental until the full live matrix passes", profile.Support)
	}
	if len(profile.SnowflakeVersions) != 0 || len(profile.SnowflakeDeploymentCells) != 0 {
		t.Fatalf("Snowflake SQL profile already records reviewed versions/cells %v/%v; promotion requires this whole gate matrix", profile.SnowflakeVersions, profile.SnowflakeDeploymentCells)
	}
	t.Logf("reviewed deployment cell: region=%s CURRENT_VERSION()=%s edition=%q (profile remains experimental)", region, version, edition)
}

// TestSnowflakeManagedProfileRoleIsolation proves the object-owner and execution
// roles are distinct and that any additional writer grant fails admission.
func TestSnowflakeManagedProfileRoleIsolation(t *testing.T) {
	fixture := newSnowflakeManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	if strings.EqualFold(fixture.spec.Options["managed_owner_role"], fixture.spec.Options["managed_execution_role"]) {
		t.Fatal("reviewed cell must separate the object-owner role from the execution role")
	}

	// Grant a write privilege to an unrelated role (PUBLIC exists in every
	// account). A fresh open must reject the target because an alternate writer
	// can now mutate owned data outside the execution grant.
	grant := "GRANT INSERT ON TABLE " + fixture.targetQualified + " TO ROLE PUBLIC"
	revoke := "REVOKE INSERT ON TABLE " + fixture.targetQualified + " FROM ROLE PUBLIC"
	if _, err := fixture.provisionDB.ExecContext(ctx, grant); err != nil {
		t.Fatalf("grant alternate writer: %v", err)
	}
	revoked := false
	t.Cleanup(func() {
		if revoked {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		if _, err := fixture.provisionDB.ExecContext(cleanupCtx, revoke); err != nil {
			t.Errorf("revoke alternate writer: %v", err)
		}
	})
	candidate := &snowflake.Destination{}
	err := candidate.Open(ctx, fixture.spec)
	_ = candidate.Close(context.Background())
	if err == nil || !strings.Contains(err.Error(), "additional writer role") {
		t.Fatalf("alternate-writer admission error=%v, want additional writer rejection", err)
	}
	if _, err := fixture.provisionDB.ExecContext(ctx, revoke); err != nil {
		t.Fatalf("revoke alternate writer: %v", err)
	}
	revoked = true

	// After revocation the reviewed grants are admitted again; a plain insert
	// commits and reconciles, proving the isolation is the only barrier removed.
	transaction := snowflakeManagedInsertTransaction(fixture.schema, 61, "role-isolation")
	intent := snowflakeManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acquisition-1")
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("apply after restoring reviewed grants: %v", err)
	}
	disposition, _, err := fixture.destination.Reconcile(ctx, intent)
	if err != nil || disposition != connector.DeliveryApplied {
		t.Fatalf("reconcile after restoring reviewed grants=%v/%v", disposition, err)
	}
}

// TestSnowflakeManagedProfileTaskIsolation proves a task in the managed schema
// fails admission: automation must not be able to mutate owned objects.
func TestSnowflakeManagedProfileTaskIsolation(t *testing.T) {
	fixture := newSnowflakeManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	database := fixture.spec.Options["managed_database"]
	schema := fixture.spec.Options["managed_schema"]
	warehouse := fixture.spec.Options["managed_warehouse"]
	task := quoteSnowflakeIdent(database) + "." + quoteSnowflakeIdent(schema) + "." + quoteSnowflakeIdent("WALLABY_SF_GATE_TASK")
	create := fmt.Sprintf("CREATE TASK IF NOT EXISTS %s WAREHOUSE = %s SCHEDULE = '1440 MINUTE' AS SELECT 1", task, quoteSnowflakeIdent(warehouse))
	if _, err := fixture.provisionDB.ExecContext(ctx, create); err != nil {
		t.Fatalf("create isolation task: %v", err)
	}
	dropped := false
	t.Cleanup(func() {
		if dropped {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		if _, err := fixture.provisionDB.ExecContext(cleanupCtx, "DROP TASK IF EXISTS "+task); err != nil {
			t.Errorf("drop isolation task: %v", err)
		}
	})
	candidate := &snowflake.Destination{}
	err := candidate.Open(ctx, fixture.spec)
	_ = candidate.Close(context.Background())
	if err == nil || !strings.Contains(err.Error(), "tasks") {
		t.Fatalf("task-isolation admission error=%v, want task rejection", err)
	}
	if _, err := fixture.provisionDB.ExecContext(ctx, "DROP TASK IF EXISTS "+task); err != nil {
		t.Fatalf("drop isolation task: %v", err)
	}
	dropped = true
	reopened := &snowflake.Destination{}
	if err := reopened.Open(ctx, fixture.spec); err != nil {
		t.Fatalf("reopen after dropping isolation task: %v", err)
	}
	if err := reopened.Close(context.Background()); err != nil {
		t.Fatalf("close reopened destination: %v", err)
	}
}

// TestSnowflakeManagedProfileCommitAndDetachedTakeover proves a different
// session resolves a durable committed receipt through cross-session reconciliation.
func TestSnowflakeManagedProfileCommitAndDetachedTakeover(t *testing.T) {
	fixture := newSnowflakeManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	transaction := snowflakeManagedInsertTransaction(fixture.schema, 71, "transport-loss")
	intent := snowflakeManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acquisition-1")
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("commit error=%v", err)
	}

	// A detached takeover: a brand-new destination session (as a replacement
	// worker would open) must see the committed receipt via READ_LATEST_WRITES
	// and report applied without re-executing the DML.
	takeover := &snowflake.Destination{}
	if err := takeover.Open(ctx, fixture.spec); err != nil {
		t.Fatalf("open detached takeover session: %v", err)
	}
	defer func() { _ = takeover.Close(context.Background()) }()
	disposition, evidence, err := takeover.Reconcile(ctx, intent)
	if err != nil || disposition != connector.DeliveryApplied || evidence.ContentHash != intent.ContentHash {
		t.Fatalf("detached takeover reconcile=%v/%+v/%v, want applied", disposition, evidence, err)
	}

	// The takeover session must refuse to bind to a foreign flow incarnation
	// while owned receipts exist, proving incarnation isolation across sessions.
	if err := takeover.ValidateManagedFlowScope(ctx, intent.FlowID, "22222222-2222-2222-2222-222222222222"); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("foreign-incarnation takeover scope error=%v, want conflict", err)
	}
	var rows int
	if err := fixture.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+fixture.targetQualified+` WHERE "ID"=71`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("committed rows after transport loss=%d, want exactly one", rows)
	}
}

// TestSnowflakeManagedProfileWorkerSIGKILLRecovery proves a full worker SIGKILL
// during an open pre-commit transaction leaves nothing committed and that a
// fresh process replays to exactly one row.
func TestSnowflakeManagedProfileWorkerSIGKILLRecovery(t *testing.T) {
	fixture := newSnowflakeManagedFixture(t)
	signal := filepath.Join(t.TempDir(), "snowflake-sigkill-open-transaction")
	encodedSpec, err := json.Marshal(fixture.spec)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestSnowflakeManagedProfileProcessKillHelper$", "-test.v")
	command.Env = append(os.Environ(),
		"WALLABY_SNOWFLAKE_PROCESS_HELPER=1",
		"WALLABY_SNOWFLAKE_PROCESS_SPEC="+string(encodedSpec),
		"WALLABY_SNOWFLAKE_PROCESS_SIGNAL="+signal,
	)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		if _, err := os.Stat(signal); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = command.Process.Signal(syscall.SIGKILL)
			_ = command.Wait()
			t.Fatal("worker did not reach the pre-COMMIT open-transaction boundary")
		}
		time.Sleep(100 * time.Millisecond)
	}
	// A full, ungraceful worker SIGKILL: no adapter Close, no rollback path runs.
	if err := command.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = command.Wait()

	transaction := snowflakeManagedInsertTransaction(fixture.schema, 99, "process-kill")
	intent := snowflakeManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "process-helper")
	deadline = time.Now().Add(2 * time.Minute)
	for {
		disposition, _, reconcileErr := fixture.destination.Reconcile(context.Background(), intent)
		if reconcileErr == nil && disposition == connector.DeliveryNotApplied {
			break
		}
		if reconcileErr == nil && disposition == connector.DeliveryApplied {
			t.Fatal("SIGKILL before COMMIT unexpectedly left a committed receipt")
		}
		if time.Now().After(deadline) {
			t.Fatalf("post-SIGKILL reconciliation=%v/%v", disposition, reconcileErr)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); err != nil {
		t.Fatalf("replay after worker SIGKILL: %v", err)
	}
	var rows int
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+fixture.targetQualified+` WHERE "ID"=99`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("rows after SIGKILL replay=%d, want 1", rows)
	}
	stats := fixture.destination.ManagedSnowflakePoolStats()
	if stats.InUse != 0 || stats.OpenConnections > stats.MaxOpenConnections {
		t.Fatalf("pool stats after SIGKILL recovery=%+v, want no leaked in-use connection", stats)
	}
}

// TestSnowflakeManagedProfileBoundedLoadAndBackpressure proves a bounded burst
// commits within the connection pool ceiling and that an over-bound transaction
// is rejected before any DML.
func TestSnowflakeManagedProfileBoundedLoadAndBackpressure(t *testing.T) {
	fixture := newSnowflakeManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const burst = 24
	for offset := 0; offset < burst; offset++ {
		id := int64(200 + offset)
		transaction := snowflakeManagedInsertTransaction(fixture.schema, id, fmt.Sprintf("bounded-%d", id))
		intent := snowflakeManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acquisition-1")
		if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
			t.Fatalf("bounded load apply id=%d: %v", id, err)
		}
	}
	var committed int
	if err := fixture.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+fixture.targetQualified+` WHERE "ID" BETWEEN 200 AND `+fmt.Sprintf("%d", 200+burst-1)).Scan(&committed); err != nil {
		t.Fatal(err)
	}
	if committed != burst {
		t.Fatalf("bounded burst committed=%d, want %d", committed, burst)
	}
	stats := fixture.destination.ManagedSnowflakePoolStats()
	if stats.MaxOpenConnections <= 0 || stats.OpenConnections > stats.MaxOpenConnections || stats.InUse != 0 {
		t.Fatalf("pool stats under bounded load=%+v, want bounded and released", stats)
	}

	// An over-bound transaction (more fragments than admitted) must fail closed
	// before any DML and leave the target unchanged.
	fragments := make([]connector.TransactionFragment, 0, 65)
	for ordinal := 0; ordinal < 65; ordinal++ {
		id := int64(4000 + ordinal)
		after := map[string]any{}
		for _, column := range fixture.schema.Columns {
			after[column.Name] = nil
		}
		after["id"] = id
		after["value"] = "over-bound"
		fragments = append(fragments, connector.TransactionFragment{Ordinal: uint64(ordinal), Batch: connector.Batch{Schema: fixture.schema, Records: []connector.Record{{
			Table: "widgets", Operation: connector.OpInsert, Key: recordKey(t, map[string]any{"id": id}), After: after,
		}}}})
	}
	overBound := snowflakeManagedTransaction(fixture.schema, 6000, fragments)
	intent := snowflakeManagedIntent(t, fixture.spec.Options["destination_revision_id"], overBound, 1, "acquisition-1")
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, overBound); err == nil || !strings.Contains(err.Error(), "fragments") {
		t.Fatalf("over-bound apply error=%v, want fragment-bound rejection", err)
	}
	var overBoundRows int
	if err := fixture.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+fixture.targetQualified+` WHERE "ID" >= 4000`).Scan(&overBoundRows); err != nil {
		t.Fatal(err)
	}
	if overBoundRows != 0 {
		t.Fatalf("over-bound transaction leaked %d rows, want 0", overBoundRows)
	}
}

// TestSnowflakeManagedProfileSecretRedaction proves connector errors never echo
// the DSN or its embedded key material.
func TestSnowflakeManagedProfileSecretRedaction(t *testing.T) {
	fixture := newSnowflakeManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	dsn := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_DSN"))
	if dsn == "" {
		t.Fatal("WALLABY_TEST_SNOWFLAKE_DSN is required for the redaction gate")
	}
	secret := extractDSNParam(dsn, "privateKey")
	assertRedacted := func(context string, err error) {
		if err == nil {
			t.Fatalf("%s: expected an error to inspect for redaction", context)
		}
		message := err.Error()
		if strings.Contains(message, dsn) {
			t.Fatalf("%s error leaks the full DSN", context)
		}
		if secret != "" && len(secret) >= 16 && strings.Contains(message, secret) {
			t.Fatalf("%s error leaks embedded key material", context)
		}
	}

	// Admission failure on a mismatched runtime pin.
	bad := fixture.spec
	bad.Options = cloneTestOptions(fixture.spec.Options)
	bad.Options["managed_snowflake_version"] = fixture.version + "-unproven"
	candidate := &snowflake.Destination{}
	admissionErr := candidate.Open(ctx, bad)
	_ = candidate.Close(context.Background())
	assertRedacted("admission", admissionErr)

	// Delivery-time failure on an over-bound transaction.
	over := snowflakeManagedTransaction(fixture.schema, 5000, make([]connector.TransactionFragment, 0))
	overIntent := snowflakeManagedIntent(t, fixture.spec.Options["destination_revision_id"], over, 1, "acquisition-1")
	_, deliverErr := fixture.destination.ApplyTransaction(ctx, overIntent, over)
	assertRedacted("empty transaction", deliverErr)
}

// TestSnowflakeManagedProfileCleanup proves owner-scoped cleanup: the execution
// role owns no destructive privilege, an owner cleanup of the delivered rows and
// receipt returns the destination to not-applied, and a replay recovers exactly
// one row.
func TestSnowflakeManagedProfileCleanup(t *testing.T) {
	fixture := newSnowflakeManagedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	transaction := snowflakeManagedInsertTransaction(fixture.schema, 700, "cleanup")
	intent := snowflakeManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acquisition-1")
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatal(err)
	}
	disposition, _, err := fixture.destination.Reconcile(ctx, intent)
	if err != nil || disposition != connector.DeliveryApplied {
		t.Fatalf("post-apply reconcile=%v/%v", disposition, err)
	}

	// Every receipt in the owned table belongs to this flow, incarnation, and
	// revision; the Open ownership guard already rejects foreign rows, and the
	// count here confirms the managed profile mutates no shared metadata.
	var ownedReceipts, foreignReceipts int
	if err := fixture.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+fixture.receiptQualified+` WHERE "FLOW_ID"=? AND "DESTINATION_REVISION_ID"=? AND "FLOW_INCARNATION_ID"=?`,
		intent.FlowID, intent.DestinationRevisionID, intent.FlowIncarnationID).Scan(&ownedReceipts); err != nil {
		t.Fatal(err)
	}
	if ownedReceipts < 1 {
		t.Fatalf("owned receipts=%d, want at least one", ownedReceipts)
	}
	if err := fixture.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+fixture.receiptQualified+` WHERE "FLOW_ID"<>? OR "DESTINATION_REVISION_ID"<>? OR "FLOW_INCARNATION_ID"<>?`,
		intent.FlowID, intent.DestinationRevisionID, intent.FlowIncarnationID).Scan(&foreignReceipts); err != nil {
		t.Fatal(err)
	}
	if foreignReceipts != 0 {
		t.Fatalf("foreign receipts=%d, want none", foreignReceipts)
	}

	// The execution role must not hold a destructive grant on the receipts
	// table; only the owner can clean it up.
	if _, err := fixture.db.ExecContext(ctx, "DELETE FROM "+fixture.receiptQualified+` WHERE "LOGICAL_BATCH_ID"=?`, intent.LogicalBatchID); err == nil {
		t.Fatal("execution role deleted an owned receipt; cleanup must be owner-scoped")
	}

	// Owner cleanup of the delivered row and receipt returns the destination to
	// a not-applied state, and a replay recovers exactly one committed row.
	if _, err := fixture.provisionDB.ExecContext(ctx, "DELETE FROM "+fixture.receiptQualified+` WHERE "FLOW_INCARNATION_ID"=? AND "DESTINATION_REVISION_ID"=? AND "LOGICAL_BATCH_ID"=?`,
		intent.FlowIncarnationID, intent.DestinationRevisionID, intent.LogicalBatchID); err != nil {
		t.Fatalf("owner receipt cleanup: %v", err)
	}
	if _, err := fixture.provisionDB.ExecContext(ctx, "DELETE FROM "+fixture.targetQualified+` WHERE "ID"=700`); err != nil {
		t.Fatalf("owner target cleanup: %v", err)
	}
	disposition, _, err = fixture.destination.Reconcile(ctx, intent)
	if err != nil || disposition != connector.DeliveryNotApplied {
		t.Fatalf("post-cleanup reconcile=%v/%v, want not applied", disposition, err)
	}
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("replay after owner cleanup: %v", err)
	}
	var rows int
	if err := fixture.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+fixture.targetQualified+` WHERE "ID"=700`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("rows after cleanup replay=%d, want 1", rows)
	}
}

func extractDSNParam(dsn, name string) string {
	marker := name + "="
	index := strings.Index(dsn, marker)
	if index < 0 {
		return ""
	}
	value := dsn[index+len(marker):]
	if end := strings.IndexAny(value, "&"); end >= 0 {
		value = value[:end]
	}
	return value
}
