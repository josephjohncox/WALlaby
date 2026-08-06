package snowflake

import (
	"context"
	"errors"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func streamTestFixture(t *testing.T) (streamConfig, connector.SourceTransaction, connector.DeliveryIntent, managedStreamPlan) {
	t.Helper()
	cfg := streamTestConfig(t)
	transaction := managedTestTransaction(managedTestSchema())
	intent := streamTestIntent(t, cfg, transaction)
	plan, err := planManagedStreamTransaction(cfg, intent, transaction)
	if err != nil {
		t.Fatalf("plan streaming transaction: %v", err)
	}
	plan.catalogFingerprint = "catalog-fingerprint"
	plan.receipt.catalogFingerprint = "catalog-fingerprint"
	return cfg, transaction, intent, plan
}

func TestStreamDriverAppendHappyPathWritesReceipt(t *testing.T) {
	cfg, transaction, intent, plan := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	driver := newStreamTestDriver(cfg, proto)

	evidence, err := driver.apply(context.Background(), intent, transaction)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if evidence.ExternalID != plan.identity.externalID {
		t.Fatalf("evidence external id=%q want %q", evidence.ExternalID, plan.identity.externalID)
	}
	if proto.insertCalls != 1 {
		t.Fatalf("insert calls=%d want 1", proto.insertCalls)
	}
	for _, hash := range plan.rowHashes {
		if entry := proto.committed[hash]; entry == nil || entry.count != 1 {
			t.Fatalf("row %s committed count!=1: %+v", hash, entry)
		}
	}
	// Channel/pipe revision and committed-token evidence must be persisted.
	state, ok, err := proto.LookupChannelState(context.Background(), cfg, streamChannelStateKey{flowIncarnationID: plan.receipt.flowIncarnationID, destinationRevisionID: plan.receipt.destinationRevisionID, channelName: plan.identity.channelName})
	if err != nil || !ok {
		t.Fatalf("channel state missing: ok=%v err=%v", ok, err)
	}
	if state.committedOffsetToken == "" || state.channelRevision == 0 {
		t.Fatalf("channel evidence incomplete: %+v", state)
	}
}

func TestStreamDriverReplayIsIdempotent(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	driver := newStreamTestDriver(cfg, proto)

	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	appendCallsAfterFirst := proto.appendCalls
	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("replay apply: %v", err)
	}
	if proto.insertCalls != 1 {
		t.Fatalf("replay inserted a second receipt: insertCalls=%d", proto.insertCalls)
	}
	if proto.appendCalls != appendCallsAfterFirst {
		t.Fatalf("replay re-appended rows: before=%d after=%d", appendCallsAfterFirst, proto.appendCalls)
	}
}

func TestStreamDriverCompleteUnreceiptedRecoveryAppendsNothing(t *testing.T) {
	cfg, transaction, intent, plan := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	// Every row is already SQL-observed present, but no receipt exists yet.
	for _, hash := range plan.rowHashes {
		proto.seedCommittedRow(hash, 1)
	}
	driver := newStreamTestDriver(cfg, proto)

	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if proto.appendCalls != 0 {
		t.Fatalf("complete-unreceipted recovery appended rows: appendCalls=%d", proto.appendCalls)
	}
	if proto.insertCalls != 1 {
		t.Fatalf("complete-unreceipted recovery did not adopt a receipt: insertCalls=%d", proto.insertCalls)
	}
}

func TestStreamDriverAppendsOnlyProvenMissingAfterPartialRecovery(t *testing.T) {
	cfg, transaction, intent, plan := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	// The first row already committed on a prior attempt.
	proto.seedCommittedRow(plan.rowHashes[0], 1)
	driver := newStreamTestDriver(cfg, proto)

	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("apply: %v", err)
	}
	for _, hash := range plan.rowHashes {
		if entry := proto.committed[hash]; entry == nil || entry.count != 1 {
			t.Fatalf("row %s committed count!=1 after partial recovery: %+v", hash, entry)
		}
	}
}

func TestStreamDriverRejectedRowsFailClosed(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	proto.appendRejectsRows = true
	driver := newStreamTestDriver(cfg, proto)

	_, err := driver.apply(context.Background(), intent, transaction)
	if !errors.Is(err, connector.ErrDeliveryConflict) || !errors.Is(err, errStreamRowsRejected) {
		t.Fatalf("rejected rows must fail closed, got %v", err)
	}
	if proto.insertCalls != 0 {
		t.Fatalf("rejected rows wrote a receipt: insertCalls=%d", proto.insertCalls)
	}
}

func TestStreamDriverChannelInvalidationReopensAndConverges(t *testing.T) {
	cfg, transaction, intent, plan := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	proto.appendInvalidateThenReopen = true
	driver := newStreamTestDriver(cfg, proto)

	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if proto.openCalls < 2 {
		t.Fatalf("channel invalidation did not reopen: openCalls=%d", proto.openCalls)
	}
	for _, hash := range plan.rowHashes {
		if entry := proto.committed[hash]; entry == nil || entry.count != 1 {
			t.Fatalf("row %s committed count!=1 after reopen: %+v", hash, entry)
		}
	}
}

func TestStreamDriverAuthExpiryRefreshesAndConverges(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	proto.appendAuthExpiresOnce = true
	refreshed := 0
	driver := newStreamTestDriver(cfg, proto)
	driver.hooks.RefreshAuth = func(context.Context) error { refreshed++; return nil }

	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if refreshed == 0 {
		t.Fatal("auth expiry did not trigger credential refresh")
	}
}

func TestStreamDriverThrottlingBacksOffAndConverges(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	proto.appendThrottleTimes = 3
	driver := newStreamTestDriver(cfg, proto)

	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("apply under throttling: %v", err)
	}
	if proto.insertCalls != 1 {
		t.Fatalf("throttling failed to converge: insertCalls=%d", proto.insertCalls)
	}
}

func TestStreamDriverCommitThenLostResponseConvergesWithoutDuplicate(t *testing.T) {
	cfg, transaction, intent, plan := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	// The append durably commits the rows, then the response is lost (throttle).
	proto.appendCommitsThenThrottle = true
	driver := newStreamTestDriver(cfg, proto)

	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("apply: %v", err)
	}
	for _, hash := range plan.rowHashes {
		if entry := proto.committed[hash]; entry == nil || entry.count != 1 {
			t.Fatalf("row %s committed count!=1 after lost response: %+v", hash, entry)
		}
	}
}

// TestStreamDriverLostResponseWithLaggingObservationFailsClosed pins the
// promotion-relevant liveness boundary the response-loss test alone does not
// reach. When an append durably commits but its response is lost (throttle) AND
// SQL observation lags the commit (commitVisibilityDelay), the bounded retry
// re-appends the still-unobserved rows, and the next SQL observation sees the
// duplicated deterministic identity. Rather than acknowledge a silent duplicate,
// the driver fails closed with errStreamObservationInconsistent and writes no
// receipt. This is exactly why admission requires READ_LATEST_WRITES=true: the
// recovery invariant depends on read-after-append observation. Without it,
// delivery is stuck fail-closed, never silently duplicated.
func TestStreamDriverLostResponseWithLaggingObservationFailsClosed(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	proto.appendCommitsThenThrottle = true // durable commit, lost response
	proto.commitVisibilityDelay = 2        // observation lags the durable commit
	driver := newStreamTestDriver(cfg, proto)

	_, err := driver.apply(context.Background(), intent, transaction)
	if !errors.Is(err, errStreamObservationInconsistent) {
		t.Fatalf("lagging observation after a lost response must fail closed, got %v", err)
	}
	if proto.insertCalls != 0 {
		t.Fatalf("fail-closed outcome wrote a receipt: insertCalls=%d", proto.insertCalls)
	}
}

func TestStreamDriverObservationInconsistentFailsClosed(t *testing.T) {
	cfg, transaction, intent, plan := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	proto.seedCommittedRow(plan.rowHashes[0], 2) // duplicate identity hazard
	driver := newStreamTestDriver(cfg, proto)

	_, err := driver.apply(context.Background(), intent, transaction)
	if !errors.Is(err, errStreamObservationInconsistent) {
		t.Fatalf("duplicate identity must fail closed, got %v", err)
	}
}

func TestStreamDriverReceiptConflictFailsClosed(t *testing.T) {
	cfg, transaction, intent, plan := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	conflict := plan.receipt
	conflict.rowsContentHash = "deadbeef"
	conflict.manifestHash = plan.receipt.manifestHash // same identity key, different content
	proto.seedReceipt(conflict)
	driver := newStreamTestDriver(cfg, proto)

	_, err := driver.apply(context.Background(), intent, transaction)
	if !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("receipt conflict must fail closed, got %v", err)
	}
}

func TestStreamDriverConcurrentOwnerAdoptsExistingReceipt(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	proto.insertCommitsThenDuplicate = true
	driver := newStreamTestDriver(cfg, proto)

	evidence, err := driver.apply(context.Background(), intent, transaction)
	if err != nil {
		t.Fatalf("apply with concurrent owner: %v", err)
	}
	if evidence.ExternalID == "" {
		t.Fatal("concurrent owner adoption returned no evidence")
	}
}

func TestStreamDriverIncompleteObservationIsIndeterminate(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	cfg.observeAttempts = 2
	proto := newFakeStreamProtocol()
	proto.commitVisibilityDelay = 1000 // rows never become visible within the bound
	driver := newStreamTestDriver(cfg, proto)

	_, err := driver.apply(context.Background(), intent, transaction)
	if !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("unobserved rows must be indeterminate, got %v", err)
	}
	if proto.insertCalls != 0 {
		t.Fatalf("indeterminate outcome wrote a receipt: insertCalls=%d", proto.insertCalls)
	}
}

func TestStreamDriverMissingCommittedTokenIsIndeterminate(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	proto.statusSuppressCommittedToken = true
	driver := newStreamTestDriver(cfg, proto)
	// The offset token never lands on the channel status either.
	driver.cfg.appendAttempts = 1

	_, err := driver.apply(context.Background(), intent, transaction)
	if err == nil {
		t.Fatal("missing committed token must not acknowledge")
	}
	if !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("missing committed token must be indeterminate, got %v", err)
	}
}

func TestStreamDriverReconcileMatchesReceipt(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	driver := newStreamTestDriver(cfg, proto)

	disposition, _, err := driver.reconcile(context.Background(), intent)
	if err != nil {
		t.Fatalf("reconcile before apply: %v", err)
	}
	if disposition != connector.DeliveryNotApplied {
		t.Fatalf("reconcile before apply disposition=%v want NotApplied", disposition)
	}
	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("apply: %v", err)
	}
	disposition, evidence, err := driver.reconcile(context.Background(), intent)
	if err != nil {
		t.Fatalf("reconcile after apply: %v", err)
	}
	if disposition != connector.DeliveryApplied || evidence.ExternalID == "" {
		t.Fatalf("reconcile after apply disposition=%v evidence=%+v", disposition, evidence)
	}
}

func TestStreamDriverCleanupReleasesAndRemovesChannelState(t *testing.T) {
	cfg, transaction, intent, plan := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	driver := newStreamTestDriver(cfg, proto)

	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("apply: %v", err)
	}
	released, err := driver.cleanup(context.Background(), intent.FlowIncarnationID)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if released != 1 {
		t.Fatalf("cleanup released=%d want 1", released)
	}
	if _, ok, _ := proto.LookupChannelState(context.Background(), cfg, streamChannelStateKey{flowIncarnationID: plan.receipt.flowIncarnationID, destinationRevisionID: plan.receipt.destinationRevisionID, channelName: plan.identity.channelName}); ok {
		t.Fatal("cleanup did not remove channel state")
	}
	// A release receipt must exist and be idempotent on a second pass.
	released, err = driver.cleanup(context.Background(), intent.FlowIncarnationID)
	if err != nil {
		t.Fatalf("second cleanup: %v", err)
	}
	if released != 0 {
		t.Fatalf("second cleanup released=%d want 0", released)
	}
}
