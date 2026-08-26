package snowflake

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

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

func TestStreamDriverCompleteUnjournaledTargetFailsClosed(t *testing.T) {
	cfg, transaction, intent, plan := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	// Every row is already SQL-observed present, but no receipt exists yet.
	for _, hash := range plan.rowHashes {
		proto.seedCommittedRow(hash, 1)
	}
	driver := newStreamTestDriver(cfg, proto)

	if _, err := driver.apply(context.Background(), intent, transaction); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("unjournaled target rows error=%v, want indeterminate", err)
	}
	if proto.appendCalls != 0 || proto.insertCalls != 0 {
		t.Fatalf("unjournaled target rows caused append/receipt=%d/%d", proto.appendCalls, proto.insertCalls)
	}
}

func TestStreamDriverPartialUnjournaledTargetFailsClosed(t *testing.T) {
	cfg, transaction, intent, plan := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	// The first row already committed on a prior attempt.
	proto.seedCommittedRow(plan.rowHashes[0], 1)
	driver := newStreamTestDriver(cfg, proto)

	if _, err := driver.apply(context.Background(), intent, transaction); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("partial unjournaled target error=%v, want indeterminate", err)
	}
	if proto.appendCalls != 0 || proto.insertCalls != 0 {
		t.Fatalf("partial unjournaled target caused append/receipt=%d/%d", proto.appendCalls, proto.insertCalls)
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
	backoffs := 0
	driver.sleep = func(context.Context, time.Duration) error { backoffs++; return nil }

	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("apply under throttling: %v", err)
	}
	if proto.insertCalls != 1 || backoffs != 3 {
		t.Fatalf("throttling failed to converge: insertCalls/backoffs=%d/%d, want 1/3", proto.insertCalls, backoffs)
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

// A lost response is reconciled from the exact durable request before target
// visibility catches up. The driver polls the committed request and never sends
// a duplicate append.
func TestStreamDriverAcceptedThenEOFConvergesWithoutDuplicate(t *testing.T) {
	cfg, transaction, intent, plan := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	proto.appendCommitsThenEOF = true
	if _, err := newStreamTestDriver(cfg, proto).apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("accepted then EOF recovery: %v", err)
	}
	if proto.appendCalls != 1 || proto.insertCalls != 1 {
		t.Fatalf("accepted then EOF append/receipt=%d/%d, want 1/1", proto.appendCalls, proto.insertCalls)
	}
	for _, hash := range plan.rowHashes {
		if proto.committed[hash].count != 1 {
			t.Fatalf("accepted then EOF duplicated row %s", hash)
		}
	}
}

func TestStreamDriverLostResponseWithLaggingObservationConvergesOnce(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	proto.appendCommitsThenThrottle = true // durable commit, lost response
	proto.commitVisibilityDelay = 2        // observation lags the durable commit
	driver := newStreamTestDriver(cfg, proto)

	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("lagging observation recovery: %v", err)
	}
	if proto.appendCalls != 1 || proto.insertCalls != 1 {
		t.Fatalf("lagging observation append/receipt=%d/%d, want 1/1", proto.appendCalls, proto.insertCalls)
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

func TestStreamDriverUnexpectedTargetRequestRowFailsClosed(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	proto.seedCommittedRow(strings.Repeat("f", 64), 1)
	driver := newStreamTestDriver(cfg, proto)
	_, err := driver.apply(context.Background(), intent, transaction)
	if !errors.Is(err, errStreamObservationInconsistent) {
		t.Fatalf("unexpected target row error=%v, want inconsistent observation", err)
	}
	if proto.appendCalls != 0 || proto.insertCalls != 0 {
		t.Fatalf("unexpected target row append/receipt=%d/%d", proto.appendCalls, proto.insertCalls)
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

func TestStreamDriverContradictoryAbsenceFailsConflict(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	proto.appendUnknownOnce = io.EOF
	proto.requestStatusContradictoryAbsent = true
	_, err := newStreamTestDriver(cfg, proto).apply(context.Background(), intent, transaction)
	if !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("contradictory absence error=%v, want conflict", err)
	}
	if proto.appendCalls != 1 || proto.insertCalls != 0 {
		t.Fatalf("contradictory absence append/receipt=%d/%d", proto.appendCalls, proto.insertCalls)
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
	proto.statusSuppressCommittedToken = false
	if _, err := newStreamTestDriver(cfg, proto).apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("restart after missing token later committed: %v", err)
	}
	if proto.appendCalls != 1 || proto.insertCalls != 1 {
		t.Fatalf("missing-token restart append/receipt=%d/%d, want 1/1", proto.appendCalls, proto.insertCalls)
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

func TestStreamRequestIdentityBindsChannelOffsetAndAttempt(t *testing.T) {
	cfg, _, _, plan := streamTestFixture(t)
	status := streamChannelStatus{valid: true, channelName: plan.identity.channelName, channelRevision: 3, pipeRevision: "pipe-3", continuationToken: "continuation-9"}
	first, err := newManagedStreamRequest(plan, status, 1)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := newManagedStreamRequest(plan, status, 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newManagedStreamRequest(plan, status, 2)
	if err != nil {
		t.Fatal(err)
	}
	if first.requestID != replay.requestID || first.requestID == second.requestID || first.requestedOffset != plan.identity.offsetToken || first.channelRevision != 3 || first.inputContinuation != "continuation-9" || first.destinationRevisionID != cfg.destinationRevision {
		t.Fatalf("request identities first/replay/second=%+v/%+v/%+v", first, replay, second)
	}
}

func TestStreamRequestAmbiguousTransportNeverResendsWithoutProof(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	proto.appendUnknownOnce = errors.New("connection reset after send")
	driver := newStreamTestDriver(cfg, proto)
	_, err := driver.apply(context.Background(), intent, transaction)
	if !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("ambiguous append error=%v, want indeterminate", err)
	}
	request, found, lookupErr := proto.LookupRequest(context.Background(), cfg, streamRequestKey{flowIncarnationID: intent.FlowIncarnationID, destinationRevisionID: intent.DestinationRevisionID, logicalBatchID: intent.LogicalBatchID})
	if lookupErr != nil || !found || request.phase != streamRequestSendingUnknown || request.responseKind != "transport_error" {
		t.Fatalf("ambiguous request=%+v found/error=%t/%v", request, found, lookupErr)
	}
	if _, err := driver.apply(context.Background(), intent, transaction); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("second unknown reconciliation error=%v, want indeterminate", err)
	}
	if proto.appendCalls != 1 || proto.insertCalls != 0 {
		t.Fatalf("ambiguous request append/receipt=%d/%d, want 1/0", proto.appendCalls, proto.insertCalls)
	}
}

func TestStreamRequestAmbiguityClassesNeverBlindlyResend(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "timeout", err: context.DeadlineExceeded},
		{name: "eof", err: io.EOF},
		{name: "disconnect", err: errors.New("connection reset")},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, transaction, intent, _ := streamTestFixture(t)
			proto := newFakeStreamProtocol()
			proto.appendUnknownOnce = test.err
			driver := newStreamTestDriver(cfg, proto)
			if _, err := driver.apply(context.Background(), intent, transaction); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
				t.Fatalf("first ambiguity error=%v", err)
			}
			if _, err := driver.apply(context.Background(), intent, transaction); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
				t.Fatalf("second ambiguity error=%v", err)
			}
			if proto.appendCalls != 1 {
				t.Fatalf("ambiguity reappended request: %d", proto.appendCalls)
			}
		})
	}
}

func TestStreamRequestCancellationAfterPreparePersistsUnknown(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	ctx, cancel := context.WithCancel(context.Background())
	driver := newStreamTestDriver(cfg, proto)
	driver.hooks.AfterOpen = func() error { cancel(); return nil }
	_, err := driver.apply(ctx, intent, transaction)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error=%v, want context canceled", err)
	}
	request, found, lookupErr := proto.LookupRequest(context.Background(), cfg, streamRequestKey{flowIncarnationID: intent.FlowIncarnationID, destinationRevisionID: intent.DestinationRevisionID, logicalBatchID: intent.LogicalBatchID})
	if lookupErr != nil || !found || request.phase != streamRequestSendingUnknown || proto.appendCalls != 0 {
		t.Fatalf("canceled request=%+v found/error/appends=%t/%v/%d", request, found, lookupErr, proto.appendCalls)
	}
}

func TestStreamRequestUnknownRestartReconcilesWithoutResend(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	proto.requestStatusUnknownTimes = 1
	first := newStreamTestDriver(cfg, proto)
	_, err := first.apply(context.Background(), intent, transaction)
	if !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("unknown response error=%v, want indeterminate", err)
	}
	if proto.appendCalls != 1 || proto.insertCalls != 0 {
		t.Fatalf("first process append/receipt=%d/%d, want 1/0", proto.appendCalls, proto.insertCalls)
	}
	request, found, lookupErr := proto.LookupRequest(context.Background(), cfg, streamRequestKey{flowIncarnationID: intent.FlowIncarnationID, destinationRevisionID: intent.DestinationRevisionID, logicalBatchID: intent.LogicalBatchID})
	if lookupErr != nil || !found || request.phase != streamRequestAccepted || request.responseKind == "" {
		t.Fatalf("durable unknown request=%+v found/error=%t/%v", request, found, lookupErr)
	}
	second := newStreamTestDriver(cfg, proto)
	if _, err := second.apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("restart reconcile: %v", err)
	}
	if proto.appendCalls != 1 || proto.insertCalls != 1 {
		t.Fatalf("restart append/receipt=%d/%d, want 1/1", proto.appendCalls, proto.insertCalls)
	}
}

func TestStreamRequestDefinitelyNotAcceptedRetriesOnce(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	proto.appendDefinitelyAbsentOnce = true
	if _, err := newStreamTestDriver(cfg, proto).apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("proven-absence retry: %v", err)
	}
	if proto.appendCalls != 2 || proto.insertCalls != 1 || len(proto.requests) != 2 {
		t.Fatalf("proven-absence append/receipt/requests=%d/%d/%d, want 2/1/2", proto.appendCalls, proto.insertCalls, len(proto.requests))
	}
}

func TestStreamRequestDivergenceFailsConflictWithoutResend(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	proto.requestStatusDivergent = true
	driver := newStreamTestDriver(cfg, proto)
	_, err := driver.apply(context.Background(), intent, transaction)
	if !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("divergent request status error=%v, want conflict", err)
	}
	if proto.appendCalls != 1 || proto.insertCalls != 0 {
		t.Fatalf("divergent request append/receipt=%d/%d, want 1/0", proto.appendCalls, proto.insertCalls)
	}
}

func TestStreamSendBoundaryAbsentLookupRaceAdmitsExactlyOneAppend(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	proto.openKeepsRevision = true
	loserLookedAbsent := make(chan struct{})
	loserCandidateReady := make(chan struct{})
	resumeLoser := make(chan struct{})
	winnerClaimed := make(chan struct{})
	releaseWinner := make(chan struct{})
	var loserLookupOnce sync.Once
	var loserInsertOnce sync.Once
	loser := newStreamDriver(proto, cfg, "catalog-fingerprint", streamingHooks{
		AfterRequestLookup: func(found bool) error {
			if !found {
				loserLookupOnce.Do(func() { close(loserLookedAbsent) })
			}
			return nil
		},
		BeforeRequestInsert: func(managedStreamRequest) error {
			loserInsertOnce.Do(func() {
				close(loserCandidateReady)
				<-resumeLoser
			})
			return nil
		},
	})
	loser.sleep = func(context.Context, time.Duration) error { return nil }
	loserErr := make(chan error, 1)
	go func() {
		_, err := loser.apply(context.Background(), intent, transaction)
		loserErr <- err
	}()
	<-loserLookedAbsent
	<-loserCandidateReady

	winner := newStreamDriver(proto, cfg, "catalog-fingerprint", streamingHooks{AfterSendClaim: func() error {
		close(winnerClaimed)
		<-releaseWinner
		return nil
	}})
	winner.sleep = func(context.Context, time.Duration) error { return nil }
	winnerErr := make(chan error, 1)
	go func() {
		_, err := winner.apply(context.Background(), intent, transaction)
		winnerErr <- err
	}()
	<-winnerClaimed
	close(resumeLoser)
	if err := <-loserErr; !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("duplicate-insert loser error=%v, want reconciliation-only indeterminate", err)
	}
	proto.mu.Lock()
	beforeRelease := proto.appendCalls
	proto.mu.Unlock()
	if beforeRelease != 0 {
		t.Fatalf("duplicate-insert loser appended %d times before owner release", beforeRelease)
	}
	close(releaseWinner)
	if err := <-winnerErr; err != nil {
		t.Fatalf("duplicate-insert winner: %v", err)
	}
	if _, err := loser.apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("duplicate-insert loser adoption: %v", err)
	}
	proto.mu.Lock()
	defer proto.mu.Unlock()
	if proto.appendCalls != 1 || proto.insertCalls != 1 {
		t.Fatalf("duplicate-insert append/receipt calls=%d/%d, want 1/1", proto.appendCalls, proto.insertCalls)
	}
}

func TestStreamSendBoundaryCASAdmitsExactlyOneAppend(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	claimed := make(chan struct{})
	release := make(chan struct{})
	winner := newStreamDriver(proto, cfg, "catalog-fingerprint", streamingHooks{AfterSendClaim: func() error {
		close(claimed)
		<-release
		return nil
	}})
	winner.sleep = func(context.Context, time.Duration) error { return nil }
	winnerErr := make(chan error, 1)
	go func() {
		_, err := winner.apply(context.Background(), intent, transaction)
		winnerErr <- err
	}()
	<-claimed
	loser := newStreamTestDriver(cfg, proto)
	if _, err := loser.apply(context.Background(), intent, transaction); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("send-CAS loser error=%v, want reconciliation-only indeterminate", err)
	}
	proto.mu.Lock()
	appendCallsBeforeRelease := proto.appendCalls
	proto.mu.Unlock()
	if appendCallsBeforeRelease != 0 {
		t.Fatalf("send-CAS loser appended %d times before owner release", appendCallsBeforeRelease)
	}
	close(release)
	if err := <-winnerErr; err != nil {
		t.Fatalf("send-CAS winner: %v", err)
	}
	if _, err := loser.apply(context.Background(), intent, transaction); err != nil {
		t.Fatalf("send-CAS loser receipt adoption: %v", err)
	}
	proto.mu.Lock()
	defer proto.mu.Unlock()
	if proto.appendCalls != 1 || proto.insertCalls != 1 {
		t.Fatalf("send-CAS append/receipt calls=%d/%d, want 1/1", proto.appendCalls, proto.insertCalls)
	}
}

func TestStreamChannelStateCASRejectsStaleAndRegressedWriters(t *testing.T) {
	cfg, _, _, plan := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	key := streamChannelStateKey{flowIncarnationID: plan.receipt.flowIncarnationID, destinationRevisionID: plan.receipt.destinationRevisionID, channelName: plan.identity.channelName}
	state := managedStreamChannelState{flowIncarnationID: key.flowIncarnationID, destinationRevisionID: key.destinationRevisionID, channelName: key.channelName, pipeName: cfg.pipe, pipeRevision: "pipe-1", channelRevision: 2, continuationToken: "cont-2", stateVersion: 1}
	if _, applied, err := proto.CompareAndSwapChannelState(context.Background(), cfg, managedStreamChannelState{}, state); err != nil || !applied {
		t.Fatalf("initial CAS applied/error=%t/%v", applied, err)
	}
	stale := state
	stale.stateVersion = 2
	if _, applied, err := proto.CompareAndSwapChannelState(context.Background(), cfg, managedStreamChannelState{}, stale); err != nil || applied {
		t.Fatalf("stale CAS applied/error=%t/%v", applied, err)
	}
	regressed := state
	regressed.stateVersion = 2
	regressed.channelRevision = 1
	if _, applied, err := proto.CompareAndSwapChannelState(context.Background(), cfg, state, regressed); err != nil || applied {
		t.Fatalf("regressed CAS applied/error=%t/%v", applied, err)
	}
	divergentExpected := state
	divergentExpected.continuationToken = "stale-continuation"
	candidate := state
	candidate.stateVersion = 2
	candidate.continuationToken = "cont-3"
	if _, applied, err := proto.CompareAndSwapChannelState(context.Background(), cfg, divergentExpected, candidate); err != nil || applied {
		t.Fatalf("divergent prior-token CAS applied/error=%t/%v", applied, err)
	}
}

func TestStreamChannelStateCASConcurrentWritersAdmitOne(t *testing.T) {
	cfg, _, _, plan := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	base := managedStreamChannelState{flowIncarnationID: plan.receipt.flowIncarnationID, destinationRevisionID: plan.receipt.destinationRevisionID, channelName: plan.identity.channelName, pipeName: cfg.pipe, pipeRevision: "pipe-1", channelRevision: 1, continuationToken: "cont-1", stateVersion: 1}
	if _, applied, err := proto.CompareAndSwapChannelState(context.Background(), cfg, managedStreamChannelState{}, base); err != nil || !applied {
		t.Fatalf("initial CAS applied/error=%t/%v", applied, err)
	}
	var wg sync.WaitGroup
	results := make(chan bool, 2)
	for index := 0; index < 2; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			candidate := base
			candidate.stateVersion = 2
			candidate.channelRevision = 2
			candidate.continuationToken = "cont-2-" + string(rune('a'+index))
			_, applied, _ := proto.CompareAndSwapChannelState(context.Background(), cfg, base, candidate)
			results <- applied
		}(index)
	}
	wg.Wait()
	close(results)
	applied := 0
	for result := range results {
		if result {
			applied++
		}
	}
	if applied != 1 {
		t.Fatalf("concurrent CAS winners=%d, want 1", applied)
	}
}

func TestStreamDriverCleanupRefusesUnresolvedRequest(t *testing.T) {
	cfg, transaction, intent, _ := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	driver := newStreamTestDriver(cfg, proto)
	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatal(err)
	}
	proto.mu.Lock()
	for id, request := range proto.requests {
		request.phase = streamRequestCommitted
		request.phaseVersion++
		proto.requests[id] = request
	}
	proto.mu.Unlock()
	if _, err := driver.cleanup(context.Background(), intent.FlowIncarnationID); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("cleanup unresolved request error=%v, want indeterminate", err)
	}
	if proto.deleteCalls != 0 {
		t.Fatalf("cleanup deleted channel state with unresolved request: %d", proto.deleteCalls)
	}
}

func TestStreamDriverCleanupRetriesReceiptCommittedDeletion(t *testing.T) {
	cfg, transaction, intent, plan := streamTestFixture(t)
	proto := newFakeStreamProtocol()
	driver := newStreamTestDriver(cfg, proto)
	if _, err := driver.apply(context.Background(), intent, transaction); err != nil {
		t.Fatal(err)
	}
	proto.releaseDeleteFailsOnce = true
	if _, err := driver.cleanup(context.Background(), intent.FlowIncarnationID); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("incomplete cleanup error=%v, want indeterminate", err)
	}
	if _, found, _ := proto.LookupChannelState(context.Background(), cfg, streamChannelStateKey{flowIncarnationID: plan.receipt.flowIncarnationID, destinationRevisionID: plan.receipt.destinationRevisionID, channelName: plan.identity.channelName}); !found {
		t.Fatal("incomplete cleanup removed channel state")
	}
	if released, err := driver.cleanup(context.Background(), intent.FlowIncarnationID); err != nil || released != 1 {
		t.Fatalf("cleanup retry released/error=%d/%v, want 1/nil", released, err)
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
