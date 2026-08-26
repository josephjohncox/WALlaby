package snowflake

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestClassifyStagedTargetStates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		expected          []string
		manifest, matches bool
		actual            map[string]int
		want              stagedTargetState
	}{
		{name: "absent", expected: []string{"a"}, actual: map[string]int{}, want: stagedTargetAbsent},
		{name: "partial", expected: []string{"a", "b"}, actual: map[string]int{"a": 1}, want: stagedTargetPartial},
		{name: "complete", expected: []string{"a", "b"}, manifest: true, matches: true, actual: map[string]int{"a": 1, "b": 1}, want: stagedTargetComplete},
		{name: "duplicate", expected: []string{"a"}, manifest: true, matches: true, actual: map[string]int{"a": 2}, want: stagedTargetDuplicate},
		{name: "conflicting manifest", expected: []string{"a"}, manifest: true, actual: map[string]int{"a": 1}, want: stagedTargetConflict},
		{name: "unreceipted rows", expected: []string{"a"}, matches: true, actual: map[string]int{"a": 1}, want: stagedTargetPartial},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := classifyStagedTarget(test.expected, test.manifest, test.matches, test.actual).state; got != test.want {
				t.Fatalf("state=%d, want %d", got, test.want)
			}
		})
	}
}

func TestStagedAuthorityFencesABAAndStaleOwners(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	plan := stagedPlanFor(t, cfg, intent, transaction)
	plan.catalogFingerprint = "catalog-fingerprint"
	proto := newFakeStageProtocol()
	request := stagedLeaseRequestForPlan(intent, plan.catalogFingerprint)
	lease, err := proto.AcquireRuntimeLease(context.Background(), cfg, request)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := proto.AcquireLoadClaim(context.Background(), cfg, lease, stagedLoadClaimForPlan(lease, plan))
	if err != nil {
		t.Fatal(err)
	}
	proto.mu.Lock()
	proto.provisionEpoch++
	proto.mu.Unlock()
	if err := proto.RevalidateRuntimeLease(context.Background(), cfg, lease); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("stale lease error=%v, want indeterminate", err)
	}
	if err := proto.PromoteTarget(context.Background(), cfg, lease, claim, plan.rowHashes); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("stale owner promotion error=%v, want indeterminate", err)
	}
}

func TestStagedAuthorityPromotionAndReceiptProofIgnoreCopyHistory(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	proto := newFakeStageProtocol()
	proto.autoIngestDelayCalls = 1000
	driver := newStagedTestDriver(cfg, proto)
	evidence, err := driver.apply(context.Background(), intent, transaction)
	assertStagedApplied(t, proto, intent, evidence, err)
	if proto.historyCalls != 0 {
		t.Fatalf("COPY_HISTORY calls=%d, want zero", proto.historyCalls)
	}
	plan := stagedPlanFor(t, cfg, intent, transaction)
	plan.catalogFingerprint = "catalog-fingerprint"
	claim := stagedLoadClaimForPlan(stagedRuntimeLease{stagedLeaseRequest: stagedLeaseRequestForPlan(intent, plan.catalogFingerprint), provisionEpoch: 1}, plan)
	observation, err := proto.ObserveTarget(context.Background(), cfg, claim, plan.rowHashes)
	if err != nil || observation.state != stagedTargetComplete {
		t.Fatalf("target proof=%+v err=%v", observation, err)
	}
	for _, receipt := range proto.receipts {
		if receipt.kind == stagedReceiptKindLoad {
			if err := proto.ValidateReceiptTargetProof(context.Background(), cfg, receipt); err != nil {
				t.Fatalf("receipt proof: %v", err)
			}
		}
	}
}

func TestStagedAuthorityZeroRowManifestIsDurable(t *testing.T) {
	t.Parallel()
	cfg := stagedTestConfig(t)
	transaction := connector.SourceTransaction{SourceLineageID: "lineage-1", TransactionID: 9, BeginLSN: "0/10", CommitLSN: "0/20", EndLSN: "0/28", Checkpoint: connector.Checkpoint{LSN: "0/28"}}
	intent := stagedTestIntent(t, cfg, transaction)
	proto := newFakeStageProtocol()
	if _, err := newStagedTestDriver(cfg, proto).apply(context.Background(), intent, transaction); err != nil {
		t.Fatal(err)
	}
	if len(proto.manifests) != 1 || len(proto.target) != 1 || proto.copyCalls != 0 {
		t.Fatalf("zero-row manifests/targets/copy=%d/%d/%d, want 1/1/0", len(proto.manifests), len(proto.target), proto.copyCalls)
	}
}

func TestStagedAuthorityConcurrentSameIdentityConverges(t *testing.T) {
	t.Parallel()
	cfg, intent, transaction := stagedFixture(t)
	proto := newFakeStageProtocol()
	var wait sync.WaitGroup
	errorsSeen := make(chan error, 2)
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := newStagedTestDriver(cfg, proto).apply(context.Background(), intent, transaction)
			errorsSeen <- err
		}()
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil && !errors.Is(err, connector.ErrDeliveryIndeterminate) {
			t.Fatalf("concurrent apply error=%v", err)
		}
	}
	if len(proto.manifests) != 1 {
		t.Fatalf("target manifests=%d, want one", len(proto.manifests))
	}
}
