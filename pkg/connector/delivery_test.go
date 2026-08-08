package connector

import (
	"strings"
	"testing"
)

func TestDeliveryIntentRequiresCurrentLogicalBatchIdentity(t *testing.T) {
	contentHash := strings.Repeat("b", 64)
	logicalBatchID, err := DeliveryLogicalBatchID("lineage", "0/10", contentHash)
	if err != nil {
		t.Fatal(err)
	}
	valid := DeliveryIntent{FlowIncarnationID: "incarnation", SourceLineageID: "lineage", Generation: 1, AcquisitionID: "acquisition", LeaseEpoch: 1, DestinationRevisionID: "revision", LogicalBatchID: logicalBatchID, PositionID: "0/10", ContentHash: contentHash}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, logical := range []string{"", "   ", "legacy:0/10", " " + strings.Repeat("a", 64)} {
		intent := valid
		intent.LogicalBatchID = logical
		if err := intent.Validate(); err == nil {
			t.Fatalf("logical_batch_id %q was accepted", logical)
		}
	}
}

func TestBindProjectionFingerprintChangesRecoveryIdentity(t *testing.T) {
	first, err := BindProjectionFingerprint("deployment", "mapping-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := BindProjectionFingerprint("deployment", "mapping-b")
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("mapping change did not change effective recovery identity")
	}
	if _, err := BindProjectionFingerprint("deployment", ""); err == nil {
		t.Fatal("missing projection fingerprint was admitted")
	}
}
