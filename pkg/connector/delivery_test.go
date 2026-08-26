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

func TestManagedPartReservationRejectsDuplicateIdentityAndQueryID(t *testing.T) {
	base := ManagedPartReservationRequest{
		Resource: ManagedPartResourceClickHouseActivePartsV1, DestinationRevisionID: "revision", SourceLineageID: "lineage",
		LogicalBatchID: "batch", PositionID: "0/10", ContentHash: strings.Repeat("a", 64), Capacity: 10,
	}
	for _, test := range []struct {
		name  string
		parts []ManagedPartIdentity
	}{
		{name: "kind ordinal", parts: []ManagedPartIdentity{{Kind: "changelog", Ordinal: 0, QueryID: "query-a"}, {Kind: "changelog", Ordinal: 0, QueryID: "query-b"}}},
		{name: "query id", parts: []ManagedPartIdentity{{Kind: "changelog", Ordinal: 0, QueryID: "query-a"}, {Kind: "receipt", Ordinal: 0, QueryID: "query-a"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := base
			request.Parts = test.parts
			request.PlanHash, _ = ManagedPartPlanHash(test.parts)
			if err := request.Validate(); err == nil {
				t.Fatal("duplicate managed part plan was accepted")
			}
		})
	}
}

func TestManagedPartReservationPlanHashBindsOrderAndQueryIDs(t *testing.T) {
	parts := []ManagedPartIdentity{{Kind: "changelog", Ordinal: 0, QueryID: "query-a"}, {Kind: "receipt", Ordinal: 0, QueryID: "query-b"}}
	first, err := ManagedPartPlanHash(parts)
	if err != nil {
		t.Fatal(err)
	}
	parts[0].QueryID = "query-c"
	second, err := ManagedPartPlanHash(parts)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("query ID change did not change immutable managed part plan hash")
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
