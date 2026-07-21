package connector

import "testing"

func TestPostgresManagedProfilePromotionContract(t *testing.T) {
	t.Parallel()
	profile := PostgresToPostgresV1Profile()
	if err := profile.ValidatePromotion(); err != nil {
		t.Fatal(err)
	}
	if profile.Name != ManagedProfilePostgresToPostgresV1 || profile.Support != SupportMaintained {
		t.Fatalf("profile=%+v", profile)
	}
	for _, version := range []int{14, 15, 16, 17} {
		if !profile.SupportsPostgresVersion(version) {
			t.Fatalf("PostgreSQL %d is absent from the named profile", version)
		}
	}
	if profile.SupportsPostgresVersion(13) || profile.SupportsPostgresVersion(18) {
		t.Fatal("unnamed PostgreSQL versions must remain outside the promoted profile")
	}
	if profile.DeliveryGuarantee != "at-least-once" || !profile.SingleSink {
		t.Fatalf("profile guarantee=%q single_sink=%t", profile.DeliveryGuarantee, profile.SingleSink)
	}
}

func TestMaintainedManagedProfileRequiresEveryLiveGate(t *testing.T) {
	t.Parallel()
	profile := PostgresToPostgresV1Profile()
	profile.Gates[0].Live = false
	if err := profile.ValidatePromotion(); err == nil {
		t.Fatal("maintained profile accepted a disabled real-service gate")
	}
}
