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

func TestClickHouseManagedAppendProfilePromotionContract(t *testing.T) {
	t.Parallel()
	profile := PostgresToClickHouseAppendV1Profile()
	if err := profile.ValidatePromotion(); err != nil {
		t.Fatal(err)
	}
	if profile.Name != ManagedProfilePostgresToClickHouseAppendV1 || profile.Support != SupportMaintained {
		t.Fatalf("profile=%+v", profile)
	}
	if profile.Destination != EndpointClickHouse || profile.Deployment != "self-managed-keeper" {
		t.Fatalf("endpoint/deployment=%s/%q", profile.Destination, profile.Deployment)
	}
	if !profile.SupportsClickHouseVersion("25.12.1.649") || profile.SupportsClickHouseVersion("25.12.10.7") || profile.SupportsClickHouseVersion("25.11.9") || profile.SupportsClickHouseVersion("26.1.1") {
		t.Fatalf("ClickHouse version admission is not the exact tested patch: %v", profile.ClickHouseVersions)
	}
	if !profile.SupportsPostgresVersion(16) || profile.SupportsPostgresVersion(14) || profile.SupportsPostgresVersion(15) || profile.SupportsPostgresVersion(17) {
		t.Fatalf("PostgreSQL version admission exceeds the real-service pairing: %v", profile.PostgresVersions)
	}
	if profile.DeliveryGuarantee != "at-least-once" || !profile.SingleSink {
		t.Fatalf("profile guarantee=%q single_sink=%t", profile.DeliveryGuarantee, profile.SingleSink)
	}
}

func TestMaintainedManagedProfileRequiresEveryLiveGate(t *testing.T) {
	t.Parallel()
	for _, profile := range []ManagedProfileContract{PostgresToPostgresV1Profile(), PostgresToClickHouseAppendV1Profile()} {
		profile := profile
		t.Run(profile.Name, func(t *testing.T) {
			t.Parallel()
			profile.Gates[0].Live = false
			if err := profile.ValidatePromotion(); err == nil {
				t.Fatal("maintained profile accepted a disabled real-service gate")
			}
		})
	}
}
