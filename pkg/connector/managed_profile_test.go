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

func TestSnowflakeSQLProfileSpecIdentityIsExact(t *testing.T) {
	t.Parallel()
	valid := Spec{Type: EndpointSnowflake, Options: map[string]string{"managed_profile": ManagedProfilePostgresToSnowflakeSQLV1}}
	if !IsPostgresToSnowflakeSQLV1Spec(valid) {
		t.Fatal("exact Snowflake SQL profile was not recognized")
	}
	valid.Type = EndpointPostgres
	if IsPostgresToSnowflakeSQLV1Spec(valid) {
		t.Fatal("profile name on the wrong endpoint was admitted")
	}
	valid.Type = EndpointSnowflake
	valid.Options["managed_profile"] = "postgresql-to-snowflake-sql-v2"
	if IsPostgresToSnowflakeSQLV1Spec(valid) {
		t.Fatal("unknown Snowflake profile was admitted")
	}
}

func TestSnowflakeSQLManagedProfileRemainsExperimentalWithoutLiveRecoveryEvidence(t *testing.T) {
	t.Parallel()
	profile := PostgresToSnowflakeSQLV1Profile()
	if err := profile.ValidatePromotion(); err != nil {
		t.Fatal(err)
	}
	if profile.Name != ManagedProfilePostgresToSnowflakeSQLV1 || profile.Support != SupportExperimental {
		t.Fatalf("profile=%+v", profile)
	}
	if profile.Destination != EndpointSnowflake || profile.Deployment != "commercial-aws-snowflake-hybrid-table" {
		t.Fatalf("endpoint/deployment=%s/%q", profile.Destination, profile.Deployment)
	}
	if profile.SnowflakeVersionPolicy != "configured-exact-version-unreviewed" {
		t.Fatalf("Snowflake version policy=%q", profile.SnowflakeVersionPolicy)
	}
	if len(profile.SnowflakeVersions) != 0 || len(profile.SnowflakeDeploymentCells) != 0 {
		t.Fatalf("unproven Snowflake profile contains reviewed live cells: versions=%v cells=%v", profile.SnowflakeVersions, profile.SnowflakeDeploymentCells)
	}
	if !profile.SupportsPostgresVersion(16) || profile.SupportsPostgresVersion(14) || profile.SupportsPostgresVersion(17) {
		t.Fatalf("PostgreSQL version admission exceeds the unpromoted live pairing: %v", profile.PostgresVersions)
	}
	if profile.DeliveryGuarantee != "at-least-once" || !profile.SingleSink {
		t.Fatalf("profile guarantee=%q single_sink=%t", profile.DeliveryGuarantee, profile.SingleSink)
	}
	for _, gate := range profile.Gates {
		if gate.Capability != "telemetry" && !gate.Live {
			t.Fatalf("recovery gate is not marked as requiring a real Snowflake service: %+v", gate)
		}
	}
	profile.Support = SupportMaintained
	if err := profile.ValidatePromotion(); err == nil {
		t.Fatal("Snowflake profile was promoted without a reviewed executable promotion gate set")
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
