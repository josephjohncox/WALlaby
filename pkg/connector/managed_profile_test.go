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

func TestSnowflakeStagedAppendManagedProfileRemainsExperimentalWithoutLiveRecoveryEvidence(t *testing.T) {
	t.Parallel()
	profile := PostgresToSnowflakeStagedAppendV1Profile()
	if err := profile.ValidatePromotion(); err != nil {
		t.Fatal(err)
	}
	if profile.Name != ManagedProfilePostgresToSnowflakeStagedAppendV1 || profile.Support != SupportExperimental {
		t.Fatalf("profile=%+v", profile)
	}
	if profile.Destination != EndpointSnowflake || profile.Deployment != "commercial-aws-snowflake-internal-stage-copy" {
		t.Fatalf("endpoint/deployment=%s/%q", profile.Destination, profile.Deployment)
	}
	if profile.SnowflakeVersionPolicy != "configured-exact-version-unreviewed" {
		t.Fatalf("Snowflake version policy=%q", profile.SnowflakeVersionPolicy)
	}
	if len(profile.SnowflakeVersions) != 0 || len(profile.SnowflakeDeploymentCells) != 0 {
		t.Fatalf("unproven staged Snowflake profile contains reviewed live cells: versions=%v cells=%v", profile.SnowflakeVersions, profile.SnowflakeDeploymentCells)
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
		t.Fatal("staged Snowflake profile was promoted without a reviewed executable promotion gate set")
	}
}

func TestSnowflakeStreamingAppendManagedProfileRemainsExperimentalWithoutLiveRecoveryEvidence(t *testing.T) {
	t.Parallel()
	profile := PostgresToSnowflakeStreamingRestAppendV1Profile()
	if err := profile.ValidatePromotion(); err != nil {
		t.Fatal(err)
	}
	if profile.Name != ManagedProfilePostgresToSnowflakeStreamingRestAppendV1 || profile.Support != SupportExperimental {
		t.Fatalf("profile=%+v", profile)
	}
	if profile.Destination != EndpointSnowflake || profile.Deployment != "commercial-aws-snowpipe-streaming-highperf-rest" {
		t.Fatalf("endpoint/deployment=%s/%q", profile.Destination, profile.Deployment)
	}
	if profile.SnowflakeVersionPolicy != "configured-exact-version-unreviewed" {
		t.Fatalf("Snowflake version policy=%q", profile.SnowflakeVersionPolicy)
	}
	if len(profile.SnowflakeVersions) != 0 || len(profile.SnowflakeDeploymentCells) != 0 {
		t.Fatalf("unproven streaming Snowflake profile contains reviewed live cells: versions=%v cells=%v", profile.SnowflakeVersions, profile.SnowflakeDeploymentCells)
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
		t.Fatal("streaming Snowflake profile was promoted without a reviewed executable promotion gate set")
	}
}

func TestIsManagedSnowflakeProfile(t *testing.T) {
	t.Parallel()
	if !IsManagedSnowflakeProfile(ManagedProfilePostgresToSnowflakeSQLV1) || !IsManagedSnowflakeProfile(ManagedProfilePostgresToSnowflakeStagedAppendV1) ||
		!IsManagedSnowflakeProfile(ManagedProfilePostgresToSnowflakeStreamingRestAppendV1) {
		t.Fatal("every constrained Snowflake profile must be recognized as a Snowflake managed profile")
	}
	for _, name := range []string{"", ManagedProfilePostgresToPostgresV1, ManagedProfilePostgresToClickHouseAppendV1, "postgresql-to-snowflake-staged-append-v2"} {
		if IsManagedSnowflakeProfile(name) {
			t.Fatalf("non-Snowflake profile %q was misclassified as a Snowflake managed profile", name)
		}
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
