package connector

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const (
	// ManagedProfilePostgresToPostgresV1 promotes only the named PostgreSQL
	// source/target contract. Generic PostgreSQL connector modes remain experimental.
	ManagedProfilePostgresToPostgresV1 = "postgresql-to-postgresql-v1"
	// ManagedProfilePostgresToClickHouseAppendV1 promotes only the Keeper-backed,
	// append-only changelog contract. Generic ClickHouse mutation modes remain experimental.
	ManagedProfilePostgresToClickHouseAppendV1 = "postgresql-to-clickhouse-append-v1"
	// ManagedProfilePostgresToSnowflakeSQLV1 names the constrained transactional
	// Snowflake SQL contract. It remains experimental until its opt-in real-service
	// recovery matrix has passed for a reviewed Snowflake service version.
	ManagedProfilePostgresToSnowflakeSQLV1 = "postgresql-to-snowflake-sql-v1"
	// ManagedProfilePostgresToSnowflakeStagedAppendV1 names the constrained staged
	// COPY append-only Snowflake contract: each logical batch is serialized into a
	// deterministic immutable stage object, loaded with fail-closed COPY options,
	// and acknowledged only after Snowflake load history plus a durable destination
	// receipt prove full, non-partial completion. It remains experimental until its
	// opt-in real-service recovery matrix passes on one reviewed Snowflake SHA.
	ManagedProfilePostgresToSnowflakeStagedAppendV1 = "postgresql-to-snowflake-staged-append-v1"
	// ManagedProfilePostgresToSnowflakeStreamingRestAppendV1 names the constrained
	// Snowpipe Streaming high-performance REST append contract: each committed
	// source transaction becomes an ordered set of deterministic-identity rows,
	// appended to a durable channel, and acknowledged only after the destination's
	// SQL-observed row completeness plus a durable destination receipt prove full
	// arrival. Continuation, request, and committed-offset tokens are persisted as
	// evidence but are never sufficient deduplication proof on their own. It remains
	// experimental and, absent a reviewed high-performance Go append transport with
	// live commercial evidence, fails closed at admission rather than performing
	// local-token theater.
	ManagedProfilePostgresToSnowflakeStreamingRestAppendV1 = "postgresql-to-snowflake-streaming-rest-append-v1"
)

// IsManagedSnowflakeProfile reports whether name is one of the constrained
// Snowflake managed profiles. All three share the same PostgreSQL-authoritative
// source-cut, version-pin, and single-relation publication admission; they
// differ only in how the destination materializes and reconciles a batch.
func IsManagedSnowflakeProfile(name string) bool {
	switch name {
	case ManagedProfilePostgresToSnowflakeSQLV1, ManagedProfilePostgresToSnowflakeStagedAppendV1,
		ManagedProfilePostgresToSnowflakeStreamingRestAppendV1:
		return true
	default:
		return false
	}
}

// ManagedProfileGate binds one support claim to a required real-service test.
type ManagedProfileGate struct {
	Capability string
	Test       string
	Live       bool
}

// ManagedProfileContract is the executable support and admission declaration
// rendered into the generated connector support matrix.
type ManagedProfileContract struct {
	Name                     string
	Support                  SupportLevel
	Source                   EndpointType
	Destination              EndpointType
	PostgresVersions         []int
	ClickHouseVersions       []string
	SnowflakeVersions        []string
	SnowflakeVersionPolicy   string
	SnowflakeDeploymentCells []string
	Deployment               string
	SameMajorOnly            bool
	AckPolicies              []string
	SingleSink               bool
	DeliveryGuarantee        string
	Gates                    []ManagedProfileGate
}

// PostgresToPostgresV1Profile returns a defensive copy of the promoted profile.
func PostgresToPostgresV1Profile() ManagedProfileContract {
	contract := ManagedProfileContract{
		Name:              ManagedProfilePostgresToPostgresV1,
		Support:           SupportMaintained,
		Source:            EndpointPostgres,
		Destination:       EndpointPostgres,
		PostgresVersions:  []int{14, 15, 16, 17},
		SameMajorOnly:     true,
		AckPolicies:       []string{"all"},
		SingleSink:        true,
		DeliveryGuarantee: "at-least-once",
		Gates: []ManagedProfileGate{
			{Capability: "postgres versions", Test: "TestPostgresManagedProfileVersionContract", Live: true},
			{Capability: "streamed transactions", Test: "TestPostgresManagedStreamedSubtransactionAbort", Live: true},
			{Capability: "target admission", Test: "TestPostgresManagedProfileTargetAdmission", Live: true},
			{Capability: "schema evolution", Test: "TestPostgresManagedProfileSourceSchemaEvolutionAfterRestart", Live: true},
			{Capability: "DDL reconciliation", Test: "TestPostgresManagedProfileDDLCommitReconciliation", Live: true},
			{Capability: "snapshot to CDC", Test: "TestManagedBootstrapWorkerWiringConcurrentBoundary", Live: true},
			{Capability: "process kill", Test: "TestWallabyWorkerProcessKillRecovery", Live: true},
			{Capability: "pool exhaustion", Test: "TestPostgresManagedProfilePoolExhaustion", Live: true},
			{Capability: "restart", Test: "TestPostgresManagedOverlappingTakeoverAdoptsConcurrentCommit", Live: true},
			{Capability: "retry and retention", Test: "TestPostgresManagedDeliveryRetryAndRetention", Live: true},
			{Capability: "metrics", Test: "TestPostgresManagedProfileMetrics", Live: false},
			{Capability: "upgrade migrations", Test: "TestPostgresManagedProfileUpgradeMigrations", Live: true},
		},
	}
	contract.PostgresVersions = append([]int(nil), contract.PostgresVersions...)
	contract.AckPolicies = append([]string(nil), contract.AckPolicies...)
	contract.Gates = append([]ManagedProfileGate(nil), contract.Gates...)
	return contract
}

// PostgresToClickHouseAppendV1Profile returns the exact promoted append-only
// ClickHouse profile. Version support is deliberately limited to the exact
// PostgreSQL and ClickHouse patch pairing exercised by the real-service gate.
func PostgresToClickHouseAppendV1Profile() ManagedProfileContract {
	contract := ManagedProfileContract{
		Name:               ManagedProfilePostgresToClickHouseAppendV1,
		Support:            SupportMaintained,
		Source:             EndpointPostgres,
		Destination:        EndpointClickHouse,
		PostgresVersions:   []int{16},
		ClickHouseVersions: []string{"25.12.1.649"},
		Deployment:         "self-managed-keeper",
		AckPolicies:        []string{"all"},
		SingleSink:         true,
		DeliveryGuarantee:  "at-least-once",
		Gates: []ManagedProfileGate{
			{Capability: "clickhouse versions", Test: "TestClickHouseManagedProfileVersionMatrix", Live: true},
			{Capability: "target admission", Test: "TestClickHouseManagedProfileAdmission", Live: true},
			{Capability: "ambiguous response", Test: "TestClickHouseManagedProfileCommitBeforeReceipt", Live: true},
			{Capability: "deduplication window", Test: "TestClickHouseManagedProfileDedupWindowEviction", Live: true},
			{Capability: "ordered fragments", Test: "TestClickHouseManagedProfileOrderingAndConcurrency", Live: true},
			{Capability: "key changes and tombstones", Test: "TestClickHouseManagedProfileKeyChangesAndTombstones", Live: true},
			{Capability: "schema and types", Test: "TestClickHouseManagedProfileSchemaEvolutionAndTypes", Live: true},
			{Capability: "PostgreSQL recovery", Test: "TestPostgresToClickHouseManagedProfileRecoveryContract", Live: true},
			{Capability: "bounded load", Test: "TestClickHouseManagedProfileBoundedLoad", Live: true},
			{Capability: "process kill", Test: "TestClickHouseManagedProfileProcessKillRecovery", Live: true},
			{Capability: "keeper recovery", Test: "TestClickHouseManagedProfileKeeperFailureRecovery", Live: true},
			{Capability: "backpressure", Test: "TestClickHouseManagedProfileBackpressure", Live: true},
			{Capability: "TLS", Test: "TestClickHouseManagedProfileTLS", Live: true},
			{Capability: "telemetry", Test: "TestClickHouseManagedProfileTelemetry", Live: false},
		},
	}
	contract.PostgresVersions = append([]int(nil), contract.PostgresVersions...)
	contract.ClickHouseVersions = append([]string(nil), contract.ClickHouseVersions...)
	contract.AckPolicies = append([]string(nil), contract.AckPolicies...)
	contract.Gates = append([]ManagedProfileGate(nil), contract.Gates...)
	return contract
}

// PostgresToSnowflakeSQLV1Profile returns the constrained but unpromoted
// transactional Snowflake SQL profile. Admission compares a configured service
// version with CURRENT_VERSION(), but no service version or deployment cell is
// reviewed yet. Promotion requires complete same-SHA real-service evidence.
func PostgresToSnowflakeSQLV1Profile() ManagedProfileContract {
	contract := ManagedProfileContract{
		Name:                   ManagedProfilePostgresToSnowflakeSQLV1,
		Support:                SupportExperimental,
		Source:                 EndpointPostgres,
		Destination:            EndpointSnowflake,
		PostgresVersions:       []int{16},
		SnowflakeVersionPolicy: "configured-exact-version-unreviewed",
		Deployment:             "commercial-aws-snowflake-hybrid-table",
		AckPolicies:            []string{"all"},
		SingleSink:             true,
		DeliveryGuarantee:      "at-least-once",
		Gates: []ManagedProfileGate{
			{Capability: "runtime deployment", Test: "TestSnowflakeManagedProfileReviewedDeploymentCell", Live: true},
			{Capability: "source catalog and clean cut", Test: "TestPostgresToSnowflakeManagedProfileRecoveryContract", Live: true},
			{Capability: "target direct grants objects and constraints", Test: "TestSnowflakeManagedProfileLiveAdmission", Live: true},
			{Capability: "role hierarchy and alternate writers", Test: "TestSnowflakeManagedProfileRoleIsolation", Live: true},
			{Capability: "task visibility and automation isolation", Test: "TestSnowflakeManagedProfileTaskIsolation", Live: true},
			{Capability: "rollback cardinality ordering and types", Test: "TestSnowflakeManagedProfileOrderedFragmentsAndTypes", Live: true},
			{Capability: "confirmed commit reconciliation", Test: "TestSnowflakeManagedProfileAmbiguousCommit", Live: true},
			{Capability: "commit transport loss and detached takeover", Test: "TestSnowflakeManagedProfileCommitTransportLossAndDetachedTakeover", Live: true},
			{Capability: "DDL rejection and replacement", Test: "TestSnowflakeManagedProfileSchemaReconciliation", Live: true},
			{Capability: "adapter process kill", Test: "TestSnowflakeManagedProfileProcessKillRecovery", Live: true},
			{Capability: "full worker SIGKILL", Test: "TestSnowflakeManagedProfileWorkerSIGKILLRecovery", Live: true},
			{Capability: "network fault matrix", Test: "TestSnowflakeManagedProfileNetworkFaultMatrix", Live: true},
			{Capability: "cancellation and pool safety", Test: "TestSnowflakeManagedProfileCancellationAndPoolSafety", Live: true},
			{Capability: "bounded load and backpressure", Test: "TestSnowflakeManagedProfileBoundedLoadAndBackpressure", Live: true},
			{Capability: "PostgreSQL receipt checkpoint and feedback recovery", Test: "TestPostgresToSnowflakeManagedProfileRecoveryContract", Live: true},
			{Capability: "TLS and JWT", Test: "TestSnowflakeManagedProfileLiveAdmission", Live: true},
			{Capability: "secret redaction", Test: "TestSnowflakeManagedProfileSecretRedaction", Live: true},
			{Capability: "cleanup", Test: "TestSnowflakeManagedProfileCleanup", Live: true},
			{Capability: "telemetry", Test: "TestSnowflakeManagedProfileTelemetry", Live: false},
		},
	}
	contract.PostgresVersions = append([]int(nil), contract.PostgresVersions...)
	contract.SnowflakeVersions = append([]string(nil), contract.SnowflakeVersions...)
	contract.SnowflakeDeploymentCells = append([]string(nil), contract.SnowflakeDeploymentCells...)
	contract.AckPolicies = append([]string(nil), contract.AckPolicies...)
	contract.Gates = append([]ManagedProfileGate(nil), contract.Gates...)
	return contract
}

// PostgresToSnowflakeStagedAppendV1Profile returns the constrained but unpromoted
// staged COPY append-only Snowflake profile. Like the SQL profile, admission
// compares a configured service version with CURRENT_VERSION() but reviews no
// service version or deployment cell yet. Promotion requires complete same-SHA
// real-service recovery evidence for the PUT/COPY/load-history/receipt protocol.
func PostgresToSnowflakeStagedAppendV1Profile() ManagedProfileContract {
	contract := ManagedProfileContract{
		Name:                   ManagedProfilePostgresToSnowflakeStagedAppendV1,
		Support:                SupportExperimental,
		Source:                 EndpointPostgres,
		Destination:            EndpointSnowflake,
		PostgresVersions:       []int{16},
		SnowflakeVersionPolicy: "configured-exact-version-unreviewed",
		Deployment:             "commercial-aws-snowflake-internal-stage-copy",
		AckPolicies:            []string{"all"},
		SingleSink:             true,
		DeliveryGuarantee:      "at-least-once",
		Gates: []ManagedProfileGate{
			{Capability: "runtime deployment", Test: "TestSnowflakeStagedManagedProfileReviewedDeploymentCell", Live: true},
			{Capability: "source catalog and clean cut", Test: "TestPostgresToSnowflakeStagedManagedProfileRecoveryContract", Live: true},
			{Capability: "target stage grants objects and file format", Test: "TestSnowflakeStagedManagedProfileLiveAdmission", Live: true},
			{Capability: "role hierarchy and alternate writers", Test: "TestSnowflakeStagedManagedProfileRoleIsolation", Live: true},
			{Capability: "pipe visibility and auto-ingest isolation", Test: "TestSnowflakeStagedManagedProfilePipeIsolation", Live: true},
			{Capability: "deterministic stage identity and wrong-byte collision", Test: "TestSnowflakeStagedManagedProfileStageIdentityCollision", Live: true},
			{Capability: "PUT uncertainty reconciliation", Test: "TestSnowflakeStagedManagedProfilePutUncertainty", Live: true},
			{Capability: "fail-closed COPY and partial-load rejection", Test: "TestSnowflakeStagedManagedProfileFailClosedCopy", Live: true},
			{Capability: "load history verification and receipt adoption", Test: "TestSnowflakeStagedManagedProfileLoadHistoryAdoption", Live: true},
			{Capability: "auto-ingest verified completion", Test: "TestSnowflakeStagedManagedProfileAutoIngestCompletion", Live: true},
			{Capability: "copy transport loss and detached takeover", Test: "TestSnowflakeStagedManagedProfileCopyTransportLossAndDetachedTakeover", Live: true},
			{Capability: "DDL rejection and replacement", Test: "TestSnowflakeStagedManagedProfileSchemaReconciliation", Live: true},
			{Capability: "adapter process kill", Test: "TestSnowflakeStagedManagedProfileProcessKillRecovery", Live: true},
			{Capability: "full worker SIGKILL", Test: "TestSnowflakeStagedManagedProfileWorkerSIGKILLRecovery", Live: true},
			{Capability: "network fault matrix", Test: "TestSnowflakeStagedManagedProfileNetworkFaultMatrix", Live: true},
			{Capability: "cancellation and pool safety", Test: "TestSnowflakeStagedManagedProfileCancellationAndPoolSafety", Live: true},
			{Capability: "bounded load and backpressure", Test: "TestSnowflakeStagedManagedProfileBoundedLoadAndBackpressure", Live: true},
			{Capability: "cleanup release receipts and retention roots", Test: "TestSnowflakeStagedManagedProfileCleanup", Live: true},
			{Capability: "PostgreSQL receipt checkpoint and feedback recovery", Test: "TestPostgresToSnowflakeStagedManagedProfileRecoveryContract", Live: true},
			{Capability: "TLS and JWT", Test: "TestSnowflakeStagedManagedProfileLiveAdmission", Live: true},
			{Capability: "secret redaction", Test: "TestSnowflakeStagedManagedProfileSecretRedaction", Live: true},
			{Capability: "telemetry", Test: "TestSnowflakeStagedManagedProfileTelemetry", Live: false},
		},
	}
	contract.PostgresVersions = append([]int(nil), contract.PostgresVersions...)
	contract.SnowflakeVersions = append([]string(nil), contract.SnowflakeVersions...)
	contract.SnowflakeDeploymentCells = append([]string(nil), contract.SnowflakeDeploymentCells...)
	contract.AckPolicies = append([]string(nil), contract.AckPolicies...)
	contract.Gates = append([]ManagedProfileGate(nil), contract.Gates...)
	return contract
}

// PostgresToSnowflakeStreamingRestAppendV1Profile returns the constrained but
// unpromoted Snowpipe Streaming high-performance REST append profile. Admission
// compares a configured service version with CURRENT_VERSION() but reviews no
// service version or deployment cell yet. Promotion requires complete same-SHA
// real-service recovery evidence for the channel append / SQL-observed
// completeness / durable-receipt protocol executed through a reviewed
// high-performance append transport. Until such a transport is linked and
// exercised by live recovery evidence, the profile fails closed at admission.
func PostgresToSnowflakeStreamingRestAppendV1Profile() ManagedProfileContract {
	contract := ManagedProfileContract{
		Name:                   ManagedProfilePostgresToSnowflakeStreamingRestAppendV1,
		Support:                SupportExperimental,
		Source:                 EndpointPostgres,
		Destination:            EndpointSnowflake,
		PostgresVersions:       []int{16},
		SnowflakeVersionPolicy: "configured-exact-version-unreviewed",
		Deployment:             "commercial-aws-snowpipe-streaming-highperf-rest",
		AckPolicies:            []string{"all"},
		SingleSink:             true,
		DeliveryGuarantee:      "at-least-once",
		Gates: []ManagedProfileGate{
			{Capability: "reviewed high-performance append transport", Test: "TestSnowflakeStreamingManagedProfileReviewedTransport", Live: true},
			{Capability: "runtime deployment", Test: "TestSnowflakeStreamingManagedProfileReviewedDeploymentCell", Live: true},
			{Capability: "source catalog and clean cut", Test: "TestPostgresToSnowflakeStreamingManagedProfileRecoveryContract", Live: true},
			{Capability: "target channel grants objects and pipe", Test: "TestSnowflakeStreamingManagedProfileLiveAdmission", Live: true},
			{Capability: "role hierarchy and alternate writers", Test: "TestSnowflakeStreamingManagedProfileRoleIsolation", Live: true},
			{Capability: "channel and pipe revision evidence", Test: "TestSnowflakeStreamingManagedProfileChannelRevisionEvidence", Live: true},
			{Capability: "deterministic row identity and SQL-observed completeness", Test: "TestSnowflakeStreamingManagedProfileDeterministicRowObservation", Live: true},
			{Capability: "reopen after uncommitted rows and append proven-missing", Test: "TestSnowflakeStreamingManagedProfileReopenAppendsProvenMissing", Live: true},
			{Capability: "terminal token with rejected rows fails closed", Test: "TestSnowflakeStreamingManagedProfileRejectedRowsFailClosed", Live: true},
			{Capability: "complete-unreceipted recovery and receipt adoption", Test: "TestSnowflakeStreamingManagedProfileCompleteUnreceiptedRecovery", Live: true},
			{Capability: "receipt conflicts and channel invalidation", Test: "TestSnowflakeStreamingManagedProfileReceiptConflictAndChannelInvalidation", Live: true},
			{Capability: "schema evolution and TOAST unchanged fields", Test: "TestSnowflakeStreamingManagedProfileSchemaEvolutionAndToast", Live: true},
			{Capability: "auth expiry refresh", Test: "TestSnowflakeStreamingManagedProfileAuthExpiryRefresh", Live: true},
			{Capability: "throttling and backpressure", Test: "TestSnowflakeStreamingManagedProfileThrottlingBackpressure", Live: true},
			{Capability: "oversize rejection", Test: "TestSnowflakeStreamingManagedProfileOversizeRejection", Live: true},
			{Capability: "adapter process kill", Test: "TestSnowflakeStreamingManagedProfileProcessKillRecovery", Live: true},
			{Capability: "full worker SIGKILL", Test: "TestSnowflakeStreamingManagedProfileWorkerSIGKILLRecovery", Live: true},
			{Capability: "cancellation and pool safety", Test: "TestSnowflakeStreamingManagedProfileCancellationAndPoolSafety", Live: true},
			{Capability: "cleanup release receipts and channel state", Test: "TestSnowflakeStreamingManagedProfileCleanup", Live: true},
			{Capability: "PostgreSQL receipt checkpoint and feedback recovery", Test: "TestPostgresToSnowflakeStreamingManagedProfileRecoveryContract", Live: true},
			{Capability: "TLS and JWT", Test: "TestSnowflakeStreamingManagedProfileLiveAdmission", Live: true},
			{Capability: "secret redaction", Test: "TestSnowflakeStreamingManagedProfileSecretRedaction", Live: true},
			{Capability: "telemetry", Test: "TestSnowflakeStreamingManagedProfileTelemetry", Live: false},
		},
	}
	contract.PostgresVersions = append([]int(nil), contract.PostgresVersions...)
	contract.SnowflakeVersions = append([]string(nil), contract.SnowflakeVersions...)
	contract.SnowflakeDeploymentCells = append([]string(nil), contract.SnowflakeDeploymentCells...)
	contract.AckPolicies = append([]string(nil), contract.AckPolicies...)
	contract.Gates = append([]ManagedProfileGate(nil), contract.Gates...)
	return contract
}

// ValidatePromotion rejects maintained status unless every required admission
// and real-service evidence gate is named and enabled.
func (c ManagedProfileContract) ValidatePromotion() error {
	if c.Name == "" || c.Source == "" || c.Destination == "" {
		return errors.New("managed profile identity and endpoints are required")
	}
	if c.DeliveryGuarantee != "at-least-once" {
		return fmt.Errorf("managed profile %s must not claim %q", c.Name, c.DeliveryGuarantee)
	}
	if !c.SingleSink {
		return fmt.Errorf("managed profile %s currently admits exactly one sink", c.Name)
	}
	if c.Support == SupportMaintained && c.Destination == EndpointPostgres && !c.SameMajorOnly {
		return fmt.Errorf("managed profile %s lacks mixed-major PostgreSQL evidence", c.Name)
	}
	if len(c.PostgresVersions) == 0 || len(c.AckPolicies) != 1 || c.AckPolicies[0] != "all" {
		return fmt.Errorf("managed profile %s has incomplete version or acknowledgement admission", c.Name)
	}
	seenVersions := make(map[int]struct{}, len(c.PostgresVersions))
	for _, version := range c.PostgresVersions {
		if version < 14 {
			return fmt.Errorf("managed profile %s cannot use pgoutput streaming on PostgreSQL %d", c.Name, version)
		}
		if _, duplicate := seenVersions[version]; duplicate {
			return fmt.Errorf("managed profile %s repeats PostgreSQL %d", c.Name, version)
		}
		seenVersions[version] = struct{}{}
	}
	if c.Destination == EndpointSnowflake {
		if c.SnowflakeVersionPolicy != "configured-exact-version-unreviewed" {
			return fmt.Errorf("managed profile %s lacks fail-closed configured Snowflake runtime version admission", c.Name)
		}
		if c.Support == SupportMaintained && (len(c.SnowflakeVersions) == 0 || len(c.SnowflakeDeploymentCells) == 0) {
			return fmt.Errorf("managed profile %s lacks reviewed Snowflake service versions or deployment cells", c.Name)
		}
		for _, version := range c.SnowflakeVersions {
			if strings.TrimSpace(version) == "" {
				return fmt.Errorf("managed profile %s contains an empty Snowflake version", c.Name)
			}
		}
		for _, cell := range c.SnowflakeDeploymentCells {
			if strings.TrimSpace(cell) == "" {
				return fmt.Errorf("managed profile %s contains an empty Snowflake deployment cell", c.Name)
			}
		}
	}
	if c.Destination == EndpointClickHouse {
		if c.Deployment != "self-managed-keeper" || len(c.ClickHouseVersions) == 0 {
			return fmt.Errorf("managed profile %s lacks exact ClickHouse deployment or version admission", c.Name)
		}
		seenClickHouseVersions := make(map[string]struct{}, len(c.ClickHouseVersions))
		for _, version := range c.ClickHouseVersions {
			parts := strings.Split(version, ".")
			if len(parts) < 3 {
				return fmt.Errorf("managed profile %s has non-patch-specific ClickHouse version %q", c.Name, version)
			}
			for _, part := range parts {
				if _, err := strconv.ParseUint(part, 10, 32); err != nil {
					return fmt.Errorf("managed profile %s has invalid ClickHouse version %q", c.Name, version)
				}
			}
			if _, duplicate := seenClickHouseVersions[version]; duplicate {
				return fmt.Errorf("managed profile %s repeats ClickHouse %s", c.Name, version)
			}
			seenClickHouseVersions[version] = struct{}{}
		}
	}
	if c.Support == SupportMaintained {
		required, err := managedProfileRequiredGates(c.Name)
		if err != nil {
			return err
		}
		seen := make(map[string]struct{}, len(c.Gates))
		for _, gate := range c.Gates {
			if gate.Capability == "" || gate.Test == "" {
				return fmt.Errorf("managed profile %s has incomplete gate %+v", c.Name, gate)
			}
			liveRequired, expected := required[gate.Capability]
			if !expected {
				return fmt.Errorf("managed profile %s declares unknown gate %q", c.Name, gate.Capability)
			}
			if _, duplicate := seen[gate.Capability]; duplicate {
				return fmt.Errorf("managed profile %s repeats gate %q", c.Name, gate.Capability)
			}
			if liveRequired && !gate.Live {
				return fmt.Errorf("managed profile %s gate %q requires real-service evidence", c.Name, gate.Capability)
			}
			seen[gate.Capability] = struct{}{}
			delete(required, gate.Capability)
		}
		if len(required) != 0 {
			missing := make([]string, 0, len(required))
			for capability := range required {
				missing = append(missing, capability)
			}
			sort.Strings(missing)
			return fmt.Errorf("managed profile %s lacks promotion gates %v", c.Name, missing)
		}
	}
	return nil
}

func managedProfileRequiredGates(name string) (map[string]bool, error) {
	switch name {
	case ManagedProfilePostgresToPostgresV1:
		return map[string]bool{
			"postgres versions": true, "streamed transactions": true,
			"target admission": true, "schema evolution": true, "DDL reconciliation": true,
			"snapshot to CDC": true, "process kill": true, "pool exhaustion": true,
			"restart": true, "retry and retention": true, "metrics": false,
			"upgrade migrations": true,
		}, nil
	case ManagedProfilePostgresToClickHouseAppendV1:
		return map[string]bool{
			"clickhouse versions": true, "target admission": true, "ambiguous response": true,
			"deduplication window": true, "ordered fragments": true, "key changes and tombstones": true,
			"schema and types": true, "PostgreSQL recovery": true, "bounded load": true,
			"process kill": true, "keeper recovery": true, "backpressure": true,
			"TLS": true, "telemetry": false,
		}, nil
	case ManagedProfilePostgresToSnowflakeSQLV1:
		return map[string]bool{
			"runtime deployment": true, "source catalog and clean cut": true,
			"target direct grants objects and constraints": true, "role hierarchy and alternate writers": true,
			"task visibility and automation isolation": true, "rollback cardinality ordering and types": true,
			"confirmed commit reconciliation": true, "commit transport loss and detached takeover": true,
			"DDL rejection and replacement": true, "adapter process kill": true, "full worker SIGKILL": true,
			"network fault matrix": true, "cancellation and pool safety": true,
			"bounded load and backpressure": true, "PostgreSQL receipt checkpoint and feedback recovery": true,
			"TLS and JWT": true, "secret redaction": true, "cleanup": true, "telemetry": false,
		}, nil
	case ManagedProfilePostgresToSnowflakeStagedAppendV1:
		return map[string]bool{
			"runtime deployment": true, "source catalog and clean cut": true,
			"target stage grants objects and file format": true, "role hierarchy and alternate writers": true,
			"pipe visibility and auto-ingest isolation":             true,
			"deterministic stage identity and wrong-byte collision": true, "PUT uncertainty reconciliation": true,
			"fail-closed COPY and partial-load rejection": true, "load history verification and receipt adoption": true,
			"auto-ingest verified completion": true, "copy transport loss and detached takeover": true,
			"DDL rejection and replacement": true, "adapter process kill": true, "full worker SIGKILL": true,
			"network fault matrix": true, "cancellation and pool safety": true,
			"bounded load and backpressure": true, "cleanup release receipts and retention roots": true,
			"PostgreSQL receipt checkpoint and feedback recovery": true,
			"TLS and JWT": true, "secret redaction": true, "telemetry": false,
		}, nil
	case ManagedProfilePostgresToSnowflakeStreamingRestAppendV1:
		return map[string]bool{
			"reviewed high-performance append transport": true, "runtime deployment": true,
			"source catalog and clean cut": true, "target channel grants objects and pipe": true,
			"role hierarchy and alternate writers": true, "channel and pipe revision evidence": true,
			"deterministic row identity and SQL-observed completeness": true,
			"reopen after uncommitted rows and append proven-missing":  true,
			"terminal token with rejected rows fails closed":           true,
			"complete-unreceipted recovery and receipt adoption":       true,
			"receipt conflicts and channel invalidation":               true,
			"schema evolution and TOAST unchanged fields":              true,
			"auth expiry refresh":                                      true, "throttling and backpressure": true, "oversize rejection": true,
			"adapter process kill": true, "full worker SIGKILL": true, "cancellation and pool safety": true,
			"cleanup release receipts and channel state":          true,
			"PostgreSQL receipt checkpoint and feedback recovery": true,
			"TLS and JWT": true, "secret redaction": true, "telemetry": false,
		}, nil
	default:
		return nil, fmt.Errorf("managed profile %s has no executable promotion gate set", name)
	}
}

// SupportsClickHouseVersion reports whether the server version is one exact
// ClickHouse patch build admitted by this profile.
func (c ManagedProfileContract) SupportsClickHouseVersion(version string) bool {
	version = strings.TrimSpace(version)
	for _, supported := range c.ClickHouseVersions {
		if version == supported {
			return true
		}
	}
	return false
}

// SupportsPostgresVersion reports whether the named profile admits a live
// PostgreSQL server_version_num major.
// ManagedPostgresVersionProvider exposes the admitted live server major after
// connector Open. The runner compares both endpoints before managed delivery.
type ManagedPostgresVersionProvider interface {
	ManagedPostgresMajor() int
}

// ManagedPostgresPublicationProvider exposes the exact live publication
// relation set observed during named-profile source admission.
type ManagedPostgresPublicationProvider interface {
	ManagedPostgresPublicationTables() []string
	ManagedPostgresPublicationSchemas() []Schema
}

// ManagedSourceSchemaValidator lets a named destination compare a live source
// catalog schema with its immutable destination contract before reading WAL.
type ManagedSourceSchemaValidator interface {
	ValidateManagedSourceSchema(Schema) error
}

// ManagedFlowScopeValidator proves that an already-open destination contains
// no receipts owned by another incarnation before the runner reads WAL.
type ManagedFlowScopeValidator interface {
	ValidateManagedFlowScope(context.Context, string, string) error
}

// ManagedClickHouseVersionProvider exposes the admitted live server version
// after connector Open.
type ManagedClickHouseVersionProvider interface {
	ManagedClickHouseVersion() string
}

// ManagedSnowflakeVersionProvider exposes the exact CURRENT_VERSION() value
// admitted by the constrained Snowflake SQL destination during Open.
type ManagedSnowflakeVersionProvider interface {
	ManagedSnowflakeVersion() string
}

func (c ManagedProfileContract) SupportsPostgresVersion(major int) bool {
	for _, supported := range c.PostgresVersions {
		if supported == major {
			return true
		}
	}
	return false
}
