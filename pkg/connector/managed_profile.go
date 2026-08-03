package connector

import (
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
)

// ManagedProfileGate binds one support claim to a required real-service test.
type ManagedProfileGate struct {
	Capability string
	Test       string
	Live       bool
}

// ManagedProfileContract is the executable support and admission declaration
// rendered into the generated connector support matrix.
type ManagedProfileContract struct {
	Name               string
	Support            SupportLevel
	Source             EndpointType
	Destination        EndpointType
	PostgresVersions   []int
	ClickHouseVersions []string
	Deployment         string
	SameMajorOnly      bool
	AckPolicies        []string
	SingleSink         bool
	DeliveryGuarantee  string
	Gates              []ManagedProfileGate
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

// ManagedClickHouseVersionProvider exposes the admitted live server version
// after connector Open.
type ManagedClickHouseVersionProvider interface {
	ManagedClickHouseVersion() string
}

func (c ManagedProfileContract) SupportsPostgresVersion(major int) bool {
	for _, supported := range c.PostgresVersions {
		if supported == major {
			return true
		}
	}
	return false
}
