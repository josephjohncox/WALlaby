package connector

import (
	"errors"
	"fmt"
	"sort"
)

const (
	// ManagedProfilePostgresToPostgresV1 is the only promoted managed runtime
	// profile. The generic postgres connector modes remain experimental.
	ManagedProfilePostgresToPostgresV1 = "postgresql-to-postgresql-v1"
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
	Name              string
	Support           SupportLevel
	Source            EndpointType
	Destination       EndpointType
	PostgresVersions  []int
	SameMajorOnly     bool
	AckPolicies       []string
	SingleSink        bool
	DeliveryGuarantee string
	Gates             []ManagedProfileGate
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
	if c.Support == SupportMaintained && !c.SameMajorOnly {
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
	if c.Support == SupportMaintained {
		required := map[string]bool{
			"postgres versions": true, "streamed transactions": true,
			"target admission": true, "schema evolution": true, "DDL reconciliation": true,
			"snapshot to CDC": true, "process kill": true, "pool exhaustion": true,
			"restart": true, "retry and retention": true, "metrics": false,
			"upgrade migrations": true,
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

// SupportsPostgresVersion reports whether the named profile admits a live
// PostgreSQL server_version_num major.
// ManagedPostgresVersionProvider exposes the admitted live server major after
// connector Open. The runner compares both endpoints before managed delivery.
type ManagedPostgresVersionProvider interface {
	ManagedPostgresMajor() int
}

func (c ManagedProfileContract) SupportsPostgresVersion(major int) bool {
	for _, supported := range c.PostgresVersions {
		if supported == major {
			return true
		}
	}
	return false
}
