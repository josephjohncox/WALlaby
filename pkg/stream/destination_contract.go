package stream

import (
	"fmt"
	"strings"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// ValidateDestinationContracts checks whether configured destinations can honor
// the flow's acknowledgement and DDL policies before any connector is opened.
func ValidateDestinationContracts(
	destinations []DestinationConfig,
	ackPolicy AckPolicy,
	primaryDestination string,
	requireDDLExecution bool,
) error {
	if ackPolicy == "" {
		ackPolicy = AckPolicyAll
	}
	if ackPolicy != AckPolicyAll && ackPolicy != AckPolicyPrimary && ackPolicy != AckPolicyMaterialized {
		return fmt.Errorf("unsupported acknowledgement policy %q", ackPolicy)
	}
	if ackPolicy == AckPolicyMaterialized {
		if strings.TrimSpace(primaryDestination) != "" {
			return fmt.Errorf("materialized acknowledgement does not use primary destination %q", primaryDestination)
		}
		if len(destinations) != 1 {
			return fmt.Errorf("materialized acknowledgement currently requires exactly one destination revision; got %d", len(destinations))
		}
		if _, ok := destinations[0].Dest.(connector.ManagedTransactionDestination); !ok {
			return fmt.Errorf("materialized destination %s must implement full-transaction durable reconciliation", destinationLabel(destinations[0].Spec))
		}
	}

	primaryFound := false
	for _, destination := range destinations {
		if destination.Dest == nil {
			return fmt.Errorf("destination %s is not configured", destinationLabel(destination.Spec))
		}
		capabilities := connector.ResolveDestinationCapabilities(destination.Dest, destination.Spec)
		label := destinationLabel(destination.Spec)
		if destination.Spec.Type != "" && !capabilities.Delivery.Declared {
			return fmt.Errorf("destination %s does not declare delivery semantics", label)
		}
		if capabilities.Delivery.ReplaySafe && !capabilities.Delivery.IdempotentReplay {
			return fmt.Errorf("destination %s declares replay safety without idempotent replay", label)
		}
		if capabilities.Delivery.Lossy && capabilities.Delivery.ReplaySafe {
			return fmt.Errorf("destination %s declares incompatible lossy and replay-safe behavior", label)
		}
		if capabilities.Delivery.Lossy {
			return fmt.Errorf("destination %s may drop records and cannot participate in source acknowledgement", label)
		}
		if requireDDLExecution && !capabilities.ExecutesDDL() {
			return fmt.Errorf("destination %s cannot execute DDL required by the flow policy", label)
		}
		if requireDDLExecution && strings.TrimSpace(destination.Spec.Name) == "" {
			return fmt.Errorf("automatic DDL execution requires a stable destination name")
		}
		if requireDDLExecution {
			if _, ok := destination.Dest.(connector.DDLReconciler); !ok {
				return fmt.Errorf("destination %s cannot reconcile DDL after an ambiguous execution", label)
			}
		}

		if !capabilities.Delivery.IdempotentReplay || !capabilities.Delivery.ReplaySafe {
			if ackPolicy == AckPolicyMaterialized {
				// Source acknowledgement is owned by the canonical artifact log.
				// PostgreSQL delivery attempts/receipts, not source replay, govern
				// asynchronous consumer retries.
				continue
			}
			if ackPolicy == AckPolicyAll {
				// A single destination may duplicate after a downstream commit and
				// pre-checkpoint crash; that is the explicit at-least-once mode. It
				// cannot amplify partial fan-out success into sibling replays, so
				// reject only unsafe fan-out until delivery state is durable per sink.
				if len(destinations) > 1 {
					return fmt.Errorf("all acknowledgement fan-out requires replay-safe idempotent destination %s", label)
				}
				continue
			}
			return fmt.Errorf("primary acknowledgement requires replay-safe idempotent destination %s", label)
		}
		if ackPolicy != AckPolicyPrimary {
			continue
		}
		if destination.Spec.Name == primaryDestination {
			primaryFound = true
			if !capabilities.Delivery.TransactionalBatch {
				return fmt.Errorf("primary acknowledgement requires transactional batch writes from destination %s", label)
			}
		}
	}

	if ackPolicy == AckPolicyPrimary {
		if strings.TrimSpace(primaryDestination) == "" {
			return fmt.Errorf("primary acknowledgement requires a primary destination name")
		}
		if !primaryFound {
			return fmt.Errorf("primary destination %q not found", primaryDestination)
		}
	}
	return nil
}

func destinationLabel(spec connector.Spec) string {
	if strings.TrimSpace(spec.Name) != "" {
		return fmt.Sprintf("%q", spec.Name)
	}
	if spec.Type != "" {
		return fmt.Sprintf("type %q", spec.Type)
	}
	return "<unnamed>"
}
