package runner

import (
	"errors"
	"fmt"
	"strings"

	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func validateManagedAdmission(f flow.Flow, source connector.Source, sourceSpec connector.Spec, destinations []stream.DestinationConfig, cfg StreamRunnerConfig) error {
	if sourceSpec.Type != connector.EndpointPostgres {
		return errors.New("managed execution currently requires a PostgreSQL source")
	}
	if _, ok := source.(connector.TransactionalSource); !ok {
		return errors.New("managed PostgreSQL execution requires the transactional source interface")
	}
	if cfg.RunFence == nil || cfg.DeliveryCoordinator == nil {
		return errors.New("managed execution requires PostgreSQL run authority and delivery coordination")
	}
	if _, ok := cfg.Checkpoints.(checkpoint.FencedStore); !ok {
		return errors.New("managed execution requires PostgreSQL-authoritative generation-fenced checkpoints")
	}
	if strings.TrimSpace(sourceSpec.Options["start_lsn"]) != "" {
		return errors.New("managed execution rejects arbitrary start_lsn; restore is derived from the authoritative checkpoint or slot consistent point")
	}
	for _, key := range []string{"source_system_identifier", "source_lineage_id", "publication_revision"} {
		if strings.TrimSpace(sourceSpec.Options[key]) == "" {
			return fmt.Errorf("managed execution requires %s", key)
		}
	}
	if strings.EqualFold(strings.TrimSpace(sourceSpec.Options["mode"]), "backfill") {
		return errors.New("managed execution rejects legacy mode=backfill")
	}
	bootstrapMode := strings.ToLower(strings.TrimSpace(sourceSpec.Options["bootstrap"]))
	switch bootstrapMode {
	case "never":
		// The production worker does not yet wire the slot-exported bootstrap
		// coordinator. Admitting auto/required would silently omit existing rows.
	case "", "auto", "required":
		return errors.New("managed bootstrap is not wired into production execution; set bootstrap=never and provision the initial dataset separately")
	default:
		return fmt.Errorf("unsupported managed bootstrap mode %q", bootstrapMode)
	}
	if backend := strings.ToLower(strings.TrimSpace(sourceSpec.Options["snapshot_state_backend"])); backend == "file" || backend == "none" {
		return fmt.Errorf("managed execution rejects snapshot authority backend %q", backend)
	}
	if parseEnabledOption(sourceSpec.Options["ensure_publication"], true) {
		return errors.New("managed execution requires ensure_publication=false; publication changes are not yet fenced")
	}
	if parseEnabledOption(sourceSpec.Options["sync_publication"], false) {
		return errors.New("managed execution rejects sync_publication until source-resource operations are fenced")
	}
	if parseEnabledOption(sourceSpec.Options["capture_ddl"], false) {
		return errors.New("managed execution rejects capture_ddl until registry mutations carry the run fence")
	}
	if parseEnabledOption(sourceSpec.Options["ensure_state"], true) {
		return errors.New("managed execution requires ensure_state=false; the legacy slot-keyed source-state table is unfenced")
	}
	if f.Config.FailureMode == stream.FailureModeDropSlot {
		return errors.New("managed execution rejects failure_mode=drop_slot; cleanup requires fenced source-resource ownership")
	}
	if f.Config.AckPolicy != "" && f.Config.AckPolicy != stream.AckPolicyAll {
		return errors.New("managed PostgreSQL profile currently requires ack_policy=all")
	}
	if f.Config.DDL.AutoApply != nil && *f.Config.DDL.AutoApply {
		return errors.New("managed PostgreSQL profile rejects automatic raw-SQL DDL; structured receipt-backed DDL is not yet admitted")
	}
	if cfg.ResolveStaging {
		return errors.New("managed execution rejects generic staging resolution without durable publication receipts")
	}
	if len(destinations) != 1 {
		return errors.New("managed PostgreSQL profile currently requires exactly one destination revision")
	}
	destination := destinations[0]
	if destination.Spec.Type == connector.EndpointClickHouse {
		return errors.New("managed ClickHouse mutation delivery is experimental and has no admitted reconciliation contract")
	}
	if destination.Spec.Type != connector.EndpointPostgres {
		return fmt.Errorf("managed destination type %q is not admitted by the initial profile", destination.Spec.Type)
	}
	if _, ok := destination.Dest.(connector.ManagedDestination); !ok {
		return errors.New("managed destination does not implement durable reconciliation")
	}
	if strings.TrimSpace(destination.Spec.Options["destination_revision_id"]) == "" {
		return errors.New("managed destination_revision_id is required")
	}
	if mode := strings.ToLower(strings.TrimSpace(destination.Spec.Options["write_mode"])); mode != "" && mode != "target" {
		return fmt.Errorf("managed PostgreSQL destination rejects write_mode=%q", mode)
	}
	if mode := strings.ToLower(strings.TrimSpace(destination.Spec.Options["batch_mode"])); mode != "" && mode != "target" {
		return fmt.Errorf("managed PostgreSQL destination rejects batch_mode=%q", mode)
	}
	if syncCommit := strings.ToLower(strings.TrimSpace(destination.Spec.Options["synchronous_commit"])); syncCommit != "on" && syncCommit != "remote_apply" {
		return fmt.Errorf("managed PostgreSQL destination requires explicit durable synchronous_commit=on or remote_apply; got %q", syncCommit)
	}
	return nil
}

// parseEnabledOption is intentionally fail-closed: an unknown non-empty value
// is treated as enabled so managed admission rejects rather than silently
// disabling a safety-relevant option.
func parseEnabledOption(raw string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return fallback
	case "true", "1", "yes", "on":
		return true
	case "false", "0", "no", "off":
		return false
	default:
		return true
	}
}
