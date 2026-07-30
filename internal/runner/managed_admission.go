package runner

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	chclient "github.com/ClickHouse/clickhouse-go/v2"
	snowflakedest "github.com/josephjohncox/wallaby/connectors/destinations/snowflake"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"github.com/snowflakedb/gosnowflake"
)

func managedSourceSpec(spec connector.Spec) bool {
	if spec.Options == nil {
		return false
	}
	if strings.TrimSpace(spec.Options["managed_profile"]) != "" {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(spec.Options["managed"])) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func validateManagedAdmission(f flow.Flow, source connector.Source, sourceSpec connector.Spec, destinations []stream.DestinationConfig, cfg StreamRunnerConfig) error {
	profileName := strings.TrimSpace(sourceSpec.Options["managed_profile"])
	switch profileName {
	case "", connector.ManagedProfilePostgresToPostgresV1, connector.ManagedProfilePostgresToClickHouseAppendV1,
		connector.ManagedProfilePostgresToSnowflakeSQLV1, connector.ManagedProfilePostgresToSnowflakeStagedAppendV1,
		connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1:
	default:
		return fmt.Errorf("unsupported managed_profile %q", profileName)
	}
	if sourceSpec.Type != connector.EndpointPostgres {
		return errors.New("managed execution currently requires a PostgreSQL source")
	}
	if _, ok := source.(connector.TransactionalSource); !ok {
		return errors.New("managed PostgreSQL execution requires the transactional source interface")
	}
	if _, ok := source.(connector.FlushEvidenceSource); !ok {
		return errors.New("managed PostgreSQL execution requires source flush evidence")
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
	if bootstrapMode == "" {
		bootstrapMode = "auto"
	}
	switch bootstrapMode {
	case "never":
		for _, option := range []string{"ensure_state", "ensure_publication", "sync_publication"} {
			if raw, present := sourceSpec.Options[option]; !present || parseEnabledOption(raw, true) {
				return fmt.Errorf("managed bootstrap=never requires explicit %s=false; resource mutation/adoption must be fenced", option)
			}
		}
		createSlot, createSlotPresent := sourceSpec.Options["create_slot"]
		if connector.IsManagedSnowflakeProfile(profileName) {
			if !createSlotPresent || !parseEnabledOption(createSlot, false) {
				return fmt.Errorf("%s requires create_slot=true for its fenced clean source cut", profileName)
			}
		} else if !createSlotPresent || parseEnabledOption(createSlot, true) {
			return errors.New("managed bootstrap=never requires explicit create_slot=false; resource mutation/adoption must be fenced")
		}
	case "auto", "required":
		if raw := strings.TrimSpace(sourceSpec.Options["pool_max_conns"]); raw != "" {
			maxConns, err := strconv.Atoi(raw)
			if err != nil || maxConns < 2 {
				return errors.New("managed bootstrap requires pool_max_conns>=2 before connector side effects")
			}
		}
		if _, ok := source.(connector.ManagedBootstrapSource); !ok {
			return errors.New("managed bootstrap requires a slot-anchored bootstrap source")
		}
		if len(destinations) == 1 {
			if _, ok := destinations[0].Dest.(connector.ManagedBootstrapDestination); !ok {
				return errors.New("managed bootstrap requires an atomically publishable destination snapshot")
			}
		}
	default:
		return fmt.Errorf("unsupported managed bootstrap mode %q", bootstrapMode)
	}
	if backend := strings.ToLower(strings.TrimSpace(sourceSpec.Options["snapshot_state_backend"])); backend == "file" || backend == "none" {
		return fmt.Errorf("managed execution rejects snapshot authority backend %q", backend)
	}
	if parseEnabledOption(sourceSpec.Options["capture_ddl"], false) {
		return errors.New("managed execution rejects capture_ddl until registry mutations carry the run fence")
	}
	if f.Config.FailureMode == stream.FailureModeDropSlot {
		return errors.New("managed execution rejects failure_mode=drop_slot; cleanup requires fenced source-resource ownership")
	}
	ackPolicy := f.Config.AckPolicy
	if ackPolicy == "" {
		ackPolicy = stream.AckPolicyAll
	}
	switch ackPolicy {
	case stream.AckPolicyAll:
		if f.Config.Materialization != (flow.MaterializationPolicy{}) {
			return errors.New("materialization policy requires ack_policy=materialized")
		}
	case stream.AckPolicyMaterialized:
		if profileName != "" {
			return fmt.Errorf("named managed profile %s requires ack_policy=all; materialized publication remains experimental", profileName)
		}
		if cfg.ArtifactLog == nil {
			return errors.New("ack_policy=materialized requires a configured PostgreSQL-authoritative artifact log")
		}
		if f.Config.Materialization.ProjectionID != "canonical_cdc_parquet_v1" {
			return fmt.Errorf("ack_policy=materialized requires materialization.projection_id=canonical_cdc_parquet_v1; got %q", f.Config.Materialization.ProjectionID)
		}
	default:
		return errors.New("managed PostgreSQL profile currently requires ack_policy=all or the explicit materialized artifact contract")
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
	if strings.TrimSpace(destination.Spec.Options["destination_revision_id"]) == "" {
		return errors.New("managed destination_revision_id is required")
	}
	if ackPolicy == stream.AckPolicyMaterialized {
		if _, ok := destination.Dest.(connector.CanonicalArtifactDestination); ok {
			if destination.Spec.Type != connector.EndpointIceberg {
				return errors.New("canonical artifact consumer must use the iceberg endpoint type")
			}
			if bootstrapMode != "never" {
				return errors.New("iceberg canonical consumption currently requires bootstrap=never; snapshot rows are not yet published through the artifact log")
			}
			return nil
		}
	}
	if _, ok := destination.Dest.(connector.ManagedTransactionDestination); !ok {
		return errors.New("managed destination does not implement full-transaction durable reconciliation")
	}

	switch profileName {
	case connector.ManagedProfilePostgresToClickHouseAppendV1:
		return validateManagedClickHouseAdmission(sourceSpec, destination, bootstrapMode)
	case connector.ManagedProfilePostgresToSnowflakeSQLV1:
		return validateManagedSnowflakeAdmission(f.ID, sourceSpec, destination, bootstrapMode)
	case connector.ManagedProfilePostgresToSnowflakeStagedAppendV1:
		return validateManagedSnowflakeStagedAppendAdmission(f.ID, sourceSpec, destination, bootstrapMode)
	case connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1:
		return validateManagedSnowflakeStreamingAppendAdmission(f.ID, sourceSpec, destination, bootstrapMode)
	case "", connector.ManagedProfilePostgresToPostgresV1:
		return validateManagedPostgresDestinationAdmission(sourceSpec, destination, bootstrapMode, profileName)
	default:
		return fmt.Errorf("unsupported managed_profile %q", profileName)
	}
}

func validateManagedPostgresDestinationAdmission(sourceSpec connector.Spec, destination stream.DestinationConfig, bootstrapMode, profileName string) error {
	if destination.Spec.Type == connector.EndpointClickHouse {
		return errors.New("generic ClickHouse mutation delivery is experimental; use the exact append-only managed profile")
	}
	if destination.Spec.Type != connector.EndpointPostgres {
		return fmt.Errorf("managed destination type %q is not admitted by the PostgreSQL profile", destination.Spec.Type)
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
	if profileName == "" {
		return nil
	}
	profile := connector.PostgresToPostgresV1Profile()
	if err := profile.ValidatePromotion(); err != nil {
		return fmt.Errorf("managed profile promotion contract: %w", err)
	}
	if bootstrapMode != "required" {
		return fmt.Errorf("%s requires bootstrap=required", profileName)
	}
	if !parseEnabledOption(sourceSpec.Options["streaming_transactions"], false) {
		return fmt.Errorf("%s requires streaming_transactions=true", profileName)
	}
	if destinationProfile := strings.TrimSpace(destination.Spec.Options["managed_profile"]); destinationProfile != profileName {
		return fmt.Errorf("destination managed_profile %q does not match source profile %q", destinationProfile, profileName)
	}
	return nil
}

func validateManagedClickHouseAdmission(sourceSpec connector.Spec, destination stream.DestinationConfig, bootstrapMode string) error {
	const profileName = connector.ManagedProfilePostgresToClickHouseAppendV1
	if destination.Spec.Type != connector.EndpointClickHouse {
		return fmt.Errorf("%s requires a ClickHouse destination", profileName)
	}
	if destinationProfile := strings.TrimSpace(destination.Spec.Options["managed_profile"]); destinationProfile != profileName {
		return fmt.Errorf("destination managed_profile %q does not match source profile %q", destinationProfile, profileName)
	}
	profile := connector.PostgresToClickHouseAppendV1Profile()
	if err := profile.ValidatePromotion(); err != nil {
		return fmt.Errorf("managed profile promotion contract: %w", err)
	}
	if bootstrapMode != "never" {
		return fmt.Errorf("%s currently requires bootstrap=never; the promoted profile is an append-only CDC stream", profileName)
	}
	if !parseEnabledOption(sourceSpec.Options["streaming_transactions"], false) {
		return fmt.Errorf("%s requires streaming_transactions=true", profileName)
	}
	options := destination.Spec.Options
	dsnOptions, err := chclient.ParseDSN(strings.TrimSpace(options["dsn"]))
	if err != nil {
		return fmt.Errorf("%s requires a valid native ClickHouse DSN: %w", profileName, err)
	}
	if dsnOptions.Protocol != chclient.Native || dsnOptions.TLS == nil {
		return fmt.Errorf("%s requires verified native TLS", profileName)
	}
	if dsnOptions.TLS.InsecureSkipVerify {
		return fmt.Errorf("%s rejects TLS skip_verify", profileName)
	}
	replicaDSNOptions, err := chclient.ParseDSN(strings.TrimSpace(options["managed_replica_dsn"]))
	if err != nil {
		return fmt.Errorf("%s requires a valid managed_replica_dsn: %w", profileName, err)
	}
	if replicaDSNOptions.Protocol != chclient.Native || replicaDSNOptions.TLS == nil {
		return fmt.Errorf("%s requires verified native TLS for managed_replica_dsn", profileName)
	}
	if replicaDSNOptions.TLS.InsecureSkipVerify {
		return fmt.Errorf("%s rejects managed replica TLS skip_verify", profileName)
	}
	if strings.Join(dsnOptions.Addr, ",") == strings.Join(replicaDSNOptions.Addr, ",") {
		return fmt.Errorf("%s requires distinct primary and replica endpoints", profileName)
	}
	if mode := strings.ToLower(strings.TrimSpace(options["write_mode"])); mode != "managed_append" {
		return fmt.Errorf("%s requires write_mode=managed_append; got %q", profileName, mode)
	}
	if mode := strings.ToLower(strings.TrimSpace(options["batch_mode"])); mode != "target" {
		return fmt.Errorf("%s requires batch_mode=target; got %q", profileName, mode)
	}
	if resolution := strings.ToLower(strings.TrimSpace(options["batch_resolution"])); resolution != "" && resolution != "none" {
		return fmt.Errorf("%s requires batch_resolution=none; got %q", profileName, resolution)
	}
	if parseEnabledOption(options["meta_table_enabled"], true) {
		return fmt.Errorf("%s requires meta_table_enabled=false; target metadata is the immutable changelog and receipt tables", profileName)
	}
	if parseEnabledOption(options["async_insert"], false) {
		return fmt.Errorf("%s requires async_insert=false", profileName)
	}
	if !parseEnabledOption(options["wait_for_async_insert"], false) {
		return fmt.Errorf("%s requires wait_for_async_insert=true", profileName)
	}
	if deployment := strings.ToLower(strings.TrimSpace(options["managed_deployment"])); deployment != profile.Deployment {
		return fmt.Errorf("%s requires managed_deployment=%s; got %q", profileName, profile.Deployment, deployment)
	}
	keeperPathPrefix := strings.TrimSuffix(strings.TrimSpace(options["managed_keeper_path_prefix"]), "/")
	if keeperPathPrefix == "" || !strings.HasPrefix(keeperPathPrefix, "/") || strings.ContainsAny(keeperPathPrefix, "'\\") {
		return fmt.Errorf("%s requires an absolute managed_keeper_path_prefix", profileName)
	}
	keeperHost, keeperPort, err := net.SplitHostPort(strings.TrimSpace(options["managed_keeper_address"]))
	if err != nil || strings.TrimSpace(keeperHost) == "" || strings.TrimSpace(keeperPort) == "" {
		return fmt.Errorf("%s requires managed_keeper_address as host:port", profileName)
	}
	replicas := make(map[string]struct{})
	for _, raw := range strings.Split(options["managed_replica_names"], ",") {
		if name := strings.TrimSpace(raw); name != "" {
			replicas[name] = struct{}{}
		}
	}
	if len(replicas) != 2 {
		return fmt.Errorf("%s requires exactly two unique managed_replica_names", profileName)
	}
	if quorum := strings.TrimSpace(options["insert_quorum"]); quorum != "2" {
		return fmt.Errorf("%s requires insert_quorum=2 so fragments and receipts reach both admitted replicas; got %q", profileName, quorum)
	}
	for _, key := range []string{"managed_database", "managed_changelog_table", "managed_receipts_table", "managed_final_view"} {
		if strings.TrimSpace(options[key]) == "" {
			return fmt.Errorf("%s requires %s", profileName, key)
		}
	}
	destinationLimits := make(map[string]uint64)
	for _, key := range []string{
		"managed_max_active_parts", "managed_max_transaction_rows", "managed_max_transaction_bytes",
		"managed_max_transaction_fragments", "managed_max_rows_per_batch", "managed_max_batch_bytes",
	} {
		value, err := requiredManagedLimit(options, key)
		if err != nil {
			return fmt.Errorf("%s: %w", profileName, err)
		}
		destinationLimits[key] = value
	}
	if destinationLimits["managed_max_active_parts"] >= 200 {
		return fmt.Errorf("%s requires managed_max_active_parts below the admitted parts_to_throw_insert floor 200", profileName)
	}
	if destinationLimits["managed_max_rows_per_batch"] > destinationLimits["managed_max_transaction_rows"] {
		return fmt.Errorf("%s managed_max_rows_per_batch exceeds managed_max_transaction_rows", profileName)
	}
	if destinationLimits["managed_max_batch_bytes"] > destinationLimits["managed_max_transaction_bytes"] {
		return fmt.Errorf("%s managed_max_batch_bytes exceeds managed_max_transaction_bytes", profileName)
	}
	for sourceKey, destinationKey := range map[string]string{
		"max_transaction_records":   "managed_max_transaction_rows",
		"max_transaction_bytes":     "managed_max_transaction_bytes",
		"max_transaction_fragments": "managed_max_transaction_fragments",
	} {
		sourceLimit, err := requiredManagedLimit(sourceSpec.Options, sourceKey)
		if err != nil {
			return fmt.Errorf("%s: %w", profileName, err)
		}
		if sourceLimit > destinationLimits[destinationKey] {
			return fmt.Errorf("%s source %s=%d exceeds destination %s=%d", profileName, sourceKey, sourceLimit, destinationKey, destinationLimits[destinationKey])
		}
	}
	return nil
}

func validateManagedSnowflakeAdmission(flowID string, sourceSpec connector.Spec, destination stream.DestinationConfig, bootstrapMode string) error {
	const profileName = connector.ManagedProfilePostgresToSnowflakeSQLV1
	if strings.TrimSpace(destination.Spec.Options["flow_id"]) != flowID {
		return fmt.Errorf("%s destination flow_id %q does not match flow %q", profileName, destination.Spec.Options["flow_id"], flowID)
	}
	if destination.Spec.Type != connector.EndpointSnowflake {
		return fmt.Errorf("%s requires a Snowflake destination", profileName)
	}
	if destinationProfile := strings.TrimSpace(destination.Spec.Options["managed_profile"]); destinationProfile != profileName {
		return fmt.Errorf("destination managed_profile %q does not match source profile %q", destinationProfile, profileName)
	}
	profile := connector.PostgresToSnowflakeSQLV1Profile()
	if err := profile.ValidatePromotion(); err != nil {
		return fmt.Errorf("managed profile contract: %w", err)
	}
	if bootstrapMode != "never" {
		return fmt.Errorf("%s currently requires bootstrap=never; target state must be provisioned before admission", profileName)
	}
	if strings.TrimSpace(sourceSpec.Options["slot"]) != "managed" {
		return fmt.Errorf("%s requires slot=managed so the RunFence derives a flow-incarnation-specific slot", profileName)
	}
	if !parseEnabledOption(sourceSpec.Options["streaming_transactions"], false) {
		return fmt.Errorf("%s requires streaming_transactions=true", profileName)
	}
	if toastFetch := strings.ToLower(strings.TrimSpace(sourceSpec.Options["toast_fetch"])); toastFetch != "off" {
		return fmt.Errorf("%s requires toast_fetch=off; replay must not read future source state", profileName)
	}

	options := destination.Spec.Options
	if err := snowflakedest.ValidateManagedProfileOptions(options); err != nil {
		return fmt.Errorf("%s: %w", profileName, err)
	}
	for key, want := range map[string]string{
		"write_mode": "target", "batch_mode": "target", "batch_resolution": "none",
	} {
		if got := strings.ToLower(strings.TrimSpace(options[key])); got != want {
			return fmt.Errorf("%s requires %s=%s; got %q", profileName, key, want, got)
		}
	}
	if parseEnabledOption(options["meta_table_enabled"], true) {
		return fmt.Errorf("%s requires meta_table_enabled=false; the owned receipt table is the only target metadata object", profileName)
	}
	if parseEnabledOption(options["disable_transactions"], false) {
		return fmt.Errorf("%s requires disable_transactions=false", profileName)
	}
	if parseEnabledOption(options["session_keep_alive"], false) {
		return fmt.Errorf("%s requires session_keep_alive=false", profileName)
	}
	if strings.TrimSpace(options["type_mappings"]) != "" || strings.TrimSpace(options["type_mappings_file"]) != "" {
		return fmt.Errorf("%s rejects type mapping overrides until each mapping has real-service evidence", profileName)
	}
	for _, key := range []string{
		"schema", "table", "staging_schema", "staging_table", "staging_suffix", "warehouse", "warehouse_size",
		"warehouse_auto_suspend", "warehouse_auto_resume", "meta_schema", "meta_table", "meta_pk_prefix",
	} {
		if strings.TrimSpace(options[key]) != "" {
			return fmt.Errorf("%s rejects generic option %s", profileName, key)
		}
	}

	dsnConfig, err := gosnowflake.ParseDSN(strings.TrimSpace(options["dsn"]))
	if err != nil {
		return fmt.Errorf("%s requires a valid Snowflake DSN: %w", profileName, err)
	}
	if !strings.EqualFold(dsnConfig.Protocol, "https") || dsnConfig.DisableOCSPChecks || dsnConfig.OCSPFailOpen != gosnowflake.OCSPFailOpenFalse {
		return fmt.Errorf("%s requires verified HTTPS with OCSP fail-closed", profileName)
	}
	if dsnConfig.Authenticator != gosnowflake.AuthTypeJwt || dsnConfig.PrivateKey == nil {
		return fmt.Errorf("%s requires key-pair JWT authentication", profileName)
	}
	readLatestWrites := false
	clientSessionKeepAlive := false
	timezone := ""
	for key, value := range dsnConfig.Params {
		if value == nil {
			continue
		}
		enabled, _ := strconv.ParseBool(strings.TrimSpace(*value))
		switch {
		case strings.EqualFold(strings.TrimSpace(key), "READ_LATEST_WRITES"):
			readLatestWrites = enabled
		case strings.EqualFold(strings.TrimSpace(key), "CLIENT_SESSION_KEEP_ALIVE"):
			clientSessionKeepAlive = enabled
		case strings.EqualFold(strings.TrimSpace(key), "TIMEZONE"):
			timezone = strings.TrimSpace(*value)
		}
	}
	if !readLatestWrites {
		return fmt.Errorf("%s requires DSN session parameter READ_LATEST_WRITES=true for cross-session receipt reconciliation", profileName)
	}
	if !strings.EqualFold(timezone, "UTC") {
		return fmt.Errorf("%s requires DSN session parameter TIMEZONE=UTC", profileName)
	}
	if clientSessionKeepAlive {
		return fmt.Errorf("%s rejects DSN session parameter CLIENT_SESSION_KEEP_ALIVE=true", profileName)
	}

	identities := []struct {
		option string
		dsn    string
		label  string
	}{
		{option: "managed_account", dsn: dsnConfig.Account, label: "account"},
		{option: "managed_database", dsn: dsnConfig.Database, label: "database"},
		{option: "managed_schema", dsn: dsnConfig.Schema, label: "schema"},
		{option: "managed_execution_role", dsn: dsnConfig.Role, label: "role"},
		{option: "managed_warehouse", dsn: dsnConfig.Warehouse, label: "warehouse"},
	}
	for _, identity := range identities {
		configured := strings.TrimSpace(options[identity.option])
		if configured == "" {
			return fmt.Errorf("%s requires %s", profileName, identity.option)
		}
		if !strings.EqualFold(configured, identity.dsn) {
			return fmt.Errorf("%s %s %q does not match DSN %s %q", profileName, identity.option, configured, identity.label, identity.dsn)
		}
	}
	if err := validateManagedSnowflakeRevisionID(options["destination_revision_id"]); err != nil {
		return fmt.Errorf("%s: %w", profileName, err)
	}
	for _, key := range []string{"managed_database", "managed_schema", "managed_table", "managed_receipts_table", "managed_owner_role", "managed_execution_role", "managed_warehouse"} {
		if err := validateManagedSnowflakeIdentifier(key, options[key]); err != nil {
			return fmt.Errorf("%s: %w", profileName, err)
		}
	}
	if options["managed_owner_role"] == options["managed_execution_role"] {
		return fmt.Errorf("%s execution role must not own target objects", profileName)
	}
	for _, key := range []string{"managed_source_schema", "managed_source_table", "managed_schema_contract", "managed_snowflake_version", "managed_target_created_on", "managed_receipts_created_on"} {
		if strings.TrimSpace(options[key]) == "" {
			return fmt.Errorf("%s requires %s", profileName, key)
		}
	}
	var schemaContract connector.Schema
	if err := json.Unmarshal([]byte(options["managed_schema_contract"]), &schemaContract); err != nil {
		return fmt.Errorf("%s managed_schema_contract is invalid JSON: %w", profileName, err)
	}
	if schemaContract.Name != options["managed_source_table"] || schemaContract.Namespace != options["managed_source_schema"] || len(schemaContract.Columns) == 0 {
		return fmt.Errorf("%s managed_schema_contract must exactly identify the configured non-empty source schema", profileName)
	}
	if !isLowerHexDigest(options["managed_schema_contract_hash"]) {
		return fmt.Errorf("%s managed_schema_contract_hash must be 64 lowercase hexadecimal characters", profileName)
	}
	contractHash, err := snowflakedest.ManagedSchemaContractHash(schemaContract)
	if err != nil {
		return fmt.Errorf("%s managed_schema_contract: %w", profileName, err)
	}
	if contractHash != options["managed_schema_contract_hash"] {
		return fmt.Errorf("%s managed_schema_contract_hash does not identify managed_schema_contract", profileName)
	}
	primaryColumns := 0
	for _, column := range schemaContract.Columns {
		if column.TypeMetadata["nullability_known"] != "true" || column.TypeMetadata["generated_known"] != "true" {
			return fmt.Errorf("%s schema column %q requires known nullability and generation status", profileName, column.Name)
		}
		if column.Generated {
			return fmt.Errorf("%s rejects generated source column %q", profileName, column.Name)
		}
		if column.TypeMetadata["primary_key"] == "true" {
			if column.Nullable {
				return fmt.Errorf("%s source primary-key column %q must be NOT NULL", profileName, column.Name)
			}
			primaryColumns++
		}
	}
	if primaryColumns == 0 {
		return fmt.Errorf("%s requires a source primary key", profileName)
	}

	destinationLimits := make(map[string]uint64)
	for _, key := range []string{"managed_max_transaction_rows", "managed_max_transaction_bytes", "managed_max_transaction_fragments"} {
		value, err := requiredManagedLimit(options, key)
		if err != nil {
			return fmt.Errorf("%s: %w", profileName, err)
		}
		destinationLimits[key] = value
	}
	if destinationLimits["managed_max_transaction_rows"] > 1_000 || destinationLimits["managed_max_transaction_bytes"] > 8<<20 || destinationLimits["managed_max_transaction_fragments"] > 128 {
		return fmt.Errorf("%s transaction bounds exceed the admitted rows=1000 bytes=8388608 fragments=128 ceilings", profileName)
	}
	maxOpenConnections, err := requiredManagedLimit(options, "managed_max_open_conns")
	if err != nil || maxOpenConnections > 8 {
		return fmt.Errorf("%s managed_max_open_conns must be between 1 and 8", profileName)
	}
	for sourceKey, destinationKey := range map[string]string{
		"max_transaction_records":   "managed_max_transaction_rows",
		"max_transaction_bytes":     "managed_max_transaction_bytes",
		"max_transaction_fragments": "managed_max_transaction_fragments",
	} {
		sourceLimit, err := requiredManagedLimit(sourceSpec.Options, sourceKey)
		if err != nil {
			return fmt.Errorf("%s: %w", profileName, err)
		}
		if sourceLimit > destinationLimits[destinationKey] {
			return fmt.Errorf("%s source %s=%d exceeds destination %s=%d", profileName, sourceKey, sourceLimit, destinationKey, destinationLimits[destinationKey])
		}
	}
	if err := snowflakedest.ValidateManagedProfileSpec(destination.Spec); err != nil {
		return fmt.Errorf("%s destination contract: %w", profileName, err)
	}
	return nil
}

func validateManagedSnowflakeStagedAppendAdmission(flowID string, sourceSpec connector.Spec, destination stream.DestinationConfig, bootstrapMode string) error {
	const profileName = connector.ManagedProfilePostgresToSnowflakeStagedAppendV1
	if strings.TrimSpace(destination.Spec.Options["flow_id"]) != flowID {
		return fmt.Errorf("%s destination flow_id %q does not match flow %q", profileName, destination.Spec.Options["flow_id"], flowID)
	}
	if destination.Spec.Type != connector.EndpointSnowflake {
		return fmt.Errorf("%s requires a Snowflake destination", profileName)
	}
	if destinationProfile := strings.TrimSpace(destination.Spec.Options["managed_profile"]); destinationProfile != profileName {
		return fmt.Errorf("destination managed_profile %q does not match source profile %q", destinationProfile, profileName)
	}
	profile := connector.PostgresToSnowflakeStagedAppendV1Profile()
	if err := profile.ValidatePromotion(); err != nil {
		return fmt.Errorf("managed profile contract: %w", err)
	}
	if bootstrapMode != "never" {
		return fmt.Errorf("%s currently requires bootstrap=never; target state must be provisioned before admission", profileName)
	}
	if strings.TrimSpace(sourceSpec.Options["slot"]) != "managed" {
		return fmt.Errorf("%s requires slot=managed so the RunFence derives a flow-incarnation-specific slot", profileName)
	}
	if !parseEnabledOption(sourceSpec.Options["streaming_transactions"], false) {
		return fmt.Errorf("%s requires streaming_transactions=true", profileName)
	}
	if toastFetch := strings.ToLower(strings.TrimSpace(sourceSpec.Options["toast_fetch"])); toastFetch != "off" {
		return fmt.Errorf("%s requires toast_fetch=off; replay must not read future source state", profileName)
	}

	options := destination.Spec.Options
	if err := snowflakedest.ValidateManagedStagedProfileOptions(options); err != nil {
		return fmt.Errorf("%s: %w", profileName, err)
	}
	for key, want := range map[string]string{
		"write_mode": "staged_append", "batch_mode": "target", "batch_resolution": "none",
	} {
		if got := strings.ToLower(strings.TrimSpace(options[key])); got != want {
			return fmt.Errorf("%s requires %s=%s; got %q", profileName, key, want, got)
		}
	}
	if parseEnabledOption(options["meta_table_enabled"], true) {
		return fmt.Errorf("%s requires meta_table_enabled=false; the owned receipt table is the only target metadata object", profileName)
	}
	if parseEnabledOption(options["disable_transactions"], false) {
		return fmt.Errorf("%s requires disable_transactions=false", profileName)
	}
	if parseEnabledOption(options["session_keep_alive"], false) {
		return fmt.Errorf("%s requires session_keep_alive=false", profileName)
	}
	if strings.TrimSpace(options["type_mappings"]) != "" || strings.TrimSpace(options["type_mappings_file"]) != "" {
		return fmt.Errorf("%s rejects type mapping overrides until each mapping has real-service evidence", profileName)
	}
	for _, key := range []string{
		"schema", "table", "staging_schema", "staging_table", "staging_suffix", "warehouse", "warehouse_size",
		"warehouse_auto_suspend", "warehouse_auto_resume", "meta_schema", "meta_table", "meta_pk_prefix",
	} {
		if strings.TrimSpace(options[key]) != "" {
			return fmt.Errorf("%s rejects generic option %s", profileName, key)
		}
	}

	dsnConfig, err := gosnowflake.ParseDSN(strings.TrimSpace(options["dsn"]))
	if err != nil {
		return fmt.Errorf("%s requires a valid Snowflake DSN: %w", profileName, err)
	}
	if !strings.EqualFold(dsnConfig.Protocol, "https") || dsnConfig.DisableOCSPChecks || dsnConfig.OCSPFailOpen != gosnowflake.OCSPFailOpenFalse {
		return fmt.Errorf("%s requires verified HTTPS with OCSP fail-closed", profileName)
	}
	if dsnConfig.Authenticator != gosnowflake.AuthTypeJwt || dsnConfig.PrivateKey == nil {
		return fmt.Errorf("%s requires key-pair JWT authentication", profileName)
	}
	readLatestWrites := false
	clientSessionKeepAlive := false
	timezone := ""
	for key, value := range dsnConfig.Params {
		if value == nil {
			continue
		}
		enabled, _ := strconv.ParseBool(strings.TrimSpace(*value))
		switch {
		case strings.EqualFold(strings.TrimSpace(key), "READ_LATEST_WRITES"):
			readLatestWrites = enabled
		case strings.EqualFold(strings.TrimSpace(key), "CLIENT_SESSION_KEEP_ALIVE"):
			clientSessionKeepAlive = enabled
		case strings.EqualFold(strings.TrimSpace(key), "TIMEZONE"):
			timezone = strings.TrimSpace(*value)
		}
	}
	if !readLatestWrites {
		return fmt.Errorf("%s requires DSN session parameter READ_LATEST_WRITES=true for cross-session receipt reconciliation", profileName)
	}
	if !strings.EqualFold(timezone, "UTC") {
		return fmt.Errorf("%s requires DSN session parameter TIMEZONE=UTC", profileName)
	}
	if clientSessionKeepAlive {
		return fmt.Errorf("%s rejects DSN session parameter CLIENT_SESSION_KEEP_ALIVE=true", profileName)
	}

	for _, identity := range []struct{ option, dsn, label string }{
		{option: "managed_account", dsn: dsnConfig.Account, label: "account"},
		{option: "managed_database", dsn: dsnConfig.Database, label: "database"},
		{option: "managed_schema", dsn: dsnConfig.Schema, label: "schema"},
		{option: "managed_execution_role", dsn: dsnConfig.Role, label: "role"},
		{option: "managed_warehouse", dsn: dsnConfig.Warehouse, label: "warehouse"},
	} {
		configured := strings.TrimSpace(options[identity.option])
		if configured == "" {
			return fmt.Errorf("%s requires %s", profileName, identity.option)
		}
		if !strings.EqualFold(configured, identity.dsn) {
			return fmt.Errorf("%s %s %q does not match DSN %s %q", profileName, identity.option, configured, identity.label, identity.dsn)
		}
	}
	if err := validateManagedSnowflakeRevisionID(options["destination_revision_id"]); err != nil {
		return fmt.Errorf("%s: %w", profileName, err)
	}
	stagedIdentifiers := []string{"managed_database", "managed_schema", "managed_stage", "managed_table", "managed_receipts_table", "managed_file_format", "managed_owner_role", "managed_execution_role", "managed_warehouse"}
	if parseEnabledOption(options["managed_auto_ingest"], false) {
		stagedIdentifiers = append(stagedIdentifiers, "managed_pipe")
	}
	for _, key := range stagedIdentifiers {
		if err := validateManagedSnowflakeIdentifier(key, options[key]); err != nil {
			return fmt.Errorf("%s: %w", profileName, err)
		}
	}
	if options["managed_owner_role"] == options["managed_execution_role"] {
		return fmt.Errorf("%s execution role must not own target objects", profileName)
	}
	requiredCreated := []string{"managed_source_schema", "managed_source_table", "managed_schema_contract", "managed_snowflake_version", "managed_stage_created_on", "managed_target_created_on", "managed_receipts_created_on", "managed_file_format_created_on"}
	if parseEnabledOption(options["managed_auto_ingest"], false) {
		requiredCreated = append(requiredCreated, "managed_pipe_created_on")
	}
	for _, key := range requiredCreated {
		if strings.TrimSpace(options[key]) == "" {
			return fmt.Errorf("%s requires %s", profileName, key)
		}
	}
	var schemaContract connector.Schema
	if err := json.Unmarshal([]byte(options["managed_schema_contract"]), &schemaContract); err != nil {
		return fmt.Errorf("%s managed_schema_contract is invalid JSON: %w", profileName, err)
	}
	if schemaContract.Name != options["managed_source_table"] || schemaContract.Namespace != options["managed_source_schema"] || len(schemaContract.Columns) == 0 {
		return fmt.Errorf("%s managed_schema_contract must exactly identify the configured non-empty source schema", profileName)
	}
	if !isLowerHexDigest(options["managed_schema_contract_hash"]) {
		return fmt.Errorf("%s managed_schema_contract_hash must be 64 lowercase hexadecimal characters", profileName)
	}
	contractHash, err := snowflakedest.ManagedSchemaContractHash(schemaContract)
	if err != nil {
		return fmt.Errorf("%s managed_schema_contract: %w", profileName, err)
	}
	if contractHash != options["managed_schema_contract_hash"] {
		return fmt.Errorf("%s managed_schema_contract_hash does not identify managed_schema_contract", profileName)
	}
	primaryColumns := 0
	for _, column := range schemaContract.Columns {
		if column.TypeMetadata["nullability_known"] != "true" || column.TypeMetadata["generated_known"] != "true" {
			return fmt.Errorf("%s schema column %q requires known nullability and generation status", profileName, column.Name)
		}
		if column.Generated {
			return fmt.Errorf("%s rejects generated source column %q", profileName, column.Name)
		}
		if column.TypeMetadata["primary_key"] == "true" {
			if column.Nullable {
				return fmt.Errorf("%s source primary-key column %q must be NOT NULL", profileName, column.Name)
			}
			primaryColumns++
		}
	}
	if primaryColumns == 0 {
		return fmt.Errorf("%s requires a source primary key", profileName)
	}
	for _, key := range []string{"managed_max_transaction_rows", "managed_max_transaction_bytes", "managed_max_transaction_fragments", "managed_max_open_conns"} {
		if _, err := requiredManagedLimit(options, key); err != nil {
			return fmt.Errorf("%s: %w", profileName, err)
		}
	}
	for sourceKey, destinationKey := range map[string]string{
		"max_transaction_records":   "managed_max_transaction_rows",
		"max_transaction_bytes":     "managed_max_transaction_bytes",
		"max_transaction_fragments": "managed_max_transaction_fragments",
	} {
		sourceLimit, err := requiredManagedLimit(sourceSpec.Options, sourceKey)
		if err != nil {
			return fmt.Errorf("%s: %w", profileName, err)
		}
		destinationLimit, err := requiredManagedLimit(options, destinationKey)
		if err != nil {
			return fmt.Errorf("%s: %w", profileName, err)
		}
		if sourceLimit > destinationLimit {
			return fmt.Errorf("%s source %s=%d exceeds destination %s=%d", profileName, sourceKey, sourceLimit, destinationKey, destinationLimit)
		}
	}
	if err := snowflakedest.ValidateManagedStagedProfileSpec(destination.Spec); err != nil {
		return fmt.Errorf("%s destination contract: %w", profileName, err)
	}
	return nil
}

func validateManagedSnowflakeStreamingAppendAdmission(flowID string, sourceSpec connector.Spec, destination stream.DestinationConfig, bootstrapMode string) error {
	const profileName = connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1
	if strings.TrimSpace(destination.Spec.Options["flow_id"]) != flowID {
		return fmt.Errorf("%s destination flow_id %q does not match flow %q", profileName, destination.Spec.Options["flow_id"], flowID)
	}
	if destination.Spec.Type != connector.EndpointSnowflake {
		return fmt.Errorf("%s requires a Snowflake destination", profileName)
	}
	if destinationProfile := strings.TrimSpace(destination.Spec.Options["managed_profile"]); destinationProfile != profileName {
		return fmt.Errorf("destination managed_profile %q does not match source profile %q", destinationProfile, profileName)
	}
	profile := connector.PostgresToSnowflakeStreamingRestAppendV1Profile()
	if err := profile.ValidatePromotion(); err != nil {
		return fmt.Errorf("managed profile contract: %w", err)
	}
	if bootstrapMode != "never" {
		return fmt.Errorf("%s currently requires bootstrap=never; target state must be provisioned before admission", profileName)
	}
	if strings.TrimSpace(sourceSpec.Options["slot"]) != "managed" {
		return fmt.Errorf("%s requires slot=managed so the RunFence derives a flow-incarnation-specific slot", profileName)
	}
	if !parseEnabledOption(sourceSpec.Options["streaming_transactions"], false) {
		return fmt.Errorf("%s requires streaming_transactions=true", profileName)
	}
	if toastFetch := strings.ToLower(strings.TrimSpace(sourceSpec.Options["toast_fetch"])); toastFetch != "off" {
		return fmt.Errorf("%s requires toast_fetch=off; replay must reconstruct the identical partial after image", profileName)
	}

	options := destination.Spec.Options
	if err := snowflakedest.ValidateManagedStreamingProfileOptions(options); err != nil {
		return fmt.Errorf("%s: %w", profileName, err)
	}
	for key, want := range map[string]string{
		"write_mode": "streaming_append", "batch_mode": "target", "batch_resolution": "none",
	} {
		if got := strings.ToLower(strings.TrimSpace(options[key])); got != want {
			return fmt.Errorf("%s requires %s=%s; got %q", profileName, key, want, got)
		}
	}
	if parseEnabledOption(options["meta_table_enabled"], true) {
		return fmt.Errorf("%s requires meta_table_enabled=false; the owned receipt and channel-state tables are the only target metadata objects", profileName)
	}
	if parseEnabledOption(options["disable_transactions"], false) {
		return fmt.Errorf("%s requires disable_transactions=false", profileName)
	}
	if parseEnabledOption(options["session_keep_alive"], false) {
		return fmt.Errorf("%s requires session_keep_alive=false", profileName)
	}
	if strings.TrimSpace(options["type_mappings"]) != "" || strings.TrimSpace(options["type_mappings_file"]) != "" {
		return fmt.Errorf("%s rejects type mapping overrides until each mapping has real-service evidence", profileName)
	}
	for _, key := range []string{
		"schema", "table", "staging_schema", "staging_table", "staging_suffix", "warehouse", "warehouse_size",
		"warehouse_auto_suspend", "warehouse_auto_resume", "meta_schema", "meta_table", "meta_pk_prefix",
	} {
		if strings.TrimSpace(options[key]) != "" {
			return fmt.Errorf("%s rejects generic option %s", profileName, key)
		}
	}
	if got := strings.TrimSpace(options["managed_streaming_transport"]); got == "" {
		return fmt.Errorf("%s requires managed_streaming_transport naming the reviewed high-performance append transport", profileName)
	}

	dsnConfig, err := gosnowflake.ParseDSN(strings.TrimSpace(options["dsn"]))
	if err != nil {
		return fmt.Errorf("%s requires a valid Snowflake DSN: %w", profileName, err)
	}
	if !strings.EqualFold(dsnConfig.Protocol, "https") || dsnConfig.DisableOCSPChecks || dsnConfig.OCSPFailOpen != gosnowflake.OCSPFailOpenFalse {
		return fmt.Errorf("%s requires verified HTTPS with OCSP fail-closed", profileName)
	}
	if dsnConfig.Authenticator != gosnowflake.AuthTypeJwt || dsnConfig.PrivateKey == nil {
		return fmt.Errorf("%s requires key-pair JWT authentication", profileName)
	}
	readLatestWrites := false
	clientSessionKeepAlive := false
	timezone := ""
	for key, value := range dsnConfig.Params {
		if value == nil {
			continue
		}
		enabled, _ := strconv.ParseBool(strings.TrimSpace(*value))
		switch {
		case strings.EqualFold(strings.TrimSpace(key), "READ_LATEST_WRITES"):
			readLatestWrites = enabled
		case strings.EqualFold(strings.TrimSpace(key), "CLIENT_SESSION_KEEP_ALIVE"):
			clientSessionKeepAlive = enabled
		case strings.EqualFold(strings.TrimSpace(key), "TIMEZONE"):
			timezone = strings.TrimSpace(*value)
		}
	}
	if !readLatestWrites {
		return fmt.Errorf("%s requires DSN session parameter READ_LATEST_WRITES=true for cross-session observed-row and receipt reconciliation", profileName)
	}
	if !strings.EqualFold(timezone, "UTC") {
		return fmt.Errorf("%s requires DSN session parameter TIMEZONE=UTC", profileName)
	}
	if clientSessionKeepAlive {
		return fmt.Errorf("%s rejects DSN session parameter CLIENT_SESSION_KEEP_ALIVE=true", profileName)
	}

	for _, identity := range []struct{ option, dsn, label string }{
		{option: "managed_account", dsn: dsnConfig.Account, label: "account"},
		{option: "managed_database", dsn: dsnConfig.Database, label: "database"},
		{option: "managed_schema", dsn: dsnConfig.Schema, label: "schema"},
		{option: "managed_execution_role", dsn: dsnConfig.Role, label: "role"},
		{option: "managed_warehouse", dsn: dsnConfig.Warehouse, label: "warehouse"},
	} {
		configured := strings.TrimSpace(options[identity.option])
		if configured == "" {
			return fmt.Errorf("%s requires %s", profileName, identity.option)
		}
		if !strings.EqualFold(configured, identity.dsn) {
			return fmt.Errorf("%s %s %q does not match DSN %s %q", profileName, identity.option, configured, identity.label, identity.dsn)
		}
	}
	if err := validateManagedSnowflakeRevisionID(options["destination_revision_id"]); err != nil {
		return fmt.Errorf("%s: %w", profileName, err)
	}
	for _, key := range []string{"managed_database", "managed_schema", "managed_pipe", "managed_table", "managed_receipts_table", "managed_channel_state_table", "managed_owner_role", "managed_execution_role", "managed_warehouse"} {
		if err := validateManagedSnowflakeIdentifier(key, options[key]); err != nil {
			return fmt.Errorf("%s: %w", profileName, err)
		}
	}
	if options["managed_owner_role"] == options["managed_execution_role"] {
		return fmt.Errorf("%s execution role must not own target objects", profileName)
	}
	for _, key := range []string{"managed_source_schema", "managed_source_table", "managed_schema_contract", "managed_snowflake_version", "managed_pipe_created_on", "managed_target_created_on", "managed_receipts_created_on", "managed_channel_state_created_on"} {
		if strings.TrimSpace(options[key]) == "" {
			return fmt.Errorf("%s requires %s", profileName, key)
		}
	}
	var schemaContract connector.Schema
	if err := json.Unmarshal([]byte(options["managed_schema_contract"]), &schemaContract); err != nil {
		return fmt.Errorf("%s managed_schema_contract is invalid JSON: %w", profileName, err)
	}
	if schemaContract.Name != options["managed_source_table"] || schemaContract.Namespace != options["managed_source_schema"] || len(schemaContract.Columns) == 0 {
		return fmt.Errorf("%s managed_schema_contract must exactly identify the configured non-empty source schema", profileName)
	}
	if !isLowerHexDigest(options["managed_schema_contract_hash"]) {
		return fmt.Errorf("%s managed_schema_contract_hash must be 64 lowercase hexadecimal characters", profileName)
	}
	contractHash, err := snowflakedest.ManagedSchemaContractHash(schemaContract)
	if err != nil {
		return fmt.Errorf("%s managed_schema_contract: %w", profileName, err)
	}
	if contractHash != options["managed_schema_contract_hash"] {
		return fmt.Errorf("%s managed_schema_contract_hash does not identify managed_schema_contract", profileName)
	}
	primaryColumns := 0
	for _, column := range schemaContract.Columns {
		if column.TypeMetadata["nullability_known"] != "true" || column.TypeMetadata["generated_known"] != "true" {
			return fmt.Errorf("%s schema column %q requires known nullability and generation status", profileName, column.Name)
		}
		if column.Generated {
			return fmt.Errorf("%s rejects generated source column %q", profileName, column.Name)
		}
		if column.TypeMetadata["primary_key"] == "true" {
			if column.Nullable {
				return fmt.Errorf("%s source primary-key column %q must be NOT NULL", profileName, column.Name)
			}
			primaryColumns++
		}
	}
	if primaryColumns == 0 {
		return fmt.Errorf("%s requires a source primary key", profileName)
	}
	for _, key := range []string{"managed_max_transaction_rows", "managed_max_transaction_bytes", "managed_max_transaction_fragments", "managed_max_row_bytes", "managed_max_open_conns"} {
		if _, err := requiredManagedLimit(options, key); err != nil {
			return fmt.Errorf("%s: %w", profileName, err)
		}
	}
	for sourceKey, destinationKey := range map[string]string{
		"max_transaction_records":   "managed_max_transaction_rows",
		"max_transaction_bytes":     "managed_max_transaction_bytes",
		"max_transaction_fragments": "managed_max_transaction_fragments",
	} {
		sourceLimit, err := requiredManagedLimit(sourceSpec.Options, sourceKey)
		if err != nil {
			return fmt.Errorf("%s: %w", profileName, err)
		}
		destinationLimit, err := requiredManagedLimit(options, destinationKey)
		if err != nil {
			return fmt.Errorf("%s: %w", profileName, err)
		}
		if sourceLimit > destinationLimit {
			return fmt.Errorf("%s source %s=%d exceeds destination %s=%d", profileName, sourceKey, sourceLimit, destinationKey, destinationLimit)
		}
	}
	if err := snowflakedest.ValidateManagedStreamingProfileSpec(destination.Spec); err != nil {
		return fmt.Errorf("%s destination contract: %w", profileName, err)
	}
	// Fail closed: the entire admission contract is executable and satisfied, but
	// no reviewed high-performance Snowpipe Streaming append transport is linked.
	// Rather than proving delivery from local continuation/offset tokens, admission
	// is refused here. This is the exact executable admission boundary: startup
	// fails closed until a reviewed transport is linked and its live matrix passes.
	if !snowflakedest.ManagedStreamingTransportAvailable() {
		return fmt.Errorf("%s: %w", profileName, snowflakedest.ErrManagedStreamingTransportUnavailable)
	}
	return nil
}

func validateManagedSnowflakeRevisionID(raw string) error {
	value := strings.TrimSpace(raw)
	if value == "" || len(value) > 128 {
		return errors.New("destination_revision_id must be a 1-128 character identifier")
	}
	for _, character := range value {
		if (character < 'A' || character > 'Z') && (character < 'a' || character > 'z') &&
			(character < '0' || character > '9') && character != '-' && character != '_' && character != '.' {
			return errors.New("destination_revision_id may contain only letters, digits, dash, underscore, and dot")
		}
	}
	return nil
}

func validateManagedSnowflakeIdentifier(name, raw string) error {
	value := strings.TrimSpace(raw)
	if value == "" {
		return fmt.Errorf("%s is required", name)
	}
	if len(value) > 255 || strings.ToUpper(value) != value || ((value[0] < 'A' || value[0] > 'Z') && value[0] != '_') {
		return fmt.Errorf("%s must be one unquoted uppercase identifier", name)
	}
	for _, character := range value[1:] {
		if (character < 'A' || character > 'Z') && (character < '0' || character > '9') && character != '_' && character != '$' {
			return fmt.Errorf("%s must be one unquoted uppercase identifier", name)
		}
	}
	return nil
}

func isLowerHexDigest(raw string) bool {
	if len(raw) != 64 || strings.ToLower(raw) != raw {
		return false
	}
	_, err := hex.DecodeString(raw)
	return err == nil
}

func requiredManagedLimit(options map[string]string, key string) (uint64, error) {
	raw := strings.TrimSpace(options[key])
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value == 0 {
		return 0, fmt.Errorf("%s must be an explicit positive integer", key)
	}
	return value, nil
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
