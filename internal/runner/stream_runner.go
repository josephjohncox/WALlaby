package runner

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	snowflakedest "github.com/josephjohncox/wallaby/connectors/destinations/snowflake"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/tablemap"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"go.opentelemetry.io/otel/trace"
)

// StreamRunnerConfig contains process-level defaults and dependencies used to
// construct a stream runner for a flow.
type StreamRunnerConfig struct {
	Checkpoints         connector.CheckpointStore
	Tracer              trace.Tracer
	Meters              *telemetry.Meters
	DefaultWireFormat   connector.WireFormat
	StrictFormat        bool
	MaxEmptyReads       int
	DefaultParallelism  int
	ResolveStaging      bool
	DDLExecutions       stream.DDLExecutionStore
	TraceSink           stream.TraceSink
	RunFence            *authority.RunFence
	DeliveryCoordinator stream.ManagedDeliveryCoordinator
	ArtifactLog         stream.ManagedArtifactLog
}

// NewStreamRunner constructs a stream runner without mutating the flow or
// destination configuration supplied by the caller. Construction fails unless
// the flow has durable checkpoint storage and a stable identity.
func NewStreamRunner(f flow.Flow, source connector.Source, destinations []stream.DestinationConfig, cfg StreamRunnerConfig) (stream.Runner, error) {
	sourceSpec := cloneSpec(f.Source)
	if sourceSpec.Type == connector.EndpointPostgres {
		if sourceSpec.Options == nil {
			sourceSpec.Options = make(map[string]string)
		}
		if sourceSpec.Options["flow_id"] == "" {
			sourceSpec.Options["flow_id"] = f.ID
		}
	}
	if cfg.MaxEmptyReads > 0 {
		if sourceSpec.Options == nil {
			sourceSpec.Options = make(map[string]string)
		}
		if sourceSpec.Options["emit_empty"] == "" {
			sourceSpec.Options["emit_empty"] = "true"
		}
	}

	if len(destinations) > 0 {
		if err := flow.ValidateDefinition(f); err != nil {
			return stream.Runner{}, fmt.Errorf("validate flow definition: %w", err)
		}
	}
	clonedDestinations := make([]stream.DestinationConfig, len(destinations))
	for i, destination := range destinations {
		clonedDestinations[i] = destination
		clonedDestinations[i].Spec = cloneSpec(destination.Spec)
		if clonedDestinations[i].Spec.Options == nil {
			clonedDestinations[i].Spec.Options = map[string]string{}
		}
		if configuredFlow := strings.TrimSpace(clonedDestinations[i].Spec.Options["flow_id"]); configuredFlow != "" && configuredFlow != f.ID {
			return stream.Runner{}, fmt.Errorf("destination %s flow_id %q does not match flow %q", destination.Spec.Name, configuredFlow, f.ID)
		}
		clonedDestinations[i].Spec.Options["flow_id"] = f.ID
		projector, err := tablemap.New(f.Config.TableMappings, destination.Spec.Name)
		if err != nil {
			return stream.Runner{}, fmt.Errorf("build table projector for destination %s: %w", destination.Spec.Name, err)
		}
		clonedDestinations[i].Projector = projector
		clonedDestinations[i].MappingFingerprint = projector.Fingerprint()
		mapping, _ := f.Config.TableMappings.ForDestination(destination.Spec.Name)
		if err := projectManagedSnowflakeContract(&clonedDestinations[i].Spec, mapping, projector); err != nil {
			return stream.Runner{}, fmt.Errorf("project managed Snowflake contract for destination %s: %w", destination.Spec.Name, err)
		}
		if err := validateDestinationTableWrites(destination, mapping); err != nil {
			return stream.Runner{}, fmt.Errorf("validate table writes for destination %s: %w", destination.Spec.Name, err)
		}
	}

	wireFormat := f.WireFormat
	if wireFormat == "" {
		wireFormat = cfg.DefaultWireFormat
	}
	parallelism := f.Parallelism
	if parallelism <= 0 {
		parallelism = cfg.DefaultParallelism
	}

	if cfg.Checkpoints == nil {
		return stream.Runner{}, fmt.Errorf("streaming requires a durable checkpoint store before source acknowledgement")
	}
	if f.ID == "" {
		return stream.Runner{}, fmt.Errorf("streaming requires a non-empty flow id for durable checkpoints")
	}

	requireDDLExecution := f.Config.DDL.AutoApply != nil && *f.Config.DDL.AutoApply
	if f.Config.AckPolicy == stream.AckPolicyMaterialized && !connector.IsManagedSourceSpec(sourceSpec) {
		return stream.Runner{}, fmt.Errorf("ack_policy=materialized requires managed PostgreSQL transactional execution")
	}
	if connector.IsManagedSourceSpec(sourceSpec) {
		if err := validateManagedAdmission(f, source, sourceSpec, clonedDestinations, cfg); err != nil {
			return stream.Runner{}, err
		}
	}

	var checkpointOutbox connector.CheckpointOutboxStore
	if f.Config.AckPolicy == stream.AckPolicyPrimary {
		store, ok := cfg.Checkpoints.(connector.CheckpointOutboxStore)
		if !ok {
			return stream.Runner{}, fmt.Errorf("primary acknowledgement requires a durable checkpoint store with atomic outbox support")
		}
		checkpointOutbox = store
	}

	if err := stream.ValidateDestinationContracts(
		clonedDestinations,
		f.Config.AckPolicy,
		f.Config.PrimaryDestination,
		requireDDLExecution,
	); err != nil {
		return stream.Runner{}, fmt.Errorf("validate flow destination contracts: %w", err)
	}
	if requireDDLExecution && cfg.DDLExecutions == nil {
		return stream.Runner{}, fmt.Errorf("automatic DDL execution requires durable execution receipt storage")
	}
	return stream.Runner{
		Source:              source,
		SourceSpec:          sourceSpec,
		Destinations:        clonedDestinations,
		Checkpoints:         cfg.Checkpoints,
		CheckpointOutbox:    checkpointOutbox,
		FlowID:              f.ID,
		ResolveStaging:      cfg.ResolveStaging,
		Tracer:              cfg.Tracer,
		Meters:              cfg.Meters,
		MaxEmptyReads:       cfg.MaxEmptyReads,
		WireFormat:          wireFormat,
		StrictFormat:        cfg.StrictFormat,
		Parallelism:         parallelism,
		AckPolicy:           f.Config.AckPolicy,
		PrimaryDestination:  f.Config.PrimaryDestination,
		RequireDDLExecution: requireDDLExecution,
		FailureMode:         f.Config.FailureMode,
		GiveUpPolicy:        f.Config.GiveUpPolicy,
		DDLExecutions:       cfg.DDLExecutions,
		TraceSink:           cfg.TraceSink,
		RunFence:            cfg.RunFence,
		DeliveryCoordinator: cfg.DeliveryCoordinator,
		ArtifactLog:         cfg.ArtifactLog,
	}, nil
}

func projectManagedSnowflakeContract(spec *connector.Spec, mapping flow.DestinationTableMappings, projector *tablemap.Projector) error {
	if strings.TrimSpace(spec.Options["managed_profile"]) != connector.ManagedProfilePostgresToSnowflakeSQLV1 {
		return nil
	}
	if mapping.FutureTables.Action != flow.MappingActionExclude {
		return errors.New("managed Snowflake SQL requires future_tables=exclude")
	}
	included := 0
	var admitted flow.TableMapping
	for _, table := range mapping.Tables {
		if table.Action != flow.MappingActionInclude {
			continue
		}
		included++
		admitted = table
		if table.Write.Mode != flow.TableWriteModeUpsert {
			return fmt.Errorf("managed Snowflake SQL relation %s.%s requires upsert", table.SourceSchema, table.SourceTable)
		}
		if table.Write.WatermarkColumn != "" {
			return fmt.Errorf("managed Snowflake SQL relation %s.%s does not support watermark", table.SourceSchema, table.SourceTable)
		}
	}
	if included != 1 {
		return fmt.Errorf("managed Snowflake SQL requires exactly one admitted relation mapping, got %d", included)
	}
	if !isLowerHexDigest(spec.Options["managed_schema_contract_hash"]) {
		return errors.New("managed_schema_contract_hash must be 64 lowercase hexadecimal characters")
	}
	var source connector.Schema
	if err := json.Unmarshal([]byte(spec.Options["managed_schema_contract"]), &source); err != nil {
		return fmt.Errorf("decode managed_schema_contract: %w", err)
	}
	sourceHash, err := snowflakedest.ManagedSchemaContractHash(source)
	if err != nil {
		return err
	}
	if sourceHash != spec.Options["managed_schema_contract_hash"] {
		return errors.New("managed_schema_contract_hash does not identify persisted managed_schema_contract")
	}
	sourcePrimary, err := orderedManagedSnowflakePrimaryKey(source, "source")
	if err != nil {
		return err
	}
	if !slices.Equal(admitted.Write.KeyColumns, sourcePrimary) {
		return fmt.Errorf("managed Snowflake key_columns %v must equal complete ordered source primary key %v", admitted.Write.KeyColumns, sourcePrimary)
	}
	mapped, policy, ok, err := projector.ProjectBootstrapSchema(source)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("managed Snowflake source relation is excluded by its destination mapping")
	}
	if policy.Mode != connector.ResolvedWriteUpsert || policy.WatermarkColumn != "" {
		return errors.New("managed Snowflake projected policy must be upsert without watermark")
	}
	projectedPrimary, err := orderedManagedSnowflakePrimaryKey(mapped, "projected")
	if err != nil {
		return err
	}
	if len(projectedPrimary) != len(sourcePrimary) {
		return fmt.Errorf("managed Snowflake projection must preserve every source primary-key component: source %v projected %v", sourcePrimary, projectedPrimary)
	}
	if !slices.Equal(policy.KeyColumns, projectedPrimary) {
		return fmt.Errorf("managed Snowflake projected key columns %v must equal complete projected primary key %v", policy.KeyColumns, projectedPrimary)
	}
	if err := validateManagedSnowflakeIdentifier("managed_schema", spec.Options["managed_schema"]); err != nil {
		return err
	}
	if err := validateManagedSnowflakeIdentifier("managed_table", spec.Options["managed_table"]); err != nil {
		return err
	}
	if mapped.Namespace != spec.Options["managed_schema"] || mapped.Name != spec.Options["managed_table"] {
		return fmt.Errorf("managed Snowflake mapped target %s.%s must equal provisioned target %s.%s", mapped.Namespace, mapped.Name, spec.Options["managed_schema"], spec.Options["managed_table"])
	}
	encoded, err := json.Marshal(mapped)
	if err != nil {
		return err
	}
	hash, err := snowflakedest.ManagedSchemaContractHash(mapped)
	if err != nil {
		return err
	}
	spec.Options["managed_schema_contract"] = string(encoded)
	spec.Options["managed_schema_contract_hash"] = hash
	return nil
}

func orderedManagedSnowflakePrimaryKey(schema connector.Schema, label string) ([]string, error) {
	type component struct {
		name    string
		ordinal int
	}
	components := make([]component, 0)
	for _, column := range schema.Columns {
		if column.TypeMetadata["primary_key"] != "true" {
			continue
		}
		ordinal, err := strconv.Atoi(column.TypeMetadata["primary_key_ordinal"])
		if err != nil || ordinal <= 0 {
			return nil, fmt.Errorf("managed Snowflake %s primary key column %q has invalid ordinal", label, column.Name)
		}
		components = append(components, component{name: column.Name, ordinal: ordinal})
	}
	if len(components) == 0 {
		return nil, fmt.Errorf("managed Snowflake %s schema contract requires a complete ordered primary key", label)
	}
	sort.Slice(components, func(i, j int) bool { return components[i].ordinal < components[j].ordinal })
	names := make([]string, len(components))
	for i, component := range components {
		if component.ordinal != i+1 {
			return nil, fmt.Errorf("managed Snowflake %s primary key ordinals must be complete and contiguous from 1", label)
		}
		names[i] = component.name
	}
	return names, nil
}

func validateDestinationTableWrites(destination stream.DestinationConfig, mapping flow.DestinationTableMappings) error {
	if destination.Spec.Type == "" {
		return nil
	}
	if _, artifactOnly := destination.Dest.(connector.CanonicalArtifactDestination); artifactOnly {
		return nil
	}
	capabilities := connector.ResolveDestinationCapabilities(destination.Dest, destination.Spec)
	if !capabilities.TableWrites.Declared && !capabilities.Delivery.Declared {
		return nil
	}
	validate := func(write flow.TableWritePolicy) error {
		return capabilities.SupportsTablePolicy(connector.TableWritePolicy{
			Mode: connector.ResolvedWriteMode(write.Mode), KeyColumns: append([]string(nil), write.KeyColumns...), WatermarkColumn: write.WatermarkColumn,
		})
	}
	if mapping.FutureTables.Action == flow.MappingActionInclude {
		if err := validate(mapping.FutureTables.Write); err != nil {
			return fmt.Errorf("future tables: %w", err)
		}
	}
	for _, table := range mapping.Tables {
		if table.Action != flow.MappingActionInclude {
			continue
		}
		if err := validate(table.Write); err != nil {
			return fmt.Errorf("table %s.%s: %w", table.SourceSchema, table.SourceTable, err)
		}
	}
	return nil
}

func cloneSpec(spec connector.Spec) connector.Spec {
	clone := spec
	if spec.Options != nil {
		clone.Options = make(map[string]string, len(spec.Options))
		for key, value := range spec.Options {
			clone.Options[key] = value
		}
	}
	return clone
}
