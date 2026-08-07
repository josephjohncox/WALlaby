// Package endpointcodec is the single conversion boundary between persisted,
// typed API endpoints and the string-valued connector.RuntimeSpec runtime adapter.
package endpointcodec

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/options"
	"github.com/josephjohncox/wallaby/internal/typemapping"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Role is the role an endpoint occupies in a flow.
type Role uint8

const (
	RoleSource Role = iota + 1
	RoleDestination
)

// Decode converts a typed endpoint to a runtime connector specification.
// The returned option map never aliases protobuf-owned memory.
func Decode(endpoint *wallabypb.Endpoint, role Role) (connector.RuntimeSpec, error) {
	return DecodeWithRegistry(endpoint, role, connector.DefaultRegistry)
}

// DecodeWithRegistry converts and validates custom endpoint registration
// against the same registry that workers use for construction.
func DecodeWithRegistry(endpoint *wallabypb.Endpoint, role Role, registry *connector.Registry) (connector.RuntimeSpec, error) {
	if endpoint == nil {
		return connector.RuntimeSpec{}, errors.New("endpoint is required")
	}
	if err := validateProtoValue(endpoint.ProtoReflect(), "endpoint", true); err != nil {
		return connector.RuntimeSpec{}, err
	}
	if strings.TrimSpace(endpoint.GetName()) != endpoint.GetName() {
		return connector.RuntimeSpec{}, errors.New("endpoint name must not have surrounding whitespace")
	}
	if endpoint.GetConfig() == nil {
		return connector.RuntimeSpec{}, errors.New("endpoint config branch is required")
	}
	options := make(map[string]string)
	spec := connector.RuntimeSpec{Name: endpoint.GetName(), Options: options}
	var err error
	switch cfg := endpoint.GetConfig().(type) {
	case *wallabypb.Endpoint_PostgresSource:
		if role != RoleSource {
			return connector.RuntimeSpec{}, errors.New("postgres_source is only valid for a source endpoint")
		}
		spec.Type = connector.EndpointPostgres
		err = encodePostgresSource(options, cfg.PostgresSource)
	case *wallabypb.Endpoint_PostgresDestination:
		if role != RoleDestination {
			return connector.RuntimeSpec{}, errors.New("postgres_destination is only valid for a destination endpoint")
		}
		spec.Type = connector.EndpointPostgres
		err = encodePostgresDestination(options, cfg.PostgresDestination)
	case *wallabypb.Endpoint_Pgstream:
		err = destinationOnly(role, "pgstream")
		spec.Type = connector.EndpointPGStream
		if err == nil {
			err = encodePGStream(options, cfg.Pgstream)
		}
	case *wallabypb.Endpoint_Kafka:
		err = destinationOnly(role, "kafka")
		spec.Type = connector.EndpointKafka
		if err == nil {
			err = encodeKafka(options, cfg.Kafka)
		}
	case *wallabypb.Endpoint_Redpanda:
		err = destinationOnly(role, "redpanda")
		spec.Type = connector.EndpointRedpanda
		if err == nil {
			err = encodeKafka(options, cfg.Redpanda.GetKafka())
		}
	case *wallabypb.Endpoint_S3:
		err = destinationOnly(role, "s3")
		spec.Type = connector.EndpointS3
		if err == nil {
			err = encodeS3(options, cfg.S3)
		}
	case *wallabypb.Endpoint_Http:
		err = destinationOnly(role, "http")
		spec.Type = connector.EndpointHTTP
		if err == nil {
			err = encodeHTTP(options, cfg.Http)
		}
	case *wallabypb.Endpoint_Grpc:
		err = destinationOnly(role, "grpc")
		spec.Type = connector.EndpointGRPC
		if err == nil {
			err = encodeGRPC(options, cfg.Grpc)
		}
	case *wallabypb.Endpoint_Snowflake:
		err = destinationOnly(role, "snowflake")
		spec.Type = connector.EndpointSnowflake
		if err == nil {
			err = encodeSnowflake(options, cfg.Snowflake)
		}
	case *wallabypb.Endpoint_SnowflakePostgresSql:
		err = destinationOnly(role, "snowflake_postgres_sql")
		spec.Type = connector.EndpointSnowflake
		if err == nil {
			options["managed_profile"] = connector.ManagedProfilePostgresToSnowflakeSQLV1
			err = encodeSnowflakeSQL(options, cfg.SnowflakePostgresSql)
		}
	case *wallabypb.Endpoint_SnowflakePostgresStaged:
		err = destinationOnly(role, "snowflake_postgres_staged")
		spec.Type = connector.EndpointSnowflake
		if err == nil {
			options["managed_profile"] = connector.ManagedProfilePostgresToSnowflakeStagedAppendV1
			err = encodeSnowflakeStaged(options, cfg.SnowflakePostgresStaged)
		}
	case *wallabypb.Endpoint_SnowflakePostgresStreaming:
		err = destinationOnly(role, "snowflake_postgres_streaming")
		spec.Type = connector.EndpointSnowflake
		if err == nil {
			options["managed_profile"] = connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1
			err = encodeSnowflakeStreaming(options, cfg.SnowflakePostgresStreaming)
		}
	case *wallabypb.Endpoint_Snowpipe:
		err = destinationOnly(role, "snowpipe")
		spec.Type = connector.EndpointSnowpipe
		if err == nil {
			err = encodeSnowpipe(options, cfg.Snowpipe)
		}
	case *wallabypb.Endpoint_Clickhouse:
		err = destinationOnly(role, "clickhouse")
		spec.Type = connector.EndpointClickHouse
		if err == nil {
			err = encodeClickHouse(options, cfg.Clickhouse)
		}
	case *wallabypb.Endpoint_ClickhousePostgresAppend:
		err = destinationOnly(role, "clickhouse_postgres_append")
		spec.Type = connector.EndpointClickHouse
		if err == nil {
			options["managed_profile"] = connector.ManagedProfilePostgresToClickHouseAppendV1
			err = encodeClickHouseManaged(options, cfg.ClickhousePostgresAppend)
		}
	case *wallabypb.Endpoint_Duckdb:
		err = destinationOnly(role, "duckdb")
		spec.Type = connector.EndpointDuckDB
		if err == nil {
			err = encodeDuckDB(options, cfg.Duckdb)
		}
	case *wallabypb.Endpoint_Ducklake:
		err = destinationOnly(role, "ducklake")
		spec.Type = connector.EndpointDuckLake
		if err == nil {
			err = encodeDuckLake(options, cfg.Ducklake)
		}
	case *wallabypb.Endpoint_Iceberg:
		err = destinationOnly(role, "iceberg")
		spec.Type = connector.EndpointIceberg
		if err == nil {
			err = encodeIceberg(options, cfg.Iceberg)
		}
	case *wallabypb.Endpoint_Custom:
		custom := cfg.Custom
		if custom == nil || custom.GetConnectorType() == "" {
			return connector.RuntimeSpec{}, errors.New("custom.connector_type is required")
		}
		if custom.GetConnectorType() != strings.TrimSpace(custom.GetConnectorType()) {
			return connector.RuntimeSpec{}, errors.New("custom.connector_type must not contain surrounding whitespace")
		}
		connectorType := connector.EndpointType(custom.GetConnectorType())
		if connector.IsBuiltinEndpointType(connectorType) {
			return connector.RuntimeSpec{}, fmt.Errorf("custom connector type %q collides with a built-in connector", connectorType)
		}
		if role == RoleSource {
			if registry == nil || !registry.HasSource(connectorType) {
				return connector.RuntimeSpec{}, fmt.Errorf("custom source connector %q is not registered", connectorType)
			}
		} else if registry == nil || !registry.HasDestination(connectorType) {
			return connector.RuntimeSpec{}, fmt.Errorf("custom destination connector %q is not registered", connectorType)
		}
		spec.Type = connectorType
		spec.Options = cloneMap(custom.GetOptions())
	default:
		return connector.RuntimeSpec{}, fmt.Errorf("unsupported endpoint config branch %T", cfg)
	}
	if err != nil {
		return connector.RuntimeSpec{}, err
	}
	return spec, nil
}

func destinationOnly(role Role, branch string) error {
	if role != RoleDestination {
		return fmt.Errorf("%s is only valid for a destination endpoint", branch)
	}
	return nil
}

// Encode converts a persisted runtime specification back to its typed endpoint.
// Runtime-only enrichment keys are rejected rather than silently persisted.
func Encode(spec connector.RuntimeSpec, role Role) (*wallabypb.Endpoint, error) {
	values := cloneMap(spec.Options)
	endpoint := &wallabypb.Endpoint{Name: spec.Name}
	var err error
	switch spec.Type {
	case connector.EndpointPostgres:
		if role == RoleSource {
			var cfg *wallabypb.PostgresSourceConfig
			cfg, err = decodePostgresSource(values)
			endpoint.Config = &wallabypb.Endpoint_PostgresSource{PostgresSource: cfg}
		} else {
			var cfg *wallabypb.PostgresDestinationConfig
			cfg, err = decodePostgresDestination(values)
			endpoint.Config = &wallabypb.Endpoint_PostgresDestination{PostgresDestination: cfg}
		}
	case connector.EndpointPGStream:
		var cfg *wallabypb.PGStreamDestinationConfig
		cfg, err = decodePGStream(values)
		endpoint.Config = &wallabypb.Endpoint_Pgstream{Pgstream: cfg}
	case connector.EndpointKafka, connector.EndpointRedpanda:
		var cfg *wallabypb.KafkaDestinationConfig
		cfg, err = decodeKafka(values)
		if spec.Type == connector.EndpointKafka {
			endpoint.Config = &wallabypb.Endpoint_Kafka{Kafka: cfg}
		} else {
			endpoint.Config = &wallabypb.Endpoint_Redpanda{Redpanda: &wallabypb.RedpandaDestinationConfig{Kafka: cfg}}
		}
	case connector.EndpointS3:
		var cfg *wallabypb.S3DestinationConfig
		cfg, err = decodeS3(values)
		endpoint.Config = &wallabypb.Endpoint_S3{S3: cfg}
	case connector.EndpointHTTP:
		var cfg *wallabypb.HTTPDestinationConfig
		cfg, err = decodeHTTP(values)
		endpoint.Config = &wallabypb.Endpoint_Http{Http: cfg}
	case connector.EndpointGRPC:
		var cfg *wallabypb.GRPCDestinationConfig
		cfg, err = decodeGRPC(values)
		endpoint.Config = &wallabypb.Endpoint_Grpc{Grpc: cfg}
	case connector.EndpointSnowflake:
		profile := take(values, "managed_profile")
		switch profile {
		case "":
			var cfg *wallabypb.SnowflakeDestinationConfig
			cfg, err = decodeSnowflake(values)
			endpoint.Config = &wallabypb.Endpoint_Snowflake{Snowflake: cfg}
		case connector.ManagedProfilePostgresToSnowflakeSQLV1:
			var cfg *wallabypb.SnowflakePostgresSQLConfig
			cfg, err = decodeSnowflakeSQL(values)
			endpoint.Config = &wallabypb.Endpoint_SnowflakePostgresSql{SnowflakePostgresSql: cfg}
		case connector.ManagedProfilePostgresToSnowflakeStagedAppendV1:
			var cfg *wallabypb.SnowflakePostgresStagedConfig
			cfg, err = decodeSnowflakeStaged(values)
			endpoint.Config = &wallabypb.Endpoint_SnowflakePostgresStaged{SnowflakePostgresStaged: cfg}
		case connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1:
			var cfg *wallabypb.SnowflakePostgresStreamingConfig
			cfg, err = decodeSnowflakeStreaming(values)
			endpoint.Config = &wallabypb.Endpoint_SnowflakePostgresStreaming{SnowflakePostgresStreaming: cfg}
		default:
			err = fmt.Errorf("unsupported snowflake managed_profile %q", profile)
		}
	case connector.EndpointSnowpipe:
		var cfg *wallabypb.SnowpipeDestinationConfig
		cfg, err = decodeSnowpipe(values)
		endpoint.Config = &wallabypb.Endpoint_Snowpipe{Snowpipe: cfg}
	case connector.EndpointClickHouse:
		profile := take(values, "managed_profile")
		switch profile {
		case "":
			var cfg *wallabypb.ClickHouseDestinationConfig
			cfg, err = decodeClickHouse(values)
			endpoint.Config = &wallabypb.Endpoint_Clickhouse{Clickhouse: cfg}
		case connector.ManagedProfilePostgresToClickHouseAppendV1:
			var cfg *wallabypb.ClickHousePostgresAppendConfig
			cfg, err = decodeClickHouseManaged(values)
			endpoint.Config = &wallabypb.Endpoint_ClickhousePostgresAppend{ClickhousePostgresAppend: cfg}
		default:
			err = fmt.Errorf("unsupported clickhouse managed_profile %q", profile)
		}
	case connector.EndpointDuckDB:
		var cfg *wallabypb.DuckDBDestinationConfig
		cfg, err = decodeDuckDB(values)
		endpoint.Config = &wallabypb.Endpoint_Duckdb{Duckdb: cfg}
	case connector.EndpointDuckLake:
		var cfg *wallabypb.DuckLakeDestinationConfig
		cfg, err = decodeDuckLake(values)
		endpoint.Config = &wallabypb.Endpoint_Ducklake{Ducklake: cfg}
	case connector.EndpointIceberg:
		var cfg *wallabypb.IcebergDestinationConfig
		cfg, err = decodeIceberg(values)
		endpoint.Config = &wallabypb.Endpoint_Iceberg{Iceberg: cfg}
	default:
		if connector.IsBuiltinEndpointType(spec.Type) {
			return nil, fmt.Errorf("built-in connector %q has no public endpoint branch", spec.Type)
		}
		endpoint.Config = &wallabypb.Endpoint_Custom{Custom: &wallabypb.CustomEndpointConfig{ConnectorType: string(spec.Type), Options: values}}
		values = nil
	}
	if err != nil {
		return nil, err
	}
	if role == RoleSource {
		if _, ok := endpoint.Config.(*wallabypb.Endpoint_PostgresSource); !ok {
			if _, custom := endpoint.Config.(*wallabypb.Endpoint_Custom); !custom {
				return nil, fmt.Errorf("connector %q cannot be used as a source", spec.Type)
			}
		}
	}
	if len(values) != 0 {
		return nil, fmt.Errorf("connector %q contains non-persistable or unknown options: %s", spec.Type, strings.Join(sortedKeys(values), ", "))
	}
	return endpoint, nil
}

// Clone returns a deep protobuf clone.
func Clone(endpoint *wallabypb.Endpoint) *wallabypb.Endpoint {
	if endpoint == nil {
		return nil
	}
	return proto.Clone(endpoint).(*wallabypb.Endpoint)
}

func validateProtoValue(message protoreflect.Message, path string, root bool) error {
	fields := message.Descriptor().Fields()
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		if !message.Has(field) {
			continue
		}
		fieldPath := path + "." + string(field.Name())
		if field.IsMap() {
			if field.MapValue().Kind() == protoreflect.EnumKind {
				var invalid protoreflect.EnumNumber
				valid := true
				message.Get(field).Map().Range(func(_ protoreflect.MapKey, value protoreflect.Value) bool {
					invalid = value.Enum()
					valid = field.MapValue().Enum().Values().ByNumber(invalid) != nil
					return valid
				})
				if !valid {
					return fmt.Errorf("%s contains unknown enum value %d", fieldPath, invalid)
				}
			}
			continue
		}
		if field.IsList() {
			list := message.Get(field).List()
			for item := 0; item < list.Len(); item++ {
				if field.Kind() == protoreflect.EnumKind && field.Enum().Values().ByNumber(list.Get(item).Enum()) == nil {
					return fmt.Errorf("%s[%d] contains unknown enum value %d", fieldPath, item, list.Get(item).Enum())
				}
				if field.Kind() == protoreflect.MessageKind {
					if err := validateProtoValue(list.Get(item).Message(), fmt.Sprintf("%s[%d]", fieldPath, item), false); err != nil {
						return err
					}
				}
			}
			continue
		}
		switch field.Kind() {
		case protoreflect.EnumKind:
			if field.Enum().Values().ByNumber(message.Get(field).Enum()) == nil {
				return fmt.Errorf("%s contains unknown enum value %d", fieldPath, message.Get(field).Enum())
			}
		case protoreflect.MessageKind:
			child := message.Get(field).Message()
			if field.ContainingOneof() == nil && child.Descriptor().FullName() != "google.protobuf.Duration" && child.Descriptor().FullName() != "wallaby.v1.TypeMappingsConfig" && child.Descriptor().FullName() != "wallaby.v1.KafkaDestinationConfig" && !messageHasFields(child) {
				return fmt.Errorf("%s is an empty nested message whose presence cannot round trip", fieldPath)
			}
			if err := validateProtoValue(child, fieldPath, false); err != nil {
				return err
			}
		}
	}
	_ = root
	return nil
}

func messageHasFields(message protoreflect.Message) bool {
	has := false
	message.Range(func(protoreflect.FieldDescriptor, protoreflect.Value) bool {
		has = true
		return false
	})
	return has
}

func encodePostgresSource(out map[string]string, cfg *wallabypb.PostgresSourceConfig) error {
	if cfg == nil {
		return errors.New("postgres_source config is required")
	}
	mode, err := postgresSourceMode(cfg.GetMode())
	if err != nil {
		return err
	}
	putEnum(out, "mode", mode)
	if cfg.GetMode() == wallabypb.PostgresSourceMode_POSTGRES_SOURCE_MODE_CDC && (len(cfg.GetBackfillTables()) != 0 || len(cfg.GetBackfillSchemas()) != 0) {
		return errors.New("postgres_source CDC mode rejects backfill_tables and backfill_schemas")
	}
	if cfg.GetMode() == wallabypb.PostgresSourceMode_POSTGRES_SOURCE_MODE_BACKFILL && (len(cfg.GetPublicationTables()) != 0 || len(cfg.GetPublicationSchemas()) != 0 || cfg.SyncPublication != nil || cfg.GetSyncPublicationMode() != wallabypb.SyncPublicationMode_SYNC_PUBLICATION_MODE_UNSPECIFIED) {
		return errors.New("postgres_source BACKFILL mode rejects CDC publication selection and synchronization fields")
	}
	if err := encodePGConnection(out, cfg.GetConnection()); err != nil {
		return err
	}
	put(out, "slot", cfg.GetSlot())
	put(out, "publication", cfg.GetPublication())
	putU32(out, "batch_size", cfg.BatchSize)
	if err := putDuration(out, "batch_timeout", cfg.GetBatchTimeout()); err != nil {
		return err
	}
	if err := putDuration(out, "status_interval", cfg.GetStatusInterval()); err != nil {
		return err
	}
	putBool(out, "create_slot", cfg.CreateSlot)
	putBool(out, "emit_empty", cfg.EmitEmpty)
	putBool(out, "ensure_publication", cfg.EnsurePublication)
	putBool(out, "validate_replication", cfg.ValidateReplication)
	putCSV(out, "publication_tables", cfg.GetPublicationTables())
	putCSV(out, "publication_schemas", cfg.GetPublicationSchemas())
	putBool(out, "sync_publication", cfg.SyncPublication)
	putEnum(out, "sync_publication_mode", syncPublicationMode(cfg.GetSyncPublicationMode()))
	putBool(out, "resolve_types", cfg.ResolveTypes)
	putBool(out, "ensure_state", cfg.EnsureState)
	put(out, "state_schema", cfg.GetStateSchema())
	put(out, "state_table", cfg.GetStateTable())
	putBool(out, "capture_ddl", cfg.CaptureDdl)
	put(out, "ddl_trigger_schema", cfg.GetDdlTriggerSchema())
	put(out, "ddl_trigger_name", cfg.GetDdlTriggerName())
	put(out, "ddl_message_prefix", cfg.GetDdlMessagePrefix())
	putEnum(out, "toast_fetch", toastFetch(cfg.GetToastFetch()))
	putU32(out, "toast_cache_size", cfg.ToastCacheSize)
	putBool(out, "managed", cfg.Managed)
	putEnum(out, "managed_profile", managedProfile(cfg.GetManagedProfile()))
	putU64(out, "max_transaction_records", cfg.MaxTransactionRecords)
	putU64(out, "max_transaction_bytes", cfg.MaxTransactionBytes)
	putU32(out, "max_transaction_fragments", cfg.MaxTransactionFragments)
	putBool(out, "streaming_transactions", cfg.StreamingTransactions)
	put(out, "source_system_identifier", cfg.GetSourceSystemIdentifier())
	put(out, "source_lineage_id", cfg.GetSourceLineageId())
	put(out, "publication_revision", cfg.GetPublicationRevision())
	putEnum(out, "bootstrap", bootstrapMode(cfg.GetBootstrap()))
	putU32(out, "bootstrap_restart_limit", cfg.BootstrapRestartLimit)
	putU32(out, "snapshot_max_tables", cfg.SnapshotMaxTables)
	putU32(out, "snapshot_workers", cfg.SnapshotWorkers)
	if err := putDuration(out, "snapshot_claim_lease", cfg.GetSnapshotClaimLease()); err != nil {
		return err
	}
	if cfg.GetMode() == wallabypb.PostgresSourceMode_POSTGRES_SOURCE_MODE_BACKFILL {
		putCSV(out, "tables", cfg.GetBackfillTables())
		putCSV(out, "schemas", cfg.GetBackfillSchemas())
	}
	put(out, "partition_column", cfg.GetPartitionColumn())
	putU32(out, "partition_count", cfg.PartitionCount)
	putBool(out, "snapshot_consistent", cfg.SnapshotConsistent)
	if err := encodeSnapshotState(out, cfg.GetSnapshotState()); err != nil {
		return err
	}
	putEnum(out, "format", wireFormat(cfg.GetFormat()))
	if err := putPositiveDuration(out, "delivery_retention", cfg.GetDeliveryRetention()); err != nil {
		return err
	}
	return putPositiveDuration(out, "delivery_prune_interval", cfg.GetDeliveryPruneInterval())
}

func encodeSnapshotState(out map[string]string, cfg *wallabypb.SnapshotStateConfig) error {
	if cfg == nil {
		return nil
	}
	switch backend := cfg.Backend.(type) {
	case *wallabypb.SnapshotStateConfig_Disabled:
		if !backend.Disabled {
			return errors.New("snapshot_state.disabled must be true when selected")
		}
		out["snapshot_state_backend"] = "none"
	case *wallabypb.SnapshotStateConfig_FilePath:
		out["snapshot_state_backend"] = "file"
		put(out, "snapshot_state_path", backend.FilePath)
	case *wallabypb.SnapshotStateConfig_Postgres:
		out["snapshot_state_backend"] = "postgres"
		if backend.Postgres != nil {
			put(out, "snapshot_state_dsn", backend.Postgres.GetDsn())
		}
	default:
		return errors.New("snapshot_state backend is required")
	}
	put(out, "snapshot_state_schema", cfg.GetSchema())
	put(out, "snapshot_state_table", cfg.GetTable())
	return nil
}

func encodePostgresDestination(out map[string]string, cfg *wallabypb.PostgresDestinationConfig) error {
	if cfg == nil {
		return errors.New("postgres_destination config is required")
	}
	if err := encodePGConnection(out, cfg.GetConnection()); err != nil {
		return err
	}
	encodeStaging(out, cfg.GetStaging())
	encodeMetadata(out, cfg.GetMetadata())
	put(out, "synchronous_commit", cfg.GetSynchronousCommit())
	putEnum(out, "managed_profile", managedProfile(cfg.GetManagedProfile()))
	put(out, "destination_revision_id", cfg.GetDestinationRevisionId())
	return encodeTypeMappings(out, cfg.GetTypeMappings())
}

func encodePGConnection(out map[string]string, cfg *wallabypb.PostgresConnectionConfig) error {
	if cfg == nil {
		return nil
	}
	put(out, "dsn", cfg.GetDsn())
	putU32(out, "pool_max_conns", cfg.PoolMaxConnections)
	if iam := cfg.GetRdsIam(); iam != nil {
		if strings.TrimSpace(iam.GetRegion()) == "" {
			return errors.New("postgres connection rds_iam.region is required")
		}
		if strings.TrimSpace(iam.GetRoleArn()) != "" && strings.TrimSpace(iam.GetRoleSessionName()) == "" {
			return errors.New("postgres connection rds_iam.role_session_name is required when role_arn is configured")
		}
		out["aws_rds_iam"] = "true"
		put(out, "aws_region", iam.GetRegion())
		put(out, "aws_profile", iam.GetProfile())
		put(out, "aws_role_arn", iam.GetRoleArn())
		put(out, "aws_role_session_name", iam.GetRoleSessionName())
		put(out, "aws_role_external_id", iam.GetRoleExternalId())
		put(out, "aws_endpoint", iam.GetEndpoint())
	}
	return nil
}

func encodePGStream(out map[string]string, cfg *wallabypb.PGStreamDestinationConfig) error {
	if cfg == nil {
		return errors.New("pgstream config is required")
	}
	if connection := cfg.GetConnection(); connection != nil {
		put(out, "dsn", connection.GetDsn())
	}
	put(out, "stream", cfg.GetStream())
	putEnum(out, "format", wireFormat(cfg.GetFormat()))
	if err := encodeRegistry(out, cfg.GetSchemaRegistry()); err != nil {
		return err
	}
	put(out, "schema_registry_subject", cfg.GetSchemaRegistrySubject())
	put(out, "schema_registry_proto_types_subject", cfg.GetSchemaRegistryProtoTypesSubject())
	return encodeTypeMappings(out, cfg.GetTypeMappings())
}

func encodeKafka(out map[string]string, cfg *wallabypb.KafkaDestinationConfig) error {
	if cfg == nil {
		return errors.New("kafka config is required")
	}
	putCSV(out, "brokers", cfg.GetBrokers())
	put(out, "topic", cfg.GetTopic())
	putEnum(out, "format", wireFormat(cfg.GetFormat()))
	putEnum(out, "compression", compression(cfg.GetCompression()))
	putEnum(out, "acks", kafkaAcks(cfg.GetAcks()))
	putU32(out, "max_message_bytes", cfg.MaxMessageBytes)
	putU32(out, "max_batch_bytes", cfg.MaxBatchBytes)
	putU32(out, "max_record_bytes", cfg.MaxRecordBytes)
	putBool(out, "transactional_producer", cfg.TransactionalProducer)
	putBool(out, "allow_oversize_skip", cfg.AllowOversizeSkip)
	putEnum(out, "message_mode", kafkaMessageMode(cfg.GetMessageMode()))
	putEnum(out, "key_mode", kafkaKeyMode(cfg.GetKeyMode()))
	put(out, "transactional_id", cfg.GetTransactionalId())
	if err := putDuration(out, "transaction_timeout", cfg.GetTransactionTimeout()); err != nil {
		return err
	}
	put(out, "transaction_header", cfg.GetTransactionHeader())
	if err := encodeRegistry(out, cfg.GetSchemaRegistry()); err != nil {
		return err
	}
	put(out, "schema_registry_subject", cfg.GetSchemaRegistrySubject())
	put(out, "schema_registry_proto_types_subject", cfg.GetSchemaRegistryProtoTypesSubject())
	put(out, "schema_registry_subject_mode", cfg.GetSchemaRegistrySubjectMode())
	return encodeTypeMappings(out, cfg.GetTypeMappings())
}

func encodeS3(out map[string]string, cfg *wallabypb.S3DestinationConfig) error {
	if cfg == nil {
		return errors.New("s3 config is required")
	}
	put(out, "bucket", cfg.GetBucket())
	put(out, "prefix", cfg.GetPrefix())
	put(out, "region", cfg.GetRegion())
	putEnum(out, "format", wireFormat(cfg.GetFormat()))
	putEnum(out, "compression", compression(cfg.GetCompression()))
	putCSV(out, "partition_by", cfg.GetPartitionBy())
	put(out, "endpoint", cfg.GetEndpoint())
	put(out, "access_key", cfg.GetAccessKey())
	put(out, "secret_key", cfg.GetSecretKey())
	put(out, "session_token", cfg.GetSessionToken())
	putBool(out, "force_path_style", cfg.ForcePathStyle)
	putBool(out, "use_fips", cfg.UseFips)
	putBool(out, "use_dualstack", cfg.UseDualstack)
	if err := encodeRegistry(out, cfg.GetSchemaRegistry()); err != nil {
		return err
	}
	put(out, "schema_registry_subject", cfg.GetSchemaRegistrySubject())
	put(out, "schema_registry_proto_types_subject", cfg.GetSchemaRegistryProtoTypesSubject())
	return encodeTypeMappings(out, cfg.GetTypeMappings())
}

func encodeHTTP(out map[string]string, cfg *wallabypb.HTTPDestinationConfig) error {
	if cfg == nil {
		return errors.New("http config is required")
	}
	put(out, "url", cfg.GetUrl())
	put(out, "method", cfg.GetMethod())
	putEnum(out, "format", wireFormat(cfg.GetFormat()))
	putEnum(out, "payload_mode", payloadMode(cfg.GetPayloadMode()))
	if err := putDuration(out, "timeout", cfg.GetTimeout()); err != nil {
		return err
	}
	if err := putKeyValues(out, "headers", cfg.GetHeaders(), true); err != nil {
		return err
	}
	if err := encodeRetry(out, cfg.GetRetry()); err != nil {
		return err
	}
	put(out, "idempotency_header", cfg.GetIdempotencyHeader())
	if err := putDuration(out, "dedupe_window", cfg.GetDedupeWindow()); err != nil {
		return err
	}
	put(out, "transaction_header", cfg.GetTransactionHeader())
	if err := encodeRegistry(out, cfg.GetSchemaRegistry()); err != nil {
		return err
	}
	put(out, "schema_registry_subject", cfg.GetSchemaRegistrySubject())
	put(out, "schema_registry_proto_types_subject", cfg.GetSchemaRegistryProtoTypesSubject())
	return encodeTypeMappings(out, cfg.GetTypeMappings())
}

func encodeGRPC(out map[string]string, cfg *wallabypb.GRPCDestinationConfig) error {
	if cfg == nil {
		return errors.New("grpc config is required")
	}
	put(out, "endpoint", cfg.GetEndpoint())
	encodeGRPCTLS(out, cfg.GetTls())
	if err := putDuration(out, "timeout", cfg.GetTimeout()); err != nil {
		return err
	}
	putEnum(out, "format", wireFormat(cfg.GetFormat()))
	putEnum(out, "payload_mode", payloadMode(cfg.GetPayloadMode()))
	if err := putKeyValues(out, "headers", cfg.GetMetadata(), false); err != nil {
		return err
	}
	if err := encodeRetry(out, cfg.GetRetry()); err != nil {
		return err
	}
	if err := encodeRegistry(out, cfg.GetSchemaRegistry()); err != nil {
		return err
	}
	put(out, "schema_registry_subject", cfg.GetSchemaRegistrySubject())
	put(out, "schema_registry_proto_types_subject", cfg.GetSchemaRegistryProtoTypesSubject())
	return encodeTypeMappings(out, cfg.GetTypeMappings())
}

func encodeRetry(out map[string]string, cfg *wallabypb.RetryConfig) error {
	if cfg == nil {
		return nil
	}
	putU32(out, "max_retries", cfg.MaxRetries)
	if err := putDuration(out, "backoff_base", cfg.GetBackoffBase()); err != nil {
		return err
	}
	if err := putDuration(out, "backoff_max", cfg.GetBackoffMax()); err != nil {
		return err
	}
	if cfg.BackoffFactor != nil {
		if math.IsNaN(*cfg.BackoffFactor) || math.IsInf(*cfg.BackoffFactor, 0) {
			return errors.New("retry.backoff_factor must be finite")
		}
		out["backoff_factor"] = strconv.FormatFloat(*cfg.BackoffFactor, 'g', -1, 64)
	}
	return nil
}

func encodeGRPCTLS(out map[string]string, cfg *wallabypb.GRPCTLSConfig) {
	if cfg == nil {
		return
	}
	putBool(out, "insecure", cfg.Insecure)
	put(out, "tls_ca_file", cfg.GetCaFile())
	put(out, "tls_server_name", cfg.GetServerName())
}

func encodeClickHouseTLS(out map[string]string, cfg *wallabypb.ClickHouseTLSConfig) {
	if cfg == nil {
		return
	}
	putBool(out, "insecure", cfg.Insecure)
	put(out, "tls_ca_file", cfg.GetCaFile())
	put(out, "tls_server_name", cfg.GetServerName())
	put(out, "tls_cert_file", cfg.GetCertificateFile())
	put(out, "tls_key_file", cfg.GetPrivateKeyFile())
	put(out, "managed_replica_tls_server_name", cfg.GetReplicaServerName())
}

func encodeSnowflake(out map[string]string, cfg *wallabypb.SnowflakeDestinationConfig) error {
	if cfg == nil {
		return errors.New("snowflake config is required")
	}
	put(out, "dsn", cfg.GetDsn())
	putBool(out, "disable_transactions", cfg.DisableTransactions)
	encodeWarehouse(out, cfg.GetWarehouse())
	encodeStaging(out, cfg.GetStaging())
	encodeMetadata(out, cfg.GetMetadata())
	if err := encodeRegistry(out, cfg.GetSchemaRegistry()); err != nil {
		return err
	}
	put(out, "schema_registry_subject", cfg.GetSchemaRegistrySubject())
	return encodeTypeMappings(out, cfg.GetTypeMappings())
}

func encodeSnowflakeSQL(out map[string]string, cfg *wallabypb.SnowflakePostgresSQLConfig) error {
	if cfg == nil {
		return errors.New("snowflake_postgres_sql config is required")
	}
	put(out, "dsn", cfg.GetDsn())
	out["batch_mode"], out["batch_resolution"], out["meta_table_enabled"] = "target", "none", "false"
	out["disable_transactions"], out["session_keep_alive"] = "false", "false"
	put(out, "destination_revision_id", cfg.GetDestinationRevisionId())
	put(out, "managed_account", cfg.GetAccount())
	put(out, "managed_database", cfg.GetDatabase())
	put(out, "managed_schema", cfg.GetSchema())
	put(out, "managed_table", cfg.GetTable())
	put(out, "managed_receipts_table", cfg.GetReceiptsTable())
	put(out, "managed_owner_role", cfg.GetOwnerRole())
	put(out, "managed_execution_role", cfg.GetExecutionRole())
	put(out, "managed_warehouse", cfg.GetManagedWarehouse())
	put(out, "managed_snowflake_version", cfg.GetSnowflakeVersion())
	put(out, "managed_target_created_on", cfg.GetTargetCreatedOn())
	put(out, "managed_receipts_created_on", cfg.GetReceiptsCreatedOn())
	putU32(out, "managed_max_transaction_rows", cfg.MaxTransactionRows)
	putU64(out, "managed_max_transaction_bytes", cfg.MaxTransactionBytes)
	putU32(out, "managed_max_transaction_fragments", cfg.MaxTransactionFragments)
	putU32(out, "managed_max_open_conns", cfg.MaxOpenConnections)
	putU32(out, "managed_statement_timeout_seconds", cfg.StatementTimeoutSeconds)
	putU32(out, "managed_hybrid_table_lock_timeout_seconds", cfg.HybridTableLockTimeoutSeconds)
	return nil
}

func encodeSnowflakeStaged(out map[string]string, cfg *wallabypb.SnowflakePostgresStagedConfig) error {
	if cfg == nil {
		return errors.New("snowflake_postgres_staged config is required")
	}
	put(out, "dsn", cfg.GetDsn())
	out["batch_mode"], out["batch_resolution"], out["meta_table_enabled"] = "target", "none", "false"
	out["disable_transactions"], out["session_keep_alive"] = "false", "false"
	put(out, "managed_stage", cfg.GetStage())
	put(out, "managed_file_format", cfg.GetFileFormat())
	putBool(out, "managed_auto_ingest", cfg.AutoIngest)
	put(out, "destination_revision_id", cfg.GetDestinationRevisionId())
	put(out, "managed_account", cfg.GetAccount())
	put(out, "managed_database", cfg.GetDatabase())
	put(out, "managed_schema", cfg.GetSchema())
	put(out, "managed_table", cfg.GetTable())
	put(out, "managed_receipts_table", cfg.GetReceiptsTable())
	put(out, "managed_pipe", cfg.GetPipe())
	put(out, "managed_owner_role", cfg.GetOwnerRole())
	put(out, "managed_execution_role", cfg.GetExecutionRole())
	put(out, "managed_warehouse", cfg.GetManagedWarehouse())
	put(out, "managed_snowflake_version", cfg.GetSnowflakeVersion())
	put(out, "managed_stage_created_on", cfg.GetStageCreatedOn())
	put(out, "managed_target_created_on", cfg.GetTargetCreatedOn())
	put(out, "managed_receipts_created_on", cfg.GetReceiptsCreatedOn())
	put(out, "managed_file_format_created_on", cfg.GetFileFormatCreatedOn())
	put(out, "managed_pipe_created_on", cfg.GetPipeCreatedOn())
	putU32(out, "managed_max_transaction_rows", cfg.MaxTransactionRows)
	putU64(out, "managed_max_transaction_bytes", cfg.MaxTransactionBytes)
	putU32(out, "managed_max_transaction_fragments", cfg.MaxTransactionFragments)
	putU32(out, "managed_max_open_conns", cfg.MaxOpenConnections)
	putU32(out, "managed_statement_timeout_seconds", cfg.StatementTimeoutSeconds)
	putU32(out, "managed_load_verify_attempts", cfg.LoadVerifyAttempts)
	putU32(out, "managed_load_verify_interval_ms", cfg.LoadVerifyIntervalMillis)
	putU32(out, "managed_cleanup_max_objects", cfg.CleanupMaxObjects)
	putU32(out, "managed_cleanup_retention_seconds", cfg.CleanupRetentionSeconds)
	return nil
}

func encodeSnowflakeStreaming(out map[string]string, cfg *wallabypb.SnowflakePostgresStreamingConfig) error {
	if cfg == nil {
		return errors.New("snowflake_postgres_streaming config is required")
	}
	put(out, "dsn", cfg.GetDsn())
	out["batch_mode"], out["batch_resolution"], out["meta_table_enabled"] = "target", "none", "false"
	out["disable_transactions"], out["session_keep_alive"] = "false", "false"
	put(out, "destination_revision_id", cfg.GetDestinationRevisionId())
	put(out, "managed_streaming_transport", cfg.GetTransport())
	put(out, "managed_account", cfg.GetAccount())
	put(out, "managed_database", cfg.GetDatabase())
	put(out, "managed_schema", cfg.GetSchema())
	put(out, "managed_pipe", cfg.GetPipe())
	put(out, "managed_table", cfg.GetTable())
	put(out, "managed_receipts_table", cfg.GetReceiptsTable())
	put(out, "managed_channel_state_table", cfg.GetChannelStateTable())
	put(out, "managed_channel_name_prefix", cfg.GetChannelNamePrefix())
	put(out, "managed_owner_role", cfg.GetOwnerRole())
	put(out, "managed_execution_role", cfg.GetExecutionRole())
	put(out, "managed_warehouse", cfg.GetManagedWarehouse())
	put(out, "managed_snowflake_version", cfg.GetSnowflakeVersion())
	put(out, "managed_pipe_created_on", cfg.GetPipeCreatedOn())
	put(out, "managed_target_created_on", cfg.GetTargetCreatedOn())
	put(out, "managed_receipts_created_on", cfg.GetReceiptsCreatedOn())
	put(out, "managed_channel_state_created_on", cfg.GetChannelStateCreatedOn())
	putU32(out, "managed_max_transaction_rows", cfg.MaxTransactionRows)
	putU64(out, "managed_max_transaction_bytes", cfg.MaxTransactionBytes)
	putU32(out, "managed_max_transaction_fragments", cfg.MaxTransactionFragments)
	putU64(out, "managed_max_row_bytes", cfg.MaxRowBytes)
	putU32(out, "managed_max_open_conns", cfg.MaxOpenConnections)
	putU32(out, "managed_statement_timeout_seconds", cfg.StatementTimeoutSeconds)
	putU32(out, "managed_observe_attempts", cfg.ObserveAttempts)
	putU32(out, "managed_observe_interval_ms", cfg.ObserveIntervalMillis)
	putU32(out, "managed_append_attempts", cfg.AppendAttempts)
	putU32(out, "managed_append_backoff_ms", cfg.AppendBackoffMillis)
	putU32(out, "managed_cleanup_max_objects", cfg.CleanupMaxObjects)
	putU32(out, "managed_cleanup_retention_seconds", cfg.CleanupRetentionSeconds)
	return nil
}

func encodeSnowpipe(out map[string]string, cfg *wallabypb.SnowpipeDestinationConfig) error {
	if cfg == nil {
		return errors.New("snowpipe config is required")
	}
	put(out, "dsn", cfg.GetDsn())
	put(out, "stage", cfg.GetStage())
	put(out, "stage_path", cfg.GetStagePath())
	putEnum(out, "format", wireFormat(cfg.GetFormat()))
	put(out, "file_format", cfg.GetFileFormat())
	encodeWarehouse(out, cfg.GetWarehouse())
	putBool(out, "copy_on_write", cfg.CopyOnWrite)
	put(out, "copy_pattern", cfg.GetCopyPattern())
	put(out, "copy_on_error", cfg.GetCopyOnError())
	putBool(out, "copy_purge", cfg.CopyPurge)
	put(out, "copy_match_by_column_name", cfg.GetCopyMatchByColumnName())
	putBool(out, "auto_ingest", cfg.AutoIngest)
	encodeMetadata(out, cfg.GetMetadata())
	if err := encodeRegistry(out, cfg.GetSchemaRegistry()); err != nil {
		return err
	}
	put(out, "schema_registry_subject", cfg.GetSchemaRegistrySubject())
	return encodeTypeMappings(out, cfg.GetTypeMappings())
}

func encodeClickHouse(out map[string]string, cfg *wallabypb.ClickHouseDestinationConfig) error {
	if cfg == nil {
		return errors.New("clickhouse config is required")
	}
	put(out, "dsn", cfg.GetDsn())
	encodeStaging(out, cfg.GetStaging())
	encodeClickHouseMetadata(out, cfg.GetMetadata())
	return encodeTypeMappings(out, cfg.GetTypeMappings())
}

func encodeClickHouseManaged(out map[string]string, cfg *wallabypb.ClickHousePostgresAppendConfig) error {
	if cfg == nil {
		return errors.New("clickhouse_postgres_append config is required")
	}
	put(out, "dsn", cfg.GetDsn())
	out["managed_deployment"], out["batch_mode"], out["batch_resolution"], out["meta_table_enabled"], out["async_insert"], out["wait_for_async_insert"] = "self-managed-keeper", "target", "none", "false", "false", "true"
	encodeClickHouseTLS(out, cfg.GetTls())
	put(out, "destination_revision_id", cfg.GetDestinationRevisionId())
	put(out, "managed_database", cfg.GetDatabase())
	put(out, "managed_changelog_table", cfg.GetChangelogTable())
	put(out, "managed_receipts_table", cfg.GetReceiptsTable())
	put(out, "managed_final_view", cfg.GetFinalView())
	put(out, "managed_keeper_path_prefix", cfg.GetKeeperPathPrefix())
	put(out, "managed_keeper_address", cfg.GetKeeperAddress())
	put(out, "managed_replica_dsn", cfg.GetReplicaDsn())
	putCSV(out, "managed_replica_names", cfg.GetReplicaNames())
	putU32(out, "insert_quorum", cfg.InsertQuorum)
	putU32(out, "managed_max_active_parts", cfg.MaxActiveParts)
	putU32(out, "managed_max_transaction_rows", cfg.MaxTransactionRows)
	putU64(out, "managed_max_transaction_bytes", cfg.MaxTransactionBytes)
	putU32(out, "managed_max_transaction_fragments", cfg.MaxTransactionFragments)
	putU32(out, "managed_max_rows_per_batch", cfg.MaxRowsPerBatch)
	putU64(out, "managed_max_batch_bytes", cfg.MaxBatchBytes)
	return encodeTypeMappings(out, cfg.GetTypeMappings())
}

func encodeDuckDB(out map[string]string, cfg *wallabypb.DuckDBDestinationConfig) error {
	if cfg == nil {
		return errors.New("duckdb config is required")
	}
	put(out, "dsn", cfg.GetDsn())
	encodeStaging(out, cfg.GetStaging())
	encodeMetadata(out, cfg.GetMetadata())
	return encodeTypeMappings(out, cfg.GetTypeMappings())
}

func encodeDuckLake(out map[string]string, cfg *wallabypb.DuckLakeDestinationConfig) error {
	if cfg == nil {
		return errors.New("ducklake config is required")
	}
	put(out, "dsn", cfg.GetDsn())
	put(out, "catalog", cfg.GetCatalog())
	put(out, "catalog_name", cfg.GetCatalogName())
	put(out, "data_path", cfg.GetDataPath())
	putBool(out, "override_data_path", cfg.OverrideDataPath)
	putBool(out, "install_extensions", cfg.InstallExtensions)
	encodeStaging(out, cfg.GetStaging())
	encodeMetadata(out, cfg.GetMetadata())
	return encodeTypeMappings(out, cfg.GetTypeMappings())
}

func encodeIceberg(out map[string]string, cfg *wallabypb.IcebergDestinationConfig) error {
	if cfg == nil {
		return errors.New("iceberg config is required")
	}
	putEnum(out, "catalog_profile", icebergProfile(cfg.GetCatalogProfile()))
	put(out, "control_table", cfg.GetControlTable())
	put(out, "destination_revision_id", cfg.GetDestinationRevisionId())
	return nil
}

func encodeStaging(out map[string]string, cfg *wallabypb.StagingConfig) {
	if cfg == nil {
		return
	}
	putEnum(out, "batch_mode", batchMode(cfg.GetBatchMode()))
	putEnum(out, "batch_resolution", batchResolution(cfg.GetBatchResolution()))
	put(out, "staging_schema", cfg.GetSchema())
	put(out, "staging_table", cfg.GetTable())
	put(out, "staging_suffix", cfg.GetSuffix())
}

func encodeMetadata(out map[string]string, cfg *wallabypb.MetadataConfig) {
	if cfg == nil {
		return
	}
	putBool(out, "meta_table_enabled", cfg.Enabled)
	put(out, "meta_schema", cfg.GetSchema())
	put(out, "meta_table", cfg.GetTable())
	put(out, "meta_pk_prefix", cfg.GetPrimaryKeyPrefix())
}

func encodeClickHouseMetadata(out map[string]string, cfg *wallabypb.ClickHouseMetadataConfig) {
	if cfg == nil {
		return
	}
	putBool(out, "meta_table_enabled", cfg.Enabled)
	put(out, "meta_schema", cfg.GetSchema())
	put(out, "meta_table", cfg.GetTable())
	put(out, "meta_pk_prefix", cfg.GetPrimaryKeyPrefix())
	put(out, "meta_engine", cfg.GetEngine())
	put(out, "meta_order_by", cfg.GetOrderBy())
}

func encodeWarehouse(out map[string]string, cfg *wallabypb.WarehouseConfig) {
	if cfg == nil {
		return
	}
	put(out, "warehouse", cfg.GetName())
	put(out, "warehouse_size", cfg.GetSize())
	putU32(out, "warehouse_auto_suspend", cfg.AutoSuspendSeconds)
	putBool(out, "warehouse_auto_resume", cfg.AutoResume)
	putBool(out, "session_keep_alive", cfg.SessionKeepAlive)
}

func encodeTypeMappings(out map[string]string, cfg *wallabypb.TypeMappingsConfig) error {
	if cfg == nil {
		return nil
	}
	original := cloneMap(cfg.GetMappings())
	encoded, err := json.Marshal(original)
	if err != nil {
		return fmt.Errorf("encode inline type mappings: %w", err)
	}
	parsed, err := typemapping.Parse(string(encoded))
	if err != nil {
		return fmt.Errorf("validate inline type mappings: %w", err)
	}
	if !maps.Equal(original, parsed) {
		return errors.New("type mapping keys and values must already use their canonical spellings")
	}
	out[typemapping.OptTypeMappings] = string(encoded)
	return nil
}

func encodeRegistry(out map[string]string, cfg *wallabypb.SchemaRegistryConfig) error {
	if cfg == nil {
		return nil
	}
	switch backend := cfg.Backend.(type) {
	case *wallabypb.SchemaRegistryConfig_Confluent:
		out["schema_registry"] = "csr"
		if err := encodeHTTPRegistry(out, backend.Confluent.GetUrl(), backend.Confluent.GetUsername(), backend.Confluent.GetPassword(), backend.Confluent.GetToken(), backend.Confluent.GetTimeout()); err != nil {
			return err
		}
	case *wallabypb.SchemaRegistryConfig_Apicurio:
		out["schema_registry"] = "apicurio"
		if err := encodeHTTPRegistry(out, backend.Apicurio.GetUrl(), backend.Apicurio.GetUsername(), backend.Apicurio.GetPassword(), backend.Apicurio.GetToken(), backend.Apicurio.GetTimeout()); err != nil {
			return err
		}
		putBool(out, "schema_registry_apicurio_compat", backend.Apicurio.Compatibility)
	case *wallabypb.SchemaRegistryConfig_Glue:
		out["schema_registry"] = "glue"
		put(out, "schema_registry_region", backend.Glue.GetRegion())
		put(out, "schema_registry_endpoint", backend.Glue.GetEndpoint())
		put(out, "schema_registry_profile", backend.Glue.GetProfile())
		put(out, "schema_registry_role_arn", backend.Glue.GetRoleArn())
		put(out, "schema_registry_glue_registry", backend.Glue.GetRegistry())
		put(out, "schema_registry_glue_schema", backend.Glue.GetSchema())
	case *wallabypb.SchemaRegistryConfig_Postgres:
		if backend.Postgres == nil || backend.Postgres.GetConnection() == nil || strings.TrimSpace(backend.Postgres.GetConnection().GetDsn()) == "" {
			return errors.New("schema_registry.postgres.connection.dsn is required")
		}
		out["schema_registry"] = "postgres"
		put(out, "schema_registry_dsn", backend.Postgres.GetConnection().GetDsn())
		if err := putDuration(out, "schema_registry_timeout", backend.Postgres.GetTimeout()); err != nil {
			return err
		}
	case *wallabypb.SchemaRegistryConfig_Local:
		if backend.Local == nil || strings.TrimSpace(backend.Local.GetDirectory()) == "" {
			return errors.New("schema_registry.local.directory is required")
		}
		out["schema_registry"] = "local"
		put(out, schemaregistry.OptRegistryLocalDirectory, backend.Local.GetDirectory())
	default:
		return errors.New("schema_registry backend is required")
	}
	return nil
}

func encodeHTTPRegistry(out map[string]string, url, username, password, token string, timeout *durationpb.Duration) error {
	put(out, "schema_registry_url", url)
	put(out, "schema_registry_username", username)
	put(out, "schema_registry_password", password)
	put(out, "schema_registry_token", token)
	return putDuration(out, "schema_registry_timeout", timeout)
}

// Reverse decoding intentionally accepts only the canonical spellings emitted above.
func decodePostgresSource(v map[string]string) (*wallabypb.PostgresSourceConfig, error) {
	connection, err := decodePGConnection(v)
	if err != nil {
		return nil, err
	}
	cfg := &wallabypb.PostgresSourceConfig{Connection: connection}
	cfg.Mode, err = parsePostgresSourceMode(take(v, "mode"))
	if err != nil {
		return nil, err
	}
	cfg.Slot = take(v, "slot")
	cfg.Publication = take(v, "publication")
	cfg.BatchSize, err = takeU32(v, "batch_size")
	if err != nil {
		return nil, err
	}
	cfg.BatchTimeout, err = takeDuration(v, "batch_timeout")
	if err != nil {
		return nil, err
	}
	cfg.StatusInterval, err = takeDuration(v, "status_interval")
	if err != nil {
		return nil, err
	}
	if cfg.CreateSlot, err = takeBool(v, "create_slot"); err != nil {
		return nil, err
	}
	if cfg.EmitEmpty, err = takeBool(v, "emit_empty"); err != nil {
		return nil, err
	}
	if cfg.EnsurePublication, err = takeBool(v, "ensure_publication"); err != nil {
		return nil, err
	}
	if cfg.ValidateReplication, err = takeBool(v, "validate_replication"); err != nil {
		return nil, err
	}
	cfg.PublicationTables, err = takeCSV(v, "publication_tables")
	if err != nil {
		return nil, err
	}
	cfg.PublicationSchemas, err = takeCSV(v, "publication_schemas")
	if err != nil {
		return nil, err
	}
	if cfg.SyncPublication, err = takeBool(v, "sync_publication"); err != nil {
		return nil, err
	}
	cfg.SyncPublicationMode, err = parseSyncPublicationMode(take(v, "sync_publication_mode"))
	if err != nil {
		return nil, err
	}
	if cfg.ResolveTypes, err = takeBool(v, "resolve_types"); err != nil {
		return nil, err
	}
	if cfg.EnsureState, err = takeBool(v, "ensure_state"); err != nil {
		return nil, err
	}
	cfg.StateSchema = take(v, "state_schema")
	cfg.StateTable = take(v, "state_table")
	if cfg.CaptureDdl, err = takeBool(v, "capture_ddl"); err != nil {
		return nil, err
	}
	cfg.DdlTriggerSchema = take(v, "ddl_trigger_schema")
	cfg.DdlTriggerName = take(v, "ddl_trigger_name")
	cfg.DdlMessagePrefix = take(v, "ddl_message_prefix")
	cfg.ToastFetch, err = parseToastFetch(take(v, "toast_fetch"))
	if err != nil {
		return nil, err
	}
	cfg.ToastCacheSize, err = takeU32(v, "toast_cache_size")
	if err != nil {
		return nil, err
	}
	if cfg.Managed, err = takeBool(v, "managed"); err != nil {
		return nil, err
	}
	cfg.ManagedProfile, err = parseManagedProfile(take(v, "managed_profile"))
	if err != nil {
		return nil, err
	}
	cfg.MaxTransactionRecords, err = takeU64(v, "max_transaction_records")
	if err != nil {
		return nil, err
	}
	cfg.MaxTransactionBytes, err = takeU64(v, "max_transaction_bytes")
	if err != nil {
		return nil, err
	}
	cfg.MaxTransactionFragments, err = takeU32(v, "max_transaction_fragments")
	if err != nil {
		return nil, err
	}
	if cfg.StreamingTransactions, err = takeBool(v, "streaming_transactions"); err != nil {
		return nil, err
	}
	cfg.SourceSystemIdentifier = take(v, "source_system_identifier")
	cfg.SourceLineageId = take(v, "source_lineage_id")
	cfg.PublicationRevision = take(v, "publication_revision")
	cfg.Bootstrap, err = parseBootstrap(take(v, "bootstrap"))
	if err != nil {
		return nil, err
	}
	cfg.BootstrapRestartLimit, err = takeU32(v, "bootstrap_restart_limit")
	if err != nil {
		return nil, err
	}
	cfg.SnapshotMaxTables, err = takeU32(v, "snapshot_max_tables")
	if err != nil {
		return nil, err
	}
	cfg.SnapshotWorkers, err = takeU32(v, "snapshot_workers")
	if err != nil {
		return nil, err
	}
	cfg.SnapshotClaimLease, err = takeDuration(v, "snapshot_claim_lease")
	if err != nil {
		return nil, err
	}
	cfg.BackfillTables, err = takeCSV(v, "tables")
	if err != nil {
		return nil, err
	}
	cfg.BackfillSchemas, err = takeCSV(v, "schemas")
	if err != nil {
		return nil, err
	}
	cfg.PartitionColumn = take(v, "partition_column")
	cfg.PartitionCount, err = takeU32(v, "partition_count")
	if err != nil {
		return nil, err
	}
	if cfg.SnapshotConsistent, err = takeBool(v, "snapshot_consistent"); err != nil {
		return nil, err
	}
	cfg.SnapshotState, err = decodeSnapshotState(v)
	if err != nil {
		return nil, err
	}
	cfg.Format, err = parseWireFormat(take(v, "format"))
	if err != nil {
		return nil, err
	}
	cfg.DeliveryRetention, err = takePositiveDuration(v, "delivery_retention")
	if err != nil {
		return nil, err
	}
	cfg.DeliveryPruneInterval, err = takePositiveDuration(v, "delivery_prune_interval")
	if err != nil {
		return nil, err
	}
	if cfg.Mode == wallabypb.PostgresSourceMode_POSTGRES_SOURCE_MODE_CDC && (len(cfg.BackfillTables) != 0 || len(cfg.BackfillSchemas) != 0) {
		return nil, errors.New("postgres source mode=cdc rejects backfill tables and schemas")
	}
	if cfg.Mode == wallabypb.PostgresSourceMode_POSTGRES_SOURCE_MODE_BACKFILL && (len(cfg.PublicationTables) != 0 || len(cfg.PublicationSchemas) != 0 || cfg.SyncPublication != nil || cfg.SyncPublicationMode != wallabypb.SyncPublicationMode_SYNC_PUBLICATION_MODE_UNSPECIFIED) {
		return nil, errors.New("postgres source mode=backfill rejects CDC publication options")
	}
	return cfg, nil
}

func decodeSnapshotState(v map[string]string) (*wallabypb.SnapshotStateConfig, error) {
	backend := take(v, "snapshot_state_backend")
	path := take(v, "snapshot_state_path")
	dsn := take(v, "snapshot_state_dsn")
	schema := take(v, "snapshot_state_schema")
	table := take(v, "snapshot_state_table")
	if backend == "" && path == "" && dsn == "" && schema == "" && table == "" {
		return nil, nil //nolint:nilnil // an absent optional snapshot-state message is valid
	}
	cfg := &wallabypb.SnapshotStateConfig{Schema: schema, Table: table}
	switch backend {
	case "none":
		if path != "" || dsn != "" {
			return nil, errors.New("snapshot_state_backend=none rejects file and postgres backend fields")
		}
		cfg.Backend = &wallabypb.SnapshotStateConfig_Disabled{Disabled: true}
	case "file":
		if dsn != "" {
			return nil, errors.New("snapshot_state_backend=file rejects snapshot_state_dsn")
		}
		cfg.Backend = &wallabypb.SnapshotStateConfig_FilePath{FilePath: path}
	case "postgres":
		if path != "" {
			return nil, errors.New("snapshot_state_backend=postgres rejects snapshot_state_path")
		}
		cfg.Backend = &wallabypb.SnapshotStateConfig_Postgres{Postgres: &wallabypb.PostgresDSNConfig{Dsn: dsn}}
	default:
		return nil, fmt.Errorf("unsupported snapshot_state_backend %q", backend)
	}
	return cfg, nil
}

func decodePostgresDestination(v map[string]string) (*wallabypb.PostgresDestinationConfig, error) {
	tm, err := decodeTypeMappings(v)
	if err != nil {
		return nil, err
	}
	profile, err := parseManagedProfile(take(v, "managed_profile"))
	if err != nil {
		return nil, err
	}
	connection, err := decodePGConnection(v)
	if err != nil {
		return nil, err
	}
	staging, err := decodeStaging(v)
	if err != nil {
		return nil, err
	}
	metadata, err := decodeMetadata(v)
	if err != nil {
		return nil, err
	}
	return &wallabypb.PostgresDestinationConfig{
		Connection:            connection,
		Staging:               staging,
		Metadata:              metadata,
		SynchronousCommit:     take(v, "synchronous_commit"),
		TypeMappings:          tm,
		ManagedProfile:        profile,
		DestinationRevisionId: take(v, "destination_revision_id"),
	}, nil
}
func decodePGStream(v map[string]string) (*wallabypb.PGStreamDestinationConfig, error) {
	format, err := parseWireFormat(take(v, "format"))
	if err != nil {
		return nil, err
	}
	tm, err := decodeTypeMappings(v)
	if err != nil {
		return nil, err
	}
	registry, err := decodeRegistry(v)
	if err != nil {
		return nil, err
	}
	dsn := take(v, "dsn")
	var connection *wallabypb.PostgresDSNConfig
	if dsn != "" {
		connection = &wallabypb.PostgresDSNConfig{Dsn: dsn}
	}
	return &wallabypb.PGStreamDestinationConfig{
		Connection: connection, Stream: take(v, "stream"), Format: format, TypeMappings: tm, SchemaRegistry: registry,
		SchemaRegistrySubject:           take(v, "schema_registry_subject"),
		SchemaRegistryProtoTypesSubject: take(v, "schema_registry_proto_types_subject"),
	}, nil
}
func decodeKafka(v map[string]string) (*wallabypb.KafkaDestinationConfig, error) {
	cfg := &wallabypb.KafkaDestinationConfig{}
	var err error
	cfg.Brokers, err = takeCSV(v, "brokers")
	if err != nil {
		return nil, err
	}
	cfg.Topic = take(v, "topic")
	cfg.Format, err = parseWireFormat(take(v, "format"))
	if err != nil {
		return nil, err
	}
	cfg.Compression, err = parseCompression(take(v, "compression"))
	if err != nil {
		return nil, err
	}
	cfg.Acks, err = parseKafkaAcks(take(v, "acks"))
	if err != nil {
		return nil, err
	}
	cfg.MaxMessageBytes, err = takeU32(v, "max_message_bytes")
	if err != nil {
		return nil, err
	}
	cfg.MaxBatchBytes, err = takeU32(v, "max_batch_bytes")
	if err != nil {
		return nil, err
	}
	cfg.MaxRecordBytes, err = takeU32(v, "max_record_bytes")
	if err != nil {
		return nil, err
	}
	if cfg.TransactionalProducer, err = takeBool(v, "transactional_producer"); err != nil {
		return nil, err
	}
	if cfg.AllowOversizeSkip, err = takeBool(v, "allow_oversize_skip"); err != nil {
		return nil, err
	}
	cfg.MessageMode, err = parseKafkaMessageMode(take(v, "message_mode"))
	if err != nil {
		return nil, err
	}
	cfg.KeyMode, err = parseKafkaKeyMode(take(v, "key_mode"))
	if err != nil {
		return nil, err
	}
	cfg.TransactionalId = take(v, "transactional_id")
	cfg.TransactionTimeout, err = takeDuration(v, "transaction_timeout")
	if err != nil {
		return nil, err
	}
	cfg.TransactionHeader = take(v, "transaction_header")
	cfg.SchemaRegistry, err = decodeRegistry(v)
	if err != nil {
		return nil, err
	}
	cfg.SchemaRegistrySubject = take(v, "schema_registry_subject")
	cfg.SchemaRegistryProtoTypesSubject = take(v, "schema_registry_proto_types_subject")
	cfg.SchemaRegistrySubjectMode = take(v, "schema_registry_subject_mode")
	cfg.TypeMappings, err = decodeTypeMappings(v)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
func decodeS3(v map[string]string) (*wallabypb.S3DestinationConfig, error) {
	cfg := &wallabypb.S3DestinationConfig{Bucket: take(v, "bucket"), Prefix: take(v, "prefix"), Region: take(v, "region"), Endpoint: take(v, "endpoint"), AccessKey: take(v, "access_key"), SecretKey: take(v, "secret_key"), SessionToken: take(v, "session_token")}
	var err error
	cfg.Format, err = parseWireFormat(take(v, "format"))
	if err != nil {
		return nil, err
	}
	cfg.Compression, err = parseCompression(take(v, "compression"))
	if err != nil {
		return nil, err
	}
	cfg.PartitionBy, err = takeCSV(v, "partition_by")
	if err != nil {
		return nil, err
	}
	if cfg.ForcePathStyle, err = takeBool(v, "force_path_style"); err != nil {
		return nil, err
	}
	if cfg.UseFips, err = takeBool(v, "use_fips"); err != nil {
		return nil, err
	}
	if cfg.UseDualstack, err = takeBool(v, "use_dualstack"); err != nil {
		return nil, err
	}
	cfg.SchemaRegistry, err = decodeRegistry(v)
	if err != nil {
		return nil, err
	}
	cfg.SchemaRegistrySubject = take(v, "schema_registry_subject")
	cfg.SchemaRegistryProtoTypesSubject = take(v, "schema_registry_proto_types_subject")
	cfg.TypeMappings, err = decodeTypeMappings(v)
	return cfg, err
}
func decodeHTTP(v map[string]string) (*wallabypb.HTTPDestinationConfig, error) {
	cfg := &wallabypb.HTTPDestinationConfig{Url: take(v, "url"), Method: take(v, "method"), IdempotencyHeader: take(v, "idempotency_header"), TransactionHeader: take(v, "transaction_header")}
	var err error
	cfg.Format, err = parseWireFormat(take(v, "format"))
	if err != nil {
		return nil, err
	}
	cfg.PayloadMode, err = parsePayloadMode(take(v, "payload_mode"))
	if err != nil {
		return nil, err
	}
	cfg.Timeout, err = takeDuration(v, "timeout")
	if err != nil {
		return nil, err
	}
	cfg.Headers, err = takeKeyValues(v, "headers", true)
	if err != nil {
		return nil, err
	}
	cfg.Retry, err = decodeRetry(v)
	if err != nil {
		return nil, err
	}
	cfg.DedupeWindow, err = takeDuration(v, "dedupe_window")
	if err != nil {
		return nil, err
	}
	cfg.SchemaRegistry, err = decodeRegistry(v)
	if err != nil {
		return nil, err
	}
	cfg.SchemaRegistrySubject = take(v, "schema_registry_subject")
	cfg.SchemaRegistryProtoTypesSubject = take(v, "schema_registry_proto_types_subject")
	cfg.TypeMappings, err = decodeTypeMappings(v)
	return cfg, err
}
func decodeGRPC(v map[string]string) (*wallabypb.GRPCDestinationConfig, error) {
	tls, err := decodeGRPCTLS(v)
	if err != nil {
		return nil, err
	}
	cfg := &wallabypb.GRPCDestinationConfig{Endpoint: take(v, "endpoint"), Tls: tls}
	cfg.Timeout, err = takeDuration(v, "timeout")
	if err != nil {
		return nil, err
	}
	cfg.Format, err = parseWireFormat(take(v, "format"))
	if err != nil {
		return nil, err
	}
	cfg.PayloadMode, err = parsePayloadMode(take(v, "payload_mode"))
	if err != nil {
		return nil, err
	}
	cfg.Metadata, err = takeKeyValues(v, "headers", false)
	if err != nil {
		return nil, err
	}
	cfg.Retry, err = decodeRetry(v)
	if err != nil {
		return nil, err
	}
	cfg.SchemaRegistry, err = decodeRegistry(v)
	if err != nil {
		return nil, err
	}
	cfg.SchemaRegistrySubject = take(v, "schema_registry_subject")
	cfg.SchemaRegistryProtoTypesSubject = take(v, "schema_registry_proto_types_subject")
	cfg.TypeMappings, err = decodeTypeMappings(v)
	return cfg, err
}
func decodeSnowflake(v map[string]string) (*wallabypb.SnowflakeDestinationConfig, error) {
	warehouse, err := decodeWarehouse(v)
	if err != nil {
		return nil, err
	}
	staging, err := decodeStaging(v)
	if err != nil {
		return nil, err
	}
	metadata, err := decodeMetadata(v)
	if err != nil {
		return nil, err
	}
	cfg := &wallabypb.SnowflakeDestinationConfig{Dsn: take(v, "dsn"), Warehouse: warehouse, Staging: staging, Metadata: metadata}
	if cfg.DisableTransactions, err = takeBool(v, "disable_transactions"); err != nil {
		return nil, err
	}
	cfg.SchemaRegistry, err = decodeRegistry(v)
	if err != nil {
		return nil, err
	}
	cfg.SchemaRegistrySubject = take(v, "schema_registry_subject")
	cfg.TypeMappings, err = decodeTypeMappings(v)
	return cfg, err
}
func decodeSnowflakeSQL(v map[string]string) (*wallabypb.SnowflakePostgresSQLConfig, error) {
	if err := consumeFixedOptions(v, snowflakeManagedFixedOptions()); err != nil {
		return nil, err
	}
	cfg := &wallabypb.SnowflakePostgresSQLConfig{Dsn: take(v, "dsn"), DestinationRevisionId: take(v, "destination_revision_id"), Account: take(v, "managed_account"), Database: take(v, "managed_database"), Schema: take(v, "managed_schema"), Table: take(v, "managed_table"), ReceiptsTable: take(v, "managed_receipts_table"), OwnerRole: take(v, "managed_owner_role"), ExecutionRole: take(v, "managed_execution_role"), ManagedWarehouse: take(v, "managed_warehouse"), SnowflakeVersion: take(v, "managed_snowflake_version"), TargetCreatedOn: take(v, "managed_target_created_on"), ReceiptsCreatedOn: take(v, "managed_receipts_created_on")}
	var err error
	cfg.MaxTransactionRows, err = takeU32(v, "managed_max_transaction_rows")
	if err != nil {
		return nil, err
	}
	cfg.MaxTransactionBytes, err = takeU64(v, "managed_max_transaction_bytes")
	if err != nil {
		return nil, err
	}
	cfg.MaxTransactionFragments, err = takeU32(v, "managed_max_transaction_fragments")
	if err != nil {
		return nil, err
	}
	cfg.MaxOpenConnections, err = takeU32(v, "managed_max_open_conns")
	if err != nil {
		return nil, err
	}
	cfg.StatementTimeoutSeconds, err = takeU32(v, "managed_statement_timeout_seconds")
	if err != nil {
		return nil, err
	}
	cfg.HybridTableLockTimeoutSeconds, err = takeU32(v, "managed_hybrid_table_lock_timeout_seconds")
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
func decodeSnowflakeStaged(v map[string]string) (*wallabypb.SnowflakePostgresStagedConfig, error) {
	if err := consumeFixedOptions(v, snowflakeManagedFixedOptions()); err != nil {
		return nil, err
	}
	cfg := &wallabypb.SnowflakePostgresStagedConfig{Dsn: take(v, "dsn"), Stage: take(v, "managed_stage"), FileFormat: take(v, "managed_file_format"), DestinationRevisionId: take(v, "destination_revision_id"), Account: take(v, "managed_account"), Database: take(v, "managed_database"), Schema: take(v, "managed_schema"), Table: take(v, "managed_table"), ReceiptsTable: take(v, "managed_receipts_table"), Pipe: take(v, "managed_pipe"), OwnerRole: take(v, "managed_owner_role"), ExecutionRole: take(v, "managed_execution_role"), ManagedWarehouse: take(v, "managed_warehouse"), SnowflakeVersion: take(v, "managed_snowflake_version"), StageCreatedOn: take(v, "managed_stage_created_on"), TargetCreatedOn: take(v, "managed_target_created_on"), ReceiptsCreatedOn: take(v, "managed_receipts_created_on"), FileFormatCreatedOn: take(v, "managed_file_format_created_on"), PipeCreatedOn: take(v, "managed_pipe_created_on")}
	var err error
	if cfg.AutoIngest, err = takeBool(v, "managed_auto_ingest"); err != nil {
		return nil, err
	}
	cfg.MaxTransactionRows, err = takeU32(v, "managed_max_transaction_rows")
	if err != nil {
		return nil, err
	}
	cfg.MaxTransactionBytes, err = takeU64(v, "managed_max_transaction_bytes")
	if err != nil {
		return nil, err
	}
	cfg.MaxTransactionFragments, err = takeU32(v, "managed_max_transaction_fragments")
	if err != nil {
		return nil, err
	}
	cfg.MaxOpenConnections, err = takeU32(v, "managed_max_open_conns")
	if err != nil {
		return nil, err
	}
	cfg.StatementTimeoutSeconds, err = takeU32(v, "managed_statement_timeout_seconds")
	if err != nil {
		return nil, err
	}
	cfg.LoadVerifyAttempts, err = takeU32(v, "managed_load_verify_attempts")
	if err != nil {
		return nil, err
	}
	cfg.LoadVerifyIntervalMillis, err = takeU32(v, "managed_load_verify_interval_ms")
	if err != nil {
		return nil, err
	}
	cfg.CleanupMaxObjects, err = takeU32(v, "managed_cleanup_max_objects")
	if err != nil {
		return nil, err
	}
	cfg.CleanupRetentionSeconds, err = takeU32(v, "managed_cleanup_retention_seconds")
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
func decodeSnowflakeStreaming(v map[string]string) (*wallabypb.SnowflakePostgresStreamingConfig, error) {
	if err := consumeFixedOptions(v, snowflakeManagedFixedOptions()); err != nil {
		return nil, err
	}
	cfg := &wallabypb.SnowflakePostgresStreamingConfig{Dsn: take(v, "dsn"), DestinationRevisionId: take(v, "destination_revision_id"), Transport: take(v, "managed_streaming_transport"), Account: take(v, "managed_account"), Database: take(v, "managed_database"), Schema: take(v, "managed_schema"), Pipe: take(v, "managed_pipe"), Table: take(v, "managed_table"), ReceiptsTable: take(v, "managed_receipts_table"), ChannelStateTable: take(v, "managed_channel_state_table"), ChannelNamePrefix: take(v, "managed_channel_name_prefix"), OwnerRole: take(v, "managed_owner_role"), ExecutionRole: take(v, "managed_execution_role"), ManagedWarehouse: take(v, "managed_warehouse"), SnowflakeVersion: take(v, "managed_snowflake_version"), PipeCreatedOn: take(v, "managed_pipe_created_on"), TargetCreatedOn: take(v, "managed_target_created_on"), ReceiptsCreatedOn: take(v, "managed_receipts_created_on"), ChannelStateCreatedOn: take(v, "managed_channel_state_created_on")}
	var err error
	cfg.MaxTransactionRows, err = takeU32(v, "managed_max_transaction_rows")
	if err != nil {
		return nil, err
	}
	cfg.MaxTransactionBytes, err = takeU64(v, "managed_max_transaction_bytes")
	if err != nil {
		return nil, err
	}
	cfg.MaxTransactionFragments, err = takeU32(v, "managed_max_transaction_fragments")
	if err != nil {
		return nil, err
	}
	cfg.MaxRowBytes, err = takeU64(v, "managed_max_row_bytes")
	if err != nil {
		return nil, err
	}
	cfg.MaxOpenConnections, err = takeU32(v, "managed_max_open_conns")
	if err != nil {
		return nil, err
	}
	cfg.StatementTimeoutSeconds, err = takeU32(v, "managed_statement_timeout_seconds")
	if err != nil {
		return nil, err
	}
	cfg.ObserveAttempts, err = takeU32(v, "managed_observe_attempts")
	if err != nil {
		return nil, err
	}
	cfg.ObserveIntervalMillis, err = takeU32(v, "managed_observe_interval_ms")
	if err != nil {
		return nil, err
	}
	cfg.AppendAttempts, err = takeU32(v, "managed_append_attempts")
	if err != nil {
		return nil, err
	}
	cfg.AppendBackoffMillis, err = takeU32(v, "managed_append_backoff_ms")
	if err != nil {
		return nil, err
	}
	cfg.CleanupMaxObjects, err = takeU32(v, "managed_cleanup_max_objects")
	if err != nil {
		return nil, err
	}
	cfg.CleanupRetentionSeconds, err = takeU32(v, "managed_cleanup_retention_seconds")
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
func decodeSnowpipe(v map[string]string) (*wallabypb.SnowpipeDestinationConfig, error) {
	warehouse, err := decodeWarehouse(v)
	if err != nil {
		return nil, err
	}
	metadata, err := decodeMetadata(v)
	if err != nil {
		return nil, err
	}
	cfg := &wallabypb.SnowpipeDestinationConfig{Dsn: take(v, "dsn"), Stage: take(v, "stage"), StagePath: take(v, "stage_path"), FileFormat: take(v, "file_format"), Warehouse: warehouse, CopyPattern: take(v, "copy_pattern"), CopyOnError: take(v, "copy_on_error"), CopyMatchByColumnName: take(v, "copy_match_by_column_name"), Metadata: metadata}
	cfg.Format, err = parseWireFormat(take(v, "format"))
	if err != nil {
		return nil, err
	}
	if cfg.CopyOnWrite, err = takeBool(v, "copy_on_write"); err != nil {
		return nil, err
	}
	if cfg.CopyPurge, err = takeBool(v, "copy_purge"); err != nil {
		return nil, err
	}
	if cfg.AutoIngest, err = takeBool(v, "auto_ingest"); err != nil {
		return nil, err
	}
	cfg.SchemaRegistry, err = decodeRegistry(v)
	if err != nil {
		return nil, err
	}
	cfg.SchemaRegistrySubject = take(v, "schema_registry_subject")
	cfg.TypeMappings, err = decodeTypeMappings(v)
	return cfg, err
}
func decodeClickHouse(v map[string]string) (*wallabypb.ClickHouseDestinationConfig, error) {
	tm, err := decodeTypeMappings(v)
	if err != nil {
		return nil, err
	}
	staging, err := decodeStaging(v)
	if err != nil {
		return nil, err
	}
	metadata, err := decodeClickHouseMetadata(v)
	if err != nil {
		return nil, err
	}
	return &wallabypb.ClickHouseDestinationConfig{Dsn: take(v, "dsn"), Staging: staging, Metadata: metadata, TypeMappings: tm}, nil
}
func decodeClickHouseManaged(v map[string]string) (*wallabypb.ClickHousePostgresAppendConfig, error) {
	if err := consumeFixedOptions(v, map[string]string{"managed_deployment": "self-managed-keeper", "batch_mode": "target", "batch_resolution": "none", "meta_table_enabled": "false", "async_insert": "false", "wait_for_async_insert": "true"}); err != nil {
		return nil, err
	}
	tm, err := decodeTypeMappings(v)
	if err != nil {
		return nil, err
	}
	tls, err := decodeClickHouseTLS(v)
	if err != nil {
		return nil, err
	}
	cfg := &wallabypb.ClickHousePostgresAppendConfig{Dsn: take(v, "dsn"), Tls: tls, DestinationRevisionId: take(v, "destination_revision_id"), TypeMappings: tm, Database: take(v, "managed_database"), ChangelogTable: take(v, "managed_changelog_table"), ReceiptsTable: take(v, "managed_receipts_table"), FinalView: take(v, "managed_final_view"), KeeperPathPrefix: take(v, "managed_keeper_path_prefix"), KeeperAddress: take(v, "managed_keeper_address"), ReplicaDsn: take(v, "managed_replica_dsn")}
	cfg.ReplicaNames, err = takeCSV(v, "managed_replica_names")
	if err != nil {
		return nil, err
	}
	cfg.InsertQuorum, err = takeU32(v, "insert_quorum")
	if err != nil {
		return nil, err
	}
	cfg.MaxActiveParts, err = takeU32(v, "managed_max_active_parts")
	if err != nil {
		return nil, err
	}
	cfg.MaxTransactionRows, err = takeU32(v, "managed_max_transaction_rows")
	if err != nil {
		return nil, err
	}
	cfg.MaxTransactionBytes, err = takeU64(v, "managed_max_transaction_bytes")
	if err != nil {
		return nil, err
	}
	cfg.MaxTransactionFragments, err = takeU32(v, "managed_max_transaction_fragments")
	if err != nil {
		return nil, err
	}
	cfg.MaxRowsPerBatch, err = takeU32(v, "managed_max_rows_per_batch")
	if err != nil {
		return nil, err
	}
	cfg.MaxBatchBytes, err = takeU64(v, "managed_max_batch_bytes")
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
func decodeDuckDB(v map[string]string) (*wallabypb.DuckDBDestinationConfig, error) {
	tm, err := decodeTypeMappings(v)
	if err != nil {
		return nil, err
	}
	staging, err := decodeStaging(v)
	if err != nil {
		return nil, err
	}
	metadata, err := decodeMetadata(v)
	if err != nil {
		return nil, err
	}
	return &wallabypb.DuckDBDestinationConfig{Dsn: take(v, "dsn"), Staging: staging, Metadata: metadata, TypeMappings: tm}, nil
}
func decodeDuckLake(v map[string]string) (*wallabypb.DuckLakeDestinationConfig, error) {
	staging, err := decodeStaging(v)
	if err != nil {
		return nil, err
	}
	metadata, err := decodeMetadata(v)
	if err != nil {
		return nil, err
	}
	cfg := &wallabypb.DuckLakeDestinationConfig{Dsn: take(v, "dsn"), Catalog: take(v, "catalog"), CatalogName: take(v, "catalog_name"), DataPath: take(v, "data_path"), Staging: staging, Metadata: metadata}
	if cfg.OverrideDataPath, err = takeBool(v, "override_data_path"); err != nil {
		return nil, err
	}
	if cfg.InstallExtensions, err = takeBool(v, "install_extensions"); err != nil {
		return nil, err
	}
	cfg.TypeMappings, err = decodeTypeMappings(v)
	return cfg, err
}
func decodeIceberg(v map[string]string) (*wallabypb.IcebergDestinationConfig, error) {
	profile, err := parseIcebergProfile(take(v, "catalog_profile"))
	if err != nil {
		return nil, err
	}
	return &wallabypb.IcebergDestinationConfig{CatalogProfile: profile, ControlTable: take(v, "control_table"), DestinationRevisionId: take(v, "destination_revision_id")}, nil
}

func decodePGConnection(v map[string]string) (*wallabypb.PostgresConnectionConfig, error) {
	cfg := &wallabypb.PostgresConnectionConfig{Dsn: take(v, "dsn")}
	var err error
	cfg.PoolMaxConnections, err = takeU32(v, "pool_max_conns")
	if err != nil {
		return nil, err
	}
	enabled, err := takeBool(v, "aws_rds_iam")
	if err != nil {
		return nil, err
	}
	if enabled != nil && *enabled {
		cfg.RdsIam = &wallabypb.RDSIAMConfig{Region: take(v, "aws_region"), Profile: take(v, "aws_profile"), RoleArn: take(v, "aws_role_arn"), RoleSessionName: take(v, "aws_role_session_name"), RoleExternalId: take(v, "aws_role_external_id"), Endpoint: take(v, "aws_endpoint")}
		if strings.TrimSpace(cfg.RdsIam.GetRegion()) == "" {
			return nil, errors.New("aws_region is required when aws_rds_iam=true")
		}
		if strings.TrimSpace(cfg.RdsIam.GetRoleArn()) != "" && strings.TrimSpace(cfg.RdsIam.GetRoleSessionName()) == "" {
			return nil, errors.New("aws_role_session_name is required when aws_role_arn is configured")
		}
	} else {
		for _, key := range []string{"aws_region", "aws_profile", "aws_role_arn", "aws_role_session_name", "aws_role_external_id", "aws_endpoint"} {
			if value := take(v, key); value != "" {
				return nil, fmt.Errorf("%s requires aws_rds_iam=true", key)
			}
		}
	}
	if cfg.Dsn == "" && cfg.PoolMaxConnections == nil && cfg.RdsIam == nil {
		return nil, nil //nolint:nilnil // absent optional connection message
	}
	return cfg, nil
}
func decodeStaging(v map[string]string) (*wallabypb.StagingConfig, error) {
	rawMode := take(v, "batch_mode")
	rawResolution := take(v, "batch_resolution")
	schema := take(v, "staging_schema")
	table := take(v, "staging_table")
	suffix := take(v, "staging_suffix")
	if rawMode == "" && rawResolution == "" && schema == "" && table == "" && suffix == "" {
		return nil, nil //nolint:nilnil // absent optional staging message
	}
	mode, err := parseBatchMode(rawMode)
	if err != nil {
		return nil, err
	}
	resolution, err := parseBatchResolution(rawResolution)
	if err != nil {
		return nil, err
	}
	return &wallabypb.StagingConfig{BatchMode: mode, BatchResolution: resolution, Schema: schema, Table: table, Suffix: suffix}, nil
}
func decodeMetadata(v map[string]string) (*wallabypb.MetadataConfig, error) {
	enabled, err := takeBool(v, "meta_table_enabled")
	if err != nil {
		return nil, err
	}
	schema := take(v, "meta_schema")
	table := take(v, "meta_table")
	prefix := take(v, "meta_pk_prefix")
	if enabled == nil && schema == "" && table == "" && prefix == "" {
		return nil, nil //nolint:nilnil // absent optional metadata message
	}
	return &wallabypb.MetadataConfig{Enabled: enabled, Schema: schema, Table: table, PrimaryKeyPrefix: prefix}, nil
}

func decodeClickHouseMetadata(v map[string]string) (*wallabypb.ClickHouseMetadataConfig, error) {
	enabled, err := takeBool(v, "meta_table_enabled")
	if err != nil {
		return nil, err
	}
	schema := take(v, "meta_schema")
	table := take(v, "meta_table")
	prefix := take(v, "meta_pk_prefix")
	engine := take(v, "meta_engine")
	order := take(v, "meta_order_by")
	if enabled == nil && schema == "" && table == "" && prefix == "" && engine == "" && order == "" {
		return nil, nil //nolint:nilnil // absent optional metadata message
	}
	return &wallabypb.ClickHouseMetadataConfig{Enabled: enabled, Schema: schema, Table: table, PrimaryKeyPrefix: prefix, Engine: engine, OrderBy: order}, nil
}
func decodeWarehouse(v map[string]string) (*wallabypb.WarehouseConfig, error) {
	name := take(v, "warehouse")
	size := take(v, "warehouse_size")
	suspend, err := takeU32(v, "warehouse_auto_suspend")
	if err != nil {
		return nil, err
	}
	resume, err := takeBool(v, "warehouse_auto_resume")
	if err != nil {
		return nil, err
	}
	keep, err := takeBool(v, "session_keep_alive")
	if err != nil {
		return nil, err
	}
	if name == "" && size == "" && suspend == nil && resume == nil && keep == nil {
		return nil, nil //nolint:nilnil // absent optional warehouse message
	}
	return &wallabypb.WarehouseConfig{Name: name, Size: size, AutoSuspendSeconds: suspend, AutoResume: resume, SessionKeepAlive: keep}, nil
}
func decodeGRPCTLS(v map[string]string) (*wallabypb.GRPCTLSConfig, error) {
	insecure, err := takeBool(v, "insecure")
	if err != nil {
		return nil, err
	}
	ca := take(v, "tls_ca_file")
	server := take(v, "tls_server_name")
	if insecure == nil && ca == "" && server == "" {
		return nil, nil //nolint:nilnil // absent optional TLS message
	}
	return &wallabypb.GRPCTLSConfig{Insecure: insecure, CaFile: ca, ServerName: server}, nil
}

func decodeClickHouseTLS(v map[string]string) (*wallabypb.ClickHouseTLSConfig, error) {
	insecure, err := takeBool(v, "insecure")
	if err != nil {
		return nil, err
	}
	ca := take(v, "tls_ca_file")
	server := take(v, "tls_server_name")
	cert := take(v, "tls_cert_file")
	key := take(v, "tls_key_file")
	replica := take(v, "managed_replica_tls_server_name")
	if insecure == nil && ca == "" && server == "" && cert == "" && key == "" && replica == "" {
		return nil, nil //nolint:nilnil // absent optional TLS message
	}
	return &wallabypb.ClickHouseTLSConfig{Insecure: insecure, CaFile: ca, ServerName: server, CertificateFile: cert, PrivateKeyFile: key, ReplicaServerName: replica}, nil
}
func decodeRetry(v map[string]string) (*wallabypb.RetryConfig, error) {
	max, err := takeU32(v, "max_retries")
	if err != nil {
		return nil, err
	}
	base, err := takeDuration(v, "backoff_base")
	if err != nil {
		return nil, err
	}
	upper, err := takeDuration(v, "backoff_max")
	if err != nil {
		return nil, err
	}
	factor, err := takeFloat(v, "backoff_factor")
	if err != nil {
		return nil, err
	}
	if max == nil && base == nil && upper == nil && factor == nil {
		return nil, nil //nolint:nilnil // an absent optional retry message is valid
	}
	return &wallabypb.RetryConfig{MaxRetries: max, BackoffBase: base, BackoffMax: upper, BackoffFactor: factor}, nil
}
func decodeTypeMappings(v map[string]string) (*wallabypb.TypeMappingsConfig, error) {
	raw := take(v, typemapping.OptTypeMappings)
	if raw == "" {
		return nil, nil //nolint:nilnil // an absent optional type-mapping message is valid
	}
	m, err := typemapping.Parse(raw)
	if err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	if string(canonical) != raw {
		return nil, errors.New("type_mappings must use canonical JSON with canonical keys and values")
	}
	return &wallabypb.TypeMappingsConfig{Mappings: m}, nil
}
func decodeRegistry(v map[string]string) (*wallabypb.SchemaRegistryConfig, error) {
	kind := take(v, "schema_registry")
	url := take(v, "schema_registry_url")
	localDirectory := take(v, schemaregistry.OptRegistryLocalDirectory)
	username := take(v, "schema_registry_username")
	password := take(v, "schema_registry_password")
	token := take(v, "schema_registry_token")
	dsn := take(v, "schema_registry_dsn")
	timeout, err := takeDuration(v, "schema_registry_timeout")
	if err != nil {
		return nil, err
	}
	compat, err := takeBool(v, "schema_registry_apicurio_compat")
	if err != nil {
		return nil, err
	}
	region := take(v, "schema_registry_region")
	endpoint := take(v, "schema_registry_endpoint")
	profile := take(v, "schema_registry_profile")
	role := take(v, "schema_registry_role_arn")
	registry := take(v, "schema_registry_glue_registry")
	schema := take(v, "schema_registry_glue_schema")
	present := map[string]bool{
		"url": url != "", "local_directory": localDirectory != "", "username": username != "", "password": password != "", "token": token != "", "dsn": dsn != "", "timeout": timeout != nil, "apicurio_compat": compat != nil,
		"region": region != "", "endpoint": endpoint != "", "profile": profile != "", "role_arn": role != "", "glue_registry": registry != "", "glue_schema": schema != "",
	}
	if kind == "" {
		for _, configured := range present {
			if configured {
				return nil, errors.New("schema_registry backend must be explicit")
			}
		}
		return nil, nil //nolint:nilnil // an absent optional registry message is valid
	}
	cfg := &wallabypb.SchemaRegistryConfig{}
	switch kind {
	case "csr":
		if err := rejectRegistryFields(kind, present, "url", "username", "password", "token", "timeout"); err != nil {
			return nil, err
		}
		cfg.Backend = &wallabypb.SchemaRegistryConfig_Confluent{Confluent: &wallabypb.ConfluentSchemaRegistryConfig{Url: url, Username: username, Password: password, Token: token, Timeout: timeout}}
	case "apicurio":
		if err := rejectRegistryFields(kind, present, "url", "username", "password", "token", "timeout", "apicurio_compat"); err != nil {
			return nil, err
		}
		cfg.Backend = &wallabypb.SchemaRegistryConfig_Apicurio{Apicurio: &wallabypb.ApicurioSchemaRegistryConfig{Url: url, Username: username, Password: password, Token: token, Timeout: timeout, Compatibility: compat}}
	case "glue":
		if err := rejectRegistryFields(kind, present, "region", "endpoint", "profile", "role_arn", "glue_registry", "glue_schema"); err != nil {
			return nil, err
		}
		cfg.Backend = &wallabypb.SchemaRegistryConfig_Glue{Glue: &wallabypb.GlueSchemaRegistryConfig{Region: region, Endpoint: endpoint, Profile: profile, RoleArn: role, Registry: registry, Schema: schema}}
	case "postgres":
		if err := rejectRegistryFields(kind, present, "dsn", "timeout"); err != nil {
			return nil, err
		}
		if strings.TrimSpace(dsn) == "" {
			return nil, errors.New("schema_registry_dsn is required for postgres registry")
		}
		cfg.Backend = &wallabypb.SchemaRegistryConfig_Postgres{Postgres: &wallabypb.PostgresSchemaRegistryConfig{Connection: &wallabypb.PostgresDSNConfig{Dsn: dsn}, Timeout: timeout}}
	case "local":
		if err := rejectRegistryFields(kind, present, "local_directory"); err != nil {
			return nil, err
		}
		if strings.TrimSpace(localDirectory) == "" {
			return nil, errors.New("schema_registry_local_directory is required for local registry")
		}
		cfg.Backend = &wallabypb.SchemaRegistryConfig_Local{Local: &wallabypb.LocalSchemaRegistryConfig{Directory: localDirectory}}
	default:
		return nil, fmt.Errorf("unsupported schema_registry backend %q", kind)
	}
	return cfg, nil
}

func rejectRegistryFields(kind string, present map[string]bool, allowed ...string) error {
	allow := make(map[string]struct{}, len(allowed))
	for _, field := range allowed {
		allow[field] = struct{}{}
	}
	for field, configured := range present {
		if !configured {
			continue
		}
		if _, ok := allow[field]; !ok {
			return fmt.Errorf("schema_registry backend %q rejects field %s", kind, field)
		}
	}
	return nil
}

func snowflakeManagedFixedOptions() map[string]string {
	return map[string]string{"batch_mode": "target", "batch_resolution": "none", "meta_table_enabled": "false", "disable_transactions": "false", "session_keep_alive": "false"}
}

func consumeFixedOptions(values map[string]string, expected map[string]string) error {
	for key, want := range expected {
		if raw, exists := values[key]; exists {
			delete(values, key)
			if raw != want {
				return fmt.Errorf("%s must be %q for the selected typed profile", key, want)
			}
		}
	}
	return nil
}

func put(out map[string]string, key, value string) {
	if value != "" {
		out[key] = value
	}
}
func putEnum(out map[string]string, key, value string) { put(out, key, value) }
func putBool(out map[string]string, key string, value *bool) {
	if value != nil {
		out[key] = strconv.FormatBool(*value)
	}
}
func putU32(out map[string]string, key string, value *uint32) {
	if value != nil {
		out[key] = strconv.FormatUint(uint64(*value), 10)
	}
}
func putU64(out map[string]string, key string, value *uint64) {
	if value != nil {
		out[key] = strconv.FormatUint(*value, 10)
	}
}
func putCSV(out map[string]string, key string, values []string) {
	if len(values) == 0 {
		return
	}
	var b strings.Builder
	w := csv.NewWriter(&b)
	_ = w.Write(values)
	w.Flush()
	out[key] = strings.TrimSuffix(b.String(), "\n")
}
func putDuration(out map[string]string, key string, value *durationpb.Duration) error {
	if value == nil {
		return nil
	}
	if err := value.CheckValid(); err != nil {
		return fmt.Errorf("%s: invalid duration: %w", key, err)
	}
	roundTrip := durationpb.New(value.AsDuration())
	if roundTrip.GetSeconds() != value.GetSeconds() || roundTrip.GetNanos() != value.GetNanos() {
		return fmt.Errorf("%s is outside time.Duration's exact round-trip range", key)
	}
	out[key] = value.AsDuration().String()
	return nil
}

func putPositiveDuration(out map[string]string, key string, value *durationpb.Duration) error {
	if value == nil {
		return nil
	}
	if err := putDuration(out, key, value); err != nil {
		return err
	}
	if value.AsDuration() <= 0 {
		delete(out, key)
		return fmt.Errorf("%s must be positive", key)
	}
	return nil
}
func putKeyValues(out map[string]string, key string, values map[string]string, httpHeaders bool) error {
	if len(values) == 0 {
		return nil
	}
	keys := sortedKeys(values)
	record := make([]string, 0, len(keys))
	for _, k := range keys {
		if strings.TrimSpace(k) == "" {
			return fmt.Errorf("%s contains an empty key", key)
		}
		record = append(record, k+":"+values[k])
	}
	var b strings.Builder
	w := csv.NewWriter(&b)
	if err := w.Write(record); err != nil {
		return err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	encoded := strings.TrimSuffix(b.String(), "\n")
	var parsed map[string]string
	var err error
	if httpHeaders {
		parsed, err = options.ParseHeaderList(encoded)
	} else {
		parsed, err = options.ParseCaseInsensitiveKeyValueList(encoded)
	}
	if err != nil {
		return err
	}
	if !maps.Equal(values, parsed) {
		return fmt.Errorf("%s keys and values must already use canonical spellings", key)
	}
	out[key] = encoded
	return nil
}
func take(v map[string]string, key string) string { value := v[key]; delete(v, key); return value }
func takeBool(v map[string]string, key string) (*bool, error) {
	raw, ok := v[key]
	delete(v, key)
	if !ok {
		return nil, nil //nolint:nilnil // absence is distinct from an explicit false value
	}
	if raw != "true" && raw != "false" {
		return nil, fmt.Errorf("%s must be the canonical boolean true or false", key)
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return nil, fmt.Errorf("%s must be a canonical boolean: %w", key, err)
	}
	return &value, nil
}
func takeU32(v map[string]string, key string) (*uint32, error) {
	raw, ok := v[key]
	delete(v, key)
	if !ok {
		return nil, nil //nolint:nilnil // absence is distinct from an explicit zero value
	}
	n, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("%s must be an unsigned 32-bit integer: %w", key, err)
	}
	if strconv.FormatUint(n, 10) != raw {
		return nil, fmt.Errorf("%s must use canonical base-10 syntax", key)
	}
	value := uint32(n)
	return &value, nil
}
func takeU64(v map[string]string, key string) (*uint64, error) {
	raw, ok := v[key]
	delete(v, key)
	if !ok {
		return nil, nil //nolint:nilnil // absence is distinct from an explicit zero value
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s must be an unsigned 64-bit integer: %w", key, err)
	}
	if strconv.FormatUint(n, 10) != raw {
		return nil, fmt.Errorf("%s must use canonical base-10 syntax", key)
	}
	return &n, nil
}
func takeFloat(v map[string]string, key string) (*float64, error) {
	raw, ok := v[key]
	delete(v, key)
	if !ok {
		return nil, nil //nolint:nilnil // absence is distinct from an explicit zero value
	}
	n, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
		return nil, fmt.Errorf("%s must be finite", key)
	}
	if strconv.FormatFloat(n, 'g', -1, 64) != raw {
		return nil, fmt.Errorf("%s must use canonical floating-point syntax", key)
	}
	return &n, nil
}
func takeDuration(v map[string]string, key string) (*durationpb.Duration, error) {
	raw, ok := v[key]
	delete(v, key)
	if !ok {
		return nil, nil //nolint:nilnil // an absent optional duration is valid
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	if d.String() != raw {
		return nil, fmt.Errorf("%s must use canonical Go duration syntax; got %q, canonical %q", key, raw, d.String())
	}
	return durationpb.New(d), nil
}

func takePositiveDuration(v map[string]string, key string) (*durationpb.Duration, error) {
	value, err := takeDuration(v, key)
	if err != nil || value == nil {
		return value, err
	}
	if value.AsDuration() <= 0 {
		return nil, fmt.Errorf("%s must be positive", key)
	}
	return value, nil
}
func takeCSV(v map[string]string, key string) ([]string, error) {
	raw, ok := v[key]
	delete(v, key)
	if !ok || raw == "" {
		return nil, nil
	}
	r := csv.NewReader(strings.NewReader(raw))
	record, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	var canonical strings.Builder
	writer := csv.NewWriter(&canonical)
	_ = writer.Write(record)
	writer.Flush()
	if strings.TrimSuffix(canonical.String(), "\n") != raw {
		return nil, fmt.Errorf("%s must use canonical CSV syntax", key)
	}
	return append([]string(nil), record...), nil
}
func takeKeyValues(v map[string]string, key string, httpHeaders bool) (map[string]string, error) {
	raw, ok := v[key]
	delete(v, key)
	if !ok || raw == "" {
		return nil, nil //nolint:nilnil // an absent optional key/value map is valid
	}
	var parsed map[string]string
	var err error
	if httpHeaders {
		parsed, err = options.ParseHeaderList(raw)
	} else {
		parsed, err = options.ParseCaseInsensitiveKeyValueList(raw)
	}
	if err != nil {
		return nil, err
	}
	canonical := make(map[string]string, len(parsed))
	for mapKey, value := range parsed {
		if mapKey != strings.ToLower(strings.TrimSpace(mapKey)) || value != strings.TrimSpace(value) {
			return nil, fmt.Errorf("%s must use canonical lowercase keys and trimmed values", key)
		}
		canonical[mapKey] = value
	}
	encoded := make(map[string]string)
	if err := putKeyValues(encoded, key, canonical, httpHeaders); err != nil {
		return nil, err
	}
	if encoded[key] != raw {
		return nil, fmt.Errorf("%s must use deterministic sorted canonical CSV syntax", key)
	}
	return canonical, nil
}
func cloneMap(in map[string]string) map[string]string {
	if in == nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func wireFormat(v wallabypb.WireFormat) string {
	switch v {
	case wallabypb.WireFormat_WIRE_FORMAT_ARROW:
		return "arrow"
	case wallabypb.WireFormat_WIRE_FORMAT_PARQUET:
		return "parquet"
	case wallabypb.WireFormat_WIRE_FORMAT_PROTO:
		return "proto"
	case wallabypb.WireFormat_WIRE_FORMAT_AVRO:
		return "avro"
	case wallabypb.WireFormat_WIRE_FORMAT_JSON:
		return "json"
	default:
		return ""
	}
}
func parseWireFormat(v string) (wallabypb.WireFormat, error) {
	switch v {
	case "":
		return wallabypb.WireFormat_WIRE_FORMAT_UNSPECIFIED, nil
	case "arrow":
		return wallabypb.WireFormat_WIRE_FORMAT_ARROW, nil
	case "parquet":
		return wallabypb.WireFormat_WIRE_FORMAT_PARQUET, nil
	case "proto":
		return wallabypb.WireFormat_WIRE_FORMAT_PROTO, nil
	case "avro":
		return wallabypb.WireFormat_WIRE_FORMAT_AVRO, nil
	case "json":
		return wallabypb.WireFormat_WIRE_FORMAT_JSON, nil
	default:
		return 0, fmt.Errorf("unsupported format %q", v)
	}
}
func compression(v wallabypb.Compression) string {
	switch v {
	case wallabypb.Compression_COMPRESSION_NONE:
		return "none"
	case wallabypb.Compression_COMPRESSION_GZIP:
		return "gzip"
	case wallabypb.Compression_COMPRESSION_SNAPPY:
		return "snappy"
	case wallabypb.Compression_COMPRESSION_LZ4:
		return "lz4"
	case wallabypb.Compression_COMPRESSION_ZSTD:
		return "zstd"
	default:
		return ""
	}
}
func parseCompression(v string) (wallabypb.Compression, error) {
	switch v {
	case "":
		return 0, nil
	case "none":
		return wallabypb.Compression_COMPRESSION_NONE, nil
	case "gzip":
		return wallabypb.Compression_COMPRESSION_GZIP, nil
	case "snappy":
		return wallabypb.Compression_COMPRESSION_SNAPPY, nil
	case "lz4":
		return wallabypb.Compression_COMPRESSION_LZ4, nil
	case "zstd":
		return wallabypb.Compression_COMPRESSION_ZSTD, nil
	default:
		return 0, fmt.Errorf("unsupported compression %q", v)
	}
}
func payloadMode(v wallabypb.PayloadMode) string {
	switch v {
	case wallabypb.PayloadMode_PAYLOAD_MODE_WIRE:
		return "wire"
	case wallabypb.PayloadMode_PAYLOAD_MODE_RECORD_JSON:
		return "record_json"
	case wallabypb.PayloadMode_PAYLOAD_MODE_WAL:
		return "wal"
	default:
		return ""
	}
}
func parsePayloadMode(v string) (wallabypb.PayloadMode, error) {
	switch v {
	case "":
		return 0, nil
	case "wire":
		return wallabypb.PayloadMode_PAYLOAD_MODE_WIRE, nil
	case "record_json":
		return wallabypb.PayloadMode_PAYLOAD_MODE_RECORD_JSON, nil
	case "wal":
		return wallabypb.PayloadMode_PAYLOAD_MODE_WAL, nil
	default:
		return 0, fmt.Errorf("unsupported payload_mode %q", v)
	}
}
func kafkaAcks(v wallabypb.KafkaAcks) string {
	switch v {
	case wallabypb.KafkaAcks_KAFKA_ACKS_NONE:
		return "none"
	case wallabypb.KafkaAcks_KAFKA_ACKS_LEADER:
		return "leader"
	case wallabypb.KafkaAcks_KAFKA_ACKS_ALL:
		return "all"
	default:
		return ""
	}
}
func parseKafkaAcks(v string) (wallabypb.KafkaAcks, error) {
	switch v {
	case "":
		return 0, nil
	case "none":
		return wallabypb.KafkaAcks_KAFKA_ACKS_NONE, nil
	case "leader":
		return wallabypb.KafkaAcks_KAFKA_ACKS_LEADER, nil
	case "all":
		return wallabypb.KafkaAcks_KAFKA_ACKS_ALL, nil
	default:
		return 0, fmt.Errorf("unsupported acks %q", v)
	}
}
func kafkaMessageMode(v wallabypb.KafkaMessageMode) string {
	switch v {
	case wallabypb.KafkaMessageMode_KAFKA_MESSAGE_MODE_BATCH:
		return "batch"
	case wallabypb.KafkaMessageMode_KAFKA_MESSAGE_MODE_RECORD:
		return "record"
	default:
		return ""
	}
}
func parseKafkaMessageMode(v string) (wallabypb.KafkaMessageMode, error) {
	switch v {
	case "":
		return 0, nil
	case "batch":
		return wallabypb.KafkaMessageMode_KAFKA_MESSAGE_MODE_BATCH, nil
	case "record":
		return wallabypb.KafkaMessageMode_KAFKA_MESSAGE_MODE_RECORD, nil
	default:
		return 0, fmt.Errorf("unsupported message_mode %q", v)
	}
}
func kafkaKeyMode(v wallabypb.KafkaKeyMode) string {
	switch v {
	case wallabypb.KafkaKeyMode_KAFKA_KEY_MODE_HASH:
		return "hash"
	case wallabypb.KafkaKeyMode_KAFKA_KEY_MODE_RAW:
		return "raw"
	default:
		return ""
	}
}
func parseKafkaKeyMode(v string) (wallabypb.KafkaKeyMode, error) {
	switch v {
	case "":
		return 0, nil
	case "hash":
		return wallabypb.KafkaKeyMode_KAFKA_KEY_MODE_HASH, nil
	case "raw":
		return wallabypb.KafkaKeyMode_KAFKA_KEY_MODE_RAW, nil
	default:
		return 0, fmt.Errorf("unsupported key_mode %q", v)
	}
}
func batchMode(v wallabypb.BatchMode) string {
	switch v {
	case wallabypb.BatchMode_BATCH_MODE_STAGING:
		return "staging"
	case wallabypb.BatchMode_BATCH_MODE_TARGET:
		return "target"
	default:
		return ""
	}
}
func parseBatchMode(v string) (wallabypb.BatchMode, error) {
	switch v {
	case "":
		return 0, nil
	case "staging":
		return wallabypb.BatchMode_BATCH_MODE_STAGING, nil
	case "target":
		return wallabypb.BatchMode_BATCH_MODE_TARGET, nil
	default:
		return 0, fmt.Errorf("unsupported batch_mode %q", v)
	}
}
func batchResolution(v wallabypb.BatchResolution) string {
	switch v {
	case wallabypb.BatchResolution_BATCH_RESOLUTION_NONE:
		return "none"
	case wallabypb.BatchResolution_BATCH_RESOLUTION_APPEND:
		return "append"
	case wallabypb.BatchResolution_BATCH_RESOLUTION_REPLACE:
		return "replace"
	default:
		return ""
	}
}
func parseBatchResolution(v string) (wallabypb.BatchResolution, error) {
	switch v {
	case "":
		return 0, nil
	case "none":
		return wallabypb.BatchResolution_BATCH_RESOLUTION_NONE, nil
	case "append":
		return wallabypb.BatchResolution_BATCH_RESOLUTION_APPEND, nil
	case "replace":
		return wallabypb.BatchResolution_BATCH_RESOLUTION_REPLACE, nil
	default:
		return 0, fmt.Errorf("unsupported batch_resolution %q", v)
	}
}
func syncPublicationMode(v wallabypb.SyncPublicationMode) string {
	switch v {
	case wallabypb.SyncPublicationMode_SYNC_PUBLICATION_MODE_ADD:
		return "add"
	case wallabypb.SyncPublicationMode_SYNC_PUBLICATION_MODE_SYNC:
		return "sync"
	default:
		return ""
	}
}
func parseSyncPublicationMode(v string) (wallabypb.SyncPublicationMode, error) {
	switch v {
	case "":
		return 0, nil
	case "add":
		return wallabypb.SyncPublicationMode_SYNC_PUBLICATION_MODE_ADD, nil
	case "sync":
		return wallabypb.SyncPublicationMode_SYNC_PUBLICATION_MODE_SYNC, nil
	default:
		return 0, fmt.Errorf("unsupported sync_publication_mode %q", v)
	}
}
func postgresSourceMode(v wallabypb.PostgresSourceMode) (string, error) {
	switch v {
	case wallabypb.PostgresSourceMode_POSTGRES_SOURCE_MODE_UNSPECIFIED:
		return "", errors.New("postgres_source.mode must be POSTGRES_SOURCE_MODE_CDC or POSTGRES_SOURCE_MODE_BACKFILL")
	case wallabypb.PostgresSourceMode_POSTGRES_SOURCE_MODE_CDC:
		return connector.SourceModeCDC, nil
	case wallabypb.PostgresSourceMode_POSTGRES_SOURCE_MODE_BACKFILL:
		return connector.SourceModeBackfill, nil
	default:
		return "", fmt.Errorf("unsupported postgres source mode %d", v)
	}
}
func parsePostgresSourceMode(v string) (wallabypb.PostgresSourceMode, error) {
	switch v {
	case "":
		return wallabypb.PostgresSourceMode_POSTGRES_SOURCE_MODE_CDC, nil
	case connector.SourceModeCDC:
		return wallabypb.PostgresSourceMode_POSTGRES_SOURCE_MODE_CDC, nil
	case connector.SourceModeBackfill:
		return wallabypb.PostgresSourceMode_POSTGRES_SOURCE_MODE_BACKFILL, nil
	default:
		return 0, fmt.Errorf("unsupported postgres source mode %q", v)
	}
}
func toastFetch(v wallabypb.ToastFetchMode) string {
	switch v {
	case wallabypb.ToastFetchMode_TOAST_FETCH_MODE_OFF:
		return "off"
	case wallabypb.ToastFetchMode_TOAST_FETCH_MODE_SOURCE:
		return "source"
	case wallabypb.ToastFetchMode_TOAST_FETCH_MODE_CACHE:
		return "cache"
	case wallabypb.ToastFetchMode_TOAST_FETCH_MODE_FULL:
		return "full"
	default:
		return ""
	}
}
func parseToastFetch(v string) (wallabypb.ToastFetchMode, error) {
	switch v {
	case "":
		return 0, nil
	case "off":
		return wallabypb.ToastFetchMode_TOAST_FETCH_MODE_OFF, nil
	case "source":
		return wallabypb.ToastFetchMode_TOAST_FETCH_MODE_SOURCE, nil
	case "cache":
		return wallabypb.ToastFetchMode_TOAST_FETCH_MODE_CACHE, nil
	case "full":
		return wallabypb.ToastFetchMode_TOAST_FETCH_MODE_FULL, nil
	default:
		return 0, fmt.Errorf("unsupported toast_fetch %q", v)
	}
}
func bootstrapMode(v wallabypb.BootstrapMode) string {
	switch v {
	case wallabypb.BootstrapMode_BOOTSTRAP_MODE_NEVER:
		return "never"
	case wallabypb.BootstrapMode_BOOTSTRAP_MODE_AUTO:
		return "auto"
	case wallabypb.BootstrapMode_BOOTSTRAP_MODE_REQUIRED:
		return "required"
	default:
		return ""
	}
}
func parseBootstrap(v string) (wallabypb.BootstrapMode, error) {
	switch v {
	case "":
		return 0, nil
	case "never":
		return wallabypb.BootstrapMode_BOOTSTRAP_MODE_NEVER, nil
	case "auto":
		return wallabypb.BootstrapMode_BOOTSTRAP_MODE_AUTO, nil
	case "required":
		return wallabypb.BootstrapMode_BOOTSTRAP_MODE_REQUIRED, nil
	default:
		return 0, fmt.Errorf("unsupported bootstrap %q", v)
	}
}
func managedProfile(v wallabypb.ManagedProfile) string {
	switch v {
	case wallabypb.ManagedProfile_MANAGED_PROFILE_POSTGRES_TO_POSTGRES_V1:
		return connector.ManagedProfilePostgresToPostgresV1
	case wallabypb.ManagedProfile_MANAGED_PROFILE_POSTGRES_TO_CLICKHOUSE_APPEND_V1:
		return connector.ManagedProfilePostgresToClickHouseAppendV1
	case wallabypb.ManagedProfile_MANAGED_PROFILE_POSTGRESQL_TO_SNOWFLAKE_SQL_V1:
		return connector.ManagedProfilePostgresToSnowflakeSQLV1
	case wallabypb.ManagedProfile_MANAGED_PROFILE_POSTGRESQL_TO_SNOWFLAKE_STAGED_V1:
		return connector.ManagedProfilePostgresToSnowflakeStagedAppendV1
	case wallabypb.ManagedProfile_MANAGED_PROFILE_POSTGRESQL_TO_SNOWFLAKE_STREAMING_V1:
		return connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1
	default:
		return ""
	}
}
func parseManagedProfile(v string) (wallabypb.ManagedProfile, error) {
	switch v {
	case "":
		return 0, nil
	case connector.ManagedProfilePostgresToPostgresV1:
		return wallabypb.ManagedProfile_MANAGED_PROFILE_POSTGRES_TO_POSTGRES_V1, nil
	case connector.ManagedProfilePostgresToClickHouseAppendV1:
		return wallabypb.ManagedProfile_MANAGED_PROFILE_POSTGRES_TO_CLICKHOUSE_APPEND_V1, nil
	case connector.ManagedProfilePostgresToSnowflakeSQLV1:
		return wallabypb.ManagedProfile_MANAGED_PROFILE_POSTGRESQL_TO_SNOWFLAKE_SQL_V1, nil
	case connector.ManagedProfilePostgresToSnowflakeStagedAppendV1:
		return wallabypb.ManagedProfile_MANAGED_PROFILE_POSTGRESQL_TO_SNOWFLAKE_STAGED_V1, nil
	case connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1:
		return wallabypb.ManagedProfile_MANAGED_PROFILE_POSTGRESQL_TO_SNOWFLAKE_STREAMING_V1, nil
	default:
		return 0, fmt.Errorf("unsupported managed_profile %q", v)
	}
}
func icebergProfile(v wallabypb.IcebergCatalogProfile) string {
	switch v {
	case wallabypb.IcebergCatalogProfile_ICEBERG_CATALOG_PROFILE_REST:
		return "rest"
	case wallabypb.IcebergCatalogProfile_ICEBERG_CATALOG_PROFILE_S3_TABLES:
		return "s3tables"
	default:
		return ""
	}
}
func parseIcebergProfile(v string) (wallabypb.IcebergCatalogProfile, error) {
	switch v {
	case "":
		return 0, nil
	case "rest":
		return wallabypb.IcebergCatalogProfile_ICEBERG_CATALOG_PROFILE_REST, nil
	case "s3tables":
		return wallabypb.IcebergCatalogProfile_ICEBERG_CATALOG_PROFILE_S3_TABLES, nil
	default:
		return 0, fmt.Errorf("unsupported iceberg catalog_profile %q", v)
	}
}
