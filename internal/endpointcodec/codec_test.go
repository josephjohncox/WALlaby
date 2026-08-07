package endpointcodec_test

import (
	"reflect"
	"testing"

	chdest "github.com/josephjohncox/wallaby/connectors/destinations/clickhouse"
	sfdest "github.com/josephjohncox/wallaby/connectors/destinations/snowflake"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestTypedEndpointRoundTripsCanonicalRuntimeSpecs(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		role endpointcodec.Role
		spec connector.RuntimeSpec
	}{
		{name: "postgres source", role: endpointcodec.RoleSource, spec: connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres, Options: map[string]string{"mode": connector.SourceModeCDC, "dsn": "postgres://source", "batch_size": "42", "batch_timeout": "1.25s", "create_slot": "false", "publication_tables": "public.a,public.b", "snapshot_workers": "3", "managed": "true", "bootstrap": "never", "aws_rds_iam": "true", "aws_region": "us-east-1", "format": "arrow"}}},
		{name: "postgres destination", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "postgres", Type: connector.EndpointPostgres, Options: map[string]string{"dsn": "postgres://destination", "batch_mode": "target", "meta_table_enabled": "false", "managed_profile": connector.ManagedProfilePostgresToPostgresV1, "destination_revision_id": "postgres-v1", "type_mappings": `{"jsonb":"json"}`}}},
		{name: "pgstream", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "stream", Type: connector.EndpointPGStream, Options: map[string]string{"dsn": "postgres://stream", "stream": "events", "format": "proto"}}},
		{name: "kafka", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "kafka", Type: connector.EndpointKafka, Options: map[string]string{"brokers": "a:9092,b:9092", "topic": "events", "format": "avro", "compression": "zstd", "acks": "all", "transactional_producer": "true", "transactional_id": "tx", "transaction_timeout": "30s", "message_mode": "record", "key_mode": "hash", "schema_registry": "csr", "schema_registry_url": "https://registry", "schema_registry_subject": "events"}}},
		{name: "kafka local registry", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "kafka-local", Type: connector.EndpointKafka, Options: map[string]string{"brokers": "a:9092", "topic": "events", "format": "avro", "schema_registry": "local", schemaregistry.OptRegistryLocalDirectory: "/var/lib/wallaby/registry"}}},
		{name: "redpanda", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "redpanda", Type: connector.EndpointRedpanda, Options: map[string]string{"brokers": "redpanda:9092", "topic": "events", "acks": "leader"}}},
		{name: "s3", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "s3", Type: connector.EndpointS3, Options: map[string]string{"bucket": "bucket", "partition_by": "schema,table", "force_path_style": "true", "compression": "gzip"}}},
		{name: "http", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "http", Type: connector.EndpointHTTP, Options: map[string]string{"url": "https://example.test/hook", "payload_mode": "record_json", "headers": "authorization:secret,x-test:value", "max_retries": "4", "backoff_factor": "1.5", "timeout": "2s"}}},
		{name: "grpc", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "grpc", Type: connector.EndpointGRPC, Options: map[string]string{"endpoint": "example.test:443", "payload_mode": "wire", "headers": "x-test:value", "insecure": "false", "timeout": "3s"}}},
		{name: "snowflake", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "snowflake", Type: connector.EndpointSnowflake, Options: map[string]string{"dsn": "snowflake", "disable_transactions": "true", "warehouse_auto_suspend": "60"}}},
		{name: "snowflake sql", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "snowflake-sql", Type: connector.EndpointSnowflake, Options: map[string]string{"managed_profile": connector.ManagedProfilePostgresToSnowflakeSQLV1, "dsn": "snowflake", "destination_revision_id": "v1", "batch_mode": "target", "batch_resolution": "none", "meta_table_enabled": "false", "disable_transactions": "false", "session_keep_alive": "false"}}},
		{name: "snowflake staged", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "snowflake-staged", Type: connector.EndpointSnowflake, Options: map[string]string{"managed_profile": connector.ManagedProfilePostgresToSnowflakeStagedAppendV1, "dsn": "snowflake", "managed_stage": "stage", "destination_revision_id": "v1", "batch_mode": "target", "batch_resolution": "none", "meta_table_enabled": "false", "disable_transactions": "false", "session_keep_alive": "false"}}},
		{name: "snowflake streaming", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "snowflake-streaming", Type: connector.EndpointSnowflake, Options: map[string]string{"managed_profile": connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1, "dsn": "snowflake", "destination_revision_id": "v1", "batch_mode": "target", "batch_resolution": "none", "meta_table_enabled": "false", "disable_transactions": "false", "session_keep_alive": "false"}}},
		{name: "snowpipe", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "snowpipe", Type: connector.EndpointSnowpipe, Options: map[string]string{"dsn": "snowflake", "stage": "stage", "copy_purge": "false", "format": "parquet"}}},
		{name: "clickhouse", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "clickhouse", Type: connector.EndpointClickHouse, Options: map[string]string{"dsn": "clickhouse", "meta_schema": "wallaby"}}},
		{name: "clickhouse managed", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "clickhouse-managed", Type: connector.EndpointClickHouse, Options: map[string]string{"managed_profile": connector.ManagedProfilePostgresToClickHouseAppendV1, "dsn": "clickhouse", "tls_server_name": "clickhouse.example", "destination_revision_id": "v1", "managed_deployment": "self-managed-keeper", "batch_mode": "target", "batch_resolution": "none", "meta_table_enabled": "false", "async_insert": "false", "wait_for_async_insert": "true"}}},
		{name: "duckdb", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "duckdb", Type: connector.EndpointDuckDB, Options: map[string]string{"dsn": "db.duckdb", "meta_table_enabled": "true"}}},
		{name: "ducklake", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "ducklake", Type: connector.EndpointDuckLake, Options: map[string]string{"dsn": "ducklake", "catalog": "postgres", "override_data_path": "false", "install_extensions": "true"}}},
		{name: "iceberg", role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "iceberg", Type: connector.EndpointIceberg, Options: map[string]string{"catalog_profile": "s3tables", "control_table": "control", "destination_revision_id": "v1"}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			endpoint, err := endpointcodec.Encode(test.spec, test.role)
			if err != nil {
				t.Fatal(err)
			}
			roundTrip, err := endpointcodec.Decode(endpoint, test.role)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(roundTrip, test.spec) {
				t.Fatalf("round trip mismatch\ngot:  %#v\nwant: %#v", roundTrip, test.spec)
			}
			if len(test.spec.Options) > 0 {
				for key := range roundTrip.Options {
					roundTrip.Options[key] = "mutated"
					break
				}
				if reflect.DeepEqual(roundTrip.Options, test.spec.Options) {
					t.Fatal("decoded options alias caller-owned map")
				}
			}
		})
	}
}

func TestEveryRunnableFirstPartyDestinationHasTypedBranch(t *testing.T) {
	t.Parallel()
	for _, registration := range runner.DestinationRegistrations() {
		if registration.New == nil {
			continue
		}
		endpoint, err := endpointcodec.Encode(connector.RuntimeSpec{Name: string(registration.Type), Type: registration.Type}, endpointcodec.RoleDestination)
		if err != nil {
			t.Fatalf("%s: %v", registration.Type, err)
		}
		if _, custom := endpoint.GetConfig().(*wallabypb.Endpoint_Custom); custom {
			t.Fatalf("first-party destination %s encoded through custom", registration.Type)
		}
	}
}

func TestBuiltInTypeMappingsExposeOnlyNativeInlineMap(t *testing.T) {
	t.Parallel()
	descriptor := (&wallabypb.TypeMappingsConfig{}).ProtoReflect().Descriptor()
	if descriptor.Oneofs().Len() != 0 || descriptor.Fields().Len() != 1 {
		t.Fatalf("TypeMappingsConfig shape has %d oneofs and %d fields", descriptor.Oneofs().Len(), descriptor.Fields().Len())
	}
	field := descriptor.Fields().Get(0)
	if field.Name() != "mappings" || !field.IsMap() || field.MapKey().Kind() != protoreflect.StringKind || field.MapValue().Kind() != protoreflect.StringKind {
		t.Fatalf("TypeMappingsConfig field=%v, want native map<string,string> mappings", field)
	}
}

func TestSnapshotAndRegistryPostgresBackendsExposeOnlyDSN(t *testing.T) {
	t.Parallel()
	registryConnection := (&wallabypb.PostgresSchemaRegistryConfig{}).ProtoReflect().Descriptor().Fields().ByName("connection").Message()
	snapshotPostgres := (&wallabypb.SnapshotStateConfig{}).ProtoReflect().Descriptor().Oneofs().ByName("backend").Fields().ByName("postgres").Message()
	for name, descriptor := range map[string]protoreflect.MessageDescriptor{"registry": registryConnection, "snapshot": snapshotPostgres} {
		if descriptor.Fields().Len() != 1 || descriptor.Fields().ByName("dsn") == nil || descriptor.Fields().ByName("pool_max_connections") != nil || descriptor.Fields().ByName("rds_iam") != nil {
			t.Fatalf("%s postgres backend descriptor=%s fields=%v, want dsn only", name, descriptor.FullName(), descriptor.Fields())
		}
	}
}

func TestOnlyCustomEndpointConfigContainsArbitraryOptions(t *testing.T) {
	t.Parallel()
	seen := 0
	walkMessages(wallabypb.File_wallaby_v1_types_proto.Messages(), func(message protoreflect.MessageDescriptor) {
		fields := message.Fields()
		for index := 0; index < fields.Len(); index++ {
			field := fields.Get(index)
			if field.Name() != "options" || !field.IsMap() {
				continue
			}
			seen++
			if message.FullName() != "wallaby.v1.CustomEndpointConfig" || field.MapKey().Kind() != protoreflect.StringKind || field.MapValue().Kind() != protoreflect.StringKind {
				t.Errorf("arbitrary options map found at %s.%s", message.FullName(), field.Name())
			}
		}
	})
	if seen != 1 {
		t.Fatalf("arbitrary options maps=%d, want exactly 1", seen)
	}
}

func TestRoleValidationAndLegacyAliasesFailClosed(t *testing.T) {
	t.Parallel()
	if _, err := endpointcodec.Decode(&wallabypb.Endpoint{Config: &wallabypb.Endpoint_PostgresDestination{PostgresDestination: &wallabypb.PostgresDestinationConfig{}}}, endpointcodec.RoleSource); err == nil {
		t.Fatal("destination branch accepted as source")
	}
	for key, value := range map[string]string{"address": "example:443", "payload_mode": "raw", "acks": "-1", "parallel_tables": "2", "meta_database": "wallaby"} {
		t.Run(key, func(t *testing.T) {
			typeName := connector.EndpointGRPC
			if key == "acks" {
				typeName = connector.EndpointKafka
			}
			if key == "parallel_tables" {
				typeName = connector.EndpointPostgres
			}
			if key == "meta_database" {
				typeName = connector.EndpointClickHouse
			}
			role := endpointcodec.RoleDestination
			if key == "parallel_tables" {
				role = endpointcodec.RoleSource
			}
			if _, err := endpointcodec.Encode(connector.RuntimeSpec{Type: typeName, Options: map[string]string{key: value}}, role); err == nil {
				t.Fatalf("legacy alias %s was accepted", key)
			}
		})
	}
}

func TestProtocolEnumsMapOnlyToExactRuntimeValues(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		cfg  *wallabypb.StagingConfig
		want string
	}{
		{name: "none", cfg: &wallabypb.StagingConfig{BatchResolution: wallabypb.BatchResolution(1)}, want: "none"},
		{name: "append", cfg: &wallabypb.StagingConfig{BatchResolution: wallabypb.BatchResolution(2)}, want: "append"},
		{name: "replace", cfg: &wallabypb.StagingConfig{BatchResolution: wallabypb.BatchResolution(3)}, want: "replace"},
	} {
		t.Run(test.name, func(t *testing.T) {
			spec, err := endpointcodec.Decode(&wallabypb.Endpoint{Config: &wallabypb.Endpoint_Duckdb{Duckdb: &wallabypb.DuckDBDestinationConfig{Staging: test.cfg}}}, endpointcodec.RoleDestination)
			if err != nil {
				t.Fatal(err)
			}
			if spec.Options["batch_resolution"] != test.want {
				t.Fatalf("batch_resolution=%q, want %q", spec.Options["batch_resolution"], test.want)
			}
		})
	}
	for _, old := range []string{"latest", "all", "reconcile", "add_only"} {
		options := map[string]string{"batch_resolution": old}
		if old == "reconcile" || old == "add_only" {
			options = map[string]string{"mode": connector.SourceModeCDC, "sync_publication_mode": old}
			if _, err := endpointcodec.Encode(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: options}, endpointcodec.RoleSource); err == nil {
				t.Fatalf("removed runtime enum alias %q was accepted", old)
			}
			continue
		}
		if _, err := endpointcodec.Encode(connector.RuntimeSpec{Type: connector.EndpointDuckDB, Options: options}, endpointcodec.RoleDestination); err == nil {
			t.Fatalf("removed runtime enum alias %q was accepted", old)
		}
	}

	for mode, want := range map[wallabypb.SyncPublicationMode]string{
		wallabypb.SyncPublicationMode(1): "add",
		wallabypb.SyncPublicationMode(2): "sync",
	} {
		cfg := postgresSourceConfigWithMode(t, 1)
		cfg.SyncPublicationMode = mode
		spec, err := endpointcodec.Decode(&wallabypb.Endpoint{Config: &wallabypb.Endpoint_PostgresSource{PostgresSource: cfg}}, endpointcodec.RoleSource)
		if err != nil {
			t.Fatal(err)
		}
		if spec.Options["sync_publication_mode"] != want {
			t.Fatalf("sync mode=%q, want %q", spec.Options["sync_publication_mode"], want)
		}
	}
}

func TestPostgresSourceModeSeparatesCDCAndBackfillSelection(t *testing.T) {
	t.Parallel()
	cdcConfig := postgresSourceConfigWithMode(t, 1)
	cdcConfig.PublicationTables = []string{"public.cdc"}
	cdcConfig.PublicationSchemas = []string{"public"}
	cdcConfig.Bootstrap = wallabypb.BootstrapMode_BOOTSTRAP_MODE_REQUIRED
	cdcConfig.BootstrapTables = []string{`"Mixed Schema"."Odd, Table"`}
	cdcConfig.BootstrapSchemas = []string{`" bootstrap "`}
	cdc, err := endpointcodec.Decode(&wallabypb.Endpoint{Config: &wallabypb.Endpoint_PostgresSource{PostgresSource: cdcConfig}}, endpointcodec.RoleSource)
	if err != nil {
		t.Fatal(err)
	}
	if cdc.Options["mode"] != connector.SourceModeCDC || cdc.Options["publication_tables"] == "" || cdc.Options["tables"] == "" || cdc.Options["schemas"] == "" {
		t.Fatalf("CDC runtime options=%v", cdc.Options)
	}
	cdcRoundTrip, err := endpointcodec.Encode(cdc, endpointcodec.RoleSource)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(cdcRoundTrip.GetPostgresSource(), cdcConfig) {
		t.Fatalf("CDC bootstrap selection did not round-trip\ngot:  %v\nwant: %v", cdcRoundTrip.GetPostgresSource(), cdcConfig)
	}
	backfillConfig := postgresSourceConfigWithMode(t, 2)
	backfillConfig.BackfillTables = []string{"public.snapshot"}
	backfillConfig.BackfillSchemas = []string{"archive"}
	backfill, err := endpointcodec.Decode(&wallabypb.Endpoint{Config: &wallabypb.Endpoint_PostgresSource{PostgresSource: backfillConfig}}, endpointcodec.RoleSource)
	if err != nil {
		t.Fatal(err)
	}
	if backfill.Options["mode"] != connector.SourceModeBackfill || backfill.Options["tables"] == "" || backfill.Options["schemas"] == "" || backfill.Options["publication_tables"] != "" {
		t.Fatalf("backfill runtime options=%v", backfill.Options)
	}
	invalidCDC := postgresSourceConfigWithMode(t, 1)
	invalidCDC.BackfillTables = []string{"public.bad"}
	invalidBootstrap := postgresSourceConfigWithMode(t, 1)
	invalidBootstrap.BootstrapTables = []string{"public.bad"}
	invalidBackfill := postgresSourceConfigWithMode(t, 2)
	invalidBackfill.PublicationTables = []string{"public.bad"}
	invalidBackfill.BootstrapTables = []string{"public.also_bad"}
	for _, invalid := range []*wallabypb.PostgresSourceConfig{invalidCDC, invalidBootstrap, invalidBackfill, {}} {
		if _, err := endpointcodec.Decode(&wallabypb.Endpoint{Config: &wallabypb.Endpoint_PostgresSource{PostgresSource: invalid}}, endpointcodec.RoleSource); err == nil {
			t.Fatalf("incompatible/unspecified source config accepted: %v", invalid)
		}
	}
}

func TestPostgresSourceDeliveryRetentionUsesTypedPositiveDurations(t *testing.T) {
	t.Parallel()
	spec := connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: map[string]string{
		"mode": connector.SourceModeCDC, "delivery_retention": "24h0m0s", "delivery_prune_interval": "5m0s",
	}}
	endpoint, err := endpointcodec.Encode(spec, endpointcodec.RoleSource)
	if err != nil {
		t.Fatal(err)
	}
	message := endpoint.GetPostgresSource().ProtoReflect()
	for _, fieldName := range []protoreflect.Name{"delivery_retention", "delivery_prune_interval"} {
		field := message.Descriptor().Fields().ByName(fieldName)
		if field == nil || !message.Has(field) {
			t.Fatalf("typed %s was not populated", fieldName)
		}
	}
	roundTrip, err := endpointcodec.Decode(endpoint, endpointcodec.RoleSource)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(roundTrip, spec) {
		t.Fatalf("delivery duration round trip=%v, want %v", roundTrip, spec)
	}
	for _, value := range []string{"0s", "-1s"} {
		if _, err := endpointcodec.Encode(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: map[string]string{"mode": connector.SourceModeCDC, "delivery_retention": value}}, endpointcodec.RoleSource); err == nil {
			t.Fatalf("non-positive delivery_retention %q was accepted", value)
		}
	}
}

func TestLocalRegistryRequiresCanonicalDirectoryRuntimeKey(t *testing.T) {
	t.Parallel()
	for _, options := range []map[string]string{
		{"brokers": "broker:9092", "topic": "events", "schema_registry": "local"},
		{"brokers": "broker:9092", "topic": "events", "schema_registry": "local", "schema_registry_url": "/legacy"},
	} {
		if _, err := endpointcodec.Encode(connector.RuntimeSpec{Type: connector.EndpointKafka, Options: options}, endpointcodec.RoleDestination); err == nil {
			t.Fatalf("local registry without canonical directory accepted: %v", options)
		}
	}
}

func TestEveryRegistryCapableDestinationHasTypedRegistryRoundTrip(t *testing.T) {
	t.Parallel()
	registry := &wallabypb.SchemaRegistryConfig{Backend: &wallabypb.SchemaRegistryConfig_Local{Local: &wallabypb.LocalSchemaRegistryConfig{Directory: "/registry"}}}
	pgstream := &wallabypb.PGStreamDestinationConfig{SchemaRegistrySubject: "events", SchemaRegistryProtoTypesSubject: "types"}
	s3 := &wallabypb.S3DestinationConfig{SchemaRegistrySubject: "events", SchemaRegistryProtoTypesSubject: "types"}
	snowflake := &wallabypb.SnowflakeDestinationConfig{}
	snowpipe := &wallabypb.SnowpipeDestinationConfig{}
	setMessageField(t, pgstream, "schema_registry", registry)
	setMessageField(t, s3, "schema_registry", registry)
	setMessageField(t, snowflake, "schema_registry", registry)
	setMessageField(t, snowpipe, "schema_registry", registry)
	endpoints := []*wallabypb.Endpoint{
		{Config: &wallabypb.Endpoint_Pgstream{Pgstream: pgstream}},
		{Config: &wallabypb.Endpoint_Kafka{Kafka: &wallabypb.KafkaDestinationConfig{SchemaRegistry: registry}}},
		{Config: &wallabypb.Endpoint_S3{S3: s3}},
		{Config: &wallabypb.Endpoint_Http{Http: &wallabypb.HTTPDestinationConfig{SchemaRegistry: registry}}},
		{Config: &wallabypb.Endpoint_Grpc{Grpc: &wallabypb.GRPCDestinationConfig{SchemaRegistry: registry}}},
		{Config: &wallabypb.Endpoint_Snowflake{Snowflake: snowflake}},
		{Config: &wallabypb.Endpoint_Snowpipe{Snowpipe: snowpipe}},
	}
	for _, endpoint := range endpoints {
		spec, err := endpointcodec.Decode(endpoint, endpointcodec.RoleDestination)
		if err != nil {
			t.Fatalf("%T: %v", endpoint.GetConfig(), err)
		}
		roundTrip, err := endpointcodec.Encode(spec, endpointcodec.RoleDestination)
		if err != nil {
			t.Fatalf("%T: %v", endpoint.GetConfig(), err)
		}
		if !proto.Equal(roundTrip, endpoint) {
			t.Fatalf("%T registry round trip=%v, want %v", endpoint.GetConfig(), roundTrip, endpoint)
		}
	}
}

func TestManagedDescriptorsExactlyRoundTripThroughProfileAllowlists(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		message   proto.Message
		wrap      func(proto.Message) *wallabypb.Endpoint
		forbidden []protoreflect.Name
		validate  func(map[string]string) error
	}{
		{
			name: "snowflake sql", message: &wallabypb.SnowflakePostgresSQLConfig{},
			wrap: func(message proto.Message) *wallabypb.Endpoint {
				return &wallabypb.Endpoint{Config: &wallabypb.Endpoint_SnowflakePostgresSql{SnowflakePostgresSql: message.(*wallabypb.SnowflakePostgresSQLConfig)}}
			},
			forbidden: []protoreflect.Name{"warehouse", "staging", "metadata", "type_mappings"}, validate: sfdest.ValidateManagedProfileOptions,
		},
		{
			name: "snowflake staged", message: &wallabypb.SnowflakePostgresStagedConfig{},
			wrap: func(message proto.Message) *wallabypb.Endpoint {
				return &wallabypb.Endpoint{Config: &wallabypb.Endpoint_SnowflakePostgresStaged{SnowflakePostgresStaged: message.(*wallabypb.SnowflakePostgresStagedConfig)}}
			},
			forbidden: []protoreflect.Name{"stage_path", "warehouse", "copy_on_write", "copy_pattern", "copy_on_error", "copy_purge", "copy_match_by_column_name", "metadata", "type_mappings"}, validate: sfdest.ValidateManagedStagedProfileOptions,
		},
		{
			name: "snowflake streaming", message: &wallabypb.SnowflakePostgresStreamingConfig{},
			wrap: func(message proto.Message) *wallabypb.Endpoint {
				return &wallabypb.Endpoint{Config: &wallabypb.Endpoint_SnowflakePostgresStreaming{SnowflakePostgresStreaming: message.(*wallabypb.SnowflakePostgresStreamingConfig)}}
			},
			forbidden: []protoreflect.Name{"type_mappings"}, validate: sfdest.ValidateManagedStreamingProfileOptions,
		},
		{
			name: "clickhouse append", message: &wallabypb.ClickHousePostgresAppendConfig{},
			wrap: func(message proto.Message) *wallabypb.Endpoint {
				return &wallabypb.Endpoint{Config: &wallabypb.Endpoint_ClickhousePostgresAppend{ClickhousePostgresAppend: message.(*wallabypb.ClickHousePostgresAppendConfig)}}
			},
			forbidden: []protoreflect.Name{"staging", "metadata"}, validate: chdest.ValidateManagedProfileOptions,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			descriptor := test.message.ProtoReflect().Descriptor()
			for _, forbidden := range test.forbidden {
				if descriptor.Fields().ByName(forbidden) != nil {
					t.Fatalf("forbidden/ignored field %s remains in %s", forbidden, descriptor.FullName())
				}
			}
			fillProtoMessage(test.message.ProtoReflect())
			endpoint := test.wrap(test.message)
			spec, err := endpointcodec.Decode(endpoint, endpointcodec.RoleDestination)
			if err != nil {
				t.Fatal(err)
			}
			if err := test.validate(spec.Options); err != nil {
				t.Fatalf("codec emitted option outside exact allowlist: %v; options=%v", err, spec.Options)
			}
			roundTrip, err := endpointcodec.Encode(spec, endpointcodec.RoleDestination)
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(roundTrip, endpoint) {
				t.Fatalf("descriptor field was ignored or decoded non-reversibly:\ngot  %v\nwant %v", roundTrip, endpoint)
			}
		})
	}
}

func TestCodecRejectsUnknownEnumsLossyScalarsAndEmptyNestedMessages(t *testing.T) {
	t.Parallel()
	unknown := &wallabypb.Endpoint{Config: &wallabypb.Endpoint_Kafka{Kafka: &wallabypb.KafkaDestinationConfig{Format: wallabypb.WireFormat(999)}}}
	if _, err := endpointcodec.Decode(unknown, endpointcodec.RoleDestination); err == nil {
		t.Fatal("unknown protobuf enum was accepted")
	}
	overflow := &wallabypb.Endpoint{Config: &wallabypb.Endpoint_Http{Http: &wallabypb.HTTPDestinationConfig{Timeout: &durationpb.Duration{Seconds: 315576000000}}}}
	if _, err := endpointcodec.Decode(overflow, endpointcodec.RoleDestination); err == nil {
		t.Fatal("duration outside time.Duration exact range was accepted")
	}
	emptyRetry := &wallabypb.Endpoint{Config: &wallabypb.Endpoint_Http{Http: &wallabypb.HTTPDestinationConfig{Retry: &wallabypb.RetryConfig{}}}}
	if _, err := endpointcodec.Decode(emptyRetry, endpointcodec.RoleDestination); err == nil {
		t.Fatal("meaningless empty retry message was accepted")
	}
	for _, endpoint := range []*wallabypb.Endpoint{
		{Config: &wallabypb.Endpoint_Http{Http: &wallabypb.HTTPDestinationConfig{Headers: map[string]string{"Authorization": "secret"}}}},
		{Config: &wallabypb.Endpoint_Grpc{Grpc: &wallabypb.GRPCDestinationConfig{Metadata: map[string]string{"X-Trace": " value "}}}},
		{Config: &wallabypb.Endpoint_Duckdb{Duckdb: &wallabypb.DuckDBDestinationConfig{TypeMappings: &wallabypb.TypeMappingsConfig{Mappings: map[string]string{" JSONB ": " JSON "}}}}},
	} {
		if _, err := endpointcodec.Decode(endpoint, endpointcodec.RoleDestination); err == nil {
			t.Fatalf("noncanonical typed map accepted: %v", endpoint)
		}
	}
	for _, options := range []map[string]string{
		{"schema_registry": "csr", "schema_registry_url": "https://registry", "schema_registry_region": "us-east-1"},
		{"schema_registry": "postgres"},
		{"schema_registry": "postgres", "schema_registry_dsn": ""},
	} {
		if _, err := endpointcodec.Encode(connector.RuntimeSpec{Type: connector.EndpointKafka, Options: options}, endpointcodec.RoleDestination); err == nil {
			t.Fatalf("backend-incompatible registry options were accepted: %v", options)
		}
	}
	for _, options := range []map[string]string{
		{"mode": connector.SourceModeCDC, "snapshot_state_backend": "file", "snapshot_state_dsn": "postgres://state"},
		{"mode": connector.SourceModeCDC, "snapshot_state_backend": "postgres", "snapshot_state_path": "state.json"},
	} {
		if _, err := endpointcodec.Encode(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: options}, endpointcodec.RoleSource); err == nil {
			t.Fatalf("backend-incompatible snapshot options were accepted: %v", options)
		}
	}
	for key, value := range map[string]string{"pool_max_conns": "01", "meta_table_enabled": "1", "batch_resolution": "LATEST", "insecure": "yes"} {
		endpointType := connector.EndpointPostgres
		if key == "insecure" {
			endpointType = connector.EndpointGRPC
		}
		if _, err := endpointcodec.Encode(connector.RuntimeSpec{Type: endpointType, Options: map[string]string{key: value}}, endpointcodec.RoleDestination); err == nil {
			t.Fatalf("malformed reverse scalar %s=%q was accepted", key, value)
		}
	}
}

func TestTypedDeliveryFingerprintIsVersionedDeterministicAndRuntimeIsolated(t *testing.T) {
	t.Parallel()
	first := connector.RuntimeSpec{Name: "target", Type: connector.EndpointIceberg, Options: map[string]string{"catalog_profile": "rest", "control_table": "one", "destination_revision_id": "revision-a"}}
	reordered := connector.RuntimeSpec{Name: "target", Type: connector.EndpointIceberg, Options: map[string]string{"destination_revision_id": "revision-b", "control_table": "one", "catalog_profile": "rest"}}
	firstFingerprint, err := endpointcodec.DeliveryConfigFingerprint(fingerprintEndpoint(t, first), "mapping-v1")
	if err != nil {
		t.Fatal(err)
	}
	reorderedFingerprint, err := endpointcodec.DeliveryConfigFingerprint(fingerprintEndpoint(t, reordered), "mapping-v1")
	if err != nil {
		t.Fatal(err)
	}
	if firstFingerprint != reorderedFingerprint {
		t.Fatal("revision identity or map order changed typed fingerprint")
	}
	changed := first
	changed.Options = map[string]string{"catalog_profile": "rest", "control_table": "two", "destination_revision_id": "revision-a"}
	changedFingerprint, err := endpointcodec.DeliveryConfigFingerprint(fingerprintEndpoint(t, changed), "mapping-v1")
	if err != nil {
		t.Fatal(err)
	}
	if changedFingerprint == firstFingerprint {
		t.Fatal("typed config change did not change fingerprint")
	}
	enriched := first
	enriched.Options = map[string]string{"catalog_profile": "rest", "control_table": "one", "destination_revision_id": "revision-a", "flow_id": "runtime"}
	if _, err := endpointcodec.Encode(enriched, endpointcodec.RoleDestination); err == nil {
		t.Fatal("runtime enrichment was silently encoded into a persisted endpoint")
	}
	if _, err := endpointcodec.DeliveryConfigFingerprint(fingerprintEndpoint(t, first), ""); err == nil {
		t.Fatal("projection-free fingerprint accepted")
	}

	postgres := connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": "postgres://destination", "managed_profile": connector.ManagedProfilePostgresToPostgresV1,
		"destination_revision_id": "revision-a", "synchronous_commit": "remote_apply", "type_mappings": `{"jsonb":"json"}`,
	}}
	postgresFingerprint, err := endpointcodec.DeliveryConfigFingerprint(fingerprintEndpoint(t, postgres), "mapping-v1")
	if err != nil {
		t.Fatal(err)
	}
	postgresRevisionChanged := postgres
	postgresRevisionChanged.Options = cloneOptions(postgres.Options)
	postgresRevisionChanged.Options["destination_revision_id"] = "revision-b"
	withoutRevision, err := endpointcodec.DeliveryConfigFingerprint(fingerprintEndpoint(t, postgresRevisionChanged), "mapping-v1")
	if err != nil {
		t.Fatal(err)
	}
	if postgresFingerprint != withoutRevision {
		t.Fatal("managed Postgres destination revision changed delivery config fingerprint")
	}
	postgresConfigChanged := postgres
	postgresConfigChanged.Options = cloneOptions(postgres.Options)
	postgresConfigChanged.Options["synchronous_commit"] = "on"
	withConfigChange, err := endpointcodec.DeliveryConfigFingerprint(fingerprintEndpoint(t, postgresConfigChanged), "mapping-v1")
	if err != nil {
		t.Fatal(err)
	}
	if postgresFingerprint == withConfigChange {
		t.Fatal("managed Postgres typed configuration change was excluded from fingerprint")
	}
}

func fingerprintEndpoint(t *testing.T, spec connector.RuntimeSpec) *wallabypb.Endpoint {
	t.Helper()
	endpoint, err := endpointcodec.Encode(spec, endpointcodec.RoleDestination)
	if err != nil {
		t.Fatal(err)
	}
	return endpoint
}

func cloneOptions(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func TestCustomEndpointRequiresRoleRegistration(t *testing.T) {
	registry := connector.NewRegistry()
	endpoint := &wallabypb.Endpoint{Name: "custom", Config: &wallabypb.Endpoint_Custom{Custom: &wallabypb.CustomEndpointConfig{ConnectorType: "acme", Options: map[string]string{"token": "secret"}}}}
	if _, err := endpointcodec.DecodeWithRegistry(endpoint, endpointcodec.RoleSource, registry); err == nil {
		t.Fatal("unregistered custom source accepted")
	}
	if err := registry.RegisterSource("acme", func() connector.Source { return nil }); err != nil {
		t.Fatal(err)
	}
	spec, err := endpointcodec.DecodeWithRegistry(endpoint, endpointcodec.RoleSource, registry)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Type != "acme" || spec.Options["token"] != "secret" {
		t.Fatalf("custom spec=%+v", spec)
	}
	if _, err := endpointcodec.DecodeWithRegistry(endpoint, endpointcodec.RoleDestination, registry); err == nil {
		t.Fatal("source-only custom connector accepted as destination")
	}
	whitespace := &wallabypb.Endpoint{Config: &wallabypb.Endpoint_Custom{Custom: &wallabypb.CustomEndpointConfig{ConnectorType: " acme"}}}
	if _, err := endpointcodec.DecodeWithRegistry(whitespace, endpointcodec.RoleSource, registry); err == nil {
		t.Fatal("custom connector_type surrounding whitespace was normalized instead of rejected")
	}
}

func fillProtoMessage(message protoreflect.Message) {
	fields := message.Descriptor().Fields()
	selectedOneofs := make(map[protoreflect.FullName]bool)
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		if oneof := field.ContainingOneof(); oneof != nil && !oneof.IsSynthetic() {
			if selectedOneofs[oneof.FullName()] {
				continue
			}
			selectedOneofs[oneof.FullName()] = true
		}
		switch {
		case field.IsMap():
			mapping := message.Mutable(field).Map()
			mapping.Set(protoreflect.ValueOfString("text").MapKey(), protoreflect.ValueOfString("text"))
		case field.IsList():
			message.Mutable(field).List().Append(protoreflect.ValueOfString("value"))
		case field.Kind() == protoreflect.MessageKind:
			child := message.NewField(field).Message()
			fillProtoMessage(child)
			message.Set(field, protoreflect.ValueOfMessage(child))
		case field.Kind() == protoreflect.StringKind:
			message.Set(field, protoreflect.ValueOfString("value"))
		case field.Kind() == protoreflect.BoolKind:
			message.Set(field, protoreflect.ValueOfBool(true))
		case field.Kind() == protoreflect.EnumKind:
			values := field.Enum().Values()
			if values.Len() > 1 {
				message.Set(field, protoreflect.ValueOfEnum(values.Get(1).Number()))
			}
		case field.Kind() == protoreflect.Uint32Kind || field.Kind() == protoreflect.Uint64Kind:
			message.Set(field, protoreflect.ValueOfUint64(1))
		case field.Kind() == protoreflect.Int32Kind || field.Kind() == protoreflect.Int64Kind:
			message.Set(field, protoreflect.ValueOfInt64(1))
		case field.Kind() == protoreflect.DoubleKind || field.Kind() == protoreflect.FloatKind:
			message.Set(field, protoreflect.ValueOfFloat64(1.5))
		}
	}
}

func postgresSourceConfigWithMode(t *testing.T, mode int32) *wallabypb.PostgresSourceConfig {
	t.Helper()
	cfg := &wallabypb.PostgresSourceConfig{}
	field := cfg.ProtoReflect().Descriptor().Fields().ByName("mode")
	if field == nil || field.Kind() != protoreflect.EnumKind {
		t.Fatal("PostgresSourceConfig.mode enum field is missing")
	}
	cfg.ProtoReflect().Set(field, protoreflect.ValueOfEnum(protoreflect.EnumNumber(mode)))
	return cfg
}

func setMessageField(t *testing.T, target proto.Message, name protoreflect.Name, value proto.Message) {
	t.Helper()
	field := target.ProtoReflect().Descriptor().Fields().ByName(name)
	if field == nil || field.Kind() != protoreflect.MessageKind {
		t.Fatalf("%s.%s message field is missing", target.ProtoReflect().Descriptor().FullName(), name)
	}
	target.ProtoReflect().Set(field, protoreflect.ValueOfMessage(proto.Clone(value).ProtoReflect()))
}

func walkMessages(messages protoreflect.MessageDescriptors, visit func(protoreflect.MessageDescriptor)) {
	for index := 0; index < messages.Len(); index++ {
		message := messages.Get(index)
		visit(message)
		walkMessages(message.Messages(), visit)
	}
}
