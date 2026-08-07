# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [wallaby/v1/table_mapping.proto](#wallaby_v1_table_mapping-proto)
    - [ColumnMapping](#wallaby-v1-ColumnMapping)
    - [DestinationTableMappings](#wallaby-v1-DestinationTableMappings)
    - [FutureColumnMapping](#wallaby-v1-FutureColumnMapping)
    - [FutureTableMapping](#wallaby-v1-FutureTableMapping)
    - [TableMapping](#wallaby-v1-TableMapping)
    - [TableMappings](#wallaby-v1-TableMappings)
    - [TableWritePolicy](#wallaby-v1-TableWritePolicy)

    - [MappingAction](#wallaby-v1-MappingAction)
    - [TableWriteMode](#wallaby-v1-TableWriteMode)

- [wallaby/v1/types.proto](#wallaby_v1_types-proto)
    - [ApicurioSchemaRegistryConfig](#wallaby-v1-ApicurioSchemaRegistryConfig)
    - [Checkpoint](#wallaby-v1-Checkpoint)
    - [Checkpoint.MetadataEntry](#wallaby-v1-Checkpoint-MetadataEntry)
    - [ClickHouseDestinationConfig](#wallaby-v1-ClickHouseDestinationConfig)
    - [ClickHouseMetadataConfig](#wallaby-v1-ClickHouseMetadataConfig)
    - [ClickHousePostgresAppendConfig](#wallaby-v1-ClickHousePostgresAppendConfig)
    - [ClickHouseTLSConfig](#wallaby-v1-ClickHouseTLSConfig)
    - [ConfluentSchemaRegistryConfig](#wallaby-v1-ConfluentSchemaRegistryConfig)
    - [CustomEndpointConfig](#wallaby-v1-CustomEndpointConfig)
    - [CustomEndpointConfig.OptionsEntry](#wallaby-v1-CustomEndpointConfig-OptionsEntry)
    - [DDLPolicy](#wallaby-v1-DDLPolicy)
    - [DuckDBDestinationConfig](#wallaby-v1-DuckDBDestinationConfig)
    - [DuckLakeDestinationConfig](#wallaby-v1-DuckLakeDestinationConfig)
    - [Endpoint](#wallaby-v1-Endpoint)
    - [Flow](#wallaby-v1-Flow)
    - [FlowConfig](#wallaby-v1-FlowConfig)
    - [GRPCDestinationConfig](#wallaby-v1-GRPCDestinationConfig)
    - [GRPCDestinationConfig.MetadataEntry](#wallaby-v1-GRPCDestinationConfig-MetadataEntry)
    - [GRPCTLSConfig](#wallaby-v1-GRPCTLSConfig)
    - [GlueSchemaRegistryConfig](#wallaby-v1-GlueSchemaRegistryConfig)
    - [HTTPDestinationConfig](#wallaby-v1-HTTPDestinationConfig)
    - [HTTPDestinationConfig.HeadersEntry](#wallaby-v1-HTTPDestinationConfig-HeadersEntry)
    - [IcebergDestinationConfig](#wallaby-v1-IcebergDestinationConfig)
    - [KafkaDestinationConfig](#wallaby-v1-KafkaDestinationConfig)
    - [LocalSchemaRegistryConfig](#wallaby-v1-LocalSchemaRegistryConfig)
    - [MaterializationPolicy](#wallaby-v1-MaterializationPolicy)
    - [MetadataConfig](#wallaby-v1-MetadataConfig)
    - [PGStreamDestinationConfig](#wallaby-v1-PGStreamDestinationConfig)
    - [PostgresConnectionConfig](#wallaby-v1-PostgresConnectionConfig)
    - [PostgresDSNConfig](#wallaby-v1-PostgresDSNConfig)
    - [PostgresDestinationConfig](#wallaby-v1-PostgresDestinationConfig)
    - [PostgresSchemaRegistryConfig](#wallaby-v1-PostgresSchemaRegistryConfig)
    - [PostgresSourceConfig](#wallaby-v1-PostgresSourceConfig)
    - [RDSIAMConfig](#wallaby-v1-RDSIAMConfig)
    - [RedpandaDestinationConfig](#wallaby-v1-RedpandaDestinationConfig)
    - [RetryConfig](#wallaby-v1-RetryConfig)
    - [S3DestinationConfig](#wallaby-v1-S3DestinationConfig)
    - [SchemaRegistryConfig](#wallaby-v1-SchemaRegistryConfig)
    - [SnapshotStateConfig](#wallaby-v1-SnapshotStateConfig)
    - [SnowflakeDestinationConfig](#wallaby-v1-SnowflakeDestinationConfig)
    - [SnowflakePostgresSQLConfig](#wallaby-v1-SnowflakePostgresSQLConfig)
    - [SnowflakePostgresStagedConfig](#wallaby-v1-SnowflakePostgresStagedConfig)
    - [SnowflakePostgresStreamingConfig](#wallaby-v1-SnowflakePostgresStreamingConfig)
    - [SnowpipeDestinationConfig](#wallaby-v1-SnowpipeDestinationConfig)
    - [StagingConfig](#wallaby-v1-StagingConfig)
    - [TypeMappingsConfig](#wallaby-v1-TypeMappingsConfig)
    - [TypeMappingsConfig.MappingsEntry](#wallaby-v1-TypeMappingsConfig-MappingsEntry)
    - [WarehouseConfig](#wallaby-v1-WarehouseConfig)

    - [AckPolicy](#wallaby-v1-AckPolicy)
    - [BatchMode](#wallaby-v1-BatchMode)
    - [BatchResolution](#wallaby-v1-BatchResolution)
    - [BootstrapMode](#wallaby-v1-BootstrapMode)
    - [Compression](#wallaby-v1-Compression)
    - [FailureMode](#wallaby-v1-FailureMode)
    - [FlowState](#wallaby-v1-FlowState)
    - [GiveUpPolicy](#wallaby-v1-GiveUpPolicy)
    - [IcebergCatalogProfile](#wallaby-v1-IcebergCatalogProfile)
    - [KafkaAcks](#wallaby-v1-KafkaAcks)
    - [KafkaKeyMode](#wallaby-v1-KafkaKeyMode)
    - [KafkaMessageMode](#wallaby-v1-KafkaMessageMode)
    - [ManagedProfile](#wallaby-v1-ManagedProfile)
    - [PayloadMode](#wallaby-v1-PayloadMode)
    - [PostgresSourceMode](#wallaby-v1-PostgresSourceMode)
    - [SyncPublicationMode](#wallaby-v1-SyncPublicationMode)
    - [ToastFetchMode](#wallaby-v1-ToastFetchMode)
    - [WireFormat](#wallaby-v1-WireFormat)

- [wallaby/v1/checkpoint.proto](#wallaby_v1_checkpoint-proto)
    - [FlowCheckpoint](#wallaby-v1-FlowCheckpoint)
    - [GetCheckpointRequest](#wallaby-v1-GetCheckpointRequest)
    - [ListCheckpointsRequest](#wallaby-v1-ListCheckpointsRequest)
    - [ListCheckpointsResponse](#wallaby-v1-ListCheckpointsResponse)
    - [PutCheckpointRequest](#wallaby-v1-PutCheckpointRequest)

    - [CheckpointService](#wallaby-v1-CheckpointService)

- [wallaby/v1/data.proto](#wallaby_v1_data-proto)
    - [Batch](#wallaby-v1-Batch)
    - [Record](#wallaby-v1-Record)
    - [Schema](#wallaby-v1-Schema)
    - [SchemaColumn](#wallaby-v1-SchemaColumn)

- [wallaby/v1/ddl.proto](#wallaby_v1_ddl-proto)
    - [ApproveDDLRequest](#wallaby-v1-ApproveDDLRequest)
    - [ApproveDDLResponse](#wallaby-v1-ApproveDDLResponse)
    - [DDLEvent](#wallaby-v1-DDLEvent)
    - [ListDDLRequest](#wallaby-v1-ListDDLRequest)
    - [ListDDLResponse](#wallaby-v1-ListDDLResponse)
    - [ListPendingDDLRequest](#wallaby-v1-ListPendingDDLRequest)
    - [ListPendingDDLResponse](#wallaby-v1-ListPendingDDLResponse)
    - [RejectDDLRequest](#wallaby-v1-RejectDDLRequest)
    - [RejectDDLResponse](#wallaby-v1-RejectDDLResponse)

    - [DDLService](#wallaby-v1-DDLService)

- [wallaby/v1/flow.proto](#wallaby_v1_flow-proto)
    - [AddPublicationTablesRequest](#wallaby-v1-AddPublicationTablesRequest)
    - [CleanupFlowRequest](#wallaby-v1-CleanupFlowRequest)
    - [CleanupFlowResponse](#wallaby-v1-CleanupFlowResponse)
    - [CreateFlowRequest](#wallaby-v1-CreateFlowRequest)
    - [DeleteFlowRequest](#wallaby-v1-DeleteFlowRequest)
    - [DeleteFlowResponse](#wallaby-v1-DeleteFlowResponse)
    - [DropPublicationTablesRequest](#wallaby-v1-DropPublicationTablesRequest)
    - [DropReplicationSlotRequest](#wallaby-v1-DropReplicationSlotRequest)
    - [DropReplicationSlotResponse](#wallaby-v1-DropReplicationSlotResponse)
    - [GetFlowRequest](#wallaby-v1-GetFlowRequest)
    - [GetReplicationSlotRequest](#wallaby-v1-GetReplicationSlotRequest)
    - [GetReplicationSlotResponse](#wallaby-v1-GetReplicationSlotResponse)
    - [ListFlowsRequest](#wallaby-v1-ListFlowsRequest)
    - [ListFlowsResponse](#wallaby-v1-ListFlowsResponse)
    - [ListPublicationTablesRequest](#wallaby-v1-ListPublicationTablesRequest)
    - [ListPublicationTablesResponse](#wallaby-v1-ListPublicationTablesResponse)
    - [ListReplicationSlotsRequest](#wallaby-v1-ListReplicationSlotsRequest)
    - [ListReplicationSlotsResponse](#wallaby-v1-ListReplicationSlotsResponse)
    - [PauseFlowRequest](#wallaby-v1-PauseFlowRequest)
    - [PublicationTablesMutationResponse](#wallaby-v1-PublicationTablesMutationResponse)
    - [ReconfigureFlowRequest](#wallaby-v1-ReconfigureFlowRequest)
    - [ReplicationSlotInfo](#wallaby-v1-ReplicationSlotInfo)
    - [ResumeFlowRequest](#wallaby-v1-ResumeFlowRequest)
    - [RunFlowOnceRequest](#wallaby-v1-RunFlowOnceRequest)
    - [RunFlowOnceResponse](#wallaby-v1-RunFlowOnceResponse)
    - [ScrapePublicationTablesRequest](#wallaby-v1-ScrapePublicationTablesRequest)
    - [ScrapePublicationTablesResponse](#wallaby-v1-ScrapePublicationTablesResponse)
    - [StartFlowRequest](#wallaby-v1-StartFlowRequest)
    - [StopFlowRequest](#wallaby-v1-StopFlowRequest)
    - [SyncPublicationTablesRequest](#wallaby-v1-SyncPublicationTablesRequest)
    - [SyncPublicationTablesResponse](#wallaby-v1-SyncPublicationTablesResponse)
    - [UpdateFlowRequest](#wallaby-v1-UpdateFlowRequest)

    - [FlowService](#wallaby-v1-FlowService)

- [wallaby/v1/ingest.proto](#wallaby_v1_ingest-proto)
    - [IngestBatchRequest](#wallaby-v1-IngestBatchRequest)
    - [IngestBatchResponse](#wallaby-v1-IngestBatchResponse)

    - [IngestService](#wallaby-v1-IngestService)

- [wallaby/v1/stream.proto](#wallaby_v1_stream-proto)
    - [StreamAckRequest](#wallaby-v1-StreamAckRequest)
    - [StreamAckResponse](#wallaby-v1-StreamAckResponse)
    - [StreamMessage](#wallaby-v1-StreamMessage)
    - [StreamPullRequest](#wallaby-v1-StreamPullRequest)
    - [StreamPullResponse](#wallaby-v1-StreamPullResponse)
    - [StreamReplayRequest](#wallaby-v1-StreamReplayRequest)
    - [StreamReplayResponse](#wallaby-v1-StreamReplayResponse)

    - [StreamService](#wallaby-v1-StreamService)

- [Scalar Value Types](#scalar-value-types)



<a name="wallaby_v1_table_mapping-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## wallaby/v1/table_mapping.proto



<a name="wallaby-v1-ColumnMapping"></a>

### ColumnMapping



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| source_column | [string](#string) |  |  |
| action | [MappingAction](#wallaby-v1-MappingAction) |  |  |
| target_column | [string](#string) |  |  |






<a name="wallaby-v1-DestinationTableMappings"></a>

### DestinationTableMappings



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| destination | [string](#string) |  |  |
| future_tables | [FutureTableMapping](#wallaby-v1-FutureTableMapping) |  |  |
| tables | [TableMapping](#wallaby-v1-TableMapping) | repeated |  |






<a name="wallaby-v1-FutureColumnMapping"></a>

### FutureColumnMapping



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| action | [MappingAction](#wallaby-v1-MappingAction) |  |  |
| target_column | [string](#string) |  | When action is INCLUDE, version 2 requires one restricted Go text/template field action: {{ .Column }}. When action is EXCLUDE, this must be empty. Only typed Schema, Table, and Column data is injected. Functions, pipelines, variables, conditions, loops, and template inclusion are unsupported. |






<a name="wallaby-v1-FutureTableMapping"></a>

### FutureTableMapping



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| action | [MappingAction](#wallaby-v1-MappingAction) |  |  |
| target_schema | [string](#string) |  | When action is INCLUDE, version 2 requires one restricted Go text/template field action: {{ .Schema }}. When action is EXCLUDE, this must be empty. |
| target_table | [string](#string) |  | When action is INCLUDE, version 2 requires one restricted Go text/template field action: {{ .Table }}. When action is EXCLUDE, this must be empty. |
| future_columns | [FutureColumnMapping](#wallaby-v1-FutureColumnMapping) |  |  |
| write | [TableWritePolicy](#wallaby-v1-TableWritePolicy) |  |  |






<a name="wallaby-v1-TableMapping"></a>

### TableMapping



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| source_schema | [string](#string) |  |  |
| source_table | [string](#string) |  |  |
| action | [MappingAction](#wallaby-v1-MappingAction) |  |  |
| target_schema | [string](#string) |  |  |
| target_table | [string](#string) |  |  |
| future_columns | [FutureColumnMapping](#wallaby-v1-FutureColumnMapping) |  |  |
| columns | [ColumnMapping](#wallaby-v1-ColumnMapping) | repeated |  |
| write | [TableWritePolicy](#wallaby-v1-TableWritePolicy) |  |  |






<a name="wallaby-v1-TableMappings"></a>

### TableMappings



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [uint32](#uint32) |  | Version 2 is required. Version 1 and legacy placeholder syntax are unsupported. |
| destinations | [DestinationTableMappings](#wallaby-v1-DestinationTableMappings) | repeated |  |






<a name="wallaby-v1-TableWritePolicy"></a>

### TableWritePolicy



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mode | [TableWriteMode](#wallaby-v1-TableWriteMode) |  |  |
| key_columns | [string](#string) | repeated |  |
| watermark_column | [string](#string) |  |  |








<a name="wallaby-v1-MappingAction"></a>

### MappingAction


| Name | Number | Description |
| ---- | ------ | ----------- |
| MAPPING_ACTION_UNSPECIFIED | 0 |  |
| MAPPING_ACTION_INCLUDE | 1 |  |
| MAPPING_ACTION_EXCLUDE | 2 |  |



<a name="wallaby-v1-TableWriteMode"></a>

### TableWriteMode


| Name | Number | Description |
| ---- | ------ | ----------- |
| TABLE_WRITE_MODE_UNSPECIFIED | 0 |  |
| TABLE_WRITE_MODE_APPEND | 1 |  |
| TABLE_WRITE_MODE_UPSERT | 2 |  |










<a name="wallaby_v1_types-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## wallaby/v1/types.proto



<a name="wallaby-v1-ApicurioSchemaRegistryConfig"></a>

### ApicurioSchemaRegistryConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| url | [string](#string) |  |  |
| username | [string](#string) |  |  |
| password | [string](#string) |  |  |
| token | [string](#string) |  |  |
| timeout | [google.protobuf.Duration](https://protobuf.dev/reference/protobuf/google.protobuf/#duration) |  |  |
| compatibility | [bool](#bool) | optional |  |






<a name="wallaby-v1-Checkpoint"></a>

### Checkpoint



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| lsn | [string](#string) |  |  |
| timestamp_unix_millis | [int64](#int64) |  |  |
| metadata | [Checkpoint.MetadataEntry](#wallaby-v1-Checkpoint-MetadataEntry) | repeated |  |






<a name="wallaby-v1-Checkpoint-MetadataEntry"></a>

### Checkpoint.MetadataEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="wallaby-v1-ClickHouseDestinationConfig"></a>

### ClickHouseDestinationConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dsn | [string](#string) |  |  |
| staging | [StagingConfig](#wallaby-v1-StagingConfig) |  |  |
| metadata | [ClickHouseMetadataConfig](#wallaby-v1-ClickHouseMetadataConfig) |  |  |
| type_mappings | [TypeMappingsConfig](#wallaby-v1-TypeMappingsConfig) |  |  |






<a name="wallaby-v1-ClickHouseMetadataConfig"></a>

### ClickHouseMetadataConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| enabled | [bool](#bool) | optional |  |
| schema | [string](#string) |  |  |
| table | [string](#string) |  |  |
| primary_key_prefix | [string](#string) |  |  |
| engine | [string](#string) |  |  |
| order_by | [string](#string) |  |  |






<a name="wallaby-v1-ClickHousePostgresAppendConfig"></a>

### ClickHousePostgresAppendConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dsn | [string](#string) |  |  |
| tls | [ClickHouseTLSConfig](#wallaby-v1-ClickHouseTLSConfig) |  |  |
| destination_revision_id | [string](#string) |  |  |
| type_mappings | [TypeMappingsConfig](#wallaby-v1-TypeMappingsConfig) |  |  |
| database | [string](#string) |  |  |
| changelog_table | [string](#string) |  |  |
| receipts_table | [string](#string) |  |  |
| final_view | [string](#string) |  |  |
| keeper_path_prefix | [string](#string) |  |  |
| keeper_address | [string](#string) |  |  |
| replica_dsn | [string](#string) |  |  |
| replica_names | [string](#string) | repeated |  |
| insert_quorum | [uint32](#uint32) | optional |  |
| max_active_parts | [uint32](#uint32) | optional |  |
| max_transaction_rows | [uint32](#uint32) | optional |  |
| max_transaction_bytes | [uint64](#uint64) | optional |  |
| max_transaction_fragments | [uint32](#uint32) | optional |  |
| max_rows_per_batch | [uint32](#uint32) | optional |  |
| max_batch_bytes | [uint64](#uint64) | optional |  |






<a name="wallaby-v1-ClickHouseTLSConfig"></a>

### ClickHouseTLSConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| insecure | [bool](#bool) | optional |  |
| ca_file | [string](#string) |  |  |
| server_name | [string](#string) |  |  |
| certificate_file | [string](#string) |  |  |
| private_key_file | [string](#string) |  |  |
| replica_server_name | [string](#string) |  |  |






<a name="wallaby-v1-ConfluentSchemaRegistryConfig"></a>

### ConfluentSchemaRegistryConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| url | [string](#string) |  |  |
| username | [string](#string) |  |  |
| password | [string](#string) |  |  |
| token | [string](#string) |  |  |
| timeout | [google.protobuf.Duration](https://protobuf.dev/reference/protobuf/google.protobuf/#duration) |  |  |






<a name="wallaby-v1-CustomEndpointConfig"></a>

### CustomEndpointConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| connector_type | [string](#string) |  |  |
| options | [CustomEndpointConfig.OptionsEntry](#wallaby-v1-CustomEndpointConfig-OptionsEntry) | repeated |  |






<a name="wallaby-v1-CustomEndpointConfig-OptionsEntry"></a>

### CustomEndpointConfig.OptionsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="wallaby-v1-DDLPolicy"></a>

### DDLPolicy



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| gate | [bool](#bool) | optional |  |
| auto_approve | [bool](#bool) | optional |  |
| auto_apply | [bool](#bool) | optional |  |






<a name="wallaby-v1-DuckDBDestinationConfig"></a>

### DuckDBDestinationConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dsn | [string](#string) |  |  |
| staging | [StagingConfig](#wallaby-v1-StagingConfig) |  |  |
| metadata | [MetadataConfig](#wallaby-v1-MetadataConfig) |  |  |
| type_mappings | [TypeMappingsConfig](#wallaby-v1-TypeMappingsConfig) |  |  |






<a name="wallaby-v1-DuckLakeDestinationConfig"></a>

### DuckLakeDestinationConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dsn | [string](#string) |  |  |
| catalog | [string](#string) |  |  |
| catalog_name | [string](#string) |  |  |
| data_path | [string](#string) |  |  |
| override_data_path | [bool](#bool) | optional |  |
| install_extensions | [bool](#bool) | optional |  |
| staging | [StagingConfig](#wallaby-v1-StagingConfig) |  |  |
| metadata | [MetadataConfig](#wallaby-v1-MetadataConfig) |  |  |
| type_mappings | [TypeMappingsConfig](#wallaby-v1-TypeMappingsConfig) |  |  |






<a name="wallaby-v1-Endpoint"></a>

### Endpoint



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| postgres_source | [PostgresSourceConfig](#wallaby-v1-PostgresSourceConfig) |  |  |
| postgres_destination | [PostgresDestinationConfig](#wallaby-v1-PostgresDestinationConfig) |  |  |
| pgstream | [PGStreamDestinationConfig](#wallaby-v1-PGStreamDestinationConfig) |  |  |
| kafka | [KafkaDestinationConfig](#wallaby-v1-KafkaDestinationConfig) |  |  |
| redpanda | [RedpandaDestinationConfig](#wallaby-v1-RedpandaDestinationConfig) |  |  |
| s3 | [S3DestinationConfig](#wallaby-v1-S3DestinationConfig) |  |  |
| http | [HTTPDestinationConfig](#wallaby-v1-HTTPDestinationConfig) |  |  |
| grpc | [GRPCDestinationConfig](#wallaby-v1-GRPCDestinationConfig) |  |  |
| snowflake | [SnowflakeDestinationConfig](#wallaby-v1-SnowflakeDestinationConfig) |  |  |
| snowflake_postgres_sql | [SnowflakePostgresSQLConfig](#wallaby-v1-SnowflakePostgresSQLConfig) |  |  |
| snowflake_postgres_staged | [SnowflakePostgresStagedConfig](#wallaby-v1-SnowflakePostgresStagedConfig) |  |  |
| snowflake_postgres_streaming | [SnowflakePostgresStreamingConfig](#wallaby-v1-SnowflakePostgresStreamingConfig) |  |  |
| snowpipe | [SnowpipeDestinationConfig](#wallaby-v1-SnowpipeDestinationConfig) |  |  |
| clickhouse | [ClickHouseDestinationConfig](#wallaby-v1-ClickHouseDestinationConfig) |  |  |
| clickhouse_postgres_append | [ClickHousePostgresAppendConfig](#wallaby-v1-ClickHousePostgresAppendConfig) |  |  |
| duckdb | [DuckDBDestinationConfig](#wallaby-v1-DuckDBDestinationConfig) |  |  |
| ducklake | [DuckLakeDestinationConfig](#wallaby-v1-DuckLakeDestinationConfig) |  |  |
| iceberg | [IcebergDestinationConfig](#wallaby-v1-IcebergDestinationConfig) |  |  |
| custom | [CustomEndpointConfig](#wallaby-v1-CustomEndpointConfig) |  |  |






<a name="wallaby-v1-Flow"></a>

### Flow



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  |  |
| name | [string](#string) |  |  |
| source | [Endpoint](#wallaby-v1-Endpoint) |  |  |
| destinations | [Endpoint](#wallaby-v1-Endpoint) | repeated |  |
| state | [FlowState](#wallaby-v1-FlowState) |  |  |
| wire_format | [WireFormat](#wallaby-v1-WireFormat) |  |  |
| parallelism | [int32](#int32) |  |  |
| config | [FlowConfig](#wallaby-v1-FlowConfig) |  |  |






<a name="wallaby-v1-FlowConfig"></a>

### FlowConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ack_policy | [AckPolicy](#wallaby-v1-AckPolicy) |  |  |
| primary_destination | [string](#string) |  |  |
| failure_mode | [FailureMode](#wallaby-v1-FailureMode) |  |  |
| give_up_policy | [GiveUpPolicy](#wallaby-v1-GiveUpPolicy) |  |  |
| ddl | [DDLPolicy](#wallaby-v1-DDLPolicy) |  |  |
| materialization | [MaterializationPolicy](#wallaby-v1-MaterializationPolicy) |  |  |
| table_mappings | [TableMappings](#wallaby-v1-TableMappings) |  |  |






<a name="wallaby-v1-GRPCDestinationConfig"></a>

### GRPCDestinationConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| endpoint | [string](#string) |  |  |
| tls | [GRPCTLSConfig](#wallaby-v1-GRPCTLSConfig) |  |  |
| timeout | [google.protobuf.Duration](https://protobuf.dev/reference/protobuf/google.protobuf/#duration) |  |  |
| format | [WireFormat](#wallaby-v1-WireFormat) |  |  |
| payload_mode | [PayloadMode](#wallaby-v1-PayloadMode) |  |  |
| metadata | [GRPCDestinationConfig.MetadataEntry](#wallaby-v1-GRPCDestinationConfig-MetadataEntry) | repeated |  |
| retry | [RetryConfig](#wallaby-v1-RetryConfig) |  |  |
| schema_registry | [SchemaRegistryConfig](#wallaby-v1-SchemaRegistryConfig) |  |  |
| type_mappings | [TypeMappingsConfig](#wallaby-v1-TypeMappingsConfig) |  |  |
| schema_registry_subject | [string](#string) |  |  |
| schema_registry_proto_types_subject | [string](#string) |  |  |






<a name="wallaby-v1-GRPCDestinationConfig-MetadataEntry"></a>

### GRPCDestinationConfig.MetadataEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="wallaby-v1-GRPCTLSConfig"></a>

### GRPCTLSConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| insecure | [bool](#bool) | optional |  |
| ca_file | [string](#string) |  |  |
| server_name | [string](#string) |  |  |






<a name="wallaby-v1-GlueSchemaRegistryConfig"></a>

### GlueSchemaRegistryConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| region | [string](#string) |  |  |
| endpoint | [string](#string) |  |  |
| profile | [string](#string) |  |  |
| role_arn | [string](#string) |  |  |
| registry | [string](#string) |  |  |
| schema | [string](#string) |  |  |






<a name="wallaby-v1-HTTPDestinationConfig"></a>

### HTTPDestinationConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| url | [string](#string) |  |  |
| method | [string](#string) |  |  |
| format | [WireFormat](#wallaby-v1-WireFormat) |  |  |
| payload_mode | [PayloadMode](#wallaby-v1-PayloadMode) |  |  |
| timeout | [google.protobuf.Duration](https://protobuf.dev/reference/protobuf/google.protobuf/#duration) |  |  |
| headers | [HTTPDestinationConfig.HeadersEntry](#wallaby-v1-HTTPDestinationConfig-HeadersEntry) | repeated |  |
| retry | [RetryConfig](#wallaby-v1-RetryConfig) |  |  |
| idempotency_header | [string](#string) |  |  |
| dedupe_window | [google.protobuf.Duration](https://protobuf.dev/reference/protobuf/google.protobuf/#duration) |  |  |
| transaction_header | [string](#string) |  |  |
| schema_registry | [SchemaRegistryConfig](#wallaby-v1-SchemaRegistryConfig) |  |  |
| type_mappings | [TypeMappingsConfig](#wallaby-v1-TypeMappingsConfig) |  |  |
| schema_registry_subject | [string](#string) |  |  |
| schema_registry_proto_types_subject | [string](#string) |  |  |






<a name="wallaby-v1-HTTPDestinationConfig-HeadersEntry"></a>

### HTTPDestinationConfig.HeadersEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="wallaby-v1-IcebergDestinationConfig"></a>

### IcebergDestinationConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| catalog_profile | [IcebergCatalogProfile](#wallaby-v1-IcebergCatalogProfile) |  |  |
| control_table | [string](#string) |  |  |
| destination_revision_id | [string](#string) |  |  |






<a name="wallaby-v1-KafkaDestinationConfig"></a>

### KafkaDestinationConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| brokers | [string](#string) | repeated |  |
| topic | [string](#string) |  |  |
| format | [WireFormat](#wallaby-v1-WireFormat) |  |  |
| compression | [Compression](#wallaby-v1-Compression) |  |  |
| acks | [KafkaAcks](#wallaby-v1-KafkaAcks) |  |  |
| max_message_bytes | [uint32](#uint32) | optional |  |
| max_batch_bytes | [uint32](#uint32) | optional |  |
| max_record_bytes | [uint32](#uint32) | optional |  |
| transactional_producer | [bool](#bool) | optional |  |
| allow_oversize_skip | [bool](#bool) | optional |  |
| message_mode | [KafkaMessageMode](#wallaby-v1-KafkaMessageMode) |  |  |
| key_mode | [KafkaKeyMode](#wallaby-v1-KafkaKeyMode) |  |  |
| transactional_id | [string](#string) |  |  |
| transaction_timeout | [google.protobuf.Duration](https://protobuf.dev/reference/protobuf/google.protobuf/#duration) |  |  |
| transaction_header | [string](#string) |  |  |
| schema_registry | [SchemaRegistryConfig](#wallaby-v1-SchemaRegistryConfig) |  |  |
| type_mappings | [TypeMappingsConfig](#wallaby-v1-TypeMappingsConfig) |  |  |
| schema_registry_subject | [string](#string) |  |  |
| schema_registry_proto_types_subject | [string](#string) |  |  |
| schema_registry_subject_mode | [string](#string) |  |  |






<a name="wallaby-v1-LocalSchemaRegistryConfig"></a>

### LocalSchemaRegistryConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| directory | [string](#string) |  |  |






<a name="wallaby-v1-MaterializationPolicy"></a>

### MaterializationPolicy



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| projection_id | [string](#string) |  | Mapped materialized flows require exactly canonical_cdc_parquet_v2. canonical_cdc_parquet_v1 is frozen for historical encoder verification only. |






<a name="wallaby-v1-MetadataConfig"></a>

### MetadataConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| enabled | [bool](#bool) | optional |  |
| schema | [string](#string) |  |  |
| table | [string](#string) |  |  |
| primary_key_prefix | [string](#string) |  |  |






<a name="wallaby-v1-PGStreamDestinationConfig"></a>

### PGStreamDestinationConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| connection | [PostgresDSNConfig](#wallaby-v1-PostgresDSNConfig) |  |  |
| stream | [string](#string) |  |  |
| format | [WireFormat](#wallaby-v1-WireFormat) |  |  |
| type_mappings | [TypeMappingsConfig](#wallaby-v1-TypeMappingsConfig) |  |  |
| schema_registry | [SchemaRegistryConfig](#wallaby-v1-SchemaRegistryConfig) |  |  |
| schema_registry_subject | [string](#string) |  |  |
| schema_registry_proto_types_subject | [string](#string) |  |  |






<a name="wallaby-v1-PostgresConnectionConfig"></a>

### PostgresConnectionConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dsn | [string](#string) |  |  |
| pool_max_connections | [uint32](#uint32) | optional |  |
| rds_iam | [RDSIAMConfig](#wallaby-v1-RDSIAMConfig) |  |  |






<a name="wallaby-v1-PostgresDSNConfig"></a>

### PostgresDSNConfig
PostgresDSNConfig is intentionally narrow for components whose runtime
contract accepts only a DSN and ignores connection-pool and IAM options.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dsn | [string](#string) |  |  |






<a name="wallaby-v1-PostgresDestinationConfig"></a>

### PostgresDestinationConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| connection | [PostgresConnectionConfig](#wallaby-v1-PostgresConnectionConfig) |  |  |
| staging | [StagingConfig](#wallaby-v1-StagingConfig) |  |  |
| metadata | [MetadataConfig](#wallaby-v1-MetadataConfig) |  |  |
| synchronous_commit | [string](#string) |  |  |
| type_mappings | [TypeMappingsConfig](#wallaby-v1-TypeMappingsConfig) |  |  |
| managed_profile | [ManagedProfile](#wallaby-v1-ManagedProfile) |  |  |
| destination_revision_id | [string](#string) |  |  |






<a name="wallaby-v1-PostgresSchemaRegistryConfig"></a>

### PostgresSchemaRegistryConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| connection | [PostgresDSNConfig](#wallaby-v1-PostgresDSNConfig) |  |  |
| timeout | [google.protobuf.Duration](https://protobuf.dev/reference/protobuf/google.protobuf/#duration) |  |  |






<a name="wallaby-v1-PostgresSourceConfig"></a>

### PostgresSourceConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| connection | [PostgresConnectionConfig](#wallaby-v1-PostgresConnectionConfig) |  |  |
| slot | [string](#string) |  |  |
| publication | [string](#string) |  |  |
| batch_size | [uint32](#uint32) | optional |  |
| batch_timeout | [google.protobuf.Duration](https://protobuf.dev/reference/protobuf/google.protobuf/#duration) |  |  |
| status_interval | [google.protobuf.Duration](https://protobuf.dev/reference/protobuf/google.protobuf/#duration) |  |  |
| create_slot | [bool](#bool) | optional |  |
| emit_empty | [bool](#bool) | optional |  |
| ensure_publication | [bool](#bool) | optional |  |
| validate_replication | [bool](#bool) | optional |  |
| publication_tables | [string](#string) | repeated |  |
| publication_schemas | [string](#string) | repeated |  |
| sync_publication | [bool](#bool) | optional |  |
| sync_publication_mode | [SyncPublicationMode](#wallaby-v1-SyncPublicationMode) |  |  |
| resolve_types | [bool](#bool) | optional |  |
| ensure_state | [bool](#bool) | optional |  |
| state_schema | [string](#string) |  |  |
| state_table | [string](#string) |  |  |
| capture_ddl | [bool](#bool) | optional |  |
| ddl_trigger_schema | [string](#string) |  |  |
| ddl_trigger_name | [string](#string) |  |  |
| ddl_message_prefix | [string](#string) |  |  |
| toast_fetch | [ToastFetchMode](#wallaby-v1-ToastFetchMode) |  |  |
| toast_cache_size | [uint32](#uint32) | optional |  |
| managed | [bool](#bool) | optional |  |
| managed_profile | [ManagedProfile](#wallaby-v1-ManagedProfile) |  |  |
| max_transaction_records | [uint64](#uint64) | optional |  |
| max_transaction_bytes | [uint64](#uint64) | optional |  |
| max_transaction_fragments | [uint32](#uint32) | optional |  |
| streaming_transactions | [bool](#bool) | optional |  |
| source_system_identifier | [string](#string) |  |  |
| source_lineage_id | [string](#string) |  |  |
| publication_revision | [string](#string) |  |  |
| bootstrap | [BootstrapMode](#wallaby-v1-BootstrapMode) |  |  |
| bootstrap_restart_limit | [uint32](#uint32) | optional |  |
| snapshot_max_tables | [uint32](#uint32) | optional |  |
| snapshot_workers | [uint32](#uint32) | optional |  |
| snapshot_claim_lease | [google.protobuf.Duration](https://protobuf.dev/reference/protobuf/google.protobuf/#duration) |  |  |
| backfill_tables | [string](#string) | repeated |  |
| backfill_schemas | [string](#string) | repeated |  |
| partition_column | [string](#string) |  |  |
| partition_count | [uint32](#uint32) | optional |  |
| snapshot_consistent | [bool](#bool) | optional |  |
| snapshot_state | [SnapshotStateConfig](#wallaby-v1-SnapshotStateConfig) |  |  |
| format | [WireFormat](#wallaby-v1-WireFormat) |  |  |
| mode | [PostgresSourceMode](#wallaby-v1-PostgresSourceMode) |  |  |
| delivery_retention | [google.protobuf.Duration](https://protobuf.dev/reference/protobuf/google.protobuf/#duration) |  |  |
| delivery_prune_interval | [google.protobuf.Duration](https://protobuf.dev/reference/protobuf/google.protobuf/#duration) |  |  |
| bootstrap_tables | [string](#string) | repeated |  |
| bootstrap_schemas | [string](#string) | repeated |  |






<a name="wallaby-v1-RDSIAMConfig"></a>

### RDSIAMConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| region | [string](#string) |  |  |
| profile | [string](#string) |  |  |
| role_arn | [string](#string) |  |  |
| role_session_name | [string](#string) |  |  |
| role_external_id | [string](#string) |  |  |
| endpoint | [string](#string) |  |  |






<a name="wallaby-v1-RedpandaDestinationConfig"></a>

### RedpandaDestinationConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kafka | [KafkaDestinationConfig](#wallaby-v1-KafkaDestinationConfig) |  |  |






<a name="wallaby-v1-RetryConfig"></a>

### RetryConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| max_retries | [uint32](#uint32) | optional |  |
| backoff_base | [google.protobuf.Duration](https://protobuf.dev/reference/protobuf/google.protobuf/#duration) |  |  |
| backoff_max | [google.protobuf.Duration](https://protobuf.dev/reference/protobuf/google.protobuf/#duration) |  |  |
| backoff_factor | [double](#double) | optional |  |






<a name="wallaby-v1-S3DestinationConfig"></a>

### S3DestinationConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| bucket | [string](#string) |  |  |
| prefix | [string](#string) |  |  |
| region | [string](#string) |  |  |
| format | [WireFormat](#wallaby-v1-WireFormat) |  |  |
| compression | [Compression](#wallaby-v1-Compression) |  |  |
| partition_by | [string](#string) | repeated |  |
| endpoint | [string](#string) |  |  |
| access_key | [string](#string) |  |  |
| secret_key | [string](#string) |  |  |
| session_token | [string](#string) |  |  |
| force_path_style | [bool](#bool) | optional |  |
| use_fips | [bool](#bool) | optional |  |
| use_dualstack | [bool](#bool) | optional |  |
| type_mappings | [TypeMappingsConfig](#wallaby-v1-TypeMappingsConfig) |  |  |
| schema_registry | [SchemaRegistryConfig](#wallaby-v1-SchemaRegistryConfig) |  |  |
| schema_registry_subject | [string](#string) |  |  |
| schema_registry_proto_types_subject | [string](#string) |  |  |






<a name="wallaby-v1-SchemaRegistryConfig"></a>

### SchemaRegistryConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| confluent | [ConfluentSchemaRegistryConfig](#wallaby-v1-ConfluentSchemaRegistryConfig) |  |  |
| apicurio | [ApicurioSchemaRegistryConfig](#wallaby-v1-ApicurioSchemaRegistryConfig) |  |  |
| glue | [GlueSchemaRegistryConfig](#wallaby-v1-GlueSchemaRegistryConfig) |  |  |
| postgres | [PostgresSchemaRegistryConfig](#wallaby-v1-PostgresSchemaRegistryConfig) |  |  |
| local | [LocalSchemaRegistryConfig](#wallaby-v1-LocalSchemaRegistryConfig) |  |  |






<a name="wallaby-v1-SnapshotStateConfig"></a>

### SnapshotStateConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| disabled | [bool](#bool) |  |  |
| file_path | [string](#string) |  |  |
| postgres | [PostgresDSNConfig](#wallaby-v1-PostgresDSNConfig) |  |  |
| schema | [string](#string) |  |  |
| table | [string](#string) |  |  |






<a name="wallaby-v1-SnowflakeDestinationConfig"></a>

### SnowflakeDestinationConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dsn | [string](#string) |  |  |
| disable_transactions | [bool](#bool) | optional |  |
| warehouse | [WarehouseConfig](#wallaby-v1-WarehouseConfig) |  |  |
| staging | [StagingConfig](#wallaby-v1-StagingConfig) |  |  |
| metadata | [MetadataConfig](#wallaby-v1-MetadataConfig) |  |  |
| type_mappings | [TypeMappingsConfig](#wallaby-v1-TypeMappingsConfig) |  |  |
| schema_registry | [SchemaRegistryConfig](#wallaby-v1-SchemaRegistryConfig) |  |  |
| schema_registry_subject | [string](#string) |  |  |






<a name="wallaby-v1-SnowflakePostgresSQLConfig"></a>

### SnowflakePostgresSQLConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dsn | [string](#string) |  |  |
| destination_revision_id | [string](#string) |  |  |
| account | [string](#string) |  |  |
| database | [string](#string) |  |  |
| schema | [string](#string) |  |  |
| table | [string](#string) |  |  |
| receipts_table | [string](#string) |  |  |
| owner_role | [string](#string) |  |  |
| execution_role | [string](#string) |  |  |
| managed_warehouse | [string](#string) |  |  |
| snowflake_version | [string](#string) |  |  |
| target_created_on | [string](#string) |  |  |
| receipts_created_on | [string](#string) |  |  |
| max_transaction_rows | [uint32](#uint32) | optional |  |
| max_transaction_bytes | [uint64](#uint64) | optional |  |
| max_transaction_fragments | [uint32](#uint32) | optional |  |
| max_open_connections | [uint32](#uint32) | optional |  |
| statement_timeout_seconds | [uint32](#uint32) | optional |  |
| hybrid_table_lock_timeout_seconds | [uint32](#uint32) | optional |  |






<a name="wallaby-v1-SnowflakePostgresStagedConfig"></a>

### SnowflakePostgresStagedConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dsn | [string](#string) |  |  |
| stage | [string](#string) |  |  |
| file_format | [string](#string) |  |  |
| auto_ingest | [bool](#bool) | optional |  |
| destination_revision_id | [string](#string) |  |  |
| account | [string](#string) |  |  |
| database | [string](#string) |  |  |
| schema | [string](#string) |  |  |
| table | [string](#string) |  |  |
| receipts_table | [string](#string) |  |  |
| pipe | [string](#string) |  |  |
| owner_role | [string](#string) |  |  |
| execution_role | [string](#string) |  |  |
| managed_warehouse | [string](#string) |  |  |
| snowflake_version | [string](#string) |  |  |
| stage_created_on | [string](#string) |  |  |
| target_created_on | [string](#string) |  |  |
| receipts_created_on | [string](#string) |  |  |
| file_format_created_on | [string](#string) |  |  |
| pipe_created_on | [string](#string) |  |  |
| max_transaction_rows | [uint32](#uint32) | optional |  |
| max_transaction_bytes | [uint64](#uint64) | optional |  |
| max_transaction_fragments | [uint32](#uint32) | optional |  |
| max_open_connections | [uint32](#uint32) | optional |  |
| statement_timeout_seconds | [uint32](#uint32) | optional |  |
| load_verify_attempts | [uint32](#uint32) | optional |  |
| load_verify_interval_millis | [uint32](#uint32) | optional |  |
| cleanup_max_objects | [uint32](#uint32) | optional |  |
| cleanup_retention_seconds | [uint32](#uint32) | optional |  |






<a name="wallaby-v1-SnowflakePostgresStreamingConfig"></a>

### SnowflakePostgresStreamingConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dsn | [string](#string) |  |  |
| destination_revision_id | [string](#string) |  |  |
| transport | [string](#string) |  |  |
| account | [string](#string) |  |  |
| database | [string](#string) |  |  |
| schema | [string](#string) |  |  |
| pipe | [string](#string) |  |  |
| table | [string](#string) |  |  |
| receipts_table | [string](#string) |  |  |
| channel_state_table | [string](#string) |  |  |
| owner_role | [string](#string) |  |  |
| execution_role | [string](#string) |  |  |
| managed_warehouse | [string](#string) |  |  |
| snowflake_version | [string](#string) |  |  |
| pipe_created_on | [string](#string) |  |  |
| target_created_on | [string](#string) |  |  |
| receipts_created_on | [string](#string) |  |  |
| channel_state_created_on | [string](#string) |  |  |
| max_transaction_rows | [uint32](#uint32) | optional |  |
| max_transaction_bytes | [uint64](#uint64) | optional |  |
| max_transaction_fragments | [uint32](#uint32) | optional |  |
| max_row_bytes | [uint64](#uint64) | optional |  |
| max_open_connections | [uint32](#uint32) | optional |  |
| statement_timeout_seconds | [uint32](#uint32) | optional |  |
| observe_attempts | [uint32](#uint32) | optional |  |
| observe_interval_millis | [uint32](#uint32) | optional |  |
| append_attempts | [uint32](#uint32) | optional |  |
| append_backoff_millis | [uint32](#uint32) | optional |  |
| cleanup_max_objects | [uint32](#uint32) | optional |  |
| cleanup_retention_seconds | [uint32](#uint32) | optional |  |
| channel_name_prefix | [string](#string) |  |  |






<a name="wallaby-v1-SnowpipeDestinationConfig"></a>

### SnowpipeDestinationConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dsn | [string](#string) |  |  |
| stage | [string](#string) |  |  |
| stage_path | [string](#string) |  |  |
| format | [WireFormat](#wallaby-v1-WireFormat) |  |  |
| file_format | [string](#string) |  |  |
| warehouse | [WarehouseConfig](#wallaby-v1-WarehouseConfig) |  |  |
| copy_on_write | [bool](#bool) | optional |  |
| copy_pattern | [string](#string) |  |  |
| copy_on_error | [string](#string) |  |  |
| copy_purge | [bool](#bool) | optional |  |
| copy_match_by_column_name | [string](#string) |  |  |
| auto_ingest | [bool](#bool) | optional |  |
| metadata | [MetadataConfig](#wallaby-v1-MetadataConfig) |  |  |
| type_mappings | [TypeMappingsConfig](#wallaby-v1-TypeMappingsConfig) |  |  |
| schema_registry | [SchemaRegistryConfig](#wallaby-v1-SchemaRegistryConfig) |  |  |
| schema_registry_subject | [string](#string) |  |  |






<a name="wallaby-v1-StagingConfig"></a>

### StagingConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| batch_mode | [BatchMode](#wallaby-v1-BatchMode) |  |  |
| batch_resolution | [BatchResolution](#wallaby-v1-BatchResolution) |  |  |
| schema | [string](#string) |  |  |
| table | [string](#string) |  |  |
| suffix | [string](#string) |  |  |






<a name="wallaby-v1-TypeMappingsConfig"></a>

### TypeMappingsConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mappings | [TypeMappingsConfig.MappingsEntry](#wallaby-v1-TypeMappingsConfig-MappingsEntry) | repeated |  |






<a name="wallaby-v1-TypeMappingsConfig-MappingsEntry"></a>

### TypeMappingsConfig.MappingsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="wallaby-v1-WarehouseConfig"></a>

### WarehouseConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| size | [string](#string) |  |  |
| auto_suspend_seconds | [uint32](#uint32) | optional |  |
| auto_resume | [bool](#bool) | optional |  |
| session_keep_alive | [bool](#bool) | optional |  |








<a name="wallaby-v1-AckPolicy"></a>

### AckPolicy


| Name | Number | Description |
| ---- | ------ | ----------- |
| ACK_POLICY_UNSPECIFIED | 0 |  |
| ACK_POLICY_ALL | 1 |  |
| ACK_POLICY_PRIMARY | 2 |  |
| ACK_POLICY_MATERIALIZED | 3 | ACK_POLICY_MATERIALIZED acknowledges a CDC transaction only after its canonical immutable objects and fenced PostgreSQL publication/checkpoint commit. A data-free startup cut is rooted as an object-free canonical publication before feedback. A configured Iceberg endpoint consumes the publication asynchronously and never delays source acknowledgement. |



<a name="wallaby-v1-BatchMode"></a>

### BatchMode


| Name | Number | Description |
| ---- | ------ | ----------- |
| BATCH_MODE_UNSPECIFIED | 0 |  |
| BATCH_MODE_STAGING | 1 |  |
| BATCH_MODE_TARGET | 2 |  |



<a name="wallaby-v1-BatchResolution"></a>

### BatchResolution


| Name | Number | Description |
| ---- | ------ | ----------- |
| BATCH_RESOLUTION_UNSPECIFIED | 0 |  |
| BATCH_RESOLUTION_NONE | 1 |  |
| BATCH_RESOLUTION_APPEND | 2 |  |
| BATCH_RESOLUTION_REPLACE | 3 |  |



<a name="wallaby-v1-BootstrapMode"></a>

### BootstrapMode


| Name | Number | Description |
| ---- | ------ | ----------- |
| BOOTSTRAP_MODE_UNSPECIFIED | 0 |  |
| BOOTSTRAP_MODE_NEVER | 1 |  |
| BOOTSTRAP_MODE_AUTO | 2 |  |
| BOOTSTRAP_MODE_REQUIRED | 3 |  |



<a name="wallaby-v1-Compression"></a>

### Compression


| Name | Number | Description |
| ---- | ------ | ----------- |
| COMPRESSION_UNSPECIFIED | 0 |  |
| COMPRESSION_NONE | 1 |  |
| COMPRESSION_GZIP | 2 |  |
| COMPRESSION_SNAPPY | 3 |  |
| COMPRESSION_LZ4 | 4 |  |
| COMPRESSION_ZSTD | 5 |  |



<a name="wallaby-v1-FailureMode"></a>

### FailureMode


| Name | Number | Description |
| ---- | ------ | ----------- |
| FAILURE_MODE_UNSPECIFIED | 0 |  |
| FAILURE_MODE_HOLD_SLOT | 1 |  |
| FAILURE_MODE_DROP_SLOT | 2 |  |



<a name="wallaby-v1-FlowState"></a>

### FlowState


| Name | Number | Description |
| ---- | ------ | ----------- |
| FLOW_STATE_UNSPECIFIED | 0 |  |
| FLOW_STATE_CREATED | 1 |  |
| FLOW_STATE_RUNNING | 2 |  |
| FLOW_STATE_PAUSED | 3 |  |
| FLOW_STATE_STOPPING | 4 |  |
| FLOW_STATE_FAILED | 5 |  |
| FLOW_STATE_STOPPED | 6 |  |



<a name="wallaby-v1-GiveUpPolicy"></a>

### GiveUpPolicy


| Name | Number | Description |
| ---- | ------ | ----------- |
| GIVE_UP_POLICY_UNSPECIFIED | 0 |  |
| GIVE_UP_POLICY_NEVER | 1 |  |
| GIVE_UP_POLICY_ON_RETRY_EXHAUSTION | 2 |  |



<a name="wallaby-v1-IcebergCatalogProfile"></a>

### IcebergCatalogProfile


| Name | Number | Description |
| ---- | ------ | ----------- |
| ICEBERG_CATALOG_PROFILE_UNSPECIFIED | 0 |  |
| ICEBERG_CATALOG_PROFILE_REST | 1 |  |
| ICEBERG_CATALOG_PROFILE_S3_TABLES | 2 |  |



<a name="wallaby-v1-KafkaAcks"></a>

### KafkaAcks


| Name | Number | Description |
| ---- | ------ | ----------- |
| KAFKA_ACKS_UNSPECIFIED | 0 |  |
| KAFKA_ACKS_NONE | 1 |  |
| KAFKA_ACKS_LEADER | 2 |  |
| KAFKA_ACKS_ALL | 3 |  |



<a name="wallaby-v1-KafkaKeyMode"></a>

### KafkaKeyMode


| Name | Number | Description |
| ---- | ------ | ----------- |
| KAFKA_KEY_MODE_UNSPECIFIED | 0 |  |
| KAFKA_KEY_MODE_HASH | 1 |  |
| KAFKA_KEY_MODE_RAW | 2 |  |



<a name="wallaby-v1-KafkaMessageMode"></a>

### KafkaMessageMode


| Name | Number | Description |
| ---- | ------ | ----------- |
| KAFKA_MESSAGE_MODE_UNSPECIFIED | 0 |  |
| KAFKA_MESSAGE_MODE_BATCH | 1 |  |
| KAFKA_MESSAGE_MODE_RECORD | 2 |  |



<a name="wallaby-v1-ManagedProfile"></a>

### ManagedProfile


| Name | Number | Description |
| ---- | ------ | ----------- |
| MANAGED_PROFILE_UNSPECIFIED | 0 |  |
| MANAGED_PROFILE_POSTGRES_TO_POSTGRES_V1 | 1 |  |
| MANAGED_PROFILE_POSTGRES_TO_CLICKHOUSE_APPEND_V1 | 2 |  |
| MANAGED_PROFILE_POSTGRESQL_TO_SNOWFLAKE_SQL_V1 | 3 |  |
| MANAGED_PROFILE_POSTGRESQL_TO_SNOWFLAKE_STAGED_V1 | 4 |  |
| MANAGED_PROFILE_POSTGRESQL_TO_SNOWFLAKE_STREAMING_V1 | 5 |  |



<a name="wallaby-v1-PayloadMode"></a>

### PayloadMode


| Name | Number | Description |
| ---- | ------ | ----------- |
| PAYLOAD_MODE_UNSPECIFIED | 0 |  |
| PAYLOAD_MODE_WIRE | 1 |  |
| PAYLOAD_MODE_RECORD_JSON | 2 |  |
| PAYLOAD_MODE_WAL | 3 |  |



<a name="wallaby-v1-PostgresSourceMode"></a>

### PostgresSourceMode


| Name | Number | Description |
| ---- | ------ | ----------- |
| POSTGRES_SOURCE_MODE_UNSPECIFIED | 0 |  |
| POSTGRES_SOURCE_MODE_CDC | 1 |  |
| POSTGRES_SOURCE_MODE_BACKFILL | 2 |  |



<a name="wallaby-v1-SyncPublicationMode"></a>

### SyncPublicationMode


| Name | Number | Description |
| ---- | ------ | ----------- |
| SYNC_PUBLICATION_MODE_UNSPECIFIED | 0 |  |
| SYNC_PUBLICATION_MODE_ADD | 1 |  |
| SYNC_PUBLICATION_MODE_SYNC | 2 |  |



<a name="wallaby-v1-ToastFetchMode"></a>

### ToastFetchMode


| Name | Number | Description |
| ---- | ------ | ----------- |
| TOAST_FETCH_MODE_UNSPECIFIED | 0 |  |
| TOAST_FETCH_MODE_OFF | 1 |  |
| TOAST_FETCH_MODE_SOURCE | 2 |  |
| TOAST_FETCH_MODE_CACHE | 3 |  |
| TOAST_FETCH_MODE_FULL | 4 |  |



<a name="wallaby-v1-WireFormat"></a>

### WireFormat


| Name | Number | Description |
| ---- | ------ | ----------- |
| WIRE_FORMAT_UNSPECIFIED | 0 |  |
| WIRE_FORMAT_ARROW | 1 |  |
| WIRE_FORMAT_PARQUET | 2 |  |
| WIRE_FORMAT_PROTO | 3 |  |
| WIRE_FORMAT_AVRO | 4 |  |
| WIRE_FORMAT_JSON | 5 |  |










<a name="wallaby_v1_checkpoint-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## wallaby/v1/checkpoint.proto



<a name="wallaby-v1-FlowCheckpoint"></a>

### FlowCheckpoint



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |
| checkpoint | [Checkpoint](#wallaby-v1-Checkpoint) |  |  |






<a name="wallaby-v1-GetCheckpointRequest"></a>

### GetCheckpointRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |






<a name="wallaby-v1-ListCheckpointsRequest"></a>

### ListCheckpointsRequest







<a name="wallaby-v1-ListCheckpointsResponse"></a>

### ListCheckpointsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| checkpoints | [FlowCheckpoint](#wallaby-v1-FlowCheckpoint) | repeated |  |






<a name="wallaby-v1-PutCheckpointRequest"></a>

### PutCheckpointRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |
| checkpoint | [Checkpoint](#wallaby-v1-Checkpoint) |  |  |












<a name="wallaby-v1-CheckpointService"></a>

### CheckpointService
CheckpointService reads and writes durable flow positions.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetCheckpoint | [GetCheckpointRequest](#wallaby-v1-GetCheckpointRequest) | [FlowCheckpoint](#wallaby-v1-FlowCheckpoint) |  |
| PutCheckpoint | [PutCheckpointRequest](#wallaby-v1-PutCheckpointRequest) | [FlowCheckpoint](#wallaby-v1-FlowCheckpoint) |  |
| ListCheckpoints | [ListCheckpointsRequest](#wallaby-v1-ListCheckpointsRequest) | [ListCheckpointsResponse](#wallaby-v1-ListCheckpointsResponse) |  |





<a name="wallaby_v1_data-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## wallaby/v1/data.proto



<a name="wallaby-v1-Batch"></a>

### Batch



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| schema | [Schema](#wallaby-v1-Schema) |  |  |
| records | [Record](#wallaby-v1-Record) | repeated |  |
| checkpoint | [Checkpoint](#wallaby-v1-Checkpoint) |  |  |
| wire_format | [WireFormat](#wallaby-v1-WireFormat) |  |  |






<a name="wallaby-v1-Record"></a>

### Record



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table | [string](#string) |  |  |
| operation | [string](#string) |  |  |
| key | [bytes](#bytes) |  |  |
| before_json | [bytes](#bytes) |  |  |
| after_json | [bytes](#bytes) |  |  |
| ddl | [string](#string) |  |  |
| timestamp_unix_millis | [int64](#int64) |  |  |
| schema_version | [int64](#int64) |  |  |
| unchanged | [string](#string) | repeated |  |






<a name="wallaby-v1-Schema"></a>

### Schema



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| namespace | [string](#string) |  |  |
| version | [int64](#int64) |  |  |
| columns | [SchemaColumn](#wallaby-v1-SchemaColumn) | repeated |  |






<a name="wallaby-v1-SchemaColumn"></a>

### SchemaColumn



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| type | [string](#string) |  |  |
| nullable | [bool](#bool) |  |  |
| generated | [bool](#bool) |  |  |
| expression | [string](#string) |  |  |















<a name="wallaby_v1_ddl-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## wallaby/v1/ddl.proto



<a name="wallaby-v1-ApproveDDLRequest"></a>

### ApproveDDLRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [int64](#int64) |  |  |






<a name="wallaby-v1-ApproveDDLResponse"></a>

### ApproveDDLResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [DDLEvent](#wallaby-v1-DDLEvent) |  |  |






<a name="wallaby-v1-DDLEvent"></a>

### DDLEvent



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [int64](#int64) |  |  |
| ddl | [string](#string) |  |  |
| lsn | [string](#string) |  |  |
| status | [string](#string) |  |  |
| plan_json | [string](#string) |  |  |
| created_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |
| flow_id | [string](#string) |  |  |






<a name="wallaby-v1-ListDDLRequest"></a>

### ListDDLRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [string](#string) |  |  |
| flow_id | [string](#string) |  |  |






<a name="wallaby-v1-ListDDLResponse"></a>

### ListDDLResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| events | [DDLEvent](#wallaby-v1-DDLEvent) | repeated |  |






<a name="wallaby-v1-ListPendingDDLRequest"></a>

### ListPendingDDLRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |






<a name="wallaby-v1-ListPendingDDLResponse"></a>

### ListPendingDDLResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| events | [DDLEvent](#wallaby-v1-DDLEvent) | repeated |  |






<a name="wallaby-v1-RejectDDLRequest"></a>

### RejectDDLRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [int64](#int64) |  |  |






<a name="wallaby-v1-RejectDDLResponse"></a>

### RejectDDLResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [DDLEvent](#wallaby-v1-DDLEvent) |  |  |












<a name="wallaby-v1-DDLService"></a>

### DDLService
DDLService lists schema changes and manages approval state. Applied state is
advanced only by durable data-plane execution receipts.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| ListPendingDDL | [ListPendingDDLRequest](#wallaby-v1-ListPendingDDLRequest) | [ListPendingDDLResponse](#wallaby-v1-ListPendingDDLResponse) |  |
| ListDDL | [ListDDLRequest](#wallaby-v1-ListDDLRequest) | [ListDDLResponse](#wallaby-v1-ListDDLResponse) |  |
| ApproveDDL | [ApproveDDLRequest](#wallaby-v1-ApproveDDLRequest) | [ApproveDDLResponse](#wallaby-v1-ApproveDDLResponse) |  |
| RejectDDL | [RejectDDLRequest](#wallaby-v1-RejectDDLRequest) | [RejectDDLResponse](#wallaby-v1-RejectDDLResponse) |  |





<a name="wallaby_v1_flow-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## wallaby/v1/flow.proto



<a name="wallaby-v1-AddPublicationTablesRequest"></a>

### AddPublicationTablesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |
| dsn | [string](#string) |  |  |
| publication | [string](#string) |  |  |
| tables | [string](#string) | repeated |  |
| rds_iam | [RDSIAMConfig](#wallaby-v1-RDSIAMConfig) |  |  |






<a name="wallaby-v1-CleanupFlowRequest"></a>

### CleanupFlowRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |
| drop_slot | [bool](#bool) | optional |  |
| drop_publication | [bool](#bool) | optional |  |
| drop_source_state | [bool](#bool) | optional |  |






<a name="wallaby-v1-CleanupFlowResponse"></a>

### CleanupFlowResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cleaned | [bool](#bool) |  |  |






<a name="wallaby-v1-CreateFlowRequest"></a>

### CreateFlowRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow | [Flow](#wallaby-v1-Flow) |  |  |
| start_immediately | [bool](#bool) |  |  |






<a name="wallaby-v1-DeleteFlowRequest"></a>

### DeleteFlowRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |






<a name="wallaby-v1-DeleteFlowResponse"></a>

### DeleteFlowResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="wallaby-v1-DropPublicationTablesRequest"></a>

### DropPublicationTablesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |
| dsn | [string](#string) |  |  |
| publication | [string](#string) |  |  |
| tables | [string](#string) | repeated |  |
| rds_iam | [RDSIAMConfig](#wallaby-v1-RDSIAMConfig) |  |  |






<a name="wallaby-v1-DropReplicationSlotRequest"></a>

### DropReplicationSlotRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |
| dsn | [string](#string) |  |  |
| slot | [string](#string) |  |  |
| if_exists | [bool](#bool) |  |  |
| rds_iam | [RDSIAMConfig](#wallaby-v1-RDSIAMConfig) |  |  |






<a name="wallaby-v1-DropReplicationSlotResponse"></a>

### DropReplicationSlotResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [string](#string) |  |  |
| found | [bool](#bool) |  |  |
| dropped | [bool](#bool) |  |  |
| flow_id | [string](#string) |  |  |






<a name="wallaby-v1-GetFlowRequest"></a>

### GetFlowRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |






<a name="wallaby-v1-GetReplicationSlotRequest"></a>

### GetReplicationSlotRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |
| dsn | [string](#string) |  |  |
| slot | [string](#string) |  |  |
| rds_iam | [RDSIAMConfig](#wallaby-v1-RDSIAMConfig) |  |  |






<a name="wallaby-v1-GetReplicationSlotResponse"></a>

### GetReplicationSlotResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [ReplicationSlotInfo](#wallaby-v1-ReplicationSlotInfo) |  |  |






<a name="wallaby-v1-ListFlowsRequest"></a>

### ListFlowsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| page_size | [int32](#int32) |  |  |
| page_token | [string](#string) |  |  |






<a name="wallaby-v1-ListFlowsResponse"></a>

### ListFlowsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flows | [Flow](#wallaby-v1-Flow) | repeated |  |
| next_page_token | [string](#string) |  |  |






<a name="wallaby-v1-ListPublicationTablesRequest"></a>

### ListPublicationTablesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |
| dsn | [string](#string) |  |  |
| publication | [string](#string) |  |  |
| rds_iam | [RDSIAMConfig](#wallaby-v1-RDSIAMConfig) |  |  |






<a name="wallaby-v1-ListPublicationTablesResponse"></a>

### ListPublicationTablesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tables | [string](#string) | repeated |  |
| publication | [string](#string) |  |  |
| flow_id | [string](#string) |  |  |






<a name="wallaby-v1-ListReplicationSlotsRequest"></a>

### ListReplicationSlotsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |
| dsn | [string](#string) |  |  |
| slot | [string](#string) |  |  |
| rds_iam | [RDSIAMConfig](#wallaby-v1-RDSIAMConfig) |  |  |






<a name="wallaby-v1-ListReplicationSlotsResponse"></a>

### ListReplicationSlotsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slots | [ReplicationSlotInfo](#wallaby-v1-ReplicationSlotInfo) | repeated |  |
| flow_id | [string](#string) |  |  |






<a name="wallaby-v1-PauseFlowRequest"></a>

### PauseFlowRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |






<a name="wallaby-v1-PublicationTablesMutationResponse"></a>

### PublicationTablesMutationResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tables | [string](#string) | repeated |  |
| publication | [string](#string) |  |  |






<a name="wallaby-v1-ReconfigureFlowRequest"></a>

### ReconfigureFlowRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow | [Flow](#wallaby-v1-Flow) |  |  |
| pause_first | [bool](#bool) | optional |  |
| resume_after | [bool](#bool) | optional |  |
| sync_publication | [bool](#bool) | optional |  |






<a name="wallaby-v1-ReplicationSlotInfo"></a>

### ReplicationSlotInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot_name | [string](#string) |  |  |
| plugin | [string](#string) |  |  |
| slot_type | [string](#string) |  |  |
| database | [string](#string) |  |  |
| active | [bool](#bool) |  |  |
| active_pid | [int32](#int32) |  |  |
| temporary | [bool](#bool) |  |  |
| wal_status | [string](#string) |  |  |
| restart_lsn | [string](#string) |  |  |
| confirmed_flush_lsn | [string](#string) |  |  |
| active_pid_present | [bool](#bool) |  |  |






<a name="wallaby-v1-ResumeFlowRequest"></a>

### ResumeFlowRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |






<a name="wallaby-v1-RunFlowOnceRequest"></a>

### RunFlowOnceRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |






<a name="wallaby-v1-RunFlowOnceResponse"></a>

### RunFlowOnceResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dispatched | [bool](#bool) |  |  |






<a name="wallaby-v1-ScrapePublicationTablesRequest"></a>

### ScrapePublicationTablesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |
| dsn | [string](#string) |  |  |
| publication | [string](#string) |  |  |
| schemas | [string](#string) | repeated |  |
| apply | [bool](#bool) |  |  |
| rds_iam | [RDSIAMConfig](#wallaby-v1-RDSIAMConfig) |  |  |






<a name="wallaby-v1-ScrapePublicationTablesResponse"></a>

### ScrapePublicationTablesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| discovered_tables | [string](#string) | repeated |  |
| missing_tables | [string](#string) | repeated |  |
| applied | [bool](#bool) |  |  |
| flow_id | [string](#string) |  |  |






<a name="wallaby-v1-StartFlowRequest"></a>

### StartFlowRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |






<a name="wallaby-v1-StopFlowRequest"></a>

### StopFlowRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |






<a name="wallaby-v1-SyncPublicationTablesRequest"></a>

### SyncPublicationTablesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |
| dsn | [string](#string) |  |  |
| publication | [string](#string) |  |  |
| tables | [string](#string) | repeated |  |
| mode | [string](#string) |  |  |
| rds_iam | [RDSIAMConfig](#wallaby-v1-RDSIAMConfig) |  |  |






<a name="wallaby-v1-SyncPublicationTablesResponse"></a>

### SyncPublicationTablesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| added | [string](#string) | repeated |  |
| removed | [string](#string) | repeated |  |
| publication | [string](#string) |  |  |
| flow_id | [string](#string) |  |  |






<a name="wallaby-v1-UpdateFlowRequest"></a>

### UpdateFlowRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow | [Flow](#wallaby-v1-Flow) |  |  |












<a name="wallaby-v1-FlowService"></a>

### FlowService
FlowService stores flow definitions and controls lifecycle and PostgreSQL source resources.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreateFlow | [CreateFlowRequest](#wallaby-v1-CreateFlowRequest) | [Flow](#wallaby-v1-Flow) |  |
| UpdateFlow | [UpdateFlowRequest](#wallaby-v1-UpdateFlowRequest) | [Flow](#wallaby-v1-Flow) |  |
| ReconfigureFlow | [ReconfigureFlowRequest](#wallaby-v1-ReconfigureFlowRequest) | [Flow](#wallaby-v1-Flow) |  |
| StartFlow | [StartFlowRequest](#wallaby-v1-StartFlowRequest) | [Flow](#wallaby-v1-Flow) |  |
| RunFlowOnce | [RunFlowOnceRequest](#wallaby-v1-RunFlowOnceRequest) | [RunFlowOnceResponse](#wallaby-v1-RunFlowOnceResponse) |  |
| PauseFlow | [PauseFlowRequest](#wallaby-v1-PauseFlowRequest) | [Flow](#wallaby-v1-Flow) | PauseFlow quiesces a running flow and keeps it resumable. |
| StopFlow | [StopFlowRequest](#wallaby-v1-StopFlowRequest) | [Flow](#wallaby-v1-Flow) | StopFlow cancels active execution and moves the flow to terminal stopped state. |
| ResumeFlow | [ResumeFlowRequest](#wallaby-v1-ResumeFlowRequest) | [Flow](#wallaby-v1-Flow) | ResumeFlow returns a paused flow to running state. |
| GetFlow | [GetFlowRequest](#wallaby-v1-GetFlowRequest) | [Flow](#wallaby-v1-Flow) |  |
| ListFlows | [ListFlowsRequest](#wallaby-v1-ListFlowsRequest) | [ListFlowsResponse](#wallaby-v1-ListFlowsResponse) |  |
| DeleteFlow | [DeleteFlowRequest](#wallaby-v1-DeleteFlowRequest) | [DeleteFlowResponse](#wallaby-v1-DeleteFlowResponse) |  |
| CleanupFlow | [CleanupFlowRequest](#wallaby-v1-CleanupFlowRequest) | [CleanupFlowResponse](#wallaby-v1-CleanupFlowResponse) |  |
| ListReplicationSlots | [ListReplicationSlotsRequest](#wallaby-v1-ListReplicationSlotsRequest) | [ListReplicationSlotsResponse](#wallaby-v1-ListReplicationSlotsResponse) |  |
| GetReplicationSlot | [GetReplicationSlotRequest](#wallaby-v1-GetReplicationSlotRequest) | [GetReplicationSlotResponse](#wallaby-v1-GetReplicationSlotResponse) |  |
| DropReplicationSlot | [DropReplicationSlotRequest](#wallaby-v1-DropReplicationSlotRequest) | [DropReplicationSlotResponse](#wallaby-v1-DropReplicationSlotResponse) |  |
| ListPublicationTables | [ListPublicationTablesRequest](#wallaby-v1-ListPublicationTablesRequest) | [ListPublicationTablesResponse](#wallaby-v1-ListPublicationTablesResponse) |  |
| AddPublicationTables | [AddPublicationTablesRequest](#wallaby-v1-AddPublicationTablesRequest) | [PublicationTablesMutationResponse](#wallaby-v1-PublicationTablesMutationResponse) |  |
| DropPublicationTables | [DropPublicationTablesRequest](#wallaby-v1-DropPublicationTablesRequest) | [PublicationTablesMutationResponse](#wallaby-v1-PublicationTablesMutationResponse) |  |
| SyncPublicationTables | [SyncPublicationTablesRequest](#wallaby-v1-SyncPublicationTablesRequest) | [SyncPublicationTablesResponse](#wallaby-v1-SyncPublicationTablesResponse) |  |
| ScrapePublicationTables | [ScrapePublicationTablesRequest](#wallaby-v1-ScrapePublicationTablesRequest) | [ScrapePublicationTablesResponse](#wallaby-v1-ScrapePublicationTablesResponse) |  |





<a name="wallaby_v1_ingest-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## wallaby/v1/ingest.proto



<a name="wallaby-v1-IngestBatchRequest"></a>

### IngestBatchRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |
| destination | [string](#string) |  |  |
| wire_format | [WireFormat](#wallaby-v1-WireFormat) |  |  |
| payload | [bytes](#bytes) |  |  |
| checkpoint | [Checkpoint](#wallaby-v1-Checkpoint) |  |  |






<a name="wallaby-v1-IngestBatchResponse"></a>

### IngestBatchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| accepted | [bool](#bool) |  |  |
| message | [string](#string) |  |  |












<a name="wallaby-v1-IngestService"></a>

### IngestService
IngestService accepts an encoded batch from a remote WALlaby destination.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| IngestBatch | [IngestBatchRequest](#wallaby-v1-IngestBatchRequest) | [IngestBatchResponse](#wallaby-v1-IngestBatchResponse) |  |





<a name="wallaby_v1_stream-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## wallaby/v1/stream.proto



<a name="wallaby-v1-StreamAckRequest"></a>

### StreamAckRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stream | [string](#string) |  |  |
| consumer_group | [string](#string) |  |  |
| ids | [int64](#int64) | repeated |  |






<a name="wallaby-v1-StreamAckResponse"></a>

### StreamAckResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| acked | [int64](#int64) |  |  |






<a name="wallaby-v1-StreamMessage"></a>

### StreamMessage



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [int64](#int64) |  |  |
| stream | [string](#string) |  |  |
| namespace | [string](#string) |  |  |
| table | [string](#string) |  |  |
| lsn | [string](#string) |  |  |
| wire_format | [string](#string) |  |  |
| payload | [bytes](#bytes) |  |  |
| created_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |
| registry_subject | [string](#string) |  |  |
| registry_id | [string](#string) |  |  |
| registry_version | [int32](#int32) |  |  |






<a name="wallaby-v1-StreamPullRequest"></a>

### StreamPullRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stream | [string](#string) |  |  |
| consumer_group | [string](#string) |  |  |
| max_messages | [int32](#int32) |  |  |
| visibility_timeout_seconds | [int32](#int32) |  |  |
| consumer_id | [string](#string) |  |  |






<a name="wallaby-v1-StreamPullResponse"></a>

### StreamPullResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| messages | [StreamMessage](#wallaby-v1-StreamMessage) | repeated |  |






<a name="wallaby-v1-StreamReplayRequest"></a>

### StreamReplayRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stream | [string](#string) |  |  |
| consumer_group | [string](#string) |  |  |
| from_lsn | [string](#string) |  |  |
| since | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |






<a name="wallaby-v1-StreamReplayResponse"></a>

### StreamReplayResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reset | [int64](#int64) |  |  |












<a name="wallaby-v1-StreamService"></a>

### StreamService
StreamService provides pull, acknowledgement, and replay operations for stored streams.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Pull | [StreamPullRequest](#wallaby-v1-StreamPullRequest) | [StreamPullResponse](#wallaby-v1-StreamPullResponse) |  |
| Ack | [StreamAckRequest](#wallaby-v1-StreamAckRequest) | [StreamAckResponse](#wallaby-v1-StreamAckResponse) |  |
| Replay | [StreamReplayRequest](#wallaby-v1-StreamReplayRequest) | [StreamReplayResponse](#wallaby-v1-StreamReplayResponse) |  |





## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |
