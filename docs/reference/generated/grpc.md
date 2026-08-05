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
    - [Checkpoint](#wallaby-v1-Checkpoint)
    - [Checkpoint.MetadataEntry](#wallaby-v1-Checkpoint-MetadataEntry)
    - [DDLPolicy](#wallaby-v1-DDLPolicy)
    - [Endpoint](#wallaby-v1-Endpoint)
    - [Endpoint.OptionsEntry](#wallaby-v1-Endpoint-OptionsEntry)
    - [Flow](#wallaby-v1-Flow)
    - [FlowConfig](#wallaby-v1-FlowConfig)
    - [MaterializationPolicy](#wallaby-v1-MaterializationPolicy)

    - [AckPolicy](#wallaby-v1-AckPolicy)
    - [EndpointType](#wallaby-v1-EndpointType)
    - [FailureMode](#wallaby-v1-FailureMode)
    - [FlowState](#wallaby-v1-FlowState)
    - [GiveUpPolicy](#wallaby-v1-GiveUpPolicy)
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
    - [AddPublicationTablesRequest.OptionsEntry](#wallaby-v1-AddPublicationTablesRequest-OptionsEntry)
    - [CleanupFlowRequest](#wallaby-v1-CleanupFlowRequest)
    - [CleanupFlowResponse](#wallaby-v1-CleanupFlowResponse)
    - [CreateFlowRequest](#wallaby-v1-CreateFlowRequest)
    - [DeleteFlowRequest](#wallaby-v1-DeleteFlowRequest)
    - [DeleteFlowResponse](#wallaby-v1-DeleteFlowResponse)
    - [DropPublicationTablesRequest](#wallaby-v1-DropPublicationTablesRequest)
    - [DropPublicationTablesRequest.OptionsEntry](#wallaby-v1-DropPublicationTablesRequest-OptionsEntry)
    - [DropReplicationSlotRequest](#wallaby-v1-DropReplicationSlotRequest)
    - [DropReplicationSlotRequest.OptionsEntry](#wallaby-v1-DropReplicationSlotRequest-OptionsEntry)
    - [DropReplicationSlotResponse](#wallaby-v1-DropReplicationSlotResponse)
    - [GetFlowRequest](#wallaby-v1-GetFlowRequest)
    - [GetReplicationSlotRequest](#wallaby-v1-GetReplicationSlotRequest)
    - [GetReplicationSlotRequest.OptionsEntry](#wallaby-v1-GetReplicationSlotRequest-OptionsEntry)
    - [GetReplicationSlotResponse](#wallaby-v1-GetReplicationSlotResponse)
    - [ListFlowsRequest](#wallaby-v1-ListFlowsRequest)
    - [ListFlowsResponse](#wallaby-v1-ListFlowsResponse)
    - [ListPublicationTablesRequest](#wallaby-v1-ListPublicationTablesRequest)
    - [ListPublicationTablesRequest.OptionsEntry](#wallaby-v1-ListPublicationTablesRequest-OptionsEntry)
    - [ListPublicationTablesResponse](#wallaby-v1-ListPublicationTablesResponse)
    - [ListReplicationSlotsRequest](#wallaby-v1-ListReplicationSlotsRequest)
    - [ListReplicationSlotsRequest.OptionsEntry](#wallaby-v1-ListReplicationSlotsRequest-OptionsEntry)
    - [ListReplicationSlotsResponse](#wallaby-v1-ListReplicationSlotsResponse)
    - [PauseFlowRequest](#wallaby-v1-PauseFlowRequest)
    - [PublicationTablesMutationResponse](#wallaby-v1-PublicationTablesMutationResponse)
    - [ReconfigureFlowRequest](#wallaby-v1-ReconfigureFlowRequest)
    - [ReplicationSlotInfo](#wallaby-v1-ReplicationSlotInfo)
    - [ResumeFlowRequest](#wallaby-v1-ResumeFlowRequest)
    - [RunFlowOnceRequest](#wallaby-v1-RunFlowOnceRequest)
    - [RunFlowOnceResponse](#wallaby-v1-RunFlowOnceResponse)
    - [ScrapePublicationTablesRequest](#wallaby-v1-ScrapePublicationTablesRequest)
    - [ScrapePublicationTablesRequest.OptionsEntry](#wallaby-v1-ScrapePublicationTablesRequest-OptionsEntry)
    - [ScrapePublicationTablesResponse](#wallaby-v1-ScrapePublicationTablesResponse)
    - [StartFlowRequest](#wallaby-v1-StartFlowRequest)
    - [StopFlowRequest](#wallaby-v1-StopFlowRequest)
    - [SyncPublicationTablesRequest](#wallaby-v1-SyncPublicationTablesRequest)
    - [SyncPublicationTablesRequest.OptionsEntry](#wallaby-v1-SyncPublicationTablesRequest-OptionsEntry)
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
| target_column | [string](#string) |  |  |






<a name="wallaby-v1-FutureTableMapping"></a>

### FutureTableMapping



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| action | [MappingAction](#wallaby-v1-MappingAction) |  |  |
| target_schema | [string](#string) |  |  |
| target_table | [string](#string) |  |  |
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
| version | [uint32](#uint32) |  |  |
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






<a name="wallaby-v1-DDLPolicy"></a>

### DDLPolicy



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| gate | [bool](#bool) | optional |  |
| auto_approve | [bool](#bool) | optional |  |
| auto_apply | [bool](#bool) | optional |  |






<a name="wallaby-v1-Endpoint"></a>

### Endpoint



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| type | [EndpointType](#wallaby-v1-EndpointType) |  |  |
| options | [Endpoint.OptionsEntry](#wallaby-v1-Endpoint-OptionsEntry) | repeated |  |






<a name="wallaby-v1-Endpoint-OptionsEntry"></a>

### Endpoint.OptionsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






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
| schema_registry_subject | [string](#string) |  |  |
| schema_registry_proto_types_subject | [string](#string) |  |  |
| schema_registry_subject_mode | [string](#string) |  |  |
| materialization | [MaterializationPolicy](#wallaby-v1-MaterializationPolicy) |  |  |
| table_mappings | [TableMappings](#wallaby-v1-TableMappings) |  |  |






<a name="wallaby-v1-MaterializationPolicy"></a>

### MaterializationPolicy



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| projection_id | [string](#string) |  | Mapped materialized flows require exactly canonical_cdc_parquet_v2. canonical_cdc_parquet_v1 is frozen for historical encoder verification only. |








<a name="wallaby-v1-AckPolicy"></a>

### AckPolicy


| Name | Number | Description |
| ---- | ------ | ----------- |
| ACK_POLICY_UNSPECIFIED | 0 |  |
| ACK_POLICY_ALL | 1 |  |
| ACK_POLICY_PRIMARY | 2 |  |
| ACK_POLICY_MATERIALIZED | 3 | ACK_POLICY_MATERIALIZED acknowledges a CDC transaction only after its canonical immutable objects and fenced PostgreSQL publication/checkpoint commit. A data-free startup cut is rooted as an object-free canonical publication before feedback. A configured Iceberg endpoint consumes the publication asynchronously and never delays source acknowledgement. |



<a name="wallaby-v1-EndpointType"></a>

### EndpointType


| Name | Number | Description |
| ---- | ------ | ----------- |
| ENDPOINT_TYPE_UNSPECIFIED | 0 |  |
| ENDPOINT_TYPE_POSTGRES | 1 |  |
| ENDPOINT_TYPE_SNOWFLAKE | 2 |  |
| ENDPOINT_TYPE_S3 | 3 |  |
| ENDPOINT_TYPE_KAFKA | 4 |  |
| ENDPOINT_TYPE_HTTP | 5 |  |
| ENDPOINT_TYPE_GRPC | 6 |  |
| ENDPOINT_TYPE_PROTO | 7 |  |
| ENDPOINT_TYPE_PGSTREAM | 8 |  |
| ENDPOINT_TYPE_SNOWPIPE | 9 |  |
| ENDPOINT_TYPE_PARQUET | 10 |  |
| ENDPOINT_TYPE_DUCKDB | 11 |  |
| ENDPOINT_TYPE_BUFSTREAM | 12 |  |
| ENDPOINT_TYPE_CLICKHOUSE | 13 |  |
| ENDPOINT_TYPE_DUCKLAKE | 14 |  |
| ENDPOINT_TYPE_ICEBERG | 15 | Iceberg is an asynchronous consumer of the canonical artifact log, including AWS S3 Tables exposed read-only through external catalogs such as Snowflake. It is never a direct current-state/upsert destination. |



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
| options | [AddPublicationTablesRequest.OptionsEntry](#wallaby-v1-AddPublicationTablesRequest-OptionsEntry) | repeated |  |






<a name="wallaby-v1-AddPublicationTablesRequest-OptionsEntry"></a>

### AddPublicationTablesRequest.OptionsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






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
| options | [DropPublicationTablesRequest.OptionsEntry](#wallaby-v1-DropPublicationTablesRequest-OptionsEntry) | repeated |  |






<a name="wallaby-v1-DropPublicationTablesRequest-OptionsEntry"></a>

### DropPublicationTablesRequest.OptionsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="wallaby-v1-DropReplicationSlotRequest"></a>

### DropReplicationSlotRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flow_id | [string](#string) |  |  |
| dsn | [string](#string) |  |  |
| slot | [string](#string) |  |  |
| if_exists | [bool](#bool) |  |  |
| options | [DropReplicationSlotRequest.OptionsEntry](#wallaby-v1-DropReplicationSlotRequest-OptionsEntry) | repeated |  |






<a name="wallaby-v1-DropReplicationSlotRequest-OptionsEntry"></a>

### DropReplicationSlotRequest.OptionsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






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
| options | [GetReplicationSlotRequest.OptionsEntry](#wallaby-v1-GetReplicationSlotRequest-OptionsEntry) | repeated |  |






<a name="wallaby-v1-GetReplicationSlotRequest-OptionsEntry"></a>

### GetReplicationSlotRequest.OptionsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






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
| options | [ListPublicationTablesRequest.OptionsEntry](#wallaby-v1-ListPublicationTablesRequest-OptionsEntry) | repeated |  |






<a name="wallaby-v1-ListPublicationTablesRequest-OptionsEntry"></a>

### ListPublicationTablesRequest.OptionsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






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
| options | [ListReplicationSlotsRequest.OptionsEntry](#wallaby-v1-ListReplicationSlotsRequest-OptionsEntry) | repeated |  |






<a name="wallaby-v1-ListReplicationSlotsRequest-OptionsEntry"></a>

### ListReplicationSlotsRequest.OptionsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






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
| options | [ScrapePublicationTablesRequest.OptionsEntry](#wallaby-v1-ScrapePublicationTablesRequest-OptionsEntry) | repeated |  |






<a name="wallaby-v1-ScrapePublicationTablesRequest-OptionsEntry"></a>

### ScrapePublicationTablesRequest.OptionsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






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
| options | [SyncPublicationTablesRequest.OptionsEntry](#wallaby-v1-SyncPublicationTablesRequest-OptionsEntry) | repeated |  |






<a name="wallaby-v1-SyncPublicationTablesRequest-OptionsEntry"></a>

### SyncPublicationTablesRequest.OptionsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






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
