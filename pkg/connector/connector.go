package connector

import (
	"context"
	"time"
)

// EndpointType identifies the connector implementation.
type EndpointType string

const (
	EndpointPostgres   EndpointType = "postgres"
	EndpointSnowflake  EndpointType = "snowflake"
	EndpointS3         EndpointType = "s3"
	EndpointKafka      EndpointType = "kafka"
	EndpointHTTP       EndpointType = "http"
	EndpointGRPC       EndpointType = "grpc"
	EndpointProto      EndpointType = "proto"
	EndpointPGStream   EndpointType = "pgstream"
	EndpointSnowpipe   EndpointType = "snowpipe"
	EndpointParquet    EndpointType = "parquet"
	EndpointDuckDB     EndpointType = "duckdb"
	EndpointDuckLake   EndpointType = "ducklake"
	EndpointBufStream  EndpointType = "bufstream"
	EndpointClickHouse EndpointType = "clickhouse"
)

// WireFormat describes the wire encoding used between connectors.
type WireFormat string

const (
	WireFormatArrow   WireFormat = "arrow"
	WireFormatAvro    WireFormat = "avro"
	WireFormatParquet WireFormat = "parquet"
	WireFormatProto   WireFormat = "proto"
	WireFormatJSON    WireFormat = "json"
)

// Operation indicates the change type for a record.
type Operation string

const (
	OpInsert Operation = "insert"
	OpUpdate Operation = "update"
	OpDelete Operation = "delete"
	OpDDL    Operation = "ddl"
	OpLoad   Operation = "load"
)

// Spec defines a connector instance plus implementation-specific options.
type Spec struct {
	Name    string
	Type    EndpointType
	Options map[string]string
}

// Capabilities describe what a connector can handle.
type Capabilities struct {
	SupportsDDL           bool
	SupportsSchemaChanges bool
	SupportsStreaming     bool
	SupportsBulkLoad      bool
	SupportsTypeMapping   bool
	SupportedWireFormats  []WireFormat
}

// Schema describes a table-level schema snapshot.
type Schema struct {
	Name      string
	Namespace string
	Version   int64
	Columns   []Column
	// QuotedIdentifiers tracks identifiers that were quoted in source DDL.
	QuotedIdentifiers map[string]bool
}

// Column defines a schema field.
type Column struct {
	Name       string
	Type       string
	Nullable   bool
	Generated  bool
	Expression string
	// TypeMetadata carries optional source-specific type info (e.g. extension, oid).
	TypeMetadata map[string]string
}

// Checkpoint identifies a durable offset for a flow.
type Checkpoint struct {
	LSN       string
	Timestamp time.Time
	Metadata  map[string]string
}

// FlowCheckpoint ties a checkpoint to a flow ID.
type FlowCheckpoint struct {
	FlowID     string
	Checkpoint Checkpoint
}

// Record represents a single change or DDL event.
type Record struct {
	Table         string
	Operation     Operation
	SchemaVersion int64
	Key           []byte
	Payload       []byte
	Before        map[string]any
	After         map[string]any
	Unchanged     []string
	DDL           string
	Timestamp     time.Time
}

// Batch is the unit passed between sources and destinations.
type Batch struct {
	Records    []Record
	Schema     Schema
	Checkpoint Checkpoint
	WireFormat WireFormat
}

// Source reads from an upstream system.
type Source interface {
	Open(ctx context.Context, spec Spec) error
	Read(ctx context.Context) (Batch, error)
	Ack(ctx context.Context, checkpoint Checkpoint) error
	Close(ctx context.Context) error
	Capabilities() Capabilities
}

// SlotDropper is implemented by sources that can drop replication slots.
type SlotDropper interface {
	DropSlot(ctx context.Context) error
}

// Destination writes to a downstream system.
type Destination interface {
	Open(ctx context.Context, spec Spec) error
	Write(ctx context.Context, batch Batch) error
	ApplyDDL(ctx context.Context, schema Schema, record Record) error
	TypeMappings() map[string]string
	Close(ctx context.Context) error
	Capabilities() Capabilities
}

// CheckpointStore persists checkpoints for recovery.
type CheckpointStore interface {
	Get(ctx context.Context, flowID string) (Checkpoint, error)
	Put(ctx context.Context, flowID string, checkpoint Checkpoint) error
	List(ctx context.Context) ([]FlowCheckpoint, error)
}
