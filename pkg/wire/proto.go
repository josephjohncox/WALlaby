package wire

import (
	"encoding/json"
	"fmt"

	ductstreampb "github.com/josephjohncox/ductstream/gen/go/ductstream/v1"
	"github.com/josephjohncox/ductstream/pkg/connector"
	"google.golang.org/protobuf/proto"
)

// ProtoCodec encodes batches as protobuf messages.
type ProtoCodec struct{}

func (c *ProtoCodec) Name() connector.WireFormat {
	return connector.WireFormatProto
}

func (c *ProtoCodec) ContentType() string {
	return "application/x-protobuf"
}

func (c *ProtoCodec) Encode(batch connector.Batch) ([]byte, error) {
	pbBatch, err := batchToProto(batch)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(pbBatch)
}

func batchToProto(batch connector.Batch) (*ductstreampb.Batch, error) {
	schema := &ductstreampb.Schema{
		Name:      batch.Schema.Name,
		Namespace: batch.Schema.Namespace,
		Version:   batch.Schema.Version,
	}
	for _, col := range batch.Schema.Columns {
		schema.Columns = append(schema.Columns, &ductstreampb.SchemaColumn{
			Name:       col.Name,
			Type:       col.Type,
			Nullable:   col.Nullable,
			Generated:  col.Generated,
			Expression: col.Expression,
		})
	}

	records := make([]*ductstreampb.Record, 0, len(batch.Records))
	for _, rec := range batch.Records {
		before, err := json.Marshal(rec.Before)
		if err != nil {
			return nil, fmt.Errorf("marshal before: %w", err)
		}
		after, err := json.Marshal(rec.After)
		if err != nil {
			return nil, fmt.Errorf("marshal after: %w", err)
		}

		records = append(records, &ductstreampb.Record{
			Table:               rec.Table,
			Operation:           string(rec.Operation),
			Key:                 rec.Key,
			BeforeJson:          before,
			AfterJson:           after,
			Ddl:                 rec.DDL,
			TimestampUnixMillis: rec.Timestamp.UnixMilli(),
			SchemaVersion:       rec.SchemaVersion,
			Unchanged:           rec.Unchanged,
		})
	}

	return &ductstreampb.Batch{
		Schema:  schema,
		Records: records,
		Checkpoint: &ductstreampb.Checkpoint{
			Lsn:                 batch.Checkpoint.LSN,
			TimestampUnixMillis: batch.Checkpoint.Timestamp.UnixMilli(),
			Metadata:            batch.Checkpoint.Metadata,
		},
		WireFormat: wireFormatToProto(batch.WireFormat),
	}, nil
}

func wireFormatToProto(format connector.WireFormat) ductstreampb.WireFormat {
	switch format {
	case connector.WireFormatArrow:
		return ductstreampb.WireFormat_WIRE_FORMAT_ARROW
	case connector.WireFormatParquet:
		return ductstreampb.WireFormat_WIRE_FORMAT_PARQUET
	case connector.WireFormatProto:
		return ductstreampb.WireFormat_WIRE_FORMAT_PROTO
	case connector.WireFormatAvro:
		return ductstreampb.WireFormat_WIRE_FORMAT_AVRO
	case connector.WireFormatJSON:
		return ductstreampb.WireFormat_WIRE_FORMAT_JSON
	default:
		return ductstreampb.WireFormat_WIRE_FORMAT_UNSPECIFIED
	}
}
