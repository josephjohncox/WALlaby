package wire

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// ArrowIPCCodec encodes batches using Arrow IPC.
type ArrowIPCCodec struct{}

func (c *ArrowIPCCodec) Name() connector.WireFormat {
	return connector.WireFormatArrow
}

func (c *ArrowIPCCodec) ContentType() string {
	return "application/vnd.apache.arrow.stream"
}

func (c *ArrowIPCCodec) Encode(batch connector.Batch) ([]byte, error) {
	if len(batch.Records) == 0 {
		return nil, nil
	}

	record, err := buildArrowRecord(batch)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, nil
	}
	defer record.Release()

	buf := bytes.NewBuffer(nil)
	writer := ipc.NewWriter(buf, ipc.WithSchema(record.Schema()))
	if err := writer.Write(record); err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("write arrow ipc: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close arrow ipc: %w", err)
	}

	return buf.Bytes(), nil
}
