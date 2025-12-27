package wire

import (
	"fmt"
	"strings"

	"github.com/josephjohncox/ductstream/pkg/connector"
)

// Codec encodes a batch into a wire payload.
type Codec interface {
	Name() connector.WireFormat
	ContentType() string
	Encode(batch connector.Batch) ([]byte, error)
}

// NewCodec returns a codec by name.
func NewCodec(format string) (Codec, error) {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", string(connector.WireFormatArrow):
		return &ArrowIPCCodec{}, nil
	case string(connector.WireFormatParquet):
		return &ParquetCodec{}, nil
	case string(connector.WireFormatAvro):
		return &AvroCodec{}, nil
	case string(connector.WireFormatProto):
		return &ProtoCodec{}, nil
	case string(connector.WireFormatJSON):
		return &JSONCodec{}, nil
	default:
		return nil, fmt.Errorf("unsupported wire format: %s", format)
	}
}
