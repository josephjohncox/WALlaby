package wire

import (
	"encoding/json"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// JSONCodec encodes batches as JSON.
type JSONCodec struct{}

func (c *JSONCodec) Name() connector.WireFormat {
	return connector.WireFormatJSON
}

func (c *JSONCodec) ContentType() string {
	return "application/json"
}

func (c *JSONCodec) Encode(batch connector.Batch) ([]byte, error) {
	return json.Marshal(batch)
}
