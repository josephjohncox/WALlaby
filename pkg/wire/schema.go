package wire

import (
	"fmt"

	"github.com/josephjohncox/wallaby/pkg/connector"
	protoembed "github.com/josephjohncox/wallaby/proto"
)

// ProtoSchema holds the protobuf schema text plus dependency files.
type ProtoSchema struct {
	Schema       string
	Dependencies map[string]string
}

// AvroSchema returns the Avro schema used for the provided table schema.
func AvroSchema(schema connector.Schema) string {
	return avroSchemaFor(schema)
}

// ProtoBatchSchema returns the protobuf schema for wallaby.v1.Batch and its dependencies.
func ProtoBatchSchema() (ProtoSchema, error) {
	mainSchema, err := readProto("wallaby/v1/data.proto")
	if err != nil {
		return ProtoSchema{}, err
	}
	typesSchema, err := readProto("wallaby/v1/types.proto")
	if err != nil {
		return ProtoSchema{}, err
	}
	return ProtoSchema{
		Schema: mainSchema,
		Dependencies: map[string]string{
			"wallaby/v1/types.proto": typesSchema,
		},
	}, nil
}

func readProto(path string) (string, error) {
	data, err := protoembed.Files.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read proto %s: %w", path, err)
	}
	return string(data), nil
}
