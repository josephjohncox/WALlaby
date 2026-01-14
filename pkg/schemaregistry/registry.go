package schemaregistry

import "context"

// SchemaType represents the registry schema type.
type SchemaType string

const (
	SchemaTypeAvro     SchemaType = "AVRO"
	SchemaTypeProtobuf SchemaType = "PROTOBUF"
)

// Reference describes a dependent schema reference for Protobuf/Avro.
type Reference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// RegisterRequest describes a schema registration.
type RegisterRequest struct {
	Subject    string
	Schema     string
	SchemaType SchemaType
	References []Reference
}

// RegisterResult captures registry identifiers.
type RegisterResult struct {
	ID      string
	Version int
}

// Registry provides schema registration.
type Registry interface {
	Register(ctx context.Context, req RegisterRequest) (RegisterResult, error)
	Close() error
}
