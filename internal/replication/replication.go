package replication

import (
	"context"

	"github.com/jackc/pglogrepl"
	"github.com/josephjohncox/ductstream/pkg/connector"
)

// LSN mirrors Postgres log sequence numbers.
type LSN = pglogrepl.LSN

// Stream exposes logical replication changes.
type Stream interface {
	Start(ctx context.Context, slot, publication string) (<-chan Change, error)
	Stop(ctx context.Context) error
}

// Change represents a decoded logical replication event.
type Change struct {
	LSN       LSN
	Schema    string
	Table     string
	Operation string
	Payload   []byte
	DDL       string
	Record    *connector.Record
	SchemaDef *connector.Schema
}
