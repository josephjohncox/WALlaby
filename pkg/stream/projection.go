package stream

import "github.com/josephjohncox/wallaby/pkg/connector"

// ProjectionDecision reports whether a source unit contains destination work.
type ProjectionDecision uint8

const (
	ProjectionIncluded ProjectionDecision = iota + 1
	ProjectionFiltered
)

// Projector applies one immutable destination-scoped logical projection.
type Projector interface {
	ProjectBatch(connector.Batch) (connector.Batch, ProjectionDecision, error)
	ProjectTransaction(connector.SourceTransaction) (connector.SourceTransaction, ProjectionDecision, error)
	Fingerprint() string
}
