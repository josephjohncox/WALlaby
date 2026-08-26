// Package partauthority contains the repository-internal capability that binds
// managed ClickHouse writes to a PostgreSQL reservation. The Go internal import
// boundary prevents external connector implementations from constructing or
// binding this authority.
package partauthority

import (
	"context"
	"errors"
	"strings"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// Grant is a nominal, repository-internal managed-part write capability.
type Grant struct {
	reservationID string
	guard         func(context.Context, connector.ManagedPartIdentity, func(context.Context) error) error
}

// NewGrant is callable only by packages inside this repository's internal
// import boundary. Production grants are issued by the delivery coordinator.
func NewGrant(reservationID string, guard func(context.Context, connector.ManagedPartIdentity, func(context.Context) error) error) (*Grant, error) {
	if strings.TrimSpace(reservationID) == "" || guard == nil {
		return nil, errors.New("managed part authority is incomplete")
	}
	return &Grant{reservationID: reservationID, guard: guard}, nil
}

func (g *Grant) ReservationID() string {
	if g == nil {
		return ""
	}
	return g.reservationID
}

func (g *Grant) GuardPartWrite(ctx context.Context, part connector.ManagedPartIdentity, write func(context.Context) error) error {
	if g == nil || g.guard == nil {
		return errors.New("managed part authority is not initialized")
	}
	return g.guard(ctx, part, write)
}

// Prepared is the internal extension implemented by managed write plans that
// require PostgreSQL part authority before Apply may perform external I/O.
type Prepared interface {
	connector.PreparedManagedTransaction
	PartReservationRequest() (connector.ManagedPartReservationRequest, error)
	ObservePartReservation(context.Context, bool) (connector.ManagedPartReservationObservation, error)
	BindPartReservation(*Grant) error
}
