package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// DropReplicationSlot removes a logical replication slot if it exists.
func DropReplicationSlot(ctx context.Context, dsn, slot string, options map[string]string) error {
	if slot == "" {
		return nil
	}
	if dsn == "" {
		return errors.New("postgres dsn is required")
	}
	pool, err := newPool(ctx, dsn, options)
	if err != nil {
		return err
	}
	defer pool.Close()

	_, err = pool.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slot)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42704" {
			return nil
		}
		return err
	}
	return nil
}

// DropPublication removes a publication if it exists.
func DropPublication(ctx context.Context, dsn, publication string, options map[string]string) error {
	if publication == "" {
		return nil
	}
	if dsn == "" {
		return errors.New("postgres dsn is required")
	}
	pool, err := newPool(ctx, dsn, options)
	if err != nil {
		return err
	}
	defer pool.Close()

	ident := pgx.Identifier{publication}.Sanitize()
	if _, err := pool.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", ident)); err != nil {
		return fmt.Errorf("drop publication: %w", err)
	}
	return nil
}

// DeleteSourceState removes the source state row for the given flow/slot.
func DeleteSourceState(ctx context.Context, spec connector.Spec, slot string) error {
	if !parseBool(spec.Options[optEnsureState], true) {
		return nil
	}
	dsn := spec.Options[optDSN]
	if dsn == "" {
		return errors.New("postgres dsn is required")
	}
	schema := spec.Options[optStateSchema]
	if schema == "" {
		schema = "wallaby"
	}
	table := spec.Options[optStateTable]
	if table == "" {
		table = "source_state"
	}
	store, err := newSourceStateStore(ctx, dsn, schema, table, spec.Options)
	if err != nil {
		return err
	}
	defer store.Close()

	stateID := sourceStateID(spec, slot)
	return store.Delete(ctx, stateID)
}
