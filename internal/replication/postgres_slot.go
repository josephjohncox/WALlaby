package replication

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	replicationSlotNamePattern = regexp.MustCompile(`^[a-z0-9_]+$`)
	errReplicationSlotNotFound = errors.New("replication slot not found")
	// ErrReplicationSlotActive reports a transient handoff race while PostgreSQL
	// is still releasing the prior replication connection.
	ErrReplicationSlotActive = errors.New("replication slot is already active")
)

type replicationSlotState struct {
	SlotType          string
	Plugin            string
	Database          string
	Active            bool
	RestartLSN        pglogrepl.LSN
	ConfirmedFlushLSN pglogrepl.LSN
	WALStatus         string
}

func loadReplicationSlotState(ctx context.Context, conn *pgconn.PgConn, slot string) (*replicationSlotState, error) {
	if !replicationSlotNamePattern.MatchString(slot) {
		return nil, fmt.Errorf("invalid replication slot name %q", slot)
	}
	// Replication connections reject the extended query protocol, so use the
	// connection-aware PostgreSQL string sanitizer before issuing a simple query.
	escapedSlot, err := conn.EscapeString(slot)
	if err != nil {
		return nil, fmt.Errorf("escape replication slot name %q: %w", slot, err)
	}
	query := fmt.Sprintf(`
SELECT slot_type,
       COALESCE(plugin, ''),
       COALESCE(database, ''),
       active::text,
       COALESCE(restart_lsn::text, ''),
       COALESCE(confirmed_flush_lsn::text, ''),
       COALESCE(wal_status, '')
FROM pg_catalog.pg_replication_slots
WHERE slot_name = '%s'`, escapedSlot)
	results, err := conn.Exec(ctx, query).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("inspect replication slot %q: %w", slot, err)
	}
	if len(results) != 1 {
		return nil, fmt.Errorf("inspect replication slot %q: unexpected result count %d", slot, len(results))
	}
	result := results[0]
	if result.Err != nil {
		return nil, fmt.Errorf("inspect replication slot %q: %w", slot, result.Err)
	}
	if len(result.Rows) == 0 {
		return nil, errReplicationSlotNotFound
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 7 {
		return nil, fmt.Errorf("inspect replication slot %q: unexpected result shape", slot)
	}

	values := result.Rows[0]
	active, err := strconv.ParseBool(string(values[3]))
	if err != nil {
		return nil, fmt.Errorf("inspect replication slot %q active flag: %w", slot, err)
	}
	restartLSN, err := parseOptionalLSN(values[4])
	if err != nil {
		return nil, fmt.Errorf("inspect replication slot %q restart_lsn: %w", slot, err)
	}
	confirmedFlushLSN, err := parseOptionalLSN(values[5])
	if err != nil {
		return nil, fmt.Errorf("inspect replication slot %q confirmed_flush_lsn: %w", slot, err)
	}

	return &replicationSlotState{
		SlotType:          string(values[0]),
		Plugin:            string(values[1]),
		Database:          string(values[2]),
		Active:            active,
		RestartLSN:        restartLSN,
		ConfirmedFlushLSN: confirmedFlushLSN,
		WALStatus:         string(values[6]),
	}, nil
}

func parseOptionalLSN(raw []byte) (pglogrepl.LSN, error) {
	value := strings.TrimSpace(string(raw))
	if value == "" {
		return 0, nil
	}
	lsn, err := pglogrepl.ParseLSN(value)
	if err != nil {
		return 0, fmt.Errorf("parse LSN %q: %w", value, err)
	}
	return lsn, nil
}

func validateExistingSlotAuthorization(required bool, authorized pglogrepl.LSN) error {
	if required && authorized == 0 {
		return errors.New("managed existing slot requires an authoritative durable checkpoint")
	}
	return nil
}

func resolveNewSlotStart(requireAuthorized bool, requested, consistent pglogrepl.LSN) (pglogrepl.LSN, error) {
	if requested == 0 {
		return consistent, nil
	}
	if requireAuthorized && requested != consistent {
		return 0, fmt.Errorf(
			"authorized start_lsn %s does not match new slot consistent point %s",
			requested,
			consistent,
		)
	}
	return requested, nil
}

func resolveSlotStart(state replicationSlotState, expectedPlugin, expectedDatabase string, authorized, serverEnd pglogrepl.LSN) (pglogrepl.LSN, error) {
	if state.SlotType != "logical" {
		return 0, fmt.Errorf("replication slot is %q, want logical", state.SlotType)
	}
	if state.Plugin != expectedPlugin {
		return 0, fmt.Errorf("replication slot plugin is %q, want %q", state.Plugin, expectedPlugin)
	}
	if state.Database != expectedDatabase {
		return 0, fmt.Errorf("replication slot database is %q, want %q", state.Database, expectedDatabase)
	}
	if state.Active {
		return 0, ErrReplicationSlotActive
	}
	if state.WALStatus == "lost" {
		return 0, errors.New("replication slot has lost required WAL")
	}

	if authorized != 0 {
		if state.ConfirmedFlushLSN != 0 && state.ConfirmedFlushLSN > authorized {
			return 0, fmt.Errorf(
				"replication slot confirmed_flush_lsn %s exceeds authorized durable checkpoint %s",
				state.ConfirmedFlushLSN,
				authorized,
			)
		}
		if state.RestartLSN != 0 && authorized < state.RestartLSN {
			return 0, fmt.Errorf(
				"authorized durable checkpoint %s precedes retained restart_lsn %s",
				authorized,
				state.RestartLSN,
			)
		}
		if serverEnd != 0 && authorized > serverEnd {
			return 0, fmt.Errorf("authorized durable checkpoint %s exceeds server WAL end %s", authorized, serverEnd)
		}
		return authorized, nil
	}

	start := state.ConfirmedFlushLSN
	if start == 0 {
		start = state.RestartLSN
	}
	if start == 0 {
		return 0, errors.New("replication slot has no restart or confirmed flush position")
	}
	if serverEnd != 0 && start > serverEnd {
		return 0, fmt.Errorf("replication slot start %s exceeds server WAL end %s", start, serverEnd)
	}
	return start, nil
}
