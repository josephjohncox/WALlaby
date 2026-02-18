package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ReplicationSlotInfo contains key fields for inspecting logical replication slots.
type ReplicationSlotInfo struct {
	SlotName     string
	SlotType     string
	Plugin       string
	Database     string
	Active       bool
	ActivePID    *int32
	WalStatus    string
	RestartLSN   string
	ConfirmedLSN string
	Temporary    bool
}

// ListReplicationSlots returns logical replication slot metadata from the source DB.
func ListReplicationSlots(ctx context.Context, dsn string, options map[string]string) ([]ReplicationSlotInfo, error) {
	if dsn == "" {
		return nil, errors.New("postgres dsn is required")
	}

	pool, err := newPool(ctx, dsn, options)
	if err != nil {
		return nil, err
	}
	defer pool.Close()

	const query = `
SELECT
  slot_name,
  plugin,
  slot_type,
  database,
  active,
  active_pid,
  wal_status,
  restart_lsn::text,
  confirmed_flush_lsn::text,
  temporary
FROM pg_replication_slots
WHERE slot_type = 'logical'
ORDER BY slot_name`

	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query replication slots: %w", err)
	}
	defer rows.Close()

	out := make([]ReplicationSlotInfo, 0)
	for rows.Next() {
		var item ReplicationSlotInfo
		var activePID sql.NullInt32
		var restartLSN, confirmedLSN sql.NullString
		if err := rows.Scan(
			&item.SlotName,
			&item.Plugin,
			&item.SlotType,
			&item.Database,
			&item.Active,
			&activePID,
			&item.WalStatus,
			&restartLSN,
			&confirmedLSN,
			&item.Temporary,
		); err != nil {
			return nil, fmt.Errorf("scan replication slot: %w", err)
		}
		if activePID.Valid {
			pid := activePID.Int32
			item.ActivePID = &pid
		}
		if restartLSN.Valid {
			item.RestartLSN = restartLSN.String
		}
		if confirmedLSN.Valid {
			item.ConfirmedLSN = confirmedLSN.String
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate replication slots: %w", err)
	}

	return out, nil
}

// GetReplicationSlot returns metadata for one logical slot.
func GetReplicationSlot(ctx context.Context, dsn, slot string, options map[string]string) (ReplicationSlotInfo, bool, error) {
	if dsn == "" {
		return ReplicationSlotInfo{}, false, errors.New("postgres dsn is required")
	}
	if slot == "" {
		return ReplicationSlotInfo{}, false, errors.New("slot name is required")
	}

	pool, err := newPool(ctx, dsn, options)
	if err != nil {
		return ReplicationSlotInfo{}, false, err
	}
	defer pool.Close()

	const query = `
SELECT
  slot_name,
  plugin,
  slot_type,
  database,
  active,
  active_pid,
  wal_status,
  restart_lsn::text,
  confirmed_flush_lsn::text,
  temporary
FROM pg_replication_slots
WHERE slot_type = 'logical' AND slot_name = $1`

	var item ReplicationSlotInfo
	var activePID sql.NullInt32
	var restartLSN, confirmedLSN sql.NullString
	if err := pool.QueryRow(ctx, query, slot).Scan(
		&item.SlotName,
		&item.Plugin,
		&item.SlotType,
		&item.Database,
		&item.Active,
		&activePID,
		&item.WalStatus,
		&restartLSN,
		&confirmedLSN,
		&item.Temporary,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ReplicationSlotInfo{}, false, nil
		}
		return ReplicationSlotInfo{}, false, fmt.Errorf("query replication slot: %w", err)
	}
	if activePID.Valid {
		pid := activePID.Int32
		item.ActivePID = &pid
	}
	if restartLSN.Valid {
		item.RestartLSN = restartLSN.String
	}
	if confirmedLSN.Valid {
		item.ConfirmedLSN = confirmedLSN.String
	}

	return item, true, nil
}
