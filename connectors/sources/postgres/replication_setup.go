package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ensureReplication(ctx context.Context, dsn, publication string, tables []string, ensurePublication, validateSettings bool) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	if validateSettings {
		if err := validateLogicalReplication(ctx, pool); err != nil {
			return err
		}
	}

	if ensurePublication {
		if publication == "" {
			return errors.New("publication is required")
		}
		exists, err := publicationExists(ctx, pool, publication)
		if err != nil {
			return err
		}
		if !exists {
			if err := createPublication(ctx, pool, publication, tables); err != nil {
				return err
			}
		}
	}

	return nil
}

func validateLogicalReplication(ctx context.Context, pool *pgxpool.Pool) error {
	var walLevel string
	if err := pool.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		return fmt.Errorf("read wal_level: %w", err)
	}
	if strings.ToLower(walLevel) != "logical" {
		return fmt.Errorf("wal_level must be logical (current: %s)", walLevel)
	}

	var maxSlots int
	if err := pool.QueryRow(ctx, "SHOW max_replication_slots").Scan(&maxSlots); err != nil {
		return fmt.Errorf("read max_replication_slots: %w", err)
	}
	if maxSlots < 1 {
		return errors.New("max_replication_slots must be >= 1")
	}

	var maxSenders int
	if err := pool.QueryRow(ctx, "SHOW max_wal_senders").Scan(&maxSenders); err != nil {
		return fmt.Errorf("read max_wal_senders: %w", err)
	}
	if maxSenders < 1 {
		return errors.New("max_wal_senders must be >= 1")
	}

	return nil
}

func publicationExists(ctx context.Context, pool *pgxpool.Pool, name string) (bool, error) {
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", name).Scan(&exists); err != nil {
		return false, fmt.Errorf("check publication: %w", err)
	}
	return exists, nil
}

func createPublication(ctx context.Context, pool *pgxpool.Pool, name string, tables []string) error {
	if len(tables) == 0 {
		_, err := pool.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", pgx.Identifier{name}.Sanitize()))
		if err != nil {
			return fmt.Errorf("create publication: %w", err)
		}
		return nil
	}

	qualified := make([]string, 0, len(tables))
	for _, table := range tables {
		qualifiedTable, err := qualifyTable(table)
		if err != nil {
			return err
		}
		qualified = append(qualified, qualifiedTable)
	}

	query := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pgx.Identifier{name}.Sanitize(), strings.Join(qualified, ", "))
	if _, err := pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("create publication: %w", err)
	}
	return nil
}

func qualifyTable(name string) (string, error) {
	parts := strings.Split(name, ".")
	switch len(parts) {
	case 1:
		return pgx.Identifier{"public", parts[0]}.Sanitize(), nil
	case 2:
		return pgx.Identifier{parts[0], parts[1]}.Sanitize(), nil
	default:
		return "", fmt.Errorf("invalid table name %q", name)
	}
}
