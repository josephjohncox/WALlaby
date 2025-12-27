package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

type pgTypeResolver struct {
	pool  *pgxpool.Pool
	mu    sync.Mutex
	cache map[uint32]string
}

func newTypeResolver(ctx context.Context, dsn string) (*pgTypeResolver, error) {
	if dsn == "" {
		return nil, errors.New("postgres dsn is required for type resolver")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &pgTypeResolver{
		pool:  pool,
		cache: make(map[uint32]string),
	}, nil
}

func (r *pgTypeResolver) ResolveType(ctx context.Context, oid uint32) (string, bool, error) {
	if oid == 0 {
		return "", false, nil
	}
	r.mu.Lock()
	if name, ok := r.cache[oid]; ok {
		r.mu.Unlock()
		return name, true, nil
	}
	r.mu.Unlock()

	var schema string
	var formatted string
	if err := r.pool.QueryRow(ctx,
		`SELECT ns.nspname, format_type(t.oid, t.typtypmod)
		 FROM pg_type t
		 JOIN pg_namespace ns ON ns.oid = t.typnamespace
		 WHERE t.oid = $1`, oid).Scan(&schema, &formatted); err != nil {
		return "", false, fmt.Errorf("resolve type %d: %w", oid, err)
	}

	name := formatTypeName(schema, formatted)
	r.mu.Lock()
	r.cache[oid] = name
	r.mu.Unlock()
	return name, true, nil
}

func (r *pgTypeResolver) Close() {
	if r.pool != nil {
		r.pool.Close()
	}
}

func formatTypeName(schema, formatted string) string {
	formatted = strings.ToLower(strings.TrimSpace(formatted))
	schema = strings.ToLower(strings.TrimSpace(schema))
	if formatted == "" {
		return formatted
	}
	if schema == "" || schema == "pg_catalog" || schema == "pg_toast" {
		return formatted
	}
	if strings.Contains(formatted, ".") {
		return formatted
	}
	return schema + "." + formatted
}
