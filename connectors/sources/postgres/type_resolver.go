package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/replication"
)

type pgTypeResolver struct {
	pool  *pgxpool.Pool
	mu    sync.Mutex
	cache map[uint32]string
	info  map[uint32]replication.TypeInfo
}

func newTypeResolver(ctx context.Context, dsn string, options map[string]string) (*pgTypeResolver, error) {
	if dsn == "" {
		return nil, errors.New("postgres dsn is required for type resolver")
	}
	pool, err := newPool(ctx, dsn, options)
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
		info:  make(map[uint32]replication.TypeInfo),
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

func (r *pgTypeResolver) ResolveTypeInfo(ctx context.Context, oid uint32) (replication.TypeInfo, bool, error) {
	if oid == 0 {
		return replication.TypeInfo{}, false, nil
	}
	r.mu.Lock()
	if info, ok := r.info[oid]; ok {
		r.mu.Unlock()
		return info, true, nil
	}
	r.mu.Unlock()

	var schema string
	var formatted string
	var extension *string
	if err := r.pool.QueryRow(ctx,
		`SELECT ns.nspname,
		        format_type(t.oid, t.typtypmod),
		        ext.extname
		 FROM pg_type t
		 JOIN pg_namespace ns ON ns.oid = t.typnamespace
		 LEFT JOIN pg_depend dep ON dep.classid = 'pg_type'::regclass
		   AND dep.objid = t.oid AND dep.deptype = 'e'
		 LEFT JOIN pg_extension ext ON ext.oid = dep.refobjid
		 WHERE t.oid = $1`, oid).Scan(&schema, &formatted, &extension); err != nil {
		return replication.TypeInfo{}, false, fmt.Errorf("resolve type info %d: %w", oid, err)
	}

	info := replication.TypeInfo{
		OID:    oid,
		Name:   formatTypeName(schema, formatted),
		Schema: strings.ToLower(strings.TrimSpace(schema)),
	}
	if extension != nil {
		info.Extension = strings.ToLower(strings.TrimSpace(*extension))
	}

	r.mu.Lock()
	r.info[oid] = info
	r.cache[oid] = info.Name
	r.mu.Unlock()
	return info, true, nil
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
