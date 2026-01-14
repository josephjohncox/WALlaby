package schemaregistry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

type postgresRegistry struct {
	pool *pgxpool.Pool
}

func newPostgresRegistry(ctx context.Context, dsn string) (*postgresRegistry, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres registry: %w", err)
	}
	if err := runMigrations(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return &postgresRegistry{pool: pool}, nil
}

func (r *postgresRegistry) Register(ctx context.Context, req RegisterRequest) (RegisterResult, error) {
	if req.Subject == "" {
		return RegisterResult{}, fmt.Errorf("schema registry subject is required")
	}
	if req.Schema == "" {
		return RegisterResult{}, fmt.Errorf("schema registry schema is required")
	}
	refs := normalizeReferences(req.References)
	refsJSON, err := json.Marshal(refs)
	if err != nil {
		return RegisterResult{}, fmt.Errorf("marshal references: %w", err)
	}
	schemaHash := hashString(req.Schema)
	refsHash := hashString(string(refsJSON))

	var existingID string
	var existingVersion int
	err = r.pool.QueryRow(ctx, `SELECT id::text, version
		FROM wallaby_schema_registry
		WHERE subject=$1 AND schema_hash=$2 AND references_hash=$3`,
		req.Subject, schemaHash, refsHash).Scan(&existingID, &existingVersion)
	if err == nil {
		return RegisterResult{ID: existingID, Version: existingVersion}, nil
	}

	var nextVersion int
	if err := r.pool.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) + 1 FROM wallaby_schema_registry WHERE subject=$1`, req.Subject).Scan(&nextVersion); err != nil {
		return RegisterResult{}, fmt.Errorf("fetch next version: %w", err)
	}

	var id int64
	if err := r.pool.QueryRow(ctx, `INSERT INTO wallaby_schema_registry
		(subject, schema_type, schema, schema_hash, references, references_hash, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
		req.Subject, string(req.SchemaType), req.Schema, schemaHash, string(refsJSON), refsHash, nextVersion).Scan(&id); err != nil {
		return RegisterResult{}, fmt.Errorf("insert schema: %w", err)
	}

	return RegisterResult{ID: fmt.Sprintf("%d", id), Version: nextVersion}, nil
}

func (r *postgresRegistry) Close() error {
	if r.pool != nil {
		r.pool.Close()
	}
	return nil
}

func hashString(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func normalizeReferences(refs []Reference) []Reference {
	if len(refs) == 0 {
		return nil
	}
	clone := append([]Reference(nil), refs...)
	sort.Slice(clone, func(i, j int) bool {
		if clone[i].Subject == clone[j].Subject {
			return clone[i].Name < clone[j].Name
		}
		return clone[i].Subject < clone[j].Subject
	})
	return clone
}
