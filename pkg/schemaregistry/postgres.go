package schemaregistry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
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
	if err := verifyPreparedSchema(ctx, pool); err != nil {
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

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RegisterResult{}, fmt.Errorf("begin schema registration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Serialize version allocation per subject while allowing unrelated subjects
	// to register concurrently.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, req.Subject); err != nil {
		return RegisterResult{}, fmt.Errorf("lock schema registry subject: %w", err)
	}

	var existingID string
	var existingVersion int
	err = tx.QueryRow(ctx, `SELECT id::text, version
		FROM wallaby_schema_registry
		WHERE subject=$1 AND schema_hash=$2 AND references_hash=$3`,
		req.Subject, schemaHash, refsHash).Scan(&existingID, &existingVersion)
	switch {
	case err == nil:
		if err := tx.Commit(ctx); err != nil {
			return RegisterResult{}, fmt.Errorf("commit existing schema registration: %w", err)
		}
		return RegisterResult{ID: existingID, Version: existingVersion}, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return RegisterResult{}, fmt.Errorf("lookup existing schema: %w", err)
	}

	var nextVersion int
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) + 1 FROM wallaby_schema_registry WHERE subject=$1`, req.Subject).Scan(&nextVersion); err != nil {
		return RegisterResult{}, fmt.Errorf("fetch next version: %w", err)
	}

	var id int64
	if err := tx.QueryRow(ctx, `INSERT INTO wallaby_schema_registry
		(subject, schema_type, schema, schema_hash, schema_references, references_hash, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
		req.Subject, string(req.SchemaType), req.Schema, schemaHash, string(refsJSON), refsHash, nextVersion).Scan(&id); err != nil {
		return RegisterResult{}, fmt.Errorf("insert schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return RegisterResult{}, fmt.Errorf("commit schema registration: %w", err)
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
		if clone[i].Subject != clone[j].Subject {
			return clone[i].Subject < clone[j].Subject
		}
		if clone[i].Name != clone[j].Name {
			return clone[i].Name < clone[j].Name
		}
		return clone[i].Version < clone[j].Version
	})
	return clone
}
