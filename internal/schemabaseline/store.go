// Package schemabaseline stores managed PostgreSQL logical-decoding schema
// baselines under the active producer fence.
package schemabaseline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// Store uses the caller-owned control PostgreSQL pool.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore constructs the managed schema-baseline authority repository.
func NewStore(pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, errors.New("managed schema-baseline PostgreSQL pool is required")
	}
	return &Store{pool: pool}, nil
}

// Load validates the current active fence and every stored row's immutable
// incarnation, lineage, writer provenance, and payload fingerprint. Rows from
// prior lifecycle generations in the same incarnation remain the baseline for
// a resumed generation; a different incarnation cannot address them.
func (s *Store) Load(ctx context.Context, fence connector.RunFence, sourceLineageID string) ([]connector.Schema, error) {
	if err := validateRequest(fence, sourceLineageID); err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin managed schema-baseline load: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
SELECT baseline.source_namespace,baseline.source_relation,baseline.schema_json,baseline.schema_fingerprint,
       baseline.generation,baseline.lease_epoch,
       acquisition.incarnation_id,acquisition.generation,acquisition.lease_epoch
FROM ONLY public.managed_schema_baselines AS baseline
LEFT JOIN public.execution_acquisitions AS acquisition
  ON acquisition.acquisition_id=baseline.acquisition_id
WHERE baseline.flow_id=$1
  AND baseline.flow_incarnation_id=$2
  AND baseline.source_lineage_id=$3
ORDER BY baseline.source_namespace,baseline.source_relation`, fence.FlowID, fence.FlowIncarnationID, sourceLineageID)
	if err != nil {
		return nil, fmt.Errorf("load managed schema baselines: %w", err)
	}
	defer rows.Close()
	var baselines []connector.Schema
	for rows.Next() {
		var namespace, relation, fingerprint string
		var encoded []byte
		var writerGeneration, writerLeaseEpoch int64
		var acquisitionIncarnationID *string
		var acquisitionGeneration, acquisitionLeaseEpoch *int64
		if err := rows.Scan(&namespace, &relation, &encoded, &fingerprint, &writerGeneration, &writerLeaseEpoch, &acquisitionIncarnationID, &acquisitionGeneration, &acquisitionLeaseEpoch); err != nil {
			return nil, fmt.Errorf("scan managed schema baseline: %w", err)
		}
		if writerGeneration > fence.Generation || acquisitionIncarnationID == nil || acquisitionGeneration == nil || acquisitionLeaseEpoch == nil ||
			*acquisitionIncarnationID != fence.FlowIncarnationID.String() || *acquisitionGeneration != writerGeneration || *acquisitionLeaseEpoch != writerLeaseEpoch {
			return nil, fmt.Errorf("managed schema baseline %s.%s has invalid or future writer provenance", namespace, relation)
		}
		var schema connector.Schema
		if err := json.Unmarshal(encoded, &schema); err != nil {
			return nil, fmt.Errorf("decode managed schema baseline %s.%s: %w", namespace, relation, err)
		}
		canonical, err := json.Marshal(schema)
		if err != nil {
			return nil, fmt.Errorf("re-encode managed schema baseline %s.%s: %w", namespace, relation, err)
		}
		if schemaFingerprint(canonical) != fingerprint {
			return nil, fmt.Errorf("managed schema baseline %s.%s fingerprint mismatch", namespace, relation)
		}
		if schema.Namespace != namespace || schema.Name != relation {
			return nil, fmt.Errorf("managed schema baseline %s.%s identity differs from schema payload %s.%s", namespace, relation, schema.Namespace, schema.Name)
		}
		baselines = append(baselines, schema)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate managed schema baselines: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit managed schema-baseline load: %w", err)
	}
	return baselines, nil
}

// UpsertExactTx advances every relation in payload inside the caller's
// authoritative checkpoint transaction. It is the sole write seam: the active
// fence must match exactly and writer generation cannot move backward.
func UpsertExactTx(ctx context.Context, tx pgx.Tx, fence connector.RunFence, payload connector.ManagedSchemaBaselinePayload) error {
	if tx == nil {
		return errors.New("managed schema-baseline PostgreSQL transaction is required")
	}
	encodedPayload, _, err := payload.Canonical()
	if err != nil {
		return err
	}
	var canonical connector.ManagedSchemaBaselinePayload
	if err := json.Unmarshal(encodedPayload, &canonical); err != nil {
		return fmt.Errorf("decode canonical managed schema-baseline payload: %w", err)
	}
	if err := validateRequest(fence, canonical.SourceLineageID); err != nil {
		return err
	}
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	for _, schema := range canonical.Schemas {
		encoded, err := json.Marshal(schema)
		if err != nil {
			return fmt.Errorf("encode managed schema baseline %s.%s: %w", schema.Namespace, schema.Name, err)
		}
		tag, err := tx.Exec(ctx, `
INSERT INTO public.managed_schema_baselines (
  flow_id,flow_incarnation_id,source_lineage_id,source_namespace,source_relation,
  generation,acquisition_id,lease_epoch,schema_json,schema_fingerprint
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10)
ON CONFLICT (flow_id,flow_incarnation_id,source_lineage_id,source_namespace,source_relation)
DO UPDATE SET generation=EXCLUDED.generation,
 acquisition_id=EXCLUDED.acquisition_id,lease_epoch=EXCLUDED.lease_epoch,
 schema_json=EXCLUDED.schema_json,schema_fingerprint=EXCLUDED.schema_fingerprint,
 updated_at=clock_timestamp()
WHERE public.managed_schema_baselines.generation<=EXCLUDED.generation`, fence.FlowID, fence.FlowIncarnationID, canonical.SourceLineageID,
			schema.Namespace, schema.Name, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch,
			encoded, schemaFingerprint(encoded))
		if err != nil {
			return fmt.Errorf("persist managed schema baseline %s.%s: %w", schema.Namespace, schema.Name, err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("managed schema baseline %s.%s rejects stale generation %d", schema.Namespace, schema.Name, fence.Generation)
		}
	}
	return nil
}

func validateRequest(fence connector.RunFence, sourceLineageID string) error {
	if err := fence.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(sourceLineageID) == "" {
		return errors.New("managed schema-baseline source lineage is required")
	}
	return nil
}

func schemaFingerprint(encoded []byte) string {
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

var _ connector.ManagedSchemaBaselineStore = (*Store)(nil)
