package iceberg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	iceberggo "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

const (
	SummaryFlowID            = "wallaby.flow-id"
	SummaryLogicalBatchID    = "wallaby.logical-batch-id"
	SummaryManifestSHA256    = "wallaby.manifest-sha256"
	SummaryProjectionID      = "wallaby.projection-id"
	SummarySchemaFingerprint = "wallaby.schema-fingerprint"
	SummaryCommitID          = "wallaby.commit-id"
	SummaryPublicationID     = "wallaby.publication-id"
	SummaryProjectionGroupID = "wallaby.projection-group-id"
	// SummaryFieldMapping records the deterministic canonical-to-catalog
	// field-ID mapping fingerprint as immutable commit metadata. It is audit
	// evidence only and is not part of snapshot identity matching.
	SummaryFieldMapping = "wallaby.field-id-mapping"

	controlSchemaFingerprint = "d4a782568e80af1e1eb00428ff21a65dfa9c7ea30bb41547e2c4f681bc0eb83f"
)

var (
	ErrCatalogConflict      = errors.New("iceberg optimistic catalog conflict")
	ErrCatalogIndeterminate = errors.New("iceberg catalog commit outcome indeterminate")
	ErrTableNotFound        = errors.New("iceberg table not found")
)

type catalogSnapshot struct {
	ID        int64
	ParentID  *int64
	Timestamp time.Time
	Summary   map[string]string
}

type catalogTable struct {
	Identifier        table.Identifier
	Schema            *iceberggo.Schema
	PartitionSpec     iceberggo.PartitionSpec
	CurrentSnapshotID *int64
	Snapshots         []catalogSnapshot
	opaque            any
}

type catalogBackend interface {
	Load(context.Context, table.Identifier) (catalogTable, error)
	Create(context.Context, table.Identifier, *iceberggo.Schema) (catalogTable, error)
	// Evolve applies additive columns and supported renames through the
	// catalog, which owns the resulting field IDs. It returns the reloaded
	// table state.
	Evolve(context.Context, catalogTable, []iceberggo.NestedField, []renameOp) (catalogTable, error)
	Append(context.Context, catalogTable, *iceberggo.Schema, []arrow.RecordBatch, map[string]string) (catalogSnapshot, error)
}

// CommitterHooks exposes deterministic catalog failure boundaries.
type CommitterHooks struct {
	Reach func(context.Context, string) error
}

// CommitterOption configures optional committer behavior.
type CommitterOption func(*Committer)

func WithCommitterHooks(hooks CommitterHooks) CommitterOption {
	return func(committer *Committer) { committer.hooks = hooks }
}

// Committer is the append-only changelog deep module. It verifies exact
// canonical objects, rewrites them with Iceberg field IDs and table partition
// semantics, performs optimistic commits, and reconciles snapshot summaries.
type Committer struct {
	objects CanonicalObjectReader
	catalog catalogBackend
	config  Config
	hooks   CommitterHooks
}

func NewCommitter(objects CanonicalObjectReader, catalog catalogBackend, config Config, options ...CommitterOption) (*Committer, error) {
	if objects == nil || catalog == nil {
		return nil, errors.New("iceberg committer requires canonical objects and a catalog backend")
	}
	if config.MaxCommitRetries < 1 || config.RequestTimeout <= 0 || config.ReconciliationHorizon <= 0 {
		return nil, errors.New("iceberg committer retry, timeout, and reconciliation settings must be positive")
	}
	committer := &Committer{objects: objects, catalog: catalog, config: config}
	for _, option := range options {
		option(committer)
	}
	return committer, nil
}

func (c *Committer) reach(ctx context.Context, boundary string) error {
	if c.hooks.Reach == nil {
		return nil
	}
	return c.hooks.Reach(ctx, boundary)
}

func (c *Committer) Commit(ctx context.Context, request artifactlog.CommitRequest) (result artifactlog.CommitResult, retErr error) {
	ctx, finish := telemetry.StartIcebergConsumerSpan(ctx, "commit", request.FlowID, request.LogicalBatchID, request.CommitID)
	defer func() { finish(retErr) }()
	if err := validateRequest(request); err != nil {
		return artifactlog.CommitResult{}, err
	}
	plan, err := buildProjection(ctx, request, c.objects, c.config)
	if err != nil {
		return artifactlog.CommitResult{}, err
	}
	defer plan.release()

	snapshots := make(map[string]string, len(plan.groups))
	for _, group := range plan.groups {
		snapshotID, groupErr := c.commitGroup(ctx, request, group)
		if groupErr != nil {
			return artifactlog.CommitResult{}, groupErr
		}
		snapshots[group.id] = snapshotID
	}
	return commitResult(request, snapshots)
}

// commitGroup loads, creates, and evolves one target table, then rewrites the
// canonical data files with the catalog-assigned field IDs before appending
// them. The Iceberg catalog owns the field IDs; the authoritative mapping is
// rebuilt from the schema the catalog returns on every attempt.
func (c *Committer) commitGroup(ctx context.Context, request artifactlog.CommitRequest, group *projectionGroup) (string, error) {
	identitySummary := snapshotSummary(request, group.id, group.schemaFingerprint)
	qualified := strings.Join(group.target, ".")
	for attempt := 1; attempt <= c.config.MaxCommitRetries; attempt++ {
		state, loadErr := c.catalog.Load(ctx, group.target)
		if errors.Is(loadErr, ErrTableNotFound) {
			state, loadErr = c.catalog.Create(ctx, group.target, group.schema)
			if errors.Is(loadErr, ErrCatalogConflict) {
				continue
			}
		}
		if loadErr != nil {
			return "", loadErr
		}

		disposition, existing, reconcileErr := reconcileGroup(state, request, group.id, group.schemaFingerprint)
		if reconcileErr != nil {
			return "", reconcileErr
		}
		if disposition == artifactlog.CommitApplied {
			return strconv.FormatInt(existing.ID, 10), nil
		}
		if disposition == artifactlog.CommitIndeterminate {
			return "", fmt.Errorf("%w: projection group %s", ErrCatalogIndeterminate, group.id)
		}

		// Evolve the catalog table so it represents every canonical field.
		// Additive columns and supported renames are keyed on stable identity.
		adds, renames, planErr := evolutionPlan(state.Schema, group.schema)
		if planErr != nil {
			return "", fmt.Errorf("iceberg table %s: %w", qualified, planErr)
		}
		if len(adds) > 0 || len(renames) > 0 {
			evolved, evolveErr := c.catalog.Evolve(ctx, state, adds, renames)
			if errors.Is(evolveErr, ErrCatalogConflict) {
				continue
			}
			if evolveErr != nil {
				return "", fmt.Errorf("iceberg table %s: %w", qualified, evolveErr)
			}
			state = evolved
		}

		// Rebuild the authoritative canonical-to-catalog field-ID mapping from
		// the schema the catalog returned. This validates names, types,
		// requiredness, stable identity, and collisions.
		mapping, mapErr := buildFieldMapping(state.Schema, group.schema)
		if mapErr != nil {
			return "", fmt.Errorf("iceberg table %s: %w", qualified, mapErr)
		}
		if err := validatePartitionSpec(state); err != nil {
			return "", fmt.Errorf("iceberg table %s: %w", qualified, err)
		}

		rewritten, rewriteErr := rewriteRecordFieldIDs(group.records, mapping)
		if rewriteErr != nil {
			return "", rewriteErr
		}
		appendSummary := maps.Clone(identitySummary)
		appendSummary[SummaryFieldMapping] = mappingFingerprint(mapping)

		snapshot, appendErr := c.catalog.Append(ctx, state, state.Schema, rewritten, appendSummary)
		releaseRecordBatches(rewritten)
		if errors.Is(appendErr, ErrCatalogConflict) {
			continue
		}
		if appendErr != nil {
			return "", appendErr
		}
		if !summaryMatches(snapshot.Summary, identitySummary) {
			return "", fmt.Errorf("%w: catalog returned a snapshot with different summary identity", connector.ErrDeliveryConflict)
		}
		if err := c.reach(ctx, "after_catalog_commit:"+group.id); err != nil {
			return "", err
		}
		return strconv.FormatInt(snapshot.ID, 10), nil
	}
	return "", fmt.Errorf("%w: projection group %s exceeded %d retries", ErrCatalogConflict, group.id, c.config.MaxCommitRetries)
}

func (c *Committer) Reconcile(ctx context.Context, request artifactlog.ReconcileRequest) (result artifactlog.ReconcileResult, retErr error) {
	ctx, finish := telemetry.StartIcebergConsumerSpan(ctx, "reconcile", request.FlowID, request.LogicalBatchID, request.CommitID)
	defer func() { finish(retErr) }()
	if err := validateRequest(request); err != nil {
		return artifactlog.ReconcileResult{}, err
	}
	expected, err := expectedProjectionGroups(request, c.config)
	if err != nil {
		return artifactlog.ReconcileResult{}, err
	}
	snapshots := make(map[string]string, len(expected))
	allApplied := true
	for _, group := range expected {
		state, loadErr := c.catalog.Load(ctx, group.target)
		if errors.Is(loadErr, ErrTableNotFound) {
			allApplied = false
			continue
		}
		if loadErr != nil {
			return artifactlog.ReconcileResult{}, loadErr
		}
		disposition, snapshot, reconcileErr := reconcileGroup(state, request, group.id, group.schemaFingerprint)
		if reconcileErr != nil {
			return artifactlog.ReconcileResult{}, reconcileErr
		}
		switch disposition {
		case artifactlog.CommitApplied:
			snapshots[group.id] = strconv.FormatInt(snapshot.ID, 10)
		case artifactlog.CommitNotApplied:
			allApplied = false
		case artifactlog.CommitIndeterminate:
			return artifactlog.ReconcileResult{Disposition: artifactlog.CommitIndeterminate}, nil
		}
	}
	if !allApplied {
		return artifactlog.ReconcileResult{Disposition: artifactlog.CommitNotApplied}, nil
	}
	commitEvidence, err := commitResult(request, snapshots)
	if err != nil {
		return artifactlog.ReconcileResult{}, err
	}
	return artifactlog.ReconcileResult{Disposition: artifactlog.CommitApplied, Commit: commitEvidence}, nil
}

func validateRequest(request artifactlog.CommitRequest) error {
	for name, value := range map[string]string{
		"flow_id": request.FlowID, "consumer_revision_id": request.ConsumerRevisionID,
		"position_id": request.PositionID, "checkpoint_lsn": request.CheckpointLSN,
		"logical_batch_id": request.LogicalBatchID, "projection_id": request.ProjectionID,
		"manifest_sha256": request.ManifestSHA256, "commit_id": request.CommitID,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("iceberg commit request %s is required", name)
		}
	}
	if request.FlowIncarnationID == [16]byte{} || request.PublicationID == [16]byte{} || request.PublicationSequence <= 0 {
		return errors.New("iceberg commit request incarnation, publication, and positive sequence are required")
	}
	if request.AttemptedAt.IsZero() {
		return errors.New("iceberg commit request attempted_at is required")
	}
	if len(request.Objects) == 0 && len(request.Barriers) == 0 {
		return errors.New("iceberg commit request has no rooted objects or barriers")
	}
	expectedCommitID := artifactlog.DeterministicCommitID(request.FlowIncarnationID, request.ConsumerRevisionID, request.PublicationID, request.ManifestSHA256)
	if request.CommitID != expectedCommitID {
		return fmt.Errorf("%w: Iceberg commit ID differs", connector.ErrDeliveryConflict)
	}
	return nil
}

// validatePartitionSpec rejects a table whose partitioning references a field
// that is absent from the catalog schema. Wallaby creates unpartitioned tables;
// a pre-existing partitioned table is accepted only when every partition source
// field still resolves in the current table schema.
func validatePartitionSpec(state catalogTable) error {
	if state.Schema == nil {
		return errors.New("catalog table schema is required")
	}
	for partition := range state.PartitionSpec.Fields() {
		if _, ok := state.Schema.FindFieldByID(partition.SourceID); !ok {
			return fmt.Errorf("partition field %s references source ID %d absent from the table schema", partition.Name, partition.SourceID)
		}
	}
	return nil
}

func snapshotSummary(request artifactlog.CommitRequest, groupID, schemaFingerprint string) map[string]string {
	return map[string]string{
		SummaryFlowID: request.FlowID, SummaryLogicalBatchID: request.LogicalBatchID,
		SummaryManifestSHA256: request.ManifestSHA256, SummaryProjectionID: request.ProjectionID,
		SummarySchemaFingerprint: schemaFingerprint, SummaryCommitID: request.CommitID,
		SummaryPublicationID: request.PublicationID.String(), SummaryProjectionGroupID: groupID,
	}
}

func summaryMatches(actual, expected map[string]string) bool {
	for key, value := range expected {
		if actual[key] != value {
			return false
		}
	}
	return true
}

func reconcileGroup(state catalogTable, request artifactlog.CommitRequest, groupID, schemaFingerprint string) (artifactlog.CommitDisposition, catalogSnapshot, error) {
	expected := snapshotSummary(request, groupID, schemaFingerprint)
	for _, snapshot := range state.Snapshots {
		if summaryMatches(snapshot.Summary, expected) {
			return artifactlog.CommitApplied, snapshot, nil
		}
		if snapshot.Summary[SummaryPublicationID] == request.PublicationID.String() ||
			snapshot.Summary[SummaryCommitID] == request.CommitID ||
			(snapshot.Summary[SummaryLogicalBatchID] == request.LogicalBatchID && snapshot.Summary[SummaryProjectionGroupID] == groupID) {
			return artifactlog.CommitIndeterminate, catalogSnapshot{}, fmt.Errorf("%w: conflicting Iceberg snapshot summary for projection group %s", connector.ErrDeliveryConflict, groupID)
		}
	}
	if conclusiveSnapshotAbsence(state, request.AttemptedAt) {
		return artifactlog.CommitNotApplied, catalogSnapshot{}, nil
	}
	return artifactlog.CommitIndeterminate, catalogSnapshot{}, nil
}

func conclusiveSnapshotAbsence(state catalogTable, attemptedAt time.Time) bool {
	if state.CurrentSnapshotID == nil || len(state.Snapshots) == 0 {
		return true
	}
	byID := make(map[int64]catalogSnapshot, len(state.Snapshots))
	for _, snapshot := range state.Snapshots {
		byID[snapshot.ID] = snapshot
	}
	current, ok := byID[*state.CurrentSnapshotID]
	if !ok {
		return false
	}
	for {
		if !current.Timestamp.After(attemptedAt) {
			return true
		}
		if current.ParentID == nil {
			return false
		}
		parent, exists := byID[*current.ParentID]
		if !exists {
			return false
		}
		current = parent
	}
}

type expectedProjectionGroup struct {
	id                string
	target            table.Identifier
	schemaFingerprint string
}

func expectedProjectionGroups(request artifactlog.CommitRequest, cfg Config) ([]expectedProjectionGroup, error) {
	groups := make(map[string]expectedProjectionGroup)
	if len(request.Barriers) > 0 {
		target, err := cfg.controlTarget()
		if err != nil {
			return nil, err
		}
		id := projectionGroupID(target, controlSchemaFingerprint, true)
		groups[id] = expectedProjectionGroup{id: id, target: target, schemaFingerprint: controlSchemaFingerprint}
	}
	for _, object := range request.Objects {
		target, err := cfg.target(object.Namespace, object.Table)
		if err != nil {
			return nil, err
		}
		id := projectionGroupID(target, object.SchemaID, false)
		groups[id] = expectedProjectionGroup{id: id, target: target, schemaFingerprint: object.SchemaID}
	}
	result := make([]expectedProjectionGroup, 0, len(groups))
	for _, group := range groups {
		result = append(result, group)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].id < result[j].id })
	return result, nil
}

func commitResult(request artifactlog.CommitRequest, snapshots map[string]string) (artifactlog.CommitResult, error) {
	if len(snapshots) == 0 {
		return artifactlog.CommitResult{}, errors.New("iceberg commit produced no snapshot evidence")
	}
	encoded, err := json.Marshal(snapshots)
	if err != nil {
		return artifactlog.CommitResult{}, err
	}
	digest := sha256.Sum256(encoded)
	return artifactlog.CommitResult{
		SnapshotID: "iceberg-snapshots:" + hex.EncodeToString(digest[:]), SnapshotIDs: snapshots,
		ManifestSHA256: request.ManifestSHA256, CommitID: request.CommitID,
		LogicalBatchID: request.LogicalBatchID,
	}, nil
}
