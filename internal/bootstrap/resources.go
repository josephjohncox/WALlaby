package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/josephjohncox/wallaby/internal/authority"
	postgrescodec "github.com/josephjohncox/wallaby/internal/postgres"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// PublicationRelation is one exact relation in an owned or adopted
// publication manifest.
type PublicationRelation struct {
	OID          uint32
	Namespace    string
	Table        string
	RelationKind string
	IsPartition  bool
}

// SourceResource identifies one PostgreSQL resource and its ownership.
type SourceResource struct {
	ID       uuid.UUID
	Name     string
	Revision string
	Owned    bool
}

var ownedSlotNamePattern = regexp.MustCompile(`^wallaby_[1-9][0-9]*_[0-9a-f]{16}$`)

func ownedSlotRevision(sourceSystem, databaseName, slotName string) string {
	digest := sha256.Sum256([]byte(sourceSystem + "\x00" + databaseName + "\x00" + slotName + "\x00pgoutput"))
	return hex.EncodeToString(digest[:])
}

type preparedResource struct {
	resourceID  uuid.UUID
	operationID uuid.UUID
	operation   string
	owned       bool
}

func (b *Bootstrapper) reconcilePreparedSlotCreates(ctx context.Context, fence authority.RunFence, sourceSystem, databaseName string) error {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	type orphan struct {
		operationID uuid.UUID
		name        string
		system      string
		database    string
	}
	rows, err := tx.Query(ctx, `
SELECT operation_id,physical_name,source_system_id,database_name
FROM source_resource_operations operation_row
WHERE flow_incarnation_id=$1 AND resource_kind='slot' AND operation='create'
  AND status IN ('prepared','indeterminate')
  AND NOT EXISTS (
    SELECT 1 FROM source_resources resource_row
    WHERE resource_row.flow_incarnation_id=operation_row.flow_incarnation_id
      AND resource_row.resource_kind='slot'
      AND resource_row.resource_id=operation_row.resource_id
  )
ORDER BY prepared_at,operation_id`, fence.FlowIncarnationID)
	if err != nil {
		return fmt.Errorf("load prepared slot creates: %w", err)
	}
	var orphans []orphan
	for rows.Next() {
		var item orphan
		if err := rows.Scan(&item.operationID, &item.name, &item.system, &item.database); err != nil {
			rows.Close()
			return err
		}
		orphans = append(orphans, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	for _, item := range orphans {
		if item.system != sourceSystem || item.database != databaseName || !ownedSlotNamePattern.MatchString(item.name) {
			return fmt.Errorf("%w: prepared slot %q source identity does not match the connected source", connector.ErrDeliveryConflict, item.name)
		}
		var slotDatabase, plugin, slotType string
		var active bool
		err := b.source.QueryRow(ctx, `
SELECT database,plugin,slot_type,active
FROM pg_catalog.pg_replication_slots
WHERE slot_name=$1`, item.name).Scan(&slotDatabase, &plugin, &slotType, &active)
		if errors.Is(err, pgx.ErrNoRows) {
			if err := b.finishReconciledSlotCreate(ctx, fence, item.operationID, true); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect prepared slot %s: %w", item.name, err)
		}
		if slotDatabase != databaseName || plugin != "pgoutput" || slotType != "logical" || active {
			return fmt.Errorf("%w: prepared slot %s database=%s plugin=%s type=%s active=%t", connector.ErrDeliveryConflict, item.name, slotDatabase, plugin, slotType, active)
		}
		if _, err := b.source.Exec(ctx, `SELECT pg_catalog.pg_drop_replication_slot($1)`, item.name); err != nil {
			return fmt.Errorf("drop inactive prepared slot %s: %w", item.name, err)
		}
		if err := b.finishReconciledSlotCreate(ctx, fence, item.operationID, true); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bootstrapper) finishReconciledSlotCreate(ctx context.Context, fence authority.RunFence, operationID uuid.UUID, slotAbsent bool) error {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
UPDATE source_resource_operations
SET status='rejected',external_evidence=jsonb_build_object('slot_absent',$3::boolean),completed_at=clock_timestamp()
WHERE operation_id=$1 AND flow_incarnation_id=$2 AND resource_kind='slot'
  AND operation='create' AND status IN ('prepared','indeterminate')`, operationID, fence.FlowIncarnationID, slotAbsent)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: prepared slot operation changed during reconciliation", authority.ErrFenceRejected)
	}
	return tx.Commit(ctx)
}

func (b *Bootstrapper) prepareOwnedSlot(ctx context.Context, fence authority.RunFence, bootstrapID uuid.UUID, sourceSystem, databaseName, slotName string) (preparedResource, string, error) {
	if bootstrapID == uuid.Nil || strings.TrimSpace(sourceSystem) == "" || strings.TrimSpace(databaseName) == "" || strings.TrimSpace(slotName) == "" {
		return preparedResource{}, "", errors.New("complete owned slot identity is required")
	}
	revision := ownedSlotRevision(sourceSystem, databaseName, slotName)
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return preparedResource{}, "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return preparedResource{}, "", err
	}
	var existingName, state string
	err = tx.QueryRow(ctx, `
SELECT physical_name,state FROM source_resources
WHERE flow_incarnation_id=$1 AND resource_kind='slot'
  AND state IN ('prepared','ready','cleanup_pending')
FOR UPDATE`, fence.FlowIncarnationID).Scan(&existingName, &state)
	switch {
	case err == nil:
		return preparedResource{}, "", fmt.Errorf("%w: active slot resource %s is still %s", connector.ErrDeliveryConflict, existingName, state)
	case !errors.Is(err, pgx.ErrNoRows):
		return preparedResource{}, "", err
	}
	prepared := preparedResource{resourceID: uuid.New(), operationID: uuid.New(), operation: "create", owned: true}
	if _, err := tx.Exec(ctx, `
INSERT INTO source_resource_operations (
  operation_id,flow_incarnation_id,resource_kind,resource_id,operation,desired_revision,
  generation,acquisition_id,lease_epoch,status,bootstrap_id,source_system_id,database_name,physical_name
) VALUES($1,$2,'slot',$3,'create',$4,$5,$6,$7,'prepared',$8,$9,$10,$11)`, prepared.operationID, fence.FlowIncarnationID, prepared.resourceID, revision, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, bootstrapID, sourceSystem, databaseName, slotName); err != nil {
		return preparedResource{}, "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return preparedResource{}, "", err
	}
	return prepared, revision, nil
}

func (b *Bootstrapper) rejectPreparedSlot(ctx context.Context, fence authority.RunFence, prepared preparedResource, status string) error {
	if prepared.operationID == uuid.Nil {
		return nil
	}
	return b.finishResourceOperation(ctx, fence, prepared.operationID, status)
}

// cleanupUnpersistedSlot resolves the create-before-persistence window. A
// rejected create is recorded only after pg_catalog confirms that the physical
// slot is absent; every drop or inspection ambiguity remains indeterminate for
// the next fenced owner to reconcile.
func (b *Bootstrapper) cleanupUnpersistedSlot(ctx context.Context, fence authority.RunFence, prepared preparedResource, slotName string) error {
	if prepared.operationID == uuid.Nil {
		return nil
	}
	status := "indeterminate"
	if strings.TrimSpace(slotName) != "" {
		var dropErr error
		if b.hooks.DropSlot != nil {
			dropErr = b.hooks.DropSlot(ctx, slotName)
		} else {
			_, dropErr = b.source.Exec(ctx, `SELECT pg_catalog.pg_drop_replication_slot($1)`, slotName)
		}
		var exists bool
		inspectErr := b.source.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name=$1)`, slotName).Scan(&exists)
		if inspectErr == nil && !exists {
			status = "rejected"
		} else if dropErr != nil {
			// Keep the operation reconcilable. The original Start error remains
			// the caller-visible error; this cleanup path records authority only.
			status = "indeterminate"
		}
	} else {
		// Slot creation was never attempted because preparation did not finish.
		status = "rejected"
	}
	return b.rejectPreparedSlot(ctx, fence, prepared, status)
}

// EnsurePublication journals an exact create/adopt operation before source DDL,
// reconciles pg_catalog, and publishes the resource row only under the current
// RunFence. Existing user resources are adopted but never altered or dropped.
func (b *Bootstrapper) EnsurePublication(ctx context.Context, fence authority.RunFence, snapshot ExportedSnapshot, name, revision string, relations []PublicationRelation, allowCreate bool) (SourceResource, error) {
	name = strings.TrimSpace(name)
	revision = strings.TrimSpace(revision)
	if name == "" || revision == "" || len(relations) == 0 {
		return SourceResource{}, errors.New("publication name, revision, and relations are required")
	}
	exists, err := b.publicationExists(ctx, name)
	if err != nil {
		return SourceResource{}, err
	}
	if !exists && !allowCreate {
		return SourceResource{}, fmt.Errorf("managed publication %q does not exist and creation is disabled", name)
	}
	prepared, err := b.prepareResource(ctx, fence, snapshot, "publication", name, revision, exists)
	if err != nil {
		return SourceResource{}, err
	}
	if !exists {
		if err := b.createPublication(ctx, name, relations); err != nil {
			_ = b.finishResourceOperation(context.WithoutCancel(ctx), fence, prepared.operationID, "indeterminate")
			return SourceResource{}, fmt.Errorf("create managed publication %s: %w", name, err)
		}
		if b.hooks.AfterPublicationCreated != nil {
			if err := b.hooks.AfterPublicationCreated(ctx, name); err != nil {
				_ = b.finishResourceOperation(context.WithoutCancel(ctx), fence, prepared.operationID, "indeterminate")
				return SourceResource{}, err
			}
		}
	}
	actual, err := b.publicationRelations(ctx, name)
	if err != nil {
		_ = b.finishResourceOperation(context.WithoutCancel(ctx), fence, prepared.operationID, "indeterminate")
		return SourceResource{}, err
	}
	if !equalPublicationRelations(actual, relations) {
		_ = b.finishResourceOperation(context.WithoutCancel(ctx), fence, prepared.operationID, "rejected")
		if !exists && prepared.owned {
			_ = b.dropPublication(context.WithoutCancel(ctx), name)
		}
		return SourceResource{}, fmt.Errorf("%w: publication %s does not match frozen relation manifest", connector.ErrDeliveryConflict, name)
	}
	actualRevision, err := b.publicationRevision(ctx, name)
	if err != nil {
		_ = b.finishResourceOperation(context.WithoutCancel(ctx), fence, prepared.operationID, "indeterminate")
		return SourceResource{}, err
	}
	if actualRevision != revision {
		_ = b.finishResourceOperation(context.WithoutCancel(ctx), fence, prepared.operationID, "rejected")
		if !exists && prepared.owned {
			_ = b.dropPublication(context.WithoutCancel(ctx), name)
		}
		return SourceResource{}, fmt.Errorf("%w: publication %s semantics revision %s differs from frozen revision %s", connector.ErrDeliveryConflict, name, actualRevision, revision)
	}
	evidence := map[string]any{"relations": len(actual), "desired_revision": revision, "actual_revision": actualRevision}
	if err := b.publishResource(ctx, fence, snapshot, prepared, "publication", name, actualRevision, evidence); err != nil {
		return SourceResource{}, err
	}
	return SourceResource{ID: prepared.resourceID, Name: name, Revision: actualRevision, Owned: prepared.owned}, nil
}

func (b *Bootstrapper) prepareResource(ctx context.Context, fence authority.RunFence, snapshot ExportedSnapshot, kind, name, revision string, exists bool) (preparedResource, error) {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return preparedResource{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return preparedResource{}, err
	}
	if snapshot.BootstrapID != uuid.Nil {
		if _, phase, err := loadSnapshotForUpdate(ctx, tx, fence, snapshot.BootstrapID); err != nil {
			return preparedResource{}, err
		} else if phase != "snapshotting" {
			return preparedResource{}, fmt.Errorf("source resource requires snapshotting phase, got %s", phase)
		}
	}
	var currentID uuid.UUID
	var currentName, currentRevision, ownership, state string
	err = tx.QueryRow(ctx, `
SELECT resource_id,physical_name,revision,ownership,state
FROM source_resources
WHERE flow_incarnation_id=$1 AND resource_kind=$2
  AND state IN ('prepared','ready','cleanup_pending')
FOR UPDATE`, fence.FlowIncarnationID, kind).Scan(&currentID, &currentName, &currentRevision, &ownership, &state)
	resourceID := uuid.New()
	resourceRevision := revision
	owned := !exists
	switch {
	case err == nil:
		if currentName != name {
			return preparedResource{}, fmt.Errorf("%w: current %s resource %s differs from %s", connector.ErrDeliveryConflict, kind, currentName, name)
		}
		resourceID = currentID
		resourceRevision = currentRevision
		owned = ownership == "owned"
	case !errors.Is(err, pgx.ErrNoRows):
		return preparedResource{}, err
	}
	operation := "create"
	if exists {
		operation = "adopt"
	}
	operationID := uuid.New()
	var bootstrapID any
	if snapshot.BootstrapID != uuid.Nil {
		bootstrapID = snapshot.BootstrapID
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO source_resources (
  flow_incarnation_id,resource_kind,resource_id,flow_id,generation,acquisition_id,lease_epoch,
  created_generation,created_acquisition_id,created_lease_epoch,
  source_system_id,database_name,physical_name,ownership,revision,state,bootstrap_id
) VALUES ($1,$2,$3,$4,$5,$6,$7,$5,$6,$7,$8,$9,$10,$11,$12,'prepared',$13)
ON CONFLICT (flow_incarnation_id,resource_kind,resource_id) DO UPDATE SET
  generation=EXCLUDED.generation,acquisition_id=EXCLUDED.acquisition_id,
  lease_epoch=EXCLUDED.lease_epoch,bootstrap_id=EXCLUDED.bootstrap_id,updated_at=clock_timestamp()
WHERE source_resources.physical_name=EXCLUDED.physical_name
  AND source_resources.revision=EXCLUDED.revision
  AND source_resources.ownership=EXCLUDED.ownership`, fence.FlowIncarnationID, kind, resourceID, fence.FlowID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, snapshot.SourceSystem, snapshot.DatabaseName, name, map[bool]string{true: "owned", false: "adopted"}[owned], resourceRevision, bootstrapID); err != nil {
		return preparedResource{}, err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO source_resource_operations (
  operation_id,flow_incarnation_id,resource_kind,resource_id,operation,desired_revision,
  generation,acquisition_id,lease_epoch,status,bootstrap_id,source_system_id,database_name,physical_name
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'prepared',$10,$11,$12,$13)`, operationID, fence.FlowIncarnationID, kind, resourceID, operation, revision, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, bootstrapID, snapshot.SourceSystem, snapshot.DatabaseName, name); err != nil {
		return preparedResource{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return preparedResource{}, err
	}
	return preparedResource{resourceID: resourceID, operationID: operationID, operation: operation, owned: owned}, nil
}

func (b *Bootstrapper) publishResource(ctx context.Context, fence authority.RunFence, snapshot ExportedSnapshot, prepared preparedResource, kind, name, revision string, evidence map[string]any) error {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
UPDATE source_resources
SET state='ready',generation=$4,acquisition_id=$5,lease_epoch=$6,revision=$8,updated_at=clock_timestamp()
WHERE flow_incarnation_id=$1 AND resource_kind=$2 AND resource_id=$3
  AND physical_name=$7 AND state IN ('prepared','ready')`, fence.FlowIncarnationID, kind, prepared.resourceID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, name, revision)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: source resource ownership changed", authority.ErrFenceRejected)
	}
	encoded, err := jsonMarshal(evidence)
	if err != nil {
		return err
	}
	tag, err = tx.Exec(ctx, `
UPDATE source_resource_operations
SET status='applied',external_evidence=$4,completed_at=clock_timestamp()
WHERE operation_id=$1 AND flow_incarnation_id=$2 AND resource_id=$3 AND status='prepared'`, prepared.operationID, fence.FlowIncarnationID, prepared.resourceID, encoded)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: source resource operation changed", authority.ErrFenceRejected)
	}
	if kind == "publication" && snapshot.BootstrapID != uuid.Nil {
		if _, err := tx.Exec(ctx, `UPDATE source_bootstraps SET publication_name=$2,publication_revision=$3,updated_at=clock_timestamp() WHERE bootstrap_id=$1`, snapshot.BootstrapID, name, revision); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// AttachPublication binds a pre-slot create/adopt operation to the exact
// bootstrap generation after CREATE_REPLICATION_SLOT returns its cut.
func (b *Bootstrapper) AttachPublication(ctx context.Context, fence authority.RunFence, snapshot ExportedSnapshot, resource SourceResource) (ExportedSnapshot, error) {
	if resource.ID == uuid.Nil || strings.TrimSpace(resource.Name) == "" || strings.TrimSpace(resource.Revision) == "" {
		return ExportedSnapshot{}, errors.New("complete publication resource is required")
	}
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return ExportedSnapshot{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return ExportedSnapshot{}, err
	}
	persisted, phase, err := loadSnapshotForUpdate(ctx, tx, fence, snapshot.BootstrapID)
	if err != nil {
		return ExportedSnapshot{}, err
	}
	if phase != "snapshotting" || compareSnapshot(persisted, snapshot) != nil {
		return ExportedSnapshot{}, errors.New("publication attachment requires the current snapshotting generation")
	}
	tag, err := tx.Exec(ctx, `
UPDATE source_resources
SET bootstrap_id=$4,generation=$5,acquisition_id=$6,lease_epoch=$7,updated_at=clock_timestamp()
WHERE flow_incarnation_id=$1 AND resource_kind='publication' AND resource_id=$2
  AND physical_name=$3 AND revision=$8 AND state='ready'`, fence.FlowIncarnationID, resource.ID, resource.Name, snapshot.BootstrapID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, resource.Revision)
	if err != nil {
		return ExportedSnapshot{}, fmt.Errorf("attach publication resource: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ExportedSnapshot{}, fmt.Errorf("attach publication resource: affected=%d", tag.RowsAffected())
	}
	tag, err = tx.Exec(ctx, `
UPDATE source_bootstraps
SET publication_name=$2,publication_revision=$3,updated_at=clock_timestamp()
WHERE bootstrap_id=$1 AND flow_incarnation_id=$4 AND owner_generation=$5 AND phase='snapshotting'`, snapshot.BootstrapID, resource.Name, resource.Revision, fence.FlowIncarnationID, fence.Generation)
	if err != nil {
		return ExportedSnapshot{}, fmt.Errorf("attach publication to bootstrap: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ExportedSnapshot{}, fmt.Errorf("attach publication to bootstrap: affected=%d", tag.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		return ExportedSnapshot{}, err
	}
	snapshot.Publication = resource.Name
	snapshot.PublicationRevision = resource.Revision
	return snapshot, nil
}

func (b *Bootstrapper) finishResourceOperation(ctx context.Context, fence authority.RunFence, operationID uuid.UUID, status string) error {
	if status != "indeterminate" && status != "rejected" {
		return errors.New("invalid source resource failure status")
	}
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := authority.ValidateRunFence(ctx, tx, fence); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE source_resource_operations SET status=$2,external_evidence='{}'::jsonb,completed_at=clock_timestamp() WHERE operation_id=$1 AND status='prepared'`, operationID, status)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type terminalResource struct {
	id          uuid.UUID
	operationID uuid.UUID
	kind        string
	name        string
	revision    string
	state       string
	system      string
	database    string
	orphan      bool
}

// CleanupOwnedResources drops exact owned resources under a purpose-built
// quiescent lifecycle fence. This includes create operations interrupted after
// the external side effect but before the source resource became durable. The
// slot is always retired before the publication; adopted resources are not
// selected and pause never calls this terminal path.
func (b *Bootstrapper) CleanupOwnedResources(ctx context.Context, fence authority.CleanupFence) error {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateCleanupFence(ctx, tx, fence); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `
SELECT resource_id,resource_kind,physical_name,revision,state,source_system_id,database_name
FROM source_resources
WHERE flow_incarnation_id=$1 AND ownership='owned'
  AND resource_kind IN ('slot','publication')
  AND state IN ('prepared','ready','cleanup_pending')
ORDER BY CASE resource_kind WHEN 'slot' THEN 0 ELSE 1 END,created_at,resource_id
FOR UPDATE`, fence.FlowIncarnationID)
	if err != nil {
		return fmt.Errorf("load terminal owned resources: %w", err)
	}
	var resources []terminalResource
	for rows.Next() {
		var resource terminalResource
		if err := rows.Scan(&resource.id, &resource.kind, &resource.name, &resource.revision, &resource.state, &resource.system, &resource.database); err != nil {
			rows.Close()
			return err
		}
		resources = append(resources, resource)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	orphanRows, err := tx.Query(ctx, `
SELECT operation_id,resource_id,resource_kind,physical_name,desired_revision,status,
       source_system_id,database_name
FROM source_resource_operations operation_row
WHERE flow_incarnation_id=$1 AND operation='create'
  AND resource_kind IN ('slot','publication')
  AND status IN ('prepared','indeterminate')
  AND NOT EXISTS (
    SELECT 1 FROM source_resources resource_row
    WHERE resource_row.flow_incarnation_id=operation_row.flow_incarnation_id
      AND resource_row.resource_kind=operation_row.resource_kind
      AND resource_row.resource_id=operation_row.resource_id
  )
ORDER BY CASE resource_kind WHEN 'slot' THEN 0 ELSE 1 END,prepared_at,operation_id
FOR UPDATE`, fence.FlowIncarnationID)
	if err != nil {
		return fmt.Errorf("load terminal orphan creates: %w", err)
	}
	for orphanRows.Next() {
		resource := terminalResource{orphan: true}
		if err := orphanRows.Scan(&resource.operationID, &resource.id, &resource.kind, &resource.name, &resource.revision, &resource.state, &resource.system, &resource.database); err != nil {
			orphanRows.Close()
			return fmt.Errorf("load exact terminal orphan identity: %w", err)
		}
		resources = append(resources, resource)
	}
	orphanRows.Close()
	if err := orphanRows.Err(); err != nil {
		return err
	}
	sort.SliceStable(resources, func(i, j int) bool {
		return resources[i].kind == "slot" && resources[j].kind != "slot"
	})
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	for _, resource := range resources {
		if err := b.cleanupOwnedResource(ctx, fence, resource); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bootstrapper) cleanupOwnedResource(ctx context.Context, fence authority.CleanupFence, resource terminalResource) error {
	var liveSystem, liveDatabase string
	if err := b.source.QueryRow(ctx, `SELECT system_identifier::text,current_database() FROM pg_catalog.pg_control_system()`).Scan(&liveSystem, &liveDatabase); err != nil {
		return fmt.Errorf("verify terminal cleanup source identity: %w", err)
	}
	if liveSystem != resource.system || liveDatabase != resource.database {
		return fmt.Errorf("%w: terminal cleanup source identity differs for %s %q", connector.ErrDeliveryConflict, resource.kind, resource.name)
	}
	if resource.kind == "slot" {
		if !ownedSlotNamePattern.MatchString(resource.name) || resource.revision != ownedSlotRevision(resource.system, resource.database, resource.name) {
			return fmt.Errorf("%w: terminal slot %q ownership identity is invalid", connector.ErrDeliveryConflict, resource.name)
		}
	}
	operationID := resource.operationID
	if !resource.orphan {
		var err error
		operationID, err = b.prepareTerminalDrop(ctx, fence, resource)
		if err != nil {
			return err
		}
	}

	absent, inspectErr := b.terminalResourceAbsent(ctx, resource)
	if inspectErr == nil && !absent {
		var dropErr error
		switch resource.kind {
		case "slot":
			if b.hooks.DropSlot != nil {
				dropErr = b.hooks.DropSlot(ctx, resource.name)
			} else {
				_, dropErr = b.source.Exec(ctx, `SELECT pg_catalog.pg_drop_replication_slot($1)`, resource.name)
			}
		case "publication":
			dropErr = b.dropPublication(ctx, resource.name)
		default:
			return fmt.Errorf("unsupported terminal resource kind %q", resource.kind)
		}
		absent, inspectErr = b.terminalResourceAbsent(ctx, resource)
		if inspectErr != nil || !absent {
			_ = b.markTerminalDropIndeterminate(context.WithoutCancel(ctx), fence, operationID)
			if inspectErr != nil {
				return errors.Join(dropErr, fmt.Errorf("reconcile %s %q absence: %w", resource.kind, resource.name, inspectErr))
			}
			return fmt.Errorf("terminal drop of %s %q remains indeterminate: %w", resource.kind, resource.name, dropErr)
		}
	}
	if inspectErr != nil {
		_ = b.markTerminalDropIndeterminate(context.WithoutCancel(ctx), fence, operationID)
		return fmt.Errorf("inspect terminal %s %q: %w", resource.kind, resource.name, inspectErr)
	}
	if resource.orphan {
		return b.finalizeTerminalOrphanCreate(ctx, fence, resource)
	}
	return b.finalizeTerminalDrop(ctx, fence, operationID, resource)
}

func (b *Bootstrapper) prepareTerminalDrop(ctx context.Context, fence authority.CleanupFence, resource terminalResource) (uuid.UUID, error) {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateCleanupFence(ctx, tx, fence); err != nil {
		return uuid.Nil, err
	}
	operationID := uuid.New()
	if _, err := tx.Exec(ctx, `
INSERT INTO source_resource_operations(
 operation_id,flow_incarnation_id,resource_kind,resource_id,operation,desired_revision,
 generation,acquisition_id,lease_epoch,status,source_system_id,database_name,physical_name
) VALUES($1,$2,$3,$4,'drop',$5,$6,$7,$8,'prepared',$9,$10,$11)
ON CONFLICT(flow_incarnation_id,resource_kind,resource_id,operation,desired_revision,acquisition_id,lease_epoch)
DO NOTHING`, operationID, fence.FlowIncarnationID, resource.kind, resource.id, resource.revision, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, resource.system, resource.database, resource.name); err != nil {
		return uuid.Nil, fmt.Errorf("journal terminal %s drop: %w", resource.kind, err)
	}
	if err := tx.QueryRow(ctx, `
SELECT operation_id FROM source_resource_operations
WHERE flow_incarnation_id=$1 AND resource_kind=$2 AND resource_id=$3
 AND operation='drop' AND desired_revision=$4 AND acquisition_id=$5 AND lease_epoch=$6`, fence.FlowIncarnationID, resource.kind, resource.id, resource.revision, fence.AcquisitionID, fence.LeaseEpoch).Scan(&operationID); err != nil {
		return uuid.Nil, fmt.Errorf("load terminal drop operation: %w", err)
	}
	tag, err := tx.Exec(ctx, `
UPDATE source_resources
SET state='cleanup_pending',generation=$4,acquisition_id=$5,lease_epoch=$6,updated_at=clock_timestamp()
WHERE flow_incarnation_id=$1 AND resource_kind=$2 AND resource_id=$3
 AND ownership='owned' AND physical_name=$7 AND revision=$8
 AND state IN ('prepared','ready','cleanup_pending')`, fence.FlowIncarnationID, resource.kind, resource.id, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, resource.name, resource.revision)
	if err != nil {
		return uuid.Nil, err
	}
	if tag.RowsAffected() != 1 {
		return uuid.Nil, fmt.Errorf("%w: terminal owned resource changed before cleanup", authority.ErrFenceRejected)
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, err
	}
	return operationID, nil
}

func (b *Bootstrapper) terminalResourceAbsent(ctx context.Context, resource terminalResource) (bool, error) {
	var exists bool
	switch resource.kind {
	case "slot":
		var database, plugin, slotType string
		var active bool
		err := b.source.QueryRow(ctx, `
SELECT database,plugin,slot_type,active FROM pg_catalog.pg_replication_slots WHERE slot_name=$1`, resource.name).Scan(&database, &plugin, &slotType, &active)
		if errors.Is(err, pgx.ErrNoRows) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if database != resource.database || plugin != "pgoutput" || slotType != "logical" {
			return false, fmt.Errorf("%w: slot identity changed", connector.ErrDeliveryConflict)
		}
		if active {
			return false, fmt.Errorf("%w: slot %q is active", connector.ErrDeliveryConflict, resource.name)
		}
		return false, nil
	case "publication":
		err := b.source.QueryRow(ctx, `SELECT true FROM pg_catalog.pg_publication WHERE pubname=$1`, resource.name).Scan(&exists)
		if errors.Is(err, pgx.ErrNoRows) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		actualRevision, err := b.publicationRevision(ctx, resource.name)
		if err != nil {
			return false, err
		}
		if actualRevision != resource.revision {
			return false, fmt.Errorf("%w: owned publication %q revision changed from %s to %s", connector.ErrDeliveryConflict, resource.name, resource.revision, actualRevision)
		}
		return false, nil
	default:
		return false, fmt.Errorf("unsupported terminal resource kind %q", resource.kind)
	}
}

func (b *Bootstrapper) finalizeTerminalOrphanCreate(ctx context.Context, fence authority.CleanupFence, resource terminalResource) error {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateCleanupFence(ctx, tx, fence); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
UPDATE source_resource_operations
SET status='rejected',external_evidence=jsonb_build_object('resource_absent',true),completed_at=clock_timestamp()
WHERE operation_id=$1 AND flow_incarnation_id=$2 AND resource_kind=$3
  AND resource_id=$4 AND operation='create' AND desired_revision=$5
  AND source_system_id=$6 AND database_name=$7 AND physical_name=$8
  AND status IN ('prepared','indeterminate')`, resource.operationID, fence.FlowIncarnationID, resource.kind, resource.id, resource.revision, resource.system, resource.database, resource.name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: terminal orphan create operation changed", authority.ErrFenceRejected)
	}
	return tx.Commit(ctx)
}

func (b *Bootstrapper) markTerminalDropIndeterminate(ctx context.Context, fence authority.CleanupFence, operationID uuid.UUID) error {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateCleanupFence(ctx, tx, fence); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE source_resource_operations SET status='indeterminate' WHERE operation_id=$1 AND flow_incarnation_id=$2 AND status='prepared'`, operationID, fence.FlowIncarnationID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (b *Bootstrapper) finalizeTerminalDrop(ctx context.Context, fence authority.CleanupFence, operationID uuid.UUID, resource terminalResource) error {
	tx, err := b.control.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := authority.ValidateCleanupFence(ctx, tx, fence); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
UPDATE source_resources SET state='retired',updated_at=clock_timestamp()
WHERE flow_incarnation_id=$1 AND resource_kind=$2 AND resource_id=$3
 AND ownership='owned' AND physical_name=$4 AND revision=$5 AND state='cleanup_pending'`, fence.FlowIncarnationID, resource.kind, resource.id, resource.name, resource.revision)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: terminal resource retirement changed", authority.ErrFenceRejected)
	}
	tag, err = tx.Exec(ctx, `
UPDATE source_resource_operations
SET status='applied',external_evidence=jsonb_build_object('resource_absent',true),completed_at=clock_timestamp()
WHERE flow_incarnation_id=$2 AND resource_kind=$3 AND resource_id=$4
  AND operation='drop' AND desired_revision=$5
  AND status IN ('prepared','indeterminate')
  AND (operation_id=$1 OR status='indeterminate')`, operationID, fence.FlowIncarnationID, resource.kind, resource.id, resource.revision)
	if err != nil {
		return err
	}
	if tag.RowsAffected() < 1 {
		return fmt.Errorf("%w: terminal drop operation changed", authority.ErrFenceRejected)
	}
	if _, err := tx.Exec(ctx, `
UPDATE source_resource_operations
SET status='rejected',external_evidence=jsonb_build_object('resource_absent',true),completed_at=clock_timestamp()
WHERE flow_incarnation_id=$1 AND resource_kind=$2 AND resource_id=$3
  AND operation='create' AND desired_revision=$4
  AND source_system_id=$5 AND database_name=$6 AND physical_name=$7
  AND status IN ('prepared','indeterminate')`, fence.FlowIncarnationID, resource.kind, resource.id, resource.revision, resource.system, resource.database, resource.name); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	telemetry.RecordBootstrapEvent(ctx, "cleanup")
	return nil
}

func (b *Bootstrapper) createPublication(ctx context.Context, name string, relations []PublicationRelation) error {
	encoded, err := json.Marshal(relations)
	if err != nil {
		return err
	}
	conn, err := b.source.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `
CREATE OR REPLACE FUNCTION pg_temp.wallaby_create_publication(publication_name text, relation_manifest jsonb)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE relation_list text;
BEGIN
  SELECT string_agg(format('%I.%I', item->>'Namespace', item->>'Table'), ',')
  INTO relation_list
  FROM jsonb_array_elements(relation_manifest) AS item;
  IF relation_list IS NULL THEN RAISE EXCEPTION 'publication relation manifest is empty'; END IF;
  EXECUTE format('CREATE PUBLICATION %I FOR TABLE %s', publication_name, relation_list);
END
$$`); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, `SELECT pg_temp.wallaby_create_publication($1,$2::jsonb)`, name, encoded)
	return err
}

func (b *Bootstrapper) dropPublication(ctx context.Context, name string) error {
	conn, err := b.source.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `
CREATE OR REPLACE FUNCTION pg_temp.wallaby_drop_publication(publication_name text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE format('DROP PUBLICATION %I', publication_name);
END
$$`); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, `SELECT pg_temp.wallaby_drop_publication($1)`, name)
	return err
}

// ExpectedPublicationRevision computes the exact canonical definition created
// by EnsurePublication: ordinary per-table membership, all columns, no row
// filters, all DML operations, and publish_via_partition_root=false.
func ExpectedPublicationRevision(name string, relations []PublicationRelation) string {
	definition := postgrescodec.PublicationDefinition{
		Name: name, Insert: true, Update: true, Delete: true, Truncate: true,
		Relations: make([]postgrescodec.PublicationRelation, 0, len(relations)),
	}
	for _, relation := range relations {
		definition.Relations = append(definition.Relations, postgrescodec.PublicationRelation{
			Namespace: relation.Namespace,
			Table:     relation.Table,
		})
	}
	revision, err := postgrescodec.PublicationFingerprint(definition)
	if err != nil {
		return ""
	}
	return revision
}

func (b *Bootstrapper) publicationRevision(ctx context.Context, name string) (string, error) {
	return postgrescodec.LivePublicationFingerprint(ctx, b.source, name)
}

func (b *Bootstrapper) publicationExists(ctx context.Context, name string) (bool, error) {
	var exists bool
	if err := b.source.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_publication WHERE pubname=$1)`, name).Scan(&exists); err != nil {
		return false, fmt.Errorf("inspect publication %s: %w", name, err)
	}
	return exists, nil
}

func (b *Bootstrapper) publicationRelations(ctx context.Context, name string) ([]PublicationRelation, error) {
	rows, err := b.source.Query(ctx, `
SELECT c.oid,n.nspname,c.relname,c.relkind::text,c.relispartition
FROM pg_catalog.pg_publication p
JOIN pg_catalog.pg_publication_rel pr ON pr.prpubid=p.oid
JOIN pg_catalog.pg_class c ON c.oid=pr.prrelid
JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
WHERE p.pubname=$1
ORDER BY c.oid`, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []PublicationRelation
	for rows.Next() {
		var relation PublicationRelation
		if err := rows.Scan(&relation.OID, &relation.Namespace, &relation.Table, &relation.RelationKind, &relation.IsPartition); err != nil {
			return nil, err
		}
		result = append(result, relation)
	}
	return result, rows.Err()
}

func equalPublicationRelations(left, right []PublicationRelation) bool {
	if len(left) != len(right) {
		return false
	}
	copyLeft := append([]PublicationRelation(nil), left...)
	copyRight := append([]PublicationRelation(nil), right...)
	sort.Slice(copyLeft, func(i, j int) bool { return copyLeft[i].OID < copyLeft[j].OID })
	sort.Slice(copyRight, func(i, j int) bool { return copyRight[i].OID < copyRight[j].OID })
	for i := range copyLeft {
		if copyLeft[i] != copyRight[i] {
			return false
		}
	}
	return true
}

func jsonMarshal(value any) ([]byte, error) {
	if value == nil {
		return []byte("{}"), nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal source resource evidence: %w", err)
	}
	return encoded, nil
}
