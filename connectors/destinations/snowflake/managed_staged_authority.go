package snowflake

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

const stagedRuntimeLeaseMinimum = 2 * time.Minute

func stagedRuntimeLeaseDuration(cfg stagedConfig) time.Duration {
	duration := time.Duration(cfg.statementTimeoutSeconds+60) * time.Second
	if duration < stagedRuntimeLeaseMinimum {
		return stagedRuntimeLeaseMinimum
	}
	return duration
}

type stagedLeaseRequest struct {
	leaseID             string
	ownerID             string
	flowIncarnationID   string
	generation          int64
	acquisitionID       string
	leaseEpoch          int64
	catalogFingerprint  string
	destinationRevision string
}

type stagedRuntimeLease struct {
	stagedLeaseRequest
	provisionEpoch int64
	expiresAt      time.Time //nolint:unused // used by the clocked protocol fake in same-package tests
}

type stagedLoadClaim struct {
	leaseID             string
	claimID             string
	logicalBatchID      string
	stagePath           string
	manifestHash        string
	contentHash         string
	fileContentHash     string
	planHash            string
	expectedRows        int
	provisionEpoch      int64
	catalogFingerprint  string
	destinationRevision string
	expiresAt           time.Time //nolint:unused // used by the clocked protocol fake in same-package tests
}

type stagedTargetState uint8

const (
	stagedTargetUnknown stagedTargetState = iota
	stagedTargetAbsent
	stagedTargetPartial
	stagedTargetComplete
	stagedTargetDuplicate
	stagedTargetConflict
)

type stagedTargetObservation struct {
	state     stagedTargetState
	rowCount  int
	manifest  bool
	rowHashes []string
	detail    string
}

// stagedAuthorityProtocol is the Snowflake-side authority boundary for staged
// delivery. COPY history is deliberately absent: target rows plus the immutable
// companion manifest are the durable proof.
type stagedAuthorityProtocol interface {
	AcquireRuntimeLease(context.Context, stagedConfig, stagedLeaseRequest) (stagedRuntimeLease, error)
	RevalidateRuntimeLease(context.Context, stagedConfig, stagedRuntimeLease) error
	GuardCatalog(context.Context, stagedConfig, stagedRuntimeLease) error
	ReleaseRuntimeLease(context.Context, stagedConfig, stagedRuntimeLease) error
	AcquireLoadClaim(context.Context, stagedConfig, stagedRuntimeLease, stagedLoadClaim) (stagedLoadClaim, error)
	ObserveLanding(context.Context, stagedConfig, stagedLoadClaim, []string) (stagedTargetObservation, error)
	ObserveTarget(context.Context, stagedConfig, stagedLoadClaim, []string) (stagedTargetObservation, error)
	PromoteTarget(context.Context, stagedConfig, stagedRuntimeLease, stagedLoadClaim, []string) error
	InsertLoadReceipt(context.Context, stagedConfig, stagedRuntimeLease, stagedLoadClaim, managedStagedReceipt) (stageReceiptInsert, error)
	InsertReleaseReceipt(context.Context, stagedConfig, stagedRuntimeLease, stagedLoadClaim, managedStagedReceipt) (stageReceiptInsert, error)
	ValidateReceiptTargetProof(context.Context, stagedConfig, managedStagedReceipt) error
}

func stagedLeaseRequestForPlan(intent connector.DeliveryIntent, catalogFingerprint string) stagedLeaseRequest {
	ownerID := intent.AcquisitionID + ":" + fmt.Sprint(intent.LeaseEpoch)
	leaseID := stagedContentHash([]byte(strings.Join([]string{intent.FlowIncarnationID, intent.DestinationRevisionID}, "\x1f")))
	return stagedLeaseRequest{
		leaseID: leaseID, ownerID: ownerID, flowIncarnationID: intent.FlowIncarnationID,
		generation: intent.Generation, acquisitionID: intent.AcquisitionID, leaseEpoch: intent.LeaseEpoch,
		catalogFingerprint: catalogFingerprint, destinationRevision: intent.DestinationRevisionID,
	}
}

func stagedLoadClaimForPlan(lease stagedRuntimeLease, plan managedStagedPlan) stagedLoadClaim {
	return stagedLoadClaim{
		leaseID: lease.leaseID, claimID: plan.identity.externalID, logicalBatchID: plan.receipt.logicalBatchID, stagePath: plan.identity.relativePath,
		manifestHash: plan.identity.manifestHash, contentHash: plan.receipt.contentHash,
		fileContentHash: plan.fileContentHash, planHash: plan.identity.planHash, expectedRows: plan.rowCount,
		provisionEpoch: lease.provisionEpoch, catalogFingerprint: lease.catalogFingerprint,
		destinationRevision: lease.destinationRevision,
	}
}

func classifyStagedTarget(expected []string, manifest bool, manifestMatches bool, actual map[string]int) stagedTargetObservation {
	expectedCounts := make(map[string]int, len(expected))
	for _, hash := range expected {
		expectedCounts[hash]++
	}
	actualRows := 0
	for _, count := range actual {
		actualRows += count
		if count > 1 {
			return stagedTargetObservation{state: stagedTargetDuplicate, rowCount: actualRows, manifest: manifest, detail: "duplicate row identity"}
		}
	}
	if manifest && !manifestMatches {
		return stagedTargetObservation{state: stagedTargetConflict, rowCount: actualRows, manifest: true, detail: "manifest identity differs"}
	}
	if !manifest && actualRows == 0 {
		return stagedTargetObservation{state: stagedTargetAbsent}
	}
	if len(actual) != len(expectedCounts) {
		return stagedTargetObservation{state: stagedTargetPartial, rowCount: actualRows, manifest: manifest, detail: "row identity cardinality differs"}
	}
	for hash, count := range expectedCounts {
		if count != 1 || actual[hash] != 1 {
			return stagedTargetObservation{state: stagedTargetConflict, rowCount: actualRows, manifest: manifest, detail: "row identity set differs"}
		}
	}
	if !manifest {
		return stagedTargetObservation{state: stagedTargetPartial, rowCount: actualRows, detail: "rows exist without target manifest"}
	}
	hashes := append([]string(nil), expected...)
	sort.Strings(hashes)
	return stagedTargetObservation{state: stagedTargetComplete, rowCount: actualRows, manifest: true, rowHashes: hashes}
}

func (p *sqlStageProtocol) AcquireRuntimeLease(ctx context.Context, cfg stagedConfig, request stagedLeaseRequest) (stagedRuntimeLease, error) {
	table := managedSnowflakeStagedQualifiedTable(cfg, cfg.authorityTable)
	var epoch int64
	var fingerprint string
	if err := p.db.QueryRowContext(ctx, "SELECT \"PROVISION_EPOCH\", \"CATALOG_FINGERPRINT\" FROM "+table+" WHERE \"AUTHORITY_KIND\"='CATALOG' AND \"DESTINATION_REVISION_ID\"=? AND \"STATE\"='CURRENT'", request.destinationRevision).Scan(&epoch, &fingerprint); err != nil {
		return stagedRuntimeLease{}, fmt.Errorf("read staged Snowflake catalog guard: %w", err)
	}
	if epoch <= 0 || fingerprint != request.catalogFingerprint {
		return stagedRuntimeLease{}, fmt.Errorf("%w: staged Snowflake catalog guard differs", connector.ErrDeliveryConflict)
	}
	// #nosec G202 -- table is composed only from strictly validated Snowflake identifiers.
	statement := "MERGE INTO " + table + " AS A USING (SELECT ? AS LEASE_ID) AS S ON A.\"AUTHORITY_KIND\"='LEASE' AND A.\"AUTHORITY_ID\"=S.LEASE_ID " +
		"WHEN MATCHED AND (A.\"OWNER_ID\"=? OR A.\"EXPIRES_AT\"<=CURRENT_TIMESTAMP()) THEN UPDATE SET \"OWNER_ID\"=?,\"FLOW_INCARNATION_ID\"=?,\"GENERATION\"=?,\"ACQUISITION_ID\"=?,\"LEASE_EPOCH\"=?,\"PROVISION_EPOCH\"=?,\"CATALOG_FINGERPRINT\"=?,\"STATE\"='ACTIVE',\"EXPIRES_AT\"=DATEADD('second',?,CURRENT_TIMESTAMP()),\"UPDATED_AT\"=CURRENT_TIMESTAMP() " +
		"WHEN NOT MATCHED THEN INSERT (\"AUTHORITY_KIND\",\"DESTINATION_REVISION_ID\",\"AUTHORITY_ID\",\"OWNER_ID\",\"FLOW_INCARNATION_ID\",\"GENERATION\",\"ACQUISITION_ID\",\"LEASE_EPOCH\",\"PROVISION_EPOCH\",\"CATALOG_FINGERPRINT\",\"STATE\",\"EXPIRES_AT\",\"UPDATED_AT\") VALUES ('LEASE',?,?,?, ?,?,?,?,?,?,'ACTIVE',DATEADD('second',?,CURRENT_TIMESTAMP()),CURRENT_TIMESTAMP())"
	seconds := int(stagedRuntimeLeaseDuration(cfg) / time.Second)
	result, err := p.db.ExecContext(ctx, statement,
		request.leaseID, request.ownerID, request.ownerID, request.flowIncarnationID, request.generation, request.acquisitionID, request.leaseEpoch, epoch, fingerprint, seconds,
		request.destinationRevision, request.leaseID, request.ownerID, request.flowIncarnationID, request.generation, request.acquisitionID, request.leaseEpoch, epoch, fingerprint, seconds,
	)
	if err != nil {
		return stagedRuntimeLease{}, fmt.Errorf("acquire staged Snowflake runtime lease: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return stagedRuntimeLease{}, fmt.Errorf("%w: staged Snowflake runtime lease CAS did not affect one row", connector.ErrDeliveryIndeterminate)
	}
	lease := stagedRuntimeLease{stagedLeaseRequest: request, provisionEpoch: epoch}
	if err := p.RevalidateRuntimeLease(ctx, cfg, lease); err != nil {
		return stagedRuntimeLease{}, err
	}
	return lease, nil
}

func (p *sqlStageProtocol) RevalidateRuntimeLease(ctx context.Context, cfg stagedConfig, lease stagedRuntimeLease) error {
	table := managedSnowflakeStagedQualifiedTable(cfg, cfg.authorityTable)
	seconds := int(stagedRuntimeLeaseDuration(cfg) / time.Second)
	// #nosec G202 -- table is composed only from strictly validated Snowflake identifiers.
	result, err := p.db.ExecContext(ctx, "UPDATE "+table+" AS L SET \"EXPIRES_AT\"=DATEADD('second',?,CURRENT_TIMESTAMP()),\"UPDATED_AT\"=CURRENT_TIMESTAMP() WHERE L.\"AUTHORITY_KIND\"='LEASE' AND L.\"AUTHORITY_ID\"=? AND L.\"OWNER_ID\"=? AND L.\"FLOW_INCARNATION_ID\"=? AND L.\"GENERATION\"=? AND L.\"ACQUISITION_ID\"=? AND L.\"LEASE_EPOCH\"=? AND L.\"PROVISION_EPOCH\"=? AND L.\"CATALOG_FINGERPRINT\"=? AND L.\"STATE\"='ACTIVE' AND L.\"EXPIRES_AT\">CURRENT_TIMESTAMP() AND EXISTS (SELECT 1 FROM "+table+" AS C WHERE C.\"AUTHORITY_KIND\"='CATALOG' AND C.\"DESTINATION_REVISION_ID\"=L.\"DESTINATION_REVISION_ID\" AND C.\"PROVISION_EPOCH\"=L.\"PROVISION_EPOCH\" AND C.\"CATALOG_FINGERPRINT\"=L.\"CATALOG_FINGERPRINT\" AND C.\"STATE\"='CURRENT')", seconds, lease.leaseID, lease.ownerID, lease.flowIncarnationID, lease.generation, lease.acquisitionID, lease.leaseEpoch, lease.provisionEpoch, lease.catalogFingerprint)
	if err != nil {
		return fmt.Errorf("renew staged Snowflake runtime lease: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("%w: staged Snowflake runtime lease renewal lost authority", connector.ErrDeliveryIndeterminate)
	}
	var count int
	err = p.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" AS L JOIN "+table+" AS C ON C.\"AUTHORITY_KIND\"='CATALOG' AND C.\"DESTINATION_REVISION_ID\"=L.\"DESTINATION_REVISION_ID\" WHERE L.\"AUTHORITY_KIND\"='LEASE' AND L.\"AUTHORITY_ID\"=? AND L.\"OWNER_ID\"=? AND L.\"FLOW_INCARNATION_ID\"=? AND L.\"GENERATION\"=? AND L.\"ACQUISITION_ID\"=? AND L.\"LEASE_EPOCH\"=? AND L.\"PROVISION_EPOCH\"=? AND L.\"CATALOG_FINGERPRINT\"=? AND L.\"STATE\"='ACTIVE' AND L.\"EXPIRES_AT\">CURRENT_TIMESTAMP() AND C.\"PROVISION_EPOCH\"=L.\"PROVISION_EPOCH\" AND C.\"CATALOG_FINGERPRINT\"=L.\"CATALOG_FINGERPRINT\" AND C.\"STATE\"='CURRENT'",
		lease.leaseID, lease.ownerID, lease.flowIncarnationID, lease.generation, lease.acquisitionID, lease.leaseEpoch, lease.provisionEpoch, lease.catalogFingerprint).Scan(&count)
	if err != nil {
		return fmt.Errorf("revalidate staged Snowflake runtime lease: %w", err)
	}
	if count != 1 {
		return fmt.Errorf("%w: staged Snowflake runtime lease or catalog epoch is stale", connector.ErrDeliveryIndeterminate)
	}
	return nil
}

func (p *sqlStageProtocol) GuardCatalog(ctx context.Context, cfg stagedConfig, lease stagedRuntimeLease) error {
	if err := p.RevalidateRuntimeLease(ctx, cfg, lease); err != nil {
		return err
	}
	catalog, err := (&Destination{stagedConfig: cfg}).loadManagedStagedCatalog(ctx, p.db)
	if err != nil {
		return fmt.Errorf("reload live staged Snowflake catalog: %w", err)
	}
	if err := validateManagedStagedCatalog(cfg, catalog); err != nil {
		return fmt.Errorf("revalidate live staged Snowflake catalog: %w", err)
	}
	fingerprint, err := managedStagedCatalogFingerprint(catalog)
	if err != nil {
		return err
	}
	if fingerprint != lease.catalogFingerprint {
		return fmt.Errorf("%w: live staged Snowflake catalog fingerprint changed", connector.ErrDeliveryConflict)
	}
	return p.RevalidateRuntimeLease(ctx, cfg, lease)
}

func (p *sqlStageProtocol) ReleaseRuntimeLease(ctx context.Context, cfg stagedConfig, lease stagedRuntimeLease) error {
	// #nosec G202 -- the authority table is composed only from strictly validated identifiers.
	result, err := p.db.ExecContext(ctx, "UPDATE "+managedSnowflakeStagedQualifiedTable(cfg, cfg.authorityTable)+" SET \"STATE\"='RELEASED',\"UPDATED_AT\"=CURRENT_TIMESTAMP() WHERE \"AUTHORITY_KIND\"='LEASE' AND \"AUTHORITY_ID\"=? AND \"OWNER_ID\"=? AND \"PROVISION_EPOCH\"=?", lease.leaseID, lease.ownerID, lease.provisionEpoch)
	if err != nil {
		return fmt.Errorf("release staged Snowflake runtime lease: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("%w: staged Snowflake runtime lease release did not affect exactly one row", connector.ErrDeliveryIndeterminate)
	}
	return nil
}

func (p *sqlStageProtocol) AcquireLoadClaim(ctx context.Context, cfg stagedConfig, lease stagedRuntimeLease, claim stagedLoadClaim) (stagedLoadClaim, error) {
	if err := p.GuardCatalog(ctx, cfg, lease); err != nil {
		return stagedLoadClaim{}, err
	}
	table := managedSnowflakeStagedQualifiedTable(cfg, cfg.authorityTable)
	seconds := int(stagedRuntimeLeaseDuration(cfg) / time.Second)
	// A live same-owner row can renew only when every immutable field matches.
	// An expired row is a takeover and replaces every owner and identity field
	// from the current lease and plan, never from the stale claim.
	// #nosec G202 -- table is composed only from strictly validated Snowflake identifiers.
	statement := "MERGE INTO " + table + " AS A USING (SELECT ? AS CLAIM_ID, ? AS DESTINATION_REVISION_ID) AS S " +
		"ON A.\"AUTHORITY_KIND\"='CLAIM' AND A.\"AUTHORITY_ID\"=S.CLAIM_ID AND A.\"DESTINATION_REVISION_ID\"=S.DESTINATION_REVISION_ID " +
		"WHEN MATCHED AND (A.\"EXPIRES_AT\"<=CURRENT_TIMESTAMP() OR (A.\"OWNER_ID\"=? AND A.\"FLOW_INCARNATION_ID\"=? AND A.\"GENERATION\"=? AND A.\"ACQUISITION_ID\"=? AND A.\"LEASE_EPOCH\"=? AND A.\"PROVISION_EPOCH\"=? AND A.\"CATALOG_FINGERPRINT\"=? AND A.\"LOGICAL_BATCH_ID\"=? AND A.\"MANIFEST_HASH\"=? AND A.\"CONTENT_HASH\"=? AND A.\"FILE_CONTENT_HASH\"=? AND A.\"PLAN_HASH\"=? AND A.\"EXPECTED_ROW_COUNT\"=?)) " +
		"THEN UPDATE SET \"OWNER_ID\"=?,\"FLOW_INCARNATION_ID\"=?,\"GENERATION\"=?,\"ACQUISITION_ID\"=?,\"LEASE_EPOCH\"=?,\"PROVISION_EPOCH\"=?,\"CATALOG_FINGERPRINT\"=?,\"LOGICAL_BATCH_ID\"=?,\"MANIFEST_HASH\"=?,\"CONTENT_HASH\"=?,\"FILE_CONTENT_HASH\"=?,\"PLAN_HASH\"=?,\"EXPECTED_ROW_COUNT\"=?,\"STATE\"='ACTIVE',\"EXPIRES_AT\"=DATEADD('second',?,CURRENT_TIMESTAMP()),\"UPDATED_AT\"=CURRENT_TIMESTAMP() " +
		"WHEN NOT MATCHED THEN INSERT (\"AUTHORITY_KIND\",\"DESTINATION_REVISION_ID\",\"AUTHORITY_ID\",\"OWNER_ID\",\"FLOW_INCARNATION_ID\",\"GENERATION\",\"ACQUISITION_ID\",\"LEASE_EPOCH\",\"PROVISION_EPOCH\",\"CATALOG_FINGERPRINT\",\"LOGICAL_BATCH_ID\",\"MANIFEST_HASH\",\"CONTENT_HASH\",\"FILE_CONTENT_HASH\",\"PLAN_HASH\",\"EXPECTED_ROW_COUNT\",\"STATE\",\"EXPIRES_AT\",\"UPDATED_AT\") VALUES ('CLAIM',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'ACTIVE',DATEADD('second',?,CURRENT_TIMESTAMP()),CURRENT_TIMESTAMP())"
	identity := []any{lease.ownerID, lease.flowIncarnationID, lease.generation, lease.acquisitionID, lease.leaseEpoch, claim.provisionEpoch, claim.catalogFingerprint, claim.logicalBatchID, claim.manifestHash, claim.contentHash, claim.fileContentHash, claim.planHash, claim.expectedRows}
	args := make([]any, 0, 2+len(identity)*3+4)
	args = append(args, claim.claimID, claim.destinationRevision)
	args = append(args, identity...)
	args = append(args, identity...)
	args = append(args, seconds, claim.destinationRevision, claim.claimID)
	args = append(args, identity...)
	args = append(args, seconds)
	result, err := p.db.ExecContext(ctx, statement, args...)
	if err != nil {
		return stagedLoadClaim{}, fmt.Errorf("acquire staged Snowflake load claim: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return stagedLoadClaim{}, fmt.Errorf("%w: staged Snowflake load claim CAS did not affect one row", connector.ErrDeliveryIndeterminate)
	}
	var count int
	verify := "SELECT COUNT(*) FROM " + table + " WHERE \"AUTHORITY_KIND\"='CLAIM' AND \"DESTINATION_REVISION_ID\"=? AND \"AUTHORITY_ID\"=? AND \"OWNER_ID\"=? AND \"FLOW_INCARNATION_ID\"=? AND \"GENERATION\"=? AND \"ACQUISITION_ID\"=? AND \"LEASE_EPOCH\"=? AND \"PROVISION_EPOCH\"=? AND \"CATALOG_FINGERPRINT\"=? AND \"LOGICAL_BATCH_ID\"=? AND \"MANIFEST_HASH\"=? AND \"CONTENT_HASH\"=? AND \"FILE_CONTENT_HASH\"=? AND \"PLAN_HASH\"=? AND \"EXPECTED_ROW_COUNT\"=? AND \"STATE\"='ACTIVE' AND \"EXPIRES_AT\">CURRENT_TIMESTAMP()"
	verifyArgs := make([]any, 0, 2+len(identity))
	verifyArgs = append(verifyArgs, claim.destinationRevision, claim.claimID)
	verifyArgs = append(verifyArgs, identity...)
	if err := p.db.QueryRowContext(ctx, verify, verifyArgs...).Scan(&count); err != nil {
		return stagedLoadClaim{}, fmt.Errorf("verify staged Snowflake load claim: %w", err)
	}
	if count != 1 {
		return stagedLoadClaim{}, fmt.Errorf("%w: staged Snowflake load claim is owned by another or divergent writer", connector.ErrDeliveryConflict)
	}
	return claim, nil
}

func (p *sqlStageProtocol) ObserveLanding(ctx context.Context, cfg stagedConfig, claim stagedLoadClaim, expected []string) (stagedTargetObservation, error) {
	return p.observeStagedRows(ctx, cfg, cfg.landingTable, claim, expected, false)
}

func (p *sqlStageProtocol) ObserveTarget(ctx context.Context, cfg stagedConfig, claim stagedLoadClaim, expected []string) (stagedTargetObservation, error) {
	return p.observeStagedRows(ctx, cfg, cfg.table, claim, expected, true)
}

func (p *sqlStageProtocol) observeStagedRows(ctx context.Context, cfg stagedConfig, tableName string, claim stagedLoadClaim, expected []string, requireManifest bool) (stagedTargetObservation, error) {
	// #nosec G202 -- tableName is selected only from validated staged configuration.
	rows, err := p.db.QueryContext(ctx, "SELECT \"RECORD_HASH\", COUNT(*) FROM "+managedSnowflakeStagedQualifiedTable(cfg, tableName)+" WHERE \"DESTINATION_REVISION_ID\"=? AND \"LOGICAL_BATCH_ID\"=? GROUP BY \"RECORD_HASH\"", claim.destinationRevision, claim.logicalBatchID)
	if err != nil {
		return stagedTargetObservation{}, fmt.Errorf("observe staged Snowflake row identities: %w", err)
	}
	defer func() { _ = rows.Close() }()
	actual := make(map[string]int)
	for rows.Next() {
		var hash string
		var count int
		if err := rows.Scan(&hash, &count); err != nil {
			return stagedTargetObservation{}, err
		}
		actual[hash] = count
	}
	if err := rows.Err(); err != nil {
		return stagedTargetObservation{}, err
	}
	if !requireManifest {
		observation := classifyStagedTarget(expected, len(actual) > 0 || len(expected) == 0, true, actual)
		if len(expected) == 0 {
			observation.state = stagedTargetComplete
		}
		return observation, nil
	}
	var manifestHash, contentHash, fileHash, planHash, catalog string
	var expectedRows, epoch int64
	err = p.db.QueryRowContext(ctx, "SELECT \"MANIFEST_HASH\",\"CONTENT_HASH\",\"FILE_CONTENT_HASH\",\"PLAN_HASH\",\"EXPECTED_ROW_COUNT\",\"PROVISION_EPOCH\",\"CATALOG_FINGERPRINT\" FROM "+managedSnowflakeStagedQualifiedTable(cfg, cfg.targetManifestTable)+" WHERE \"DESTINATION_REVISION_ID\"=? AND \"LOGICAL_BATCH_ID\"=?", claim.destinationRevision, claim.logicalBatchID).Scan(&manifestHash, &contentHash, &fileHash, &planHash, &expectedRows, &epoch, &catalog)
	manifest := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return stagedTargetObservation{}, fmt.Errorf("observe staged Snowflake target manifest: %w", err)
	}
	matches := manifest && manifestHash == claim.manifestHash && contentHash == claim.contentHash && fileHash == claim.fileContentHash && planHash == claim.planHash && expectedRows == int64(claim.expectedRows) && epoch == claim.provisionEpoch && catalog == claim.catalogFingerprint
	return classifyStagedTarget(expected, manifest, matches, actual), nil
}

func (p *sqlStageProtocol) ValidateReceiptTargetProof(ctx context.Context, cfg stagedConfig, receipt managedStagedReceipt) error {
	var manifestHash, contentHash, fileHash, planHash, catalog, rowHashesJSON, currentCatalog, currentState string
	var expectedRows int
	var manifestEpoch, currentEpoch int64
	manifest := managedSnowflakeStagedQualifiedTable(cfg, cfg.targetManifestTable)
	authority := managedSnowflakeStagedQualifiedTable(cfg, cfg.authorityTable)
	err := p.db.QueryRowContext(ctx, "SELECT M.\"MANIFEST_HASH\",M.\"CONTENT_HASH\",M.\"FILE_CONTENT_HASH\",M.\"PLAN_HASH\",M.\"EXPECTED_ROW_COUNT\",M.\"PROVISION_EPOCH\",M.\"CATALOG_FINGERPRINT\",M.\"ROW_HASHES_JSON\",C.\"PROVISION_EPOCH\",C.\"CATALOG_FINGERPRINT\",C.\"STATE\" FROM "+manifest+" AS M JOIN "+authority+" AS C ON C.\"AUTHORITY_KIND\"='CATALOG' AND C.\"DESTINATION_REVISION_ID\"=M.\"DESTINATION_REVISION_ID\" AND C.\"AUTHORITY_ID\"='CURRENT' WHERE M.\"DESTINATION_REVISION_ID\"=? AND M.\"LOGICAL_BATCH_ID\"=?", receipt.destinationRevisionID, receipt.logicalBatchID).Scan(&manifestHash, &contentHash, &fileHash, &planHash, &expectedRows, &manifestEpoch, &catalog, &rowHashesJSON, &currentEpoch, &currentCatalog, &currentState)
	if err != nil {
		return fmt.Errorf("%w: staged Snowflake receipt target manifest is unavailable: %w", connector.ErrDeliveryIndeterminate, err)
	}
	if manifestHash != receipt.manifestHash || contentHash != receipt.contentHash || fileHash != receipt.fileContentHash || planHash != receipt.planHash || expectedRows != receipt.recordCount || manifestEpoch != receipt.provisionEpoch || catalog != receipt.catalogFingerprint || currentEpoch != receipt.provisionEpoch || currentCatalog != receipt.catalogFingerprint || currentState != "CURRENT" {
		return fmt.Errorf("%w: staged Snowflake receipt target manifest differs", connector.ErrDeliveryConflict)
	}
	var expected []string
	if err := json.Unmarshal([]byte(rowHashesJSON), &expected); err != nil {
		return fmt.Errorf("%w: decode staged Snowflake target row identities: %w", connector.ErrDeliveryConflict, err)
	}
	// #nosec G202 -- the target table is composed only from strictly validated identifiers.
	rows, err := p.db.QueryContext(ctx, "SELECT \"RECORD_HASH\",COUNT(*) FROM "+managedSnowflakeStagedQualifiedTable(cfg, cfg.table)+" WHERE \"DESTINATION_REVISION_ID\"=? AND \"LOGICAL_BATCH_ID\"=? GROUP BY \"RECORD_HASH\"", receipt.destinationRevisionID, receipt.logicalBatchID)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	actual := make(map[string]int)
	for rows.Next() {
		var hash string
		var count int
		if err := rows.Scan(&hash, &count); err != nil {
			return err
		}
		actual[hash] = count
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate staged Snowflake receipt target proof: %w", err)
	}
	observation := classifyStagedTarget(expected, true, true, actual)
	if observation.state != stagedTargetComplete {
		return fmt.Errorf("%w: staged Snowflake receipt target proof state=%d: %s", connector.ErrDeliveryConflict, observation.state, observation.detail)
	}
	return nil
}

func (p *sqlStageProtocol) InsertLoadReceipt(ctx context.Context, cfg stagedConfig, lease stagedRuntimeLease, claim stagedLoadClaim, receipt managedStagedReceipt) (stageReceiptInsert, error) {
	if err := p.GuardCatalog(ctx, cfg, lease); err != nil {
		return stageReceiptInsert{}, err
	}
	if err := p.ValidateReceiptTargetProof(ctx, cfg, receipt); err != nil {
		return stageReceiptInsert{}, err
	}
	columns := stagedReceiptColumns()
	authority := managedSnowflakeStagedQualifiedTable(cfg, cfg.authorityTable)
	// #nosec G202 -- all table names and columns are from closed validated contracts.
	statement := "INSERT INTO " + managedSnowflakeStagedQualifiedTable(cfg, cfg.receiptsTable) + " (" + quoteColumns(columns) + ", \"COMMITTED_AT\") SELECT " + placeholders(len(columns)) + ", CURRENT_TIMESTAMP() FROM " + authority + " AS L JOIN " + authority + " AS C ON C.\"AUTHORITY_KIND\"='CATALOG' AND C.\"DESTINATION_REVISION_ID\"=L.\"DESTINATION_REVISION_ID\" WHERE L.\"AUTHORITY_KIND\"='LEASE' AND L.\"AUTHORITY_ID\"=? AND L.\"OWNER_ID\"=? AND L.\"PROVISION_EPOCH\"=? AND L.\"CATALOG_FINGERPRINT\"=? AND L.\"STATE\"='ACTIVE' AND L.\"EXPIRES_AT\">CURRENT_TIMESTAMP() AND C.\"PROVISION_EPOCH\"=L.\"PROVISION_EPOCH\" AND C.\"CATALOG_FINGERPRINT\"=L.\"CATALOG_FINGERPRINT\" AND C.\"STATE\"='CURRENT' AND EXISTS (SELECT 1 FROM " + authority + " AS Q WHERE Q.\"AUTHORITY_KIND\"='CLAIM' AND Q.\"DESTINATION_REVISION_ID\"=L.\"DESTINATION_REVISION_ID\" AND Q.\"AUTHORITY_ID\"=? AND Q.\"OWNER_ID\"=L.\"OWNER_ID\" AND Q.\"FLOW_INCARNATION_ID\"=L.\"FLOW_INCARNATION_ID\" AND Q.\"GENERATION\"=L.\"GENERATION\" AND Q.\"ACQUISITION_ID\"=L.\"ACQUISITION_ID\" AND Q.\"LEASE_EPOCH\"=L.\"LEASE_EPOCH\" AND Q.\"PROVISION_EPOCH\"=L.\"PROVISION_EPOCH\" AND Q.\"CATALOG_FINGERPRINT\"=L.\"CATALOG_FINGERPRINT\" AND Q.\"STATE\"='ACTIVE' AND Q.\"EXPIRES_AT\">CURRENT_TIMESTAMP()) AND EXISTS (SELECT 1 FROM " + managedSnowflakeStagedQualifiedTable(cfg, cfg.targetManifestTable) + " AS M WHERE M.\"DESTINATION_REVISION_ID\"=L.\"DESTINATION_REVISION_ID\" AND M.\"LOGICAL_BATCH_ID\"=? AND M.\"MANIFEST_HASH\"=? AND M.\"CONTENT_HASH\"=? AND M.\"FILE_CONTENT_HASH\"=? AND M.\"PLAN_HASH\"=? AND M.\"EXPECTED_ROW_COUNT\"=? AND M.\"PROVISION_EPOCH\"=L.\"PROVISION_EPOCH\" AND M.\"CATALOG_FINGERPRINT\"=L.\"CATALOG_FINGERPRINT\")"
	values := append(stagedReceiptValues(receipt), lease.leaseID, lease.ownerID, lease.provisionEpoch, lease.catalogFingerprint, claim.claimID, claim.logicalBatchID, claim.manifestHash, claim.contentHash, claim.fileContentHash, claim.planHash, claim.expectedRows)
	result, err := p.db.ExecContext(ctx, statement, values...)
	if err != nil {
		if isStagedDuplicateKey(err) {
			return stageReceiptInsert{inserted: false}, nil
		}
		return stageReceiptInsert{}, fmt.Errorf("%w: insert guarded staged Snowflake receipt: %w", connector.ErrDeliveryIndeterminate, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return stageReceiptInsert{}, err
	}
	if affected != 1 {
		return stageReceiptInsert{}, fmt.Errorf("%w: guarded staged Snowflake receipt insert affected %d rows", connector.ErrDeliveryIndeterminate, affected)
	}
	return stageReceiptInsert{inserted: true}, nil
}

func (p *sqlStageProtocol) InsertReleaseReceipt(ctx context.Context, cfg stagedConfig, lease stagedRuntimeLease, claim stagedLoadClaim, receipt managedStagedReceipt) (stageReceiptInsert, error) {
	return p.InsertLoadReceipt(ctx, cfg, lease, claim, receipt)
}

func (p *sqlStageProtocol) PromoteTarget(ctx context.Context, cfg stagedConfig, lease stagedRuntimeLease, claim stagedLoadClaim, rowHashes []string) error {
	if err := p.GuardCatalog(ctx, cfg, lease); err != nil {
		return err
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin staged Snowflake target promotion: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	authority := managedSnowflakeStagedQualifiedTable(cfg, cfg.authorityTable)
	// #nosec G202 -- authority is composed only from strictly validated Snowflake identifiers.
	guardSQL := "UPDATE " + authority + " AS L SET \"EXPIRES_AT\"=DATEADD('second',?,CURRENT_TIMESTAMP()),\"UPDATED_AT\"=CURRENT_TIMESTAMP() WHERE L.\"AUTHORITY_KIND\"='LEASE' AND L.\"DESTINATION_REVISION_ID\"=? AND L.\"AUTHORITY_ID\"=? AND L.\"OWNER_ID\"=? AND L.\"FLOW_INCARNATION_ID\"=? AND L.\"GENERATION\"=? AND L.\"ACQUISITION_ID\"=? AND L.\"LEASE_EPOCH\"=? AND L.\"PROVISION_EPOCH\"=? AND L.\"CATALOG_FINGERPRINT\"=? AND L.\"STATE\"='ACTIVE' AND L.\"EXPIRES_AT\">CURRENT_TIMESTAMP() AND EXISTS (SELECT 1 FROM " + authority + " AS C WHERE C.\"AUTHORITY_KIND\"='CATALOG' AND C.\"DESTINATION_REVISION_ID\"=L.\"DESTINATION_REVISION_ID\" AND C.\"PROVISION_EPOCH\"=L.\"PROVISION_EPOCH\" AND C.\"CATALOG_FINGERPRINT\"=L.\"CATALOG_FINGERPRINT\" AND C.\"STATE\"='CURRENT') AND EXISTS (SELECT 1 FROM " + authority + " AS Q WHERE Q.\"AUTHORITY_KIND\"='CLAIM' AND Q.\"DESTINATION_REVISION_ID\"=L.\"DESTINATION_REVISION_ID\" AND Q.\"AUTHORITY_ID\"=? AND Q.\"OWNER_ID\"=L.\"OWNER_ID\" AND Q.\"FLOW_INCARNATION_ID\"=L.\"FLOW_INCARNATION_ID\" AND Q.\"GENERATION\"=L.\"GENERATION\" AND Q.\"ACQUISITION_ID\"=L.\"ACQUISITION_ID\" AND Q.\"LEASE_EPOCH\"=L.\"LEASE_EPOCH\" AND Q.\"PROVISION_EPOCH\"=L.\"PROVISION_EPOCH\" AND Q.\"CATALOG_FINGERPRINT\"=L.\"CATALOG_FINGERPRINT\" AND Q.\"STATE\"='ACTIVE' AND Q.\"EXPIRES_AT\">CURRENT_TIMESTAMP())"
	guardResult, err := tx.ExecContext(ctx, guardSQL, int(stagedRuntimeLeaseDuration(cfg)/time.Second), lease.destinationRevision, lease.leaseID, lease.ownerID, lease.flowIncarnationID, lease.generation, lease.acquisitionID, lease.leaseEpoch, lease.provisionEpoch, lease.catalogFingerprint, claim.claimID)
	if err != nil {
		return fmt.Errorf("renew staged Snowflake promotion guard: %w", err)
	}
	if affected, err := guardResult.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("%w: staged Snowflake promotion guard lost authority", connector.ErrDeliveryIndeterminate)
	}
	// #nosec G202 -- the landing table is composed only from strictly validated identifiers.
	landingRows, err := tx.QueryContext(ctx, "SELECT \"RECORD_HASH\",COUNT(*) FROM "+managedSnowflakeStagedQualifiedTable(cfg, cfg.landingTable)+" WHERE \"DESTINATION_REVISION_ID\"=? AND \"LOGICAL_BATCH_ID\"=? GROUP BY \"RECORD_HASH\"", claim.destinationRevision, claim.logicalBatchID)
	if err != nil {
		return fmt.Errorf("observe staged Snowflake landing proof in promotion transaction: %w", err)
	}
	actual := make(map[string]int)
	for landingRows.Next() {
		var hash string
		var count int
		if err := landingRows.Scan(&hash, &count); err != nil {
			_ = landingRows.Close()
			return err
		}
		actual[hash] = count
	}
	if err := landingRows.Err(); err != nil {
		_ = landingRows.Close()
		return fmt.Errorf("iterate staged Snowflake landing proof: %w", err)
	}
	_ = landingRows.Close()
	landing := classifyStagedTarget(rowHashes, len(actual) > 0 || len(rowHashes) == 0, true, actual)
	if len(rowHashes) == 0 {
		landing.state = stagedTargetComplete
	}
	if landing.state != stagedTargetComplete || landing.rowCount != claim.expectedRows {
		return fmt.Errorf("%w: staged Snowflake transactional landing state=%d rows=%d want=%d: %s", connector.ErrDeliveryConflict, landing.state, landing.rowCount, claim.expectedRows, landing.detail)
	}
	if claim.expectedRows > 0 {
		columns := quoteColumns(stagedChangelogColumns())
		// #nosec G202 -- target and landing names are strictly validated identifiers.
		statement := "INSERT INTO " + managedSnowflakeStagedQualifiedTable(cfg, cfg.table) + " (" + columns + ") SELECT " + columns + " FROM " + managedSnowflakeStagedQualifiedTable(cfg, cfg.landingTable) + " WHERE \"DESTINATION_REVISION_ID\"=? AND \"LOGICAL_BATCH_ID\"=?"
		result, err := tx.ExecContext(ctx, statement, claim.destinationRevision, claim.logicalBatchID)
		if err != nil {
			return fmt.Errorf("promote staged Snowflake target rows: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != int64(claim.expectedRows) {
			return fmt.Errorf("%w: staged Snowflake target promotion affected %d rows, want %d", connector.ErrDeliveryIndeterminate, affected, claim.expectedRows)
		}
	}
	rowHashesJSON, err := json.Marshal(rowHashes)
	if err != nil {
		return err
	}
	// #nosec G202 -- the manifest table is composed only from strictly validated identifiers.
	manifestSQL := "INSERT INTO " + managedSnowflakeStagedQualifiedTable(cfg, cfg.targetManifestTable) + " (\"DESTINATION_REVISION_ID\",\"LOGICAL_BATCH_ID\",\"MANIFEST_HASH\",\"CONTENT_HASH\",\"FILE_CONTENT_HASH\",\"PLAN_HASH\",\"EXPECTED_ROW_COUNT\",\"ROW_HASHES_JSON\",\"PROVISION_EPOCH\",\"CATALOG_FINGERPRINT\",\"COMMITTED_AT\") VALUES (?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP())"
	manifestResult, err := tx.ExecContext(ctx, manifestSQL, claim.destinationRevision, claim.logicalBatchID, claim.manifestHash, claim.contentHash, claim.fileContentHash, claim.planHash, claim.expectedRows, string(rowHashesJSON), claim.provisionEpoch, claim.catalogFingerprint)
	if err != nil {
		return fmt.Errorf("insert staged Snowflake target manifest: %w", err)
	}
	if affected, err := manifestResult.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("%w: staged Snowflake target manifest insert affected %d rows", connector.ErrDeliveryIndeterminate, affected)
	}
	// #nosec G202 -- the landing table is composed only from strictly validated identifiers.
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+managedSnowflakeStagedQualifiedTable(cfg, cfg.landingTable)+" WHERE \"DESTINATION_REVISION_ID\"=? AND \"LOGICAL_BATCH_ID\"=?", claim.destinationRevision, claim.logicalBatchID); err != nil {
		return fmt.Errorf("clear staged Snowflake landing rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%w: commit staged Snowflake target promotion: %w", connector.ErrDeliveryIndeterminate, err)
	}
	return nil
}
