package snowflake

import (
	"errors"
	"fmt"
	"strings"
)

// stagedReceiptColumns is the immutable receipt-table contract. Order is stable
// so INSERT, SELECT, and scan agree.
func stagedReceiptColumns() []string {
	return []string{
		"RECEIPT_KIND", "PROFILE_VERSION", "FLOW_ID", "FLOW_INCARNATION_ID", "SOURCE_LINEAGE_ID",
		"DESTINATION_REVISION_ID", "LOGICAL_BATCH_ID", "POSITION_ID", "CONTENT_HASH", "SCHEMA_CONTRACT_HASH",
		"CATALOG_FINGERPRINT", "MANIFEST_HASH", "EXTERNAL_ID", "GENERATION", "ACQUISITION_ID", "LEASE_EPOCH",
		"TRANSACTION_ID", "FRAGMENT_COUNT", "RECORD_COUNT", "STAGE_NAME", "STAGE_PATH", "FILE_CONTENT_HASH",
		"FILE_MD5", "LOAD_ROW_COUNT", "LOAD_STATUS",
	}
}

func stagedReceiptColumnsQualified(alias string) string {
	columns := stagedReceiptColumns()
	qualified := make([]string, 0, len(columns))
	for _, column := range columns {
		qualified = append(qualified, alias+"."+quoteIdent(column, '"'))
	}
	return strings.Join(qualified, ", ")
}

func managedSnowflakeStagedQualifiedTable(cfg stagedConfig, table string) string {
	return strings.Join([]string{quoteIdent(cfg.database, '"'), quoteIdent(cfg.schema, '"'), quoteIdent(table, '"')}, ".")
}

func stagedReceiptLookupSQL(cfg stagedConfig) string {
	table := managedSnowflakeStagedQualifiedTable(cfg, cfg.receiptsTable)
	return "SELECT " + quoteColumns(stagedReceiptColumns()) + " FROM " + table +
		" WHERE (\"RECEIPT_KIND\" = ? AND \"FLOW_INCARNATION_ID\" = ? AND \"DESTINATION_REVISION_ID\" = ? AND \"LOGICAL_BATCH_ID\" = ?) OR " +
		"(\"RECEIPT_KIND\" = ? AND \"FLOW_INCARNATION_ID\" = ? AND \"DESTINATION_REVISION_ID\" = ? AND \"SOURCE_LINEAGE_ID\" = ? AND \"POSITION_ID\" = ?) OR " +
		"\"EXTERNAL_ID\" = ?"
}

func stagedReceiptInsertSQL(cfg stagedConfig) string {
	columns := stagedReceiptColumns()
	return "INSERT INTO " + managedSnowflakeStagedQualifiedTable(cfg, cfg.receiptsTable) +
		" (" + quoteColumns(columns) + ", \"COMMITTED_AT\") VALUES (" + placeholders(len(columns)) + ", CURRENT_TIMESTAMP())"
}

func stagedReceiptValues(receipt managedStagedReceipt) []any {
	return []any{
		receipt.kind, receipt.profileVersion, receipt.flowID, receipt.flowIncarnationID, receipt.sourceLineageID,
		receipt.destinationRevisionID, receipt.logicalBatchID, receipt.positionID, receipt.contentHash, receipt.schemaContractHash,
		receipt.catalogFingerprint, receipt.manifestHash, receipt.externalID, receipt.generation, receipt.acquisitionID, receipt.leaseEpoch,
		int64(receipt.transactionID), receipt.fragmentCount, receipt.recordCount, receipt.stageName, receipt.stagePath, receipt.fileContentHash,
		receipt.fileMD5, receipt.loadRowCount, receipt.loadStatus,
	}
}

type stagedReceiptScanner interface {
	Scan(dest ...any) error
}

func scanStagedReceipt(rows stagedReceiptScanner) (managedStagedReceipt, error) {
	var receipt managedStagedReceipt
	var transactionID int64
	if err := rows.Scan(
		&receipt.kind, &receipt.profileVersion, &receipt.flowID, &receipt.flowIncarnationID, &receipt.sourceLineageID,
		&receipt.destinationRevisionID, &receipt.logicalBatchID, &receipt.positionID, &receipt.contentHash, &receipt.schemaContractHash,
		&receipt.catalogFingerprint, &receipt.manifestHash, &receipt.externalID, &receipt.generation, &receipt.acquisitionID, &receipt.leaseEpoch,
		&transactionID, &receipt.fragmentCount, &receipt.recordCount, &receipt.stageName, &receipt.stagePath, &receipt.fileContentHash,
		&receipt.fileMD5, &receipt.loadRowCount, &receipt.loadStatus,
	); err != nil {
		return managedStagedReceipt{}, fmt.Errorf("scan staged Snowflake receipt: %w", err)
	}
	if transactionID < 0 || transactionID > int64(^uint32(0)) {
		return managedStagedReceipt{}, errors.New("staged Snowflake receipt transaction ID is out of range")
	}
	receipt.transactionID = uint32(transactionID) // #nosec G115 -- range checked above.
	return receipt, nil
}

// stagedCopyStatement renders the deterministic, fail-closed COPY. The absence
// of any ON_ERROR continuation is the load-safety invariant: no partial file is
// ever accepted. MATCH_BY_COLUMN_NAME maps the changelog JSON keys onto the
// admitted target columns by exact name.
func stagedCopyStatement(plan stagedCopyPlan) string {
	var builder strings.Builder
	builder.WriteString("COPY INTO ")
	builder.WriteString(plan.target)
	builder.WriteString(" FROM @")
	builder.WriteString(plan.stageRef)
	builder.WriteString("/")
	builder.WriteString(plan.relativePath)
	builder.WriteString(" FILE_FORMAT = (FORMAT_NAME = ")
	builder.WriteString(plan.fileFormatRef)
	builder.WriteString(")")
	builder.WriteString(" MATCH_BY_COLUMN_NAME = ")
	builder.WriteString(plan.loadOptions["MATCH_BY_COLUMN_NAME"])
	builder.WriteString(" ON_ERROR = ")
	builder.WriteString(plan.loadOptions["ON_ERROR"])
	builder.WriteString(" FORCE = ")
	builder.WriteString(plan.loadOptions["FORCE"])
	builder.WriteString(" PURGE = ")
	builder.WriteString(plan.loadOptions["PURGE"])
	return builder.String()
}
