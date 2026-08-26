package snowflake

import (
	"errors"
	"fmt"
	"strings"
)

// streamReceiptColumns is the immutable receipt-table contract. Order is stable
// so INSERT, SELECT, and scan agree.
func streamReceiptColumns() []string {
	return []string{
		"RECEIPT_KIND", "PROFILE_VERSION", "FLOW_ID", "FLOW_INCARNATION_ID", "SOURCE_LINEAGE_ID",
		"DESTINATION_REVISION_ID", "LOGICAL_BATCH_ID", "POSITION_ID", "CONTENT_HASH", "SCHEMA_CONTRACT_HASH",
		"CATALOG_FINGERPRINT", "MANIFEST_HASH", "EXTERNAL_ID", "REQUEST_ID", "GENERATION", "ACQUISITION_ID", "LEASE_EPOCH",
		"TRANSACTION_ID", "FRAGMENT_COUNT", "RECORD_COUNT", "CHANNEL_NAME", "OFFSET_TOKEN", "PIPE_REVISION",
		"CHANNEL_REVISION", "COMMITTED_OFFSET_TOKEN", "ROWS_CONTENT_HASH", "RECEIPT_STATUS",
	}
}

func streamReceiptColumnsQualified(alias string) string {
	columns := streamReceiptColumns()
	qualified := make([]string, 0, len(columns))
	for _, column := range columns {
		qualified = append(qualified, alias+"."+quoteIdent(column, '"'))
	}
	return strings.Join(qualified, ", ")
}

func managedSnowflakeStreamQualifiedTable(cfg streamConfig, table string) string {
	return strings.Join([]string{quoteIdent(cfg.database, '"'), quoteIdent(cfg.schema, '"'), quoteIdent(table, '"')}, ".")
}

func streamReceiptLookupSQL(cfg streamConfig) string {
	table := managedSnowflakeStreamQualifiedTable(cfg, cfg.receiptsTable)
	return "SELECT " + quoteColumns(streamReceiptColumns()) + " FROM " + table +
		" WHERE (\"RECEIPT_KIND\" = ? AND \"FLOW_INCARNATION_ID\" = ? AND \"DESTINATION_REVISION_ID\" = ? AND \"LOGICAL_BATCH_ID\" = ?) OR " +
		"(\"RECEIPT_KIND\" = ? AND \"FLOW_INCARNATION_ID\" = ? AND \"DESTINATION_REVISION_ID\" = ? AND \"SOURCE_LINEAGE_ID\" = ? AND \"POSITION_ID\" = ?) OR " +
		"\"EXTERNAL_ID\" = ?"
}

func streamReceiptInsertSQL(cfg streamConfig) string {
	columns := streamReceiptColumns()
	return "INSERT INTO " + managedSnowflakeStreamQualifiedTable(cfg, cfg.receiptsTable) +
		" (" + quoteColumns(columns) + ", \"COMMITTED_AT\") VALUES (" + placeholders(len(columns)) + ", CURRENT_TIMESTAMP())"
}

func streamReceiptValues(receipt managedStreamReceipt) []any {
	return []any{
		receipt.kind, receipt.profileVersion, receipt.flowID, receipt.flowIncarnationID, receipt.sourceLineageID,
		receipt.destinationRevisionID, receipt.logicalBatchID, receipt.positionID, receipt.contentHash, receipt.schemaContractHash,
		receipt.catalogFingerprint, receipt.manifestHash, receipt.externalID, receipt.requestID, receipt.generation, receipt.acquisitionID, receipt.leaseEpoch,
		int64(receipt.transactionID), receipt.fragmentCount, receipt.recordCount, receipt.channelName, receipt.offsetToken, receipt.pipeRevision,
		receipt.channelRevision, receipt.committedOffsetToken, receipt.rowsContentHash, receipt.receiptStatus,
	}
}

type streamReceiptScanner interface {
	Scan(dest ...any) error
}

func scanStreamReceipt(rows streamReceiptScanner) (managedStreamReceipt, error) {
	var receipt managedStreamReceipt
	var transactionID int64
	if err := rows.Scan(
		&receipt.kind, &receipt.profileVersion, &receipt.flowID, &receipt.flowIncarnationID, &receipt.sourceLineageID,
		&receipt.destinationRevisionID, &receipt.logicalBatchID, &receipt.positionID, &receipt.contentHash, &receipt.schemaContractHash,
		&receipt.catalogFingerprint, &receipt.manifestHash, &receipt.externalID, &receipt.requestID, &receipt.generation, &receipt.acquisitionID, &receipt.leaseEpoch,
		&transactionID, &receipt.fragmentCount, &receipt.recordCount, &receipt.channelName, &receipt.offsetToken, &receipt.pipeRevision,
		&receipt.channelRevision, &receipt.committedOffsetToken, &receipt.rowsContentHash, &receipt.receiptStatus,
	); err != nil {
		return managedStreamReceipt{}, fmt.Errorf("scan streaming Snowflake receipt: %w", err)
	}
	if transactionID < 0 || transactionID > int64(^uint32(0)) {
		return managedStreamReceipt{}, errors.New("streaming Snowflake receipt transaction ID is out of range")
	}
	receipt.transactionID = uint32(transactionID) // #nosec G115 -- range checked above.
	return receipt, nil
}

// streamChannelStateColumns is the immutable channel-state-table contract. The
// channel identity is the primary key; the revision and token columns are the
// persisted evidence.
func streamChannelStateColumns() []string {
	return []string{
		"FLOW_INCARNATION_ID", "DESTINATION_REVISION_ID", "CHANNEL_NAME", "PIPE_NAME", "PIPE_REVISION",
		"CHANNEL_REVISION", "CONTINUATION_TOKEN", "COMMITTED_OFFSET_TOKEN", "LOGICAL_BATCH_ID", "ROWS_CONTENT_HASH", "STATE_VERSION",
	}
}

func streamChannelStateValues(state managedStreamChannelState) []any {
	return []any{
		state.flowIncarnationID, state.destinationRevisionID, state.channelName, state.pipeName, state.pipeRevision,
		state.channelRevision, state.continuationToken, state.committedOffsetToken, state.logicalBatchID, state.rowsContentHash, state.stateVersion,
	}
}

// streamChannelStateMergeSQL performs a compare-and-swap. Identity and token
// evidence can only move to a larger state version and never to an older channel
// revision.
func streamChannelStateMergeSQL(cfg streamConfig) string {
	table := managedSnowflakeStreamQualifiedTable(cfg, cfg.channelStateTable)
	return "MERGE INTO " + table + " AS T USING (SELECT ? AS \"FLOW_INCARNATION_ID\", ? AS \"DESTINATION_REVISION_ID\", ? AS \"CHANNEL_NAME\", ? AS \"PIPE_NAME\", ? AS \"PIPE_REVISION\", ? AS \"CHANNEL_REVISION\", ? AS \"CONTINUATION_TOKEN\", ? AS \"COMMITTED_OFFSET_TOKEN\", ? AS \"LOGICAL_BATCH_ID\", ? AS \"ROWS_CONTENT_HASH\", ? AS \"STATE_VERSION\", ? AS \"EXPECTED_VERSION\", ? AS \"EXPECTED_CHANNEL_REVISION\", ? AS \"EXPECTED_CONTINUATION_TOKEN\", ? AS \"EXPECTED_COMMITTED_OFFSET_TOKEN\", ? AS \"EXPECTED_LOGICAL_BATCH_ID\", ? AS \"EXPECTED_ROWS_CONTENT_HASH\") AS S" +
		" ON T.\"FLOW_INCARNATION_ID\" = S.\"FLOW_INCARNATION_ID\" AND T.\"DESTINATION_REVISION_ID\" = S.\"DESTINATION_REVISION_ID\" AND T.\"CHANNEL_NAME\" = S.\"CHANNEL_NAME\"" +
		" WHEN MATCHED AND T.\"STATE_VERSION\" = S.\"EXPECTED_VERSION\" AND T.\"CHANNEL_REVISION\" = S.\"EXPECTED_CHANNEL_REVISION\" AND T.\"CONTINUATION_TOKEN\" = S.\"EXPECTED_CONTINUATION_TOKEN\" AND T.\"COMMITTED_OFFSET_TOKEN\" = S.\"EXPECTED_COMMITTED_OFFSET_TOKEN\" AND T.\"LOGICAL_BATCH_ID\" = S.\"EXPECTED_LOGICAL_BATCH_ID\" AND T.\"ROWS_CONTENT_HASH\" = S.\"EXPECTED_ROWS_CONTENT_HASH\" AND S.\"STATE_VERSION\" = T.\"STATE_VERSION\" + 1 AND S.\"CHANNEL_REVISION\" >= T.\"CHANNEL_REVISION\" THEN UPDATE SET \"PIPE_NAME\" = S.\"PIPE_NAME\", \"PIPE_REVISION\" = S.\"PIPE_REVISION\", \"CHANNEL_REVISION\" = S.\"CHANNEL_REVISION\", \"CONTINUATION_TOKEN\" = S.\"CONTINUATION_TOKEN\", \"COMMITTED_OFFSET_TOKEN\" = S.\"COMMITTED_OFFSET_TOKEN\", \"LOGICAL_BATCH_ID\" = S.\"LOGICAL_BATCH_ID\", \"ROWS_CONTENT_HASH\" = S.\"ROWS_CONTENT_HASH\", \"STATE_VERSION\" = S.\"STATE_VERSION\", \"UPDATED_AT\" = CURRENT_TIMESTAMP()" +
		" WHEN NOT MATCHED AND S.\"EXPECTED_VERSION\" = 0 AND S.\"STATE_VERSION\" = 1 THEN INSERT (" + quoteColumns(streamChannelStateColumns()) + ", \"UPDATED_AT\") VALUES (S.\"FLOW_INCARNATION_ID\", S.\"DESTINATION_REVISION_ID\", S.\"CHANNEL_NAME\", S.\"PIPE_NAME\", S.\"PIPE_REVISION\", S.\"CHANNEL_REVISION\", S.\"CONTINUATION_TOKEN\", S.\"COMMITTED_OFFSET_TOKEN\", S.\"LOGICAL_BATCH_ID\", S.\"ROWS_CONTENT_HASH\", S.\"STATE_VERSION\", CURRENT_TIMESTAMP())"
}

func streamChannelStateLookupSQL(cfg streamConfig) string {
	table := managedSnowflakeStreamQualifiedTable(cfg, cfg.channelStateTable)
	return "SELECT " + quoteColumns(streamChannelStateColumns()) + " FROM " + table +
		" WHERE \"FLOW_INCARNATION_ID\" = ? AND \"DESTINATION_REVISION_ID\" = ? AND \"CHANNEL_NAME\" = ?"
}

func streamChannelStateDeleteCASSQL(cfg streamConfig) string {
	table := managedSnowflakeStreamQualifiedTable(cfg, cfg.channelStateTable)
	return "DELETE FROM " + table +
		" WHERE \"FLOW_INCARNATION_ID\" = ? AND \"DESTINATION_REVISION_ID\" = ? AND \"CHANNEL_NAME\" = ?" +
		" AND \"STATE_VERSION\" = ? AND \"CHANNEL_REVISION\" = ? AND \"CONTINUATION_TOKEN\" = ? AND \"COMMITTED_OFFSET_TOKEN\" = ? AND \"LOGICAL_BATCH_ID\" = ? AND \"ROWS_CONTENT_HASH\" = ?" +
		" AND NOT EXISTS (SELECT 1 FROM " + streamRequestTable(cfg) + " WHERE \"FLOW_INCARNATION_ID\" = ? AND \"DESTINATION_REVISION_ID\" = ? AND \"CHANNEL_NAME\" = ? AND \"PHASE\" IN ('PREPARED','SENDING_UNKNOWN','ACCEPTED','COMMITTED'))"
}

func scanStreamChannelState(row streamReceiptScanner) (managedStreamChannelState, error) {
	var state managedStreamChannelState
	if err := row.Scan(
		&state.flowIncarnationID, &state.destinationRevisionID, &state.channelName, &state.pipeName, &state.pipeRevision,
		&state.channelRevision, &state.continuationToken, &state.committedOffsetToken, &state.logicalBatchID, &state.rowsContentHash, &state.stateVersion,
	); err != nil {
		return managedStreamChannelState{}, err
	}
	return state, nil
}
