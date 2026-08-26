package snowflake

import "strings"

func streamRequestTable(cfg streamConfig) string {
	return managedSnowflakeStreamQualifiedTable(cfg, cfg.channelStateTable+"_REQUESTS")
}

func streamRequestColumns() []string {
	return []string{
		"REQUEST_ID", "FLOW_ID", "FLOW_INCARNATION_ID", "SOURCE_LINEAGE_ID", "DESTINATION_REVISION_ID",
		"LOGICAL_BATCH_ID", "POSITION_ID", "CONTENT_HASH", "MANIFEST_HASH", "ROWS_CONTENT_HASH", "ROW_COUNT",
		"CHANNEL_NAME", "CHANNEL_REVISION", "PIPE_REVISION", "INPUT_CONTINUATION_TOKEN", "REQUESTED_OFFSET_TOKEN",
		"RESPONSE_CONTINUATION_TOKEN", "COMMITTED_OFFSET_TOKEN", "GENERATION", "ACQUISITION_ID", "LEASE_EPOCH",
		"ATTEMPT", "PHASE", "PHASE_VERSION", "RESPONSE_KIND", "RESPONSE_EVIDENCE",
	}
}

func streamRequestValues(r managedStreamRequest) []any {
	return []any{
		r.requestID, r.flowID, r.flowIncarnationID, r.sourceLineageID, r.destinationRevisionID,
		r.logicalBatchID, r.positionID, r.contentHash, r.manifestHash, r.rowsContentHash, r.rowCount,
		r.channelName, r.channelRevision, r.pipeRevision, r.inputContinuation, r.requestedOffset,
		r.responseContinuation, r.committedOffset, r.generation, r.acquisitionID, r.leaseEpoch,
		r.attempt, string(r.phase), r.phaseVersion, r.responseKind, r.responseEvidence,
	}
}

func streamRequestInsertSQL(cfg streamConfig) string {
	columns := streamRequestColumns()
	return "INSERT INTO " + streamRequestTable(cfg) + " (" + quoteColumns(columns) + ", \"CREATED_AT\", \"UPDATED_AT\") VALUES (" + placeholders(len(columns)) + ", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"
}

func streamRequestLookupSQL(cfg streamConfig) string {
	return "SELECT " + quoteColumns(streamRequestColumns()) + " FROM " + streamRequestTable(cfg) +
		" WHERE \"FLOW_INCARNATION_ID\" = ? AND \"DESTINATION_REVISION_ID\" = ? AND \"LOGICAL_BATCH_ID\" = ? ORDER BY \"ATTEMPT\" DESC LIMIT 2"
}

func streamRequestTransitionSQL(cfg streamConfig) string {
	return "UPDATE " + streamRequestTable(cfg) +
		" SET \"PHASE\" = ?, \"PHASE_VERSION\" = \"PHASE_VERSION\" + 1, \"RESPONSE_CONTINUATION_TOKEN\" = ?, \"COMMITTED_OFFSET_TOKEN\" = ?, \"RESPONSE_KIND\" = ?, \"RESPONSE_EVIDENCE\" = ?, \"UPDATED_AT\" = CURRENT_TIMESTAMP()" +
		" WHERE \"REQUEST_ID\" = ? AND \"PHASE\" = ? AND \"PHASE_VERSION\" = ?"
}

func streamRequestLookupByIDSQL(cfg streamConfig) string {
	return "SELECT " + quoteColumns(streamRequestColumns()) + " FROM " + streamRequestTable(cfg) + " WHERE \"REQUEST_ID\" = ?"
}

func streamRequestUnresolvedSQL(cfg streamConfig) string {
	phases := []string{string(streamRequestPrepared), string(streamRequestSendingUnknown), string(streamRequestAccepted), string(streamRequestCommitted)}
	quoted := make([]string, len(phases))
	for i := range phases {
		quoted[i] = "'" + phases[i] + "'"
	}
	return "SELECT COUNT(*) FROM " + streamRequestTable(cfg) +
		" WHERE \"FLOW_INCARNATION_ID\" = ? AND \"DESTINATION_REVISION_ID\" = ? AND \"CHANNEL_NAME\" = ? AND \"PHASE\" IN (" + strings.Join(quoted, ",") + ")"
}

func scanStreamRequest(row streamReceiptScanner) (managedStreamRequest, error) {
	var r managedStreamRequest
	var phase string
	if err := row.Scan(
		&r.requestID, &r.flowID, &r.flowIncarnationID, &r.sourceLineageID, &r.destinationRevisionID,
		&r.logicalBatchID, &r.positionID, &r.contentHash, &r.manifestHash, &r.rowsContentHash, &r.rowCount,
		&r.channelName, &r.channelRevision, &r.pipeRevision, &r.inputContinuation, &r.requestedOffset,
		&r.responseContinuation, &r.committedOffset, &r.generation, &r.acquisitionID, &r.leaseEpoch,
		&r.attempt, &phase, &r.phaseVersion, &r.responseKind, &r.responseEvidence,
	); err != nil {
		return managedStreamRequest{}, err
	}
	r.phase = streamRequestPhase(phase)
	return r, r.validateIdentity()
}
