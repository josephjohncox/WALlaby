package snowflake

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// managedStreamIdentity is the immutable append identity derived only from the
// flow incarnation, destination revision, logical batch, deterministic append
// plan, and logical content hash. Two producers that replay the same committed
// transaction under the same admitted destination compute the same channel
// name, the same offset token, and the same per-row identities; any divergence
// is a conflict that fails closed.
type managedStreamIdentity struct {
	flowIncarnationID     string
	destinationRevisionID string
	logicalBatchID        string
	planHash              string
	contentHash           string
	channelName           string
	offsetToken           string
	manifestHash          string
	externalID            string
}

// streamChangelogColumns is the immutable append-only changelog contract loaded
// into the streaming target. Order is stable so the append payload, target
// column list, and plan hash are reproducible. ROW_HASH is the deterministic
// per-row identity used for SQL-observed completeness; APPEND_ORDINAL preserves
// the total order of rows within one logical batch.
func streamChangelogColumns() []string {
	return []string{
		"FLOW_ID", "FLOW_INCARNATION_ID", "SOURCE_LINEAGE_ID", "DESTINATION_REVISION_ID",
		"LOGICAL_BATCH_ID", "CONTENT_HASH", "OFFSET_TOKEN", "APPEND_ORDINAL", "SOURCE_POSITION",
		"TRANSACTION_ID", "BEGIN_LSN", "COMMIT_LSN", "END_LSN", "FRAGMENT_ORDINAL", "RECORD_ORDINAL",
		"SOURCE_NAMESPACE", "SOURCE_TABLE", "SCHEMA_CONTRACT_HASH", "OPERATION", "TOMBSTONE",
		"KEY_JSON", "BEFORE_IMAGE", "AFTER_IMAGE", "UNCHANGED_TOAST", "EVENT_TIME", "ROW_HASH",
	}
}

// streamChangelogRow is one immutable append row. Field order matches the column
// order above; encoding/json emits struct fields in declaration order so the
// serialized payload bytes are deterministic.
type streamChangelogRow struct {
	FlowID                string         `json:"FLOW_ID"`
	FlowIncarnationID     string         `json:"FLOW_INCARNATION_ID"`
	SourceLineageID       string         `json:"SOURCE_LINEAGE_ID"`
	DestinationRevisionID string         `json:"DESTINATION_REVISION_ID"`
	LogicalBatchID        string         `json:"LOGICAL_BATCH_ID"`
	ContentHash           string         `json:"CONTENT_HASH"`
	OffsetToken           string         `json:"OFFSET_TOKEN"`
	AppendOrdinal         uint64         `json:"APPEND_ORDINAL"`
	SourcePosition        string         `json:"SOURCE_POSITION"`
	TransactionID         uint64         `json:"TRANSACTION_ID"`
	BeginLSN              string         `json:"BEGIN_LSN"`
	CommitLSN             string         `json:"COMMIT_LSN"`
	EndLSN                string         `json:"END_LSN"`
	FragmentOrdinal       uint64         `json:"FRAGMENT_ORDINAL"`
	RecordOrdinal         uint64         `json:"RECORD_ORDINAL"`
	SourceNamespace       string         `json:"SOURCE_NAMESPACE"`
	SourceTable           string         `json:"SOURCE_TABLE"`
	SchemaContractHash    string         `json:"SCHEMA_CONTRACT_HASH"`
	Operation             string         `json:"OPERATION"`
	Tombstone             bool           `json:"TOMBSTONE"`
	KeyJSON               map[string]any `json:"KEY_JSON"`
	BeforeImage           map[string]any `json:"BEFORE_IMAGE"`
	AfterImage            map[string]any `json:"AFTER_IMAGE"`
	UnchangedToast        []string       `json:"UNCHANGED_TOAST"`
	EventTime             string         `json:"EVENT_TIME"`
	RowHash               string         `json:"ROW_HASH"`

	// payload is the deterministic JSON body handed to the append transport. It is
	// unexported so encoding/json ignores it during hashing and never round-trips.
	payload []byte
}

// payloadBytes returns the deterministic append payload, materializing it on
// demand when a row was reconstructed without one (for example in tests).
func (r streamChangelogRow) payloadBytes() []byte {
	if len(r.payload) != 0 {
		return r.payload
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		return nil
	}
	return encoded
}

// streamRecordHash binds every immutable field of one append row (including the
// deterministic offset token and append ordinal, but excluding the hash itself)
// so a replay that reconstructs the same logical row also reconstructs the same
// identity. This hash — not any transport token — is the SQL-observed identity
// that gates completeness.
func streamRecordHash(row streamChangelogRow) (string, error) {
	row.RowHash = ""
	encoded, err := json.Marshal(row)
	if err != nil {
		return "", fmt.Errorf("encode streaming append row for hashing: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// streamRowsContentHash binds the ordered append rows so the append plan and the
// destination manifest hash change whenever any row content or order changes.
func streamRowsContentHash(rows []streamChangelogRow) (string, error) {
	hash := sha256.New()
	for index := range rows {
		encoded, err := json.Marshal(rows[index])
		if err != nil {
			return "", fmt.Errorf("encode streaming append row %d: %w", index, err)
		}
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(encoded)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write(encoded)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// streamAppendPlan is the deterministic append target specification.
type streamAppendPlan struct {
	target      string
	pipeRef     string
	channelName string
	columns     []string
}

// streamPlanHash binds the deterministic append plan (target, pipe, channel, and
// ordered columns) so any change to how a batch is appended changes the append
// identity.
func streamPlanHash(plan streamAppendPlan) string {
	encoded, _ := json.Marshal(struct {
		Profile     string   `json:"profile"`
		Target      string   `json:"target"`
		PipeRef     string   `json:"pipe_ref"`
		ChannelName string   `json:"channel_name"`
		Columns     []string `json:"columns"`
	}{
		Profile: connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1,
		Target:  plan.target, PipeRef: plan.pipeRef, ChannelName: plan.channelName, Columns: plan.columns,
	})
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

// streamChannelName derives the deterministic channel name for one flow
// incarnation and destination revision. Snowpipe Streaming channels are named,
// durable, and single-writer; deriving the name deterministically means a
// restarted writer reopens the same channel rather than orphaning committed
// rows behind an ephemeral name.
func streamChannelName(cfg streamConfig, intent connector.DeliveryIntent) string {
	prefix := cfg.channelNamePrefix
	if prefix == "" {
		prefix = "wallaby_stream"
	}
	incarnationSegment := stagedPathSegment(intent.FlowIncarnationID)
	revisionSegment := stagedPathSegment(intent.DestinationRevisionID)
	return strings.Join([]string{prefix, incarnationSegment, revisionSegment}, "__")
}

// streamOffsetToken derives the deterministic per-batch offset token. Snowpipe
// Streaming offset tokens are opaque per-channel strings; wallaby binds one to
// the logical batch so the committed offset token observed after append is
// evidence — but never the sole proof — of that batch's arrival.
func streamOffsetToken(intent connector.DeliveryIntent) string {
	digest := sha256.Sum256([]byte("off:v1:" + intent.FlowIncarnationID + "\x00" + intent.DestinationRevisionID + "\x00" + intent.LogicalBatchID))
	return "off:v1:" + hex.EncodeToString(digest[:])
}

// newManagedStreamIdentity derives the immutable append identity. Every field is
// a pure function of the delivery identity, plan hash, and content hash so it is
// reproducible across producers and process restarts.
func newManagedStreamIdentity(cfg streamConfig, intent connector.DeliveryIntent, plan streamAppendPlan, contentHash string) (managedStreamIdentity, error) {
	for name, value := range map[string]string{
		"flow_incarnation_id":     intent.FlowIncarnationID,
		"destination_revision_id": intent.DestinationRevisionID,
		"logical_batch_id":        intent.LogicalBatchID,
	} {
		if strings.TrimSpace(value) == "" {
			return managedStreamIdentity{}, fmt.Errorf("streaming identity requires %s", name)
		}
	}
	planHash := streamPlanHash(plan)
	if !streamIsLowerHex64(planHash) {
		return managedStreamIdentity{}, errors.New("streaming identity requires a 64-character lowercase hexadecimal plan hash")
	}
	if !streamIsLowerHex64(contentHash) {
		return managedStreamIdentity{}, errors.New("streaming identity requires a 64-character lowercase hexadecimal content hash")
	}
	offsetToken := streamOffsetToken(intent)
	manifestHash := streamDestinationManifestHash(cfg, intent, planHash, contentHash, offsetToken)
	return managedStreamIdentity{
		flowIncarnationID: intent.FlowIncarnationID, destinationRevisionID: intent.DestinationRevisionID,
		logicalBatchID: intent.LogicalBatchID, planHash: planHash, contentHash: contentHash,
		channelName: plan.channelName, offsetToken: offsetToken, manifestHash: manifestHash,
		externalID: "sf-streaming:v1:" + manifestHash,
	}, nil
}

// streamIsLowerHex64 reports whether value is a 64-character lowercase
// hexadecimal digest, the fixed width every streaming identity hash uses.
func streamIsLowerHex64(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func streamDestinationManifestHash(cfg streamConfig, intent connector.DeliveryIntent, planHash, contentHash, offsetToken string) string {
	encoded, _ := json.Marshal(struct {
		Profile             string            `json:"profile"`
		FlowID              string            `json:"flow_id"`
		Account             string            `json:"account"`
		Database            string            `json:"database"`
		Schema              string            `json:"schema"`
		Pipe                string            `json:"pipe"`
		Table               string            `json:"table"`
		ReceiptsTable       string            `json:"receipts_table"`
		ChannelStateTable   string            `json:"channel_state_table"`
		OwnerRole           string            `json:"owner_role"`
		ExecutionRole       string            `json:"execution_role"`
		Warehouse           string            `json:"warehouse"`
		SnowflakeVersion    string            `json:"snowflake_version"`
		PipeCreatedOn       string            `json:"pipe_created_on"`
		TargetCreatedOn     string            `json:"target_created_on"`
		ReceiptsCreatedOn   string            `json:"receipts_created_on"`
		SourceSchema        string            `json:"source_schema"`
		SourceTable         string            `json:"source_table"`
		SchemaContractHash  string            `json:"schema_contract_hash"`
		TypeMappings        map[string]string `json:"type_mappings"`
		MaxTransactionRows  int               `json:"max_transaction_rows"`
		MaxTransactionBytes int64             `json:"max_transaction_bytes"`
		MaxFragments        int               `json:"max_fragments"`
		DestinationRevision string            `json:"destination_revision"`
		FlowIncarnationID   string            `json:"flow_incarnation_id"`
		SourceLineageID     string            `json:"source_lineage_id"`
		LogicalBatchID      string            `json:"logical_batch_id"`
		PositionID          string            `json:"position_id"`
		ContentHash         string            `json:"content_hash"`
		PlanHash            string            `json:"plan_hash"`
		OffsetToken         string            `json:"offset_token"`
	}{
		Profile: cfg.profile, FlowID: cfg.flowID, Account: cfg.account, Database: cfg.database, Schema: cfg.schema,
		Pipe: cfg.pipe, Table: cfg.table, ReceiptsTable: cfg.receiptsTable, ChannelStateTable: cfg.channelStateTable,
		OwnerRole: cfg.ownerRole, ExecutionRole: cfg.executionRole, Warehouse: cfg.warehouse,
		SnowflakeVersion: cfg.snowflakeVersion, PipeCreatedOn: cfg.pipeCreatedOn, TargetCreatedOn: cfg.targetCreatedOn,
		ReceiptsCreatedOn: cfg.receiptsCreatedOn, SourceSchema: cfg.sourceSchema, SourceTable: cfg.sourceTable,
		SchemaContractHash: cfg.schemaContractHash, TypeMappings: cfg.typeMappings, MaxTransactionRows: cfg.maxTransactionRows,
		MaxTransactionBytes: cfg.maxTransactionBytes, MaxFragments: cfg.maxFragments, DestinationRevision: intent.DestinationRevisionID,
		FlowIncarnationID: intent.FlowIncarnationID, SourceLineageID: intent.SourceLineageID, LogicalBatchID: intent.LogicalBatchID,
		PositionID: intent.PositionID, ContentHash: contentHash, PlanHash: planHash, OffsetToken: offsetToken,
	})
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}
