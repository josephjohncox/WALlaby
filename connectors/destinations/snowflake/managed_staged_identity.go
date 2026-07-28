package snowflake

import (
	"crypto/md5" //nolint:gosec // G501: Snowflake internal-stage objects are checksummed with MD5; wallaby mirrors that checksum to detect wrong-byte collisions, not for any security control.
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// stagedRetentionRoot is the fixed stage-object prefix for the staged COPY
// append profile. Every object a flow incarnation stages lives beneath a single
// deterministic root so bounded cleanup can enumerate a retention scope without
// consulting any external index.
const stagedRetentionRoot = "wallaby_staged_append_v1"

// stagedFileExtension is the immutable object suffix. The internal stage file
// format is newline-delimited JSON so COPY can MATCH_BY_COLUMN_NAME.
const stagedFileExtension = ".ndjson"

// managedStagedIdentity is the immutable stage-object identity derived only from
// the flow incarnation, destination revision, logical batch, deterministic COPY
// plan, and logical content hash. Two producers that replay the same committed
// transaction under the same admitted destination compute the same path and the
// same bytes; any divergence is a wrong-byte collision that fails closed.
type managedStagedIdentity struct {
	flowIncarnationID     string
	destinationRevisionID string
	logicalBatchID        string
	planHash              string
	contentHash           string
	retentionRoot         string
	incarnationRoot       string
	objectDir             string
	relativePath          string
	manifestHash          string
	externalID            string
}

// stagedChangelogColumns is the immutable append-only changelog contract loaded
// into the staged target. Order is stable so the file bytes, target column list,
// and plan hash are reproducible.
func stagedChangelogColumns() []string {
	return []string{
		"FLOW_ID", "FLOW_INCARNATION_ID", "SOURCE_LINEAGE_ID", "DESTINATION_REVISION_ID",
		"LOGICAL_BATCH_ID", "CONTENT_HASH", "SOURCE_POSITION", "TRANSACTION_ID",
		"BEGIN_LSN", "COMMIT_LSN", "END_LSN", "FRAGMENT_ORDINAL", "RECORD_ORDINAL",
		"SOURCE_NAMESPACE", "SOURCE_TABLE", "SCHEMA_CONTRACT_HASH", "OPERATION", "TOMBSTONE",
		"KEY_JSON", "BEFORE_IMAGE", "AFTER_IMAGE", "EVENT_TIME", "RECORD_HASH",
	}
}

// stagedChangelogRow is one immutable changelog line. Field order matches the
// column order above; encoding/json emits struct fields in declaration order so
// the serialized file bytes are deterministic.
type stagedChangelogRow struct {
	FlowID                string         `json:"FLOW_ID"`
	FlowIncarnationID     string         `json:"FLOW_INCARNATION_ID"`
	SourceLineageID       string         `json:"SOURCE_LINEAGE_ID"`
	DestinationRevisionID string         `json:"DESTINATION_REVISION_ID"`
	LogicalBatchID        string         `json:"LOGICAL_BATCH_ID"`
	ContentHash           string         `json:"CONTENT_HASH"`
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
	EventTime             string         `json:"EVENT_TIME"`
	RecordHash            string         `json:"RECORD_HASH"`
}

// serializeStagedFile renders the ordered changelog rows into newline-delimited
// JSON and returns the immutable bytes with their content hash (sha256) and the
// Snowflake-compatible MD5 stage checksum. A trailing newline is always present
// so byte identity is unambiguous.
func serializeStagedFile(rows []stagedChangelogRow) ([]byte, string, string, error) {
	var builder strings.Builder
	for index := range rows {
		encoded, err := json.Marshal(rows[index])
		if err != nil {
			return nil, "", "", fmt.Errorf("encode staged changelog row %d: %w", index, err)
		}
		if bytesContainsControl(encoded) {
			return nil, "", "", fmt.Errorf("staged changelog row %d contains an unescaped control byte", index)
		}
		builder.Write(encoded)
		builder.WriteByte('\n')
	}
	content := []byte(builder.String())
	return content, stagedContentHash(content), stagedFileMD5(content), nil
}

func bytesContainsControl(encoded []byte) bool {
	for _, character := range encoded {
		if character == '\n' || character == '\r' {
			return true
		}
	}
	return false
}

func stagedContentHash(content []byte) string {
	digest := sha256.Sum256(content)
	return hex.EncodeToString(digest[:])
}

func stagedFileMD5(content []byte) string {
	sum := md5.Sum(content) //nolint:gosec // G401: MD5 reproduces Snowflake's staged-object checksum for wrong-byte collision detection, not a security control.
	return hex.EncodeToString(sum[:])
}

// canonicalStagedValue normalizes a decoded source value into a stable,
// JSON-encodable form so identical logical content always serializes to
// identical bytes: times become RFC3339 nanosecond UTC strings and raw bytes
// become base64. json.Number is preserved so integers and decimals keep their
// exact textual form.
func canonicalStagedValue(value any) (any, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil //nolint:nilnil // nil maps to a JSON null cell.
	case json.Number:
		return typed, nil
	case string:
		return typed, nil
	case bool:
		return typed, nil
	case []byte:
		return "base64:" + base64.StdEncoding.EncodeToString(typed), nil
	case time.Time:
		if typed.Year() < 1 || typed.Year() > 9999 {
			return nil, fmt.Errorf("staged timestamp year %d is outside the admitted range 1-9999", typed.Year())
		}
		return typed.UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano), nil
	case int:
		return json.Number(strconv.FormatInt(int64(typed), 10)), nil
	case int32:
		return json.Number(strconv.FormatInt(int64(typed), 10)), nil
	case int64:
		return json.Number(strconv.FormatInt(typed, 10)), nil
	case float64:
		return nil, errors.New("staged append rejects lossy float64 source cells; decode numerics as json.Number")
	default:
		return nil, fmt.Errorf("staged append cannot canonicalize source value of type %T", value)
	}
}

// stagedRecordHash binds every immutable field of one changelog row so a replay
// that reconstructs the same logical row also reconstructs the same hash.
func stagedRecordHash(row stagedChangelogRow) (string, error) {
	row.RecordHash = ""
	encoded, err := json.Marshal(row)
	if err != nil {
		return "", fmt.Errorf("encode staged changelog row for hashing: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// stagedPlanHash binds the deterministic COPY plan (target, stage, file format,
// ordered columns, and fail-closed load options) so any change to how a batch is
// loaded changes the stage-object identity.
func stagedPlanHash(plan stagedCopyPlan) string {
	encoded, _ := json.Marshal(struct {
		Profile       string            `json:"profile"`
		Target        string            `json:"target"`
		StageRef      string            `json:"stage_ref"`
		FileFormatRef string            `json:"file_format_ref"`
		Columns       []string          `json:"columns"`
		LoadOptions   map[string]string `json:"load_options"`
	}{
		Profile: connector.ManagedProfilePostgresToSnowflakeStagedAppendV1,
		Target:  plan.target, StageRef: plan.stageRef, FileFormatRef: plan.fileFormatRef,
		Columns: plan.columns, LoadOptions: plan.loadOptions,
	})
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

// newManagedStagedIdentity derives the immutable stage-object identity. The path
// is a pure function of the delivery identity, plan hash, and content hash so it
// is reproducible across producers and process restarts.
func newManagedStagedIdentity(cfg stagedConfig, intent connector.DeliveryIntent, planHash, contentHash string) (managedStagedIdentity, error) {
	for name, value := range map[string]string{
		"flow_incarnation_id":     intent.FlowIncarnationID,
		"destination_revision_id": intent.DestinationRevisionID,
		"logical_batch_id":        intent.LogicalBatchID,
	} {
		if strings.TrimSpace(value) == "" {
			return managedStagedIdentity{}, fmt.Errorf("staged identity requires %s", name)
		}
	}
	if !stagedIsLowerHex(planHash, 64) {
		return managedStagedIdentity{}, errors.New("staged identity requires a 64-character lowercase hexadecimal plan hash")
	}
	if !stagedIsLowerHex(contentHash, 64) {
		return managedStagedIdentity{}, errors.New("staged identity requires a 64-character lowercase hexadecimal content hash")
	}
	incarnationSegment := stagedPathSegment(intent.FlowIncarnationID)
	revisionSegment := stagedPathSegment(intent.DestinationRevisionID)
	batchSegment := stagedPathSegment(intent.LogicalBatchID)
	incarnationRoot := stagedRetentionRoot + "/" + incarnationSegment
	objectDir := strings.Join([]string{incarnationRoot, revisionSegment, batchSegment}, "/")
	objectName := planHash[:16] + "-" + contentHash + stagedFileExtension
	relativePath := objectDir + "/" + objectName
	manifestHash := stagedDestinationManifestHash(cfg, intent, planHash, contentHash)
	return managedStagedIdentity{
		flowIncarnationID: intent.FlowIncarnationID, destinationRevisionID: intent.DestinationRevisionID,
		logicalBatchID: intent.LogicalBatchID, planHash: planHash, contentHash: contentHash,
		retentionRoot: stagedRetentionRoot, incarnationRoot: incarnationRoot, objectDir: objectDir,
		relativePath: relativePath, manifestHash: manifestHash, externalID: "sf-staged:v1:" + manifestHash,
	}, nil
}

// stagedPathSegment renders one identity component as a collision-free stage
// path segment. High-entropy identities are hashed so the path never depends on
// characters a stage rejects and never exceeds Snowflake's path limits, while a
// short human-readable prefix aids operator inspection.
func stagedPathSegment(value string) string {
	digest := sha256.Sum256([]byte(value))
	prefix := make([]rune, 0, len(value))
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '_' || character == '-' {
			prefix = append(prefix, character)
		} else {
			prefix = append(prefix, '_')
		}
		if len(prefix) >= 24 {
			break
		}
	}
	return string(prefix) + "_" + hex.EncodeToString(digest[:8])
}

func stagedIsLowerHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func stagedDestinationManifestHash(cfg stagedConfig, intent connector.DeliveryIntent, planHash, contentHash string) string {
	encoded, _ := json.Marshal(struct {
		Profile             string            `json:"profile"`
		FlowID              string            `json:"flow_id"`
		Account             string            `json:"account"`
		Database            string            `json:"database"`
		Schema              string            `json:"schema"`
		Stage               string            `json:"stage"`
		Table               string            `json:"table"`
		ReceiptsTable       string            `json:"receipts_table"`
		FileFormat          string            `json:"file_format"`
		Pipe                string            `json:"pipe"`
		AutoIngest          bool              `json:"auto_ingest"`
		OwnerRole           string            `json:"owner_role"`
		ExecutionRole       string            `json:"execution_role"`
		Warehouse           string            `json:"warehouse"`
		SnowflakeVersion    string            `json:"snowflake_version"`
		StageCreatedOn      string            `json:"stage_created_on"`
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
	}{
		Profile: cfg.profile, FlowID: cfg.flowID, Account: cfg.account, Database: cfg.database, Schema: cfg.schema,
		Stage: cfg.stage, Table: cfg.table, ReceiptsTable: cfg.receiptsTable, FileFormat: cfg.fileFormat,
		Pipe: cfg.pipe, AutoIngest: cfg.autoIngest, OwnerRole: cfg.ownerRole, ExecutionRole: cfg.executionRole,
		Warehouse: cfg.warehouse, SnowflakeVersion: cfg.snowflakeVersion, StageCreatedOn: cfg.stageCreatedOn,
		TargetCreatedOn: cfg.targetCreatedOn, ReceiptsCreatedOn: cfg.receiptsCreatedOn,
		SourceSchema: cfg.sourceSchema, SourceTable: cfg.sourceTable, SchemaContractHash: cfg.schemaContractHash,
		TypeMappings: cfg.typeMappings, MaxTransactionRows: cfg.maxTransactionRows, MaxTransactionBytes: cfg.maxTransactionBytes,
		MaxFragments: cfg.maxFragments, DestinationRevision: intent.DestinationRevisionID, FlowIncarnationID: intent.FlowIncarnationID,
		SourceLineageID: intent.SourceLineageID, LogicalBatchID: intent.LogicalBatchID, PositionID: intent.PositionID,
		ContentHash: contentHash, PlanHash: planHash,
	})
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}
