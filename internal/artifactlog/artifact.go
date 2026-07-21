package artifactlog

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/wire"
)

const (
	ProjectionID                = "wallaby-canonical-parquet-arrow18-zstd3-us-v2"
	MaxEncodedObject            = 64 << 20
	DefaultMaxRecords           = 1_000_000
	DefaultMaxNesting           = 32
	DefaultMaxInput             = 256 << 20
	DefaultMaxFragments         = 128
	DefaultMaxTransactionEncode = 256 << 20
)

// CanonicalField records stable logical field identity independently of
// Parquet's physical column ordering.
type CanonicalField struct {
	ID       int32  `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

type canonicalSchema struct {
	ProjectionID string           `json:"projection_id"`
	Namespace    string           `json:"namespace"`
	Table        string           `json:"table"`
	Version      int64            `json:"version"`
	Fields       []CanonicalField `json:"fields"`
}

// Artifact is a deterministic local candidate; PostgreSQL reservation occurs
// before it may be uploaded.
type Artifact struct {
	ID                 string
	SchemaID           string
	SchemaJSON         []byte
	SourcePosition     string
	FragmentOrdinal    uint64
	LogicalContentHash string
	EncodedByteHash    string
	ChecksumSHA256     string
	Encoded            []byte
	ObjectKey          string
}

// Encoder freezes the canonical Parquet implementation version and bounds.
type Encoder struct {
	maxRecords            int
	maxInput              int64
	maxEncoded            int
	maxFragments          int
	maxTransactionEncoded int64
	maxNesting            int
}

func NewEncoder() *Encoder {
	return &Encoder{
		maxRecords:            DefaultMaxRecords,
		maxInput:              DefaultMaxInput,
		maxEncoded:            MaxEncodedObject,
		maxFragments:          DefaultMaxFragments,
		maxTransactionEncoded: DefaultMaxTransactionEncode,
		maxNesting:            DefaultMaxNesting,
	}
}

func (e *Encoder) EncodeTransaction(_ context.Context, incarnationID uuid.UUID, transaction connector.SourceTransaction) ([]Artifact, error) {
	checkpointLSN, err := connector.CanonicalizeCheckpointPosition(transaction.Checkpoint.LSN)
	if err != nil {
		return nil, errors.New("canonical artifacts require a committed transaction-end checkpoint")
	}
	endLSN, err := connector.CanonicalizeCheckpointPosition(transaction.EndLSN)
	if err != nil || checkpointLSN != endLSN {
		return nil, fmt.Errorf("canonical artifact checkpoint %q must equal transaction end %q", checkpointLSN, transaction.EndLSN)
	}
	if strings.TrimSpace(transaction.SourceLineageID) == "" || transaction.TransactionID == 0 || transaction.BeginLSN == "" || transaction.CommitLSN == "" {
		return nil, errors.New("canonical artifacts require source lineage, XID, begin LSN, and commit LSN")
	}
	beginLSN, err := connector.CanonicalizeCheckpointPosition(transaction.BeginLSN)
	if err != nil {
		return nil, fmt.Errorf("canonicalize transaction begin LSN: %w", err)
	}
	commitLSN, err := connector.CanonicalizeCheckpointPosition(transaction.CommitLSN)
	if err != nil {
		return nil, fmt.Errorf("canonicalize transaction commit LSN: %w", err)
	}
	transaction.BeginLSN = beginLSN
	transaction.CommitLSN = commitLSN
	transaction.EndLSN = endLSN
	transaction.Checkpoint.LSN = checkpointLSN
	if len(transaction.Fragments) == 0 || len(transaction.Fragments) > e.maxFragments {
		return nil, fmt.Errorf("canonical artifact fragment count %d outside 1..%d", len(transaction.Fragments), e.maxFragments)
	}
	codec, err := wire.NewCodec(string(connector.WireFormatParquet))
	if err != nil {
		return nil, err
	}
	artifacts := make([]Artifact, 0, len(transaction.Fragments))
	var totalInput, totalEncoded int64
	var totalRecords int
	for index, fragment := range transaction.Fragments {
		if fragment.Ordinal != uint64(index) {
			return nil, fmt.Errorf("fragment %d has non-deterministic ordinal %d", index, fragment.Ordinal)
		}
		batch := fragment.Batch
		if len(batch.Records) == 0 {
			return nil, fmt.Errorf("fragment %d is empty", index)
		}
		totalRecords += len(batch.Records)
		if totalRecords > e.maxRecords {
			return nil, fmt.Errorf("transaction has %d records, limit %d", totalRecords, e.maxRecords)
		}
		batch.Checkpoint = transaction.Checkpoint
		batch.WireFormat = connector.WireFormatParquet
		var schemaJSON []byte
		var schemaID string
		batch.Schema, schemaJSON, schemaID, err = canonicalizeSchema(transaction.SourceLineageID, batch.Schema)
		if err != nil {
			return nil, fmt.Errorf("canonicalize fragment %d schema: %w", index, err)
		}
		logicalHash, err := connector.BatchContentHash(batch)
		if err != nil {
			return nil, fmt.Errorf("hash fragment %d: %w", index, err)
		}
		inputEstimate, err := estimateBatchInput(batch, e.maxNesting)
		if err != nil {
			return nil, fmt.Errorf("measure fragment %d: %w", index, err)
		}
		totalInput += inputEstimate
		if totalInput > e.maxInput {
			return nil, fmt.Errorf("transaction uncompressed input %d exceeds limit %d", totalInput, e.maxInput)
		}
		encoded, err := codec.Encode(batch)
		if err != nil {
			return nil, fmt.Errorf("encode fragment %d parquet: %w", index, err)
		}
		if len(encoded) == 0 || len(encoded) > e.maxEncoded {
			return nil, fmt.Errorf("fragment %d encoded size %d outside 1..%d", index, len(encoded), e.maxEncoded)
		}
		totalEncoded += int64(len(encoded))
		if totalEncoded > e.maxTransactionEncoded {
			return nil, fmt.Errorf("transaction encoded bytes %d exceed limit %d", totalEncoded, e.maxTransactionEncoded)
		}
		encodedDigest := sha256.Sum256(encoded)
		artifactID := artifactIdentity(incarnationID, transaction, fragment.Ordinal, schemaID, logicalHash)
		artifacts = append(artifacts, Artifact{
			ID:                 artifactID,
			SchemaID:           schemaID,
			SchemaJSON:         schemaJSON,
			SourcePosition:     endLSN,
			FragmentOrdinal:    fragment.Ordinal,
			LogicalContentHash: logicalHash,
			EncodedByteHash:    hex.EncodeToString(encodedDigest[:]),
			ChecksumSHA256:     hex.EncodeToString(encodedDigest[:]),
			Encoded:            encoded,
			ObjectKey: fmt.Sprintf(
				"wallaby/artifacts/%s/lineage=%s/position=%s/fragment=%06d/%s.parquet",
				incarnationID,
				shortHash(transaction.SourceLineageID),
				strings.ReplaceAll(endLSN, "/", "_"),
				fragment.Ordinal,
				artifactID,
			),
		})
	}
	return artifacts, nil
}

const canonicalSystemFieldCount int32 = 7

var canonicalSystemFields = []CanonicalField{
	{ID: 1, Name: "__op", Type: "text", Nullable: false},
	{ID: 2, Name: "__ts", Type: "timestamp_us", Nullable: true},
	{ID: 3, Name: "__schema_version", Type: "bigint", Nullable: false},
	{ID: 4, Name: "__table", Type: "text", Nullable: true},
	{ID: 5, Name: "__namespace", Type: "text", Nullable: true},
	{ID: 6, Name: "__key", Type: "bytea", Nullable: true},
	{ID: 7, Name: "__before_json", Type: "bytea", Nullable: true},
}

func canonicalizeSchema(lineage string, schema connector.Schema) (connector.Schema, []byte, string, error) {
	canonical := canonicalSchema{
		ProjectionID: ProjectionID,
		Namespace:    schema.Namespace,
		Table:        schema.Name,
		Version:      schema.Version,
		Fields:       append([]CanonicalField(nil), canonicalSystemFields...),
	}
	result := schema
	result.Columns = append([]connector.Column(nil), schema.Columns...)
	seen := make(map[int32]string, len(schema.Columns)+len(canonicalSystemFields))
	for _, field := range canonicalSystemFields {
		seen[field.ID] = field.Name
	}
	for index, column := range result.Columns {
		relationID, err := strconv.ParseUint(column.TypeMetadata["source_relation_id"], 10, 32)
		if err != nil || relationID == 0 {
			return connector.Schema{}, nil, "", fmt.Errorf("column %q lacks source_relation_id", column.Name)
		}
		columnID, err := strconv.ParseInt(column.TypeMetadata["source_column_id"], 10, 16)
		if err != nil || columnID <= 0 {
			return connector.Schema{}, nil, "", fmt.Errorf("column %q lacks source_column_id", column.Name)
		}
		fieldID := stableFieldID(lineage, uint32(relationID), int16(columnID))
		if prior, ok := seen[fieldID]; ok {
			return connector.Schema{}, nil, "", fmt.Errorf("stable field ID collision between %q and %q", prior, column.Name)
		}
		seen[fieldID] = column.Name
		metadata := make(map[string]string, len(column.TypeMetadata)+1)
		for key, value := range column.TypeMetadata {
			metadata[key] = value
		}
		metadata["wallaby.field_id"] = strconv.FormatInt(int64(fieldID), 10)
		column.TypeMetadata = metadata
		result.Columns[index] = column
		canonical.Fields = append(canonical.Fields, CanonicalField{ID: fieldID, Name: column.Name, Type: column.Type, Nullable: column.Nullable})
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return connector.Schema{}, nil, "", fmt.Errorf("marshal canonical schema: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return result, encoded, hex.EncodeToString(digest[:]), nil
}

func stableFieldID(lineage string, relationID uint32, columnID int16) int32 {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%d", lineage, relationID, columnID)))
	value := int32(binary.BigEndian.Uint32(digest[:4]) >> 1)
	if value <= canonicalSystemFieldCount {
		value += canonicalSystemFieldCount + 1
	}
	return value
}

func artifactIdentity(incarnationID uuid.UUID, transaction connector.SourceTransaction, ordinal uint64, schemaID, logicalHash string) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf(
		"%s\x00%s\x00%s\x00%d\x00%s\x00%s\x00%s\x00%d\x00%s\x00%s",
		ProjectionID,
		incarnationID,
		transaction.SourceLineageID,
		transaction.TransactionID,
		transaction.BeginLSN,
		transaction.CommitLSN,
		transaction.EndLSN,
		ordinal,
		schemaID,
		logicalHash,
	)))
	return hex.EncodeToString(digest[:])
}

func shortHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:8])
}

func canonicalSourceTransaction(transaction connector.SourceTransaction) (connector.SourceTransaction, error) {
	checkpointLSN, err := connector.CanonicalizeCheckpointPosition(transaction.Checkpoint.LSN)
	if err != nil {
		return connector.SourceTransaction{}, err
	}
	endLSN, err := connector.CanonicalizeCheckpointPosition(transaction.EndLSN)
	if err != nil || checkpointLSN != endLSN {
		return connector.SourceTransaction{}, fmt.Errorf("checkpoint %q must equal transaction end %q", checkpointLSN, transaction.EndLSN)
	}
	beginLSN, err := connector.CanonicalizeCheckpointPosition(transaction.BeginLSN)
	if err != nil {
		return connector.SourceTransaction{}, err
	}
	commitLSN, err := connector.CanonicalizeCheckpointPosition(transaction.CommitLSN)
	if err != nil {
		return connector.SourceTransaction{}, err
	}
	transaction.Checkpoint.LSN = checkpointLSN
	transaction.EndLSN = endLSN
	transaction.BeginLSN = beginLSN
	transaction.CommitLSN = commitLSN
	return transaction, nil
}

func estimateBatchInput(batch connector.Batch, maxDepth int) (int64, error) {
	total := int64(len(batch.Schema.Namespace) + len(batch.Schema.Name) + len(batch.Checkpoint.LSN))
	for _, record := range batch.Records {
		total += int64(len(record.Table) + len(record.Operation) + len(record.Key) + len(record.SourcePosition) + 32)
		for _, values := range []map[string]any{record.Before, record.After} {
			for key, value := range values {
				size, err := estimateValue(value, 1, maxDepth)
				if err != nil {
					return 0, fmt.Errorf("field %q: %w", key, err)
				}
				total += int64(len(key)) + size
			}
		}
	}
	return total, nil
}

func estimateValue(value any, depth, maxDepth int) (int64, error) {
	if depth > maxDepth {
		return 0, fmt.Errorf("nesting depth exceeds %d", maxDepth)
	}
	switch typed := value.(type) {
	case nil:
		return 1, nil
	case string:
		return int64(len(typed)), nil
	case []byte:
		return int64(len(typed)), nil
	case json.RawMessage:
		return int64(len(typed)), nil
	case time.Time:
		return 16, nil
	case map[string]any:
		var total int64
		for key, nested := range typed {
			size, err := estimateValue(nested, depth+1, maxDepth)
			if err != nil {
				return 0, err
			}
			total += int64(len(key)) + size
		}
		return total, nil
	case []any:
		var total int64
		for _, nested := range typed {
			size, err := estimateValue(nested, depth+1, maxDepth)
			if err != nil {
				return 0, err
			}
			total += size
		}
		return total, nil
	}
	reflected := reflect.ValueOf(value)
	if reflected.IsValid() && (reflected.Kind() == reflect.Slice || reflected.Kind() == reflect.Array) {
		var total int64
		for index := 0; index < reflected.Len(); index++ {
			size, err := estimateValue(reflected.Index(index).Interface(), depth+1, maxDepth)
			if err != nil {
				return 0, err
			}
			total += size
		}
		return total, nil
	}
	return int64(len(fmt.Sprint(value))), nil
}
