package artifactlog

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/wire"
)

const (
	// ProjectionID is the frozen logical contract of the first canonical CDC
	// artifact projection. Destination Parquet encodings are not this format.
	ProjectionID                = "canonical_cdc_parquet_v1"
	ProjectionIDV2              = "canonical_cdc_parquet_v2"
	TargetEncodedObject         = 32 << 20
	MaxEncodedObject            = 64 << 20
	DefaultMaxRecords           = 1_000_000
	DefaultMaxNesting           = 32
	DefaultMaxInput             = 256 << 20
	DefaultMaxFragments         = 128
	DefaultMaxTransactionEncode = 256 << 20
	UnpartitionedValue          = "unpartitioned"

	canonicalSourcePositionColumn = "__wallaby_source_position"
	canonicalRecordOrdinalColumn  = "__wallaby_record_ordinal"
	canonicalLogicalBatchColumn   = "__wallaby_logical_batch_id"
	canonicalUnchangedColumn      = "__wallaby_unchanged"
)

// CanonicalField records stable logical field identity independently of
// Parquet's physical column ordering.
type CanonicalField struct {
	ID                      int32             `json:"id"`
	Name                    string            `json:"name"`
	Type                    string            `json:"type"`
	Nullable                bool              `json:"nullable"`
	Generated               bool              `json:"generated,omitempty"`
	Expression              string            `json:"expression,omitempty"`
	Metadata                map[string]string `json:"metadata,omitempty"`
	Quoted                  bool              `json:"quoted,omitempty"`
	SourceLineageID         string            `json:"source_lineage_id,omitempty"`
	SourceRelationID        uint32            `json:"source_relation_id,omitempty"`
	SourceColumnID          int32             `json:"source_column_id,omitempty"`
	SyntheticIdentity       string            `json:"synthetic_identity,omitempty"`
	SyntheticSourceRelation string            `json:"synthetic_source_relation,omitempty"`
}

type canonicalSchemaV2 struct {
	ProjectionID       string           `json:"projection_id"`
	MappingFingerprint string           `json:"mapping_fingerprint"`
	SourceLineageID    string           `json:"source_lineage_id"`
	Namespace          string           `json:"namespace"`
	Table              string           `json:"table"`
	QuotedNamespace    bool             `json:"quoted_namespace,omitempty"`
	QuotedTable        bool             `json:"quoted_table,omitempty"`
	Fields             []CanonicalField `json:"fields"`
}

type canonicalSchema struct {
	ProjectionID    string           `json:"projection_id"`
	Namespace       string           `json:"namespace"`
	Table           string           `json:"table"`
	QuotedNamespace bool             `json:"quoted_namespace,omitempty"`
	QuotedTable     bool             `json:"quoted_table,omitempty"`
	Fields          []CanonicalField `json:"fields"`
}

// Barrier is an ordered control record. DDL is rooted in PostgreSQL and is
// never encoded as an ordinary changelog row in a canonical Parquet object.
type Barrier struct {
	FragmentOrdinal uint64
	RecordOrdinal   uint64
	Kind            string
	Namespace       string
	Table           string
	SchemaID        string
	DDL             string
	DDLPlan         []byte
	ContentHash     string
}

// Plan is one deterministic logical source transaction projection.
type Plan struct {
	LogicalBatchID string
	ContentHash    string
	Artifacts      []Artifact
	Barriers       []Barrier
}

// Artifact is a deterministic local candidate; PostgreSQL reservation occurs
// before it may be uploaded.
type Artifact struct {
	ID                 string
	LogicalBatchID     string
	SchemaID           string
	SchemaJSON         []byte
	Namespace          string
	Table              string
	Partition          string
	Shard              uint32
	SourcePosition     string
	FragmentOrdinal    uint64
	FirstRecordOrdinal uint64
	RecordCount        uint64
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
	targetEncoded         int
	maxEncoded            int
	maxFragments          int
	maxTransactionEncoded int64
	maxNesting            int
}

func NewEncoder() *Encoder {
	return &Encoder{
		maxRecords:            DefaultMaxRecords,
		maxInput:              DefaultMaxInput,
		targetEncoded:         TargetEncodedObject,
		maxEncoded:            MaxEncodedObject,
		maxFragments:          DefaultMaxFragments,
		maxTransactionEncoded: DefaultMaxTransactionEncode,
		maxNesting:            DefaultMaxNesting,
	}
}

// EncodeTransaction preserves the original package interface for callers that
// only need object candidates. New publication code uses PlanTransaction so it
// also roots the logical batch and ordered barriers.
func (e *Encoder) EncodeTransaction(ctx context.Context, incarnationID uuid.UUID, transaction connector.SourceTransaction) ([]Artifact, error) {
	plan, err := e.PlanTransaction(ctx, incarnationID, transaction)
	if err != nil {
		return nil, err
	}
	return plan.Artifacts, nil
}

// PlanTransaction validates the complete committed transaction, separates
// ordered DDL barriers, assigns replay-stable record ordinals, and creates
// bounded deterministic table/schema/partition shards.
func (e *Encoder) PlanTransaction(_ context.Context, incarnationID uuid.UUID, transaction connector.SourceTransaction) (Plan, error) {
	transaction, err := canonicalSourceTransaction(transaction)
	if err != nil {
		return Plan{}, err
	}
	if err := transaction.Validate(); err != nil {
		return Plan{}, fmt.Errorf("validate canonical source transaction: %w", err)
	}
	if len(transaction.Fragments) > e.maxFragments {
		return Plan{}, fmt.Errorf("canonical artifact fragment count %d exceeds %d", len(transaction.Fragments), e.maxFragments)
	}
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		return Plan{}, fmt.Errorf("identify canonical source transaction: %w", err)
	}
	codec, err := wire.NewCodec(string(connector.WireFormatParquet))
	if err != nil {
		return Plan{}, err
	}

	plan := Plan{LogicalBatchID: logicalBatchID, ContentHash: contentHash}
	shards := make(map[string]uint32)
	var totalInput, totalEncoded int64
	var totalRecords int
	var recordOrdinal uint64
	for fragmentIndex, fragment := range transaction.Fragments {
		batch := fragment.Batch
		totalRecords += len(batch.Records)
		if totalRecords > e.maxRecords {
			return Plan{}, fmt.Errorf("transaction has %d records, limit %d", totalRecords, e.maxRecords)
		}
		inputEstimate, err := estimateBatchInput(batch, e.maxNesting)
		if err != nil {
			return Plan{}, fmt.Errorf("measure fragment %d: %w", fragmentIndex, err)
		}
		totalInput += inputEstimate
		if totalInput > e.maxInput {
			return Plan{}, fmt.Errorf("transaction uncompressed input %d exceeds limit %d", totalInput, e.maxInput)
		}

		var run []ordinalRecord
		flushRun := func() error {
			if len(run) == 0 {
				return nil
			}
			canonical, schemaJSON, schemaID, err := canonicalizeSchema(transaction.SourceLineageID, batch.Schema)
			if err != nil {
				return fmt.Errorf("canonicalize fragment %d schema: %w", fragmentIndex, err)
			}
			encodedShards, err := e.encodeShards(codec, transaction, logicalBatchID, canonical, run)
			if err != nil {
				return fmt.Errorf("encode fragment %d: %w", fragmentIndex, err)
			}
			partition := UnpartitionedValue
			group := strings.Join([]string{transaction.SourceLineageID, canonical.Namespace, canonical.Name, schemaID, partition}, "\x00")
			for _, encodedShard := range encodedShards {
				shard := shards[group]
				shards[group] = shard + 1
				totalEncoded += int64(len(encodedShard.encoded))
				if totalEncoded > e.maxTransactionEncoded {
					return fmt.Errorf("transaction encoded bytes %d exceed limit %d", totalEncoded, e.maxTransactionEncoded)
				}
				encodedDigest := sha256.Sum256(encodedShard.encoded)
				encodedHash := hex.EncodeToString(encodedDigest[:])
				artifactID := artifactIdentity(incarnationID, transaction.SourceLineageID, logicalBatchID, canonical.Namespace, canonical.Name, schemaID, partition, shard)
				plan.Artifacts = append(plan.Artifacts, Artifact{
					ID:                 artifactID,
					LogicalBatchID:     logicalBatchID,
					SchemaID:           schemaID,
					SchemaJSON:         schemaJSON,
					Namespace:          canonical.Namespace,
					Table:              canonical.Name,
					Partition:          partition,
					Shard:              shard,
					SourcePosition:     transaction.EndLSN,
					FragmentOrdinal:    fragment.Ordinal,
					FirstRecordOrdinal: encodedShard.records[0].ordinal,
					RecordCount:        uint64(len(encodedShard.records)),
					LogicalContentHash: encodedShard.logicalHash,
					EncodedByteHash:    encodedHash,
					ChecksumSHA256:     encodedHash,
					Encoded:            encodedShard.encoded,
					ObjectKey: artifactObjectKey(
						incarnationID, transaction.SourceLineageID, canonical.Namespace, canonical.Name,
						schemaID, partition, shard, artifactID,
					),
				})
			}
			run = nil
			return nil
		}

		for _, record := range batch.Records {
			if record.Operation == connector.OpDDL {
				if err := flushRun(); err != nil {
					return Plan{}, err
				}
				barrierBatch := batch
				barrierBatch.Schema.Version = 0
				barrierRecord := record
				barrierRecord.SchemaVersion = 0
				barrierBatch.Records = []connector.Record{barrierRecord}
				barrierBatch.Checkpoint = transaction.Checkpoint
				barrierHash, hashErr := connector.BatchContentHash(barrierBatch)
				if hashErr != nil {
					return Plan{}, fmt.Errorf("hash DDL barrier at ordinal %d: %w", recordOrdinal, hashErr)
				}
				plan.Barriers = append(plan.Barriers, Barrier{
					FragmentOrdinal: fragment.Ordinal,
					RecordOrdinal:   recordOrdinal,
					Kind:            "ddl",
					Namespace:       batch.Schema.Namespace,
					Table:           record.Table,
					DDL:             record.DDL,
					DDLPlan:         append([]byte(nil), record.DDLPlan...),
					ContentHash:     barrierHash,
				})
				recordOrdinal++
				continue
			}
			run = append(run, ordinalRecord{record: record, ordinal: recordOrdinal})
			recordOrdinal++
		}
		if err := flushRun(); err != nil {
			return Plan{}, err
		}
	}
	return plan, nil
}

// PlanMappedTransaction encodes one transaction that has already been projected
// exactly once by the sole materialized destination projector. The v1 planner
// remains separate and frozen.
func (e *Encoder) PlanMappedTransaction(_ context.Context, incarnationID uuid.UUID, mappingFingerprint string, transaction connector.SourceTransaction) (Plan, error) {
	mappingFingerprint = strings.TrimSpace(mappingFingerprint)
	if len(mappingFingerprint) != 64 || mappingFingerprint != strings.ToLower(mappingFingerprint) {
		return Plan{}, errors.New("canonical v2 mapping fingerprint must be lowercase 64-hex")
	}
	if _, err := hex.DecodeString(mappingFingerprint); err != nil {
		return Plan{}, errors.New("canonical v2 mapping fingerprint must be lowercase 64-hex")
	}
	transaction, err := canonicalSourceTransaction(transaction)
	if err != nil {
		return Plan{}, err
	}
	transaction, err = normalizeMappedArtifactTransaction(transaction)
	if err != nil {
		return Plan{}, err
	}
	if err := transaction.Validate(); err != nil {
		return Plan{}, fmt.Errorf("validate canonical v2 source transaction: %w", err)
	}
	if len(transaction.Fragments) > e.maxFragments {
		return Plan{}, fmt.Errorf("canonical artifact fragment count %d exceeds %d", len(transaction.Fragments), e.maxFragments)
	}
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		return Plan{}, fmt.Errorf("identify canonical v2 source transaction: %w", err)
	}
	codec, err := wire.NewCodec(string(connector.WireFormatParquet))
	if err != nil {
		return Plan{}, err
	}
	plan := Plan{LogicalBatchID: logicalBatchID, ContentHash: contentHash}
	shards := make(map[string]uint32)
	var totalInput, totalEncoded int64
	var totalRecords int
	var recordOrdinal uint64
	for fragmentIndex, fragment := range transaction.Fragments {
		batch := fragment.Batch
		totalRecords += len(batch.Records)
		if totalRecords > e.maxRecords {
			return Plan{}, fmt.Errorf("transaction has %d records, limit %d", totalRecords, e.maxRecords)
		}
		inputEstimate, err := estimateBatchInput(batch, e.maxNesting)
		if err != nil {
			return Plan{}, fmt.Errorf("measure fragment %d: %w", fragmentIndex, err)
		}
		totalInput += inputEstimate
		if totalInput > e.maxInput {
			return Plan{}, fmt.Errorf("transaction uncompressed input %d exceeds limit %d", totalInput, e.maxInput)
		}
		var run []ordinalRecord
		flushRun := func() error {
			if len(run) == 0 {
				return nil
			}
			canonical, schemaJSON, schemaID, err := canonicalizeSchemaV2(transaction.SourceLineageID, mappingFingerprint, batch.Schema)
			if err != nil {
				return fmt.Errorf("canonicalize v2 fragment %d schema: %w", fragmentIndex, err)
			}
			encodedShards, err := e.encodeShards(codec, transaction, logicalBatchID, canonical, run)
			if err != nil {
				return fmt.Errorf("encode v2 fragment %d: %w", fragmentIndex, err)
			}
			partition := UnpartitionedValue
			group := strings.Join([]string{transaction.SourceLineageID, mappingFingerprint, canonical.Namespace, canonical.Name, schemaID, partition}, "\x00")
			for _, encodedShard := range encodedShards {
				shard := shards[group]
				shards[group] = shard + 1
				totalEncoded += int64(len(encodedShard.encoded))
				if totalEncoded > e.maxTransactionEncoded {
					return fmt.Errorf("transaction encoded bytes %d exceed limit %d", totalEncoded, e.maxTransactionEncoded)
				}
				digest := sha256.Sum256(encodedShard.encoded)
				encodedHash := hex.EncodeToString(digest[:])
				artifactID := artifactIdentityV2(incarnationID, transaction.SourceLineageID, logicalBatchID, mappingFingerprint, canonical.Namespace, canonical.Name, schemaID, partition, shard)
				plan.Artifacts = append(plan.Artifacts, Artifact{ID: artifactID, LogicalBatchID: logicalBatchID, SchemaID: schemaID, SchemaJSON: schemaJSON, Namespace: canonical.Namespace, Table: canonical.Name, Partition: partition, Shard: shard, SourcePosition: transaction.EndLSN, FragmentOrdinal: fragment.Ordinal, FirstRecordOrdinal: encodedShard.records[0].ordinal, RecordCount: uint64(len(encodedShard.records)), LogicalContentHash: encodedShard.logicalHash, EncodedByteHash: encodedHash, ChecksumSHA256: encodedHash, Encoded: encodedShard.encoded, ObjectKey: artifactObjectKeyV2(incarnationID, transaction.SourceLineageID, mappingFingerprint, canonical.Namespace, canonical.Name, schemaID, partition, shard, artifactID)})
			}
			run = nil
			return nil
		}
		for _, record := range batch.Records {
			if record.Operation == connector.OpDDL {
				if err := flushRun(); err != nil {
					return Plan{}, err
				}
				barrierBatch := batch
				barrierBatch.Schema.Version = 0
				barrierRecord := record
				barrierRecord.SchemaVersion = 0
				barrierBatch.Records = []connector.Record{barrierRecord}
				barrierBatch.Checkpoint = transaction.Checkpoint
				barrierHash, hashErr := connector.BatchContentHash(barrierBatch)
				if hashErr != nil {
					return Plan{}, fmt.Errorf("hash v2 DDL barrier at ordinal %d: %w", recordOrdinal, hashErr)
				}
				plan.Barriers = append(plan.Barriers, Barrier{FragmentOrdinal: fragment.Ordinal, RecordOrdinal: recordOrdinal, Kind: "ddl", Namespace: batch.Schema.Namespace, Table: record.Table, DDL: record.DDL, DDLPlan: append([]byte(nil), record.DDLPlan...), ContentHash: barrierHash})
				recordOrdinal++
				continue
			}
			run = append(run, ordinalRecord{record: record, ordinal: recordOrdinal})
			recordOrdinal++
		}
		if err := flushRun(); err != nil {
			return Plan{}, err
		}
	}
	return plan, nil
}

type ordinalRecord struct {
	record  connector.Record
	ordinal uint64
}

type encodedShard struct {
	records     []ordinalRecord
	encoded     []byte
	logicalHash string
}

func (e *Encoder) encodeShards(codec wire.Codec, transaction connector.SourceTransaction, logicalBatchID string, schema connector.Schema, records []ordinalRecord) ([]encodedShard, error) {
	batch, err := canonicalArtifactBatch(transaction, logicalBatchID, schema, records)
	if err != nil {
		return nil, err
	}
	encoded, err := codec.Encode(batch)
	if err != nil {
		return nil, fmt.Errorf("encode canonical Parquet: %w", err)
	}
	if len(encoded) == 0 {
		return nil, errors.New("canonical Parquet encoder returned an empty object")
	}
	if len(encoded) > e.targetEncoded && len(records) > 1 {
		middle := len(records) / 2
		left, err := e.encodeShards(codec, transaction, logicalBatchID, schema, records[:middle])
		if err != nil {
			return nil, err
		}
		right, err := e.encodeShards(codec, transaction, logicalBatchID, schema, records[middle:])
		if err != nil {
			return nil, err
		}
		return append(left, right...), nil
	}
	if len(encoded) > e.maxEncoded {
		return nil, fmt.Errorf("one canonical shard encoded to %d bytes, hard limit %d", len(encoded), e.maxEncoded)
	}
	logicalHash, err := connector.BatchContentHash(batch)
	if err != nil {
		return nil, err
	}
	return []encodedShard{{records: append([]ordinalRecord(nil), records...), encoded: encoded, logicalHash: logicalHash}}, nil
}

func canonicalArtifactBatch(transaction connector.SourceTransaction, logicalBatchID string, schema connector.Schema, records []ordinalRecord) (connector.Batch, error) {
	canonical := schema
	canonical.Version = 0
	canonical.Columns = append(append([]connector.Column(nil), schema.Columns...),
		connector.Column{Name: canonicalSourcePositionColumn, Type: "text", TypeMetadata: map[string]string{"wallaby.field_id": "8"}},
		connector.Column{Name: canonicalRecordOrdinalColumn, Type: "int8", TypeMetadata: map[string]string{"wallaby.field_id": "9"}},
		connector.Column{Name: canonicalLogicalBatchColumn, Type: "text", TypeMetadata: map[string]string{"wallaby.field_id": "10"}},
		connector.Column{Name: canonicalUnchangedColumn, Type: "jsonb", TypeMetadata: map[string]string{"wallaby.field_id": "11"}},
	)
	changes := make([]connector.Record, 0, len(records))
	for _, item := range records {
		record := item.record
		record.SchemaVersion = 0
		record.Key = append([]byte(nil), record.Key...)
		record.Payload = append([]byte(nil), record.Payload...)
		record.Unchanged = append([]string{}, record.Unchanged...)
		sort.Strings(record.Unchanged)
		unchanged, err := json.Marshal(record.Unchanged)
		if err != nil {
			return connector.Batch{}, fmt.Errorf("encode unchanged columns at record ordinal %d: %w", item.ordinal, err)
		}
		after := make(map[string]any, len(record.After)+4)
		for key, value := range record.After {
			after[key] = value
		}
		position := strings.TrimSpace(record.SourcePosition)
		if position == "" {
			position = transaction.EndLSN
		} else {
			canonicalPosition, err := connector.CanonicalizeCheckpointPosition(position)
			if err != nil {
				return connector.Batch{}, fmt.Errorf("canonicalize record ordinal %d source position: %w", item.ordinal, err)
			}
			position = canonicalPosition
		}
		record.SourcePosition = position
		after[canonicalSourcePositionColumn] = position
		after[canonicalRecordOrdinalColumn] = int64(item.ordinal) // #nosec G115 -- bounded by maxRecords.
		after[canonicalLogicalBatchColumn] = logicalBatchID
		after[canonicalUnchangedColumn] = json.RawMessage(unchanged)
		record.After = after
		changes = append(changes, record)
	}
	return connector.Batch{Schema: canonical, Records: changes, Checkpoint: transaction.Checkpoint, WireFormat: connector.WireFormatParquet}, nil
}

const canonicalSystemFieldCount int32 = 11

var canonicalSystemFields = []CanonicalField{
	{ID: 1, Name: "__op", Type: "text", Nullable: false},
	{ID: 2, Name: "__ts", Type: "timestamp_us", Nullable: true},
	{ID: 3, Name: "__schema_version", Type: "bigint", Nullable: false},
	{ID: 4, Name: "__table", Type: "text", Nullable: true},
	{ID: 5, Name: "__namespace", Type: "text", Nullable: true},
	{ID: 6, Name: "__key", Type: "bytea", Nullable: true},
	{ID: 7, Name: "__before_json", Type: "bytea", Nullable: true},
	{ID: 8, Name: canonicalSourcePositionColumn, Type: "text", Nullable: false},
	{ID: 9, Name: canonicalRecordOrdinalColumn, Type: "bigint", Nullable: false},
	{ID: 10, Name: canonicalLogicalBatchColumn, Type: "text", Nullable: false},
	{ID: 11, Name: canonicalUnchangedColumn, Type: "jsonb", Nullable: false},
}

func canonicalizeSchema(lineage string, schema connector.Schema) (connector.Schema, []byte, string, error) {
	canonical := canonicalSchema{
		ProjectionID:    ProjectionID,
		Namespace:       schema.Namespace,
		Table:           schema.Name,
		QuotedNamespace: schema.QuotedIdentifiers["namespace"],
		QuotedTable:     schema.QuotedIdentifiers["table"],
		Fields:          append([]CanonicalField(nil), canonicalSystemFields...),
	}
	result := schema
	result.Version = 0
	result.Columns = append([]connector.Column(nil), schema.Columns...)
	seen := make(map[int32]string, len(schema.Columns)+len(canonicalSystemFields))
	reservedNames := make(map[string]struct{}, len(canonicalSystemFields))
	for _, field := range canonicalSystemFields {
		seen[field.ID] = field.Name
		reservedNames[field.Name] = struct{}{}
	}
	for index, column := range result.Columns {
		if _, reserved := reservedNames[column.Name]; reserved {
			return connector.Schema{}, nil, "", fmt.Errorf("column %q collides with canonical envelope", column.Name)
		}
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
		canonical.Fields = append(canonical.Fields, CanonicalField{
			ID: fieldID, Name: column.Name, Type: column.Type, Nullable: column.Nullable,
			Generated: column.Generated, Expression: column.Expression, Metadata: metadata,
			Quoted: schema.QuotedIdentifiers[column.Name],
		})
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return connector.Schema{}, nil, "", fmt.Errorf("marshal canonical schema: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return result, encoded, hex.EncodeToString(digest[:]), nil
}

func canonicalizeSchemaV2(lineage, mappingFingerprint string, schema connector.Schema) (connector.Schema, []byte, string, error) {
	if strings.TrimSpace(lineage) == "" {
		return connector.Schema{}, nil, "", errors.New("canonical v2 source lineage is required")
	}
	system := make([]CanonicalField, len(canonicalSystemFields))
	copy(system, canonicalSystemFields)
	for index := range system {
		system[index].SyntheticIdentity = "canonical.envelope.v2:" + system[index].Name
	}
	canonical := canonicalSchemaV2{ProjectionID: ProjectionIDV2, MappingFingerprint: mappingFingerprint, SourceLineageID: lineage, Namespace: schema.Namespace, Table: schema.Name, QuotedNamespace: schema.QuotedIdentifiers["namespace"], QuotedTable: schema.QuotedIdentifiers["table"], Fields: system}
	result := schema
	result.Version = 0
	result.Columns = append([]connector.Column(nil), schema.Columns...)
	seen := make(map[int32]string, len(schema.Columns)+len(system))
	reservedNames := make(map[string]struct{}, len(system))
	for _, field := range system {
		seen[field.ID] = field.Name
		reservedNames[field.Name] = struct{}{}
	}
	for index, column := range result.Columns {
		if _, reserved := reservedNames[column.Name]; reserved {
			return connector.Schema{}, nil, "", fmt.Errorf("column %q collides with canonical envelope", column.Name)
		}
		synthetic := strings.TrimSpace(column.TypeMetadata["wallaby.synthetic_identity"])
		syntheticRelation := strings.TrimSpace(column.TypeMetadata["wallaby.synthetic_source_relation"])
		var fieldID int32
		var relationID uint64
		var columnID int64
		if synthetic != "" {
			if synthetic != "append.operation.v1" && synthetic != "append.deleted.v1" {
				return connector.Schema{}, nil, "", fmt.Errorf("column %q has unsupported synthetic identity %q", column.Name, synthetic)
			}
			if syntheticRelation == "" {
				return connector.Schema{}, nil, "", fmt.Errorf("column %q lacks synthetic source relation", column.Name)
			}
			fieldID = stableSyntheticFieldID(lineage, syntheticRelation, synthetic)
		} else {
			var err error
			relationID, err = strconv.ParseUint(column.TypeMetadata["source_relation_id"], 10, 32)
			if err != nil || relationID == 0 {
				return connector.Schema{}, nil, "", fmt.Errorf("column %q lacks source_relation_id", column.Name)
			}
			columnID, err = strconv.ParseInt(column.TypeMetadata["source_column_id"], 10, 16)
			if err != nil || columnID <= 0 {
				return connector.Schema{}, nil, "", fmt.Errorf("column %q lacks source_column_id", column.Name)
			}
			fieldID = stableFieldID(lineage, uint32(relationID), int16(columnID))
		}
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
		canonical.Fields = append(canonical.Fields, CanonicalField{ID: fieldID, Name: column.Name, Type: column.Type, Nullable: column.Nullable, Generated: column.Generated, Expression: column.Expression, Metadata: metadata, Quoted: schema.QuotedIdentifiers[column.Name], SourceLineageID: lineage, SourceRelationID: uint32(relationID), SourceColumnID: int32(columnID), SyntheticIdentity: synthetic, SyntheticSourceRelation: syntheticRelation})
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return connector.Schema{}, nil, "", fmt.Errorf("marshal canonical v2 schema: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return result, encoded, hex.EncodeToString(digest[:]), nil
}

func stableFieldID(lineage string, relationID uint32, columnID int16) int32 {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%d", lineage, relationID, columnID)))
	// #nosec G115 -- shifting an unsigned 32-bit value right yields at most MaxInt32.
	value := int32(binary.BigEndian.Uint32(digest[:4]) >> 1)
	if value <= canonicalSystemFieldCount {
		value += canonicalSystemFieldCount + 1
	}
	return value
}

func stableSyntheticFieldID(lineage, sourceRelation, identity string) int32 {
	digest := sha256.Sum256([]byte("wallaby.synthetic-field.v1\x00" + lineage + "\x00" + sourceRelation + "\x00" + identity))
	value := int32(binary.BigEndian.Uint32(digest[:4]) >> 1)
	if value <= canonicalSystemFieldCount {
		value += canonicalSystemFieldCount + 1
	}
	return value
}

func artifactIdentity(incarnationID uuid.UUID, lineage, logicalBatchID, namespace, table, schemaID, partition string, shard uint32) string {
	sourceIdentity := incarnationID.String() + "\x00" + lineage
	digest := sha256.Sum256([]byte(strings.Join([]string{
		"wallaby.artifact.v1", ProjectionID, schemaID, sourceIdentity, namespace, table,
		"unpartitioned-v1", partition, strconv.FormatUint(uint64(shard), 10), logicalBatchID,
	}, "\x00")))
	return hex.EncodeToString(digest[:])
}

func artifactIdentityV2(incarnationID uuid.UUID, lineage, logicalBatchID, mappingFingerprint, namespace, table, schemaID, partition string, shard uint32) string {
	sourceIdentity := incarnationID.String() + "\x00" + lineage
	digest := sha256.Sum256([]byte(strings.Join([]string{"wallaby.artifact.v2", ProjectionIDV2, mappingFingerprint, schemaID, sourceIdentity, namespace, table, "unpartitioned-v1", partition, strconv.FormatUint(uint64(shard), 10), logicalBatchID}, "\x00")))
	return hex.EncodeToString(digest[:])
}

func artifactObjectKeyV2(incarnationID uuid.UUID, lineage, mappingFingerprint, namespace, table, schemaID, partition string, shard uint32, artifactID string) string {
	return fmt.Sprintf("wallaby/artifacts-v2/%s/source=%s/mapping=%s/namespace=%s/table=%s/schema=%s/partition=%s/shard=%06d/%s.parquet", incarnationID, shortHash(lineage), mappingFingerprint, url.PathEscape(namespace), url.PathEscape(table), schemaID, url.PathEscape(partition), shard, artifactID)
}

func artifactObjectKey(incarnationID uuid.UUID, lineage, namespace, table, schemaID, partition string, shard uint32, artifactID string) string {
	return fmt.Sprintf(
		"wallaby/artifacts/%s/source=%s/namespace=%s/table=%s/schema=%s/partition=%s/shard=%06d/%s.parquet",
		incarnationID, shortHash(lineage), url.PathEscape(namespace), url.PathEscape(table), schemaID,
		url.PathEscape(partition), shard, artifactID,
	)
}

func shortHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:8])
}

// normalizeMappedArtifactTransaction removes the append projector's source-position
// materialization because the canonical envelope is the sole owner of that field.
// Operation and deletion metadata remain ordinary mapped fields with explicit
// synthetic identities.
func normalizeMappedArtifactTransaction(transaction connector.SourceTransaction) (connector.SourceTransaction, error) {
	for fragmentIndex := range transaction.Fragments {
		batch := &transaction.Fragments[fragmentIndex].Batch
		columns := make([]connector.Column, 0, len(batch.Schema.Columns))
		for _, column := range batch.Schema.Columns {
			if column.Name != connector.AppendSourcePositionColumn {
				columns = append(columns, column)
				continue
			}
			if column.TypeMetadata["wallaby.synthetic_identity"] != "append.source_position.v1" {
				return connector.SourceTransaction{}, fmt.Errorf("mapped column %q collides with canonical source-position envelope", column.Name)
			}
		}
		batch.Schema.Columns = columns
		for recordIndex := range batch.Records {
			record := &batch.Records[recordIndex]
			for _, image := range []map[string]any{record.Before, record.After} {
				value, exists := image[connector.AppendSourcePositionColumn]
				if !exists {
					continue
				}
				position, ok := value.(string)
				if !ok || strings.TrimSpace(position) != record.SourcePosition {
					return connector.SourceTransaction{}, fmt.Errorf("mapped source-position metadata differs at fragment %d record %d", fragmentIndex, recordIndex)
				}
				delete(image, connector.AppendSourcePositionColumn)
			}
		}
	}
	return transaction, nil
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
	var recordOrdinal uint64
	for fragmentIndex := range transaction.Fragments {
		for recordIndex := range transaction.Fragments[fragmentIndex].Batch.Records {
			record := &transaction.Fragments[fragmentIndex].Batch.Records[recordIndex]
			position := strings.TrimSpace(record.SourcePosition)
			if position == "" {
				position = endLSN
			} else {
				position, err = connector.CanonicalizeCheckpointPosition(position)
				if err != nil {
					return connector.SourceTransaction{}, fmt.Errorf("canonicalize record ordinal %d source position: %w", recordOrdinal, err)
				}
			}
			record.SourcePosition = position
			recordOrdinal++
		}
	}
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
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			size, err := estimateValue(typed[key], depth+1, maxDepth)
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
