package tests

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	iceberggo "github.com/apache/iceberg-go"
	icerest "github.com/apache/iceberg-go/catalog/rest"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	icebergdest "github.com/josephjohncox/wallaby/connectors/destinations/iceberg"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestIcebergRESTLiveAppendProjection(t *testing.T) {
	uri := os.Getenv("WALLABY_TEST_ICEBERG_REST_URI")
	warehouse := os.Getenv("WALLABY_TEST_ICEBERG_WAREHOUSE")
	namespace := os.Getenv("WALLABY_TEST_ICEBERG_NAMESPACE")
	if uri == "" || warehouse == "" || namespace == "" {
		t.Skip("WALLABY_TEST_ICEBERG_REST_URI, WALLABY_TEST_ICEBERG_WAREHOUSE, and WALLABY_TEST_ICEBERG_NAMESPACE are required")
	}
	tablePrefix := "wallaby_live_" + strings.ReplaceAll(uuid.NewString(), "-", "") + "_"
	request, objects := icebergLiveRequest(t, namespace, tablePrefix+"artifact_events")
	cfg := icebergdest.Config{
		Profile: icebergdest.CatalogProfileREST, URI: uri, Warehouse: warehouse,
		ControlTable: "__wallaby_control", DestinationRevisionID: request.ConsumerRevisionID,
		MaxCommitRetries: 4, RequestTimeout: 30 * time.Second, ReconciliationHorizon: time.Hour,
		AllowHTTP: strings.HasPrefix(uri, "http://"), OAuthToken: os.Getenv("WALLABY_TEST_ICEBERG_OAUTH_TOKEN"),
		S3Endpoint:        os.Getenv("WALLABY_TEST_ICEBERG_S3_ENDPOINT"),
		S3AccessKeyID:     os.Getenv("WALLABY_TEST_ICEBERG_S3_ACCESS_KEY"),
		S3SecretAccessKey: os.Getenv("WALLABY_TEST_ICEBERG_S3_SECRET_KEY"),
		S3Region:          os.Getenv("WALLABY_TEST_ICEBERG_S3_REGION"),
	}
	committer, err := icebergdest.NewRESTCommitter(context.Background(), objects, cfg)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := committer.Commit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(committed.SnapshotIDs) == 0 {
		t.Fatal("live REST commit returned no snapshot evidence")
	}
	reconciled, err := committer.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if reconciled.Disposition != artifactlog.CommitApplied || reconciled.Commit.SnapshotID != committed.SnapshotID {
		t.Fatalf("live REST reconciliation=%+v committed=%+v", reconciled, committed)
	}
	// Read the committed table back through a fresh catalog. A successful scan
	// that returns the canonical value proves the data files carry the
	// catalog-assigned field IDs; a mismatch would resolve columns to null.
	assertIcebergReadbackByFieldID(t, cfg, namespace, expectedDataTable(cfg, tablePrefix))
}

// expectedDataTable mirrors the connector's source-to-target table mapping for
// the canonical `public.artifact_events` relation. When the configured target
// namespace differs from the source namespace, the connector qualifies the
// table name with the source namespace to avoid cross-namespace collisions.
func expectedDataTable(_ icebergdest.Config, tablePrefix string) string {
	return tablePrefix + "artifact_events"
}

func assertIcebergReadbackByFieldID(t *testing.T, cfg icebergdest.Config, namespace, tableName string) {
	t.Helper()
	cat := freshReadbackCatalog(t, cfg)
	tbl, err := cat.LoadTable(context.Background(), table.Identifier{namespace, tableName})
	if err != nil {
		t.Fatal(err)
	}
	schema := tbl.Schema()
	valueField, ok := schema.FindFieldByName("value")
	if !ok {
		t.Fatalf("catalog schema is missing the value column: %s", schema)
	}
	scanned, err := tbl.Scan().ToArrowTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer scanned.Release()
	if scanned.NumRows() == 0 {
		t.Fatal("live readback returned no rows")
	}
	indices := scanned.Schema().FieldIndices("value")
	if len(indices) != 1 {
		t.Fatalf("scan schema is missing the value column: %s", scanned.Schema())
	}
	if id, present := scanned.Schema().Field(indices[0]).Metadata.GetValue("PARQUET:field_id"); present && id != strconv.Itoa(valueField.ID) {
		t.Fatalf("readback value field id=%s, want catalog id %d", id, valueField.ID)
	}
	reader := array.NewTableReader(scanned, scanned.NumRows())
	defer reader.Release()
	found := false
	for reader.Next() {
		record := reader.RecordBatch()
		columnIndices := record.Schema().FieldIndices("value")
		if len(columnIndices) != 1 {
			continue
		}
		if column, ok := record.Column(columnIndices[0]).(*array.String); ok {
			for row := 0; row < column.Len(); row++ {
				if !column.IsNull(row) && column.Value(row) == "canonical" {
					found = true
				}
			}
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("live readback did not resolve the canonical value by Iceberg field IDs")
	}
}

// TestIcebergRESTLiveSchemaEvolutionRename proves that identity-tracked renames
// survive a real catalog's fresh field-ID reassignment. The stable identity is
// carried in each Iceberg field's doc; if the live REST server or the iceberg-go
// update path dropped the doc, buildFieldMapping would fall back to name matching
// and an identity-tracked rename would silently degrade to an add-column (the old
// column left behind with its rows, a fresh nullable column for new rows). This
// test commits, renames a source column keeping its PostgreSQL source identity,
// commits again, and asserts through a FRESH catalog that the table evolved the
// single column in place: the new name is present, the old name is gone, and the
// pre-rename row is still readable under the new name.
func TestIcebergRESTLiveSchemaEvolutionRename(t *testing.T) {
	uri := os.Getenv("WALLABY_TEST_ICEBERG_REST_URI")
	warehouse := os.Getenv("WALLABY_TEST_ICEBERG_WAREHOUSE")
	namespace := os.Getenv("WALLABY_TEST_ICEBERG_NAMESPACE")
	if uri == "" || warehouse == "" || namespace == "" {
		t.Skip("WALLABY_TEST_ICEBERG_REST_URI, WALLABY_TEST_ICEBERG_WAREHOUSE, and WALLABY_TEST_ICEBERG_NAMESPACE are required")
	}
	objects := &icebergLiveObjects{objects: map[string][]byte{}}
	incarnationID := uuid.New()
	tablePrefix := "wallaby_evo_" + strings.ReplaceAll(uuid.NewString(), "-", "") + "_"
	cfg := icebergdest.Config{
		Profile: icebergdest.CatalogProfileREST, URI: uri, Warehouse: warehouse,
		ControlTable: "__wallaby_control", DestinationRevisionID: "iceberg-live-evo-v1",
		MaxCommitRetries: 4, RequestTimeout: 30 * time.Second, ReconciliationHorizon: time.Hour,
		AllowHTTP: strings.HasPrefix(uri, "http://"), OAuthToken: os.Getenv("WALLABY_TEST_ICEBERG_OAUTH_TOKEN"),
		S3Endpoint:        os.Getenv("WALLABY_TEST_ICEBERG_S3_ENDPOINT"),
		S3AccessKeyID:     os.Getenv("WALLABY_TEST_ICEBERG_S3_ACCESS_KEY"),
		S3SecretAccessKey: os.Getenv("WALLABY_TEST_ICEBERG_S3_SECRET_KEY"),
		S3Region:          os.Getenv("WALLABY_TEST_ICEBERG_S3_REGION"),
	}
	committer, err := icebergdest.NewRESTCommitter(context.Background(), objects, cfg)
	if err != nil {
		t.Fatal(err)
	}
	initial := icebergLivePlanRequest(t, objects, incarnationID, "iceberg-live-evo-v1", 1, "0/D0", artifactSourceTransaction(), namespace, tablePrefix+"artifact_events")
	if _, err := committer.Commit(context.Background(), initial); err != nil {
		t.Fatalf("initial commit: %v", err)
	}
	// The second publication renames the `value` column to `payload` while keeping
	// its PostgreSQL source identity (relation 84, column 2) so the committer must
	// treat it as a rename rather than a drop+add.
	renameTx := artifactRenamedTransaction()
	rename := icebergLivePlanRequest(t, objects, incarnationID, "iceberg-live-evo-v1", 2, "0/F0", renameTx, namespace, tablePrefix+"artifact_events")
	// The second publication is attempted after the first commit's snapshot, so
	// its attempt time must post-date that snapshot for the reconcile-absence
	// check to conclude the rename snapshot is not yet present. iceberg-go stamps
	// snapshots with the client clock, matching this host clock.
	rename.AttemptedAt = time.Now().UTC()
	if _, err := committer.Commit(context.Background(), rename); err != nil {
		t.Fatalf("rename commit: %v", err)
	}
	reconciled, err := committer.Reconcile(context.Background(), rename)
	if err != nil || reconciled.Disposition != artifactlog.CommitApplied {
		t.Fatalf("rename reconciliation=%+v error=%v", reconciled, err)
	}
	assertIcebergRenameReadback(t, cfg, namespace, expectedDataTable(cfg, tablePrefix))
}

// artifactRenamedTransaction renames the `value` column to `payload` while
// preserving its PostgreSQL source identity, and inserts a new row so the
// evolved table carries both the pre-rename and post-rename records.
func artifactRenamedTransaction() connector.SourceTransaction {
	return connector.SourceTransaction{
		SourceLineageID: "postgres-system-1/artifact-publication-v1",
		TransactionID:   101,
		BeginLSN:        "0/E0",
		CommitLSN:       "0/E8",
		EndLSN:          "0/F0",
		Checkpoint:      connector.Checkpoint{LSN: "0/F0", Timestamp: time.Unix(1001, 0).UTC()},
		Fragments: []connector.TransactionFragment{{
			Ordinal: 0,
			Batch: connector.Batch{
				Schema: connector.Schema{Namespace: "public", Name: "artifact_events", Version: 2, Columns: []connector.Column{
					{Name: "id", Type: "int8", TypeMetadata: map[string]string{"source_relation_id": "84", "source_column_id": "1"}},
					{Name: "payload", Type: "text", TypeMetadata: map[string]string{"source_relation_id": "84", "source_column_id": "2"}},
				}},
				Records: []connector.Record{{Table: "artifact_events", Operation: connector.OpInsert, SchemaVersion: 2, Key: []byte(`{"id":2}`), After: map[string]any{"id": int64(2), "payload": "renamed"}, Timestamp: time.Unix(1000, 0).UTC()}},
			},
		}},
	}
}

// assertIcebergRenameReadback loads the table through a fresh catalog and proves
// the rename evolved the column in place rather than degrading to add+leftover.
func assertIcebergRenameReadback(t *testing.T, cfg icebergdest.Config, namespace, tableName string) {
	t.Helper()
	cat := freshReadbackCatalog(t, cfg)
	tbl, err := cat.LoadTable(context.Background(), table.Identifier{namespace, tableName})
	if err != nil {
		t.Fatal(err)
	}
	schema := tbl.Schema()
	if _, ok := schema.FindFieldByName("payload"); !ok {
		t.Fatalf("catalog schema is missing the renamed column payload: %s", schema)
	}
	if _, ok := schema.FindFieldByName("value"); ok {
		t.Fatalf("catalog schema still carries the pre-rename column value; rename degraded to add-column: %s", schema)
	}
	scanned, err := tbl.Scan().ToArrowTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer scanned.Release()
	if scanned.NumRows() < 2 {
		t.Fatalf("live readback returned %d rows, want the pre-rename and post-rename rows", scanned.NumRows())
	}
	reader := array.NewTableReader(scanned, scanned.NumRows())
	defer reader.Release()
	values := map[string]bool{}
	for reader.Next() {
		record := reader.RecordBatch()
		indices := record.Schema().FieldIndices("payload")
		if len(indices) != 1 {
			t.Fatalf("scan schema is missing the payload column: %s", record.Schema())
		}
		if column, ok := record.Column(indices[0]).(*array.String); ok {
			for row := 0; row < column.Len(); row++ {
				if !column.IsNull(row) {
					values[column.Value(row)] = true
				}
			}
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatal(err)
	}
	// The pre-rename row must resolve under the new name. If the rename had
	// degraded to add-column, "canonical" would have landed in the dropped
	// `value` column and payload would only hold "renamed".
	if !values["canonical"] {
		t.Fatalf("pre-rename row did not resolve under payload; rename degraded to add-column. payload values=%v", values)
	}
	if !values["renamed"] {
		t.Fatalf("post-rename row is missing from payload; values=%v", values)
	}
}

func freshReadbackCatalog(t *testing.T, cfg icebergdest.Config) *icerest.Catalog {
	t.Helper()
	props := iceberggo.Properties{}
	if cfg.S3Endpoint != "" {
		props[icebergio.S3EndpointURL] = cfg.S3Endpoint
	}
	if cfg.S3AccessKeyID != "" {
		props[icebergio.S3AccessKeyID] = cfg.S3AccessKeyID
	}
	if cfg.S3SecretAccessKey != "" {
		props[icebergio.S3SecretAccessKey] = cfg.S3SecretAccessKey
	}
	if cfg.S3Region != "" {
		props[icebergio.S3Region] = cfg.S3Region
	}
	opts := []icerest.Option{icerest.WithWarehouseLocation(cfg.Warehouse), icerest.WithAdditionalProps(props)}
	if cfg.OAuthToken != "" {
		opts = append(opts, icerest.WithOAuthToken(cfg.OAuthToken))
	}
	cat, err := icerest.NewCatalog(context.Background(), "readback", cfg.URI, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

func TestS3TablesSnowflakeCatalogLinkedReadback(t *testing.T) {
	if os.Getenv("WALLABY_TEST_S3TABLES_SNOWFLAKE") != "1" {
		t.Skip("WALLABY_TEST_S3TABLES_SNOWFLAKE=1 is required")
	}
	region := strings.TrimSpace(os.Getenv("WALLABY_TEST_S3TABLES_REGION"))
	warehouse := strings.TrimSpace(os.Getenv("WALLABY_TEST_S3TABLES_WAREHOUSE"))
	bucketARN := strings.TrimSpace(os.Getenv("WALLABY_TEST_S3TABLES_TABLE_BUCKET_ARN"))
	expectedRoleARN := strings.TrimSpace(os.Getenv("WALLABY_TEST_S3TABLES_EXPECTED_ROLE_ARN"))
	namespace := strings.TrimSpace(os.Getenv("WALLABY_TEST_S3TABLES_NAMESPACE"))
	snowflakeDSN := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_DSN"))
	linkedDatabase := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_LINKED_DATABASE"))
	if region == "" || warehouse == "" || bucketARN == "" || expectedRoleARN == "" || namespace == "" || snowflakeDSN == "" || linkedDatabase == "" {
		t.Fatal("S3 Tables Snowflake gate requires region, warehouse, table bucket ARN, expected writer role ARN, namespace, Snowflake DSN, and linked database")
	}

	prefix := "wallaby_sf_" + strings.ReplaceAll(uuid.NewString(), "-", "") + "_"
	request, objects := icebergLiveRequest(t, namespace, prefix+"artifact_events")
	cfg := icebergdest.Config{
		Profile: icebergdest.CatalogProfileS3Tables,
		URI:     "https://glue." + region + ".amazonaws.com/iceberg", Warehouse: warehouse,
		Region: region, SigV4: true, SigningName: "glue", ExpectedAWSRoleARN: expectedRoleARN,
		ControlTable: "__wallaby_control", DestinationRevisionID: request.ConsumerRevisionID,
		MaxCommitRetries: 4, RequestTimeout: 30 * time.Second, ReconciliationHorizon: 24 * time.Hour,
		S3TablesTableBucketARN: bucketARN, S3TablesConfigureMaintenance: true,
		S3TablesMinSnapshotsToKeep: 100, S3TablesMaxSnapshotAgeHours: 24,
	}
	committer, err := icebergdest.NewS3TablesCommitter(context.Background(), objects, cfg)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := committer.Commit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}

	db, err := connector.OpenSnowflakeDB(snowflakeDSN, snowflakeDeploymentPolicyForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	var snowflakeVersion string
	if err := db.QueryRowContext(ctx, "SELECT CURRENT_VERSION()").Scan(&snowflakeVersion); err != nil {
		t.Fatal(err)
	}
	t.Logf("Snowflake external-catalog readback version=%s Iceberg snapshots=%v", snowflakeVersion, committed.SnapshotIDs)

	if len(request.Objects) == 0 {
		t.Fatal("S3 Tables Snowflake gate produced no canonical objects")
	}
	tableName := request.Objects[0].Table
	qualified := strings.Join([]string{quoteSnowflakeCatalogIdentifier(linkedDatabase), quoteSnowflakeCatalogIdentifier(namespace), quoteSnowflakeCatalogIdentifier(tableName)}, ".")
	query := fmt.Sprintf(`SELECT COUNT(*),COUNT_IF("__wallaby_logical_batch_id"=?),COUNT_IF("__op"='insert') FROM %s`, qualified)
	expectedRows := int64(0)
	for _, object := range request.Objects {
		if object.Table == request.Objects[0].Table {
			expectedRows += int64(object.RecordCount) // #nosec G115 -- canonical object record count is bounded by artifact admission.
		}
	}
	var rows, matchingBatch, inserts int64
	var lastErr error
	for ctx.Err() == nil {
		lastErr = db.QueryRowContext(ctx, query, request.LogicalBatchID).Scan(&rows, &matchingBatch, &inserts)
		if lastErr == nil && rows == expectedRows && matchingBatch == expectedRows && inserts > 0 {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("Snowflake did not expose exact S3 Tables changelog rows for %s: rows=%d matching_batch=%d inserts=%d expected=%d last_error=%v context=%v", qualified, rows, matchingBatch, inserts, expectedRows, lastErr, ctx.Err())
}

func quoteSnowflakeCatalogIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func TestS3TablesLiveAppendProjection(t *testing.T) {
	if os.Getenv("WALLABY_TEST_S3TABLES") != "1" {
		t.Skip("WALLABY_TEST_S3TABLES=1 is required")
	}
	region := os.Getenv("WALLABY_TEST_S3TABLES_REGION")
	warehouse := os.Getenv("WALLABY_TEST_S3TABLES_WAREHOUSE")
	bucketARN := os.Getenv("WALLABY_TEST_S3TABLES_TABLE_BUCKET_ARN")
	expectedRoleARN := os.Getenv("WALLABY_TEST_S3TABLES_EXPECTED_ROLE_ARN")
	namespace := os.Getenv("WALLABY_TEST_S3TABLES_NAMESPACE")
	if region == "" || warehouse == "" || bucketARN == "" || expectedRoleARN == "" || namespace == "" {
		t.Fatal("S3 Tables live gate requires region, warehouse, table bucket ARN, expected writer role ARN, and namespace")
	}
	prefix := "wallaby_live_" + strings.ReplaceAll(uuid.NewString(), "-", "") + "_"
	request, objects := icebergLiveRequest(t, namespace, prefix+"artifact_events")
	cfg := icebergdest.Config{
		Profile: icebergdest.CatalogProfileS3Tables,
		URI:     "https://glue." + region + ".amazonaws.com/iceberg", Warehouse: warehouse,
		Region: region, SigV4: true, SigningName: "glue", ExpectedAWSRoleARN: expectedRoleARN,
		ControlTable: "__wallaby_control", DestinationRevisionID: request.ConsumerRevisionID,
		MaxCommitRetries: 4, RequestTimeout: 30 * time.Second, ReconciliationHorizon: 24 * time.Hour,
		S3TablesTableBucketARN: bucketARN, S3TablesConfigureMaintenance: true,
		S3TablesMinSnapshotsToKeep: 100, S3TablesMaxSnapshotAgeHours: 24,
	}
	committer, err := icebergdest.NewS3TablesCommitter(context.Background(), objects, cfg)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := committer.Commit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	reconciled, err := committer.Reconcile(context.Background(), request)
	if err != nil || reconciled.Disposition != artifactlog.CommitApplied {
		t.Fatalf("S3 Tables reconciliation=%+v error=%v", reconciled, err)
	}
	if reconciled.Commit.SnapshotID != committed.SnapshotID {
		t.Fatalf("S3 Tables snapshot receipt changed: %s != %s", reconciled.Commit.SnapshotID, committed.SnapshotID)
	}
}

type icebergLiveObjects struct {
	objects map[string][]byte
}

func (objects *icebergLiveObjects) ReadVersion(_ context.Context, evidence artifactlog.ObjectEvidence) ([]byte, error) {
	value, ok := objects.objects[evidence.Key+"@"+evidence.VersionID]
	if !ok {
		return nil, artifactlog.ErrObjectNotFound
	}
	return append([]byte(nil), value...), nil
}

func icebergLiveRequest(t *testing.T, mappedNamespace, mappedTable string) (artifactlog.CommitRequest, *icebergLiveObjects) {
	t.Helper()
	objects := &icebergLiveObjects{objects: map[string][]byte{}}
	request := icebergLivePlanRequest(t, objects, uuid.New(), "iceberg-live-v1", 1, "0/D0", artifactSourceTransaction(), mappedNamespace, mappedTable)
	return request, objects
}

// icebergLivePlanRequest plans one canonical publication for transaction and
// records its rooted artifacts in objects so a single committer can replay
// several publications (for example an append followed by a schema evolution)
// against the same catalog table.
func icebergLivePlanRequest(t *testing.T, objects *icebergLiveObjects, incarnationID uuid.UUID, revisionID string, publicationSeq int64, position string, transaction connector.SourceTransaction, mappedNamespace, mappedTable string) artifactlog.CommitRequest {
	t.Helper()
	const mappingFingerprint = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	for fragmentIndex := range transaction.Fragments {
		batch := &transaction.Fragments[fragmentIndex].Batch
		batch.Schema.Namespace = mappedNamespace
		batch.Schema.Name = mappedTable
		batch.WritePolicy = connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend, ProjectionFingerprint: mappingFingerprint}
		for recordIndex := range batch.Records {
			batch.Records[recordIndex].Table = mappedTable
		}
	}
	plan, err := artifactlog.NewEncoder().PlanMappedTransaction(context.Background(), incarnationID, mappingFingerprint, transaction)
	if err != nil {
		t.Fatal(err)
	}
	publicationID := uuid.New()
	request := artifactlog.CommitRequest{
		FlowID: "iceberg-live-" + revisionID, FlowIncarnationID: incarnationID,
		ConsumerRevisionID: revisionID, PublicationID: publicationID,
		PublicationSequence: publicationSeq, PositionID: position, CheckpointLSN: position,
		LogicalBatchID: plan.LogicalBatchID, ProjectionID: artifactlog.ProjectionIDV2, MappingFingerprint: mappingFingerprint,
		AttemptedAt: time.Now().Add(-time.Second).UTC(), Barriers: plan.Barriers,
	}
	manifest := sha256.New()
	for _, artifact := range plan.Artifacts {
		evidence := artifactlog.ObjectEvidence{
			Bucket: "canonical", Key: artifact.ObjectKey, VersionID: "version-" + artifact.ID,
			ChecksumSHA256: artifact.EncodedByteHash, Length: int64(len(artifact.Encoded)), ProjectionID: artifactlog.ProjectionIDV2, MappingFingerprint: mappingFingerprint,
		}
		objects.objects[evidence.Key+"@"+evidence.VersionID] = append([]byte(nil), artifact.Encoded...)
		request.Objects = append(request.Objects, artifactlog.RootedArtifact{
			Evidence: evidence, ArtifactID: artifact.ID, LogicalBatchID: artifact.LogicalBatchID,
			Namespace: artifact.Namespace, Table: artifact.Table, SchemaID: artifact.SchemaID,
			SchemaJSON: artifact.SchemaJSON, EncodedByteHash: artifact.EncodedByteHash,
			FragmentOrdinal: artifact.FragmentOrdinal, FirstRecordOrdinal: artifact.FirstRecordOrdinal,
			RecordCount: artifact.RecordCount,
		})
		_, _ = manifest.Write([]byte(artifact.ID))
		_, _ = manifest.Write([]byte{0})
		_, _ = manifest.Write([]byte(artifact.EncodedByteHash))
		_, _ = manifest.Write([]byte{0})
	}
	for _, barrier := range plan.Barriers {
		_, _ = manifest.Write([]byte("barrier"))
		_, _ = manifest.Write([]byte{0})
		_, _ = manifest.Write([]byte(barrier.ContentHash))
		_, _ = manifest.Write([]byte{0})
	}
	request.ManifestSHA256 = hex.EncodeToString(manifest.Sum(nil))
	request.CommitID = artifactlog.DeterministicCommitID(incarnationID, request.ConsumerRevisionID, publicationID, request.ManifestSHA256)
	return request
}
