package snowflake

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/snowflakedb/gosnowflake"
)

var (
	// errStagedWrongByteCollision means a stage object exists at the deterministic
	// path but its bytes differ from the immutable plan. It is fatal and fails
	// closed; the driver never loads or overwrites it.
	errStagedWrongByteCollision = errors.New("staged Snowflake object exists with different bytes")
	// errStagedPartialLoad means Snowflake load history reports a non-complete
	// load (partially loaded, failed, or a row-count mismatch). A partial load can
	// never be adopted as a completed delivery.
	errStagedPartialLoad              = errors.New("staged Snowflake load is partial or failed")
	ErrStagedLoadNotVisibleDiagnostic = errors.New("staged Snowflake diagnostic load history is not yet visible")
)

// Load status vocabulary observed from Snowflake COPY results and history.
const (
	stagedHistoryLoaded          = "LOADED"
	stagedHistoryPartiallyLoaded = "PARTIALLY_LOADED"
	stagedHistoryLoadFailed      = "LOAD_FAILED"
	stagedHistoryLoadInProgress  = "LOAD_IN_PROGRESS"
)

// normalizeStagedLoadStatus canonicalizes a Snowflake load status onto the
// underscore vocabulary above. The two surfaces disagree: a COPY command result
// reports underscore forms (LOADED, PARTIALLY_LOADED, LOAD_FAILED) while
// INFORMATION_SCHEMA.COPY_HISTORY.STATUS reports space forms ("Loaded",
// "Partially loaded", "Load failed", "Load in progress"). Collapsing whitespace
// runs to single underscores maps both onto the same constants, so the async
// COPY_HISTORY remains a diagnostic surface only. Normalization keeps its
// operator diagnostics stable but never authorizes promotion or a receipt.
func normalizeStagedLoadStatus(raw string) string {
	return strings.Join(strings.Fields(strings.ToUpper(raw)), "_")
}

type stageObjectStat struct {
	present   bool
	md5       string
	sizeBytes int64
}

type stageLoadEntry struct {
	present    bool
	status     string
	rowCount   int
	errorCount int
	firstError string
}

type stageCopyResult struct {
	present    bool
	status     string
	rowsLoaded int
	errorsSeen int
	firstError string
}

type stageReceiptInsert struct {
	inserted bool
}

// stageProtocol is the deep seam between the staged COPY driver and Snowflake.
// Every ambiguous transport boundary in the real service is a single method so
// the driver's crash-window recovery is exhaustively testable with a fake.
type stageProtocol interface {
	stagedAuthorityProtocol
	// StatObject reports whether the deterministic stage path holds an object and
	// its Snowflake-reported MD5 checksum and size.
	StatObject(ctx context.Context, stageRef, relativePath string) (stageObjectStat, error)
	// GetObject returns the decrypted, uncompressed object bytes through the
	// Snowflake GET stream and fails if the bounded plaintext size is exceeded.
	GetObject(ctx context.Context, stageRef, relativePath string, maxBytes int) ([]byte, error)
	// PutObject uploads immutable bytes to the deterministic path. It must be
	// idempotent for identical bytes and must return errStagedWrongByteCollision
	// when a different-byte object already occupies the path.
	PutObject(ctx context.Context, stageRef, relativePath string, content []byte, expectedMD5 string) error
	// Copy loads one staged object with the plan's fail-closed options and returns
	// the per-file COPY result. A lost response is reported as an error; the driver
	// then reconciles through LoadHistory.
	Copy(ctx context.Context, plan stagedCopyPlan) (stageCopyResult, error)
	// RefreshPipe asks an auto-ingest pipe to notice a newly staged object.
	RefreshPipe(ctx context.Context, pipeRef, relativePath string) error
	// LoadHistory reports diagnostic COPY history. It never authorizes target
	// promotion, receipt insertion, or retry.
	LoadHistory(ctx context.Context, target, relativePath string) (stageLoadEntry, error)
	// LookupReceipt returns the durable destination receipt for one identity.
	LookupReceipt(ctx context.Context, cfg stagedConfig, key stagedReceiptKey) (managedStagedReceipt, bool, error)
	// InsertReceipt writes the receipt atomically; a duplicate primary key is a
	// non-error signal that a concurrent owner already committed it.
	InsertReceipt(ctx context.Context, cfg stagedConfig, receipt managedStagedReceipt) (stageReceiptInsert, error)
	// ListReleasableReceipts returns load receipts for one flow incarnation that
	// are older than the retention window and not yet released, bounded by limit.
	// Only durably recorded, fully loaded batches are eligible, so cleanup can
	// never remove a stage object whose delivery was not acknowledged.
	ListReleasableReceipts(ctx context.Context, cfg stagedConfig, flowIncarnationID string, retention time.Duration, limit int) ([]managedStagedReceipt, error)
	// RemoveObject deletes one staged object during bounded cleanup.
	RemoveObject(ctx context.Context, stageRef, relativePath string) error
}

// stagedReceiptKey selects a durable receipt by its stable delivery identity and
// kind (load or release).
type stagedReceiptKey struct {
	flowIncarnationID     string
	destinationRevisionID string
	logicalBatchID        string
	sourceLineageID       string
	positionID            string
	externalID            string
	kind                  string
}

// sqlStageProtocol is the real gosnowflake-backed protocol. It is exercised only
// by the credential-gated live recovery matrix; unit and property coverage runs
// against the in-memory fake.
type sqlStageProtocol struct {
	db *sql.DB
}

func newSQLStageProtocol(db *sql.DB) *sqlStageProtocol {
	return &sqlStageProtocol{db: db}
}

func (p *sqlStageProtocol) StatObject(ctx context.Context, stageRef, relativePath string) (stageObjectStat, error) {
	if err := validateStagedObjectReference(stageRef, relativePath); err != nil {
		return stageObjectStat{}, err
	}
	rows, err := p.db.QueryContext(ctx, "LIST @"+stageRef+"/"+relativePath)
	if err != nil {
		return stageObjectStat{}, fmt.Errorf("list staged object: %w", err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return stageObjectStat{}, fmt.Errorf("read staged listing columns: %w", err)
	}
	index := make(map[string]int, len(columns))
	for position, column := range columns {
		index[strings.ToLower(strings.TrimSpace(column))] = position
	}
	nameIndex, hasName := index["name"]
	md5Index, hasMD5 := index["md5"]
	sizeIndex, hasSize := index["size"]
	if !hasName || !hasMD5 || !hasSize {
		return stageObjectStat{}, errors.New("snowflake LIST omitted name, md5, or size")
	}
	suffix := "/" + relativePath
	var found stageObjectStat
	matches := 0
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for position := range values {
			pointers[position] = &values[position]
		}
		if err := rows.Scan(pointers...); err != nil {
			return stageObjectStat{}, fmt.Errorf("scan staged listing: %w", err)
		}
		name := sqlValueString(values[nameIndex])
		// LIST is prefix-scoped and so is GET. Any listed sibling that merely starts
		// with the deterministic path (for example a foreign ".bak" copy) would be
		// downloaded by the byte-equality GET, so it is named here as a conflict
		// instead of surfacing later as an unexplained plaintext mismatch.
		if !strings.HasSuffix(name, suffix) && !strings.HasSuffix(name, relativePath) {
			return stageObjectStat{}, fmt.Errorf("%w: staged path %s prefix also matches foreign object %s", connector.ErrDeliveryConflict, relativePath, name)
		}
		matches++
		found = stageObjectStat{present: true, md5: strings.ToLower(strings.TrimSpace(sqlValueString(values[md5Index]))), sizeBytes: sqlValueInt64(values[sizeIndex])}
	}
	if err := rows.Err(); err != nil {
		return stageObjectStat{}, fmt.Errorf("iterate staged listing: %w", err)
	}
	if matches > 1 {
		return stageObjectStat{}, fmt.Errorf("%w: staged path %s matched %d objects", connector.ErrDeliveryConflict, relativePath, matches)
	}
	return found, nil
}

var (
	// stagedQualifiedObjectPattern is the exact shape produced by
	// managedSnowflakeStagedQualified for validated unquoted uppercase identifiers.
	stagedQualifiedObjectPattern = regexp.MustCompile(`^"[A-Z_][A-Z0-9_$]*"\."[A-Z_][A-Z0-9_$]*"\."[A-Z_][A-Z0-9_$]*"$`)
	// stagedRelativePathPattern is the exact shape produced by
	// newManagedStagedIdentity: hashed, character-restricted path segments only.
	stagedRelativePathPattern = regexp.MustCompile(`^[A-Za-z0-9_.\-]+(?:/[A-Za-z0-9_.\-]+)*$`)
)

// errStagedPlaintextOversize means the staged object returned more plaintext
// than the immutable plan contains. That is definite divergence, not ambiguity.
var errStagedPlaintextOversize = errors.New("staged Snowflake GET exceeded the planned plaintext size")

// validateStagedObjectReference is one allowlist shared by every statement that
// interpolates a stage reference and object path, so no call site can diverge.
func validateStagedObjectReference(stageRef, relativePath string) error {
	if !stagedQualifiedObjectPattern.MatchString(stageRef) {
		return fmt.Errorf("staged Snowflake stage reference %q is not a validated three-part quoted identifier", stageRef)
	}
	if len(relativePath) == 0 || len(relativePath) > 1024 || !stagedRelativePathPattern.MatchString(relativePath) || strings.Contains(relativePath, "..") {
		return fmt.Errorf("staged Snowflake relative path %q is not a validated bounded stage path", relativePath)
	}
	return nil
}

// boundedStageObjectWriter caps what WALlaby retains from a streaming GET at the
// exact planned plaintext size. It is the second of two bounds and not the only
// one: the driver refuses to issue GET at all unless LIST already reported a
// stored size within the planned plaintext plus a fixed encryption-envelope
// allowance. That LIST precheck reduces, but cannot by itself eliminate,
// gosnowflake's internal materialization of the downloaded and decrypted object,
// because the object could in principle change between LIST and GET. Immutable
// content-addressed paths and OVERWRITE=FALSE make that window remote.
type boundedStageObjectWriter struct {
	buffer bytes.Buffer
	limit  int
}

func (w *boundedStageObjectWriter) Write(content []byte) (int, error) {
	remaining := w.limit - w.buffer.Len()
	if remaining <= 0 {
		return 0, errStagedPlaintextOversize
	}
	if len(content) > remaining {
		written, _ := w.buffer.Write(content[:remaining])
		return written, errStagedPlaintextOversize
	}
	return w.buffer.Write(content)
}

// GetObject streams the decrypted, uncompressed object into a plan-sized writer.
// gosnowflake validates the GET local location as an existing directory before it
// runs, even in stream mode, so a private per-call directory is created and
// removed around the statement. Stream-mode GET yields bytes only for a
// client-side-encrypted object, which is why admission requires an INTERNAL
// stage; relaxing that admission check would silently return empty plaintext and
// turn every batch into a spurious conflict. The local location is Unix-shaped
// and single-quoted, so this path assumes a POSIX deployment.
func (p *sqlStageProtocol) GetObject(ctx context.Context, stageRef, relativePath string, maxBytes int) ([]byte, error) {
	if maxBytes < 0 {
		return nil, errors.New("staged Snowflake GET requires a non-negative plaintext bound")
	}
	if err := validateStagedObjectReference(stageRef, relativePath); err != nil {
		return nil, err
	}
	directory, err := os.MkdirTemp("", "wallaby-stage-verify-")
	if err != nil {
		return nil, fmt.Errorf("create staged Snowflake GET download directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(directory) }()
	if strings.ContainsAny(directory, "'\\\r\n") || !strings.HasPrefix(directory, "/") {
		return nil, fmt.Errorf("staged Snowflake GET download directory %q is not a quotable absolute POSIX path", directory)
	}
	writer := &boundedStageObjectWriter{limit: maxBytes}
	getCtx := gosnowflake.WithFileTransferOptions(ctx, &gosnowflake.SnowflakeFileTransferOptions{GetFileToStream: true, RaisePutGetError: true})
	getCtx = gosnowflake.WithFileGetStream(getCtx, writer)
	statement := "GET @" + stageRef + "/" + relativePath + " 'file://" + directory + "'"
	if _, err := p.db.ExecContext(getCtx, statement); err != nil {
		return nil, fmt.Errorf("get staged object for byte verification: %w", err)
	}
	// writer is call-local and never reused, so its buffer can be returned without
	// an additional full-size copy of the plaintext.
	return writer.buffer.Bytes(), nil
}

func (p *sqlStageProtocol) PutObject(ctx context.Context, stageRef, relativePath string, content []byte, expectedMD5 string) error {
	if err := validateStagedObjectReference(stageRef, relativePath); err != nil {
		return err
	}
	existing, err := p.StatObject(ctx, stageRef, relativePath)
	if err != nil {
		return err
	}
	if existing.present {
		if existing.md5 != "" && existing.md5 != strings.ToLower(expectedMD5) {
			return fmt.Errorf("%w: staged path %s md5=%s want %s", errStagedWrongByteCollision, relativePath, existing.md5, expectedMD5)
		}
		return nil
	}
	slash := strings.LastIndexByte(relativePath, '/')
	directory := ""
	fileName := relativePath
	if slash >= 0 {
		directory = relativePath[:slash]
		fileName = relativePath[slash+1:]
	}
	target := "@" + stageRef
	if directory != "" {
		target += "/" + directory
	}
	putCtx := gosnowflake.WithFileStream(ctx, bytes.NewReader(content))
	putCtx = gosnowflake.WithFileTransferOptions(putCtx, &gosnowflake.SnowflakeFileTransferOptions{RaisePutGetError: true})
	statement := fmt.Sprintf("PUT file:///tmp/%s %s AUTO_COMPRESS=FALSE SOURCE_COMPRESSION=NONE OVERWRITE=FALSE", fileName, target)
	if _, err := p.db.ExecContext(putCtx, statement); err != nil {
		return fmt.Errorf("put staged object: %w", err)
	}
	return nil
}

func (p *sqlStageProtocol) Copy(ctx context.Context, plan stagedCopyPlan) (stageCopyResult, error) {
	statement := stagedCopyStatement(plan)
	ctx, recordQueryID := managedSnowflakeQueryIDContext(ctx)
	rows, err := p.db.QueryContext(ctx, statement)
	recordQueryID()
	if err != nil {
		return stageCopyResult{}, fmt.Errorf("copy staged object: %w", err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return stageCopyResult{}, fmt.Errorf("read copy result columns: %w", err)
	}
	index := make(map[string]int, len(columns))
	for position, column := range columns {
		index[strings.ToLower(strings.TrimSpace(column))] = position
	}
	result := stageCopyResult{}
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for position := range values {
			pointers[position] = &values[position]
		}
		if err := rows.Scan(pointers...); err != nil {
			return stageCopyResult{}, fmt.Errorf("scan copy result: %w", err)
		}
		result.present = true
		if position, ok := index["status"]; ok {
			result.status = normalizeStagedLoadStatus(sqlValueString(values[position]))
		}
		if position, ok := index["rows_loaded"]; ok {
			result.rowsLoaded = int(sqlValueInt64(values[position]))
		}
		if position, ok := index["errors_seen"]; ok {
			result.errorsSeen = int(sqlValueInt64(values[position]))
		}
		if position, ok := index["first_error"]; ok {
			result.firstError = sqlValueString(values[position])
		}
	}
	if err := rows.Err(); err != nil {
		return stageCopyResult{}, fmt.Errorf("iterate copy result: %w", err)
	}
	return result, nil
}

// RefreshPipe issues a stage-wide ALTER PIPE ... REFRESH. Snowflake's REFRESH
// re-imports only files modified within the last 7 days and rescans the whole
// stage, so a file that missed its auto-ingest notification and then aged past
// that window becomes unrecoverable through refresh alone. This is an
// auto-ingest-only liveness limitation that the live matrix must characterize
// before the profile leaves experimental; it never compromises fail-closed
// safety because completion is still gated on verifiable load history.
func (p *sqlStageProtocol) RefreshPipe(ctx context.Context, pipeRef, relativePath string) error {
	if strings.TrimSpace(pipeRef) == "" {
		return errors.New("staged Snowflake pipe reference is required for auto-ingest refresh")
	}
	if !stagedRelativePathPattern.MatchString(relativePath) || strings.Contains(relativePath, "..") {
		return errors.New("staged Snowflake pipe refresh path is invalid")
	}
	prefix := relativePath
	if slash := strings.LastIndexByte(relativePath, '/'); slash >= 0 {
		prefix = relativePath[:slash+1]
	}
	if _, err := p.db.ExecContext(ctx, "ALTER PIPE "+pipeRef+" REFRESH PREFIX = '"+prefix+"'"); err != nil {
		return fmt.Errorf("refresh staged Snowflake pipe: %w", err)
	}
	return nil
}

func (p *sqlStageProtocol) LoadHistory(ctx context.Context, target, relativePath string) (stageLoadEntry, error) {
	fileName := relativePath
	if slash := strings.LastIndexByte(relativePath, '/'); slash >= 0 {
		fileName = relativePath[slash+1:]
	}
	query := `SELECT STATUS, ROW_COUNT, ERROR_COUNT, COALESCE(FIRST_ERROR_MESSAGE, '')
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME=>?, START_TIME=>DATEADD('day', -14, CURRENT_TIMESTAMP())))
WHERE ENDSWITH(FILE_NAME, ?)
ORDER BY LAST_LOAD_TIME DESC
LIMIT 1`
	var status string
	var rowCount, errorCount int64
	var firstError string
	err := p.db.QueryRowContext(ctx, query, target, "/"+fileName).Scan(&status, &rowCount, &errorCount, &firstError)
	if errors.Is(err, sql.ErrNoRows) {
		return stageLoadEntry{}, nil
	}
	if err != nil {
		return stageLoadEntry{}, fmt.Errorf("read staged load history: %w", err)
	}
	// FILE_NAME in COPY_HISTORY carries the stage-relative path; matching on the
	// basename is safe because that basename embeds the full content hash, which
	// binds LOGICAL_BATCH_ID, so no two distinct batches can share a basename.
	return stageLoadEntry{present: true, status: normalizeStagedLoadStatus(status), rowCount: int(rowCount), errorCount: int(errorCount), firstError: firstError}, nil
}

func (p *sqlStageProtocol) LookupReceipt(ctx context.Context, cfg stagedConfig, key stagedReceiptKey) (managedStagedReceipt, bool, error) {
	queryCtx, recordQueryID := managedSnowflakeQueryIDContext(ctx)
	rows, err := p.db.QueryContext(queryCtx, stagedReceiptLookupSQL(cfg),
		key.kind, key.flowIncarnationID, key.destinationRevisionID, key.logicalBatchID,
		key.kind, key.flowIncarnationID, key.destinationRevisionID, key.sourceLineageID, key.positionID,
		key.externalID,
	)
	recordQueryID()
	if err != nil {
		return managedStagedReceipt{}, false, fmt.Errorf("query staged Snowflake receipt: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var receipts []managedStagedReceipt
	for rows.Next() {
		receipt, err := scanStagedReceipt(rows)
		if err != nil {
			return managedStagedReceipt{}, false, err
		}
		receipts = append(receipts, receipt)
		if len(receipts) > 1 {
			return managedStagedReceipt{}, false, fmt.Errorf("%w: multiple staged Snowflake receipts match one delivery identity", connector.ErrDeliveryConflict)
		}
	}
	if err := rows.Err(); err != nil {
		return managedStagedReceipt{}, false, fmt.Errorf("iterate staged Snowflake receipts: %w", err)
	}
	if len(receipts) == 0 {
		return managedStagedReceipt{}, false, nil
	}
	return receipts[0], true, nil
}

func (p *sqlStageProtocol) InsertReceipt(ctx context.Context, cfg stagedConfig, receipt managedStagedReceipt) (stageReceiptInsert, error) {
	ctx, recordQueryID := managedSnowflakeQueryIDContext(ctx)
	result, err := p.db.ExecContext(ctx, stagedReceiptInsertSQL(cfg), stagedReceiptValues(receipt)...)
	recordQueryID()
	if err != nil {
		if isStagedDuplicateKey(err) {
			return stageReceiptInsert{inserted: false}, nil
		}
		return stageReceiptInsert{}, fmt.Errorf("%w: insert staged Snowflake receipt: %w", connector.ErrDeliveryIndeterminate, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return stageReceiptInsert{}, fmt.Errorf("read staged Snowflake receipt cardinality: %w", err)
	}
	if affected != 1 {
		return stageReceiptInsert{}, fmt.Errorf("staged Snowflake receipt insert affected %d rows, want exactly 1", affected)
	}
	return stageReceiptInsert{inserted: true}, nil
}

func (p *sqlStageProtocol) ListReleasableReceipts(ctx context.Context, cfg stagedConfig, flowIncarnationID string, retention time.Duration, limit int) ([]managedStagedReceipt, error) {
	table := managedSnowflakeStagedQualifiedTable(cfg, cfg.receiptsTable)
	// #nosec G202 -- the receipts table identifier is composed only of validated unquoted uppercase identifiers; all values are bound parameters.
	query := "SELECT " + stagedReceiptColumnsQualified("L") + " FROM " + table + " AS L" +
		" WHERE L.\"RECEIPT_KIND\" = ? AND L.\"FLOW_INCARNATION_ID\" = ? AND L.\"LOAD_STATUS\" = ?" +
		" AND L.\"COMMITTED_AT\" < DATEADD('second', ?, CURRENT_TIMESTAMP())" +
		" AND NOT EXISTS (SELECT 1 FROM " + table + " AS R WHERE R.\"RECEIPT_KIND\" = ? AND R.\"EXTERNAL_ID\" = L.\"EXTERNAL_ID\" || ':release')" +
		" ORDER BY L.\"COMMITTED_AT\" LIMIT " + strconv.Itoa(limit)
	rows, err := p.db.QueryContext(ctx, query,
		stagedReceiptKindLoad, flowIncarnationID, stagedLoadStatusLoaded,
		-int64(retention/time.Second), stagedReceiptKindRelease,
	)
	if err != nil {
		return nil, fmt.Errorf("list releasable staged receipts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var receipts []managedStagedReceipt
	for rows.Next() {
		receipt, err := scanStagedReceipt(rows)
		if err != nil {
			return nil, err
		}
		receipts = append(receipts, receipt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate releasable staged receipts: %w", err)
	}
	return receipts, nil
}

func (p *sqlStageProtocol) RemoveObject(ctx context.Context, stageRef, relativePath string) error {
	if _, err := p.db.ExecContext(ctx, "REMOVE @"+stageRef+"/"+relativePath); err != nil {
		return fmt.Errorf("remove staged object: %w", err)
	}
	return nil
}

func isStagedDuplicateKey(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "duplicate") || strings.Contains(message, "unique constraint") || strings.Contains(message, "primary key")
}

func sqlValueInt64(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int32:
		return int64(typed)
	case int:
		return int64(typed)
	case float64:
		return int64(typed)
	case []byte:
		parsed, _ := parseInt64(string(typed))
		return parsed
	case string:
		parsed, _ := parseInt64(typed)
		return parsed
	default:
		return 0
	}
}

func parseInt64(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	var result int64
	_, err := fmt.Sscan(value, &result)
	return result, err
}
