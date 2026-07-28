package snowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestManagedSnowflakeValidatesLivePostgresSchemaBeforeWAL(t *testing.T) {
	t.Parallel()
	cfg, _ := managedCatalogFixture(t)
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	destination := &Destination{db: db, managedProfile: cfg.profile, managedConfig: cfg}
	live := managedTestSchema()
	live.Columns[0].Type = "bigint"
	live.Columns[0].TypeMetadata["replica_identity"] = "true"
	if err := destination.ValidateManagedSourceSchema(live); err != nil {
		t.Fatal(err)
	}
	live.Columns[1].Nullable = false
	if err := destination.ValidateManagedSourceSchema(live); !errors.Is(err, errManagedSnowflakeSchemaNotReconciled) {
		t.Fatalf("live source drift error=%v", err)
	}
}

func TestManagedSnowflakeFlowScopeRejectsForeignIncarnationReceipts(t *testing.T) {
	t.Parallel()
	cfg, _ := managedCatalogFixture(t)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	destination := &Destination{db: db, managedProfile: cfg.profile, managedConfig: cfg}
	query := "SELECT COUNT(*) FROM " + managedSnowflakeQualifiedTable(cfg, cfg.receiptsTable) +
		" WHERE \"FLOW_ID\" <> ? OR \"FLOW_INCARNATION_ID\" <> ?"
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(cfg.flowID, "incarnation-1").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	if err := destination.ValidateManagedFlowScope(context.Background(), cfg.flowID, "incarnation-1"); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("foreign flow scope error=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestManagedSnowflakeApplyCommitsOrderedTransactionAndReceiptAtomically(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg, _ := managedCatalogFixture(t)
	transaction := managedTestTransaction(cfg.schemaContract)
	intent := managedTestIntent(t, transaction)
	plan, err := planManagedSnowflakeTransaction(cfg, intent, transaction)
	if err != nil {
		t.Fatal(err)
	}
	destination := &Destination{db: db, managedProfile: cfg.profile, managedConfig: cfg}
	prepared := &preparedManagedSnowflakeTransaction{destination: destination, intent: intent, plan: plan}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(managedReceiptLookupSQL(cfg))).
		WithArgs(intent.FlowIncarnationID, intent.DestinationRevisionID, intent.LogicalBatchID, intent.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, intent.PositionID, plan.receipt.externalID).
		WillReturnRows(sqlmock.NewRows(managedReceiptLookupColumns()))
	mock.ExpectExec(regexp.QuoteMeta(managedReceiptInsertSQL(cfg))).
		WithArgs(managedDriverValues(managedReceiptValues(plan.receipt))...).
		WillReturnResult(sqlmock.NewResult(0, 1))
	for _, operation := range plan.operations {
		mock.ExpectExec(regexp.QuoteMeta(operation.query)).WithArgs(managedDriverValues(operation.args)...).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectCommit()

	evidence, err := prepared.Apply(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if evidence.ExternalID != plan.receipt.externalID || evidence.ContentHash != intent.ContentHash {
		t.Fatalf("evidence=%+v", evidence)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestManagedSnowflakeAmbiguousCommitReconcilesAndConflictsFailClosed(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg, _ := managedCatalogFixture(t)
	transaction := managedTestTransaction(cfg.schemaContract)
	intent := managedTestIntent(t, transaction)
	plan, err := planManagedSnowflakeTransaction(cfg, intent, transaction)
	if err != nil {
		t.Fatal(err)
	}
	destination := &Destination{db: db, managedProfile: cfg.profile, managedConfig: cfg}
	destination.SetManagedHooks(ManagedHooks{AfterCommit: func() error { return errors.New("response lost after COMMIT") }})
	prepared := &preparedManagedSnowflakeTransaction{destination: destination, intent: intent, plan: plan}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(managedReceiptLookupSQL(cfg))).WillReturnRows(sqlmock.NewRows(managedReceiptLookupColumns()))
	mock.ExpectExec(regexp.QuoteMeta(managedReceiptInsertSQL(cfg))).WillReturnResult(sqlmock.NewResult(0, 1))
	for _, operation := range plan.operations {
		mock.ExpectExec(regexp.QuoteMeta(operation.query)).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectCommit()
	if _, err := prepared.Apply(context.Background()); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("ambiguous commit error=%v", err)
	}

	destination.SetManagedHooks(ManagedHooks{})
	mock.ExpectQuery(regexp.QuoteMeta(managedReceiptLookupSQL(cfg))).
		WillReturnRows(managedReceiptRows(plan.receipt))
	disposition, evidence, err := destination.Reconcile(context.Background(), intent)
	if err != nil || disposition != connector.DeliveryApplied || evidence.ExternalID != plan.receipt.externalID {
		t.Fatalf("reconcile disposition/evidence/error=%v/%+v/%v", disposition, evidence, err)
	}

	conflict := plan.receipt
	conflict.manifestHash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	mock.ExpectQuery(regexp.QuoteMeta(managedReceiptLookupSQL(cfg))).
		WillReturnRows(managedReceiptRows(conflict))
	if _, _, err := destination.Reconcile(context.Background(), intent); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("conflicting receipt error=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestManagedSnowflakeApplyAdoptsMatchingReceiptFromPriorGeneration(t *testing.T) {
	t.Parallel()
	cfg, _ := managedCatalogFixture(t)
	transaction := managedTestTransaction(cfg.schemaContract)
	intent := managedTestIntent(t, transaction)
	intent.Generation = 2
	intent.AcquisitionID = "new-acquisition"
	intent.LeaseEpoch = 2
	plan, err := planManagedSnowflakeTransaction(cfg, intent, transaction)
	if err != nil {
		t.Fatal(err)
	}
	prior := plan.receipt
	prior.generation = 1
	prior.acquisitionID = "old-acquisition"
	prior.leaseEpoch = 1

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	destination := &Destination{db: db, managedProfile: cfg.profile, managedConfig: cfg}
	prepared := &preparedManagedSnowflakeTransaction{destination: destination, intent: intent, plan: plan}
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(managedReceiptLookupSQL(cfg))).WillReturnRows(managedReceiptRows(prior))
	mock.ExpectRollback()
	if _, err := prepared.Apply(context.Background()); err != nil {
		t.Fatalf("matching prior-generation receipt was not adopted: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestManagedSnowflakeApplyRollsBackDMLAndReceiptBeforeCommitCancellation(t *testing.T) {
	t.Parallel()
	cfg, _ := managedCatalogFixture(t)
	transaction := managedTestTransaction(cfg.schemaContract)
	intent := managedTestIntent(t, transaction)
	plan, err := planManagedSnowflakeTransaction(cfg, intent, transaction)
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	destination := &Destination{db: db, managedProfile: cfg.profile, managedConfig: cfg}
	destination.SetManagedHooks(ManagedHooks{BeforeCommit: func() error { return context.Canceled }})
	prepared := &preparedManagedSnowflakeTransaction{destination: destination, intent: intent, plan: plan}
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(managedReceiptLookupSQL(cfg))).WillReturnRows(sqlmock.NewRows(managedReceiptLookupColumns()))
	mock.ExpectExec(regexp.QuoteMeta(managedReceiptInsertSQL(cfg))).WillReturnResult(sqlmock.NewResult(0, 1))
	for _, operation := range plan.operations {
		mock.ExpectExec(regexp.QuoteMeta(operation.query)).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectRollback()
	if _, err := prepared.Apply(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("before-commit cancellation error=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestManagedSnowflakeApplyRollsBackOnCardinalityOrCancellation(t *testing.T) {
	t.Parallel()
	cfg, _ := managedCatalogFixture(t)
	transaction := managedTestTransaction(cfg.schemaContract)
	intent := managedTestIntent(t, transaction)
	plan, err := planManagedSnowflakeTransaction(cfg, intent, transaction)
	if err != nil {
		t.Fatal(err)
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	destination := &Destination{db: db, managedProfile: cfg.profile, managedConfig: cfg}
	prepared := &preparedManagedSnowflakeTransaction{destination: destination, intent: intent, plan: plan}
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(managedReceiptLookupSQL(cfg))).WillReturnRows(sqlmock.NewRows(managedReceiptLookupColumns()))
	mock.ExpectExec(regexp.QuoteMeta(managedReceiptInsertSQL(cfg))).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(plan.operations[0].query)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	if _, err := prepared.Apply(context.Background()); err == nil || errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("cardinality error=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	canceledDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer canceledDB.Close()
	destination.db = canceledDB
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := prepared.Apply(ctx); !errors.Is(err, context.Canceled) && !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("canceled apply error=%v", err)
	}
}

func TestManagedSnowflakeCommitErrorDiscardsSessionBeforeReconciliation(t *testing.T) {
	t.Parallel()
	cfg, _ := managedCatalogFixture(t)
	transaction := managedTestTransaction(cfg.schemaContract)
	intent := managedTestIntent(t, transaction)
	plan, err := planManagedSnowflakeTransaction(cfg, intent, transaction)
	if err != nil {
		t.Fatal(err)
	}
	state := &ambiguousCommitDriverState{failCommit: true}
	db := sql.OpenDB(ambiguousCommitConnector{state: state})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer db.Close()
	destination := &Destination{db: db, managedProfile: cfg.profile, managedConfig: cfg}
	prepared := &preparedManagedSnowflakeTransaction{destination: destination, intent: intent, plan: plan}

	if _, err := prepared.Apply(context.Background()); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("ambiguous commit error=%v", err)
	}
	disposition, _, err := destination.Reconcile(context.Background(), intent)
	if err != nil {
		t.Fatal(err)
	}
	if disposition != connector.DeliveryNotApplied {
		t.Fatalf("reconciliation on replacement session=%v, want not applied", disposition)
	}
	state.mu.Lock()
	opens, closes := state.opens, state.closes
	state.mu.Unlock()
	if opens < 2 || closes < 1 {
		t.Fatalf("physical Snowflake sessions opened/closed=%d/%d, want replacement after ambiguous COMMIT", opens, closes)
	}
}

type ambiguousCommitConnector struct {
	state *ambiguousCommitDriverState
}

func (c ambiguousCommitConnector) Connect(context.Context) (driver.Conn, error) {
	c.state.mu.Lock()
	c.state.opens++
	c.state.mu.Unlock()
	return &ambiguousCommitConn{state: c.state}, nil
}

func (c ambiguousCommitConnector) Driver() driver.Driver {
	return ambiguousCommitDriver(c)
}

type ambiguousCommitDriver struct {
	state *ambiguousCommitDriverState
}

func (d ambiguousCommitDriver) Open(string) (driver.Conn, error) {
	return ambiguousCommitConnector(d).Connect(context.Background())
}

type ambiguousCommitDriverState struct {
	mu         sync.Mutex
	failCommit bool
	opens      int
	closes     int
	committed  []driver.Value
}

type ambiguousCommitConn struct {
	state       *ambiguousCommitDriverState
	active      bool
	uncommitted []driver.Value
	closed      bool
}

func (c *ambiguousCommitConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepared statements are not supported by the test driver")
}

func (c *ambiguousCommitConn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	c.active = false
	c.uncommitted = nil
	c.state.mu.Lock()
	c.state.closes++
	c.state.mu.Unlock()
	return nil
}

func (c *ambiguousCommitConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *ambiguousCommitConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	if c.closed || c.active {
		return nil, errors.New("test connection cannot begin transaction")
	}
	c.active = true
	return ambiguousCommitTx{conn: c}, nil
}

func (c *ambiguousCommitConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if !c.active {
		return nil, errors.New("test execution requires transaction")
	}
	if strings.Contains(query, `"WALLABY_RECEIPTS"`) {
		c.uncommitted = make([]driver.Value, len(args))
		for index, argument := range args {
			c.uncommitted[index] = argument.Value
		}
	}
	return driver.RowsAffected(1), nil
}

func (c *ambiguousCommitConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(query, `"WALLABY_RECEIPTS"`) {
		return nil, fmt.Errorf("unexpected test query %q", query)
	}
	values := c.uncommitted
	if len(values) == 0 {
		c.state.mu.Lock()
		values = append([]driver.Value(nil), c.state.committed...)
		c.state.mu.Unlock()
	}
	rows := &ambiguousCommitRows{columns: managedReceiptLookupColumns()}
	if len(values) != 0 {
		rows.values = values
	}
	return rows, nil
}

type ambiguousCommitTx struct {
	conn *ambiguousCommitConn
}

func (tx ambiguousCommitTx) Commit() error {
	tx.conn.state.mu.Lock()
	defer tx.conn.state.mu.Unlock()
	if tx.conn.state.failCommit {
		tx.conn.state.failCommit = false
		// Simulate the ordinary gosnowflake transport-error path: the server-side
		// transaction remains active and the driver does not return ErrBadConn.
		return errors.New("commit response lost with server transaction still active")
	}
	tx.conn.state.committed = append([]driver.Value(nil), tx.conn.uncommitted...)
	tx.conn.active = false
	tx.conn.uncommitted = nil
	return nil
}

func (tx ambiguousCommitTx) Rollback() error {
	tx.conn.active = false
	tx.conn.uncommitted = nil
	return nil
}

type ambiguousCommitRows struct {
	columns []string
	values  []driver.Value
	read    bool
}

func (r *ambiguousCommitRows) Columns() []string { return r.columns }
func (*ambiguousCommitRows) Close() error        { return nil }
func (r *ambiguousCommitRows) Next(destination []driver.Value) error {
	if r.read || len(r.values) == 0 {
		return io.EOF
	}
	r.read = true
	copy(destination, r.values)
	return nil
}

func managedReceiptRows(receipt managedSnowflakeReceipt) *sqlmock.Rows {
	return sqlmock.NewRows(managedReceiptLookupColumns()).AddRow(managedDriverValues(managedReceiptValues(receipt))...)
}

func managedDriverValues(values []any) []driver.Value {
	result := make([]driver.Value, len(values))
	for index, value := range values {
		result[index] = value
	}
	return result
}
