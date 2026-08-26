package clickhouse

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	chclient "github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type managedReceiptTestQueryer struct {
	row   managedReceiptTestRow
	calls int
}

func (q *managedReceiptTestQueryer) QueryRow(context.Context, string, ...any) chdriver.Row {
	q.calls++
	return q.row
}

type managedRecoveryTestConn struct {
	chdriver.Conn
	row        managedReceiptTestRow
	calls      int
	closeCalls int
}

func (c *managedRecoveryTestConn) Close() error {
	c.closeCalls++
	return nil
}

func (c *managedRecoveryTestConn) QueryRow(context.Context, string, ...any) chdriver.Row {
	c.calls++
	return c.row
}

type managedReceiptTestRow struct {
	contentHash string
	externalID  string
	err         error
}

func (r managedReceiptTestRow) Err() error { return r.err }

func (r managedReceiptTestRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 2 {
		return errors.New("managed receipt test row requires two destinations")
	}
	contentHash, contentOK := dest[0].(*string)
	externalID, externalOK := dest[1].(*string)
	if !contentOK || !externalOK {
		return errors.New("managed receipt test row requires string destinations")
	}
	*contentHash = r.contentHash
	*externalID = r.externalID
	return nil
}

func (r managedReceiptTestRow) ScanStruct(any) error { return r.err }

func planManagedTransaction(intent connector.DeliveryIntent, transaction connector.SourceTransaction) (managedTransactionPlan, error) {
	return planManagedTransactionWithLimits(intent, transaction, managedPlanLimits{
		maxFragments: 128, maxRows: 100000, maxBytes: 128 << 20,
		maxRowsPerInsert: 10000, maxBytesPerInsert: 16 << 20,
	})
}

func TestManagedWriteSettingsAllowConcurrentQuorumTwoInserts(t *testing.T) {
	t.Parallel()

	settings := managedWriteSettings(2, "dedup-token")
	for name, want := range map[string]any{
		"async_insert":               uint64(0),
		"wait_for_async_insert":      uint64(1),
		"insert_deduplicate":         uint64(1),
		"insert_deduplication_token": "dedup-token",
		"insert_quorum":              uint64(2),
		"insert_quorum_parallel":     uint64(1),
	} {
		if got := settings[name]; got != want {
			t.Fatalf("setting %s=%v (%T), want %v (%T)", name, got, got, want, want)
		}
	}
}

func TestManagedWriteTransportFailureFallsBackToReplica(t *testing.T) {
	t.Parallel()

	transportErr := &net.OpError{Op: "write", Net: "tcp", Err: syscall.ECONNRESET}
	primaryCalls := 0
	replicaCalls := 0
	err := executeManagedWriteWithFailover(context.Background(), true, func() error {
		primaryCalls++
		return transportErr
	}, func() error {
		replicaCalls++
		return nil
	})
	if err != nil {
		t.Fatalf("transport failover: %v", err)
	}
	if primaryCalls != 1 || replicaCalls != 1 {
		t.Fatalf("write calls=(primary:%d replica:%d), want (1,1)", primaryCalls, replicaCalls)
	}

	serverErr := errors.New("server rejected insert")
	replicaCalls = 0
	if err := executeManagedWriteWithFailover(context.Background(), true, func() error { return serverErr }, func() error {
		replicaCalls++
		return nil
	}); !errors.Is(err, serverErr) {
		t.Fatalf("server error=%v, want original error", err)
	}
	if replicaCalls != 0 {
		t.Fatalf("replica calls for non-transport failure=%d, want zero", replicaCalls)
	}

	replicaErr := errors.New("replica rejected retry")
	err = executeManagedWriteWithFailover(context.Background(), true, func() error { return transportErr }, func() error { return replicaErr })
	if !errors.Is(err, connector.ErrDeliveryIndeterminate) || !errors.Is(err, transportErr) || !errors.Is(err, replicaErr) {
		t.Fatalf("dual endpoint error=%v, want indeterminate preserving both causes", err)
	}
}

func TestManagedReconcileFallsBackToSurvivingReplica(t *testing.T) {
	t.Parallel()

	transaction := managedTestTransaction()
	intent := managedTestIntent(t, transaction)
	transportErr := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
	primary := &managedReceiptTestQueryer{row: managedReceiptTestRow{err: transportErr}}
	replica := &managedReceiptTestQueryer{row: managedReceiptTestRow{
		contentHash: intent.ContentHash,
		externalID:  managedDeliveryExternalID(intent),
	}}
	disposition, evidence, err := reconcileManagedReceiptEndpoints(context.Background(), primary, replica, "SELECT receipt", "query-primary", intent)
	if err != nil || disposition != connector.DeliveryApplied {
		t.Fatalf("surviving replica reconciliation=(%v,%+v,%v)", disposition, evidence, err)
	}
	if evidence.ContentHash != intent.ContentHash || evidence.ExternalID != managedDeliveryExternalID(intent) {
		t.Fatalf("surviving replica evidence=%+v", evidence)
	}
	if primary.calls != 1 || replica.calls != 1 {
		t.Fatalf("reconcile calls=(primary:%d replica:%d), want (1,1)", primary.calls, replica.calls)
	}

	absentPrimary := &managedReceiptTestQueryer{row: managedReceiptTestRow{err: sql.ErrNoRows}}
	matchingReplicaAfterAbsence := &managedReceiptTestQueryer{row: replica.row}
	disposition, evidence, err = reconcileManagedReceiptEndpoints(context.Background(), absentPrimary, matchingReplicaAfterAbsence, "SELECT receipt", "query-primary", intent)
	if err != nil || disposition != connector.DeliveryApplied || evidence.ContentHash != intent.ContentHash {
		t.Fatalf("replica receipt after primary absence=(%v,%+v,%v)", disposition, evidence, err)
	}

	absentReplica := &managedReceiptTestQueryer{row: managedReceiptTestRow{err: sql.ErrNoRows}}
	disposition, evidence, err = reconcileManagedReceiptEndpoints(context.Background(), primary, absentReplica, "SELECT receipt", "query-primary", intent)
	if err != nil || disposition != connector.DeliveryNotApplied || evidence.ContentHash != "" {
		t.Fatalf("survivor absence reconciliation=(%v,%+v,%v)", disposition, evidence, err)
	}

	unreadableReplicaErr := errors.New("replica query unavailable")
	unreadableReplica := &managedReceiptTestQueryer{row: managedReceiptTestRow{err: unreadableReplicaErr}}
	disposition, _, err = reconcileManagedReceiptEndpoints(context.Background(), primary, unreadableReplica, "SELECT receipt", "query-primary", intent)
	if disposition != connector.DeliveryIndeterminate || !errors.Is(err, connector.ErrDeliveryIndeterminate) || !errors.Is(err, transportErr) || !errors.Is(err, unreadableReplicaErr) {
		t.Fatalf("dual endpoint reconciliation=(%v,%v), want indeterminate preserving both causes", disposition, err)
	}

	matchingPrimary := &managedReceiptTestQueryer{row: replica.row}
	conflictingReplica := &managedReceiptTestQueryer{row: managedReceiptTestRow{contentHash: strings.Repeat("f", 64), externalID: "wrong"}}
	disposition, _, err = reconcileManagedReceiptEndpoints(context.Background(), matchingPrimary, conflictingReplica, "SELECT receipt", "query-primary", intent)
	if disposition != connector.DeliveryIndeterminate || !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("replica conflict after matching primary=(%v,%v)", disposition, err)
	}

	conflictingPrimary := &managedReceiptTestQueryer{row: managedReceiptTestRow{contentHash: strings.Repeat("f", 64), externalID: "wrong"}}
	matchingReplica := &managedReceiptTestQueryer{row: replica.row}
	disposition, _, err = reconcileManagedReceiptEndpoints(context.Background(), conflictingPrimary, matchingReplica, "SELECT receipt", "query-primary", intent)
	if disposition != connector.DeliveryIndeterminate || !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("conflicting primary reconciliation=(%v,%v)", disposition, err)
	}
	if matchingReplica.calls != 0 {
		t.Fatalf("replica calls after immutable conflict=%d, want zero", matchingReplica.calls)
	}
}

func TestManagedTransactionPlanPreservesOrderedFragmentsAndLogicalIdentity(t *testing.T) {
	t.Parallel()
	transaction := managedTestTransaction()
	intent := managedTestIntent(t, transaction)

	plan, err := planManagedTransactionWithLimits(intent, transaction, managedPlanLimits{
		maxFragments: 128, maxRows: 100, maxBytes: 1 << 20, maxRowsPerInsert: 2, maxBytesPerInsert: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Fragments) != 2 || plan.Fragments[0].Ordinal != 0 || plan.Fragments[1].Ordinal != 1 {
		t.Fatalf("bounded insert order=%+v", plan.Fragments)
	}
	if len(plan.Fragments[0].Rows) != 2 || len(plan.Fragments[1].Rows) != 1 {
		t.Fatalf("bounded insert row counts=%d/%d", len(plan.Fragments[0].Rows), len(plan.Fragments[1].Rows))
	}
	first := plan.Fragments[0].Rows[0]
	if first.LogicalBatchID != intent.LogicalBatchID || first.FragmentOrdinal != 0 || first.RecordOrdinal != 0 {
		t.Fatalf("first identity=%+v", first)
	}
	if first.Operation != "update" || first.Tombstone != 0 || plan.Fragments[0].Rows[1].Tombstone != 1 || plan.Fragments[1].Rows[0].FragmentOrdinal != 1 {
		t.Fatalf("operation/tombstone=%q/%d delete=%d final_fragment=%d", first.Operation, first.Tombstone, plan.Fragments[0].Rows[1].Tombstone, plan.Fragments[1].Rows[0].FragmentOrdinal)
	}
	if plan.Receipt.FragmentCount != 2 || plan.Receipt.RecordCount != 3 || plan.Receipt.ContentHash != intent.ContentHash {
		t.Fatalf("receipt=%+v", plan.Receipt)
	}
	if plan.Fragments[0].QueryID == plan.Fragments[1].QueryID || plan.Fragments[0].DeduplicationToken == plan.Fragments[1].DeduplicationToken {
		t.Fatal("ordered fragments reused query or deduplication identity")
	}
	if len(plan.Fragments[0].QueryID) > 128 || !strings.HasPrefix(plan.Fragments[0].QueryID, "wallaby-ch-") {
		t.Fatalf("query_id=%q", plan.Fragments[0].QueryID)
	}

	replay, err := planManagedTransactionWithLimits(intent, transaction, managedPlanLimits{
		maxFragments: 128, maxRows: 100, maxBytes: 1 << 20, maxRowsPerInsert: 2, maxBytesPerInsert: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if replay.Fragments[0].QueryID != plan.Fragments[0].QueryID || replay.Fragments[0].Rows[0].RecordHash != first.RecordHash {
		t.Fatal("replayed managed identities changed")
	}
}

func TestManagedTransactionPlanEnforcesTransactionAndInsertBounds(t *testing.T) {
	t.Parallel()
	transaction := managedTestTransaction()
	intent := managedTestIntent(t, transaction)

	tests := []struct {
		name   string
		limits managedPlanLimits
		want   string
	}{
		{name: "fragments", limits: managedPlanLimits{maxFragments: 1, maxRows: 100, maxBytes: 1 << 20, maxRowsPerInsert: 100, maxBytesPerInsert: 1 << 20}, want: "fragments"},
		{name: "rows", limits: managedPlanLimits{maxFragments: 128, maxRows: 2, maxBytes: 1 << 20, maxRowsPerInsert: 100, maxBytesPerInsert: 1 << 20}, want: "rows"},
		{name: "bytes", limits: managedPlanLimits{maxFragments: 128, maxRows: 100, maxBytes: 1, maxRowsPerInsert: 100, maxBytesPerInsert: 1 << 20}, want: "encoded bytes"},
		{name: "single row bytes", limits: managedPlanLimits{maxFragments: 128, maxRows: 100, maxBytes: 1 << 20, maxRowsPerInsert: 100, maxBytesPerInsert: 1}, want: "single row"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := planManagedTransactionWithLimits(intent, transaction, test.limits); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v, want %q", err, test.want)
			}
		})
	}
}

func TestManagedTransactionPlanCanonicalizesProcessLocalSchemaVersions(t *testing.T) {
	t.Parallel()
	firstTransaction := managedTestTransaction()
	firstIntent := managedTestIntent(t, firstTransaction)
	firstPlan, err := planManagedTransaction(firstIntent, firstTransaction)
	if err != nil {
		t.Fatal(err)
	}

	replay := managedTestTransaction()
	for fragmentIndex := range replay.Fragments {
		replay.Fragments[fragmentIndex].Batch.Schema.Version += 1000
		for recordIndex := range replay.Fragments[fragmentIndex].Batch.Records {
			replay.Fragments[fragmentIndex].Batch.Records[recordIndex].SchemaVersion += 1000
		}
	}
	replayIntent := managedTestIntent(t, replay)
	if replayIntent.ContentHash != firstIntent.ContentHash || replayIntent.LogicalBatchID != firstIntent.LogicalBatchID {
		t.Fatal("process-local schema versions changed the source transaction identity")
	}
	replayPlan, err := planManagedTransaction(replayIntent, replay)
	if err != nil {
		t.Fatal(err)
	}
	firstRow := firstPlan.Fragments[0].Rows[0]
	replayRow := replayPlan.Fragments[0].Rows[0]
	if replayRow.SchemaVersion != firstRow.SchemaVersion || replayRow.SchemaJSON != firstRow.SchemaJSON || replayRow.SchemaFingerprint != firstRow.SchemaFingerprint || replayRow.RecordHash != firstRow.RecordHash {
		t.Fatalf("replay-stable schema identity changed:\nfirst=%+v\nreplay=%+v", firstRow, replayRow)
	}
}

func TestManagedInsertFragmentTreatsEmptyPlanAsNoop(t *testing.T) {
	t.Parallel()
	if err := (&Destination{}).insertManagedFragment(context.Background(), managedFragmentPlan{}); err != nil {
		t.Fatalf("empty managed fragment: %v", err)
	}
}

func TestManagedEnvelopePreservesAdmittedPostgresTypes(t *testing.T) {
	t.Parallel()
	transaction := managedTestTransaction()
	intent := managedTestIntent(t, transaction)
	plan, err := planManagedTransaction(intent, transaction)
	if err != nil {
		t.Fatal(err)
	}
	row := plan.Fragments[0].Rows[0]
	var after map[string]json.RawMessage
	if err := json.Unmarshal([]byte(row.AfterJSON), &after); err != nil {
		t.Fatal(err)
	}
	checks := map[string]string{
		"nullable": "null",
		"date":     `"1960-01-02T00:00:00Z"`,
		"numeric":  "12345678901234567890.123456789",
		"uuid":     `"12345678-90ab-cdef-0123-456789abcdef"`,
		"bytea":    `"AP+A"`,
		"array":    `[1,null,"x"]`,
		"json":     `{"a":1,"b":[true,false]}`,
	}
	for key, want := range checks {
		if got := string(after[key]); got != want {
			t.Fatalf("%s=%s, want %s", key, got, want)
		}
	}
	if got := string(after["timestamptz"]); !strings.Contains(got, "-05:00") {
		t.Fatalf("timestamptz=%s, want source offset", got)
	}
	if len(row.SchemaFingerprint) != 64 || len(row.RecordHash) != 64 {
		t.Fatalf("schema/record hashes=%q/%q", row.SchemaFingerprint, row.RecordHash)
	}
}

func TestManagedChangelogRejectsRawDDLAndLossyValues(t *testing.T) {
	t.Parallel()
	transaction := managedTestTransaction()
	transaction.Fragments[0].Batch.Records = []connector.Record{{Table: "widgets", SchemaVersion: 7, Operation: connector.OpDDL, DDL: "ALTER TABLE widgets DROP COLUMN payload"}}
	intent := managedTestIntent(t, transaction)
	if _, err := planManagedTransaction(intent, transaction); err == nil || !strings.Contains(err.Error(), "raw DDL") {
		t.Fatalf("raw DDL error=%v", err)
	}

	transaction = managedTestTransaction()
	intent = managedTestIntent(t, transaction)
	transaction.Fragments[0].Batch.Records[0].After["float"] = func() {}
	if _, err := planManagedTransaction(intent, transaction); err == nil || !strings.Contains(err.Error(), "unsupported func") {
		t.Fatalf("lossy value error=%v", err)
	}
}

func TestManagedTableAdmissionRejectsUnreconcilableDefinitions(t *testing.T) {
	t.Parallel()
	contract := managedTableContract{
		columns:        map[string]string{"wallaby_version": "UInt64"},
		sortingKey:     "destination_revision_id, logical_batch_id",
		keeperPath:     "/clickhouse/tables/01/wallaby/cdc_log",
		replicaNames:   map[string]struct{}{"replica-1": {}, "replica-2": {}},
		maxActiveParts: 180,
	}
	valid := managedTableDefinition{
		engine:     "ReplicatedReplacingMergeTree",
		engineFull: "ReplicatedReplacingMergeTree('/clickhouse/tables/01/wallaby/cdc_log','replica-1',wallaby_version)",
		createSQL: `CREATE TABLE wallaby.cdc_log (wallaby_version UInt64)
ENGINE=ReplicatedReplacingMergeTree('/clickhouse/tables/01/wallaby/cdc_log','replica-1',wallaby_version)
ORDER BY (destination_revision_id, logical_batch_id)
SETTINGS replicated_deduplication_window=1000, replicated_deduplication_window_seconds=3600,
parts_to_delay_insert=100, parts_to_throw_insert=200, max_parts_in_total=1000`,
		sortingKey:   "destination_revision_id, logical_batch_id",
		primaryKey:   "destination_revision_id, logical_batch_id",
		partitionKey: "",
		columns:      map[string]string{"wallaby_version": "UInt64"},
		columnKinds:  map[string]string{"wallaby_version": ""},
	}
	if err := validateManagedTableDefinition(valid, contract); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*managedTableDefinition)
		want   string
	}{
		{name: "non-replicated", mutate: func(definition *managedTableDefinition) { definition.engine = "ReplacingMergeTree" }, want: "ReplicatedReplacingMergeTree"},
		{name: "wrong keeper path", mutate: func(definition *managedTableDefinition) {
			definition.engineFull = "ReplicatedReplacingMergeTree('/other/path','replica-1',wallaby_version)"
		}, want: "Keeper path"},
		{name: "wrong version key", mutate: func(definition *managedTableDefinition) {
			definition.engineFull = "ReplicatedReplacingMergeTree('/clickhouse/tables/01/wallaby/cdc_log','replica-1',other_version)"
		}, want: "wallaby_version"},
		{name: "unadmitted replica", mutate: func(definition *managedTableDefinition) {
			definition.engineFull = "ReplicatedReplacingMergeTree('/clickhouse/tables/01/wallaby/cdc_log','replica-3',wallaby_version)"
		}, want: "replica name"},
		{name: "wrong sorting key", mutate: func(definition *managedTableDefinition) { definition.sortingKey = "tuple()" }, want: "sorting key"},
		{name: "wrong primary key", mutate: func(definition *managedTableDefinition) { definition.primaryKey = "destination_revision_id" }, want: "primary key"},
		{name: "partitioned evidence", mutate: func(definition *managedTableDefinition) { definition.partitionKey = "toYYYYMM(event_time)" }, want: "partitioned"},
		{name: "TTL", mutate: func(definition *managedTableDefinition) { definition.createSQL += " TTL now() + INTERVAL 1 DAY" }, want: "TTL"},
		{name: "short dedup window", mutate: func(definition *managedTableDefinition) {
			definition.createSQL = strings.Replace(definition.createSQL, "replicated_deduplication_window=1000", "replicated_deduplication_window=10", 1)
		}, want: "replicated_deduplication_window"},
		{name: "server parts below admission bound", mutate: func(definition *managedTableDefinition) {
			definition.createSQL = strings.Replace(definition.createSQL, "parts_to_throw_insert=200", "parts_to_throw_insert=150", 1)
		}, want: "managed_max_active_parts"},
		{name: "missing column", mutate: func(definition *managedTableDefinition) { definition.columns = map[string]string{} }, want: "column count"},
		{name: "materialized column", mutate: func(definition *managedTableDefinition) { definition.columnKinds["wallaby_version"] = "MATERIALIZED" }, want: "default kind"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			definition := valid
			definition.columns = map[string]string{"wallaby_version": "UInt64"}
			definition.columnKinds = map[string]string{"wallaby_version": ""}
			tt.mutate(&definition)
			if err := validateManagedTableDefinition(definition, contract); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error=%v, want %q", err, tt.want)
			}
		})
	}
}

func TestManagedKeeperImplementationAdmissionRequiresClickHouseVersionPair(t *testing.T) {
	t.Parallel()
	if err := validateManagedKeeperVersion("ClickHouse Keeper version: v25.12.1.649-stable-build", "25.12.1.649"); err != nil {
		t.Fatal(err)
	}
	for _, response := range []string{
		"ZooKeeper version: 3.8.4",
		"ClickHouse Keeper version: v25.12.10.7-stable-build",
		"ClickHouse Keeper version: v25.12.1.6499-stable-build",
		"",
	} {
		if err := validateManagedKeeperVersion(response, "25.12.1.649"); err == nil {
			t.Fatalf("unadmitted Keeper response accepted: %q", response)
		}
	}
}

func TestManagedKeeperReplicaAdmissionRequiresExactHealthyTwoNodeTopology(t *testing.T) {
	t.Parallel()
	contract := managedReplicaContract{
		keeperPath:   "/clickhouse/tables/01/wallaby/cdc_log",
		replicaNames: map[string]struct{}{"replica-1": {}, "replica-2": {}},
	}
	valid := managedReplicaStatus{
		keeperPath: contract.keeperPath, replicaName: "replica-1",
		totalReplicas: 2, activeReplicas: 2,
	}
	if err := validateManagedReplicaStatus(valid, contract); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*managedReplicaStatus)
		want   string
	}{
		{name: "path", mutate: func(status *managedReplicaStatus) { status.keeperPath = "/wrong" }, want: "keeper path"},
		{name: "name", mutate: func(status *managedReplicaStatus) { status.replicaName = "replica-3" }, want: "replica name"},
		{name: "total", mutate: func(status *managedReplicaStatus) { status.totalReplicas = 1 }, want: "total replicas"},
		{name: "active", mutate: func(status *managedReplicaStatus) { status.activeReplicas = 1 }, want: "active replicas"},
		{name: "readonly", mutate: func(status *managedReplicaStatus) { status.readonly = 1 }, want: "not writable"},
		{name: "expired", mutate: func(status *managedReplicaStatus) { status.expired = 1 }, want: "not writable"},
		{name: "queue", mutate: func(status *managedReplicaStatus) { status.queueSize = 101 }, want: "queue"},
		{name: "delay", mutate: func(status *managedReplicaStatus) { status.absoluteDelay = 1 }, want: "delay"},
		{name: "lost parts", mutate: func(status *managedReplicaStatus) { status.lostPartCount = 1 }, want: "lost parts"},
	} {
		t.Run(test.name, func(t *testing.T) {
			status := valid
			test.mutate(&status)
			if err := validateManagedReplicaStatus(status, contract); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v, want %q", err, test.want)
			}
		})
	}
}

func TestManagedRecoveryOnlyAdmissionAllowsOneHealthyReplicaAndFencesWrites(t *testing.T) {
	t.Parallel()

	contract := managedReplicaContract{
		keeperPath:    "/clickhouse/tables/01/wallaby/cdc_log",
		replicaNames:  map[string]struct{}{"replica-1": {}, "replica-2": {}},
		allowDegraded: true,
	}
	status := managedReplicaStatus{
		keeperPath: contract.keeperPath, replicaName: "replica-2",
		totalReplicas: 2, activeReplicas: 1,
	}
	if err := validateManagedReplicaStatus(status, contract); err != nil {
		t.Fatalf("one-replica recovery admission: %v", err)
	}
	status.activeReplicas = 0
	if err := validateManagedReplicaStatus(status, contract); err == nil || !strings.Contains(err.Error(), "for recovery") {
		t.Fatalf("zero-replica recovery error=%v", err)
	}

	destination := &Destination{managedRecoveryOnly: true}
	transaction := managedTestTransaction()
	intent := managedTestIntent(t, transaction)
	if _, err := destination.PrepareTransaction(context.Background(), intent, transaction); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("recovery-only write error=%v, want ErrDeliveryIndeterminate", err)
	}
}

func TestManagedOpenThenInitializeAdmitsOneActuallyFailedEndpointForRecovery(t *testing.T) {
	transaction := managedTestTransaction()
	intent := managedTestIntent(t, transaction)
	primary := &managedRecoveryTestConn{row: managedReceiptTestRow{contentHash: intent.ContentHash, externalID: managedDeliveryExternalID(intent)}}
	destination := &Destination{
		managedOpenEndpointHook: func(_ context.Context, _ *chclient.Options, _ connector.ManagedProfileContract, endpoint string) (chdriver.Conn, string, error) {
			if endpoint == "primary" {
				return primary, "25.12.1.649", nil
			}
			return nil, "", fmt.Errorf("%w: replica dial refused", errManagedEndpointUnavailable)
		},
		managedValidateTargetHook: func(_ context.Context, conn chdriver.Conn, replica string, recoveryOnly bool) error {
			if conn != primary || replica != "replica-1" || !recoveryOnly {
				return errors.New("wrong recovery survivor validation")
			}
			return nil
		},
		managedInitializeAuthorityHook: func(_ context.Context, conn chdriver.Conn, replica string, recoveryOnly bool) error {
			if conn != primary || replica != "replica-1" || !recoveryOnly {
				return errors.New("wrong initialized recovery authority")
			}
			return nil
		},
	}
	spec := connector.RuntimeSpec{Options: map[string]string{
		"dsn": "clickhouse://replica-1:9440/default?secure=true", "managed_profile": connector.ManagedProfilePostgresToClickHouseAppendV1,
		"managed_database": "wallaby", "managed_changelog_table": "cdc_log", "managed_receipts_table": "delivery_receipts",
		"managed_final_view": "cdc_log_final", "managed_deployment": "self-managed-keeper", "managed_keeper_path_prefix": "/clickhouse/tables/01", "managed_keeper_address": "127.0.0.1:9181",
		"managed_replica_dsn": "clickhouse://replica-2:9440/default?secure=true", "managed_replica_names": "replica-1,replica-2", "insert_quorum": "2",
		"batch_mode": "target", "batch_resolution": "none", "meta_table_enabled": "false", "async_insert": "false", "wait_for_async_insert": "true",
	}}
	if err := destination.Open(context.Background(), spec); err != nil {
		t.Fatalf("Open(): %v", err)
	}
	if !destination.managedRecoveryOnly || destination.managedConn != primary || destination.managedReplicaConn != nil {
		t.Fatalf("recovery handles primary=%v replica=%v recovery=%t", destination.managedConn, destination.managedReplicaConn, destination.managedRecoveryOnly)
	}
	if err := destination.InitializeManagedDelivery(context.Background()); err != nil {
		t.Fatalf("InitializeManagedDelivery(): %v", err)
	}
	disposition, _, err := destination.Reconcile(context.Background(), intent)
	if err != nil || disposition != connector.DeliveryApplied {
		t.Fatalf("reconcile existing receipt disposition=%v err=%v", disposition, err)
	}
	if _, err := destination.PrepareTransaction(context.Background(), intent, transaction); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("recovery-only new write error=%v", err)
	}
}

func TestManagedRecoveryOnlyInitializationAdoptsReceiptAndFencesNewWrites(t *testing.T) {
	t.Parallel()
	transaction := managedTestTransaction()
	intent := managedTestIntent(t, transaction)
	conn := &managedRecoveryTestConn{row: managedReceiptTestRow{contentHash: intent.ContentHash, externalID: managedDeliveryExternalID(intent)}}
	validationCalls := 0
	destination := &Destination{
		managedProfile:      connector.ManagedProfilePostgresToClickHouseAppendV1,
		managedVersion:      "25.12.1.649",
		managedRecoveryOnly: true,
		managedReplicaConn:  conn,
		managedConfig:       managedConfig{replicaNames: []string{"replica-primary", "replica-secondary"}, database: "wallaby", receiptsTable: "receipts"},
		managedInitializeAuthorityHook: func(_ context.Context, got chdriver.Conn, replica string, recoveryOnly bool) error {
			validationCalls++
			if got != conn || replica != "replica-secondary" || !recoveryOnly {
				return errors.New("recovery-only initialization validated the wrong authority")
			}
			return nil
		},
	}
	if err := destination.InitializeManagedDelivery(context.Background()); err != nil {
		t.Fatalf("InitializeManagedDelivery(): %v", err)
	}
	if validationCalls != 1 {
		t.Fatalf("survivor authority validations=%d, want one", validationCalls)
	}
	disposition, evidence, err := destination.Reconcile(context.Background(), intent)
	if err != nil || disposition != connector.DeliveryApplied || evidence.ContentHash != intent.ContentHash {
		t.Fatalf("receipt adoption=(%v,%+v,%v), want applied existing receipt", disposition, evidence, err)
	}
	adopted, err := destination.ApplyTransaction(context.Background(), intent, transaction)
	if err != nil || adopted.ContentHash != intent.ContentHash {
		t.Fatalf("recovery-only ApplyTransaction receipt adoption=(%+v,%v)", adopted, err)
	}
	if _, err := destination.PrepareTransaction(context.Background(), intent, transaction); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("recovery-only new write error=%v, want fenced ErrDeliveryIndeterminate", err)
	}
}

func TestManagedNormalInitializationRemainsStrictlyTwoEndpoint(t *testing.T) {
	t.Parallel()
	primary := &managedRecoveryTestConn{}
	replica := &managedRecoveryTestConn{}
	validations := 0
	destination := &Destination{
		managedProfile:     connector.ManagedProfilePostgresToClickHouseAppendV1,
		managedVersion:     "25.12.1.649",
		managedConn:        primary,
		managedReplicaConn: replica,
		managedConfig:      managedConfig{replicaNames: []string{"replica-primary", "replica-secondary"}},
		managedInitializeAuthorityHook: func(context.Context, chdriver.Conn, string, bool) error {
			validations++
			return nil
		},
	}
	if err := destination.InitializeManagedDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if validations != 2 {
		t.Fatalf("normal authority validations=%d, want both endpoints", validations)
	}
	destination.managedReplicaConn = nil
	if err := destination.InitializeManagedDelivery(context.Background()); err == nil || !strings.Contains(err.Error(), "both endpoints") {
		t.Fatalf("one-endpoint normal initialization error=%v, want strict rejection", err)
	}
}

func TestManagedFinalViewAdmissionRejectsFiltersAndTransforms(t *testing.T) {
	t.Parallel()
	const valid = "CREATE VIEW wallaby.cdc_log_final AS SELECT * FROM wallaby.cdc_log FINAL"
	if err := validateManagedFinalViewDefinition("View", valid, "wallaby", "cdc_log_final", "cdc_log"); err != nil {
		t.Fatal(err)
	}
	for _, createSQL := range []string{
		"CREATE VIEW wallaby.cdc_log_final AS SELECT * FROM wallaby.cdc_log FINAL WHERE tombstone=0",
		"CREATE VIEW wallaby.cdc_log_final AS SELECT flow_id FROM wallaby.cdc_log FINAL",
		"CREATE VIEW wallaby.cdc_log_final AS SELECT * FROM wallaby.other FINAL",
	} {
		if err := validateManagedFinalViewDefinition("View", createSQL, "wallaby", "cdc_log_final", "cdc_log"); err == nil {
			t.Fatalf("incompatible FINAL view admitted: %s", createSQL)
		}
	}
}

func TestManagedTLSRequiresVerifiedNativeTransport(t *testing.T) {
	t.Parallel()
	if err := configureManagedTLS(&chclient.Options{}, map[string]string{}); err == nil || !strings.Contains(err.Error(), "secure=true") {
		t.Fatalf("plaintext TLS admission error=%v", err)
	}
	if err := configureManagedTLS(&chclient.Options{TLS: &tls.Config{InsecureSkipVerify: true}}, map[string]string{}); err == nil || !strings.Contains(err.Error(), "skip_verify") {
		t.Fatalf("unverified TLS admission error=%v", err)
	}
	if err := configureManagedTLS(&chclient.Options{TLS: &tls.Config{MinVersion: tls.VersionTLS12}}, map[string]string{}); err != nil {
		t.Fatalf("verified TLS rejected: %v", err)
	}
}

func TestManagedConfigRejectsUnsafeProtocolOptionsBeforeNetwork(t *testing.T) {
	t.Parallel()
	base := connector.RuntimeSpec{Options: map[string]string{
		"managed_database": "wallaby", "managed_changelog_table": "cdc_log", "managed_receipts_table": "delivery_receipts",
		"managed_final_view": "cdc_log_final", "managed_deployment": "self-managed-keeper", "managed_keeper_path_prefix": "/clickhouse/tables/01", "managed_keeper_address": "127.0.0.1:9181", "managed_replica_dsn": "clickhouse://replica-2:9440/default?secure=true", "managed_replica_names": "replica-1,replica-2", "insert_quorum": "2",
		"batch_mode": "target", "batch_resolution": "none", "meta_table_enabled": "false",
		"async_insert": "false", "wait_for_async_insert": "true",
	}}
	cfg, err := managedConfigFromSpec(base)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.insertQuorum != 2 {
		t.Fatalf("insert quorum=%d, want both admitted replicas", cfg.insertQuorum)
	}
	for _, test := range []struct{ key, value, want string }{
		{key: "managed_changelog_table", value: "other.cdc_log", want: "unqualified"},
		{key: "staging_schema", value: "ignored", want: "does not allow option staging_schema"},
		{key: "meta_schema", value: "ignored", want: "does not allow option meta_schema"},
		{key: "managed_typo", value: "ignored", want: "does not allow option managed_typo"},
		{key: "insert_quorum", value: "1", want: "between 2 and 2"},
		{key: "managed_max_rows_per_batch", value: "100001", want: "between 1 and 100000"},
		{key: "managed_replica_names", value: "replica-1", want: "exactly two"},
		{key: "async_insert", value: "true", want: "async_insert=false"},
		{key: "async_insert", value: "truthy", want: "must be true or false"},
		{key: "wait_for_async_insert", value: "false", want: "wait_for_async_insert=true"},
	} {
		options := make(map[string]string, len(base.Options))
		for key, value := range base.Options {
			options[key] = value
		}
		options[test.key] = test.value
		if _, err := managedConfigFromSpec(connector.RuntimeSpec{Options: options}); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("%s error=%v, want %q", test.key, err, test.want)
		}
	}
}

func TestManagedPreparedTransactionRequiresPostgresPartReservation(t *testing.T) {
	prepared := &preparedManagedTransaction{}
	if _, err := prepared.Apply(context.Background()); err == nil || !strings.Contains(err.Error(), "requires a PostgreSQL part reservation") {
		t.Fatalf("unreserved managed write error=%v", err)
	}
}

func TestManagedPartReservationPlanCountsChangelogAndReceipt(t *testing.T) {
	transaction := managedTestTransaction()
	intent := managedTestIntent(t, transaction)
	plan, err := planManagedTransactionWithLimits(intent, transaction, managedPlanLimits{maxFragments: 8, maxRows: 100, maxBytes: 1 << 20, maxRowsPerInsert: 1, maxBytesPerInsert: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	prepared := &preparedManagedTransaction{destination: &Destination{managedConfig: managedConfig{maxActiveParts: 8}}, intent: intent, plan: plan}
	request, err := prepared.PartReservationRequest()
	if err != nil {
		t.Fatal(err)
	}
	if len(request.Parts) != len(plan.Fragments)+1 || request.Parts[len(request.Parts)-1].Kind != "receipt" {
		t.Fatalf("reservation parts=%+v, want %d changelog plus receipt", request.Parts, len(plan.Fragments))
	}
	if request.Capacity != 8 || request.SourceLineageID != intent.SourceLineageID || request.PositionID != intent.PositionID {
		t.Fatalf("reservation identity/capacity=%+v", request)
	}
	if want, hashErr := connector.ManagedPartPlanHash(request.Parts); hashErr != nil || request.PlanHash != want {
		t.Fatalf("reservation plan hash=%q want=%q err=%v", request.PlanHash, want, hashErr)
	}
}

func managedTestTransaction() connector.SourceTransaction {
	zone := time.FixedZone("EST", -5*60*60)
	schema := connector.Schema{
		Namespace: "public", Name: "widgets", Version: 7,
		Columns: []connector.Column{
			{Name: "id", Type: "bigint"},
			{Name: "nullable", Type: "text", Nullable: true},
			{Name: "date", Type: "date"},
			{Name: "timestamptz", Type: "timestamptz"},
			{Name: "numeric", Type: "numeric(38,9)"},
			{Name: "uuid", Type: "uuid"},
			{Name: "bytea", Type: "bytea"},
			{Name: "array", Type: "text[]"},
			{Name: "json", Type: "jsonb"},
		},
	}
	return connector.SourceTransaction{
		SourceLineageID: "postgres-system/publication-v1", TransactionID: 42,
		BeginLSN: "0/10", CommitLSN: "0/20", EndLSN: "0/28", Checkpoint: connector.Checkpoint{LSN: "0/28"},
		Fragments: []connector.TransactionFragment{
			{Ordinal: 0, Batch: connector.Batch{Schema: schema, Records: []connector.Record{
				{Table: "widgets", Operation: connector.OpUpdate, SchemaVersion: 7, Key: []byte(`{"id":1}`), Before: map[string]any{"id": int64(1)}, After: map[string]any{
					"id": int64(1), "nullable": nil, "date": time.Date(1960, 1, 2, 0, 0, 0, 0, time.UTC),
					"timestamptz": time.Date(2025, 11, 2, 1, 30, 0, 0, zone), "numeric": json.Number("12345678901234567890.123456789"),
					"uuid": "12345678-90ab-cdef-0123-456789abcdef", "bytea": []byte{0x00, 0xff, 0x80},
					"array": []any{int64(1), nil, "x"}, "json": json.RawMessage(`{"a":1,"b":[true,false]}`),
				}},
				{Table: "widgets", Operation: connector.OpDelete, SchemaVersion: 7, Key: []byte(`{"id":2}`), Before: map[string]any{"id": int64(2)}},
			}}},
			{Ordinal: 1, Batch: connector.Batch{Schema: connector.Schema{Namespace: "audit", Name: "events", Version: 1, Columns: []connector.Column{{Name: "id", Type: "uuid"}}}, Records: []connector.Record{
				{Table: "events", Operation: connector.OpInsert, SchemaVersion: 1, Key: []byte(`{"id":"e"}`), After: map[string]any{"id": "e"}},
			}}},
		},
	}
}

func managedTestIntent(t *testing.T, transaction connector.SourceTransaction) connector.DeliveryIntent {
	t.Helper()
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		t.Fatal(err)
	}
	return connector.DeliveryIntent{
		FlowID: "orders", FlowIncarnationID: "11111111-1111-1111-1111-111111111111",
		SourceLineageID: transaction.SourceLineageID, Generation: 3,
		AcquisitionID: "22222222-2222-2222-2222-222222222222", LeaseEpoch: 4,
		DestinationRevisionID: "clickhouse-revision-v1", LogicalBatchID: logicalBatchID,
		PositionID: transaction.Checkpoint.LSN, ContentHash: contentHash,
	}
}
