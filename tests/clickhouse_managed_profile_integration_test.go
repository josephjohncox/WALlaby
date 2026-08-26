package tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chclient "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	clickhousedest "github.com/josephjohncox/wallaby/connectors/destinations/clickhouse"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

var clickHouseManagedSequence atomic.Uint64

func TestClickHouseManagedProfileVersionMatrix(t *testing.T) {
	fixture := newClickHouseManagedFixture(t, 180)
	profile := connector.PostgresToClickHouseAppendV1Profile()
	if version := fixture.destination.ManagedClickHouseVersion(); !profile.SupportsClickHouseVersion(version) {
		t.Fatalf("live version %s is outside profile %v", version, profile.ClickHouseVersions)
	}
	var replicaVersion string
	if err := fixture.replicaDB.QueryRowContext(context.Background(), "SELECT version()").Scan(&replicaVersion); err != nil {
		t.Fatal(err)
	}
	if !profile.SupportsClickHouseVersion(replicaVersion) {
		t.Fatalf("live replica version %s is outside profile %v", replicaVersion, profile.ClickHouseVersions)
	}
}

func TestClickHouseManagedProfileAdmission(t *testing.T) {
	fixture := newClickHouseManagedFixture(t, 180)
	if err := fixture.destination.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.replicaDB.ExecContext(context.Background(),
		"DROP VIEW {database:Identifier}.{view:Identifier}",
		chclient.Named("database", fixture.database), chclient.Named("view", fixture.finalView),
	); err != nil {
		t.Fatal(err)
	}
	replicaCandidate := &clickhousedest.Destination{}
	if err := replicaCandidate.Open(context.Background(), fixture.spec); err == nil || !strings.Contains(err.Error(), "FINAL view") {
		if err == nil {
			_ = replicaCandidate.Close(context.Background())
		}
		t.Fatalf("second-replica admission error=%v", err)
	}
	viewDDL := fmt.Sprintf("CREATE VIEW %s.%s AS SELECT * FROM %s.%s FINAL", fixture.database, fixture.finalView, fixture.database, fixture.changelogTable)
	if _, err := fixture.replicaDB.ExecContext(context.Background(), viewDDL); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ExecContext(context.Background(),
		"ALTER TABLE {database:Identifier}.{table:Identifier} MODIFY SETTING replicated_deduplication_window=10",
		chclient.Named("database", fixture.database), chclient.Named("table", fixture.changelogTable),
	); err != nil {
		t.Fatal(err)
	}
	candidate := &clickhousedest.Destination{}
	err := candidate.Open(context.Background(), fixture.spec)
	if err == nil || !strings.Contains(err.Error(), "replicated_deduplication_window") {
		if err == nil {
			_ = candidate.Close(context.Background())
		}
		t.Fatalf("admission error=%v", err)
	}
}

func TestClickHouseManagedProfileCommitAndReconcile(t *testing.T) {
	fixture := newClickHouseManagedFixture(t, 180)
	transaction := clickHouseManagedTransaction("widgets", 1, []connector.Record{
		clickHouseManagedRecord("widgets", connector.OpInsert, 1, map[string]any{"id": int64(1), "value": "alpha"}),
	})
	intent := clickHouseManagedIntent(t, transaction)
	if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); err != nil {
		t.Fatalf("ApplyTransaction error=%v", err)
	}
	disposition, evidence, err := fixture.destination.Reconcile(context.Background(), intent)
	if err != nil || disposition != connector.DeliveryApplied || evidence.ContentHash != intent.ContentHash {
		t.Fatalf("Reconcile=(%v,%+v,%v)", disposition, evidence, err)
	}
	if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); err != nil {
		t.Fatalf("idempotent ApplyTransaction: %v", err)
	}
	if got := fixture.logicalRowCount(t, intent.LogicalBatchID); got != 1 {
		t.Fatalf("logical rows=%d, want 1", got)
	}
}

func TestClickHouseManagedProfileSecondaryEndpointWriteFailover(t *testing.T) {
	var primaryProxy *keeperProbeProxy
	fixture := newClickHouseManagedFixtureWithHooks(t, 180, 10000, delivery.CoordinatorHooks{AfterPartReservationCommit: func(context.Context, authority.RunFence, connector.DeliveryIntent, string) error {
		primaryProxy.SetBlocked(true)
		return nil
	}})
	primaryProxy = fixture.tlsProxy
	t.Cleanup(func() { primaryProxy.SetBlocked(false) })

	transaction := clickHouseManagedTransaction("secondary_write", 1, []connector.Record{
		clickHouseManagedRecord("secondary_write", connector.OpInsert, 1, map[string]any{"id": int64(1), "value": "secondary"}),
	})
	intent := clickHouseManagedIntent(t, transaction)
	if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); err != nil {
		t.Fatalf("secondary-endpoint quorum write failover: %v", err)
	}
	primaryProxy.SetBlocked(false)
	disposition, evidence, err := fixture.destination.Reconcile(context.Background(), intent)
	if err != nil || disposition != connector.DeliveryApplied || evidence.ContentHash != intent.ContentHash {
		t.Fatalf("secondary-endpoint reconciliation=(%v,%+v,%v)", disposition, evidence, err)
	}
	if got := fixture.logicalRowCount(t, intent.LogicalBatchID); got != 1 {
		t.Fatalf("secondary-endpoint logical rows=%d, want 1", got)
	}
}

func TestClickHouseManagedProfileDedupWindowEviction(t *testing.T) {
	fixture := newClickHouseManagedFixture(t, 180)
	transaction := clickHouseManagedTransaction("widgets", 1, []connector.Record{
		clickHouseManagedRecord("widgets", connector.OpInsert, 1, map[string]any{"id": int64(2), "value": "eviction"}),
	})
	intent := clickHouseManagedIntent(t, transaction)
	if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); err != nil {
		t.Fatalf("initial delivery error=%v", err)
	}

	ctx := context.Background()
	if _, err := fixture.db.ExecContext(ctx,
		"ALTER TABLE {database:Identifier}.{table:Identifier} MODIFY SETTING replicated_deduplication_window=0",
		chclient.Named("database", fixture.database), chclient.Named("table", fixture.changelogTable),
	); err != nil {
		t.Fatal(err)
	}
	// Changing the window does not eagerly remove the prior Keeper block ID.
	// Commit distinct blocks so the replicated table applies the new zero-sized
	// window and evicts the original transaction before replay.
	for id := int64(100); id < 103; id++ {
		filler := clickHouseManagedTransaction("widgets", id, []connector.Record{
			clickHouseManagedRecord("widgets", connector.OpInsert, id, map[string]any{"id": id, "value": "eviction-filler"}),
		})
		fillerIntent := clickHouseManagedIntent(t, filler)
		if _, err := fixture.destination.ApplyTransaction(ctx, fillerIntent, filler); err != nil {
			t.Fatalf("dedup eviction filler %d: %v", id, err)
		}
	}
	var physicalBeforeReplay int
	if err := fixture.db.QueryRowContext(ctx,
		"SELECT count() FROM {database:Identifier}.{table:Identifier} WHERE logical_batch_id={logical:String}",
		chclient.Named("database", fixture.database), chclient.Named("table", fixture.changelogTable), chclient.Named("logical", intent.LogicalBatchID),
	).Scan(&physicalBeforeReplay); err != nil {
		t.Fatal(err)
	}
	// Replay through the real PostgreSQL coordinator. The durable receipt and
	// physical insert_query_id evidence must prevent a new external write even
	// after ClickHouse's finite block-deduplication window has been evicted.
	if _, err := fixture.destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("coordinated replay after window eviction: %v", err)
	}
	var physical int
	if err := fixture.db.QueryRowContext(ctx,
		"SELECT count() FROM {database:Identifier}.{table:Identifier} WHERE logical_batch_id={logical:String}",
		chclient.Named("database", fixture.database), chclient.Named("table", fixture.changelogTable), chclient.Named("logical", intent.LogicalBatchID),
	).Scan(&physical); err != nil {
		t.Fatal(err)
	}
	if physical != physicalBeforeReplay {
		t.Fatalf("coordinated replay physical delta=%d, want 0 (before=%d after=%d)", physical-physicalBeforeReplay, physicalBeforeReplay, physical)
	}
	if _, err := fixture.db.ExecContext(ctx,
		"ALTER TABLE {database:Identifier}.{table:Identifier} MODIFY SETTING replicated_deduplication_window=1000",
		chclient.Named("database", fixture.database), chclient.Named("table", fixture.changelogTable),
	); err != nil {
		t.Fatal(err)
	}
	if got := fixture.logicalRowCount(t, intent.LogicalBatchID); got != 1 {
		t.Fatalf("FINAL logical rows=%d, want 1 after physical duplication", got)
	}
}

func TestClickHouseManagedProfileOrderingAndConcurrency(t *testing.T) {
	fixture := newClickHouseManagedFixture(t, 180)
	ordered := clickHouseManagedTransactionWithFragments([]connector.TransactionFragment{
		{Ordinal: 0, Batch: connector.Batch{Schema: clickHouseManagedSchema("widgets", 1), Records: []connector.Record{
			clickHouseManagedRecord("widgets", connector.OpInsert, 1, map[string]any{"id": int64(1)}),
			clickHouseManagedRecord("widgets", connector.OpUpdate, 1, map[string]any{"id": int64(1), "value": "second"}),
		}}},
		{Ordinal: 1, Batch: connector.Batch{Schema: clickHouseManagedSchema("audit_events", 1), Records: []connector.Record{
			clickHouseManagedRecord("audit_events", connector.OpInsert, 1, map[string]any{"id": int64(9)}),
		}}},
	})
	orderedIntent := clickHouseManagedIntent(t, ordered)
	if _, err := fixture.destination.ApplyTransaction(context.Background(), orderedIntent, ordered); err != nil {
		t.Fatal(err)
	}
	rows, err := fixture.db.QueryContext(context.Background(),
		"SELECT fragment_ordinal,record_ordinal FROM {database:Identifier}.{view:Identifier} WHERE logical_batch_id={logical:String} ORDER BY fragment_ordinal,record_ordinal",
		chclient.Named("database", fixture.database), chclient.Named("view", fixture.finalView), chclient.Named("logical", orderedIntent.LogicalBatchID),
	)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var fragment, record uint64
		if err := rows.Scan(&fragment, &record); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%d/%d", fragment, record))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if strings.Join(got, ",") != "0/0,0/1,1/0" {
		t.Fatalf("ordered rows=%v", got)
	}

	transactions := []connector.SourceTransaction{
		clickHouseManagedTransaction("concurrent_a", 1, []connector.Record{clickHouseManagedRecord("concurrent_a", connector.OpInsert, 1, map[string]any{"id": int64(1)})}),
		clickHouseManagedTransaction("concurrent_b", 1, []connector.Record{clickHouseManagedRecord("concurrent_b", connector.OpInsert, 1, map[string]any{"id": int64(2)})}),
	}
	// One source flow publishes checkpoints in order. Cross-worker budget
	// concurrency is covered by TestClickHousePartReservationSerializesConcurrentWriters,
	// which uses separate coordinators under the shared destination budget lock.
	for index := range transactions {
		intent := clickHouseManagedIntent(t, transactions[index])
		if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transactions[index]); err != nil {
			t.Fatalf("ordered delivery %d: %v", index, err)
		}
	}
}

func TestClickHouseManagedProfileKeyChangesAndTombstones(t *testing.T) {
	fixture := newClickHouseManagedFixture(t, 180)
	schema := clickHouseManagedSchema("keys", 1)
	oldKey, _ := json.Marshal(map[string]any{"id": int64(1)})
	newKey, _ := json.Marshal(map[string]any{"id": int64(2)})
	transaction := clickHouseManagedTransaction("keys", 1, []connector.Record{
		{Table: "keys", Operation: connector.OpInsert, SchemaVersion: 1, Key: oldKey, After: map[string]any{"id": int64(1), "value": "created"}},
		{Table: "keys", Operation: connector.OpUpdate, SchemaVersion: 1, Key: newKey, Before: map[string]any{"id": int64(1), "value": "created"}, After: map[string]any{"id": int64(2), "value": "moved"}},
		{Table: "keys", Operation: connector.OpDelete, SchemaVersion: 1, Key: newKey, Before: map[string]any{"id": int64(2), "value": "moved"}},
	})
	transaction.Fragments[0].Batch.Schema = schema
	intent := clickHouseManagedIntent(t, transaction)
	if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); err != nil {
		t.Fatal(err)
	}
	rows, err := fixture.db.QueryContext(context.Background(),
		"SELECT operation,tombstone,key_json,before_json,after_json FROM {database:Identifier}.{view:Identifier} WHERE logical_batch_id={logical:String} ORDER BY record_ordinal",
		chclient.Named("database", fixture.database), chclient.Named("view", fixture.finalView), chclient.Named("logical", intent.LogicalBatchID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var operation, keyJSON, beforeJSON, afterJSON string
		var tombstone uint8
		if err := rows.Scan(&operation, &tombstone, &keyJSON, &beforeJSON, &afterJSON); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%s/%d/%s/%s/%s", operation, tombstone, keyJSON, beforeJSON, afterJSON))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || !strings.HasPrefix(got[0], `insert/0/{"id":1}/`) || !strings.Contains(got[1], `update/0/{"id":2}/{"id":1`) || !strings.Contains(got[2], `delete/1/{"id":2}/{"id":2`) {
		t.Fatalf("key-changing append envelope=%v", got)
	}
}

func TestClickHouseManagedProfileBoundedLoad(t *testing.T) {
	fixture := newClickHouseManagedFixture(t, 180)
	for _, size := range []int{1000, 10000, 100000} {
		t.Run(fmt.Sprintf("rows_%d", size), func(t *testing.T) {
			records := make([]connector.Record, 0, size)
			for index := 0; index < size; index++ {
				records = append(records, clickHouseManagedRecord("load", connector.OpInsert, 1, map[string]any{"id": int64(index)}))
			}
			transaction := clickHouseManagedTransaction("load", 1, records)
			intent := clickHouseManagedIntent(t, transaction)
			if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); err != nil {
				t.Fatalf("deliver %d rows: %v", size, err)
			}
			var recordCount, queryCount uint64
			if err := fixture.db.QueryRowContext(context.Background(),
				"SELECT record_count,length(query_ids) FROM {database:Identifier}.{table:Identifier} FINAL WHERE logical_batch_id={logical:String}",
				chclient.Named("database", fixture.database), chclient.Named("table", fixture.receiptsTable), chclient.Named("logical", intent.LogicalBatchID),
			).Scan(&recordCount, &queryCount); err != nil {
				t.Fatal(err)
			}
			wantQueries := uint64((size + 9999) / 10000)
			if recordCount != uint64(size) || queryCount != wantQueries {
				t.Fatalf("bounded load receipt rows/queries=%d/%d, want %d/%d", recordCount, queryCount, size, wantQueries)
			}
			if got := fixture.logicalRowCount(t, intent.LogicalBatchID); got != size {
				t.Fatalf("bounded load FINAL rows=%d, want %d", got, size)
			}
		})
	}
}

func TestClickHouseManagedProfileSchemaEvolutionAndTypes(t *testing.T) {
	fixture := newClickHouseManagedFixtureWithInsertRows(t, 180, 1)
	zone := time.FixedZone("EST", -5*60*60)
	record := clickHouseManagedRecord("typed_events", connector.OpInsert, 2, map[string]any{
		"nullable":    nil,
		"date":        time.Date(1960, 1, 2, 0, 0, 0, 0, time.UTC),
		"timestamptz": time.Date(2025, 11, 2, 1, 30, 0, 0, zone),
		"numeric":     json.Number("12345678901234567890.123456789"),
		"uuid":        "12345678-90ab-cdef-0123-456789abcdef",
		"json":        json.RawMessage(`{"a":1,"b":[true,false]}`),
		"bytea":       []byte{0, 255, 128},
		"array":       []any{int64(1), nil, "x"},
	})
	schemaV2 := clickHouseManagedSchema("typed_events", 2)
	schemaV2.Columns = append(schemaV2.Columns,
		connector.Column{Name: "nullable", Type: "text", Nullable: true},
		connector.Column{Name: "date", Type: "date"}, connector.Column{Name: "timestamptz", Type: "timestamptz"},
		connector.Column{Name: "numeric", Type: "numeric(38,9)"}, connector.Column{Name: "uuid", Type: "uuid"},
		connector.Column{Name: "json", Type: "jsonb"}, connector.Column{Name: "bytea", Type: "bytea"}, connector.Column{Name: "array", Type: "text[]"},
	)
	transaction := clickHouseManagedTransactionWithFragments([]connector.TransactionFragment{
		{Ordinal: 0, Batch: connector.Batch{Schema: schemaV2, Records: []connector.Record{{
			Table: "typed_events", Operation: connector.OpDDL, SchemaVersion: 2, DDLPlan: []byte(`{"operation":"add_column","column":"array"}`),
		}}}},
		{Ordinal: 1, Batch: connector.Batch{Schema: schemaV2, Records: []connector.Record{record}}},
	})
	intent := clickHouseManagedIntent(t, transaction)
	if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); err != nil {
		t.Fatalf("structured DDL delivery: %v", err)
	}
	if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); err != nil {
		t.Fatalf("structured DDL idempotent replay: %v", err)
	}
	var operation, afterJSON, schemaJSON string
	if err := fixture.db.QueryRowContext(context.Background(),
		"SELECT operation,after_json,schema_json FROM {database:Identifier}.{view:Identifier} WHERE logical_batch_id={logical:String} AND fragment_ordinal=1",
		chclient.Named("database", fixture.database), chclient.Named("view", fixture.finalView), chclient.Named("logical", intent.LogicalBatchID),
	).Scan(&operation, &afterJSON, &schemaJSON); err != nil {
		t.Fatal(err)
	}
	if operation != "insert" || !strings.Contains(schemaJSON, `"Version":0`) {
		t.Fatalf("operation/schema=%q/%s", operation, schemaJSON)
	}
	for _, value := range []string{"1960-01-02T00:00:00Z", "-05:00", "12345678901234567890.123456789", "12345678-90ab-cdef-0123-456789abcdef", "AP+A", `"a":1`} {
		if !strings.Contains(afterJSON, value) {
			t.Fatalf("after_json missing %q: %s", value, afterJSON)
		}
	}
	var operations []string
	rows, err := fixture.db.QueryContext(context.Background(),
		"SELECT operation FROM {database:Identifier}.{view:Identifier} WHERE logical_batch_id={logical:String} ORDER BY fragment_ordinal",
		chclient.Named("database", fixture.database), chclient.Named("view", fixture.finalView), chclient.Named("logical", intent.LogicalBatchID),
	)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var operation string
		if err := rows.Scan(&operation); err != nil {
			t.Fatal(err)
		}
		operations = append(operations, operation)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if strings.Join(operations, ",") != "ddl,insert" {
		t.Fatalf("schema barrier order=%v", operations)
	}
}

func TestClickHouseManagedProfileTLS(t *testing.T) {
	if os.Getenv("WALLABY_TEST_CLICKHOUSE_TLS_DSN") == "" || os.Getenv("WALLABY_TEST_CLICKHOUSE_TLS_CA") == "" {
		t.Skip("managed ClickHouse TLS fixture not configured")
	}
	fixture := newClickHouseManagedFixture(t, 180)
	plainSpec := fixture.spec
	plainSpec.Options = make(map[string]string, len(fixture.spec.Options))
	for key, value := range fixture.spec.Options {
		plainSpec.Options[key] = value
	}
	plainSpec.Options["dsn"] = os.Getenv("WALLABY_TEST_CLICKHOUSE_DSN")
	candidate := &clickhousedest.Destination{}
	if err := candidate.Open(context.Background(), plainSpec); err == nil || !strings.Contains(err.Error(), "verified native TLS") {
		if err == nil {
			_ = candidate.Close(context.Background())
		}
		t.Fatalf("plaintext managed profile admission error=%v", err)
	}
	if version := fixture.destination.ManagedClickHouseVersion(); !connector.PostgresToClickHouseAppendV1Profile().SupportsClickHouseVersion(version) {
		t.Fatalf("TLS endpoint version %s is outside the profile", version)
	}
	transaction := clickHouseManagedTransaction("tls_events", 1, []connector.Record{
		clickHouseManagedRecord("tls_events", connector.OpInsert, 1, map[string]any{"id": int64(1), "value": "secure"}),
	})
	intent := clickHouseManagedIntent(t, transaction)
	if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); err != nil {
		t.Fatalf("TLS managed delivery: %v", err)
	}
	if got := fixture.logicalRowCount(t, intent.LogicalBatchID); got != 1 {
		t.Fatalf("TLS logical rows=%d", got)
	}
}

func TestClickHouseManagedProfileProcessKillRecovery(t *testing.T) {
	fixture := newClickHouseManagedFixture(t, 180)
	transaction := clickHouseManagedTransaction("process_kill", 1, []connector.Record{
		clickHouseManagedRecord("process_kill", connector.OpInsert, 1, map[string]any{"id": int64(1), "value": "durable"}),
	})
	intent := clickHouseManagedIntent(t, transaction)
	if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); err != nil {
		t.Fatalf("delivery before process kill: %v", err)
	}
	recoveredDSN := killClickHouseHarnessContainer(t, "clickhouse")
	fixture.reconnect(t, recoveredDSN)
	waitForClickHouseManagedReplicas(t, fixture, 90*time.Second)
	recoveredTLSDSN := restartClickHouseTLSHarnessPortForward(t)
	recoveredTLSURL, err := url.Parse(recoveredTLSDSN)
	if err != nil || recoveredTLSURL.Host == "" {
		t.Fatalf("parse recovered ClickHouse TLS DSN: %v", err)
	}
	fixture.tlsProxy.SetTarget(recoveredTLSURL.Host)

	restarted := &clickhousedest.Destination{}
	if err := restarted.Open(context.Background(), fixture.spec); err != nil {
		t.Fatalf("open after ClickHouse process kill: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close(context.Background()) })
	if err := restarted.InitializeManagedDelivery(context.Background()); err != nil {
		t.Fatalf("initialize after ClickHouse process kill: %v", err)
	}
	disposition, evidence, err := restarted.Reconcile(context.Background(), intent)
	if err != nil || disposition != connector.DeliveryApplied || evidence.ContentHash != intent.ContentHash {
		t.Fatalf("post-kill receipt reconciliation=(%v,%+v,%v)", disposition, evidence, err)
	}
	if _, err := restarted.ApplyTransaction(context.Background(), intent, transaction); err != nil {
		t.Fatalf("duplicate after restart: %v", err)
	}
	if got := fixture.logicalRowCount(t, intent.LogicalBatchID); got != 1 {
		t.Fatalf("post-kill logical rows=%d", got)
	}
}

func TestClickHouseManagedProfileSurvivorOnlyPrimaryStorageLossRecovery(t *testing.T) {
	if os.Getenv("WALLABY_TEST_CLICKHOUSE_DESTRUCTIVE_STORAGE_LOSS") != "1" {
		t.Skip("destructive primary-storage-loss evidence requires its disposable ClickHouse profile cluster")
	}
	fixture := newClickHouseManagedFixture(t, 180)
	restoreNeeded := false
	t.Cleanup(func() {
		if !restoreNeeded {
			return
		}
		if err := fixture.restorePrimaryAfterStorageLoss(context.Background()); err != nil {
			t.Errorf("restore ClickHouse primary after failed storage-loss test: %v", err)
		}
	})
	transaction := clickHouseManagedTransaction("primary_storage_loss", 1, []connector.Record{
		clickHouseManagedRecord("primary_storage_loss", connector.OpInsert, 1, map[string]any{"id": int64(1), "value": "survives"}),
	})
	intent := clickHouseManagedIntent(t, transaction)
	if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); err != nil {
		t.Fatalf("commit before primary storage loss: %v", err)
	}

	restoreNeeded = true
	recoveredDSN, recoveredTLSDSN := destroyClickHousePrimaryStorage(t)
	fixture.reconnect(t, recoveredDSN)
	recoveredTLSURL, err := url.Parse(recoveredTLSDSN)
	if err != nil || recoveredTLSURL.Host == "" {
		t.Fatalf("parse recovered ClickHouse TLS DSN: %v", err)
	}
	fixture.tlsProxy.SetTarget(recoveredTLSURL.Host)

	restarted := &clickhousedest.Destination{}
	if err := restarted.Open(context.Background(), fixture.spec); err != nil {
		t.Fatalf("open recovery-only destination after primary storage loss: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close(context.Background()) })
	if err := restarted.InitializeManagedDelivery(context.Background()); err != nil {
		t.Fatalf("initialize recovery-only destination after primary storage loss: %v", err)
	}
	disposition, evidence, err := restarted.Reconcile(context.Background(), intent)
	if err != nil || disposition != connector.DeliveryApplied || evidence.ContentHash != intent.ContentHash {
		t.Fatalf("survivor-only reconciliation=(%v,%+v,%v)", disposition, evidence, err)
	}
	// Replaying the already-applied batch must still adopt its durable receipt:
	// that read-only adoption is the entire purpose of recovery-only admission and
	// performs no write.
	if evidence, err := restarted.ApplyTransaction(context.Background(), intent, transaction); err != nil || evidence.ContentHash != intent.ContentHash {
		t.Fatalf("recovery-only replay of an applied batch=(%+v,%v), want adopted evidence", evidence, err)
	}
	// A genuinely new transaction is the write that must stay fenced, because one
	// survivor cannot satisfy the two-replica quorum contract.
	newTransaction := clickHouseManagedTransaction("primary_storage_loss_new", 2, []connector.Record{
		clickHouseManagedRecord("primary_storage_loss_new", connector.OpInsert, 2, map[string]any{"id": int64(2), "value": "fenced"}),
	})
	newIntent := clickHouseManagedIntent(t, newTransaction)
	if _, err := restarted.ApplyTransaction(context.Background(), newIntent, newTransaction); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("recovery-only write error=%v, want ErrDeliveryIndeterminate", err)
	}
	// Count on the survivor: the wiped primary no longer has the FINAL view, so
	// only the intact replica can answer for what the fence did or did not write.
	var fencedRows int
	if err := fixture.replicaDB.QueryRowContext(context.Background(),
		"SELECT count() FROM "+quoteClickHouseTestIdentifier(fixture.database)+"."+quoteClickHouseTestIdentifier(fixture.finalView)+" WHERE logical_batch_id=?",
		newIntent.LogicalBatchID,
	).Scan(&fencedRows); err != nil {
		t.Fatalf("query surviving replica for the fenced batch: %v", err)
	}
	if fencedRows != 0 {
		t.Fatalf("fenced recovery-only write left %d rows on the survivor", fencedRows)
	}
	var survivorRows int
	if err := fixture.replicaDB.QueryRowContext(context.Background(),
		"SELECT count() FROM "+quoteClickHouseTestIdentifier(fixture.database)+"."+quoteClickHouseTestIdentifier(fixture.finalView)+" WHERE logical_batch_id=?",
		intent.LogicalBatchID,
	).Scan(&survivorRows); err != nil {
		t.Fatalf("query surviving replica: %v", err)
	}
	if survivorRows != 1 {
		t.Fatalf("surviving replica logical rows=%d, want 1", survivorRows)
	}
	if err := fixture.restorePrimaryAfterStorageLoss(context.Background()); err != nil {
		t.Fatalf("restore ClickHouse primary after storage-loss evidence: %v", err)
	}
	waitForClickHouseManagedReplicas(t, fixture, 90*time.Second)
	restoreNeeded = false
}

func TestClickHouseManagedProfileKeeperFailureRecovery(t *testing.T) {
	fixture := newClickHouseManagedFixture(t, 180)
	before := clickHouseManagedTransaction("keeper_kill", 1, []connector.Record{
		clickHouseManagedRecord("keeper_kill", connector.OpInsert, 1, map[string]any{"id": int64(1), "value": "before"}),
	})
	beforeIntent := clickHouseManagedIntent(t, before)
	if _, err := fixture.destination.ApplyTransaction(context.Background(), beforeIntent, before); err != nil {
		t.Fatalf("delivery before Keeper failure: %v", err)
	}
	_ = killClickHouseHarnessContainer(t, "keeper")
	waitForClickHouseManagedReplicas(t, fixture, 90*time.Second)
	fixture.keeperProxy.SetTarget(restartClickHouseKeeperHarnessPortForward(t))

	restarted := &clickhousedest.Destination{}
	if err := restarted.Open(context.Background(), fixture.spec); err != nil {
		t.Fatalf("open after Keeper process kill: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close(context.Background()) })
	if err := restarted.InitializeManagedDelivery(context.Background()); err != nil {
		t.Fatalf("initialize after Keeper process kill: %v", err)
	}
	if disposition, evidence, err := restarted.Reconcile(context.Background(), beforeIntent); err != nil || disposition != connector.DeliveryApplied || evidence.ContentHash != beforeIntent.ContentHash {
		t.Fatalf("Keeper receipt recovery reconciliation=(%v,%+v,%v)", disposition, evidence, err)
	}
	restartedWriter := &clickHouseManagedTestDestination{Destination: restarted, coordinator: fixture.coordinator, fence: fixture.fence, revision: fixture.revision}
	if _, err := restartedWriter.ApplyTransaction(context.Background(), beforeIntent, before); err != nil {
		t.Fatalf("Keeper partial replay: %v", err)
	}
	after := clickHouseManagedTransaction("keeper_kill", 1, []connector.Record{
		clickHouseManagedRecord("keeper_kill", connector.OpInsert, 1, map[string]any{"id": int64(2), "value": "after"}),
	})
	if _, err := restartedWriter.ApplyTransaction(context.Background(), clickHouseManagedIntent(t, after), after); err != nil {
		t.Fatalf("delivery after Keeper recovery: %v", err)
	}
}

func TestClickHouseManagedProfileBackpressure(t *testing.T) {
	// One physical changelog insert plus its receipt requires capacity two.
	fixture := newClickHouseManagedFixture(t, 2)
	first := clickHouseManagedTransaction("backpressure", 1, []connector.Record{clickHouseManagedRecord("backpressure", connector.OpInsert, 1, map[string]any{"id": int64(1)})})
	firstIntent := transactionIntentForFence(t, fixture.fence, fixture.revision, first)
	if _, err := fixture.coordinator.DeliverTransaction(context.Background(), fixture.fence, firstIntent, first, managedBaselinePayload(t, first), fixture.destination); err != nil {
		t.Fatal(err)
	}
	second := clickHouseManagedTransaction("backpressure", 1, []connector.Record{clickHouseManagedRecord("backpressure", connector.OpInsert, 1, map[string]any{"id": int64(2)})})
	secondIntent := transactionIntentForFence(t, fixture.fence, fixture.revision, second)
	if _, err := fixture.coordinator.DeliverTransaction(context.Background(), fixture.fence, secondIntent, second, managedBaselinePayload(t, second), fixture.destination); err == nil || !strings.Contains(err.Error(), "backpressure") {
		t.Fatalf("backpressure error=%v", err)
	}
	var completed int
	if err := fixture.controlPool.QueryRow(context.Background(), `SELECT count(*) FROM managed_part_reservations WHERE destination_revision_id=$1 AND reservation_state='completed_pending_observation'`, fixture.revision).Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if completed != 1 {
		t.Fatalf("completed_pending_observation reservations=%d, want 1 charged reservation", completed)
	}
}

type keeperProbeProxy struct {
	listener net.Listener
	mu       sync.RWMutex
	target   string
	blocked  bool
	active   map[net.Conn]struct{}
}

func startKeeperProbeProxy(t *testing.T, target string) *keeperProbeProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("start Keeper probe proxy: %v", err)
	}
	proxy := &keeperProbeProxy{listener: listener, target: target, active: make(map[net.Conn]struct{})}
	t.Cleanup(func() { _ = listener.Close() })
	go proxy.serve()
	return proxy
}

func (p *keeperProbeProxy) Address() string {
	return p.listener.Addr().String()
}

func (p *keeperProbeProxy) SetTarget(target string) {
	p.mu.Lock()
	p.target = target
	p.mu.Unlock()
}

func (p *keeperProbeProxy) SetBlocked(blocked bool) {
	p.mu.Lock()
	p.blocked = blocked
	if blocked {
		for conn := range p.active {
			_ = conn.Close()
		}
	}
	p.mu.Unlock()
}

func (p *keeperProbeProxy) serve() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		go p.forward(client)
	}
}

func (p *keeperProbeProxy) forward(client net.Conn) {
	defer client.Close()
	p.mu.RLock()
	target := p.target
	blocked := p.blocked
	p.mu.RUnlock()
	if blocked {
		return
	}
	upstream, err := net.DialTimeout("tcp", target, 2*time.Second)
	if err != nil {
		return
	}
	defer upstream.Close()
	p.mu.Lock()
	if p.blocked {
		p.mu.Unlock()
		return
	}
	p.active[client] = struct{}{}
	p.active[upstream] = struct{}{}
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.active, client)
		delete(p.active, upstream)
		p.mu.Unlock()
	}()
	var transfers sync.WaitGroup
	transfers.Add(2)
	go func() {
		defer transfers.Done()
		_, _ = io.Copy(upstream, client)
	}()
	go func() {
		defer transfers.Done()
		_, _ = io.Copy(client, upstream)
	}()
	transfers.Wait()
}

type clickHouseManagedTestDestination struct {
	*clickhousedest.Destination
	coordinator *delivery.Coordinator
	fence       authority.RunFence
	revision    string
}

func (d *clickHouseManagedTestDestination) authoritativeIntent(intent connector.DeliveryIntent) connector.DeliveryIntent {
	intent.FlowID = d.fence.FlowID
	intent.FlowIncarnationID = d.fence.FlowIncarnationID.String()
	intent.Generation = d.fence.Generation
	intent.AcquisitionID = d.fence.AcquisitionID.String()
	intent.LeaseEpoch = d.fence.LeaseEpoch
	intent.DestinationRevisionID = d.revision
	return intent
}

func (d *clickHouseManagedTestDestination) ApplyTransaction(ctx context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (connector.DeliveryEvidence, error) {
	if d.coordinator == nil {
		return connector.DeliveryEvidence{}, errors.New("managed ClickHouse test destination requires the PostgreSQL coordinator")
	}
	intent = d.authoritativeIntent(intent)
	baselines, err := connector.NewManagedSchemaBaselinePayload(transaction.SourceLineageID, connector.SourceTransactionSchemas(transaction))
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	grant, err := d.coordinator.DeliverTransaction(ctx, d.fence, intent, transaction, baselines, d)
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	return connector.DeliveryEvidence{ExternalID: grant.PositionID, ContentHash: intent.ContentHash}, nil
}

func (d *clickHouseManagedTestDestination) Reconcile(ctx context.Context, intent connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	return d.Destination.Reconcile(ctx, d.authoritativeIntent(intent))
}

type clickHouseManagedFixture struct {
	destination     *clickHouseManagedTestDestination
	db              *sql.DB
	replicaDB       *sql.DB
	keeperProxy     *keeperProbeProxy
	tlsProxy        *keeperProbeProxy
	replicaTLSProxy *keeperProbeProxy
	spec            connector.RuntimeSpec
	database        string
	changelogTable  string
	receiptsTable   string
	finalView       string
	changelogDDL    string
	receiptsDDL     string
	viewDDL         string
	coordinator     *delivery.Coordinator
	controlPool     *pgxpool.Pool
	fence           authority.RunFence
	revision        string
}

func newClickHouseManagedFixture(t *testing.T, maxActiveParts uint64) *clickHouseManagedFixture {
	t.Helper()
	return newClickHouseManagedFixtureWithHooks(t, maxActiveParts, 10000, delivery.CoordinatorHooks{})
}

func newClickHouseManagedFixtureWithInsertRows(t *testing.T, maxActiveParts uint64, maxRowsPerInsert int) *clickHouseManagedFixture {
	t.Helper()
	return newClickHouseManagedFixtureWithHooks(t, maxActiveParts, maxRowsPerInsert, delivery.CoordinatorHooks{})
}

func newClickHouseManagedFixtureWithHooks(t *testing.T, maxActiveParts uint64, maxRowsPerInsert int, hooks delivery.CoordinatorHooks) *clickHouseManagedFixture {
	t.Helper()
	dsn := os.Getenv("WALLABY_TEST_CLICKHOUSE_TLS_DSN")
	setupDSN := os.Getenv("WALLABY_TEST_CLICKHOUSE_DSN")
	replicaDSN := os.Getenv("WALLABY_TEST_CLICKHOUSE_REPLICA_DSN")
	replicaTLSDSN := os.Getenv("WALLABY_TEST_CLICKHOUSE_REPLICA_TLS_DSN")
	keeperAddress := os.Getenv("WALLABY_TEST_CLICKHOUSE_KEEPER_ADDRESS")
	if dsn == "" || setupDSN == "" || replicaDSN == "" || replicaTLSDSN == "" || keeperAddress == "" {
		t.Skip("managed ClickHouse TLS, Keeper, and two-replica endpoints are not set")
	}
	tlsURL, err := url.Parse(dsn)
	if err != nil || tlsURL.Host == "" {
		t.Fatalf("parse managed ClickHouse TLS DSN: %v", err)
	}
	tlsProxy := startKeeperProbeProxy(t, tlsURL.Host)
	tlsURL.Host = tlsProxy.Address()
	dsn = tlsURL.String()
	replicaTLSURL, err := url.Parse(replicaTLSDSN)
	if err != nil || replicaTLSURL.Host == "" {
		t.Fatalf("parse managed ClickHouse replica TLS DSN: %v", err)
	}
	replicaTLSProxy := startKeeperProbeProxy(t, replicaTLSURL.Host)
	replicaTLSURL.Host = replicaTLSProxy.Address()
	replicaTLSDSN = replicaTLSURL.String()
	database := os.Getenv("WALLABY_TEST_CLICKHOUSE_DB")
	if database == "" {
		database = "default"
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	fixture := &clickHouseManagedFixture{
		database: database, changelogTable: "wallaby_managed_log_" + suffix,
		receiptsTable: "wallaby_managed_receipts_" + suffix, finalView: "wallaby_managed_final_" + suffix,
		keeperProxy: startKeeperProbeProxy(t, keeperAddress),
		tlsProxy:    tlsProxy, replicaTLSProxy: replicaTLSProxy,
	}
	if candidate := os.Getenv("WALLABY_TEST_CLICKHOUSE_SETUP_DSN"); candidate != "" {
		setupDSN = candidate
	}
	db, err := sql.Open("clickhouse", setupDSN)
	if err != nil {
		t.Fatal(err)
	}
	fixture.db = db
	t.Cleanup(func() { _ = db.Close() })
	replicaDB, err := sql.Open("clickhouse", replicaDSN)
	if err != nil {
		t.Fatal(err)
	}
	fixture.replicaDB = replicaDB
	t.Cleanup(func() { _ = replicaDB.Close() })
	ctx := context.Background()
	changelogDDL := fmt.Sprintf(`CREATE TABLE %s.%s (
flow_id String, flow_incarnation_id String, source_lineage_id String, destination_revision_id String,
logical_batch_id String, content_hash FixedString(64), source_position String, transaction_id UInt64,
begin_lsn String, commit_lsn String, end_lsn String, fragment_ordinal UInt64, record_ordinal UInt64,
source_namespace String, source_table String, schema_version Int64, schema_fingerprint FixedString(64), schema_json String,
operation LowCardinality(String), tombstone UInt8, key_json String, before_json String, after_json String,
payload String, ddl_plan String, event_time DateTime64(9, 'UTC'), insert_query_id String, record_hash FixedString(64), wallaby_version UInt64
) ENGINE=ReplicatedReplacingMergeTree('/clickhouse/tables/01/%s/%s','wallaby-it-1',wallaby_version)
ORDER BY (destination_revision_id,logical_batch_id,fragment_ordinal,record_ordinal)
SETTINGS replicated_deduplication_window=1000,replicated_deduplication_window_seconds=3600,
parts_to_delay_insert=100,parts_to_throw_insert=200,max_parts_in_total=1000`, database, fixture.changelogTable, database, fixture.changelogTable)
	receiptsDDL := fmt.Sprintf(`CREATE TABLE %s.%s (
flow_id String, flow_incarnation_id String, source_lineage_id String, destination_revision_id String,
logical_batch_id String, content_hash FixedString(64), source_position String, transaction_id UInt64,
fragment_count UInt64, record_count UInt64, query_ids Array(String), committed_at DateTime64(9, 'UTC'),
wallaby_version UInt64, external_id String
) ENGINE=ReplicatedReplacingMergeTree('/clickhouse/tables/01/%s/%s','wallaby-it-1',wallaby_version)
ORDER BY (destination_revision_id,logical_batch_id)
SETTINGS replicated_deduplication_window=1000,replicated_deduplication_window_seconds=3600,
parts_to_delay_insert=100,parts_to_throw_insert=200,max_parts_in_total=1000`, database, fixture.receiptsTable, database, fixture.receiptsTable)
	fixture.changelogDDL = changelogDDL
	fixture.receiptsDDL = receiptsDDL
	for _, statement := range []string{changelogDDL, receiptsDDL} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("create managed ClickHouse primary fixture: %v\n%s", err, statement)
		}
		secondReplicaStatement := strings.Replace(statement, "'wallaby-it-1'", "'wallaby-it-2'", 1)
		if _, err := replicaDB.ExecContext(ctx, secondReplicaStatement); err != nil {
			t.Fatalf("create managed ClickHouse second replica fixture: %v\n%s", err, secondReplicaStatement)
		}
	}
	viewDDL := fmt.Sprintf("CREATE VIEW %s.%s AS SELECT * FROM %s.%s FINAL", database, fixture.finalView, database, fixture.changelogTable)
	fixture.viewDDL = viewDDL
	if _, err := db.ExecContext(ctx, viewDDL); err != nil {
		t.Fatalf("create managed ClickHouse FINAL view: %v\n%s", err, viewDDL)
	}
	if _, err := replicaDB.ExecContext(ctx, viewDDL); err != nil {
		t.Fatalf("create managed ClickHouse replica FINAL view: %v\n%s", err, viewDDL)
	}
	waitForClickHouseManagedReplicas(t, fixture, 60*time.Second)
	t.Cleanup(func() {
		_, _ = fixture.replicaDB.ExecContext(context.Background(), "DROP VIEW IF EXISTS {database:Identifier}.{view:Identifier}", chclient.Named("database", database), chclient.Named("view", fixture.finalView))
		_, _ = fixture.db.ExecContext(context.Background(), "DROP VIEW IF EXISTS {database:Identifier}.{view:Identifier}", chclient.Named("database", database), chclient.Named("view", fixture.finalView))
		for _, table := range []string{fixture.receiptsTable, fixture.changelogTable} {
			_, _ = fixture.replicaDB.ExecContext(context.Background(), "DROP TABLE IF EXISTS {database:Identifier}.{table:Identifier} SYNC", chclient.Named("database", database), chclient.Named("table", table))
			_, _ = fixture.db.ExecContext(context.Background(), "DROP TABLE IF EXISTS {database:Identifier}.{table:Identifier} SYNC", chclient.Named("database", database), chclient.Named("table", table))
		}
	})
	fixture.spec = connector.RuntimeSpec{Name: "clickhouse-managed", Type: connector.EndpointClickHouse, Options: map[string]string{
		"dsn": dsn, "managed_profile": connector.ManagedProfilePostgresToClickHouseAppendV1,
		"destination_revision_id": "clickhouse-managed-v1", "batch_mode": "target", "batch_resolution": "none",
		"meta_table_enabled": "false", "managed_database": database, "managed_changelog_table": fixture.changelogTable,
		"managed_receipts_table": fixture.receiptsTable, "managed_final_view": fixture.finalView,
		"managed_deployment": "self-managed-keeper", "managed_keeper_path_prefix": "/clickhouse/tables/01",
		"managed_keeper_address": fixture.keeperProxy.Address(), "managed_replica_dsn": replicaTLSDSN, "managed_replica_names": "wallaby-it-1,wallaby-it-2", "insert_quorum": "2", "async_insert": "false", "wait_for_async_insert": "true",
		"managed_max_active_parts":     fmt.Sprintf("%d", maxActiveParts),
		"managed_max_transaction_rows": "100000", "managed_max_transaction_bytes": "134217728", "managed_max_transaction_fragments": "128",
		"managed_max_rows_per_batch": fmt.Sprintf("%d", maxRowsPerInsert), "managed_max_batch_bytes": "16777216",
		"tls_ca_file": os.Getenv("WALLABY_TEST_CLICKHOUSE_TLS_CA"),
	}}
	authorityFixture := newPartReservationFixture(t, hooks)
	revision := "clickhouse-managed-" + suffix
	authorityFixture.register(t, revision)
	fixture.coordinator, fixture.controlPool, fixture.fence, fixture.revision = authorityFixture.coordinator, authorityFixture.pool, authorityFixture.fence, revision
	fixture.destination = &clickHouseManagedTestDestination{Destination: &clickhousedest.Destination{}, coordinator: fixture.coordinator, fence: fixture.fence, revision: fixture.revision}
	if err := fixture.destination.Open(ctx, fixture.spec); err != nil {
		t.Fatalf("open managed ClickHouse destination: %v", err)
	}
	t.Cleanup(func() { _ = fixture.destination.Close(context.Background()) })
	return fixture
}

func waitForClickHouseManagedReplicas(t *testing.T, fixture *clickHouseManagedFixture, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var primaryHealthy, primaryTables, replicaHealthy, replicaTables uint64
	query := `
SELECT countIf(is_readonly=0 AND is_session_expired=0 AND total_replicas=2 AND active_replicas=2 AND queue_size=0 AND absolute_delay=0 AND lost_part_count=0),count()
FROM system.replicas
WHERE database=? AND table IN (?,?)`
	for time.Now().Before(deadline) {
		primaryErr := fixture.db.QueryRowContext(context.Background(), query, fixture.database, fixture.changelogTable, fixture.receiptsTable).Scan(&primaryHealthy, &primaryTables)
		replicaErr := fixture.replicaDB.QueryRowContext(context.Background(), query, fixture.database, fixture.changelogTable, fixture.receiptsTable).Scan(&replicaHealthy, &replicaTables)
		if primaryErr == nil && replicaErr == nil && primaryTables == 2 && primaryHealthy == 2 && replicaTables == 2 && replicaHealthy == 2 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("ClickHouse two-replica tables did not become healthy: primary=%d/%d replica=%d/%d", primaryHealthy, primaryTables, replicaHealthy, replicaTables)
}

func (f *clickHouseManagedFixture) reconnect(t *testing.T, dsn string) {
	t.Helper()
	_ = f.db.Close()
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatal(err)
	}
	f.db = db
	t.Cleanup(func() { _ = db.Close() })
}

func (f *clickHouseManagedFixture) restorePrimaryAfterStorageLoss(ctx context.Context) error {
	deadline := time.Now().Add(45 * time.Second)
	for {
		var active uint64
		err := f.replicaDB.QueryRowContext(ctx,
			"SELECT active_replicas FROM system.replicas WHERE database=? AND table=?",
			f.database, f.receiptsTable,
		).Scan(&active)
		if err == nil && active == 1 {
			break
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("wait for lost primary replica session to expire: active=%d: %w", active, err)
			}
			return fmt.Errorf("wait for lost primary replica session to expire: active=%d, want 1", active)
		}
		time.Sleep(500 * time.Millisecond)
	}
	for _, table := range []string{f.receiptsTable, f.changelogTable} {
		if _, err := f.replicaDB.ExecContext(ctx,
			"SYSTEM DROP REPLICA 'wallaby-it-1' FROM TABLE "+quoteClickHouseTestIdentifier(f.database)+"."+quoteClickHouseTestIdentifier(table)); err != nil {
			return fmt.Errorf("drop lost primary replica for %s: %w", table, err)
		}
	}
	for _, statement := range []string{f.changelogDDL, f.receiptsDDL, f.viewDDL} {
		if _, err := f.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("restore primary fixture: %w", err)
		}
	}
	return nil
}

func (f *clickHouseManagedFixture) logicalRowCount(t *testing.T, logicalBatchID string) int {
	t.Helper()
	var count int
	if err := f.db.QueryRowContext(context.Background(),
		"SELECT count() FROM {database:Identifier}.{view:Identifier} WHERE logical_batch_id={logical:String}",
		chclient.Named("database", f.database), chclient.Named("view", f.finalView), chclient.Named("logical", logicalBatchID),
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func quoteClickHouseTestIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func clickHouseManagedTransaction(table string, schemaVersion int64, records []connector.Record) connector.SourceTransaction {
	return clickHouseManagedTransactionWithFragments([]connector.TransactionFragment{{
		Ordinal: 0, Batch: connector.Batch{Schema: clickHouseManagedSchema(table, schemaVersion), Records: records},
	}})
}

func clickHouseManagedTransactionWithFragments(fragments []connector.TransactionFragment) connector.SourceTransaction {
	sequence := clickHouseManagedSequence.Add(0x100)
	return connector.SourceTransaction{
		SourceLineageID: "postgres-system/publication-v1", TransactionID: uint32(sequence),
		BeginLSN: fmt.Sprintf("0/%X", sequence-0x20), CommitLSN: fmt.Sprintf("0/%X", sequence-0x8), EndLSN: fmt.Sprintf("0/%X", sequence),
		Checkpoint: connector.Checkpoint{LSN: fmt.Sprintf("0/%X", sequence), Timestamp: time.Now().UTC()}, Fragments: fragments,
	}
}

func clickHouseManagedSchema(table string, version int64) connector.Schema {
	return connector.Schema{Namespace: "public", Name: table, Version: version, Columns: []connector.Column{{Name: "id", Type: "bigint"}, {Name: "value", Type: "text", Nullable: true}}}
}

func clickHouseManagedRecord(table string, operation connector.Operation, schemaVersion int64, after map[string]any) connector.Record {
	key, _ := json.Marshal(map[string]any{"id": after["id"]})
	return connector.Record{Table: table, Operation: operation, SchemaVersion: schemaVersion, Key: key, After: after, Timestamp: time.Now().UTC()}
}

func clickHouseManagedIntent(t *testing.T, transaction connector.SourceTransaction) connector.DeliveryIntent {
	t.Helper()
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		t.Fatal(err)
	}
	return connector.DeliveryIntent{
		FlowID: "clickhouse-managed-flow", FlowIncarnationID: "11111111-1111-1111-1111-111111111111",
		SourceLineageID: transaction.SourceLineageID, Generation: 1,
		AcquisitionID: "22222222-2222-2222-2222-222222222222", LeaseEpoch: 1,
		DestinationRevisionID: "clickhouse-managed-v1", LogicalBatchID: logicalBatchID,
		PositionID: transaction.Checkpoint.LSN, ContentHash: contentHash,
	}
}

type clickHousePodStatus struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Status struct {
			ContainerStatuses []struct {
				Name         string `json:"name"`
				Ready        bool   `json:"ready"`
				RestartCount int32  `json:"restartCount"`
			} `json:"containerStatuses"`
		} `json:"status"`
	} `json:"items"`
}

func destroyClickHousePrimaryStorage(t *testing.T) (string, string) {
	t.Helper()
	kubeconfig := os.Getenv("WALLABY_TEST_K8S_KUBECONFIG")
	if kubeconfig == "" {
		t.Skip("WALLABY_TEST_K8S_KUBECONFIG not set")
	}
	namespace := os.Getenv("WALLABY_TEST_K8S_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}
	pod, ready, found, err := clickHouseHarnessContainerStatus(kubeconfig, namespace, "app=wallaby-it-clickhouse", "clickhouse", "")
	if err != nil || !found || !ready {
		t.Fatalf("read ClickHouse primary before storage loss: pod=%q ready=%t found=%t err=%v", pod, ready, found, err)
	}
	wipe := exec.Command("kubectl", "--kubeconfig", kubeconfig, "-n", namespace, "exec", pod, "-c", "clickhouse", "--", "bash", "-ec", "kill -STOP 1; rm -rf /var/lib/clickhouse/*; sync")
	if output, err := wipe.CombinedOutput(); err != nil {
		t.Fatalf("destroy ClickHouse primary storage: %v: %s", err, output)
	}
	remove := exec.Command("kubectl", "--kubeconfig", kubeconfig, "-n", namespace, "delete", "pod", pod, "--grace-period=0", "--force", "--wait=false")
	if output, err := remove.CombinedOutput(); err != nil {
		t.Fatalf("replace ClickHouse primary after storage loss: %v: %s", err, output)
	}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		currentPod, currentReady, currentFound, currentErr := clickHouseHarnessContainerStatus(kubeconfig, namespace, "app=wallaby-it-clickhouse", "clickhouse", pod)
		if currentErr == nil && currentFound && currentPod != pod && currentReady {
			return restartClickHouseHarnessPortForward(t, kubeconfig, namespace), restartClickHouseTLSHarnessPortForward(t)
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("ClickHouse primary did not restart with empty storage")
	return "", ""
}

func killClickHouseHarnessContainer(t *testing.T, container string) string {
	t.Helper()
	kubeconfig := os.Getenv("WALLABY_TEST_K8S_KUBECONFIG")
	if kubeconfig == "" {
		t.Skip("WALLABY_TEST_K8S_KUBECONFIG not set")
	}
	namespace := os.Getenv("WALLABY_TEST_K8S_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}
	label := "app=wallaby-it-clickhouse"
	if container == "keeper" {
		label = "app=wallaby-it-clickhouse-keeper"
	}
	pod, ready, found, err := clickHouseHarnessContainerStatus(kubeconfig, namespace, label, container, "")
	if err != nil || !found || !ready {
		t.Fatalf("read %s pod before kill: pod=%q ready=%t found=%t err=%v", container, pod, ready, found, err)
	}
	command := exec.Command("kubectl", "--kubeconfig", kubeconfig, "-n", namespace, "delete", "pod", pod, "--grace-period=0", "--force", "--wait=false")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("force kill %s pod: %v: %s", container, err, output)
	}

	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		currentPod, currentReady, currentFound, currentErr := clickHouseHarnessContainerStatus(kubeconfig, namespace, label, container, pod)
		if currentErr == nil && currentFound && currentPod != pod && currentReady {
			if container == "clickhouse" {
				return restartClickHouseHarnessPortForward(t, kubeconfig, namespace)
			}
			return os.Getenv("WALLABY_TEST_CLICKHOUSE_DSN")
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("%s process did not recover in a replacement pod after forced deletion", container)
	return ""
}

func restartClickHouseHarnessPortForward(t *testing.T, kubeconfig, namespace string) string {
	t.Helper()
	parsed, err := url.Parse(os.Getenv("WALLABY_TEST_CLICKHOUSE_DSN"))
	if err != nil || parsed.Port() == "" {
		t.Fatalf("parse ClickHouse test DSN for port-forward recovery: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve replacement ClickHouse local port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	command := exec.Command("kubectl", "--kubeconfig", kubeconfig, "-n", namespace, "port-forward", "service/wallaby-it-clickhouse", strconv.Itoa(port)+":9000", "--address", "127.0.0.1")
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		t.Fatalf("restart ClickHouse port-forward: %v", err)
	}
	go func() { _ = command.Wait() }()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		connection, dialErr := net.DialTimeout("tcp", address, 500*time.Millisecond)
		if dialErr == nil {
			_ = connection.Close()
			parsed.Host = address
			recoveredDSN := parsed.String()
			if err := os.Setenv("WALLABY_TEST_CLICKHOUSE_DSN", recoveredDSN); err != nil {
				t.Fatalf("record recovered ClickHouse DSN: %v", err)
			}
			return recoveredDSN
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("ClickHouse replacement port-forward did not become ready at %s", address)
	return ""
}

func restartClickHouseTLSHarnessPortForward(t *testing.T) string {
	t.Helper()
	kubeconfig := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_KUBECONFIG"))
	namespace := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_NAMESPACE"))
	parsed, err := url.Parse(os.Getenv("WALLABY_TEST_CLICKHOUSE_TLS_DSN"))
	if kubeconfig == "" || namespace == "" || err != nil || parsed.Port() == "" {
		t.Fatalf("parse ClickHouse TLS harness recovery configuration: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve replacement ClickHouse TLS local port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	command := exec.Command("kubectl", "--kubeconfig", kubeconfig, "-n", namespace, "port-forward", "service/wallaby-it-clickhouse", strconv.Itoa(port)+":9440", "--address", "127.0.0.1")
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		t.Fatalf("restart ClickHouse TLS port-forward: %v", err)
	}
	go func() { _ = command.Wait() }()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		connection, dialErr := net.DialTimeout("tcp", address, 500*time.Millisecond)
		if dialErr == nil {
			_ = connection.Close()
			parsed.Host = address
			recoveredDSN := parsed.String()
			if err := os.Setenv("WALLABY_TEST_CLICKHOUSE_TLS_DSN", recoveredDSN); err != nil {
				t.Fatalf("record recovered ClickHouse TLS DSN: %v", err)
			}
			return recoveredDSN
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("ClickHouse TLS replacement port-forward did not become ready at %s", address)
	return ""
}

func restartClickHouseKeeperHarnessPortForward(t *testing.T) string {
	t.Helper()
	kubeconfig := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_KUBECONFIG"))
	namespace := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_NAMESPACE"))
	if kubeconfig == "" || namespace == "" {
		t.Fatal("ClickHouse Keeper recovery requires the Kubernetes harness")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve replacement Keeper local port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	command := exec.Command("kubectl", "--kubeconfig", kubeconfig, "-n", namespace, "port-forward", "service/wallaby-it-clickhouse-keeper", strconv.Itoa(port)+":9181", "--address", "127.0.0.1")
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		t.Fatalf("restart ClickHouse Keeper port-forward: %v", err)
	}
	go func() { _ = command.Wait() }()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		connection, dialErr := net.DialTimeout("tcp", address, 500*time.Millisecond)
		if dialErr == nil {
			_ = connection.Close()
			if err := os.Setenv("WALLABY_TEST_CLICKHOUSE_KEEPER_ADDRESS", address); err != nil {
				t.Fatalf("record recovered ClickHouse Keeper address: %v", err)
			}
			return address
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("ClickHouse Keeper replacement port-forward did not become ready at %s", address)
	return ""
}

func clickHouseHarnessContainerStatus(kubeconfig, namespace, label, container, excludePod string) (string, bool, bool, error) {
	command := exec.Command("kubectl", "--kubeconfig", kubeconfig, "-n", namespace, "get", "pods", "-l", label, "-o", "json")
	output, err := command.CombinedOutput()
	if err != nil {
		return "", false, false, fmt.Errorf("read pod status: %w: %s", err, output)
	}
	var pods clickHousePodStatus
	if err := json.Unmarshal(output, &pods); err != nil {
		return "", false, false, fmt.Errorf("decode pod status: %w", err)
	}
	for _, pod := range pods.Items {
		if pod.Metadata.Name == excludePod {
			continue
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == container {
				return pod.Metadata.Name, status.Ready, true, nil
			}
		}
	}
	return "", false, false, nil
}
