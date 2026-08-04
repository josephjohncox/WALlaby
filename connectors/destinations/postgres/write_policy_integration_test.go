package postgres

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestBootstrapWatermarkReplacementDropsAbsentTombstonesAndIsIdempotent(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	destination := &Destination{flowID: fmt.Sprintf("bootstrap-replace-%d", time.Now().UnixNano())}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	destination.pool = pool
	tableName := fmt.Sprintf("wallaby_bootstrap_replace_%d", time.Now().UnixNano())
	qualified := quoteIdent("public", '"') + "." + quoteIdent(tableName, '"')
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id bigint PRIMARY KEY,name text,updated_at bigint NOT NULL); INSERT INTO %s VALUES (2,'snapshot',100)`, qualified, qualified)); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM wallaby.watermark_state WHERE flow_id=$1`, destination.flowID)
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS `+qualified)
	}()
	schema := watermarkSchema()
	schema.Name = tableName
	policy := watermarkPolicy()
	table := connector.BootstrapTable{Schema: schema, WritePolicy: policy, SourcePosition: "0/100"}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureWatermarkStateTable(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	for _, key := range []string{"1", "2"} {
		if _, err := tx.Exec(ctx, `INSERT INTO wallaby.watermark_state(flow_id,target_schema,target_table,projection_fingerprint,key_columns,key_values,watermark_type,watermark_value,source_position,content_hash,deleted) VALUES($1,'public',$2,$3,$4,$5,'bigint','999','0/90','old',true)`, destination.flowID, tableName, policy.ProjectionFingerprint, policy.KeyColumns, []string{key}); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
	}
	if err := destination.seedBootstrapWatermarkState(ctx, tx, connector.BootstrapIntent{ManifestHash: "manifest"}, "public", tableName, table); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		tx, err = pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := destination.seedBootstrapWatermarkState(ctx, tx, connector.BootstrapIntent{ManifestHash: "manifest"}, "public", tableName, table); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var keys, watermarks []string
	if err := pool.QueryRow(ctx, `SELECT array_agg(key_values[1] ORDER BY key_values[1]),array_agg(watermark_value ORDER BY key_values[1]) FROM wallaby.watermark_state WHERE flow_id=$1 AND target_schema='public' AND target_table=$2 AND projection_fingerprint=$3 AND key_columns=$4`, destination.flowID, tableName, policy.ProjectionFingerprint, policy.KeyColumns).Scan(&keys, &watermarks); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "2" || watermarks[0] != "100" {
		t.Fatalf("replaced bootstrap state keys/watermarks=%v/%v", keys, watermarks)
	}
	reinsert := connector.Record{Table: tableName, Operation: connector.OpInsert, Key: []byte(`{"id":1}`), After: map[string]any{"id": int64(1), "name": "later", "updated_at": int64(1)}, SourcePosition: "0/110"}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := destination.applyWatermarkBatch(ctx, tx, qualified, schema, []connector.Record{reinsert}, policy); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := pool.QueryRow(ctx, `SELECT name FROM `+qualified+` WHERE id=1`).Scan(&name); err != nil || name != "later" {
		t.Fatalf("absent snapshot key reinsertion name=%q err=%v", name, err)
	}
}

func TestPostgresWatermarkStateSerializesAndPreservesDeleteTombstone(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	table := "wallaby_watermark_" + uuid.NewString()[:8]
	flowID := "watermark-" + uuid.NewString()
	qualified := `"public"."` + table + `"`
	if _, err := pool.Exec(ctx, `CREATE TABLE `+qualified+` (id bigint PRIMARY KEY,name text,updated_at bigint NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS `+qualified)
		_, _ = pool.Exec(context.Background(), `DELETE FROM wallaby.watermark_state WHERE flow_id=$1`, flowID)
	}()
	destination := &Destination{pool: pool, flowID: flowID}
	schema := watermarkSchema()
	schema.Name = table
	apply := func(record connector.Record) error {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(context.Background())
		if err := destination.applyWatermarkBatch(ctx, tx, qualified, schema, []connector.Record{record}, watermarkPolicy()); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	insert := func(value int64, name string) connector.Record {
		return connector.Record{Table: table, Operation: connector.OpInsert, Key: []byte(`{"id":1}`), After: map[string]any{"id": int64(1), "name": name, "updated_at": value}, SourcePosition: fmt.Sprintf("0/%X", value)}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, record := range []connector.Record{insert(10, "ten"), insert(20, "twenty")} {
		wg.Add(1)
		go func(record connector.Record) { defer wg.Done(); errs <- apply(record) }(record)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var name string
	var watermark int64
	if err := pool.QueryRow(ctx, `SELECT name,updated_at FROM `+qualified+` WHERE id=1`).Scan(&name, &watermark); err != nil {
		t.Fatal(err)
	}
	if name != "twenty" || watermark != 20 {
		t.Fatalf("target=%s/%d, want twenty/20", name, watermark)
	}
	deleteRecord := connector.Record{Table: table, Operation: connector.OpDelete, Key: []byte(`{"id":1}`), Before: map[string]any{"id": int64(1), "updated_at": int64(30)}, SourcePosition: "0/30"}
	if err := apply(deleteRecord); err != nil {
		t.Fatal(err)
	}
	if err := apply(insert(25, "stale")); err != nil {
		t.Fatal(err)
	}
	if err := apply(insert(30, "equal")); err != nil {
		t.Fatal(err)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+qualified+` WHERE id=1`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("stale/equal mutation resurrected tombstoned row")
	}
	var stored string
	var deleted bool
	if err := pool.QueryRow(ctx, `SELECT watermark_value,deleted FROM wallaby.watermark_state WHERE flow_id=$1 AND target_schema='public' AND target_table=$2 AND key_values=ARRAY['1']::text[]`, flowID, table).Scan(&stored, &deleted); err != nil {
		t.Fatal(err)
	}
	if stored != "30" || !deleted {
		t.Fatalf("state=%s/%t, want 30/tombstone", stored, deleted)
	}
}
