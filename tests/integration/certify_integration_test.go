package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/pkg/certify"
)

func TestPostgresDataCertificate(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	tableName := fmt.Sprintf("cert_table_%d", time.Now().UnixNano())
	fullTable := fmt.Sprintf(`public.%s`, tableName)
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, amount NUMERIC, payload JSONB)`, tableName))
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	}()

	_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, amount, payload) VALUES (1, 12.5, '{"a":1,"b":2}'), (2, 3.14, '{"b":2,"a":1}')`, tableName))
	if err != nil {
		t.Fatalf("insert rows: %v", err)
	}

	report, err := certify.CertifyPostgresTable(ctx, dsn, nil, dsn, nil, fullTable, certify.TableCertOptions{})
	if err != nil {
		t.Fatalf("certify: %v", err)
	}
	if !report.Match {
		t.Fatalf("expected certificate match, got %+v", report)
	}
	if report.Source.Rows != 2 || report.Destination.Rows != 2 {
		t.Fatalf("unexpected row counts: %+v", report)
	}
}
