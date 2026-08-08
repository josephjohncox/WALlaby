package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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

	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres administrator: %v", err)
	}
	defer admin.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	sourceDB := "wallaby_cert_source_" + suffix
	destinationDB := "wallaby_cert_destination_" + suffix
	for _, database := range []string{sourceDB, destinationDB} {
		if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{database}.Sanitize()); err != nil {
			t.Fatalf("create certificate database %s: %v", database, err)
		}
		database := database
		defer func() {
			_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+pgx.Identifier{database}.Sanitize()+" WITH (FORCE)")
		}()
	}
	sourceDSN, err := dsnWithDatabase(dsn, sourceDB)
	if err != nil {
		t.Fatal(err)
	}
	destinationDSN, err := dsnWithDatabase(dsn, destinationDB)
	if err != nil {
		t.Fatal(err)
	}
	source, err := pgxpool.New(ctx, sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	destination, err := pgxpool.New(ctx, destinationDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()

	const fullTable = "public.certificate_rows"
	for name, pool := range map[string]*pgxpool.Pool{"source": source, "destination": destination} {
		if _, err := pool.Exec(ctx, `CREATE TABLE public.certificate_rows (id INT PRIMARY KEY, amount NUMERIC, payload JSONB)`); err != nil {
			t.Fatalf("create %s table: %v", name, err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO public.certificate_rows (id, amount, payload) VALUES (1, 12.5, '{"a":1,"b":2}'), (2, 3.14, '{"b":2,"a":1}')`); err != nil {
			t.Fatalf("insert %s rows: %v", name, err)
		}
	}

	report, err := certify.CertifyPostgresTable(ctx, sourceDSN, nil, destinationDSN, nil, fullTable, certify.TableCertOptions{})
	if err != nil {
		t.Fatalf("certify: %v", err)
	}
	if !report.Match {
		t.Fatalf("expected certificate match, got %+v", report)
	}
	if report.Source.Rows != 2 || report.Destination.Rows != 2 {
		t.Fatalf("unexpected row counts: %+v", report)
	}
	if _, err := destination.Exec(ctx, `UPDATE public.certificate_rows SET amount=99.99 WHERE id=2`); err != nil {
		t.Fatalf("mutate destination independently: %v", err)
	}
	mismatch, err := certify.CertifyPostgresTable(ctx, sourceDSN, nil, destinationDSN, nil, fullTable, certify.TableCertOptions{})
	if err != nil {
		t.Fatalf("certify mismatch: %v", err)
	}
	if mismatch.Match || mismatch.Source.Hash == mismatch.Destination.Hash {
		t.Fatalf("independently mutated destination unexpectedly matched: %+v", mismatch)
	}
}
