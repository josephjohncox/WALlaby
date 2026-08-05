//go:build ignore

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/controlplane"
	"github.com/josephjohncox/wallaby/internal/controlstore"
)

func main() {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "TEST_PG_DSN is required")
		os.Exit(2)
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		panic(err)
	}
	controlstore.ConfigurePool(cfg)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	if err := controlplane.ApplyMigrations(ctx, pool); err != nil {
		panic(err)
	}
}
