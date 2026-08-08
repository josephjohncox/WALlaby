package app

import (
	"os"
	"strings"
	"testing"
)

func TestSharedControlMigrationsPrecedeComponentConstruction(t *testing.T) {
	raw, err := os.ReadFile("app.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	migration := strings.Index(source, "controlplane.ApplyMigrations(ctx, controlPool)")
	for _, constructor := range []string{"authority.NewPostgresStore(controlPool)", "delivery.NewCoordinator(ctx, controlPool)", "workflow.NewPostgresEngineWithPoolAndRegistry(ctx, controlPool, connectorRegistry)", "registry.NewPostgresStoreWithPool(ctx, controlPool)", "pgstream.NewStore(ctx, cfg.Postgres.DSN)"} {
		position := strings.Index(source, constructor)
		if migration < 0 || position < 0 || migration >= position {
			t.Fatalf("shared control migration must precede %s", constructor)
		}
	}
}
