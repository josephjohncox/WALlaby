package controlstore

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConfigurePoolUsesCanonicalAuthorityProtocol(t *testing.T) {
	cfg, err := pgxpool.ParseConfig("postgres://localhost/wallaby")
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["wallaby.authority_protocol"] = "v1"
	ConfigurePool(cfg)
	if AuthorityProtocol != "v2" {
		t.Fatalf("canonical authority protocol=%q, want v2", AuthorityProtocol)
	}
	if got := cfg.ConnConfig.RuntimeParams["wallaby.authority_protocol"]; got != AuthorityProtocol {
		t.Fatalf("authority protocol=%q, want canonical %s", got, AuthorityProtocol)
	}
}
