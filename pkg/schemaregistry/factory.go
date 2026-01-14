package schemaregistry

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var ErrRegistryDisabled = errors.New("schema registry disabled")

// NewRegistry creates a registry from config. Returns nil if disabled.
func NewRegistry(ctx context.Context, cfg Config) (Registry, error) {
	typ := strings.ToLower(strings.TrimSpace(cfg.Type))
	switch typ {
	case "", "none", "disabled":
		return nil, ErrRegistryDisabled
	case "csr", "confluent":
		reg, err := newConfluentRegistry(cfg)
		if err != nil {
			return nil, err
		}
		return newCachedRegistry(reg), nil
	case "apicurio":
		reg, err := newApicurioRegistry(cfg)
		if err != nil {
			return nil, err
		}
		return newCachedRegistry(reg), nil
	case "glue":
		reg, err := newGlueRegistry(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return newCachedRegistry(reg), nil
	case "local", "memory", "mem":
		return newCachedRegistry(newLocalRegistry()), nil
	case "postgres", "db":
		if cfg.DSN == "" {
			return nil, fmt.Errorf("schema_registry_dsn is required for postgres registry")
		}
		reg, err := newPostgresRegistry(ctx, cfg.DSN)
		if err != nil {
			return nil, err
		}
		return newCachedRegistry(reg), nil
	default:
		return nil, fmt.Errorf("unsupported schema registry type %q", typ)
	}
}
