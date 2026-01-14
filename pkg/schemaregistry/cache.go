package schemaregistry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
)

type cachedRegistry struct {
	base  Registry
	mu    sync.Mutex
	cache map[string]RegisterResult
}

func newCachedRegistry(base Registry) Registry {
	if base == nil {
		return nil
	}
	return &cachedRegistry{
		base:  base,
		cache: make(map[string]RegisterResult),
	}
}

func (c *cachedRegistry) Register(ctx context.Context, req RegisterRequest) (RegisterResult, error) {
	key := cacheKey(req)
	c.mu.Lock()
	if existing, ok := c.cache[key]; ok {
		c.mu.Unlock()
		return existing, nil
	}
	c.mu.Unlock()

	result, err := c.base.Register(ctx, req)
	if err != nil {
		return RegisterResult{}, err
	}
	c.mu.Lock()
	c.cache[key] = result
	c.mu.Unlock()
	return result, nil
}

func (c *cachedRegistry) Close() error {
	if c.base == nil {
		return nil
	}
	return c.base.Close()
}

func cacheKey(req RegisterRequest) string {
	refs, _ := json.Marshal(req.References)
	payload := req.Subject + "::" + string(req.SchemaType) + "::" + req.Schema + "::" + string(refs)
	hash := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(hash[:])
}
