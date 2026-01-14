package schemaregistry

import (
	"context"
	"fmt"
	"sync"
)

type localRegistry struct {
	mu       sync.Mutex
	nextID   int
	byKey    map[string]RegisterResult
	versions map[string]int
}

func newLocalRegistry() *localRegistry {
	return &localRegistry{
		nextID:   1,
		byKey:    make(map[string]RegisterResult),
		versions: make(map[string]int),
	}
}

func (r *localRegistry) Register(_ context.Context, req RegisterRequest) (RegisterResult, error) {
	if req.Subject == "" {
		return RegisterResult{}, fmt.Errorf("schema registry subject is required")
	}
	key := cacheKey(req)
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.byKey[key]; ok {
		return existing, nil
	}
	r.versions[req.Subject]++
	version := r.versions[req.Subject]
	id := r.nextID
	r.nextID++
	result := RegisterResult{ID: fmt.Sprintf("%d", id), Version: version}
	r.byKey[key] = result
	return result, nil
}

func (r *localRegistry) Close() error { return nil }
