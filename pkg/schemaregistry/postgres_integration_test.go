package schemaregistry

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestPostgresRegistryConcurrentRegistration(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	registry, err := newPostgresRegistry(ctx, dsn)
	if err != nil {
		t.Fatalf("newPostgresRegistry() error = %v", err)
	}
	defer func() { _ = registry.Close() }()

	subject := "wallaby-concurrent-" + uuid.NewString()
	defer func() {
		if _, err := registry.pool.Exec(context.Background(), "DELETE FROM wallaby_schema_registry WHERE subject = $1", subject); err != nil {
			t.Logf("cleanup schema registry subject: %v", err)
		}
	}()

	const registrations = 12
	versions := make([]int, registrations)
	errs := make([]error, registrations)
	var wg sync.WaitGroup
	for i := 0; i < registrations; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			result, registerErr := registry.Register(ctx, RegisterRequest{
				Subject:    subject,
				SchemaType: SchemaTypeAvro,
				Schema:     fmt.Sprintf(`{"type":"record","name":"Event%d","fields":[]}`, index),
			})
			versions[index] = result.Version
			errs[index] = registerErr
		}(i)
	}
	wg.Wait()
	for i, registerErr := range errs {
		if registerErr != nil {
			t.Fatalf("Register(%d) error = %v", i, registerErr)
		}
	}
	sort.Ints(versions)
	for i, version := range versions {
		if version != i+1 {
			t.Fatalf("versions = %v, want contiguous 1..%d", versions, registrations)
		}
	}

	identical := RegisterRequest{
		Subject:    subject,
		SchemaType: SchemaTypeAvro,
		Schema:     `{"type":"record","name":"Shared","fields":[]}`,
	}
	results := make([]RegisterResult, registrations)
	for i := 0; i < registrations; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			results[index], errs[index] = registry.Register(ctx, identical)
		}(i)
	}
	wg.Wait()
	for i, registerErr := range errs {
		if registerErr != nil {
			t.Fatalf("Register identical (%d) error = %v", i, registerErr)
		}
		if results[i] != results[0] {
			t.Fatalf("identical results differ: got %+v, want %+v", results[i], results[0])
		}
	}
	if results[0].Version != registrations+1 {
		t.Fatalf("identical version = %d, want %d", results[0].Version, registrations+1)
	}
}
