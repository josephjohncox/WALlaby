package schemaregistry

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestLocalRegistryPersistsRegistrationsAcrossReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	first, err := NewRegistry(ctx, Config{Type: "local", LocalDirectory: directory})
	if err != nil {
		t.Fatal(err)
	}
	request := RegisterRequest{Subject: "events-value", SchemaType: SchemaTypeAvro, Schema: `{"type":"record","name":"Event","fields":[]}`}
	registered, err := first.Register(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if registered.ID != "1" || registered.Version != 1 {
		t.Fatalf("first registration = %+v, want id=1 version=1", registered)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewRegistry(ctx, Config{Type: "local", LocalDirectory: directory})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	duplicate, err := reopened.Register(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate != registered {
		t.Fatalf("registration after reopen = %+v, want %+v", duplicate, registered)
	}
	second, err := reopened.Register(ctx, RegisterRequest{Subject: request.Subject, SchemaType: request.SchemaType, Schema: `{"type":"record","name":"Event","fields":[{"name":"id","type":"string"}]}`})
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != "2" || second.Version != 2 {
		t.Fatalf("second schema = %+v, want id=2 version=2", second)
	}
}

func TestLocalRegistryConcurrentDuplicateRegistration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	registries := make([]Registry, 2)
	for index := range registries {
		registry, err := NewRegistry(ctx, Config{Type: "local", LocalDirectory: directory})
		if err != nil {
			t.Fatal(err)
		}
		registries[index] = registry
		defer func(registry Registry) { _ = registry.Close() }(registry)
	}

	request := RegisterRequest{
		Subject:    "events-value",
		SchemaType: SchemaTypeProtobuf,
		Schema:     "message Event { string id = 1; }",
		References: []Reference{{Name: "common.proto", Subject: "common", Version: 1}},
	}
	const goroutines = 32
	results := make(chan RegisterResult, goroutines)
	errorsCh := make(chan error, goroutines)
	var wait sync.WaitGroup
	for index := 0; index < goroutines; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			result, err := registries[index%len(registries)].Register(ctx, request)
			if err != nil {
				errorsCh <- err
				return
			}
			results <- result
		}(index)
	}
	wait.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		t.Errorf("concurrent Register() error = %v", err)
	}
	for result := range results {
		if result.ID != "1" || result.Version != 1 {
			t.Errorf("concurrent Register() = %+v, want id=1 version=1", result)
		}
	}
	if t.Failed() {
		return
	}
	newSchema, err := registries[0].Register(ctx, RegisterRequest{Subject: request.Subject, SchemaType: request.SchemaType, Schema: "message Event { string id = 1; string name = 2; }", References: request.References})
	if err != nil {
		t.Fatal(err)
	}
	if newSchema.ID != "2" || newSchema.Version != 2 {
		t.Fatalf("new schema = %+v, want id=2 version=2", newSchema)
	}
}

func TestLocalRegistryRequiresUsableDirectory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	if _, err := NewRegistry(ctx, Config{Type: "local"}); err == nil {
		t.Fatal("NewRegistry(local without directory) error = nil")
	}

	parent := t.TempDir()
	filePath := filepath.Join(parent, "not-a-directory")
	if err := os.WriteFile(filePath, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRegistry(ctx, Config{Type: "local", LocalDirectory: filePath}); err == nil {
		t.Fatal("NewRegistry(local with file directory) error = nil")
	}

	blockedDatabase := filepath.Join(parent, "blocked")
	if err := os.Mkdir(blockedDatabase, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(blockedDatabase, localRegistryDatabaseFile), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRegistry(ctx, Config{Type: "local", LocalDirectory: blockedDatabase}); err == nil {
		t.Fatal("NewRegistry(local with unusable database path) error = nil")
	}
}

func TestLocalRegistryContextCancellationAndClose(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	canceledOpen, cancelOpen := context.WithCancel(ctx)
	cancelOpen()
	if registry, err := NewRegistry(canceledOpen, Config{Type: "local", LocalDirectory: t.TempDir()}); !errors.Is(err, context.Canceled) {
		if registry != nil {
			_ = registry.Close()
		}
		t.Fatalf("NewRegistry(canceled context) error = %v, want context.Canceled", err)
	}

	registry, err := NewRegistry(ctx, Config{Type: "local", LocalDirectory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	request := RegisterRequest{Subject: "events", SchemaType: SchemaTypeAvro, Schema: `{}`}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := registry.Register(canceled, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("Register(canceled context) error = %v, want context.Canceled", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(ctx, request); err == nil {
		t.Fatal("Register() after Close error = nil")
	}
}
