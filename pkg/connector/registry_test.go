package connector

import (
	"sync"
	"testing"
)

func TestRegistryRejectsBuiltinCollisionsAndDuplicateRoles(t *testing.T) {
	registry := NewRegistry()
	if err := registry.RegisterSource("postgres", func() Source { return nil }); err == nil {
		t.Fatal("built-in source override accepted")
	}
	if err := registry.RegisterDestination("kafka", func() Destination { return nil }); err == nil {
		t.Fatal("built-in destination override accepted")
	}
	if err := registry.RegisterSource("acme", func() Source { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterSource("acme", func() Source { return nil }); err == nil {
		t.Fatal("duplicate custom source accepted")
	}
	if err := registry.RegisterDestination("acme", func() Destination { return nil }); err == nil {
		t.Fatal("cross-role custom collision accepted")
	}
}

func TestRegistryConcurrentReadsAndRegistration(t *testing.T) {
	registry := NewRegistry()
	const count = 64
	var wait sync.WaitGroup
	wait.Add(count)
	for index := 0; index < count; index++ {
		index := index
		go func() {
			defer wait.Done()
			name := "custom-" + string(rune('A'+index))
			if err := registry.RegisterDestination(name, func() Destination { return nil }); err != nil {
				t.Errorf("register %s: %v", name, err)
				return
			}
			if !registry.HasDestination(EndpointType(name)) {
				t.Errorf("registered destination %s not visible", name)
			}
		}()
	}
	wait.Wait()
}
