package main

import "testing"

func TestResolveProfileCI(t *testing.T) {
	got, err := resolveProfile(" CI ")
	if err != nil {
		t.Fatalf("resolveProfile(ci): %v", err)
	}
	if got.Name != "ci" || got.InitialRows != 250 || got.Operations != 1000 || got.Writers != 2 {
		t.Fatalf("resolveProfile(ci) = %+v", got)
	}
	if got.BatchSize <= 0 || got.EmptyReads <= 0 {
		t.Fatalf("resolveProfile(ci) has invalid execution bounds: %+v", got)
	}
}
