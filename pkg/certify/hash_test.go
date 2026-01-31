package certify

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"testing"
)

func TestHashValueMapOrder(t *testing.T) {
	first := map[string]any{"b": 2, "a": 1}
	second := map[string]any{"a": 1, "b": 2}

	h1 := sha256.New()
	if err := hashValue(h1, first); err != nil {
		t.Fatalf("hash first: %v", err)
	}
	h2 := sha256.New()
	if err := hashValue(h2, second); err != nil {
		t.Fatalf("hash second: %v", err)
	}
	if !bytes.Equal(h1.Sum(nil), h2.Sum(nil)) {
		t.Fatalf("map order should not affect hash")
	}
}

func TestHashValueJSONRawMessage(t *testing.T) {
	rawA := json.RawMessage(`{"a":1,"b":2}`)
	rawB := json.RawMessage(`{"b":2,"a":1}`)

	h1 := sha256.New()
	if err := hashValue(h1, rawA); err != nil {
		t.Fatalf("hash rawA: %v", err)
	}
	h2 := sha256.New()
	if err := hashValue(h2, rawB); err != nil {
		t.Fatalf("hash rawB: %v", err)
	}
	if !bytes.Equal(h1.Sum(nil), h2.Sum(nil)) {
		t.Fatalf("json key order should not affect hash")
	}
}
