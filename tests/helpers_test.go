package tests

import (
	"encoding/json"
	"testing"
)

func recordKey(t testing.TB, key map[string]any) []byte {
	if key == nil {
		return nil
	}
	payload, err := json.Marshal(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	return payload
}
