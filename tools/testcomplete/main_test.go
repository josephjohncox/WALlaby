package main

import (
	"reflect"
	"testing"
)

func TestMissingTests(t *testing.T) {
	t.Parallel()

	expected := map[string]map[string]struct{}{
		"example/a": {"TestOne": {}, "TestTwo": {}},
		"example/b": {"TestThree": {}},
	}
	actual := map[string]map[string]struct{}{
		"example/a": {"TestOne": {}},
		"example/b": {"TestThree": {}},
	}
	if got, want := missingTests(expected, actual), []string{"example/a:TestTwo"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("missingTests()=%v, want %v", got, want)
	}
}

func TestSetDifference(t *testing.T) {
	t.Parallel()

	expected := map[string]struct{}{"example/a": {}, "example/b": {}}
	actual := map[string]struct{}{"example/b": {}}
	if got, want := setDifference(expected, actual), []string{"example/a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("setDifference()=%v, want %v", got, want)
	}
}
