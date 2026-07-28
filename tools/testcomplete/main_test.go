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

func TestRecordCompletedPackageAcceptsNoTestFileSkipButNotTestSkip(t *testing.T) {
	t.Parallel()
	completed := map[string]struct{}{}
	for _, event := range []testEvent{
		{Action: "skip", Package: "example/no-tests"},
		{Action: "pass", Package: "example/tests"},
		{Action: "skip", Package: "example/tests", Test: "TestUnavailable"},
		{Action: "fail", Package: "example/failing"},
	} {
		recordCompletedPackage(completed, event)
	}
	want := map[string]struct{}{"example/no-tests": {}, "example/tests": {}}
	if !reflect.DeepEqual(completed, want) {
		t.Fatalf("completed packages=%v, want %v", completed, want)
	}
}

func TestRecordAccountedTestCountsSkipAndFailButNotRunOrSubtests(t *testing.T) {
	t.Parallel()
	accounted := map[string]map[string]struct{}{}
	for _, event := range []testEvent{
		{Action: "run", Package: "example/a", Test: "TestOne"},
		{Action: "skip", Package: "example/a", Test: "TestOne"},
		{Action: "pass", Package: "example/a", Test: "TestTwo/subtest"},
		{Action: "fail", Package: "example/a", Test: "TestTwo"},
		{Action: "pass", Package: "example/a", Test: "TestThree"},
	} {
		recordAccountedTest(accounted, event)
	}
	// A gated skip and a real failure are both accounted for (they reached a
	// decision); a running-only event and a subtest event are not top-level
	// terminal states. This is what keeps credential-gated skips from being
	// misreported as missing while omitted tests still fail the completeness gate.
	want := map[string]map[string]struct{}{"example/a": {"TestOne": {}, "TestTwo": {}, "TestThree": {}}}
	if !reflect.DeepEqual(accounted, want) {
		t.Fatalf("accounted tests=%v, want %v", accounted, want)
	}
}

func TestRecordPassedTestIgnoresSkippedRunningAndNestedEvents(t *testing.T) {
	t.Parallel()
	passed := map[string]map[string]struct{}{}
	for _, event := range []testEvent{
		{Action: "run", Package: "example/a", Test: "TestOne"},
		{Action: "skip", Package: "example/a", Test: "TestOne"},
		{Action: "pass", Package: "example/a", Test: "TestTwo/subtest"},
		{Action: "fail", Package: "example/a", Test: "TestTwo"},
		{Action: "pass", Package: "example/a", Test: "TestThree"},
	} {
		recordPassedTest(passed, event)
	}
	want := map[string]map[string]struct{}{"example/a": {"TestThree": {}}}
	if !reflect.DeepEqual(passed, want) {
		t.Fatalf("passed tests=%v, want %v", passed, want)
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
