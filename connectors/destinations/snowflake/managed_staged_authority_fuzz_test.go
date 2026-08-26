package snowflake

import "testing"

func FuzzClassifyStagedTargetNeverCompletesWithoutManifest(f *testing.F) {
	f.Add("a", 1)
	f.Add("a,b", 2)
	f.Fuzz(func(t *testing.T, raw string, count int) {
		if count < 0 {
			count = -count
		}
		if count > 3 {
			count %= 4
		}
		expected := []string{raw}
		actual := map[string]int{raw: count}
		observation := classifyStagedTarget(expected, false, true, actual)
		if observation.state == stagedTargetComplete {
			t.Fatal("target classification completed without an immutable manifest")
		}
	})
}
