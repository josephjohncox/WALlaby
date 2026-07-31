//go:build !race

package artifactlog

// rapidPlannerMaxRecords is the full-strength record bound for the planner
// determinism property. Each drawn record can become its own artifact when the
// property also draws a small split threshold, so this bound is the dominant
// cost multiplier of the property.
const rapidPlannerMaxRecords = 32
