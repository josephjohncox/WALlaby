//go:build race

package artifactlog

// rapidPlannerMaxRecords is reduced under the race detector. Race-instrumented
// Arrow/Parquet encoding is orders of magnitude slower, and this package shares
// one `go test -race` invocation with internal/failmatrix, whose real child
// processes have wall-clock durability deadlines. An unbounded property here
// saturates the machine and makes that unrelated gate flake. Full-strength
// property coverage still runs in `just test` and `just test-rapid`.
const rapidPlannerMaxRecords = 2
