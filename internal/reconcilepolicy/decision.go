// Package reconcilepolicy contains the dependency-free reconciliation decision
// used by the managed delivery coordinator. Values deliberately use uint64 so
// the decision is stated over the full machine word, not a narrowed bound.
package reconcilepolicy

const (
	Wait   uint64 = 0
	Retry  uint64 = 1
	Adopt  uint64 = 2
	Reject uint64 = 3
)

// Decide permits a new attempt only after definite absence, and adoption only
// after definite application. Error-bearing observations never grant either
// permission. Unknown numeric values without an error are malformed, not absent.
// Wire observations: 0=indeterminate, 1=not applied, 2=applied.
//
// Gobra clauses below are comments. verification/gobra/verify.py lifts them and
// binds this single unnamed result to res. They are not executable tokens.
// @ ensures failed ==> res == Wait
// @ ensures !failed && observation == 0 ==> res == Wait
// @ ensures observation == 0 ==> res == Wait
// @ ensures !failed && observation == 1 ==> res == Retry
// @ ensures !failed && observation == 2 ==> res == Adopt
// @ ensures !failed && observation != 0 && observation != 1 && observation != 2 ==> res == Reject
// @ ensures res == Wait ==> (failed || observation == 0)
// @ ensures res == Retry ==> (!failed && observation == 1)
// @ ensures res == Adopt ==> (!failed && observation == 2)
// @ ensures res == Reject ==> (!failed && observation != 0 && observation != 1 && observation != 2)
// @ ensures res == Wait || res == Retry || res == Adopt || res == Reject
// @ ensures 0 <= res && res <= 3
// @ ensures Wait == 0 && Retry == 1 && Adopt == 2 && Reject == 3
// @ decreases
func Decide(observation uint64, failed bool) uint64 {
	if failed {
		return Wait
	}
	if observation == 1 {
		return Retry
	}
	if observation == 2 {
		return Adopt
	}
	if observation == 0 {
		return Wait
	}
	return Reject
}
