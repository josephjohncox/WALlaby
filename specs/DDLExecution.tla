---- MODULE DDLExecution ----
EXTENDS Naturals, TLC

(***************************************************************************
 Durable DDL execution across a non-transactional boundary.

 Prepare is committed in WALlaby before an external destination side effect.
 A crash may occur after the destination commit and before the receipt. On
 restart the destination must reconcile the immutable manifest: observed
 applied work records the missing receipt; observed absent work may execute;
 indeterminate observations fail closed and leave all durable state unchanged.
***************************************************************************)

CONSTANTS Destinations

ProcessStates == {"Up", "Crashed"}

VARIABLES
  process,
  attempted,
  externallyApplied,
  receipted,
  commitCount,
  lockHeld

vars == <<process, attempted, externallyApplied, receipted, commitCount, lockHeld>>

Init ==
  /\ process = "Up"
  /\ attempted = {}
  /\ externallyApplied = {}
  /\ receipted = {}
  /\ commitCount = [d \in Destinations |-> 0]
  /\ lockHeld = {}

AcquireExecutionLock(d) ==
  /\ process = "Up"
  /\ d \notin lockHeld
  /\ lockHeld' = lockHeld \cup {d}
  /\ UNCHANGED <<process, attempted, externallyApplied, receipted, commitCount>>

ReleaseExecutionLock(d) ==
  /\ process = "Up"
  /\ d \in lockHeld
  /\ lockHeld' = lockHeld \ {d}
  /\ UNCHANGED <<process, attempted, externallyApplied, receipted, commitCount>>

Prepare(d) ==
  /\ process = "Up"
  /\ d \in lockHeld
  /\ d \notin attempted
  /\ attempted' = attempted \cup {d}
  /\ UNCHANGED <<process, externallyApplied, receipted, commitCount, lockHeld>>

Apply(d) ==
  /\ process = "Up"
  /\ d \in lockHeld
  /\ d \in attempted
  /\ d \notin externallyApplied
  /\ externallyApplied' = externallyApplied \cup {d}
  /\ commitCount' = [commitCount EXCEPT ![d] = @ + 1]
  /\ UNCHANGED <<process, attempted, receipted, lockHeld>>

RecordReceipt(d) ==
  /\ process = "Up"
  /\ d \in lockHeld
  /\ d \in attempted
  /\ d \in externallyApplied
  /\ d \notin receipted
  /\ receipted' = receipted \cup {d}
  /\ UNCHANGED <<process, attempted, externallyApplied, commitCount, lockHeld>>

Crash ==
  /\ process = "Up"
  /\ process' = "Crashed"
  /\ lockHeld' = {}
  /\ UNCHANGED <<attempted, externallyApplied, receipted, commitCount>>

Restart ==
  /\ process = "Crashed"
  /\ process' = "Up"
  /\ UNCHANGED <<attempted, externallyApplied, receipted, commitCount, lockHeld>>

ReconcileApplied(d) ==
  /\ process = "Up"
  /\ d \in lockHeld
  /\ d \in attempted
  /\ d \in externallyApplied
  /\ d \notin receipted
  /\ receipted' = receipted \cup {d}
  /\ UNCHANGED <<process, attempted, externallyApplied, commitCount, lockHeld>>

ReconcileNotApplied(d) ==
  /\ process = "Up"
  /\ d \in lockHeld
  /\ d \in attempted
  /\ d \notin externallyApplied
  /\ externallyApplied' = externallyApplied \cup {d}
  /\ commitCount' = [commitCount EXCEPT ![d] = @ + 1]
  /\ UNCHANGED <<process, attempted, receipted, lockHeld>>

ReconcileIndeterminate(d) ==
  /\ process = "Up"
  /\ d \in lockHeld
  /\ d \in attempted
  /\ d \notin receipted
  /\ UNCHANGED vars

Next ==
  \/ \E d \in Destinations: AcquireExecutionLock(d)
  \/ \E d \in Destinations: ReleaseExecutionLock(d)
  \/ \E d \in Destinations: Prepare(d)
  \/ \E d \in Destinations: Apply(d)
  \/ \E d \in Destinations: RecordReceipt(d)
  \/ Crash
  \/ Restart
  \/ \E d \in Destinations: ReconcileApplied(d)
  \/ \E d \in Destinations: ReconcileNotApplied(d)
  \/ \E d \in Destinations: ReconcileIndeterminate(d)

Spec == Init /\ [][Next]_vars

TypeInvariant ==
  /\ process \in ProcessStates
  /\ attempted \subseteq Destinations
  /\ externallyApplied \subseteq Destinations
  /\ receipted \subseteq Destinations
  /\ commitCount \in [Destinations -> Nat]
  /\ lockHeld \subseteq Destinations

ExternalCommitRequiresAttempt == externallyApplied \subseteq attempted
ReceiptRequiresExternalCommit == receipted \subseteq externallyApplied
ExternalCommitExactlyOnce == \A d \in Destinations: commitCount[d] <= 1
CommitCountMatchesState ==
  \A d \in Destinations:
    commitCount[d] = IF d \in externallyApplied THEN 1 ELSE 0
IndeterminateFailsClosed ==
  \A d \in Destinations:
    ReconcileIndeterminate(d) => UNCHANGED vars

====
