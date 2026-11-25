---- MODULE DDLExecution ----
EXTENDS Naturals, TLC

(***************************************************************************
 Durable DDL execution with explicit, fallible observation.

 External truth is not visible to Apply or RecordReceipt. A new durable
 attempt grants one execution permission. After process death that permission
 is lost: only a sound Absent observation can grant it again. An Applied
 observation permits a receipt; Indeterminate permits neither action.

 Environment assumption A-OBS-SOUND: definite Applied/Absent observations
 are truthful. Indeterminate is possible whether the effect exists or not.
 This is a bounded abstract protocol model, not Go/SQL implementation proof.
***************************************************************************)

CONSTANTS Destinations

ProcessStates == {"Up", "Crashed"}
Observations == {"Unobserved", "Applied", "Absent", "Indeterminate"}

VARIABLES process, attempted, externallyApplied, receipted, commitCount,
          lockHeld, observation, mayApply, knownApplied, unsafeExecution

vars == <<process, attempted, externallyApplied, receipted, commitCount,
          lockHeld, observation, mayApply, knownApplied, unsafeExecution>>

Init ==
  /\ process = "Up"
  /\ attempted = {}
  /\ externallyApplied = {}
  /\ receipted = {}
  /\ commitCount = [d \in Destinations |-> 0]
  /\ lockHeld = {}
  /\ observation = [d \in Destinations |-> "Unobserved"]
  /\ mayApply = {}
  /\ knownApplied = {}
  /\ unsafeExecution = FALSE

AcquireExecutionLock(d) ==
  /\ process = "Up"
  /\ d \notin lockHeld
  /\ lockHeld' = lockHeld \cup {d}
  /\ UNCHANGED <<process, attempted, externallyApplied, receipted, commitCount,
                  observation, mayApply, knownApplied, unsafeExecution>>

ReleaseExecutionLock(d) ==
  /\ process = "Up"
  /\ d \in lockHeld
  /\ lockHeld' = lockHeld \ {d}
  /\ UNCHANGED <<process, attempted, externallyApplied, receipted, commitCount,
                  observation, mayApply, knownApplied, unsafeExecution>>

Prepare(d) ==
  /\ process = "Up"
  /\ d \in lockHeld
  /\ d \notin attempted
  /\ attempted' = attempted \cup {d}
  /\ mayApply' = mayApply \cup {d}
  /\ UNCHANGED <<process, externallyApplied, receipted, commitCount, lockHeld,
                  observation, knownApplied, unsafeExecution>>

Apply(d) ==
  /\ process = "Up"
  /\ d \in lockHeld
  /\ d \in attempted
  /\ d \in mayApply
  /\ externallyApplied' = externallyApplied \cup {d}
  /\ commitCount' = [commitCount EXCEPT ![d] = @ + 1]
  /\ knownApplied' = knownApplied \cup {d}
  /\ mayApply' = mayApply \ {d}
  /\ unsafeExecution' = unsafeExecution \/ (observation[d] = "Indeterminate")
  /\ observation' = [observation EXCEPT ![d] = "Applied"]
  /\ UNCHANGED <<process, attempted, receipted, lockHeld>>

RecordReceipt(d) ==
  /\ process = "Up"
  /\ d \in lockHeld
  /\ d \in attempted
  /\ d \in knownApplied
  /\ d \notin receipted
  /\ receipted' = receipted \cup {d}
  /\ UNCHANGED <<process, attempted, externallyApplied, commitCount, lockHeld,
                  observation, mayApply, knownApplied, unsafeExecution>>

Crash ==
  /\ process = "Up"
  /\ process' = "Crashed"
  /\ lockHeld' = {}
  /\ mayApply' = {}
  /\ knownApplied' = {}
  /\ observation' = [d \in Destinations |-> "Unobserved"]
  /\ UNCHANGED <<attempted, externallyApplied, receipted, commitCount, unsafeExecution>>

Restart ==
  /\ process = "Crashed"
  /\ process' = "Up"
  /\ UNCHANGED <<attempted, externallyApplied, receipted, commitCount, lockHeld,
                  observation, mayApply, knownApplied, unsafeExecution>>

ReconcileApplied(d) ==
  /\ process = "Up"
  /\ d \in lockHeld
  /\ d \in attempted
  /\ d \in externallyApplied
  /\ d \notin receipted
  /\ observation' = [observation EXCEPT ![d] = "Applied"]
  /\ knownApplied' = knownApplied \cup {d}
  /\ mayApply' = mayApply \ {d}
  /\ UNCHANGED <<process, attempted, externallyApplied, receipted, commitCount,
                  lockHeld, unsafeExecution>>

ReconcileNotApplied(d) ==
  /\ process = "Up"
  /\ d \in lockHeld
  /\ d \in attempted
  /\ d \notin externallyApplied
  /\ d \notin receipted
  /\ observation' = [observation EXCEPT ![d] = "Absent"]
  /\ mayApply' = mayApply \cup {d}
  /\ knownApplied' = knownApplied \ {d}
  /\ UNCHANGED <<process, attempted, externallyApplied, receipted, commitCount,
                  lockHeld, unsafeExecution>>

ReconcileIndeterminate(d) ==
  /\ process = "Up"
  /\ d \in lockHeld
  /\ d \in attempted
  /\ d \notin receipted
  /\ observation' = [observation EXCEPT ![d] = "Indeterminate"]
  /\ mayApply' = mayApply \ {d}
  /\ knownApplied' = knownApplied \ {d}
  /\ UNCHANGED <<process, attempted, externallyApplied, receipted, commitCount,
                  lockHeld, unsafeExecution>>

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
  /\ observation \in [Destinations -> Observations]
  /\ mayApply \subseteq Destinations
  /\ knownApplied \subseteq Destinations
  /\ unsafeExecution \in BOOLEAN

ExternalCommitRequiresAttempt == externallyApplied \subseteq attempted
ReceiptRequiresExternalCommit == receipted \subseteq externallyApplied
ExternalCommitExactlyOnce == \A d \in Destinations: commitCount[d] <= 1
CommitCountMatchesState ==
  \A d \in Destinations:
    commitCount[d] = IF d \in externallyApplied THEN 1 ELSE 0

IndeterminateFailsClosed ==
  /\ ~unsafeExecution
  /\ \A d \in Destinations:
       observation[d] = "Indeterminate" =>
         /\ d \notin mayApply
         /\ d \notin knownApplied
         /\ d \notin receipted

====
