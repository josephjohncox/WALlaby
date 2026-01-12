---- MODULE CDCFlow ----
EXTENDS Naturals, Sequences, TLC, FiniteSets

(***************************************************************************
 Lightweight CDC protocol + flow lifecycle spec.
 This captures: flow state transitions, read/deliver/ack ordering,
 and checkpoint monotonicity. It intentionally abstracts away payloads
 and focuses on ordering and durability invariants.
***************************************************************************)

CONSTANTS MaxLSN, MaxRetries, GateDDL, DDLSet, FailureMode, GiveUpMode

FailureModes == {"HoldSlot", "DropSlot"}
GiveUpModes == {"Never", "OnRetryExhaustion"}

ASSUME FailureMode \in FailureModes
ASSUME GiveUpMode \in GiveUpModes

FlowStates == {"Created", "Running", "Paused", "Stopped", "FailedHoldingSlot", "FailedDroppedSlot"}

VARIABLES
  flow,          \* current flow state
  nextRead,      \* next LSN to read from source
  inflight,      \* LSNs read but not yet delivered
  delivered,     \* LSNs delivered to destinations
  lastAcked,     \* last acked LSN (monotonic)
  lastCheckpoint,\* last persisted checkpoint (monotonic)
  ddlPending,    \* gated DDL LSNs awaiting approval
  ddlApproved,   \* DDL LSNs approved
  ddlApplied,    \* DDL LSNs applied
  readAttempts,  \* read attempt counts per LSN
  writeAttempts  \* write attempt counts per LSN

vars == <<flow, nextRead, inflight, delivered, lastAcked, lastCheckpoint,
          ddlPending, ddlApproved, ddlApplied, readAttempts, writeAttempts>>

FailState == IF FailureMode = "HoldSlot" THEN "FailedHoldingSlot" ELSE "FailedDroppedSlot"

Init ==
  /\ flow = "Created"
  /\ nextRead = 1
  /\ inflight = {}
  /\ delivered = {}
  /\ lastAcked = 0
  /\ lastCheckpoint = 0
  /\ ddlPending = {}
  /\ ddlApproved = {}
  /\ ddlApplied = {}
  /\ readAttempts = [lsn \in 1..MaxLSN |-> 0]
  /\ writeAttempts = [lsn \in 1..MaxLSN |-> 0]

Start ==
  /\ flow = "Created"
  /\ flow' = "Running"
  /\ UNCHANGED <<nextRead, inflight, delivered, lastAcked, lastCheckpoint,
                 ddlPending, ddlApproved, ddlApplied, readAttempts, writeAttempts>>

Pause ==
  /\ flow = "Running"
  /\ flow' = "Paused"
  /\ UNCHANGED <<nextRead, inflight, delivered, lastAcked, lastCheckpoint,
                 ddlPending, ddlApproved, ddlApplied, readAttempts, writeAttempts>>

Resume ==
  /\ flow = "Paused"
  /\ (\/ ~GateDDL
      \/ ddlPending = {})
  /\ flow' = "Running"
  /\ UNCHANGED <<nextRead, inflight, delivered, lastAcked, lastCheckpoint,
                 ddlPending, ddlApproved, ddlApplied, readAttempts, writeAttempts>>

Stop ==
  /\ flow \in {"Running", "Paused"}
  /\ flow' = "Stopped"
  /\ UNCHANGED <<nextRead, inflight, delivered, lastAcked, lastCheckpoint,
                 ddlPending, ddlApproved, ddlApplied, readAttempts, writeAttempts>>

Fail ==
  /\ flow \in {"Running", "Paused"}
  /\ flow' = FailState
  /\ UNCHANGED <<nextRead, inflight, delivered, lastAcked, lastCheckpoint,
                 ddlPending, ddlApproved, ddlApplied, readAttempts, writeAttempts>>

ReadBatch ==
  /\ flow = "Running"
  /\ nextRead <= MaxLSN
  /\ nextRead \notin DDLSet
  /\ readAttempts[nextRead] < MaxRetries \/ GiveUpMode = "Never"
  /\ inflight' = inflight \cup {nextRead}
  /\ nextRead' = nextRead + 1
  /\ readAttempts' =
      [readAttempts EXCEPT ![nextRead] = IF readAttempts[nextRead] < MaxRetries THEN @ + 1 ELSE @]
  /\ UNCHANGED <<flow, delivered, lastAcked, lastCheckpoint,
                 ddlPending, ddlApproved, ddlApplied, writeAttempts>>

ReadDDL ==
  /\ flow = "Running"
  /\ nextRead <= MaxLSN
  /\ nextRead \in DDLSet
  /\ readAttempts[nextRead] < MaxRetries \/ GiveUpMode = "Never"
  /\ inflight' = inflight \cup {nextRead}
  /\ ddlPending' = ddlPending \cup {nextRead}
  /\ nextRead' = nextRead + 1
  /\ readAttempts' =
      [readAttempts EXCEPT ![nextRead] = IF readAttempts[nextRead] < MaxRetries THEN @ + 1 ELSE @]
  /\ flow' = IF GateDDL THEN "Paused" ELSE flow
  /\ UNCHANGED <<delivered, lastAcked, lastCheckpoint,
                 ddlApproved, ddlApplied, writeAttempts>>

ReadFail ==
  /\ flow = "Running"
  /\ nextRead <= MaxLSN
  /\ readAttempts[nextRead] <= MaxRetries
  /\ readAttempts' = [readAttempts EXCEPT ![nextRead] = IF readAttempts[nextRead] < MaxRetries THEN @ + 1 ELSE @]
  /\ UNCHANGED <<flow, nextRead, inflight, delivered, lastAcked, lastCheckpoint,
                 ddlPending, ddlApproved, ddlApplied, writeAttempts>>

ReadGiveUp ==
  /\ flow = "Running"
  /\ nextRead <= MaxLSN
  /\ GiveUpMode = "OnRetryExhaustion"
  /\ readAttempts[nextRead] >= MaxRetries
  /\ flow' = FailState
  /\ UNCHANGED <<nextRead, inflight, delivered, lastAcked, lastCheckpoint,
                 ddlPending, ddlApproved, ddlApplied, readAttempts, writeAttempts>>

Deliver ==
  /\ flow = "Running"
  /\ inflight # {}
  /\ \E lsn \in inflight:
      /\ delivered' = delivered \cup {lsn}
      /\ inflight' = inflight \ {lsn}
      /\ writeAttempts[lsn] < MaxRetries \/ GiveUpMode = "Never"
      /\ writeAttempts' =
          [writeAttempts EXCEPT ![lsn] = IF writeAttempts[lsn] < MaxRetries THEN @ + 1 ELSE @]
      /\ UNCHANGED <<flow, nextRead, lastAcked, lastCheckpoint,
                     ddlPending, ddlApproved, ddlApplied, readAttempts>>

WriteFail ==
  /\ flow = "Running"
  /\ inflight # {}
  /\ \E lsn \in inflight:
      /\ writeAttempts[lsn] <= MaxRetries
      /\ writeAttempts' = [writeAttempts EXCEPT ![lsn] = IF writeAttempts[lsn] < MaxRetries THEN @ + 1 ELSE @]
      /\ UNCHANGED <<flow, nextRead, inflight, delivered, lastAcked, lastCheckpoint,
                     ddlPending, ddlApproved, ddlApplied, readAttempts>>

WriteGiveUp ==
  /\ flow = "Running"
  /\ inflight # {}
  /\ \E lsn \in inflight:
      /\ GiveUpMode = "OnRetryExhaustion"
      /\ writeAttempts[lsn] >= MaxRetries
      /\ flow' = FailState
      /\ UNCHANGED <<nextRead, inflight, delivered, lastAcked, lastCheckpoint,
                     ddlPending, ddlApproved, ddlApplied, readAttempts, writeAttempts>>

Ack ==
  /\ flow = "Running"
  /\ lastAcked < MaxLSN
  /\ (lastAcked + 1) \in delivered
  /\ lastAcked' = lastAcked + 1
  /\ lastCheckpoint' = lastAcked'
  /\ UNCHANGED <<flow, nextRead, inflight, delivered,
                 ddlPending, ddlApproved, ddlApplied, readAttempts, writeAttempts>>

ApproveDDL ==
  /\ ddlPending # {}
  /\ \E lsn \in ddlPending:
      /\ ddlPending' = ddlPending \ {lsn}
      /\ ddlApproved' = ddlApproved \cup {lsn}
      /\ UNCHANGED <<flow, nextRead, inflight, delivered, lastAcked, lastCheckpoint,
                     ddlApplied, readAttempts, writeAttempts>>

ApplyDDL ==
  /\ ddlApproved # {}
  /\ \E lsn \in ddlApproved:
      /\ ddlApplied' = ddlApplied \cup {lsn}
      /\ UNCHANGED <<flow, nextRead, inflight, delivered, lastAcked, lastCheckpoint,
                     ddlPending, ddlApproved, readAttempts, writeAttempts>>

ResumeAfterDDL ==
  /\ flow = "Paused"
  /\ GateDDL
  /\ ddlPending = {}
  /\ flow' = "Running"
  /\ UNCHANGED <<nextRead, inflight, delivered, lastAcked, lastCheckpoint,
                 ddlPending, ddlApproved, ddlApplied, readAttempts, writeAttempts>>

Idle ==
  /\ flow \in {"Stopped", "FailedHoldingSlot", "FailedDroppedSlot"}
  /\ UNCHANGED vars

Next ==
  \/ Start
  \/ Pause
  \/ Resume
  \/ Stop
  \/ Fail
  \/ ReadBatch
  \/ ReadDDL
  \/ ReadFail
  \/ ReadGiveUp
  \/ Deliver
  \/ WriteFail
  \/ WriteGiveUp
  \/ Ack
  \/ ApproveDDL
  \/ ApplyDDL
  \/ ResumeAfterDDL
  \/ Idle

Spec == Init /\ [][Next]_vars

SpecFair ==
  Spec
  /\ WF_vars(ReadBatch)
  /\ WF_vars(ReadDDL)
  /\ WF_vars(Deliver)
  /\ WF_vars(Ack)
  /\ WF_vars(ReadGiveUp)
  /\ WF_vars(WriteGiveUp)

SpecWitness ==
  SpecFair
  /\ WF_vars(ApproveDDL)
  /\ WF_vars(ApplyDDL)
  /\ WF_vars(ResumeAfterDDL)

RunningForever == <>[] (flow = "Running")

Termination == RunningForever => <> (lastAcked = MaxLSN)

NoTerminalStates ==
  /\ flow # "Stopped"
  /\ flow # "FailedHoldingSlot"
  /\ flow # "FailedDroppedSlot"

EventuallyDDLApproved == <> (ddlApproved # {})

EventuallyDDLApplied == <> (ddlApplied # {})

(***************************************************************************
 Invariants
***************************************************************************)
TypeInvariant ==
  /\ flow \in FlowStates
  /\ nextRead \in 1..(MaxLSN + 1)
  /\ inflight \subseteq 1..MaxLSN
  /\ delivered \subseteq 1..MaxLSN
  /\ lastAcked \in 0..MaxLSN
  /\ lastCheckpoint \in 0..MaxLSN
  /\ ddlPending \subseteq 1..MaxLSN
  /\ ddlApproved \subseteq 1..MaxLSN
  /\ ddlApplied \subseteq 1..MaxLSN
  /\ readAttempts \in [1..MaxLSN -> Nat]
  /\ writeAttempts \in [1..MaxLSN -> Nat]
  /\ DDLSet \subseteq 1..MaxLSN
  /\ FailureMode \in FailureModes
  /\ GiveUpMode \in GiveUpModes

NoAckWithoutDeliver == lastAcked \in delivered \cup {0}

AckMonotonic == lastCheckpoint <= lastAcked

CheckpointMonotonic == lastCheckpoint <= lastAcked

ReadAheadBounded == Cardinality(inflight) <= MaxLSN

RetryBounds ==
  /\ \A lsn \in 1..MaxLSN: readAttempts[lsn] <= MaxRetries
  /\ \A lsn \in 1..MaxLSN: writeAttempts[lsn] <= MaxRetries

DDLAppliedAfterApproval == ddlApplied \subseteq ddlApproved

DDLGatedPausesFlow ==
  (GateDDL /\ ddlPending # {}) => flow # "Running"

FlowTransitionsValid ==
  flow = "Created"
  \/ flow = "Running"
  \/ flow = "Paused"
  \/ flow = "Stopped"
  \/ flow = "FailedHoldingSlot"
  \/ flow = "FailedDroppedSlot"

====
